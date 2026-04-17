package com.demo.job4;

import com.demo.common.config.KafkaConfig;
import com.demo.common.model.BehaviorWithDim;
import com.demo.common.model.BhvExt;
import com.demo.common.serde.JsonSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

/**
 * Job4: Sequence Feature Job — 行为补全层
 * ----------------------------------------
 * Source : dwd_behavior_with_dim
 * Process:
 *   用户侧（2a）：pv/click/fav/cart/buy 五类行为序列 → Redis ZSet（seq:{bhv_type}:{uid}）
 *   搜索意图（2b）：bhv_page=search 时写 seq:query:{uid}（List，LPUSH+LTRIM）
 *   商品侧（2c）：show 事件写 seq:item_pv:{item_id}，click 写 seq:item_click:{item_id}
 *
 * Redis key 规范（详见 ARCHITECTURE.md Section 3.2）：
 *   seq:pv:{uid}            ZSet  score=ts, member=item_id, 最近50条, TTL 7d
 *   seq:click:{uid}         ZSet  score=ts, member=item_id, 最近50条, TTL 7d
 *   seq:fav:{uid}           ZSet  score=ts, member=item_id, 最近30条, TTL 7d
 *   seq:cart:{uid}          ZSet  score=ts, member=item_id, 最近30条, TTL 7d
 *   seq:buy:{uid}           ZSet  score=ts, member=item_id, 最近20条, TTL 30d
 *   seq:query:{uid}         List  最近20条, LPUSH+LTRIM, TTL 7d
 *   seq:item_pv:{item_id}   ZSet  score=ts, member=uid, 最近200条, TTL 2h
 *   seq:item_click:{item_id}ZSet  score=ts, member=uid, 最近100条, TTL 2h
 *
 * Submit:
 *   flink run -c com.demo.job4.SequenceFeatureJob flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar
 */
public class SequenceFeatureJob {

    private static final Logger LOG = LoggerFactory.getLogger(SequenceFeatureJob.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);
        env.setParallelism(1);

        // ── Source ─────────────────────────────────────────────────────────────
        KafkaSource<BehaviorWithDim> source = KafkaSource.<BehaviorWithDim>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .setTopics(KafkaConfig.TOPIC_DWD_BEHAVIOR_WITH_DIM)
                .setGroupId(KafkaConfig.GROUP_JOB4)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(
                        org.apache.kafka.clients.consumer.OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new JsonSchema.Deserializer<>(BehaviorWithDim.class))
                .build();

        DataStream<BehaviorWithDim> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "KafkaSource-dwd_behavior_with_dim_job4"
        );

        // ── Write sequences to Redis ───────────────────────────────────────────
        // No output stream needed; all writes go directly to Redis via ProcessFunction
        stream.process(new SequenceWriterFunction())
              .name("SequenceWriter")
              .addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>())
              .name("Sink-discard");

        env.execute("Job4-SequenceFeature");
    }

    // ── Sequence Writer ────────────────────────────────────────────────────────
    /**
     * Writes behavior sequences to Redis.
     * Returns Void (no downstream output).
     */
    static class SequenceWriterFunction extends ProcessFunction<BehaviorWithDim, Void> {

        private static final long serialVersionUID = 1L;

        // TTL constants (seconds)
        private static final int TTL_7D  = 604_800;
        private static final int TTL_30D = 2_592_000;
        private static final int TTL_2H  = 7_200;

        // ZSet size limits
        private static final int LIMIT_PV    = 50;
        private static final int LIMIT_CLICK = 50;
        private static final int LIMIT_FAV   = 30;
        private static final int LIMIT_CART  = 30;
        private static final int LIMIT_BUY   = 20;
        private static final int LIMIT_QUERY = 20;
        private static final int LIMIT_ITEM_PV    = 200;
        private static final int LIMIT_ITEM_CLICK = 100;

        private transient JedisPool jedisPool;

        @Override
        public void open(Configuration parameters) {
            JedisPoolConfig cfg = new JedisPoolConfig();
            cfg.setMaxTotal(20);
            cfg.setMaxIdle(10);
            cfg.setMinIdle(2);
            jedisPool = new JedisPool(cfg, "redis", 6379);
            LOG.info("[Job4] SequenceWriterFunction opened");
        }

        @Override
        public void close() {
            if (jedisPool != null) jedisPool.close();
        }

        @Override
        public void processElement(BehaviorWithDim e, Context ctx, Collector<Void> out) {
            if (e.uid == null) return;

            try (Jedis jedis = jedisPool.getResource()) {
                Pipeline pipe = jedis.pipelined();

                String bhvType  = e.bhvType;
                String bhvValue = e.bhvValue;
                String bhvPage  = e.bhvPage;
                BhvExt ext      = e.bhvExt;
                double ts       = (double) e.ts;

                // ── show 事件 ──────────────────────────────────────────────────
                // Job1 将 show 事件按 item 展开为多条记录，每条记录的 item_id 已提升到顶层字段；
                // bhv_ext.items 在展开后被清空，不可再从 ext.items 读取。
                if ("show".equals(bhvType) && e.itemId != null) {

                    // 2a: seq:pv:{uid} — 每条展开记录写一个 member
                    String pvKey = "seq:pv:" + e.uid;
                    pipe.zadd(pvKey, ts, e.itemId);
                    pipe.zremrangeByRank(pvKey, 0, -(LIMIT_PV + 1));
                    pipe.expire(pvKey, TTL_7D);

                    // 2c: seq:item_pv:{item_id}
                    String itemPvKey = "seq:item_pv:" + e.itemId;
                    pipe.zadd(itemPvKey, ts, e.uid);
                    pipe.zremrangeByRank(itemPvKey, 0, -(LIMIT_ITEM_PV + 1));
                    pipe.expire(itemPvKey, TTL_2H);
                }

                // ── click 事件 ─────────────────────────────────────────────────
                else if ("click".equals(bhvType)) {
                    String itemId = (ext != null) ? ext.itemId : null;

                    if (bhvValue == null) {
                        // 2a: pure click → seq:click:{uid}
                        if (itemId != null) {
                            String clickKey = "seq:click:" + e.uid;
                            pipe.zadd(clickKey, ts, itemId);
                            pipe.zremrangeByRank(clickKey, 0, -(LIMIT_CLICK + 1));
                            pipe.expire(clickKey, TTL_7D);

                            // 2c: seq:item_click:{item_id}
                            String itemClickKey = "seq:item_click:" + itemId;
                            pipe.zadd(itemClickKey, ts, e.uid);
                            pipe.zremrangeByRank(itemClickKey, 0, -(LIMIT_ITEM_CLICK + 1));
                            pipe.expire(itemClickKey, TTL_2H);
                        }
                    } else {
                        // 2a: PDP actions → fav / cart / buy
                        if (itemId != null) {
                            switch (bhvValue) {
                                case "fav": {
                                    String favKey = "seq:fav:" + e.uid;
                                    pipe.zadd(favKey, ts, itemId);
                                    pipe.zremrangeByRank(favKey, 0, -(LIMIT_FAV + 1));
                                    pipe.expire(favKey, TTL_7D);
                                    break;
                                }
                                case "cart": {
                                    String cartKey = "seq:cart:" + e.uid;
                                    pipe.zadd(cartKey, ts, itemId);
                                    pipe.zremrangeByRank(cartKey, 0, -(LIMIT_CART + 1));
                                    pipe.expire(cartKey, TTL_7D);
                                    break;
                                }
                                case "buy": {
                                    String buyKey = "seq:buy:" + e.uid;
                                    pipe.zadd(buyKey, ts, itemId);
                                    pipe.zremrangeByRank(buyKey, 0, -(LIMIT_BUY + 1));
                                    pipe.expire(buyKey, TTL_30D);
                                    break;
                                }
                                default:
                                    break;
                            }
                        }
                    }
                }

                // 2b: search query — 任何 search 页事件且 ext 有 query 字段
                if ("search".equals(bhvPage) && ext != null && ext.query != null && !ext.query.isEmpty()) {
                    String queryKey = "seq:query:" + e.uid;
                    pipe.lpush(queryKey, ext.query);
                    pipe.ltrim(queryKey, 0, LIMIT_QUERY - 1);
                    pipe.expire(queryKey, TTL_7D);
                }

                pipe.sync();

            } catch (Exception ex) {
                LOG.warn("[Job4] Redis write failed for uid={}: {}", e.uid, ex.getMessage());
            }
        }
    }
}

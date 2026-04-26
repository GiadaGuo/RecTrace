package com.demo.job4;

import com.demo.common.config.KafkaConfig;
import com.demo.common.model.BehaviorWithDim;
import com.demo.common.model.BhvExt;
import com.demo.common.serde.JsonSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Job4: Sequence Feature Job — 行为补全层
 * ----------------------------------------
 * Source : dwd_behavior_with_dim
 * Process:
 *   用户侧（2a）：pv/click/fav/cart/buy 五类行为序列 → Redis ZSet（seq:{bhv_type}:{uid}）
 *   搜索意图（2b）：bhv_page=search 时写 seq:query:{uid}（List，LPUSH+LTRIM）
 *   商品侧（2c）：show 事件写 seq:item_pv:{item_id}，click 写 seq:item_click:{item_id}
 *
 * 写入策略：
 *   keyBy(uid) 后使用 KeyedProcessFunction，每条事件加入 ListState 攒批，
 *   500ms processing-time timer 触发一次 flush。
 *   高频热点 key（seq:pv / seq:item_pv）：Lua 脚本封装单 key 写入，Pipeline 批量提交多 key。
 *   非热点 key（click/fav/cart/buy/query 及 item_click）：单次 Lua evalsha。
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

        // ── keyBy uid → 微批攒批写 Redis ──────────────────────────────────────
        stream.keyBy(e -> e.uid)
              .process(new SequenceWriterFunction())
              .name("SequenceWriter")
              .addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>())
              .name("Sink-discard");

        env.execute("Job4-SequenceFeature");
    }

    // ── Sequence Writer ────────────────────────────────────────────────────────
    /**
     * 微批写入策略：
     *   - 每条事件存入 ListState buffer
     *   - 首条事件注册 500ms processing-time timer
     *   - timer 触发时 flush：
     *       高频 show 事件 → 同 uid 多条合并为一次 Lua 调用（批量 ZADD），
     *                         多个 item_id 的 seq:item_pv 用 Pipeline 批量提交各自 Lua 调用
     *       其他事件        → 单次 Lua evalsha
     */
    static class SequenceWriterFunction extends KeyedProcessFunction<String, BehaviorWithDim, Void> {

        private static final long serialVersionUID = 2L;

        // flush 间隔（毫秒）
        private static final long FLUSH_INTERVAL_MS = 500L;

        // TTL constants (seconds)
        private static final int TTL_7D  = 604_800;
        private static final int TTL_30D = 2_592_000;
        private static final int TTL_2H  = 7_200;

        // ZSet size limits
        private static final int LIMIT_PV         = 50;
        private static final int LIMIT_CLICK       = 50;
        private static final int LIMIT_FAV         = 30;
        private static final int LIMIT_CART        = 30;
        private static final int LIMIT_BUY         = 20;
        private static final int LIMIT_QUERY       = 20;
        private static final int LIMIT_ITEM_PV     = 200;
        private static final int LIMIT_ITEM_CLICK  = 100;

        // ── Lua 脚本 ───────────────────────────────────────────────────────────
        /**
         * ZSet 批量写脚本（用于高频 seq:pv / seq:item_pv）：
         *   KEYS[1]          = ZSet key
         *   ARGV[1]          = limit（保留条数）
         *   ARGV[2]          = TTL（秒）
         *   ARGV[3,5,7,...]  = score（timestamp as string）
         *   ARGV[4,6,8,...]  = member
         */
        private static final String LUA_ZSET_BATCH =
                "for i=3,#ARGV,2 do\n" +
                "  redis.call('ZADD', KEYS[1], ARGV[i], ARGV[i+1])\n" +
                "end\n" +
                "redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -(tonumber(ARGV[1])+1))\n" +
                "redis.call('EXPIRE', KEYS[1], ARGV[2])\n" +
                "return 1";

        /**
         * ZSet 单条写脚本（用于 click/fav/cart/buy/item_click）：
         *   KEYS[1] = ZSet key
         *   ARGV[1] = limit
         *   ARGV[2] = TTL
         *   ARGV[3] = score
         *   ARGV[4] = member
         */
        private static final String LUA_ZSET_SINGLE =
                "redis.call('ZADD', KEYS[1], ARGV[3], ARGV[4])\n" +
                "redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -(tonumber(ARGV[1])+1))\n" +
                "redis.call('EXPIRE', KEYS[1], ARGV[2])\n" +
                "return 1";

        /**
         * List 写脚本（用于 seq:query）：
         *   KEYS[1] = List key
         *   ARGV[1] = limit
         *   ARGV[2] = TTL
         *   ARGV[3] = value
         */
        private static final String LUA_LIST_SINGLE =
                "redis.call('LPUSH', KEYS[1], ARGV[3])\n" +
                "redis.call('LTRIM', KEYS[1], 0, tonumber(ARGV[1])-1)\n" +
                "redis.call('EXPIRE', KEYS[1], ARGV[2])\n" +
                "return 1";

        // Lua SHA（预加载后赋值）
        private transient String luaShaBatch;
        private transient String luaShaSingle;
        private transient String luaShaList;

        private transient JedisPool jedisPool;

        // Flink 托管状态
        private transient ListState<BehaviorWithDim> buffer;
        private transient ValueState<Long> timerTs;

        @Override
        public void open(Configuration parameters) throws Exception {
            JedisPoolConfig cfg = new JedisPoolConfig();
            cfg.setMaxTotal(20);
            cfg.setMaxIdle(10);
            cfg.setMinIdle(2);
            jedisPool = new JedisPool(cfg, "redis", 6379);

            // 预加载 Lua 脚本，获取 SHA
            try (Jedis jedis = jedisPool.getResource()) {
                luaShaBatch  = jedis.scriptLoad(LUA_ZSET_BATCH);
                luaShaSingle = jedis.scriptLoad(LUA_ZSET_SINGLE);
                luaShaList   = jedis.scriptLoad(LUA_LIST_SINGLE);
            }

            buffer = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("seq-buffer", TypeInformation.of(BehaviorWithDim.class)));
            timerTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("seq-timer", Long.class));

            LOG.info("[Job4] SequenceWriterFunction opened");
        }

        @Override
        public void close() {
            if (jedisPool != null) jedisPool.close();
        }

        @Override
        public void processElement(BehaviorWithDim e, Context ctx, Collector<Void> out) throws Exception {
            if (e.uid == null) return;

            buffer.add(e);

            // 若还没有挂起的 timer，注册一个 500ms 后触发的 processing-time timer
            if (timerTs.value() == null) {
                long fireTime = ctx.timerService().currentProcessingTime() + FLUSH_INTERVAL_MS;
                ctx.timerService().registerProcessingTimeTimer(fireTime);
                timerTs.update(fireTime);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Void> out) throws Exception {
            List<BehaviorWithDim> events = new ArrayList<>();
            for (BehaviorWithDim e : buffer.get()) {
                events.add(e);
            }
            buffer.clear();
            timerTs.clear();

            if (events.isEmpty()) return;

            flushToRedis(events);
        }

        // ── Redis flush 逻辑 ──────────────────────────────────────────────────

        private void flushToRedis(List<BehaviorWithDim> events) {
            try (Jedis jedis = jedisPool.getResource()) {

                // 按事件类型分组
                List<BehaviorWithDim> showEvents  = new ArrayList<>();
                List<BehaviorWithDim> clickEvents = new ArrayList<>();
                List<BehaviorWithDim> queryEvents = new ArrayList<>();

                for (BehaviorWithDim e : events) {
                    if ("show".equals(e.bhvType)) {
                        showEvents.add(e);
                    } else if ("click".equals(e.bhvType)) {
                        clickEvents.add(e);
                    }
                    // search query 独立检查（任何类型事件都可能带 query）
                    if ("search".equals(e.bhvPage)) {
                        BhvExt ext = e.bhvExt;
                        if (ext != null && ext.query != null && !ext.query.isEmpty()) {
                            queryEvents.add(e);
                        }
                    }
                }

                // ── 高频 show 事件：Lua+Pipeline ────────────────────────────
                flushShowEvents(jedis, showEvents);

                // ── click/fav/cart/buy：单次 Lua evalsha ────────────────────
                flushClickEvents(jedis, clickEvents);

                // ── search query：单次 Lua evalsha ──────────────────────────
                flushQueryEvents(jedis, queryEvents);

            } catch (Exception ex) {
                LOG.warn("[Job4] Redis flush failed for uid={}: {}", events.get(0).uid, ex.getMessage());
            }
        }

        /**
         * 高频 show 事件写入：
         *   seq:pv:{uid}          — 同 uid 多条合并为一次 Lua 调用（批量 ZADD）
         *   seq:item_pv:{item_id} — 每个 item_id 一次 Lua 调用，通过 Pipeline 批量提交
         */
        private void flushShowEvents(Jedis jedis, List<BehaviorWithDim> showEvents) {
            if (showEvents.isEmpty()) return;

            // 2a: 合并同 uid 的所有 show 事件，构造批量 ZADD 参数
            // showEvents 已经按 uid keyBy，同一 flush 周期内全是同一 uid
            String uid = showEvents.get(0).uid;
            List<String> pvArgs = new ArrayList<>();
            pvArgs.add(String.valueOf(LIMIT_PV));  // ARGV[1] = limit
            pvArgs.add(String.valueOf(TTL_7D));     // ARGV[2] = TTL

            // 2c: item_pv 写入，需要 Pipeline 批量提交（每 item_id 独立 key）
            // 先收集 item_pv 写入参数，最后用 Pipeline 提交
            // Map<itemId, List<ts>> — 理论上同 uid 同 item 可能多次曝光
            Map<String, List<Double>> itemPvMap = new HashMap<>();

            for (BehaviorWithDim e : showEvents) {
                if (e.itemId == null) continue;
                pvArgs.add(String.valueOf((double) e.ts));  // score
                pvArgs.add(e.itemId);                       // member

                itemPvMap.computeIfAbsent(e.itemId, k -> new ArrayList<>()).add((double) e.ts);
            }

            // 2a: seq:pv:{uid} — 一次批量 Lua 调用
            if (pvArgs.size() > 2) { // 有实际 member 才写
                String pvKey = "seq:pv:" + uid;
                jedis.evalsha(luaShaBatch,
                        java.util.Collections.singletonList(pvKey),
                        pvArgs);
            }

            // 2c: seq:item_pv:{item_id} — Pipeline 批量提交各自 Lua 调用
            if (!itemPvMap.isEmpty()) {
                Pipeline pipe = jedis.pipelined();
                for (Map.Entry<String, List<Double>> entry : itemPvMap.entrySet()) {
                    String itemId = entry.getKey();
                    List<Double> tsList = entry.getValue();

                    List<String> itemPvArgs = new ArrayList<>();
                    itemPvArgs.add(String.valueOf(LIMIT_ITEM_PV));
                    itemPvArgs.add(String.valueOf(TTL_2H));
                    for (double ts : tsList) {
                        itemPvArgs.add(String.valueOf(ts));
                        itemPvArgs.add(uid); // member = uid
                    }

                    String itemPvKey = "seq:item_pv:" + itemId;
                    pipe.evalsha(luaShaBatch,
                            java.util.Collections.singletonList(itemPvKey),
                            itemPvArgs);
                }
                pipe.sync();
            }
        }

        /**
         * click 事件写入：
         *   纯点击 → seq:click:{uid} + seq:item_click:{item_id}
         *   PDP 动作 → seq:fav/cart/buy:{uid}
         * 均为单次 Lua evalsha。
         */
        private void flushClickEvents(Jedis jedis, List<BehaviorWithDim> clickEvents) {
            for (BehaviorWithDim e : clickEvents) {
                BhvExt ext    = e.bhvExt;
                String itemId = (ext != null) ? ext.itemId : null;
                double ts     = (double) e.ts;

                if (e.bhvValue == null) {
                    // 纯点击
                    if (itemId != null) {
                        // seq:click:{uid}
                        jedis.evalsha(luaShaSingle,
                                java.util.Collections.singletonList("seq:click:" + e.uid),
                                buildZSetArgs(LIMIT_CLICK, TTL_7D, ts, itemId));

                        // seq:item_click:{item_id}
                        jedis.evalsha(luaShaSingle,
                                java.util.Collections.singletonList("seq:item_click:" + itemId),
                                buildZSetArgs(LIMIT_ITEM_CLICK, TTL_2H, ts, e.uid));
                    }
                } else {
                    // PDP 动作：fav / cart / buy
                    if (itemId != null) {
                        switch (e.bhvValue) {
                            case "fav":
                                jedis.evalsha(luaShaSingle,
                                        java.util.Collections.singletonList("seq:fav:" + e.uid),
                                        buildZSetArgs(LIMIT_FAV, TTL_7D, ts, itemId));
                                break;
                            case "cart":
                                jedis.evalsha(luaShaSingle,
                                        java.util.Collections.singletonList("seq:cart:" + e.uid),
                                        buildZSetArgs(LIMIT_CART, TTL_7D, ts, itemId));
                                break;
                            case "buy":
                                jedis.evalsha(luaShaSingle,
                                        java.util.Collections.singletonList("seq:buy:" + e.uid),
                                        buildZSetArgs(LIMIT_BUY, TTL_30D, ts, itemId));
                                break;
                            default:
                                break;
                        }
                    }
                }
            }
        }

        /**
         * search query 写入：seq:query:{uid}，单次 Lua evalsha。
         */
        private void flushQueryEvents(Jedis jedis, List<BehaviorWithDim> queryEvents) {
            for (BehaviorWithDim e : queryEvents) {
                String query = e.bhvExt.query;
                jedis.evalsha(luaShaList,
                        java.util.Collections.singletonList("seq:query:" + e.uid),
                        java.util.Arrays.asList(
                                String.valueOf(LIMIT_QUERY),
                                String.valueOf(TTL_7D),
                                query));
            }
        }

        // ── 工具方法 ──────────────────────────────────────────────────────────

        private static List<String> buildZSetArgs(int limit, int ttl, double score, String member) {
            return java.util.Arrays.asList(
                    String.valueOf(limit),
                    String.valueOf(ttl),
                    String.valueOf(score),
                    member);
        }
    }
}

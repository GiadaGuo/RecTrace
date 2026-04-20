package com.demo.job2;

import com.demo.common.config.KafkaConfig;
import com.demo.common.model.BehaviorWithDim;
import com.demo.common.model.UserItemFeature;
import com.demo.common.serde.JsonSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.time.Duration;

/**
 * Job2: Window Aggregation - Home-Channel Cross Feature per (user_id + category_id)
 * ----------------------------------------------------------------------------------
 * Source : dwd_behavior_with_dim
 *
 * Filter : home 推荐漏斗全链路事件
 *   - bhv_page=home                          → show（曝光）和 click（点击）
 *   - bhv_page=pdp AND bhv_src=home          → cart/fav/buy（从 home 进入 PDP 后的转化行为）
 *   NOTE: Job1 从本版本起为 bhv_page=home 的 show 事件拼接商品维度（brand/price）。
 *
 * Process: 1-minute tumbling event-time window keyed by (user_id, category_id)
 *   Counts: show_count, click_count, cart_count, fav_count, buy_count
 *
 * Sink   : dws_user_item_feature (Kafka)
 *          feat:user_item:{uid}:{catId} (Redis Hash, TTL 600s, 覆盖写)
 *
 * 口径设计：
 *   - show_count：home 曝光次数（bhv_type=show, bhv_page=home，Job1 已拼商品维度）
 *   - click_count：home 点击次数（bhv_type=click, bhv_value=null, bhv_page=home）
 *   - cart/fav/buy：从 home 路径进入 PDP 后的转化（bhv_src=home）
 *
 * 与 cross:* 的分工：
 *   cross:{uid}:{catId} → 全渠道长期历史兴趣（Job3，指数衰减，30d TTL）
 *   feat:user_item:*    → home 渠道短期窗口转化漏斗（Job2，1min 窗口，600s TTL）
 *
 * Learning focus: TumblingEventTimeWindow, AggregateFunction + ProcessWindowFunction,
 *                 Watermark strategy for out-of-order events
 *
 * Submit:
 *   flink run -c com.demo.job2.WindowAggJob flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar
 */
public class WindowAggJob {

    private static final Logger LOG = LoggerFactory.getLogger(WindowAggJob.class);

    // [EXPERIMENT-1] Side output tag for late records（场景一：收集被丢弃的迟到事件，方便统计数量）
    static final OutputTag<BehaviorWithDim> LATE_TAG =
            new OutputTag<BehaviorWithDim>("late-records") {};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);
        env.setParallelism(1);

        // ── Source ─────────────────────────────────────────────────────────
        KafkaSource<BehaviorWithDim> source = KafkaSource.<BehaviorWithDim>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .setTopics(KafkaConfig.TOPIC_DWD_BEHAVIOR_WITH_DIM)
                .setGroupId(KafkaConfig.GROUP_JOB2)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(org.apache.kafka.clients.consumer.OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new JsonSchema.Deserializer<>(BehaviorWithDim.class))
                .build();

        DataStream<BehaviorWithDim> stream = env.fromSource(
                source,
                WatermarkStrategy.<BehaviorWithDim>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((e, ts) -> e.ts),
                "KafkaSource-dwd_behavior_with_dim"
        );

        // ── Filter: home 推荐漏斗全链路 ────────────────────────────────────
        // show + click：bhv_page=home
        // cart/fav/buy：bhv_page=pdp AND bhv_src=home（从 home 推荐路径进入 PDP 的转化）
        DataStream<BehaviorWithDim> homeStream = stream
                .filter(e -> "home".equals(e.bhvPage)
                          || ("pdp".equals(e.bhvPage) && "home".equals(e.bhvSrc)))
                .name("Filter-home-funnel");

        // ── Window aggregation ─────────────────────────────────────────────
        // [EXPERIMENT-1] sideOutputLateData 收集迟到事件（watermark 之后到达的事件）
        // 恢复正常：去掉 .sideOutputLateData(LATE_TAG)，将 SingleOutputStreamOperator 改回 DataStream
        SingleOutputStreamOperator<UserItemFeature> featureStream = homeStream
                // Key by (user_id, category_id) composite key
                .keyBy(e -> e.uid + "_" + e.categoryId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .sideOutputLateData(LATE_TAG)   // [EXPERIMENT-1] 捕获迟到事件
                .aggregate(new BehaviorAggregator(), new WindowResultFunction());

        // [EXPERIMENT-1] 打印迟到事件数量到日志，用于验证修复效果
        featureStream.getSideOutput(LATE_TAG)
                .map(e -> e)
                .name("LateRecordsMonitor")
                .addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>())
                .name("LateRecordsSink-discard");
        // getSideOutput 会在 TaskManager 日志里触发 numLateRecordsDropped 指标
        // [/EXPERIMENT-1]

        // ── Sink: Kafka ────────────────────────────────────────────────────
        KafkaSink<UserItemFeature> sink = KafkaSink.<UserItemFeature>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<UserItemFeature>builder()
                                .setTopic(KafkaConfig.TOPIC_DWS_USER_ITEM_FEATURE)
                                .setKeySerializationSchema(r -> (r.userId + "_" + r.categoryId).getBytes())
                                .setValueSerializationSchema(new JsonSchema.Serializer<>(UserItemFeature.class))
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        featureStream.sinkTo(sink).name("KafkaSink-dws_user_item_feature");

        // ── Sink: Redis ────────────────────────────────────────────────────
        // 覆盖写 feat:user_item:{uid}:{catId}，TTL 600s
        featureStream.addSink(new RedisSinkFunction()).name("RedisSink-feat_user_item");

        env.execute("Job2-WindowAgg");
    }

    // ── Accumulator ──────────────────────────────────────────────────────────
    /** Intermediate accumulator holding raw counts for the home funnel */
    static class BehaviorAccumulator {
        String uid;
        int    categoryId;
        long   showCount;    // bhv_page=home, bhv_type=show
        long   clickCount;   // bhv_page=home, bhv_type=click, bhv_value=null
        long   cartCount;
        long   favCount;
        long   buyCount;
    }

    // ── AggregateFunction ─────────────────────────────────────────────────────
    static class BehaviorAggregator
            implements AggregateFunction<BehaviorWithDim, BehaviorAccumulator, BehaviorAccumulator> {

        @Override
        public BehaviorAccumulator createAccumulator() {
            return new BehaviorAccumulator();
        }

        @Override
        public BehaviorAccumulator add(BehaviorWithDim value, BehaviorAccumulator acc) {
            if (acc.uid == null) {
                acc.uid        = value.uid;
                acc.categoryId = value.categoryId;
            }
            if ("show".equals(value.bhvType) && "home".equals(value.bhvPage)) {
                // home 曝光：分母用于 CTR 和 CVR
                acc.showCount++;
            } else if ("click".equals(value.bhvType)) {
                if (value.bhvValue == null && "home".equals(value.bhvPage)) {
                    // home 点击：分母用于 CTB，分子用于 CTR
                    acc.clickCount++;
                } else if (value.bhvValue != null) {
                    // PDP 转化行为（已由上游 filter 保证 bhv_src=home）
                    switch (value.bhvValue) {
                        case "cart": acc.cartCount++; break;
                        case "fav":  acc.favCount++;  break;
                        case "buy":  acc.buyCount++;  break;
                        // 未知 bhvValue 丢弃，避免噪声
                    }
                }
            }
            return acc;
        }

        @Override
        public BehaviorAccumulator getResult(BehaviorAccumulator acc) {
            return acc;
        }

        @Override
        public BehaviorAccumulator merge(BehaviorAccumulator a, BehaviorAccumulator b) {
            a.showCount  += b.showCount;
            a.clickCount += b.clickCount;
            a.cartCount  += b.cartCount;
            a.favCount   += b.favCount;
            a.buyCount   += b.buyCount;
            return a;
        }
    }

    // ── ProcessWindowFunction ─────────────────────────────────────────────────
    /** Enriches the aggregated result with window time boundaries and derived rate metrics */
    static class WindowResultFunction
            extends ProcessWindowFunction<BehaviorAccumulator, UserItemFeature, String, TimeWindow> {

        @Override
        public void process(String key, Context ctx, Iterable<BehaviorAccumulator> elements,
                            Collector<UserItemFeature> out) {
            BehaviorAccumulator acc = elements.iterator().next();

            UserItemFeature feature = new UserItemFeature();
            feature.userId      = acc.uid != null ? acc.uid : key.split("_")[0];
            feature.categoryId  = acc.categoryId;
            feature.windowStart = ctx.window().getStart();
            feature.windowEnd   = ctx.window().getEnd();
            feature.showCount   = acc.showCount;
            feature.clickCount  = acc.clickCount;
            feature.cartCount   = acc.cartCount;
            feature.favCount    = acc.favCount;
            feature.buyCount    = acc.buyCount;

            out.collect(feature);
        }
    }

    // ── RedisSinkFunction ─────────────────────────────────────────────────────
    /**
     * 将每个窗口聚合结果写入 Redis Hash，覆盖写语义，TTL 600s。
     *
     * Key 格式: feat:user_item:{uid}:{catId}
     * Fields  : show_cnt, click_cnt, cart_cnt, fav_cnt, buy_cnt,
     *           window_start, window_end
     *
     * 口径: home 推荐漏斗全链路（show + click 来自 bhv_page=home，转化来自 bhv_src=home）
     */
    static class RedisSinkFunction
            extends org.apache.flink.streaming.api.functions.sink.RichSinkFunction<UserItemFeature> {

        private static final int TTL_SECONDS = 600;
        private transient JedisPool jedisPool;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            JedisPoolConfig cfg = new JedisPoolConfig();
            cfg.setMaxTotal(4);
            cfg.setMaxIdle(2);
            jedisPool = new JedisPool(cfg, "redis", 6379);
        }

        @Override
        public void close() {
            if (jedisPool != null) jedisPool.close();
        }

        @Override
        public void invoke(UserItemFeature f, Context context) {
            String key = "feat:user_item:" + f.userId + ":" + f.categoryId;
            try (Jedis jedis = jedisPool.getResource()) {
                Pipeline pipe = jedis.pipelined();
                pipe.hset(key, "show_cnt",     String.valueOf(f.showCount));
                pipe.hset(key, "click_cnt",    String.valueOf(f.clickCount));
                pipe.hset(key, "cart_cnt",     String.valueOf(f.cartCount));
                pipe.hset(key, "fav_cnt",      String.valueOf(f.favCount));
                pipe.hset(key, "buy_cnt",      String.valueOf(f.buyCount));
                pipe.hset(key, "window_start", String.valueOf(f.windowStart));
                pipe.hset(key, "window_end",   String.valueOf(f.windowEnd));
                pipe.expire(key, TTL_SECONDS);
                pipe.sync();
            } catch (Exception ex) {
                LOG.warn("[Job2-WindowAgg] Redis write failed key={}: {}", key, ex.getMessage());
            }
        }
    }
}

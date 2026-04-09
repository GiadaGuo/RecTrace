package com.demo.job2;

import com.demo.common.config.KafkaConfig;
import com.demo.common.model.BehaviorWithDim;
import com.demo.common.model.UserItemFeature;
import com.demo.common.serde.JsonSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Job2: Window Aggregation - Cross Feature per (user_id + category_id)
 * ----------------------------------------------------------------------
 * Source : dwd_behavior_with_dim
 * Process: 1-minute tumbling event-time window keyed by (user_id, category_id)
 *          Aggregate: pv/cart/fav/buy counts + click-to-buy rate
 * Sink   : dws_user_item_feature
 *
 * Learning focus: TumblingEventTimeWindow, AggregateFunction + ProcessWindowFunction,
 *                 Watermark strategy for out-of-order events
 *
 * Submit:
 *   flink run -c com.demo.job2.WindowAggJob flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar
 */
public class WindowAggJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);
        env.setParallelism(2);

        // ── Source ─────────────────────────────────────────────────────────
        KafkaSource<BehaviorWithDim> source = KafkaSource.<BehaviorWithDim>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .setTopics(KafkaConfig.TOPIC_DWD_BEHAVIOR_WITH_DIM)
                .setGroupId(KafkaConfig.GROUP_JOB2)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonSchema.Deserializer<>(BehaviorWithDim.class))
                .build();

        DataStream<BehaviorWithDim> stream = env.fromSource(
                source,
                WatermarkStrategy.<BehaviorWithDim>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((e, ts) -> e.timestamp),
                "KafkaSource-dwd_behavior_with_dim"
        );

        // ── Window aggregation ─────────────────────────────────────────────
        DataStream<UserItemFeature> featureStream = stream
                // Key by (user_id, category_id) composite key
                .keyBy(e -> e.userId + "_" + e.categoryId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new BehaviorAggregator(), new WindowResultFunction());

        // ── Sink ───────────────────────────────────────────────────────────
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

        env.execute("Job2-WindowAgg");
    }

    // ── Accumulator ──────────────────────────────────────────────────────────
    /** Intermediate accumulator holding raw counts */
    static class BehaviorAccumulator {
        String userId;
        int    categoryId;
        long   pvCount;
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
            if (acc.userId == null) {
                acc.userId     = value.userId;
                acc.categoryId = value.categoryId;
            }
            switch (value.behavior) {
                case "pv":   acc.pvCount++;   break;
                case "cart": acc.cartCount++; break;
                case "fav":  acc.favCount++;  break;
                case "buy":  acc.buyCount++;  break;
            }
            return acc;
        }

        @Override
        public BehaviorAccumulator getResult(BehaviorAccumulator acc) {
            return acc;
        }

        @Override
        public BehaviorAccumulator merge(BehaviorAccumulator a, BehaviorAccumulator b) {
            a.pvCount   += b.pvCount;
            a.cartCount += b.cartCount;
            a.favCount  += b.favCount;
            a.buyCount  += b.buyCount;
            return a;
        }
    }

    // ── ProcessWindowFunction ─────────────────────────────────────────────────
    /** Enriches the aggregated result with window time boundaries */
    static class WindowResultFunction
            extends ProcessWindowFunction<BehaviorAccumulator, UserItemFeature, String, TimeWindow> {

        @Override
        public void process(String key, Context ctx, Iterable<BehaviorAccumulator> elements,
                            Collector<UserItemFeature> out) {
            BehaviorAccumulator acc = elements.iterator().next();

            UserItemFeature feature = new UserItemFeature();
            feature.userId         = acc.userId != null ? acc.userId : key.split("_")[0];
            feature.categoryId     = acc.categoryId;
            feature.windowStart    = ctx.window().getStart();
            feature.windowEnd      = ctx.window().getEnd();
            feature.pvCount        = acc.pvCount;
            feature.cartCount      = acc.cartCount;
            feature.favCount       = acc.favCount;
            feature.buyCount       = acc.buyCount;
            feature.clickToBuyRate = acc.pvCount > 0
                    ? (double) acc.buyCount / acc.pvCount
                    : 0.0;

            out.collect(feature);
        }
    }
}

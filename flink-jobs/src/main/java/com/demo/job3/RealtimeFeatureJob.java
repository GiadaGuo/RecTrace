package com.demo.job3;

import com.demo.common.config.KafkaConfig;
import com.demo.common.model.BehaviorWithDim;
import com.demo.common.model.ItemFeature;
import com.demo.common.model.UserFeature;
import com.demo.common.serde.JsonSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * Job3: Real-time Feature Computation with Long-lived KeyedState
 * ----------------------------------------------------------------
 * Source : dwd_behavior_with_dim
 * Process:
 *   - UserFeatureFunction  (keyBy userId):
 *       Maintains per-user cumulative counters and active days using ValueState + MapState
 *   - ItemFeatureFunction  (keyBy itemId):
 *       Maintains per-item cumulative counters + UV (approximate) using MapState
 * Sinks  : dws_user_feature, dws_item_feature
 *
 * Learning focus: KeyedProcessFunction, ValueState, MapState, side outputs
 *
 * Submit:
 *   flink run -c com.demo.job3.RealtimeFeatureJob flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar
 */
public class RealtimeFeatureJob {

    /** Side output tag to route item feature records to a separate stream */
    private static final OutputTag<ItemFeature> ITEM_TAG =
            new OutputTag<ItemFeature>("item-feature-side-output") {};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);
        env.setParallelism(1);

        // ── Source ─────────────────────────────────────────────────────────
        KafkaSource<BehaviorWithDim> source = KafkaSource.<BehaviorWithDim>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .setTopics(KafkaConfig.TOPIC_DWD_BEHAVIOR_WITH_DIM)
                .setGroupId(KafkaConfig.GROUP_JOB3)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonSchema.Deserializer<>(BehaviorWithDim.class))
                .build();

        DataStream<BehaviorWithDim> stream = env.fromSource(
                source,
                WatermarkStrategy.<BehaviorWithDim>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((e, ts) -> e.timestamp),
                "KafkaSource-dwd_behavior_with_dim_job3"
        );

        // ── User feature stream (keyed by userId) ──────────────────────────
        SingleOutputStreamOperator<UserFeature> userFeatureStream = stream
                .keyBy(e -> e.userId)
                .process(new UserFeatureFunction());

        // ── Item feature stream (keyed by itemId, using side output) ────────
        DataStream<ItemFeature> itemFeatureStream = stream
                .keyBy(e -> e.itemId)
                .process(new ItemFeatureFunction());

        // ── Sinks ──────────────────────────────────────────────────────────
        KafkaSink<UserFeature> userSink = KafkaSink.<UserFeature>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<UserFeature>builder()
                                .setTopic(KafkaConfig.TOPIC_DWS_USER_FEATURE)
                                .setKeySerializationSchema(r -> r.userId.getBytes())
                                .setValueSerializationSchema(new JsonSchema.Serializer<>(UserFeature.class))
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<ItemFeature> itemSink = KafkaSink.<ItemFeature>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<ItemFeature>builder()
                                .setTopic(KafkaConfig.TOPIC_DWS_ITEM_FEATURE)
                                .setKeySerializationSchema(r -> r.itemId.getBytes())
                                .setValueSerializationSchema(new JsonSchema.Serializer<>(ItemFeature.class))
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        userFeatureStream.sinkTo(userSink).name("KafkaSink-dws_user_feature");
        itemFeatureStream.sinkTo(itemSink).name("KafkaSink-dws_item_feature");

        env.execute("Job3-RealtimeFeature");
    }

    // ── User Feature Function ─────────────────────────────────────────────────
    /**
     * Maintains long-lived per-user state:
     *   - totalPv / totalCart / totalFav / totalBuy  (ValueState<Long>)
     *   - lastActiveTs                               (ValueState<Long>)
     *   - activeDaysSet: day -> 1L                   (MapState<String, Long>)
     *   - userDim: age/city/level from first event   (ValueState)
     */
    static class UserFeatureFunction extends KeyedProcessFunction<String, BehaviorWithDim, UserFeature> {

        private transient ValueState<Long>           pvState;
        private transient ValueState<Long>           cartState;
        private transient ValueState<Long>           favState;
        private transient ValueState<Long>           buyState;
        private transient ValueState<Long>           lastTsState;
        private transient ValueState<Integer>        ageState;
        private transient ValueState<String>         cityState;
        private transient ValueState<Integer>        levelState;
        private transient MapState<String, Long>     activeDaysState;

        @Override
        public void open(Configuration parameters) {
            pvState       = getRuntimeContext().getState(new ValueStateDescriptor<>("pv",       Types.LONG));
            cartState     = getRuntimeContext().getState(new ValueStateDescriptor<>("cart",     Types.LONG));
            favState      = getRuntimeContext().getState(new ValueStateDescriptor<>("fav",      Types.LONG));
            buyState      = getRuntimeContext().getState(new ValueStateDescriptor<>("buy",      Types.LONG));
            lastTsState   = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTs",   Types.LONG));
            ageState      = getRuntimeContext().getState(new ValueStateDescriptor<>("age",      Types.INT));
            cityState     = getRuntimeContext().getState(new ValueStateDescriptor<>("city",     Types.STRING));
            levelState    = getRuntimeContext().getState(new ValueStateDescriptor<>("level",    Types.INT));
            activeDaysState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("activeDays", Types.STRING, Types.LONG));
        }

        @Override
        public void processElement(BehaviorWithDim e, Context ctx, Collector<UserFeature> out)
                throws Exception {

            // Update counters
            switch (e.behavior) {
                case "pv":   pvState.update(getOrDefault(pvState) + 1);     break;
                case "cart": cartState.update(getOrDefault(cartState) + 1); break;
                case "fav":  favState.update(getOrDefault(favState) + 1);   break;
                case "buy":  buyState.update(getOrDefault(buyState) + 1);   break;
            }

            // Update last active timestamp
            lastTsState.update(e.timestamp);

            // Track active days (store YYYY-MM-DD as key, value=1)
            String day = LocalDate.ofInstant(
                    java.time.Instant.ofEpochMilli(e.timestamp), ZoneId.of("Asia/Shanghai"))
                    .toString();
            activeDaysState.put(day, 1L);

            // Latch user dimension fields on first non-zero event
            if (ageState.value() == null || ageState.value() == 0) {
                ageState.update(e.userAge);
                cityState.update(e.userCity);
                levelState.update(e.userLevel);
            }

            // Emit updated user feature
            UserFeature feature = new UserFeature();
            feature.userId       = e.userId;
            feature.userAge      = getIntOrDefault(ageState);
            feature.userCity     = cityState.value() != null ? cityState.value() : "unknown";
            feature.userLevel    = getIntOrDefault(levelState);
            feature.totalPv      = getOrDefault(pvState);
            feature.totalCart    = getOrDefault(cartState);
            feature.totalFav     = getOrDefault(favState);
            feature.totalBuy     = getOrDefault(buyState);
            feature.lastActiveTs = getOrDefault(lastTsState);
            feature.activeDays   = countActiveDays();
            feature.updateTs     = System.currentTimeMillis();

            out.collect(feature);
        }

        private long getOrDefault(ValueState<Long> state) throws Exception {
            Long v = state.value();
            return v == null ? 0L : v;
        }

        private int getIntOrDefault(ValueState<Integer> state) throws Exception {
            Integer v = state.value();
            return v == null ? 0 : v;
        }

        private long countActiveDays() throws Exception {
            long count = 0;
            for (Long ignored : activeDaysState.values()) count++;
            return count;
        }
    }

    // ── Item Feature Function ─────────────────────────────────────────────────
    /**
     * Maintains long-lived per-item state:
     *   - totalPv / totalBuy / totalCart / totalFav  (ValueState<Long>)
     *   - uvSet: userId -> 1L                        (MapState<String, Long>) approximate UV
     *   - itemDim: brand/price/category              (ValueState)
     */
    static class ItemFeatureFunction extends KeyedProcessFunction<String, BehaviorWithDim, ItemFeature> {

        private transient ValueState<Long>       pvState;
        private transient ValueState<Long>       buyState;
        private transient ValueState<Long>       cartState;
        private transient ValueState<Long>       favState;
        private transient ValueState<String>     brandState;
        private transient ValueState<Double>     priceState;
        private transient ValueState<Integer>    categoryState;
        private transient MapState<String, Long> uvState;

        @Override
        public void open(Configuration parameters) {
            pvState       = getRuntimeContext().getState(new ValueStateDescriptor<>("ipv",      Types.LONG));
            buyState      = getRuntimeContext().getState(new ValueStateDescriptor<>("ibuy",     Types.LONG));
            cartState     = getRuntimeContext().getState(new ValueStateDescriptor<>("icart",    Types.LONG));
            favState      = getRuntimeContext().getState(new ValueStateDescriptor<>("ifav",     Types.LONG));
            brandState    = getRuntimeContext().getState(new ValueStateDescriptor<>("ibrand",   Types.STRING));
            priceState    = getRuntimeContext().getState(new ValueStateDescriptor<>("iprice",   Types.DOUBLE));
            categoryState = getRuntimeContext().getState(new ValueStateDescriptor<>("icat",     Types.INT));
            uvState       = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("iuv", Types.STRING, Types.LONG));
        }

        @Override
        public void processElement(BehaviorWithDim e, Context ctx, Collector<ItemFeature> out)
                throws Exception {

            switch (e.behavior) {
                case "pv":   pvState.update(getLong(pvState) + 1);     break;
                case "buy":  buyState.update(getLong(buyState) + 1);   break;
                case "cart": cartState.update(getLong(cartState) + 1); break;
                case "fav":  favState.update(getLong(favState) + 1);   break;
            }

            // Track UV (unique visitors)
            uvState.put(e.userId, 1L);

            // Latch item dimension
            if (brandState.value() == null || brandState.value().equals("unknown")) {
                brandState.update(e.itemBrand);
                priceState.update(e.itemPrice);
                categoryState.update(e.categoryId);
            }

            long pv  = getLong(pvState);
            long buy = getLong(buyState);
            long uv  = countUv();

            ItemFeature feature    = new ItemFeature();
            feature.itemId         = e.itemId;
            feature.categoryId     = getIntOrDefault(categoryState, e.categoryId);
            feature.itemBrand      = brandState.value() != null ? brandState.value() : "unknown";
            feature.itemPrice      = priceState.value() != null ? priceState.value() : 0.0;
            feature.totalPv        = pv;
            feature.totalBuy       = buy;
            feature.totalCart      = getLong(cartState);
            feature.totalFav       = getLong(favState);
            feature.uv             = uv;
            feature.conversionRate = pv > 0 ? (double) buy / pv : 0.0;
            feature.updateTs       = System.currentTimeMillis();

            out.collect(feature);
        }

        private long getLong(ValueState<Long> state) throws Exception {
            Long v = state.value();
            return v == null ? 0L : v;
        }

        private int getIntOrDefault(ValueState<Integer> state, int def) throws Exception {
            Integer v = state.value();
            return v == null ? def : v;
        }

        private long countUv() throws Exception {
            long count = 0;
            for (Long ignored : uvState.values()) count++;
            return count;
        }
    }
}

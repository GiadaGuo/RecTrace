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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

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
                .setStartingOffsets(OffsetsInitializer.committedOffsets(org.apache.kafka.clients.consumer.OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new JsonSchema.Deserializer<>(BehaviorWithDim.class))
                .build();

        DataStream<BehaviorWithDim> stream = env.fromSource(
                source,
                WatermarkStrategy.<BehaviorWithDim>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((e, ts) -> e.ts),
                "KafkaSource-dwd_behavior_with_dim_job3"
        );

        // ── User feature stream (keyed by uid) ────────────────────────────────
        SingleOutputStreamOperator<UserFeature> userFeatureStream = stream
                .keyBy(e -> e.uid)
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

        // ── 2e: Window statistics — split by dimension to keep state bounded ─
        // UserWindowStatFunction: keyBy uid — only user-level buckets (~60 entries max)
        stream
                .keyBy(e -> e.uid)
                .process(new UserWindowStatFunction())
                .name("UserWindowStatFunction")
                .addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>())
                .name("Sink-user-windowstat-discard");

        // ItemWindowStatFunction: keyBy itemId — correct cross-user aggregation
        stream
                .keyBy(e -> e.itemId)
                .process(new ItemWindowStatFunction())
                .name("ItemWindowStatFunction")
                .addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>())
                .name("Sink-item-windowstat-discard");

        // ── 2f: Cross features uid×category / uid×brand ────────────────────
        stream
                .keyBy(e -> e.uid)
                .process(new CrossFeatureFunction())
                .name("CrossFeatureFunction")
                .addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>())
                .name("Sink-crossfeature-discard");

        env.execute("Job3-RealtimeFeature");
    }

    // ── User Feature Function ─────────────────────────────────────────────────
    /**
     * Maintains long-lived per-user state:
     *   - totalPv      : page-level request count — incremented once per req_id
     *                    (a show event with N items produces 1 pv, not N)
     *   - totalClick / totalCart / totalFav / totalBuy  (ValueState<Long>)
     *   - lastActiveTs                                  (ValueState<Long>)
     *   - activeDaysSet: day -> 1L                      (MapState<String, Long>)
     *   - userDim: age/city/level from first event      (ValueState)
     *   - lastReqId: tracks the last seen req_id to deduplicate show pv
     *
     * Redis write (feat:user:{uid} Hash, TTL 30d):
     *   Written on every event via Pipeline to minimize RTT overhead.
     *   Redis failure is caught and warned — it does not affect the Kafka sink path.
     *   Write rate is bounded by event throughput; at ~100 events/s across 1000 users,
     *   each user sees ~0.1 writes/s on average, well within Redis capacity.
     */
    static class UserFeatureFunction extends KeyedProcessFunction<String, BehaviorWithDim, UserFeature> {

        private static final int USER_FEATURE_TTL = 2_592_000; // 30 days

        private transient ValueState<Long>           pvState;
        private transient ValueState<Long>           clickState;
        private transient ValueState<Long>           cartState;
        private transient ValueState<Long>           favState;
        private transient ValueState<Long>           buyState;
        private transient ValueState<Long>           lastTsState;
        private transient ValueState<Integer>        ageState;
        private transient ValueState<String>         cityState;
        private transient ValueState<Integer>        levelState;
        private transient MapState<String, Long>     activeDaysState;
        private transient ValueState<String>         lastReqIdState;

        private transient JedisPool jedisPool;

        @Override
        public void open(Configuration parameters) {
            pvState       = getRuntimeContext().getState(new ValueStateDescriptor<>("pv",       Types.LONG));
            clickState    = getRuntimeContext().getState(new ValueStateDescriptor<>("click",    Types.LONG));
            cartState     = getRuntimeContext().getState(new ValueStateDescriptor<>("cart",     Types.LONG));
            favState      = getRuntimeContext().getState(new ValueStateDescriptor<>("fav",      Types.LONG));
            buyState      = getRuntimeContext().getState(new ValueStateDescriptor<>("buy",      Types.LONG));
            lastTsState   = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTs",   Types.LONG));
            ageState      = getRuntimeContext().getState(new ValueStateDescriptor<>("age",      Types.INT));
            cityState     = getRuntimeContext().getState(new ValueStateDescriptor<>("city",     Types.STRING));
            levelState    = getRuntimeContext().getState(new ValueStateDescriptor<>("level",    Types.INT));
            activeDaysState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("activeDays", Types.STRING, Types.LONG));
            lastReqIdState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastReqId", Types.STRING));

            JedisPoolConfig cfg = new JedisPoolConfig();
            cfg.setMaxTotal(20);
            cfg.setMaxIdle(10);
            cfg.setMinIdle(2);
            jedisPool = new JedisPool(cfg, "redis", 6379);
        }

        @Override
        public void close() {
            if (jedisPool != null) jedisPool.close();
        }

        @Override
        public void processElement(BehaviorWithDim e, Context ctx, Collector<UserFeature> out)
                throws Exception {

            // Update counters based on new schema:
            // bhv_type=show (all channels)     -> pv, counted once per req_id (page-level)
            //                                     show events are expanded per-item upstream;
            //                                     deduplicate by req_id to avoid counting N
            //                                     item records as N page views.
            // bhv_type=click + bhv_value=null -> click (pure click)
            // bhv_type=click + bhv_value=cart -> cart
            // bhv_type=click + bhv_value=fav  -> fav
            // bhv_type=click + bhv_value=buy  -> buy
            if ("show".equals(e.bhvType)) {
                String reqId = e.reqId;
                if (reqId != null && !reqId.equals(lastReqIdState.value())) {
                    pvState.update(getOrDefault(pvState) + 1);
                    lastReqIdState.update(reqId);
                }
            } else if ("click".equals(e.bhvType)) {
                if (e.bhvValue == null) {
                    clickState.update(getOrDefault(clickState) + 1);
                } else {
                    switch (e.bhvValue) {
                        case "cart": cartState.update(getOrDefault(cartState) + 1); break;
                        case "fav":  favState.update(getOrDefault(favState) + 1);   break;
                        case "buy":  buyState.update(getOrDefault(buyState) + 1);   break;
                    }
                }
            }

            // Update last active timestamp
            lastTsState.update(e.ts);

            // Track active days (store YYYY-MM-DD as key, value=1)
            // Skip state write if this day is already recorded (avoids redundant backend writes)
            String day = LocalDate.ofInstant(
                    java.time.Instant.ofEpochMilli(e.ts), ZoneId.of("Asia/Shanghai"))
                    .toString();
            if (!activeDaysState.contains(day)) {
                activeDaysState.put(day, 1L);
            }

            // Latch user dimension fields on first non-zero event
            if (ageState.value() == null || ageState.value() == 0) {
                ageState.update(e.userAge);
                cityState.update(e.userCity);
                levelState.update(e.userLevel);
            }

            // Emit updated user feature
            UserFeature feature = new UserFeature();
            feature.userId       = e.uid;
            feature.userAge      = getIntOrDefault(ageState);
            feature.userCity     = cityState.value() != null ? cityState.value() : "unknown";
            feature.userLevel    = getIntOrDefault(levelState);
            feature.totalPv      = getOrDefault(pvState);
            feature.totalClick   = getOrDefault(clickState);
            feature.totalCart    = getOrDefault(cartState);
            feature.totalFav     = getOrDefault(favState);
            feature.totalBuy     = getOrDefault(buyState);
            feature.lastActiveTs = getOrDefault(lastTsState);
            feature.activeDays   = countActiveDays();
            feature.updateTs     = System.currentTimeMillis();

            out.collect(feature);

            // ── Write to Redis feat:user:{uid} ────────────────────────────────
            // Pipeline batches all hset calls into a single RTT.
            // Redis failure is non-fatal: warn only, Kafka sink is unaffected.
            try (Jedis jedis = jedisPool.getResource()) {
                Pipeline pipe = jedis.pipelined();
                String key = "feat:user:" + feature.userId;
                pipe.hset(key, "total_pv",        String.valueOf(feature.totalPv));
                pipe.hset(key, "total_click",      String.valueOf(feature.totalClick));
                pipe.hset(key, "total_cart",       String.valueOf(feature.totalCart));
                pipe.hset(key, "total_fav",        String.valueOf(feature.totalFav));
                pipe.hset(key, "total_buy",        String.valueOf(feature.totalBuy));
                pipe.hset(key, "active_days",      String.valueOf(feature.activeDays));
                pipe.hset(key, "last_active_ts",   String.valueOf(feature.lastActiveTs));
                pipe.hset(key, "user_age",         String.valueOf(feature.userAge));
                pipe.hset(key, "user_city",        feature.userCity);
                pipe.hset(key, "user_level",       String.valueOf(feature.userLevel));
                pipe.hset(key, "update_ts",        String.valueOf(feature.updateTs));
                pipe.expire(key, USER_FEATURE_TTL);
                pipe.sync();
            } catch (Exception ex) {
                org.slf4j.LoggerFactory.getLogger(UserFeatureFunction.class)
                        .warn("[Job3-UserFeature] Redis write failed uid={}: {}", e.uid, ex.getMessage());
            }
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
     *   - totalDetailClick / totalBuy / totalCart / totalFav  (ValueState<Long>)
     *   - uvSet: userId -> 1L                                 (MapState<String, Long>) approximate UV
     *   - itemDim: brand/price/category                       (ValueState)
     *
     * Semantic note: detailClickState counts bhv_type=click + bhv_value=null events only
     * (user entering the detail page). Show/exposure events are not counted here.
     * conversionRate = totalBuy / totalDetailClick (click-to-purchase rate).
     *
     * Show-event safety: show events are filtered out at line 313 (bhv_type != "click" -> return),
     * so they never reach the brand/price/category latch block. Job1 does not join item dimension
     * for show events (itemBrand=null, itemPrice=0.0), which means a show event arriving before
     * the first click would write null into brandState if it were processed — but since show events
     * are skipped entirely, the latch is only triggered by click events that carry full item dim data.
     *
     * Redis write (feat:item:{item_id} Hash, TTL 7d):
     *   Only written on click events (show events are already filtered out above).
     *   Redis failure is non-fatal: warn only, Kafka sink is unaffected.
     */
    static class ItemFeatureFunction extends KeyedProcessFunction<String, BehaviorWithDim, ItemFeature> {

        private static final int ITEM_FEATURE_TTL = 604_800; // 7 days

        private transient ValueState<Long>       detailClickState;
        private transient ValueState<Long>       buyState;
        private transient ValueState<Long>       cartState;
        private transient ValueState<Long>       favState;
        private transient ValueState<String>     brandState;
        private transient ValueState<Double>     priceState;
        private transient ValueState<Integer>    categoryState;
        private transient MapState<String, Long> uvState;

        private transient JedisPool jedisPool;

        @Override
        public void open(Configuration parameters) {
            detailClickState = getRuntimeContext().getState(new ValueStateDescriptor<>("ipv",   Types.LONG));
            buyState      = getRuntimeContext().getState(new ValueStateDescriptor<>("ibuy",     Types.LONG));
            cartState     = getRuntimeContext().getState(new ValueStateDescriptor<>("icart",    Types.LONG));
            favState      = getRuntimeContext().getState(new ValueStateDescriptor<>("ifav",     Types.LONG));
            brandState    = getRuntimeContext().getState(new ValueStateDescriptor<>("ibrand",   Types.STRING));
            priceState    = getRuntimeContext().getState(new ValueStateDescriptor<>("iprice",   Types.DOUBLE));
            categoryState = getRuntimeContext().getState(new ValueStateDescriptor<>("icat",     Types.INT));
            uvState       = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("iuv", Types.STRING, Types.LONG));

            JedisPoolConfig cfg = new JedisPoolConfig();
            cfg.setMaxTotal(20);
            cfg.setMaxIdle(10);
            cfg.setMinIdle(2);
            jedisPool = new JedisPool(cfg, "redis", 6379);
        }

        @Override
        public void close() {
            if (jedisPool != null) jedisPool.close();
        }

        @Override
        public void processElement(BehaviorWithDim e, Context ctx, Collector<ItemFeature> out)
                throws Exception {

            // show events carry no item-level counter signal — skip entirely
            if (!"click".equals(e.bhvType)) return;

            if (e.bhvValue == null) {
                detailClickState.update(getLong(detailClickState) + 1);
            } else {
                switch (e.bhvValue) {
                    case "buy":  buyState.update(getLong(buyState) + 1);   break;
                    case "cart": cartState.update(getLong(cartState) + 1); break;
                    case "fav":  favState.update(getLong(favState) + 1);   break;
                    default: return;   // unknown bhv_value — nothing to update
                }
            }

            // Track UV (unique visitors) — only on click events, skip if already recorded
            if (!uvState.contains(e.uid)) {
                uvState.put(e.uid, 1L);
            }

            // Latch item dimension
            if (brandState.value() == null || brandState.value().equals("unknown")) {
                brandState.update(e.itemBrand);
                priceState.update(e.itemPrice);
                categoryState.update(e.categoryId);
            }

            long detailClicks = getLong(detailClickState);
            long buy = getLong(buyState);
            long uv  = countUv();

            ItemFeature feature    = new ItemFeature();
            feature.itemId         = e.itemId;
            feature.categoryId     = getIntOrDefault(categoryState, e.categoryId);
            feature.itemBrand      = brandState.value() != null ? brandState.value() : "unknown";
            feature.itemPrice      = priceState.value() != null ? priceState.value() : 0.0;
            feature.totalDetailClick = detailClicks;
            feature.totalBuy       = buy;
            feature.totalCart      = getLong(cartState);
            feature.totalFav       = getLong(favState);
            feature.uv             = uv;
            feature.conversionRate = detailClicks > 0 ? (double) buy / detailClicks : 0.0;
            feature.updateTs       = System.currentTimeMillis();

            out.collect(feature);

            // ── Write to Redis feat:item:{item_id} ────────────────────────────
            // Pipeline batches all hset calls into a single RTT.
            // Redis failure is non-fatal: warn only, Kafka sink is unaffected.
            try (Jedis jedis = jedisPool.getResource()) {
                Pipeline pipe = jedis.pipelined();
                String key = "feat:item:" + feature.itemId;
                pipe.hset(key, "total_detail_click", String.valueOf(feature.totalDetailClick));
                pipe.hset(key, "total_buy",          String.valueOf(feature.totalBuy));
                pipe.hset(key, "total_cart",         String.valueOf(feature.totalCart));
                pipe.hset(key, "total_fav",          String.valueOf(feature.totalFav));
                pipe.hset(key, "uv",                 String.valueOf(feature.uv));
                pipe.hset(key, "conversion_rate",    String.valueOf(feature.conversionRate));
                pipe.hset(key, "category_id",        String.valueOf(feature.categoryId));
                pipe.hset(key, "item_brand",         feature.itemBrand);
                pipe.hset(key, "item_price",         String.valueOf(feature.itemPrice));
                pipe.hset(key, "update_ts",          String.valueOf(feature.updateTs));
                pipe.expire(key, ITEM_FEATURE_TTL);
                pipe.sync();
            } catch (Exception ex) {
                org.slf4j.LoggerFactory.getLogger(ItemFeatureFunction.class)
                        .warn("[Job3-ItemFeature] Redis write failed itemId={}: {}", e.itemId, ex.getMessage());
            }
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

    // ── User Window Stat Function ─────────────────────────────────────────────
    /**
     * 2e-user: User-level sliding window statistics.
     *
     * KeyBy: uid — each Task instance maintains only the current user's buckets.
     * State size: at most 60 (1h) + 60 (cat×1h) entries per user — always bounded.
     *
     * Metric semantics (intentionally different from UserFeature.totalPv):
     *   pv_5min / pv_1h  = item-level impression count (one increment per show-expanded
     *                       item record). This is the standard "impression" metric used
     *                       in recommendation ranking feature stores — it reflects how many
     *                       items were shown to the user, not how many pages were requested.
     *                       Contrast: UserFeature.totalPv counts page-level requests
     *                       (deduplicated by req_id), which is the traffic/session PV metric.
     *   click_5min / click_1h = pure detail-page click count (bhv_type=click, bhv_value=null)
     *
     * Redis output:
     *   feat:user:stat:{uid}  Hash { pv_5min, pv_1h, click_5min, click_1h,
     *                                cat_click_5min:{cat_id}, cat_click_1h:{cat_id} }
     */
    static class UserWindowStatFunction extends KeyedProcessFunction<String, BehaviorWithDim, Void> {

        private static final long serialVersionUID = 1L;
        private static final long MILLIS_5MIN = 5  * 60 * 1000L;
        private static final long MILLIS_1H   = 60 * 60 * 1000L;
        private static final int  STAT_TTL    = 7200;   // 2h Redis TTL

        private transient MapState<Long, Long>   userPvBuckets;
        private transient MapState<Long, Long>   userClickBuckets;
        // key = "bucketTs:catId"
        private transient MapState<String, Long> userCatClickBuckets;

        private transient JedisPool jedisPool;

        @Override
        public void open(Configuration parameters) {
            userPvBuckets       = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("upv",    Types.LONG,   Types.LONG));
            userClickBuckets    = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("uclick", Types.LONG,   Types.LONG));
            userCatClickBuckets = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("ucatclk",Types.STRING, Types.LONG));

            JedisPoolConfig cfg = new JedisPoolConfig();
            cfg.setMaxTotal(20);
            cfg.setMaxIdle(10);
            cfg.setMinIdle(2);
            jedisPool = new JedisPool(cfg, "redis", 6379);
        }

        @Override
        public void close() {
            if (jedisPool != null) jedisPool.close();
        }

        @Override
        public void processElement(BehaviorWithDim e, Context ctx, Collector<Void> out)
                throws Exception {
            if (e.uid == null) return;

            boolean isShow  = "show".equals(e.bhvType);
            boolean isClick = "click".equals(e.bhvType) && e.bhvValue == null;
            if (!isShow && !isClick) return;

            long   now       = e.ts;
            long   bucketTs  = (now / 60_000L) * 60_000L;
            long   cutoff5m  = now - MILLIS_5MIN;
            long   cutoff1h  = now - MILLIS_1H;
            String catStr    = String.valueOf(e.categoryId);

            // ── Update buckets ────────────────────────────────────────────────
            if (isShow) {
                Long prev = userPvBuckets.get(bucketTs);
                userPvBuckets.put(bucketTs, prev == null ? 1L : prev + 1);
            }
            if (isClick) {
                Long prev = userClickBuckets.get(bucketTs);
                userClickBuckets.put(bucketTs, prev == null ? 1L : prev + 1);

                String catKey = bucketTs + ":" + catStr;
                Long prevCat  = userCatClickBuckets.get(catKey);
                userCatClickBuckets.put(catKey, prevCat == null ? 1L : prevCat + 1);
            }

            // ── Evict expired buckets (>1h) ───────────────────────────────────
            List<Long>   expPv    = new ArrayList<>();
            List<Long>   expClick = new ArrayList<>();
            List<String> expCat   = new ArrayList<>();
            for (Long k : userPvBuckets.keys())       if (k < cutoff1h) expPv.add(k);
            for (Long k : userClickBuckets.keys())    if (k < cutoff1h) expClick.add(k);
            for (String k : userCatClickBuckets.keys()) {
                String[] p = k.split(":", 2);
                if (p.length == 2 && Long.parseLong(p[0]) < cutoff1h) expCat.add(k);
            }
            for (Long k : expPv)    userPvBuckets.remove(k);
            for (Long k : expClick) userClickBuckets.remove(k);
            for (String k : expCat) userCatClickBuckets.remove(k);

            // ── Compute sums ──────────────────────────────────────────────────
            long pv5m = 0, pv1h = 0, click5m = 0, click1h = 0;
            for (java.util.Map.Entry<Long, Long> en : userPvBuckets.entries()) {
                long bts = en.getKey();
                if (bts >= cutoff5m) pv5m += en.getValue();
                if (bts >= cutoff1h) pv1h += en.getValue();
            }
            for (java.util.Map.Entry<Long, Long> en : userClickBuckets.entries()) {
                long bts = en.getKey();
                if (bts >= cutoff5m) click5m += en.getValue();
                if (bts >= cutoff1h) click1h  += en.getValue();
            }
            long catClick5m = 0, catClick1h = 0;
            for (java.util.Map.Entry<String, Long> en : userCatClickBuckets.entries()) {
                String[] p = en.getKey().split(":", 2);
                if (p.length == 2 && p[1].equals(catStr)) {
                    long bts = Long.parseLong(p[0]);
                    if (bts >= cutoff5m) catClick5m += en.getValue();
                    if (bts >= cutoff1h) catClick1h  += en.getValue();
                }
            }

            // ── Write to Redis ────────────────────────────────────────────────
            try (Jedis jedis = jedisPool.getResource()) {
                Pipeline pipe = jedis.pipelined();
                String key = "feat:user:stat:" + e.uid;
                pipe.hset(key, "pv_5min",                  String.valueOf(pv5m));
                pipe.hset(key, "pv_1h",                    String.valueOf(pv1h));
                pipe.hset(key, "click_5min",               String.valueOf(click5m));
                pipe.hset(key, "click_1h",                 String.valueOf(click1h));
                pipe.hset(key, "cat_click_5min:" + catStr, String.valueOf(catClick5m));
                pipe.hset(key, "cat_click_1h:" + catStr,   String.valueOf(catClick1h));
                pipe.expire(key, STAT_TTL);
                pipe.sync();
            } catch (Exception ex) {
                org.slf4j.LoggerFactory.getLogger(UserWindowStatFunction.class)
                        .warn("[Job3-UserWindowStat] Redis write failed uid={}: {}", e.uid, ex.getMessage());
            }
        }
    }

    // ── Item Window Stat Function ─────────────────────────────────────────────
    /**
     * 2e-item: Item-level sliding window statistics.
     *
     * KeyBy: itemId — all users' events for the same item arrive at the same Task instance,
     * so the aggregation is globally correct across all users.
     * State size: at most 60 (1h) entries per item — always bounded.
     *
     * Redis output:
     *   feat:item:stat:{item_id}  Hash { pv_5min, pv_1h, click_5min, click_1h }
     */
    static class ItemWindowStatFunction extends KeyedProcessFunction<String, BehaviorWithDim, Void> {

        private static final long serialVersionUID = 1L;
        private static final long MILLIS_5MIN = 5  * 60 * 1000L;
        private static final long MILLIS_1H   = 60 * 60 * 1000L;
        private static final int  STAT_TTL    = 7200;

        // key = bucketTs (minute-floor timestamp)
        private transient MapState<Long, Long> pvBuckets;
        private transient MapState<Long, Long> clickBuckets;

        private transient JedisPool jedisPool;

        @Override
        public void open(Configuration parameters) {
            pvBuckets    = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("ipv2",    Types.LONG, Types.LONG));
            clickBuckets = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("iclick2", Types.LONG, Types.LONG));

            JedisPoolConfig cfg = new JedisPoolConfig();
            cfg.setMaxTotal(20);
            cfg.setMaxIdle(10);
            cfg.setMinIdle(2);
            jedisPool = new JedisPool(cfg, "redis", 6379);
        }

        @Override
        public void close() {
            if (jedisPool != null) jedisPool.close();
        }

        @Override
        public void processElement(BehaviorWithDim e, Context ctx, Collector<Void> out)
                throws Exception {
            if (e.itemId == null) return;

            boolean isShow  = "show".equals(e.bhvType);
            boolean isClick = "click".equals(e.bhvType) && e.bhvValue == null;
            if (!isShow && !isClick) return;

            long now      = e.ts;
            long bucketTs = (now / 60_000L) * 60_000L;
            long cutoff5m = now - MILLIS_5MIN;
            long cutoff1h = now - MILLIS_1H;

            // ── Update buckets ────────────────────────────────────────────────
            if (isShow) {
                Long prev = pvBuckets.get(bucketTs);
                pvBuckets.put(bucketTs, prev == null ? 1L : prev + 1);
            }
            if (isClick) {
                Long prev = clickBuckets.get(bucketTs);
                clickBuckets.put(bucketTs, prev == null ? 1L : prev + 1);
            }

            // ── Evict expired buckets (>1h) ───────────────────────────────────
            List<Long> expPv    = new ArrayList<>();
            List<Long> expClick = new ArrayList<>();
            for (Long k : pvBuckets.keys())    if (k < cutoff1h) expPv.add(k);
            for (Long k : clickBuckets.keys()) if (k < cutoff1h) expClick.add(k);
            for (Long k : expPv)    pvBuckets.remove(k);
            for (Long k : expClick) clickBuckets.remove(k);

            // ── Compute sums ──────────────────────────────────────────────────
            long pv5m = 0, pv1h = 0, click5m = 0, click1h = 0;
            for (java.util.Map.Entry<Long, Long> en : pvBuckets.entries()) {
                long bts = en.getKey();
                if (bts >= cutoff5m) pv5m += en.getValue();
                if (bts >= cutoff1h) pv1h  += en.getValue();
            }
            for (java.util.Map.Entry<Long, Long> en : clickBuckets.entries()) {
                long bts = en.getKey();
                if (bts >= cutoff5m) click5m += en.getValue();
                if (bts >= cutoff1h) click1h  += en.getValue();
            }

            // ── Write to Redis ────────────────────────────────────────────────
            try (Jedis jedis = jedisPool.getResource()) {
                Pipeline pipe = jedis.pipelined();
                String key = "feat:item:stat:" + e.itemId;
                pipe.hset(key, "pv_5min",    String.valueOf(pv5m));
                pipe.hset(key, "pv_1h",      String.valueOf(pv1h));
                pipe.hset(key, "click_5min", String.valueOf(click5m));
                pipe.hset(key, "click_1h",   String.valueOf(click1h));
                pipe.expire(key, STAT_TTL);
                pipe.sync();
            } catch (Exception ex) {
                org.slf4j.LoggerFactory.getLogger(ItemWindowStatFunction.class)
                        .warn("[Job3-ItemWindowStat] Redis write failed itemId={}: {}", e.itemId, ex.getMessage());
            }
        }
    }

    // ── Cross Feature Function ────────────────────────────────────────────────
    /**
     * 2f: User × Category and User × Brand cross-feature with exponential decay.
     *
     * KeyBy: uid — one instance per user.
     *
     * Decay model:
     *   Each counter uses Exponential Moving Count (EMC) with a 7-day half-life:
     *     new_count = old_count * exp(-ln2 * Δt / T_half) + 1
     *   where Δt is the elapsed time since the last event for this (uid, catId/brand).
     *
     *   This ensures:
     *   - Counters naturally decay toward zero for inactive (uid, entity) pairs
     *   - Recent interactions carry exponentially more weight than historical ones
     *   - Interest drift is reflected in real time without explicit windowing
     *
     * State design: each counter field is a separate MapState<String, Double> keyed by catId/brand.
     *   An additional MapState<String, Long> stores the last event timestamp per (uid, entity),
     *   used to compute Δt for the decay factor.
     *   Double serialization uses Flink's native DoubleSerializer (no Kryo).
     *
     * Redis key design — no namespace prefix needed:
     *   catId  is String.valueOf(int), producing pure-digit strings "1"~"50".
     *   brand  is always an alphabetic string (Apple/Nike/H&M/etc.), never a pure-digit string.
     *   The two value spaces are disjoint at the data-production level (init_dim_data.py),
     *   so cross:{uid}:{catId} and cross:{uid}:{brand} can never collide.
     *   If the dimension data source ever changes to include numeric brand codes,
     *   add explicit prefixes: "cat:{catId}" / "brand:{brand}".
     *
     * Redis output (TTL 30d):
     *   cross:{uid}:{category_id} Hash { pv_cnt, click_cnt, cart_cnt, fav_cnt, buy_cnt, last_ts }
     *   cross:{uid}:{brand}       Hash { click_cnt, buy_cnt, last_ts }
     *   All numeric fields are decayed floating-point values formatted to 4 decimal places.
     */
    static class CrossFeatureFunction extends KeyedProcessFunction<String, BehaviorWithDim, Void> {

        private static final long   serialVersionUID = 1L;
        private static final int    CROSS_TTL        = 2_592_000;        // 30 days
        private static final double HALF_LIFE_MS     = 7.0 * 86_400_000; // 7-day half-life in ms
        private static final double LN2              = Math.log(2.0);

        // Category cross counters — key = catId (String), value = decayed count (Double)
        private transient MapState<String, Double> catPvState;
        private transient MapState<String, Double> catClickState;
        private transient MapState<String, Double> catCartState;
        private transient MapState<String, Double> catFavState;
        private transient MapState<String, Double> catBuyState;
        // Last event timestamp per catId — used to compute decay interval
        private transient MapState<String, Long>   catLastTsState;

        // Brand cross counters — key = brand (String), value = decayed count (Double)
        private transient MapState<String, Double> brandClickState;
        private transient MapState<String, Double> brandBuyState;
        // Last event timestamp per brand
        private transient MapState<String, Long>   brandLastTsState;

        private transient JedisPool jedisPool;

        @Override
        public void open(Configuration parameters) {
            catPvState    = getRuntimeContext().getMapState(new MapStateDescriptor<>("xcat_pv",    Types.STRING, Types.DOUBLE));
            catClickState = getRuntimeContext().getMapState(new MapStateDescriptor<>("xcat_click", Types.STRING, Types.DOUBLE));
            catCartState  = getRuntimeContext().getMapState(new MapStateDescriptor<>("xcat_cart",  Types.STRING, Types.DOUBLE));
            catFavState   = getRuntimeContext().getMapState(new MapStateDescriptor<>("xcat_fav",   Types.STRING, Types.DOUBLE));
            catBuyState   = getRuntimeContext().getMapState(new MapStateDescriptor<>("xcat_buy",   Types.STRING, Types.DOUBLE));
            catLastTsState = getRuntimeContext().getMapState(new MapStateDescriptor<>("xcat_last_ts", Types.STRING, Types.LONG));

            brandClickState = getRuntimeContext().getMapState(new MapStateDescriptor<>("xbrand_click", Types.STRING, Types.DOUBLE));
            brandBuyState   = getRuntimeContext().getMapState(new MapStateDescriptor<>("xbrand_buy",   Types.STRING, Types.DOUBLE));
            brandLastTsState = getRuntimeContext().getMapState(new MapStateDescriptor<>("xbrand_last_ts", Types.STRING, Types.LONG));

            JedisPoolConfig cfg = new JedisPoolConfig();
            cfg.setMaxTotal(20);
            cfg.setMaxIdle(10);
            cfg.setMinIdle(2);
            jedisPool = new JedisPool(cfg, "redis", 6379);
        }

        @Override
        public void close() {
            if (jedisPool != null) jedisPool.close();
        }

        @Override
        public void processElement(BehaviorWithDim e, Context ctx, Collector<Void> out)
                throws Exception {
            if (e.uid == null) return;

            String catId = String.valueOf(e.categoryId);
            String brand = e.itemBrand != null ? e.itemBrand : "unknown";
            long   ts    = e.ts;

            boolean catChanged   = false;
            boolean brandChanged = false;

            // ── Update category cross-counters (with decay) ───────────────────
            if ("show".equals(e.bhvType)) {
                catPvState.put(catId, decayed(catPvState, catLastTsState, catId, ts) + 1.0);
                catLastTsState.put(catId, ts);
                catChanged = true;
            } else if ("click".equals(e.bhvType)) {
                if (e.bhvValue == null) {
                    catClickState.put(catId, decayed(catClickState, catLastTsState, catId, ts) + 1.0);
                    catLastTsState.put(catId, ts);
                    catChanged = true;
                } else {
                    switch (e.bhvValue) {
                        case "cart":
                            catCartState.put(catId, decayed(catCartState, catLastTsState, catId, ts) + 1.0);
                            catLastTsState.put(catId, ts);
                            catChanged = true;
                            break;
                        case "fav":
                            catFavState.put(catId, decayed(catFavState, catLastTsState, catId, ts) + 1.0);
                            catLastTsState.put(catId, ts);
                            catChanged = true;
                            break;
                        case "buy":
                            catBuyState.put(catId, decayed(catBuyState, catLastTsState, catId, ts) + 1.0);
                            catLastTsState.put(catId, ts);
                            catChanged = true;
                            break;
                    }
                }
            }

            // ── Update brand cross-counters with decay (show events excluded) ─
            if ("click".equals(e.bhvType)) {
                if (e.bhvValue == null) {
                    brandClickState.put(brand, decayed(brandClickState, brandLastTsState, brand, ts) + 1.0);
                    brandLastTsState.put(brand, ts);
                    brandChanged = true;
                } else if ("buy".equals(e.bhvValue)) {
                    brandBuyState.put(brand, decayed(brandBuyState, brandLastTsState, brand, ts) + 1.0);
                    brandLastTsState.put(brand, ts);
                    brandChanged = true;
                }
            }

            if (!catChanged && !brandChanged) return;

            // ── Write to Redis ────────────────────────────────────────────────
            try (Jedis jedis = jedisPool.getResource()) {
                Pipeline pipe = jedis.pipelined();

                if (catChanged) {
                    String catKey = "cross:" + e.uid + ":" + catId;
                    pipe.hset(catKey, "pv_cnt",    fmt(catPvState,    catId));
                    pipe.hset(catKey, "click_cnt", fmt(catClickState, catId));
                    pipe.hset(catKey, "cart_cnt",  fmt(catCartState,  catId));
                    pipe.hset(catKey, "fav_cnt",   fmt(catFavState,   catId));
                    pipe.hset(catKey, "buy_cnt",   fmt(catBuyState,   catId));
                    pipe.hset(catKey, "last_ts",   String.valueOf(ts));
                    pipe.expire(catKey, CROSS_TTL);
                }

                if (brandChanged) {
                    String brandKey = "cross:" + e.uid + ":" + brand;
                    pipe.hset(brandKey, "click_cnt", fmt(brandClickState, brand));
                    pipe.hset(brandKey, "buy_cnt",   fmt(brandBuyState,   brand));
                    pipe.hset(brandKey, "last_ts",   String.valueOf(ts));
                    pipe.expire(brandKey, CROSS_TTL);
                }

                pipe.sync();
            } catch (Exception ex) {
                org.slf4j.LoggerFactory.getLogger(CrossFeatureFunction.class)
                        .warn("[Job3-CrossFeature] Redis write failed uid={}: {}", e.uid, ex.getMessage());
            }
        }

        /**
         * Apply exponential decay to the stored counter for the given key, based on elapsed
         * time since the last event. Returns the decayed value (before adding the new increment).
         *
         * Formula: decayed = stored * exp(-ln2 * Δt / T_half)
         * where Δt = currentTs - lastTs, T_half = HALF_LIFE_MS (7 days).
         *
         * If no prior value exists (first event), returns 0.0.
         */
        private double decayed(MapState<String, Double> countState,
                               MapState<String, Long>   lastTsState,
                               String key, long currentTs) throws Exception {
            Double stored = countState.get(key);
            if (stored == null || stored <= 0) return 0.0;
            Long lastTs = lastTsState.get(key);
            if (lastTs == null) return 0.0;
            double deltaMs = Math.max(0, currentTs - lastTs);
            return stored * Math.exp(-LN2 * deltaMs / HALF_LIFE_MS);
        }

        /** Format a decayed Double counter to 4 decimal places for Redis storage. */
        private String fmt(MapState<String, Double> state, String key) throws Exception {
            Double v = state.get(key);
            return v == null ? "0.0000" : String.format("%.4f", v);
        }
    }
}

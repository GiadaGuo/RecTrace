package com.demo.job1;

import com.demo.common.config.KafkaConfig;
import com.demo.common.model.BehaviorWithDim;
import com.demo.common.model.BhvExt;
import com.demo.common.model.UserBehavior;
import com.demo.common.serde.JsonSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Job1: Dimension Join via AsyncIO + Redis
 * -----------------------------------------
 * Source : ods_user_behavior
 * Process:
 * show events  (home/search): expand bhv_ext.items into one record per item,
 *                joining user dimension only. Item dimension (brand/price) is
 *                intentionally skipped — show events do not need it for any
 *                downstream feature or sample computation.
 *                After expansion, bhv_ext.items is cleared to avoid redundant
 *                payload; item data is in top-level item_id / category_id.
 *                req_id is set on the top-level reqId field (not via bhv_ext).
 *   click events (home/search/pdp): single item_id from bhv_ext.itemId,
 *                joining both user dimension and item dimension.
 * Sink   : dwd_behavior_with_dim
 *
 * Show event expansion:
 *   1 show event with N items  →  N records in DWD, each with distinct itemId
 *   itemBrand = null, itemPrice = 0.0  (not joined for show events)
 *
 * Why skip item dim join for show events:
 *   - Sample flow (Phase 2 Stage 1) uses bhv_page/req_id/uid/ts, not item dim
 *   - User totalPv counter needs no item dim
 *   - WindowStatFunction user/item pv buckets need itemId (from items list) but not brand/price
 *   - Avoids N×hgetAll calls per show event (one per item), replaced by zero item lookups
 *
 * Why search click events still join item dim:
 *   - search click (bhv_page=search, bhv_type=click) represents user tapping a
 *     search result and entering PDP; it contributes to ItemFeatureFunction
 *     (detailClick, UV) and CrossFeatureFunction (brand cross-feature), both of
 *     which require itemBrand and categoryId
 *
 * Submit:
 *   flink run -c com.demo.job1.DimJoinJob flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar
 */
public class DimJoinJob {

    private static final Logger LOG = LoggerFactory.getLogger(DimJoinJob.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30_000);
        env.setParallelism(1);

        // ── Source ────────────────────────────────────────────────────────────
        KafkaSource<UserBehavior> source = KafkaSource.<UserBehavior>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .setTopics(KafkaConfig.TOPIC_ODS_USER_BEHAVIOR)
                .setGroupId(KafkaConfig.GROUP_JOB1)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(org.apache.kafka.clients.consumer.OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new JsonSchema.Deserializer<>(UserBehavior.class))
                .build();

        DataStream<UserBehavior> behaviorStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "KafkaSource-ods_user_behavior"
        );

        // ── Async dimension join ──────────────────────────────────────────────
        DataStream<BehaviorWithDim> enrichedStream = AsyncDataStream.unorderedWait(
                behaviorStream,
                new RedisDimAsyncFunction(),
                5000,  // timeout ms
                java.util.concurrent.TimeUnit.MILLISECONDS,
                100    // max concurrent async requests
        );

        // ── Sink ──────────────────────────────────────────────────────────────
        KafkaSink<BehaviorWithDim> sink = KafkaSink.<BehaviorWithDim>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<BehaviorWithDim>builder()
                                .setTopic(KafkaConfig.TOPIC_DWD_BEHAVIOR_WITH_DIM)
                                .setKeySerializationSchema(r -> r.uid != null ? r.uid.getBytes() : new byte[0])
                                .setValueSerializationSchema(new JsonSchema.Serializer<>(BehaviorWithDim.class))
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        enrichedStream.sinkTo(sink).name("KafkaSink-dwd_behavior_with_dim");

        env.execute("Job1-DimJoin");
    }

    // ── Async Function ────────────────────────────────────────────────────────
    /**
     * Asynchronously fetches user and item dimension data from Redis.
     *
     * show events  : expand bhv_ext.items into N records, join user dim once,
     *                leave item dim fields at defaults (null / 0).
     * click events : resolve single item_id from bhv_ext.itemId, join both
     *                user dim and item dim.
     *
     * JedisPool is created once per task (in open()) and closed in close().
     */
    static class RedisDimAsyncFunction extends RichAsyncFunction<UserBehavior, BehaviorWithDim> {

        private static final long serialVersionUID = 1L;

        private transient JedisPool jedisPool;
        private transient ExecutorService executor;

        @Override
        public void open(Configuration parameters) {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(20);
            poolConfig.setMaxIdle(10);
            poolConfig.setMinIdle(2);
            jedisPool = new JedisPool(poolConfig, "redis", 6379);
            executor  = Executors.newFixedThreadPool(10);
            LOG.info("[Job1] RedisDimAsyncFunction opened, JedisPool created");
        }

        @Override
        public void asyncInvoke(UserBehavior input, ResultFuture<BehaviorWithDim> resultFuture) {
            CompletableFuture.supplyAsync(() -> {
                try (Jedis jedis = jedisPool.getResource()) {
                    // ── Fetch user dimension once (shared across all output records) ──
                    UserDim userDim = fetchUserDim(jedis, input.uid);

                    if ("show".equals(input.bhvType)) {
                        // ── show event: expand items list, skip item dim join ──────────
                        return buildShowRecords(input, userDim);
                    } else {
                        // ── click/pdp event: single record, join item dim ─────────────
                        BehaviorWithDim result = buildBase(input);
                        applyUserDim(result, userDim);

                        String lookupItemId = resolveClickItemId(input);
                        result.itemId = lookupItemId;
                        if (lookupItemId != null) {
                            result.categoryId = resolveCategoryId(lookupItemId);
                            fetchAndApplyItemDim(result, jedis, lookupItemId);
                        }
                        return Collections.singletonList(result);
                    }
                }
            }, executor).whenComplete((results, ex) -> {
                if (ex != null) {
                    LOG.warn("[Job1] Redis lookup failed for uid={}: {}", input.uid, ex.getMessage());
                    resultFuture.complete(buildFallback(input));
                } else {
                    resultFuture.complete(results);
                }
            });
        }

        @Override
        public void timeout(UserBehavior input, ResultFuture<BehaviorWithDim> resultFuture) {
            LOG.warn("[Job1] Async timeout for uid={}", input.uid);
            resultFuture.complete(buildFallback(input));
        }

        @Override
        public void close() {
            if (executor != null)  executor.shutdown();
            if (jedisPool != null) jedisPool.close();
        }

        // ── Helpers ───────────────────────────────────────────────────────────

        /**
         * Expand a show event into one record per item in bhv_ext.items.
         * User dim is applied to all records; item dim fields remain at defaults.
         * categoryId is derived from item_id without a Redis lookup.
         *
         * After expansion, bhv_ext.items is cleared on each output record.
         * The items list has been promoted to individual top-level item_id fields;
         * keeping the full array in every record would multiply Kafka message size by N.
         * bhv_ext in each output record retains only the fields relevant to click
         * events (itemId, position, query) so downstream Jobs remain consistent.
         * req_id is already a top-level field and is set via buildBase().
         */
        private static List<BehaviorWithDim> buildShowRecords(UserBehavior input, UserDim userDim) {
            BhvExt ext = input.bhvExt;
            List<BhvExt.ExposureItem> items = ext != null ? ext.items : null;

            if (items == null || items.isEmpty()) {
                // Defensive: show event with no items list — emit one record with no itemId
                BehaviorWithDim r = buildBase(input);
                applyUserDim(r, userDim);
                r.bhvExt = buildSlimBhvExt(ext);
                return Collections.singletonList(r);
            }

            List<BehaviorWithDim> results = new ArrayList<>(items.size());
            BhvExt slimExt = buildSlimBhvExt(ext);  // shared across all expanded records
            for (BhvExt.ExposureItem item : items) {
                BehaviorWithDim r = buildBase(input);
                applyUserDim(r, userDim);
                r.itemId     = item.itemId;
                r.categoryId = resolveCategoryId(item.itemId);
                r.bhvExt     = slimExt;
                // itemBrand stays null, itemPrice stays 0.0 — not joined for show events
                results.add(r);
            }
            return results;
        }

        /**
         * Build a slim BhvExt with items list cleared.
         * Used for show-event expanded records — items have been promoted to top-level
         * item_id / category_id; carrying the full array in every record wastes bandwidth.
         * req_id is a top-level field; other click-only fields (itemId, position, query)
         * are null for show events anyway, so the resulting object is effectively empty
         * but kept for structural consistency.
         */
        private static BhvExt buildSlimBhvExt(BhvExt original) {
            BhvExt slim = new BhvExt();
            // items intentionally not copied — already expanded to top-level fields
            if (original != null) {
                slim.query    = original.query;    // null for home, kept for search show
                slim.position = original.position; // null for show events
            }
            return slim;
        }

        /** Resolve item_id from bhv_ext for click/pdp events (single item only). */
        private static String resolveClickItemId(UserBehavior input) {
            BhvExt ext = input.bhvExt;
            if (ext == null) return null;
            if (ext.itemId != null && !ext.itemId.isEmpty()) return ext.itemId;
            return null;
        }

        /** Derive category_id from item_id format I{7-digit}. */
        private static int resolveCategoryId(String itemId) {
            if (itemId == null || itemId.length() < 2) return 0;
            try {
                int idx = Integer.parseInt(itemId.substring(1)) - 1;
                return (idx % 50) + 1;
            } catch (NumberFormatException e) {
                return 0;
            }
        }

        /** Fetch user dimension from Redis. Returns defaults if key not found. */
        private static UserDim fetchUserDim(Jedis jedis, String uid) {
            UserDim dim = new UserDim();
            Map<String, String> userDimMap = jedis.hgetAll("dim:user:" + uid);
            if (userDimMap != null && !userDimMap.isEmpty()) {
                dim.age   = parseIntSafe(userDimMap.get("age"), 0);
                dim.city  = userDimMap.getOrDefault("city", "unknown");
                dim.level = parseIntSafe(userDimMap.get("level"), 1);
            }
            return dim;
        }

        /** Apply pre-fetched user dim to a record. */
        private static void applyUserDim(BehaviorWithDim r, UserDim dim) {
            r.userAge   = dim.age;
            r.userCity  = dim.city;
            r.userLevel = dim.level;
        }

        /** Fetch item dimension from Redis and apply to record in place. */
        private static void fetchAndApplyItemDim(BehaviorWithDim r, Jedis jedis, String itemId) {
            Map<String, String> itemDimMap = jedis.hgetAll("dim:item:" + itemId);
            if (itemDimMap != null && !itemDimMap.isEmpty()) {
                r.itemBrand = itemDimMap.getOrDefault("brand", "unknown");
                r.itemPrice = parseDoubleSafe(itemDimMap.get("price"), 0.0);
            } else {
                r.itemBrand = "unknown";
                r.itemPrice = 0.0;
            }
        }

        /** Build the base BehaviorWithDim with all pass-through fields set. */
        private static BehaviorWithDim buildBase(UserBehavior input) {
            BehaviorWithDim r = new BehaviorWithDim();
            r.uid      = input.uid;
            r.reqId    = input.reqId;
            r.bhvId    = input.bhvId;
            r.bhvPage  = input.bhvPage;
            r.bhvSrc   = input.bhvSrc;
            r.bhvType  = input.bhvType;
            r.bhvValue = input.bhvValue;
            r.ts       = input.ts;
            r.bhvExt   = input.bhvExt;
            return r;
        }

        /**
         * Build fallback records when Redis lookup fails or times out.
         * show events are still expanded (item dim defaults); click events emit one record.
         */
        private static List<BehaviorWithDim> buildFallback(UserBehavior input) {
            UserDim defaultDim = new UserDim(); // all defaults

            if ("show".equals(input.bhvType)) {
                return buildShowRecords(input, defaultDim);
            }

            BehaviorWithDim f = buildBase(input);
            applyUserDim(f, defaultDim);
            f.itemId     = resolveClickItemId(input);
            f.categoryId = f.itemId != null ? resolveCategoryId(f.itemId) : 0;
            f.itemBrand  = "unknown";
            f.itemPrice  = 0.0;
            return Collections.singletonList(f);
        }

        private static int parseIntSafe(String s, int defaultVal) {
            if (s == null) return defaultVal;
            try { return Integer.parseInt(s); } catch (NumberFormatException e) { return defaultVal; }
        }

        private static double parseDoubleSafe(String s, double defaultVal) {
            if (s == null) return defaultVal;
            try { return Double.parseDouble(s); } catch (NumberFormatException e) { return defaultVal; }
        }

        // ── Value object for user dimension ───────────────────────────────────
        private static class UserDim {
            int    age   = 0;
            String city  = "unknown";
            int    level = 1;
        }
    }
}

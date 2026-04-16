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
 * Process: For each event, asynchronously look up user + item dimension from Redis.
 *          For show events the primary item_id is taken from bhv_ext.items[0] if present,
 *          otherwise from bhv_ext.itemId. Click / pdp events use bhv_ext.itemId directly.
 * Sink   : dwd_behavior_with_dim
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
     * Item dimension is resolved from bhv_ext: for click/pdp events uses itemId directly;
     * for show events uses the first item in the items list (representative lookup).
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
                BehaviorWithDim result = buildBase(input);

                // Resolve the representative item_id for dimension lookup
                String lookupItemId = resolveItemId(input);
                result.itemId = lookupItemId;
                if (lookupItemId != null) {
                    result.categoryId = resolveCategoryId(lookupItemId);
                }

                try (Jedis jedis = jedisPool.getResource()) {
                    // Fetch user dimension
                    Map<String, String> userDim = jedis.hgetAll("dim:user:" + input.uid);
                    if (userDim != null && !userDim.isEmpty()) {
                        result.userAge   = parseIntSafe(userDim.get("age"), 0);
                        result.userCity  = userDim.getOrDefault("city", "unknown");
                        result.userLevel = parseIntSafe(userDim.get("level"), 1);
                    } else {
                        result.userAge   = 0;
                        result.userCity  = "unknown";
                        result.userLevel = 1;
                    }

                    // Fetch item dimension
                    if (lookupItemId != null) {
                        Map<String, String> itemDim = jedis.hgetAll("dim:item:" + lookupItemId);
                        if (itemDim != null && !itemDim.isEmpty()) {
                            result.itemBrand = itemDim.getOrDefault("brand", "unknown");
                            result.itemPrice = parseDoubleSafe(itemDim.get("price"), 0.0);
                        } else {
                            result.itemBrand = "unknown";
                            result.itemPrice = 0.0;
                        }
                    }
                }
                return result;
            }, executor).whenComplete((res, ex) -> {
                if (ex != null) {
                    LOG.warn("[Job1] Redis lookup failed for uid={}: {}", input.uid, ex.getMessage());
                    resultFuture.complete(Collections.singleton(buildFallback(input)));
                } else {
                    resultFuture.complete(Collections.singleton(res));
                }
            });
        }

        @Override
        public void timeout(UserBehavior input, ResultFuture<BehaviorWithDim> resultFuture) {
            LOG.warn("[Job1] Async timeout for uid={}", input.uid);
            // Emit fallback rather than dropping the event
            resultFuture.complete(Collections.singleton(buildFallback(input)));
        }

        @Override
        public void close() {
            if (executor != null)  executor.shutdown();
            if (jedisPool != null) jedisPool.close();
        }

        // ── Helpers ───────────────────────────────────────────────────────────

        /** Resolve the item_id to use for dimension lookup from bhv_ext. */
        private static String resolveItemId(UserBehavior input) {
            BhvExt ext = input.bhvExt;
            if (ext == null) return null;
            // click / pdp events carry a single item_id
            if (ext.itemId != null && !ext.itemId.isEmpty()) return ext.itemId;
            // show events carry an items list; use first entry for dim lookup
            List<BhvExt.ExposureItem> items = ext.items;
            if (items != null && !items.isEmpty()) return items.get(0).itemId;
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

        /** Build the base BehaviorWithDim with all pass-through fields set. */
        private static BehaviorWithDim buildBase(UserBehavior input) {
            BehaviorWithDim r = new BehaviorWithDim();
            r.uid       = input.uid;
            r.bhvId     = input.bhvId;
            r.bhvPage   = input.bhvPage;
            r.bhvSrc    = input.bhvSrc;
            r.bhvType   = input.bhvType;
            r.bhvValue  = input.bhvValue;
            r.ts        = input.ts;
            r.bhvExt    = input.bhvExt;
            return r;
        }

        /** Build fallback record with empty dimension data. Event is never dropped. */
        private static BehaviorWithDim buildFallback(UserBehavior input) {
            BehaviorWithDim f = buildBase(input);
            f.itemId     = resolveItemId(input);
            f.categoryId = f.itemId != null ? resolveCategoryId(f.itemId) : 0;
            f.userAge    = 0;
            f.userCity   = "unknown";
            f.userLevel  = 1;
            f.itemBrand  = "unknown";
            f.itemPrice  = 0.0;
            return f;
        }

        private static int parseIntSafe(String s, int defaultVal) {
            if (s == null) return defaultVal;
            try { return Integer.parseInt(s); } catch (NumberFormatException e) { return defaultVal; }
        }

        private static double parseDoubleSafe(String s, double defaultVal) {
            if (s == null) return defaultVal;
            try { return Double.parseDouble(s); } catch (NumberFormatException e) { return defaultVal; }
        }
    }
}

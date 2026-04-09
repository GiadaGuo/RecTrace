package com.demo.job1;

import com.demo.common.config.KafkaConfig;
import com.demo.common.model.BehaviorWithDim;
import com.demo.common.model.UserBehavior;
import com.demo.common.serde.JsonSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
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

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Job1: Dimension Join via AsyncIO + Redis
 * -----------------------------------------
 * Source : ods_user_behavior
 * Process: For each event, asynchronously look up user + item dimension from Redis
 * Sink   : dwd_behavior_with_dim
 *
 * Learning focus: Flink AsyncDataStream, JedisPool management via RichFunction
 *
 * Submit:
 *   flink run -c com.demo.job1.DimJoinJob flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar
 */
public class DimJoinJob {

    private static final Logger LOG = LoggerFactory.getLogger(DimJoinJob.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing every 30s for fault tolerance
        env.enableCheckpointing(30_000);
        env.setParallelism(2);

        // ── Source ────────────────────────────────────────────────────────────
        KafkaSource<UserBehavior> source = KafkaSource.<UserBehavior>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .setTopics(KafkaConfig.TOPIC_ODS_USER_BEHAVIOR)
                .setGroupId(KafkaConfig.GROUP_JOB1)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonSchema.Deserializer<>(UserBehavior.class))
                .build();

        DataStream<UserBehavior> behaviorStream = env.fromSource(
                source,
                WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, ts) -> event.timestamp),
                "KafkaSource-ods_user_behavior"
        );

        // ── Async dimension join ──────────────────────────────────────────────
        DataStream<BehaviorWithDim> enrichedStream = AsyncDataStream.unorderedWait(
                behaviorStream,
                new RedisDimAsyncFunction(),
                5000,   // timeout ms
                java.util.concurrent.TimeUnit.MILLISECONDS,
                100     // max concurrent async requests
        );

        // ── Sink ──────────────────────────────────────────────────────────────
        KafkaSink<BehaviorWithDim> sink = KafkaSink.<BehaviorWithDim>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<BehaviorWithDim>builder()
                                .setTopic(KafkaConfig.TOPIC_DWD_BEHAVIOR_WITH_DIM)
                                .setKeySerializationSchema(r -> r.userId.getBytes())
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
     * Asynchronously fetches user and item dimension data from Redis using a thread pool.
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
                BehaviorWithDim result = new BehaviorWithDim();
                result.userId     = input.userId;
                result.itemId     = input.itemId;
                result.categoryId = input.categoryId;
                result.behavior   = input.behavior;
                result.timestamp  = input.timestamp;

                try (Jedis jedis = jedisPool.getResource()) {
                    // Fetch user dimension
                    Map<String, String> userDim = jedis.hgetAll("dim:user:" + input.userId);
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
                    Map<String, String> itemDim = jedis.hgetAll("dim:item:" + input.itemId);
                    if (itemDim != null && !itemDim.isEmpty()) {
                        result.itemBrand = itemDim.getOrDefault("brand", "unknown");
                        result.itemPrice = parseDoubleSafe(itemDim.get("price"), 0.0);
                    } else {
                        result.itemBrand = "unknown";
                        result.itemPrice = 0.0;
                    }
                }
                return result;
            }, executor).whenComplete((res, ex) -> {
                if (ex != null) {
                    LOG.warn("[Job1] Redis lookup failed for userId={}: {}", input.userId, ex.getMessage());
                    // Emit event without dim data rather than dropping it
                    BehaviorWithDim fallback = new BehaviorWithDim();
                    fallback.userId     = input.userId;
                    fallback.itemId     = input.itemId;
                    fallback.categoryId = input.categoryId;
                    fallback.behavior   = input.behavior;
                    fallback.timestamp  = input.timestamp;
                    fallback.userAge    = 0;
                    fallback.userCity   = "unknown";
                    fallback.userLevel  = 1;
                    fallback.itemBrand  = "unknown";
                    fallback.itemPrice  = 0.0;
                    resultFuture.complete(Collections.singleton(fallback));
                } else {
                    resultFuture.complete(Collections.singleton(res));
                }
            });
        }

        @Override
        public void timeout(UserBehavior input, ResultFuture<BehaviorWithDim> resultFuture) {
            LOG.warn("[Job1] Async timeout for userId={}, itemId={}", input.userId, input.itemId);
            resultFuture.complete(Collections.emptyList());
        }

        @Override
        public void close() {
            if (executor != null)  executor.shutdown();
            if (jedisPool != null) jedisPool.close();
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

package com.demo.common.config;

/**
 * Central configuration constants for Kafka bootstrap address and topic names.
 */
public final class KafkaConfig {

    private KafkaConfig() {}

    public static final String BOOTSTRAP_SERVERS = "kafka:9092";

    // ── Topic names ──────────────────────────────────────────────────────────
    /** Raw behavior event stream (input) */
    public static final String TOPIC_ODS_USER_BEHAVIOR     = "ods_user_behavior";

    /** Behavior enriched with dimension data */
    public static final String TOPIC_DWD_BEHAVIOR_WITH_DIM = "dwd_behavior_with_dim";

    /** Cross feature per user+category (tumbling window) */
    public static final String TOPIC_DWS_USER_ITEM_FEATURE = "dws_user_item_feature";

    /** Cumulative user-level feature */
    public static final String TOPIC_DWS_USER_FEATURE      = "dws_user_feature";

    /** Cumulative item-level feature */
    public static final String TOPIC_DWS_ITEM_FEATURE      = "dws_item_feature";

    /** User behavior sequence feature (Job4 output) */
    public static final String TOPIC_DWS_USER_SEQ_FEATURE  = "dws_user_seq_feature";

    // ── Consumer group IDs ───────────────────────────────────────────────────
    public static final String GROUP_JOB1 = "flink-job1-dim-join";
    public static final String GROUP_JOB2 = "flink-job2-window-agg";
    public static final String GROUP_JOB3 = "flink-job3-realtime-feature";
    public static final String GROUP_JOB4 = "flink-job4-sequence-feature";
}

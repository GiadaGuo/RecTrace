package com.demo.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Raw user behavior event from Kafka topic: ods_user_behavior
 *
 * Behavior funnel for recommendation tracing:
 *   show -> click -> cart/fav -> buy
 */
public class UserBehavior {

    @JsonProperty("user_id")
    public String userId;

    @JsonProperty("item_id")
    public String itemId;

    @JsonProperty("category_id")
    public int categoryId;

    /** show | click | cart | fav | buy */
    @JsonProperty("behavior")
    public String behavior;

    /** Unix timestamp in milliseconds */
    @JsonProperty("timestamp")
    public long timestamp;

    // ── Recommendation tracing fields ────────────────────────────────────────

    /** Session ID: {user_id}_{session_start_ts}, links all behaviors in one visit */
    @JsonProperty("session_id")
    public String sessionId;

    /**
     * Recommendation request ID: req_{8-char-uuid}.
     * One req_id maps to multiple show events (the recommendation list).
     * Used to trace which recommendation results led to clicks/purchases.
     */
    @JsonProperty("req_id")
    public String reqId;

    /** Traffic source: recommend | search | direct | ad */
    @JsonProperty("rec_source")
    public String recSource;

    /**
     * Position of the item in the recommendation list (1-based).
     * Only meaningful for show events; 0 for other behaviors.
     * Used to analyze position bias in CTR.
     */
    @JsonProperty("position")
    public int position;

    public UserBehavior() {}

    @Override
    public String toString() {
        return "UserBehavior{userId='" + userId + "', itemId='" + itemId +
               "', behavior='" + behavior + "', sessionId='" + sessionId +
               "', recSource='" + recSource + "', position=" + position +
               ", timestamp=" + timestamp + "}";
    }
}

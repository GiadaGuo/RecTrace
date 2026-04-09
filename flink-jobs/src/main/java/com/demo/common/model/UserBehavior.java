package com.demo.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Raw user behavior event from Kafka topic: ods_user_behavior
 */
public class UserBehavior {

    @JsonProperty("user_id")
    public String userId;

    @JsonProperty("item_id")
    public String itemId;

    @JsonProperty("category_id")
    public int categoryId;

    /** pv | cart | fav | buy */
    @JsonProperty("behavior")
    public String behavior;

    /** Unix timestamp in milliseconds */
    @JsonProperty("timestamp")
    public long timestamp;

    public UserBehavior() {}

    public UserBehavior(String userId, String itemId, int categoryId, String behavior, long timestamp) {
        this.userId     = userId;
        this.itemId     = itemId;
        this.categoryId = categoryId;
        this.behavior   = behavior;
        this.timestamp  = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{userId='" + userId + "', itemId='" + itemId +
               "', categoryId=" + categoryId + ", behavior='" + behavior +
               "', timestamp=" + timestamp + "}";
    }
}

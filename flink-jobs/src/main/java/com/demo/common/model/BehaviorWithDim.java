package com.demo.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Behavior event enriched with user + item dimension data.
 * Output of Job1 -> Kafka topic: dwd_behavior_with_dim
 */
public class BehaviorWithDim {

    // ── Original fields ──────────────────────────────────────────────────────
    @JsonProperty("user_id")
    public String userId;

    @JsonProperty("item_id")
    public String itemId;

    @JsonProperty("category_id")
    public int categoryId;

    @JsonProperty("behavior")
    public String behavior;

    @JsonProperty("timestamp")
    public long timestamp;

    // ── User dimension fields ────────────────────────────────────────────────
    @JsonProperty("user_age")
    public int userAge;

    @JsonProperty("user_city")
    public String userCity;

    @JsonProperty("user_level")
    public int userLevel;

    // ── Item dimension fields ────────────────────────────────────────────────
    @JsonProperty("item_brand")
    public String itemBrand;

    @JsonProperty("item_price")
    public double itemPrice;

    public BehaviorWithDim() {}

    @Override
    public String toString() {
        return "BehaviorWithDim{userId='" + userId + "', itemId='" + itemId +
               "', behavior='" + behavior + "', userCity='" + userCity +
               "', itemBrand='" + itemBrand + "', itemPrice=" + itemPrice + "}";
    }
}

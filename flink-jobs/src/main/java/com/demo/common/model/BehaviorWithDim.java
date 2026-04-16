package com.demo.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Behavior event enriched with user + item dimension data.
 * Output of Job1 -> Kafka topic: dwd_behavior_with_dim
 *
 * All new fields from UserBehavior (bhvPage, bhvSrc, bhvType, bhvValue, bhvExt)
 * are passed through so downstream Jobs can route and filter by page / behavior type.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BehaviorWithDim {

    // ── Identity ──────────────────────────────────────────────────────────────
    @JsonProperty("uid")
    public String uid;

    // ── Event classification (pass-through) ───────────────────────────────────
    @JsonProperty("bhv_id")
    public String bhvId;

    @JsonProperty("bhv_page")
    public String bhvPage;

    @JsonProperty("bhv_src")
    public String bhvSrc;

    @JsonProperty("bhv_type")
    public String bhvType;

    @JsonProperty("bhv_value")
    public String bhvValue;

    // ── Timing ────────────────────────────────────────────────────────────────
    @JsonProperty("ts")
    public long ts;

    // ── Extension payload (pass-through) ─────────────────────────────────────
    @JsonProperty("bhv_ext")
    public BhvExt bhvExt;

    // ── User dimension fields (from Redis dim:user:{uid}) ─────────────────────
    @JsonProperty("user_age")
    public int userAge;

    @JsonProperty("user_city")
    public String userCity;

    @JsonProperty("user_level")
    public int userLevel;

    // ── Item dimension fields (from Redis dim:item:{item_id}) ─────────────────
    // item_id is resolved from bhv_ext at join time; for show events the join
    // is applied per item in the items list by downstream Jobs.
    @JsonProperty("item_id")
    public String itemId;

    @JsonProperty("category_id")
    public int categoryId;

    @JsonProperty("item_brand")
    public String itemBrand;

    @JsonProperty("item_price")
    public double itemPrice;

    public BehaviorWithDim() {}

    @Override
    public String toString() {
        return "BehaviorWithDim{uid='" + uid + "', bhvPage='" + bhvPage +
               "', bhvType='" + bhvType + "', bhvValue='" + bhvValue +
               "', bhvSrc='" + bhvSrc + "', itemId='" + itemId +
               "', userCity='" + userCity + "', itemBrand='" + itemBrand +
               "', itemPrice=" + itemPrice + "}";
    }
}

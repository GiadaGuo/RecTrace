package com.demo.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Behavior event enriched with user + item dimension data.
 * Output of Job1 -> Kafka topic: dwd_behavior_with_dim
 *
 * All fields from UserBehavior (bhvPage, bhvSrc, bhvType, bhvValue, bhvExt)
 * are passed through so downstream Jobs can route and filter by page / behavior type.
 *
 * req_id is a top-level field promoted from bhv_ext, present on all event types.
 * It serves as the page-level request identifier for show-event PV deduplication.
 *
 * show events  (bhv_type=show):
 *   Job1 expands bhv_ext.items into one record per exposed item.
 *   itemId and categoryId are set per item; itemBrand is null and itemPrice is 0.0
 *   (item dimension is not joined for show events — not needed by any downstream job).
 *   bhv_ext.items is cleared after expansion — item data lives in itemId/categoryId.
 *
 * click events (bhv_type=click, any bhv_page):
 *   Single record with itemId from bhv_ext.itemId; both user dim and item dim are joined.
 *   bhv_ext is passed through intact (contains itemId, position, query).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BehaviorWithDim {

    // ── Identity ──────────────────────────────────────────────────────────────
    @JsonProperty("uid")
    public String uid;

    /**
     * Recommendation / search request ID. Top-level field, present on all events.
     * For show events: same req_id is shared by all N expanded item records from
     * the same request. Use this field to deduplicate PV at the request level.
     */
    @JsonProperty("req_id")
    public String reqId;

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
    // For show events: itemId is set per expanded item; itemBrand is null, itemPrice is 0.0.
    // For click events: itemId from bhv_ext.itemId; itemBrand and itemPrice joined from Redis.
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
        return "BehaviorWithDim{uid='" + uid + "', reqId='" + reqId +
               "', bhvPage='" + bhvPage + "', bhvType='" + bhvType +
               "', bhvValue='" + bhvValue + "', bhvSrc='" + bhvSrc +
               "', itemId='" + itemId + "', userCity='" + userCity +
               "', itemBrand='" + itemBrand + "', itemPrice=" + itemPrice + "}";
    }
}

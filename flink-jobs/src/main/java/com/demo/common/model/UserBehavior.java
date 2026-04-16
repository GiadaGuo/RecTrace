package com.demo.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Raw user behavior event from Kafka topic: ods_user_behavior
 *
 * Supports three page scenarios:
 *   home  – homepage recommendation exposure and click
 *   search – search result page exposure, click, and query capture
 *   pdp    – product detail page actions (fav / cart / buy)
 *
 * All events share the public fields below. Page-specific data lives in bhv_ext.
 * Unknown fields are ignored so old and new formats coexist safely.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UserBehavior {

    // ── Identity ──────────────────────────────────────────────────────────────

    /** User ID, format U{6-digit}, e.g. U000001 */
    @JsonProperty("uid")
    public String uid;

    /** Log ID for deduplication, format log_{uuid-prefix} */
    @JsonProperty("logid")
    public String logId;

    // ── Event classification ──────────────────────────────────────────────────

    /**
     * Behavior point ID. Used for first-level routing (what type of interaction).
     *   1001 – exposure (show)
     *   2001 – click
     *   3001 – pdp action (fav / cart / buy determined by bhv_value)
     */
    @JsonProperty("bhv_id")
    public String bhvId;

    /**
     * Page where the behavior occurred.
     * Enum: home | search | pdp
     * Used for second-level routing after bhv_id.
     */
    @JsonProperty("bhv_page")
    public String bhvPage;

    /**
     * Traffic source / referrer page.
     * Enum: direct | home | search
     * PDP events carry the page that the user came from (pass-through from frontend).
     */
    @JsonProperty("bhv_src")
    public String bhvSrc;

    /**
     * Behavior type. Simplified to two values:
     *   show  – item exposure
     *   click – any click-type interaction (including pdp actions)
     */
    @JsonProperty("bhv_type")
    public String bhvType;

    /**
     * Behavior value. Null for pure clicks; set for pdp actions.
     * Enum: null | fav | cart | buy
     */
    @JsonProperty("bhv_value")
    public String bhvValue;

    // ── Session ───────────────────────────────────────────────────────────────

    /** Unix timestamp in milliseconds */
    @JsonProperty("ts")
    public long ts;

    // ── Extension fields (page-specific payload) ──────────────────────────────

    /**
     * Extension object. Contents vary by page and bhv_type:
     *   home show  : req_id + items[]
     *   home click : req_id + item_id + position
     *   search show : req_id + query + items[]
     *   search click: req_id + query + item_id + position
     *   pdp *       : item_id + req_id
     */
    @JsonProperty("bhv_ext")
    public BhvExt bhvExt;

    public UserBehavior() {}

    @Override
    public String toString() {
        return "UserBehavior{uid='" + uid + "', bhvId='" + bhvId +
               "', bhvPage='" + bhvPage + "', bhvType='" + bhvType +
               "', bhvValue='" + bhvValue + "', bhvSrc='" + bhvSrc +
               "', ts=" + ts + ", bhvExt=" + bhvExt + "}";
    }
}

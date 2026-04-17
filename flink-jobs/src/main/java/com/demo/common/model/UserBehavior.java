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
 *
 * Field placement rationale:
 *   req_id is a top-level field (not inside bhv_ext) because it is a session-level
 *   identifier shared across all event types. bhv_ext is reserved for page-specific
 *   parameters only (items list, query, click position).
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

    /**
     * Recommendation / search request ID. Present in all events.
     * Top-level field — shared across all page types and behavior types.
     * Used for show-event deduplication when counting page-level PV.
     */
    @JsonProperty("req_id")
    public String reqId;

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
     * Extension object. Contents vary by page and bhv_type.
     * req_id is NOT included here — it is a top-level field above.
     *   home  show  : items[]
     *   home  click : item_id + position
     *   search show : query + items[]
     *   search click: query + item_id + position
     *   pdp   *     : item_id
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

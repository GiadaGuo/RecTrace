package com.demo.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Home-channel cross feature per (user_id + category_id) within a 1-minute tumbling window.
 * Output of Job2 -> Kafka topic: dws_user_item_feature, Redis feat:user_item:{uid}:{catId}
 *
 * Event scope:
 *   show_count : bhv_page=home, bhv_type=show  (item dim joined by Job1 for home show events)
 *   click_count: bhv_page=home, bhv_type=click, bhv_value=null
 *   cart/fav/buy: bhv_page=pdp, bhv_src=home, bhv_type=click, bhv_value=cart/fav/buy
 */
public class UserItemFeature {

    @JsonProperty("user_id")
    public String userId;

    @JsonProperty("category_id")
    public int categoryId;

    @JsonProperty("window_start")
    public long windowStart;

    @JsonProperty("window_end")
    public long windowEnd;

    // ── Raw counts ────────────────────────────────────────────────────────────
    /** home show 曝光次数（bhv_page=home, bhv_type=show） */
    @JsonProperty("show_count")
    public long showCount;

    /** home click 点击次数（bhv_page=home, bhv_type=click, bhv_value=null） */
    @JsonProperty("click_count")
    public long clickCount;

    @JsonProperty("cart_count")
    public long cartCount;

    @JsonProperty("fav_count")
    public long favCount;

    @JsonProperty("buy_count")
    public long buyCount;

    public UserItemFeature() {}

    @Override
    public String toString() {
        return "UserItemFeature{userId='" + userId + "', categoryId=" + categoryId +
               ", show=" + showCount + ", click=" + clickCount +
               ", cart=" + cartCount + ", fav=" + favCount + ", buy=" + buyCount + "}";
    }
}

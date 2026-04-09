package com.demo.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Cross feature per (user_id + category_id) within a tumbling window.
 * Output of Job2 -> Kafka topic: dws_user_item_feature
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

    @JsonProperty("pv_count")
    public long pvCount;

    @JsonProperty("cart_count")
    public long cartCount;

    @JsonProperty("fav_count")
    public long favCount;

    @JsonProperty("buy_count")
    public long buyCount;

    /** buy_count / pv_count, 0 if pvCount == 0 */
    @JsonProperty("click_to_buy_rate")
    public double clickToBuyRate;

    public UserItemFeature() {}

    @Override
    public String toString() {
        return "UserItemFeature{userId='" + userId + "', categoryId=" + categoryId +
               ", pv=" + pvCount + ", cart=" + cartCount + ", fav=" + favCount +
               ", buy=" + buyCount + ", ctr=" + String.format("%.4f", clickToBuyRate) + "}";
    }
}

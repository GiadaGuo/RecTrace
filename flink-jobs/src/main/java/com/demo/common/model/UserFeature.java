package com.demo.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Cumulative user-level real-time feature.
 * Output of Job3 -> Kafka topic: dws_user_feature
 */
public class UserFeature {

    @JsonProperty("user_id")
    public String userId;

    @JsonProperty("user_age")
    public int userAge;

    @JsonProperty("user_city")
    public String userCity;

    @JsonProperty("user_level")
    public int userLevel;

    @JsonProperty("total_pv")
    public long totalPv;

    /** Pure click count (bhv_type=click + bhv_value=null, excludes fav/cart/buy) */
    @JsonProperty("total_click")
    public long totalClick;

    @JsonProperty("total_cart")
    public long totalCart;

    @JsonProperty("total_fav")
    public long totalFav;

    @JsonProperty("total_buy")
    public long totalBuy;

    @JsonProperty("last_active_ts")
    public long lastActiveTs;

    /** Number of distinct active days (based on event timestamps) */
    @JsonProperty("active_days")
    public long activeDays;

    @JsonProperty("update_ts")
    public long updateTs;

    public UserFeature() {}

    @Override
    public String toString() {
        return "UserFeature{userId='" + userId + "', pv=" + totalPv +
               ", buy=" + totalBuy + ", activeDays=" + activeDays + "}";
    }
}

package com.demo.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Cumulative item-level real-time feature.
 * Output of Job3 -> Kafka topic: dws_item_feature
 */
public class ItemFeature {

    @JsonProperty("item_id")
    public String itemId;

    @JsonProperty("category_id")
    public int categoryId;

    @JsonProperty("item_brand")
    public String itemBrand;

    @JsonProperty("item_price")
    public double itemPrice;

    @JsonProperty("total_pv")
    public long totalPv;

    @JsonProperty("total_buy")
    public long totalBuy;

    @JsonProperty("total_cart")
    public long totalCart;

    @JsonProperty("total_fav")
    public long totalFav;

    /** Approximate unique visitors (distinct user count) */
    @JsonProperty("uv")
    public long uv;

    /** total_buy / total_pv, 0 if totalPv == 0 */
    @JsonProperty("conversion_rate")
    public double conversionRate;

    @JsonProperty("update_ts")
    public long updateTs;

    public ItemFeature() {}

    @Override
    public String toString() {
        return "ItemFeature{itemId='" + itemId + "', pv=" + totalPv +
               ", buy=" + totalBuy + ", uv=" + uv +
               ", cvr=" + String.format("%.4f", conversionRate) + "}";
    }
}

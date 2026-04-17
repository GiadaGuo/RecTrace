package com.demo.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Cumulative item-level real-time feature.
 * Output of Job3 -> Kafka topic: dws_item_feature
 *
 * Semantic notes:
 *   total_detail_click: number of times this item was clicked into (bhv_type=click, bhv_value=null)
 *                       i.e. users entering the detail page from home/search feed
 *   conversion_rate   : total_buy / total_detail_click — click-to-purchase rate
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

    /** Number of detail-page entries (bhv_type=click, bhv_value=null) */
    @JsonProperty("total_detail_click")
    public long totalDetailClick;

    @JsonProperty("total_buy")
    public long totalBuy;

    @JsonProperty("total_cart")
    public long totalCart;

    @JsonProperty("total_fav")
    public long totalFav;

    /** Approximate unique visitors (distinct user count) */
    @JsonProperty("uv")
    public long uv;

    /** total_buy / total_detail_click, 0 if totalDetailClick == 0 */
    @JsonProperty("conversion_rate")
    public double conversionRate;

    @JsonProperty("update_ts")
    public long updateTs;

    public ItemFeature() {}

    @Override
    public String toString() {
        return "ItemFeature{itemId='" + itemId + "', detailClick=" + totalDetailClick +
               ", buy=" + totalBuy + ", uv=" + uv +
               ", cvr=" + String.format("%.4f", conversionRate) + "}";
    }
}

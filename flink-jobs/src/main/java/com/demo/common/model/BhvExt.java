package com.demo.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Extension fields for behavior events, embedded in bhv_ext of UserBehavior.
 *
 * req_id has been promoted to a top-level field in UserBehavior and BehaviorWithDim.
 * It is intentionally removed from this class; all downstream code should use
 * the top-level reqId field instead.
 *
 * Field presence depends on bhv_page and bhv_type:
 *   home  show  : items (list of {item_id, position})
 *   home  click : item_id + position
 *   search show : query + items
 *   search click: query + item_id + position
 *   pdp   *     : item_id
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BhvExt {

    /**
     * Exposure item list for show events.
     * Each entry: {"item_id": "I0000123", "position": 1}
     * Only present when bhv_type=show.
     * After Job1 expansion, this field is cleared in DWD records — items are
     * promoted to top-level item_id. Access item data via BehaviorWithDim.itemId.
     */
    @JsonProperty("items")
    public List<ExposureItem> items;

    /** Single item ID for click / pdp events. */
    @JsonProperty("item_id")
    public String itemId;

    /** Click position within the result list (1-based). */
    @JsonProperty("position")
    public Integer position;

    /** Raw search query string. Present when bhv_page=search. */
    @JsonProperty("query")
    public String query;

    public BhvExt() {}

    // ── Inner class for exposure item list ────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ExposureItem {

        @JsonProperty("item_id")
        public String itemId;

        @JsonProperty("position")
        public int position;

        public ExposureItem() {}

        @Override
        public String toString() {
            return "{itemId='" + itemId + "', position=" + position + "}";
        }
    }

    @Override
    public String toString() {
        return "BhvExt{query='" + query +
               "', itemId='" + itemId + "', position=" + position +
               ", items=" + items + "}";
    }
}

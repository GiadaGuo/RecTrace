package com.demo.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Extension fields for behavior events, embedded in bhv_ext of UserBehavior.
 *
 * Field presence depends on bhv_page and bhv_type:
 *   home  show  : req_id + items (list of {item_id, position})
 *   home  click : req_id + item_id + position
 *   search show : req_id + query + items
 *   search click: req_id + query + item_id + position
 *   pdp   *     : item_id + req_id (pass-through from source page)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BhvExt {

    /** Recommendation or search request ID. Present in all events. */
    @JsonProperty("req_id")
    public String reqId;

    /**
     * Exposure item list for show events.
     * Each entry: {"item_id": "I0000123", "position": 1}
     * Only present when bhv_type=show.
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
        return "BhvExt{reqId='" + reqId + "', query='" + query +
               "', itemId='" + itemId + "', position=" + position +
               "', items=" + items + "}";
    }
}

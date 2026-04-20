"""
rule_model.py
-------------
Weighted rule-based ranking model (MVP).

Scoring formula per candidate item
-----------------------------------
raw_score = w1 * conversion_rate
          + w2 * cross_click_cnt   (user interest in item's category, decay-weighted)
          + w3 * cross_buy_cnt     (user purchase intent for category)
          + w4 * in_click_seq      (1.0 if item is in recent click sequence, else 0.0)
          + w5 * item_click_1h     (item popularity in last 1 hour)

All raw scores are min-max normalised to [0, 1] before returning.
If all scores are equal (e.g. all-zero cold start), scores default to 0.5.

Feature contributions are expressed as the fractional share each component
contributes to the unnormalised raw score (before normalisation).
"""

from typing import List, Optional

import redis

from feature.feature_fetcher import fetch_feature_bundle
from models.base_model import BaseModel, ScoredItem

# ── Weight configuration ──────────────────────────────────────────────────────
# Weights are module-level constants.  Adjust here when re-calibrating the model;
# a separate config file is warranted only when weights need hot-reload at runtime.
W1_CONVERSION_RATE = 0.35   # item's historical conversion rate
W2_CROSS_CLICK     = 0.25   # user's interest in item's category
W3_CROSS_BUY       = 0.20   # user's purchase intent for category
W4_IN_SEQ          = 0.10   # item already in user's click history
W5_ITEM_CLICK_1H   = 0.10   # item's recent hour popularity (normalised by /100)

# Normalise click_1h to a [0,1]-ish scale before weighting
_CLICK_1H_SCALE = 100.0


class RuleModel(BaseModel):
    """
    Stateless weighted rule scorer.  Reads all features from Redis on every call.
    """

    def predict(
        self,
        uid: str,
        candidates: List[str],
        r: Optional[redis.Redis] = None,
    ) -> List[ScoredItem]:
        if not candidates:
            return []

        scored: List[ScoredItem] = []

        for item_id in candidates:
            bundle = fetch_feature_bundle(uid, item_id, r=r)
            item = bundle.item
            cross = bundle.cross
            user = bundle.user

            # Raw component values
            conversion_rate = min(item.conversion_rate, 1.0)
            # Normalise exponential-decay counts to [0, 1]:
            #   cross_click_cnt is a decay-weighted click count with half-life 7 days.
            #   A value of ~10 represents strong sustained interest (e.g. 10 recent clicks
            #   with no decay, or equivalent decayed history).  Dividing by 10 maps this
            #   "typical upper bound" to 1.0, then min() caps any extreme outliers.
            #   cross_buy_cnt uses /5 because purchase signals are sparser — a buy count
            #   of 5 already indicates very strong intent, so the scale is tighter.
            cross_click = min(cross.click_cnt / 10.0, 1.0)   # decay-count, cap at 1
            cross_buy   = min(cross.buy_cnt   / 5.0, 1.0)    # buy intent, cap at 1
            in_seq       = 1.0 if item_id in user.click_seq else 0.0
            click_1h_norm = min(item.click_1h / _CLICK_1H_SCALE, 1.0)

            # Weighted sum
            c1 = W1_CONVERSION_RATE * conversion_rate
            c2 = W2_CROSS_CLICK     * cross_click
            c3 = W3_CROSS_BUY       * cross_buy
            c4 = W4_IN_SEQ          * in_seq
            c5 = W5_ITEM_CLICK_1H   * click_1h_norm
            raw = c1 + c2 + c3 + c4 + c5

            # Feature contributions as share of raw score (avoid division by zero)
            total = raw if raw > 0 else 1.0
            contributions = {
                "conversion_rate":  round(c1 / total, 4),
                "cross_click_cnt":  round(c2 / total, 4),
                "cross_buy_cnt":    round(c3 / total, 4),
                "in_click_seq":     round(c4 / total, 4),
                "item_click_1h":    round(c5 / total, 4),
            }

            scored.append(
                ScoredItem(
                    item_id=item_id,
                    score=raw,
                    feature_contributions=contributions,
                    item_brand=item.item_brand,
                    item_price=item.item_price,
                    category_id=item.category_id,
                )
            )

        # Min-max normalise scores
        scores_raw = [s.score for s in scored]
        s_min, s_max = min(scores_raw), max(scores_raw)
        if s_max > s_min:
            for s in scored:
                s.score = (s.score - s_min) / (s_max - s_min)
        else:
            for s in scored:
                s.score = 0.5

        scored.sort(key=lambda x: x.score, reverse=True)
        return scored

"""
test_rule_model.py
------------------
Tests for models/rule_model.py.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import fakeredis

from models.rule_model import RuleModel


def _seed_item(r, item_id: str, conversion_rate: float = 0.1,
               click_1h: float = 10.0, category_id: int = 1,
               brand: str = "Nike", price: float = 100.0):
    r.hset(f"feat:item:{item_id}", mapping={
        "total_detail_click": "100", "total_buy": str(int(conversion_rate * 100)),
        "conversion_rate": str(conversion_rate), "category_id": str(category_id),
        "item_brand": brand, "item_price": str(price),
        "total_cart": "5", "total_fav": "3", "uv": "80",
    })
    r.hset(f"feat:item:stat:{item_id}", mapping={"click_1h": str(click_1h), "click_5min": "2"})


def _seed_user(r, uid: str, click_seq=None):
    r.hset(f"feat:user:{uid}", mapping={
        "total_pv": "20", "total_click": "10", "total_cart": "3",
        "total_fav": "2", "total_buy": "1", "active_days": "5",
        "user_age": "28", "user_city": "北京", "user_level": "3",
    })
    if click_seq:
        for i, iid in enumerate(click_seq):
            r.zadd(f"seq:click:{uid}", {iid: i + 1})


class TestRuleModel:
    def test_scores_in_zero_one_range(self, fake_redis):
        """All returned scores are in [0, 1] after normalisation."""
        _seed_user(fake_redis, "U000001")
        for i in range(1, 6):
            _seed_item(fake_redis, f"I000000{i}", conversion_rate=i * 0.05, click_1h=i * 5.0)

        model = RuleModel()
        results = model.predict("U000001", [f"I000000{i}" for i in range(1, 6)], r=fake_redis)

        assert len(results) == 5
        for s in results:
            assert 0.0 <= s.score <= 1.0

    def test_results_sorted_descending(self, fake_redis):
        """Results are returned in descending score order."""
        _seed_user(fake_redis, "U000001")
        _seed_item(fake_redis, "I0000001", conversion_rate=0.5, click_1h=50.0)
        _seed_item(fake_redis, "I0000002", conversion_rate=0.01, click_1h=1.0)

        model = RuleModel()
        results = model.predict("U000001", ["I0000001", "I0000002"], r=fake_redis)

        scores = [r.score for r in results]
        assert scores == sorted(scores, reverse=True)
        assert results[0].item_id == "I0000001"

    def test_cold_start_all_features_missing(self, fake_redis):
        """All features missing → returns scores at 0.5, no exception."""
        model = RuleModel()
        results = model.predict("U_COLD", ["I_GHOST_1", "I_GHOST_2"], r=fake_redis)

        assert len(results) == 2
        for s in results:
            assert s.score == pytest.approx(0.5)

    def test_feature_contributions_sum_to_one(self, fake_redis):
        """feature_contributions values should sum to ~1.0 when item has non-zero raw score."""
        _seed_user(fake_redis, "U000001")
        _seed_item(fake_redis, "I0000001", conversion_rate=0.2, click_1h=20.0)
        fake_redis.hset("cross:U000001:1", mapping={"click_cnt": "3.0", "buy_cnt": "1.0",
                                                      "pv_cnt": "5.0", "cart_cnt": "0.5"})

        model = RuleModel()
        results = model.predict("U000001", ["I0000001"], r=fake_redis)

        contribs = results[0].feature_contributions
        total = sum(contribs.values())
        assert pytest.approx(total, abs=0.01) == 1.0

    def test_item_in_click_seq_gets_bonus(self, fake_redis):
        """An item already in the user's click history should score higher than an unseen one."""
        _seed_user(fake_redis, "U000001", click_seq=["I0000001"])
        # Both items identical except seq membership
        _seed_item(fake_redis, "I0000001", conversion_rate=0.1)
        _seed_item(fake_redis, "I0000002", conversion_rate=0.1)

        model = RuleModel()
        results = model.predict("U000001", ["I0000001", "I0000002"], r=fake_redis)

        result_map = {r.item_id: r.score for r in results}
        assert result_map["I0000001"] >= result_map["I0000002"]

    def test_empty_candidates_returns_empty(self, fake_redis):
        model = RuleModel()
        assert model.predict("U000001", [], r=fake_redis) == []

"""
test_feature_fetcher.py
-----------------------
Tests for feature/feature_fetcher.py.

All tests use fakeredis — no real Redis required.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import fakeredis

from feature.feature_fetcher import (
    build_candidate_pool,
    fetch_cross_features,
    fetch_feature_bundle,
    fetch_item_features,
    fetch_user_features,
    fetch_user_item_window,
)


# ── fetch_user_features ───────────────────────────────────────────────────────

class TestFetchUserFeatures:
    def test_normal_path(self, fake_redis):
        """Correctly maps Redis hash fields to UserFeatures dataclass."""
        fake_redis.hset("feat:user:U000001", mapping={
            "total_pv": "10", "total_click": "5", "total_cart": "2",
            "total_fav": "1", "total_buy": "0", "active_days": "3",
            "user_age": "25", "user_city": "上海", "user_level": "2",
            "update_ts": "1700000000000",
        })
        fake_redis.hset("feat:user:stat:U000001", mapping={
            "click_5min": "3.0", "click_1h": "7.5",
            "pv_5min": "5.0", "pv_1h": "12.0",
        })
        fake_redis.zadd("seq:click:U000001", {"I0000001": 100, "I0000002": 200})

        uf = fetch_user_features("U000001", r=fake_redis)

        assert uf.uid == "U000001"
        assert uf.total_pv == 10
        assert uf.total_click == 5
        assert uf.total_cart == 2
        assert uf.user_city == "上海"
        assert uf.user_level == 2
        assert uf.click_5min == 3.0
        assert uf.click_1h == 7.5
        # ZSet newest-first: I0000002 has higher score
        assert uf.click_seq[0] == "I0000002"
        assert "I0000001" in uf.click_seq

    def test_cold_start_returns_defaults(self, fake_redis):
        """Key does not exist → all fields return default values, no exception."""
        uf = fetch_user_features("U_COLD", r=fake_redis)
        assert uf.uid == "U_COLD"
        assert uf.total_pv == 0
        assert uf.total_click == 0
        assert uf.user_city == ""
        assert uf.click_seq == []

    def test_empty_seq_returns_empty_list(self, fake_redis):
        """When seq:click key exists but is empty, returns empty list."""
        fake_redis.hset("feat:user:U000002", mapping={"total_pv": "1"})
        uf = fetch_user_features("U000002", r=fake_redis)
        assert uf.click_seq == []


# ── fetch_item_features ───────────────────────────────────────────────────────

class TestFetchItemFeatures:
    def test_normal_path(self, fake_redis):
        fake_redis.hset("feat:item:I0000001", mapping={
            "total_detail_click": "50", "total_buy": "5",
            "total_cart": "8", "total_fav": "3", "uv": "40",
            "conversion_rate": "0.1", "category_id": "7",
            "item_brand": "Nike", "item_price": "299.0",
        })
        fake_redis.hset("feat:item:stat:I0000001", mapping={
            "click_5min": "2.0", "click_1h": "15.0",
        })

        itf = fetch_item_features("I0000001", r=fake_redis)

        assert itf.item_id == "I0000001"
        assert itf.total_detail_click == 50
        assert itf.conversion_rate == pytest.approx(0.1)
        assert itf.category_id == 7
        assert itf.item_brand == "Nike"
        assert itf.item_price == pytest.approx(299.0)
        assert itf.click_1h == pytest.approx(15.0)

    def test_cold_item_falls_back_to_dim(self, fake_redis):
        """feat:item key missing → falls back to dim:item for brand/price/category."""
        fake_redis.hset("dim:item:I0000999", mapping={
            "category_id": "12", "brand": "Adidas", "price": "599.0",
        })
        itf = fetch_item_features("I0000999", r=fake_redis)

        assert itf.item_id == "I0000999"
        assert itf.category_id == 12
        assert itf.item_brand == "Adidas"
        assert itf.item_price == pytest.approx(599.0)
        assert itf.total_detail_click == 0  # default

    def test_completely_cold_returns_defaults(self, fake_redis):
        """Neither feat:item nor dim:item exist → safe defaults."""
        itf = fetch_item_features("I_GHOST", r=fake_redis)
        assert itf.item_id == "I_GHOST"
        assert itf.category_id == 0
        assert itf.item_price == 0.0


# ── fetch_cross_features ──────────────────────────────────────────────────────

class TestFetchCrossFeatures:
    def test_normal_path(self, fake_redis):
        fake_redis.hset("cross:U000001:7", mapping={
            "pv_cnt": "5.1234", "click_cnt": "2.5000",
            "cart_cnt": "1.0000", "buy_cnt": "0.5000",
        })
        cf = fetch_cross_features("U000001", 7, r=fake_redis)
        assert cf.click_cnt == pytest.approx(2.5)
        assert cf.buy_cnt == pytest.approx(0.5)

    def test_missing_key_returns_zeros(self, fake_redis):
        cf = fetch_cross_features("U_COLD", 99, r=fake_redis)
        assert cf.click_cnt == 0.0
        assert cf.buy_cnt == 0.0


# ── fetch_user_item_window ────────────────────────────────────────────────────

class TestFetchUserItemWindow:
    def test_normal_path(self, fake_redis):
        fake_redis.hset("feat:user_item:U000001:7", mapping={
            "show_cnt": "13", "click_cnt": "1",
            "cart_cnt": "0", "buy_cnt": "0",
        })
        ui = fetch_user_item_window("U000001", 7, r=fake_redis)
        assert ui.show_cnt == 13
        assert ui.click_cnt == 1

    def test_missing_returns_zeros(self, fake_redis):
        ui = fetch_user_item_window("U_COLD", 1, r=fake_redis)
        assert ui.show_cnt == 0


# ── build_candidate_pool ──────────────────────────────────────────────────────

class TestBuildCandidatePool:
    def _populate_dim(self, r, n=20):
        for i in range(1, n + 1):
            r.hset(f"dim:item:I{i:07d}", mapping={"category_id": "1", "brand": "X", "price": "100"})

    def test_history_items_appear_first(self, fake_redis):
        self._populate_dim(fake_redis)
        fake_redis.zadd("seq:click:U000001", {"I0000001": 200, "I0000002": 100})
        pool = build_candidate_pool("U000001", top_k=10, r=fake_redis)
        # history items must be in pool
        assert "I0000001" in pool
        assert "I0000002" in pool

    def test_no_duplicates(self, fake_redis):
        self._populate_dim(fake_redis)
        fake_redis.zadd("seq:click:U000001", {"I0000001": 200})
        fake_redis.zadd("seq:cart:U000001", {"I0000001": 150})
        pool = build_candidate_pool("U000001", top_k=5, r=fake_redis)
        assert len(pool) == len(set(pool))

    def test_cold_start_returns_dim_items(self, fake_redis):
        """No sequences → supplemented from dim:item:*."""
        self._populate_dim(fake_redis, n=30)
        pool = build_candidate_pool("U_COLD", top_k=5, r=fake_redis)
        assert len(pool) > 0

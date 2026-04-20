"""
test_recommend_router.py
------------------------
Integration tests for POST /recommend and POST /recommend/inject.

Uses httpx.AsyncClient + FastAPI TestClient via fakeredis dependency override.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import fakeredis
from fastapi.testclient import TestClient

from main import app
from routers.recommend import get_redis_client


# ── Helpers ───────────────────────────────────────────────────────────────────

def _seed_dim_items(r, n: int = 30):
    for i in range(1, n + 1):
        r.hset(f"dim:item:I{i:07d}", mapping={
            "category_id": str((i % 10) + 1),
            "brand": "TestBrand",
            "price": "99.0",
        })


def _seed_item_features(r, item_id: str, conversion_rate: float = 0.1,
                        click_1h: float = 5.0, category_id: int = 1):
    r.hset(f"feat:item:{item_id}", mapping={
        "total_detail_click": "50", "total_buy": "5",
        "conversion_rate": str(conversion_rate),
        "category_id": str(category_id),
        "item_brand": "Nike", "item_price": "199.0",
        "total_cart": "3", "total_fav": "2", "uv": "40",
    })
    r.hset(f"feat:item:stat:{item_id}", mapping={"click_1h": str(click_1h), "click_5min": "1"})


def _seed_user(r, uid: str, click_seq=None):
    r.hset(f"feat:user:{uid}", mapping={
        "total_pv": "15", "total_click": "8", "total_cart": "2",
        "total_fav": "1", "total_buy": "1",
        "active_days": "4", "user_age": "30",
        "user_city": "北京", "user_level": "2",
    })
    if click_seq:
        for i, iid in enumerate(click_seq):
            r.zadd(f"seq:click:{uid}", {iid: i + 1})


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def client_with_redis():
    """Returns (TestClient, fake_redis) with dependency override applied."""
    fr = fakeredis.FakeRedis(decode_responses=True)

    app.dependency_overrides[get_redis_client] = lambda: fr

    client = TestClient(app)
    yield client, fr

    app.dependency_overrides.clear()


# ── POST /recommend ───────────────────────────────────────────────────────────

class TestRecommendEndpoint:
    def test_returns_200_with_items(self, client_with_redis):
        client, fr = client_with_redis
        _seed_dim_items(fr, n=30)
        _seed_user(fr, "U000001")

        resp = client.post("/recommend", json={"uid": "U000001", "top_k": 5})

        assert resp.status_code == 200
        body = resp.json()
        assert "req_id" in body
        assert body["req_id"].startswith("req_")
        assert isinstance(body["items"], list)
        assert len(body["items"]) > 0

    def test_top_k_respected(self, client_with_redis):
        client, fr = client_with_redis
        _seed_dim_items(fr, n=50)
        _seed_user(fr, "U000001")

        resp = client.post("/recommend", json={"uid": "U000001", "top_k": 3})

        assert resp.status_code == 200
        assert len(resp.json()["items"]) <= 3

    def test_item_shape_contains_required_fields(self, client_with_redis):
        client, fr = client_with_redis
        _seed_dim_items(fr, n=20)
        _seed_user(fr, "U000001")

        resp = client.post("/recommend", json={"uid": "U000001", "top_k": 5})

        item = resp.json()["items"][0]
        for field in ("item_id", "score", "feature_contributions"):
            assert field in item, f"Missing field: {field}"

    def test_snapshot_written_to_redis(self, client_with_redis):
        client, fr = client_with_redis
        _seed_dim_items(fr, n=20)
        _seed_user(fr, "U000001")

        resp = client.post("/recommend", json={"uid": "U000001", "top_k": 5})

        req_id = resp.json()["req_id"]
        snapshot = fr.hgetall(f"snapshot:rec:{req_id}")
        assert snapshot.get("uid") == "U000001"
        assert "scores_json" in snapshot

    def test_cold_start_user_still_returns_results(self, client_with_redis):
        """A brand-new user with no history should still get recommendations."""
        client, fr = client_with_redis
        _seed_dim_items(fr, n=30)

        resp = client.post("/recommend", json={"uid": "U_BRAND_NEW", "top_k": 5})

        assert resp.status_code == 200
        assert len(resp.json()["items"]) > 0


# ── POST /recommend/inject ────────────────────────────────────────────────────

class TestInjectEndpoint:
    def test_inject_click_returns_ok(self, client_with_redis):
        client, fr = client_with_redis
        _seed_user(fr, "U000001")
        _seed_item_features(fr, "I0000001")

        resp = client.post("/recommend/inject", json={
            "uid": "U000001", "item_id": "I0000001", "bhv_type": "click"
        })

        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_inject_click_increments_total_click(self, client_with_redis):
        client, fr = client_with_redis
        _seed_user(fr, "U000001")
        fr.hset("feat:item:I0000001", mapping={"total_detail_click": "10", "total_buy": "1"})

        client.post("/recommend/inject", json={
            "uid": "U000001", "item_id": "I0000001", "bhv_type": "click"
        })

        assert fr.hget("feat:user:U000001", "total_click") == "9"   # was 8, now 9
        assert fr.hget("feat:item:I0000001", "total_detail_click") == "11"

    def test_inject_click_adds_to_seq(self, client_with_redis):
        client, fr = client_with_redis
        _seed_user(fr, "U000001")

        client.post("/recommend/inject", json={
            "uid": "U000001", "item_id": "I0000001", "bhv_type": "click"
        })

        seq = fr.zrevrange("seq:click:U000001", 0, -1)
        assert "I0000001" in seq

    def test_inject_buy_updates_conversion_rate(self, client_with_redis):
        client, fr = client_with_redis
        _seed_user(fr, "U000001")
        fr.hset("feat:item:I0000001", mapping={
            "total_detail_click": "10", "total_buy": "2",
            "conversion_rate": "0.2",
        })

        client.post("/recommend/inject", json={
            "uid": "U000001", "item_id": "I0000001", "bhv_type": "buy"
        })

        new_cvr = float(fr.hget("feat:item:I0000001", "conversion_rate"))
        # total_buy=3, total_detail_click=10 → 0.3
        assert new_cvr == pytest.approx(0.3, abs=0.001)

    def test_inject_changes_recommendation_result(self, client_with_redis):
        """Inject a click for a specific item and verify /recommend result changes."""
        client, fr = client_with_redis
        _seed_dim_items(fr, n=20)
        _seed_user(fr, "U000001")
        for i in range(1, 6):
            _seed_item_features(fr, f"I000000{i}", conversion_rate=0.1)

        # First recommendation
        resp1 = client.post("/recommend", json={"uid": "U000001", "top_k": 5})
        scores_before = {it["item_id"]: it["score"] for it in resp1.json()["items"]}

        # Inject high-signal behaviour on I0000001
        for _ in range(5):
            client.post("/recommend/inject", json={
                "uid": "U000001", "item_id": "I0000001", "bhv_type": "buy"
            })

        # Second recommendation — I0000001 should rank higher or scores should differ
        resp2 = client.post("/recommend", json={"uid": "U000001", "top_k": 5})
        scores_after = {it["item_id"]: it["score"] for it in resp2.json()["items"]}

        # At minimum the scores must differ (inject had an effect)
        assert scores_before != scores_after

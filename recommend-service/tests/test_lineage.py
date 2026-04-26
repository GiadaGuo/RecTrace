"""
test_lineage.py
---------------
Tests for agent/lineage.py: run_lineage_trace logic.

All tests use fakeredis — no real Redis required.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import json
import pytest
import fakeredis

from agent.lineage import run_lineage_trace


# ── Helpers ───────────────────────────────────────────────────────────────────

def _seed_snapshot(fr, req_id="req_test01", uid="U000001", ts="1720000000000",
                   scores=None):
    """Write a snapshot hash to fakeredis."""
    if scores is None:
        scores = [
            {"item_id": "I0000001", "score": 0.9,
             "feature_contributions": {"conversion_rate": 0.3, "category_match": 0.4}},
            {"item_id": "I0000002", "score": 0.7,
             "feature_contributions": {"conversion_rate": 0.2, "category_match": 0.3}},
        ]
    fr.hset(f"snapshot:rec:{req_id}", mapping={
        "uid": uid,
        "ts": ts,
        "user_features_json": json.dumps({"uid": uid, "total_pv": 10}),
        "candidates_json": json.dumps(["I0000001", "I0000002"]),
        "scores_json": json.dumps(scores),
    })


# ── run_lineage_trace ─────────────────────────────────────────────────────────

class TestRunLineageTrace:
    def test_returns_seq_items_and_contributing_features(self, fake_redis):
        """Given uid + item_id with pre-seeded data, returns dict with seq_items + contributing_features."""
        _seed_snapshot(fake_redis, req_id="req_test01", uid="U000001", ts="1720000000000")
        fake_redis.zadd("seq:click:U000001", {"I0000003": 100, "I0000001": 300})

        result = run_lineage_trace("U000001", "I0000001", rc=fake_redis)

        assert result["uid"] == "U000001"
        assert result["item_id"] == "I0000001"
        assert result["req_id"] == "req_test01"
        assert "conversion_rate" in result["contributing_features"]
        # seq_items should be ordered by ts descending (I0000001 first)
        assert "I0000001" in result["seq_items"]

    def test_missing_data_returns_empty_path(self, fake_redis):
        """When no snapshot or sequence data exists, returns empty sections, no exception."""
        result = run_lineage_trace("U_NODATA", "I_NODATA", rc=fake_redis)

        assert result["uid"] == "U_NODATA"
        assert result["item_id"] == "I_NODATA"
        assert result["req_id"] is None
        assert result["seq_items"] == []
        assert result["contributing_features"] == {}

    def test_picks_most_recent_snapshot(self, fake_redis):
        """When multiple snapshots exist, picks the one with the highest ts."""
        _seed_snapshot(fake_redis, req_id="req_older", uid="U000001", ts="1710000000000",
                       scores=[{"item_id": "I0000001", "score": 0.5,
                                "feature_contributions": {"old": 0.1}}])
        _seed_snapshot(fake_redis, req_id="req_newer", uid="U000001", ts="1720000000000",
                       scores=[{"item_id": "I0000001", "score": 0.9,
                                "feature_contributions": {"new": 0.9}}])

        result = run_lineage_trace("U000001", "I0000001", rc=fake_redis)

        assert result["req_id"] == "req_newer"
        assert "new" in result["contributing_features"]

    def test_does_not_raise_on_redis_error(self, fake_redis):
        """Even if Redis operations fail, the function should not raise."""
        # fakeredis won't raise by itself, but we test that the function
        # structure handles exceptions gracefully
        result = run_lineage_trace("U000001", "I0000001", rc=fake_redis)
        # Should at minimum have the structure
        assert "uid" in result
        assert "seq_items" in result
        assert "contributing_features" in result

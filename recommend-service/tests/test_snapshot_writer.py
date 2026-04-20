"""
test_snapshot_writer.py
-----------------------
Tests for feature/snapshot_writer.py.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import json
import pytest
import fakeredis

from feature.feature_schema import UserFeatures
from feature.snapshot_writer import write_snapshot, _TTL_SECONDS


class TestSnapshotWriter:
    def _make_user_features(self) -> UserFeatures:
        return UserFeatures(
            uid="U000001", total_pv=10, total_click=5,
            user_age=28, user_city="上海", user_level=3,
        )

    def test_writes_all_required_fields(self, fake_redis):
        uf = self._make_user_features()
        candidates = ["I0000001", "I0000002", "I0000003"]
        scores = [{"item_id": "I0000001", "score": 0.9}]

        write_snapshot("req_abc12345", "U000001", uf, candidates, scores, r=fake_redis)

        data = fake_redis.hgetall("snapshot:rec:req_abc12345")
        assert data["uid"] == "U000001"
        assert "ts" in data
        assert json.loads(data["candidates_json"]) == candidates
        assert json.loads(data["scores_json"])[0]["item_id"] == "I0000001"
        user_f = json.loads(data["user_features_json"])
        assert user_f["total_pv"] == 10

    def test_ttl_set_to_86400(self, fake_redis):
        uf = self._make_user_features()
        write_snapshot("req_ttl_test", "U000001", uf, [], [], r=fake_redis)
        ttl = fake_redis.ttl("snapshot:rec:req_ttl_test")
        # TTL should be within [TTL_SECONDS - 2, TTL_SECONDS]
        assert _TTL_SECONDS - 2 <= ttl <= _TTL_SECONDS

    def test_click_seq_excluded_from_user_features_json(self, fake_redis):
        """click_seq (large list) must not be serialised into user_features_json."""
        uf = self._make_user_features()
        uf.click_seq = ["I0000001", "I0000002"]
        write_snapshot("req_seq_excl", "U000001", uf, [], [], r=fake_redis)

        data = fake_redis.hgetall("snapshot:rec:req_seq_excl")
        user_f = json.loads(data["user_features_json"])
        assert "click_seq" not in user_f

    def test_does_not_raise_on_redis_error(self, fake_redis):
        """snapshot_writer must be best-effort — never propagate exceptions."""
        # Pass None as Redis client to trigger an AttributeError inside the writer
        uf = self._make_user_features()
        try:
            write_snapshot("req_err", "U000001", uf, [], [], r=None)
        except Exception as exc:
            pytest.fail(f"snapshot_writer raised unexpectedly: {exc}")

"""
test_tools.py
-------------
Tests for agent/tools.py: tool function implementations and TOOL_SPECS format.

All tests use fakeredis — no real Redis or LLM required.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import json
import pytest
import fakeredis

from agent.tools import (
    TOOL_SPECS,
    dispatch_tool,
    get_feature_contributions,
    get_rec_snapshot,
    get_user_features,
    get_user_sequence,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _seed_snapshot(fr, req_id="req_test01", uid="U000001",
                   scores=None, user_features=None):
    """Write a snapshot hash to fakeredis."""
    if scores is None:
        scores = [
            {"item_id": "I0000001", "score": 0.9,
             "feature_contributions": {"conversion_rate": 0.3, "category_match": 0.4},
             "item_brand": "Nike", "item_price": 199.0, "category_id": 1},
            {"item_id": "I0000002", "score": 0.7,
             "feature_contributions": {"conversion_rate": 0.2, "category_match": 0.3},
             "item_brand": "Adidas", "item_price": 149.0, "category_id": 2},
        ]
    if user_features is None:
        user_features = {"uid": uid, "total_pv": 10, "total_click": 5}

    fr.hset(f"snapshot:rec:{req_id}", mapping={
        "uid": uid,
        "ts": "1720000000000",
        "user_features_json": json.dumps(user_features, ensure_ascii=False),
        "candidates_json": json.dumps(["I0000001", "I0000002"], ensure_ascii=False),
        "scores_json": json.dumps(scores, ensure_ascii=False),
    })


# ── get_rec_snapshot ──────────────────────────────────────────────────────────

class TestGetRecSnapshot:
    def test_returns_correct_dict(self, fake_redis):
        _seed_snapshot(fake_redis)
        result = get_rec_snapshot("req_test01", rc=fake_redis)

        assert result["uid"] == "U000001"
        assert result["ts"] == "1720000000000"
        assert isinstance(result["user_features"], dict)
        assert isinstance(result["candidates"], list)
        assert len(result["scores"]) == 2

    def test_key_not_exists_returns_empty(self, fake_redis):
        result = get_rec_snapshot("req_nonexistent", rc=fake_redis)
        assert result == {}


# ── get_feature_contributions ─────────────────────────────────────────────────

class TestGetFeatureContributions:
    def test_returns_contributions_for_item(self, fake_redis):
        _seed_snapshot(fake_redis)
        result = get_feature_contributions("req_test01", "I0000001", rc=fake_redis)

        assert result["item_id"] == "I0000001"
        assert "conversion_rate" in result["feature_contributions"]

    def test_item_not_in_snapshot_returns_empty(self, fake_redis):
        _seed_snapshot(fake_redis)
        result = get_feature_contributions("req_test01", "I9999999", rc=fake_redis)
        assert result == {}

    def test_snapshot_not_exists_returns_empty(self, fake_redis):
        result = get_feature_contributions("req_nonexistent", "I0000001", rc=fake_redis)
        assert result == {}


# ── get_user_sequence ─────────────────────────────────────────────────────────

class TestGetUserSequence:
    def test_returns_ts_desc_order(self, fake_redis):
        fake_redis.zadd("seq:click:U000001", {"I0000001": 100, "I0000002": 300, "I0000003": 200})
        result = get_user_sequence("U000001", rc=fake_redis)

        assert result["uid"] == "U000001"
        # ZREVRANGE returns by score descending
        assert result["sequence"] == ["I0000002", "I0000003", "I0000001"]

    def test_empty_sequence(self, fake_redis):
        result = get_user_sequence("U_NOSEQ", rc=fake_redis)
        assert result["uid"] == "U_NOSEQ"
        assert result["sequence"] == []


# ── get_user_features ─────────────────────────────────────────────────────────

class TestGetUserFeatures:
    def test_returns_complete_feature_dict(self, fake_redis):
        fake_redis.hset("feat:user:U000001", mapping={
            "total_pv": "10", "total_click": "5", "total_cart": "2",
            "total_fav": "1", "total_buy": "0", "active_days": "3",
            "user_age": "25", "user_city": "上海", "user_level": "2",
        })
        result = get_user_features("U000001", rc=fake_redis)

        assert result["uid"] == "U000001"
        assert result["total_pv"] == "10"
        assert result["user_city"] == "上海"

    def test_missing_user_returns_uid_only(self, fake_redis):
        result = get_user_features("U_NOFEAT", rc=fake_redis)
        assert result == {"uid": "U_NOFEAT"}


# ── TOOL_SPECS JSON Schema format validation ──────────────────────────────────

class TestToolSpecsFormat:
    def test_all_specs_have_type_object_parameters(self):
        """Each TOOL_SPEC must have parameters.type == 'object' (ChatGLM requirement)."""
        for spec in TOOL_SPECS:
            func = spec["function"]
            params = func["parameters"]
            assert params["type"] == "object", (
                f"Tool '{func['name']}' parameters.type must be 'object', "
                f"got '{params.get('type')}'"
            )
            assert "properties" in params, (
                f"Tool '{func['name']}' missing 'properties' in parameters"
            )
            assert "required" in params, (
                f"Tool '{func['name']}' missing 'required' in parameters"
            )

    def test_all_required_fields_exist_in_properties(self):
        """Each field listed in 'required' must appear in 'properties'."""
        for spec in TOOL_SPECS:
            func = spec["function"]
            params = func["parameters"]
            props = set(params["properties"].keys())
            for req_field in params.get("required", []):
                assert req_field in props, (
                    f"Tool '{func['name']}': required field '{req_field}' "
                    f"not in properties"
                )


# ── dispatch_tool ──────────────────────────────────────────────────────────────

class TestDispatchTool:
    def test_dispatch_unknown_tool_returns_error(self, fake_redis):
        result_json = dispatch_tool("nonexistent_tool", {}, rc=fake_redis)
        result = json.loads(result_json)
        assert "error" in result

    def test_dispatch_get_user_features(self, fake_redis):
        fake_redis.hset("feat:user:U000001", mapping={"total_pv": "10"})
        result_json = dispatch_tool("get_user_features", {"uid": "U000001"}, rc=fake_redis)
        result = json.loads(result_json)
        assert result["uid"] == "U000001"

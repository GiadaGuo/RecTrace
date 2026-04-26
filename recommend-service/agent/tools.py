"""
tools.py
--------
Tool functions and TOOL_SPECS for the Agent's Function Calling layer.

Each tool reads from Redis (data written by Flink jobs / snapshot_writer).
TOOL_SPECS follow JSON Schema format with `type: object` wrapping — required
by ChatGLM Function Calling (flat parameter specs won't trigger tool calls).

Redis keys used:
  snapshot:rec:{req_id}   Hash  — snapshot_writer.py
  feat:user:{uid}         Hash  — UserFeatureFunction (Job3)
  seq:click:{uid}         ZSet  — SequenceFeatureJob (Job4)
"""

import json
import logging
from typing import Any

import redis

from .lineage import run_lineage_trace

logger = logging.getLogger(__name__)


# ── TOOL_SPECS (JSON Schema, type:object wrapped) ──────────────────────────────

TOOL_SPECS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "get_rec_snapshot",
            "description": (
                "获取某次推荐请求的特征快照，包括当时的用户特征、候选商品列表和各候选品得分。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "req_id": {
                        "type": "string",
                        "description": "推荐请求ID，格式 req_xxx",
                    },
                },
                "required": ["req_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_feature_contributions",
            "description": (
                "获取某推荐请求中特定商品的特征贡献分，用于解释该商品得分的来源。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "req_id": {
                        "type": "string",
                        "description": "推荐请求ID，格式 req_xxx",
                    },
                    "item_id": {
                        "type": "string",
                        "description": "商品ID，格式 Ixxxxxxx",
                    },
                },
                "required": ["req_id", "item_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_lineage_trace",
            "description": (
                "从推荐结果反查特征与行为序列来源，追溯某商品被推荐的完整决策路径。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "uid": {
                        "type": "string",
                        "description": "用户ID，格式 Uxxxxxx",
                    },
                    "item_id": {
                        "type": "string",
                        "description": "商品ID，格式 Ixxxxxxx",
                    },
                },
                "required": ["uid", "item_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_user_sequence",
            "description": (
                "获取用户的点击行为序列，按时间戳降序排列。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "uid": {
                        "type": "string",
                        "description": "用户ID，格式 Uxxxxxx",
                    },
                },
                "required": ["uid"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_user_features",
            "description": (
                "获取用户的实时特征，包括统计指标和画像属性。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "uid": {
                        "type": "string",
                        "description": "用户ID，格式 Uxxxxxx",
                    },
                },
                "required": ["uid"],
            },
        },
    },
]


# ── Tool implementations ──────────────────────────────────────────────────────


def get_rec_snapshot(req_id: str, rc: redis.Redis) -> dict[str, Any]:
    """
    Retrieve a recommendation snapshot by req_id.

    Returns the parsed snapshot dict, or empty dict if not found.
    Never raises.
    """
    try:
        raw = rc.hgetall(f"snapshot:rec:{req_id}")
        if not raw:
            return {}
        return {
            "uid": raw.get("uid", ""),
            "ts": raw.get("ts", ""),
            "user_features": json.loads(raw.get("user_features_json", "{}")),
            "candidates": json.loads(raw.get("candidates_json", "[]")),
            "scores": json.loads(raw.get("scores_json", "[]")),
        }
    except Exception as e:
        logger.warning("[tools] get_rec_snapshot failed req_id=%s: %s", req_id, e)
        return {}


def get_feature_contributions(req_id: str, item_id: str, rc: redis.Redis) -> dict[str, Any]:
    """
    Extract feature contributions for a specific item from a snapshot.

    Returns {item_id, feature_contributions} or empty dict.
    Never raises.
    """
    try:
        raw = rc.hgetall(f"snapshot:rec:{req_id}")
        if not raw:
            return {}
        scores = json.loads(raw.get("scores_json", "[]"))
        for entry in scores:
            if entry.get("item_id") == item_id:
                return {
                    "item_id": item_id,
                    "feature_contributions": entry.get("feature_contributions", {}),
                }
        return {}
    except Exception as e:
        logger.warning(
            "[tools] get_feature_contributions failed req_id=%s item_id=%s: %s",
            req_id, item_id, e,
        )
        return {}


def get_user_sequence(uid: str, rc: redis.Redis) -> dict[str, Any]:
    """
    Fetch the user's click behaviour sequence, ordered by timestamp descending.

    Returns {uid, sequence: [item_id, ...]} or {uid, sequence: []}.
    Never raises.
    """
    try:
        seq = rc.zrevrange(f"seq:click:{uid}", 0, -1)
        return {"uid": uid, "sequence": list(seq)}
    except Exception as e:
        logger.warning("[tools] get_user_sequence failed uid=%s: %s", uid, e)
        return {"uid": uid, "sequence": []}


def get_user_features(uid: str, rc: redis.Redis) -> dict[str, Any]:
    """
    Fetch the user's real-time features from Redis.

    Returns the full feature dict, or empty dict if not found.
    Never raises.
    """
    try:
        data = rc.hgetall(f"feat:user:{uid}")
        if not data:
            return {"uid": uid}
        result = dict(data)
        result["uid"] = uid
        return result
    except Exception as e:
        logger.warning("[tools] get_user_features failed uid=%s: %s", uid, e)
        return {"uid": uid}


def _run_lineage_trace_wrapper(uid: str, item_id: str, rc: redis.Redis) -> dict[str, Any]:
    """Thin wrapper so run_lineage_trace has the same (..., rc) signature as other tools."""
    return run_lineage_trace(uid, item_id, rc)


# ── Tool function name → function mapping ─────────────────────────────────────

def _tool_map() -> dict[str, callable]:
    """Return mapping from tool name to callable."""
    return {
        "get_rec_snapshot": get_rec_snapshot,
        "get_feature_contributions": get_feature_contributions,
        "run_lineage_trace": _run_lineage_trace_wrapper,
        "get_user_sequence": get_user_sequence,
        "get_user_features": get_user_features,
    }


TOOL_FUNCTIONS: dict[str, callable] = _tool_map()


def dispatch_tool(name: str, args: dict, rc: redis.Redis) -> str:
    """
    Execute a named tool function with the given arguments and Redis client.

    Returns the JSON-serialised result string (for inclusion as a ToolMessage).
    Returns '{"error": "..."}' on unknown tool name.
    """
    fn = TOOL_FUNCTIONS.get(name)
    if fn is None:
        return json.dumps({"error": f"Unknown tool: {name}"}, ensure_ascii=False)
    try:
        result = fn(**args, rc=rc)
        return json.dumps(result, ensure_ascii=False, default=str)
    except Exception as e:
        logger.warning("[tools] dispatch_tool %s failed: %s", name, e)
        return json.dumps({"error": str(e)}, ensure_ascii=False)

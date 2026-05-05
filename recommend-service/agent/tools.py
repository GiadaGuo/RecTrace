"""
tools.py
--------
Tool implementations for the Agent's Function Calling layer.

All tools register themselves with the module-level ``registry`` singleton
(tool_registry.py) at import time.  The orchestrator calls
``registry.get_definitions()`` and ``registry.dispatch()`` — no manual
import list needed.

Toolsets:
  "user"      — get_user_features, get_user_sequence
  "diagnose"  — get_rec_snapshot, get_feature_contributions,
                run_lineage_trace, diagnose_recommendation_chain
  "olap"      — get_ranking_trace, get_filter_reason_stats, get_user_rec_quality
  "health"    — get_pipeline_health, get_feature_freshness

Redis keys used:
  snapshot:rec:{req_id}          Hash  — snapshot_writer.py
  feat:user:{uid}                Hash  — UserFeatureFunction (Job3)
  seq:click:{uid}                ZSet  — SequenceFeatureJob (Job4)
  ranking_trace:{req_id}         Hash  — RankingLogJob (Step 6, future)
  feat:user:update_ts:{uid}      String — feature freshness timestamp

ClickHouse (olap toolset) — queried via HTTP API, connection config from env.
Flink REST API (health toolset) — connection config from env.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import redis

from .lineage import run_lineage_trace as _lineage_impl
from .tool_registry import registry

logger = logging.getLogger(__name__)


# ── helpers ───────────────────────────────────────────────────────────────────

def _clickhouse_query(sql: str) -> list[dict]:
    """
    Execute a ClickHouse query via HTTP interface.

    Returns a list of row dicts. Returns [] on error (so callers can handle
    missing ClickHouse gracefully during development).
    """
    host = os.environ.get("CLICKHOUSE_HOST", "localhost")
    port = os.environ.get("CLICKHOUSE_PORT", "8123")
    url = f"http://{host}:{port}/"
    try:
        import urllib.request
        payload = (sql + " FORMAT JSON").encode()
        req = urllib.request.Request(url, data=payload, method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            return data.get("data", [])
    except Exception as e:
        logger.warning("[tools] ClickHouse query failed: %s", e)
        return []


def _flink_rest(path: str) -> dict:
    """
    Call the Flink REST API.

    Returns the parsed JSON response, or {"error": str} on failure.
    """
    host = os.environ.get("FLINK_JOBMANAGER_HOST", "localhost")
    port = os.environ.get("FLINK_JOBMANAGER_PORT", "8081")
    url = f"http://{host}:{port}{path}"
    try:
        import urllib.request
        with urllib.request.urlopen(url, timeout=5) as resp:
            return json.loads(resp.read())
    except Exception as e:
        logger.warning("[tools] Flink REST call %s failed: %s", path, e)
        return {"error": str(e)}


# ── "user" toolset ─────────────────────────────────────────────────────────────


def _get_user_features(args: dict, **ctx) -> str:
    rc: redis.Redis = ctx["rc"]
    uid = args["uid"]
    try:
        data = rc.hgetall(f"feat:user:{uid}")
        result = dict(data) if data else {}
        result["uid"] = uid
        return json.dumps(result, ensure_ascii=False, default=str)
    except Exception as e:
        logger.warning("[tools] get_user_features uid=%s: %s", uid, e)
        return json.dumps({"uid": uid, "error": str(e)}, ensure_ascii=False)


registry.register(
    name="get_user_features",
    toolset="user",
    schema={
        "name": "get_user_features",
        "description": "获取用户的实时特征，包括统计指标和画像属性。",
        "parameters": {
            "type": "object",
            "properties": {
                "uid": {"type": "string", "description": "用户ID，格式 Uxxxxxx"},
            },
            "required": ["uid"],
        },
    },
    handler=_get_user_features,
)


def _get_user_sequence(args: dict, **ctx) -> str:
    rc: redis.Redis = ctx["rc"]
    uid = args["uid"]
    try:
        seq = rc.zrevrange(f"seq:click:{uid}", 0, -1)
        return json.dumps({"uid": uid, "sequence": list(seq)}, ensure_ascii=False)
    except Exception as e:
        logger.warning("[tools] get_user_sequence uid=%s: %s", uid, e)
        return json.dumps({"uid": uid, "sequence": [], "error": str(e)}, ensure_ascii=False)


registry.register(
    name="get_user_sequence",
    toolset="user",
    schema={
        "name": "get_user_sequence",
        "description": "获取用户的点击行为序列，按时间戳降序排列。",
        "parameters": {
            "type": "object",
            "properties": {
                "uid": {"type": "string", "description": "用户ID，格式 Uxxxxxx"},
            },
            "required": ["uid"],
        },
    },
    handler=_get_user_sequence,
)


# ── "diagnose" toolset ─────────────────────────────────────────────────────────


def _get_rec_snapshot(args: dict, **ctx) -> str:
    rc: redis.Redis = ctx["rc"]
    req_id = args["req_id"]
    try:
        raw = rc.hgetall(f"snapshot:rec:{req_id}")
        if not raw:
            return json.dumps({}, ensure_ascii=False)
        result = {
            "uid": raw.get("uid", ""),
            "ts": raw.get("ts", ""),
            "user_features": json.loads(raw.get("user_features_json", "{}")),
            "candidates": json.loads(raw.get("candidates_json", "[]")),
            "scores": json.loads(raw.get("scores_json", "[]")),
        }
        return json.dumps(result, ensure_ascii=False, default=str)
    except Exception as e:
        logger.warning("[tools] get_rec_snapshot req_id=%s: %s", req_id, e)
        return json.dumps({"error": str(e)}, ensure_ascii=False)


registry.register(
    name="get_rec_snapshot",
    toolset="diagnose",
    schema={
        "name": "get_rec_snapshot",
        "description": "获取某次推荐请求的特征快照，包括当时的用户特征、候选商品列表和各候选品得分。",
        "parameters": {
            "type": "object",
            "properties": {
                "req_id": {"type": "string", "description": "推荐请求ID，格式 req_xxx"},
            },
            "required": ["req_id"],
        },
    },
    handler=_get_rec_snapshot,
)


def _get_feature_contributions(args: dict, **ctx) -> str:
    rc: redis.Redis = ctx["rc"]
    req_id = args["req_id"]
    item_id = args["item_id"]
    try:
        raw = rc.hgetall(f"snapshot:rec:{req_id}")
        if not raw:
            return json.dumps({}, ensure_ascii=False)
        scores = json.loads(raw.get("scores_json", "[]"))
        for entry in scores:
            if entry.get("item_id") == item_id:
                return json.dumps(
                    {"item_id": item_id, "feature_contributions": entry.get("feature_contributions", {})},
                    ensure_ascii=False,
                )
        return json.dumps({"item_id": item_id, "feature_contributions": {}}, ensure_ascii=False)
    except Exception as e:
        logger.warning("[tools] get_feature_contributions req_id=%s item_id=%s: %s", req_id, item_id, e)
        return json.dumps({"error": str(e)}, ensure_ascii=False)


registry.register(
    name="get_feature_contributions",
    toolset="diagnose",
    schema={
        "name": "get_feature_contributions",
        "description": "获取某推荐请求中特定商品的特征贡献分，用于解释该商品得分的来源。",
        "parameters": {
            "type": "object",
            "properties": {
                "req_id": {"type": "string", "description": "推荐请求ID，格式 req_xxx"},
                "item_id": {"type": "string", "description": "商品ID，格式 Ixxxxxxx"},
            },
            "required": ["req_id", "item_id"],
        },
    },
    handler=_get_feature_contributions,
)


def _run_lineage_trace(args: dict, **ctx) -> str:
    rc: redis.Redis = ctx["rc"]
    uid = args["uid"]
    item_id = args["item_id"]
    try:
        result = _lineage_impl(uid, item_id, rc)
        return json.dumps(result, ensure_ascii=False, default=str)
    except Exception as e:
        logger.warning("[tools] run_lineage_trace uid=%s item_id=%s: %s", uid, item_id, e)
        return json.dumps({"error": str(e)}, ensure_ascii=False)


registry.register(
    name="run_lineage_trace",
    toolset="diagnose",
    schema={
        "name": "run_lineage_trace",
        "description": "从推荐结果反查特征与行为序列来源，追溯某商品被推荐的完整决策路径。",
        "parameters": {
            "type": "object",
            "properties": {
                "uid": {"type": "string", "description": "用户ID，格式 Uxxxxxx"},
                "item_id": {"type": "string", "description": "商品ID，格式 Ixxxxxxx"},
            },
            "required": ["uid", "item_id"],
        },
    },
    handler=_run_lineage_trace,
)


def _diagnose_recommendation_chain(args: dict, **ctx) -> str:
    """
    Comprehensive diagnostic tool: combines snapshot, lineage, feature freshness,
    and pipeline health into a single structured report.
    """
    rc: redis.Redis = ctx["rc"]
    req_id = args["req_id"]

    report: dict[str, Any] = {"req_id": req_id}

    # 1. Snapshot
    snapshot_str = _get_rec_snapshot({"req_id": req_id}, **ctx)
    snapshot = json.loads(snapshot_str)
    report["snapshot"] = snapshot

    # 2. Lineage (if uid available from snapshot)
    uid = snapshot.get("uid", "")
    if uid and snapshot.get("candidates"):
        top_item = snapshot["candidates"][0] if isinstance(snapshot["candidates"], list) else ""
        if top_item:
            lineage_str = _run_lineage_trace({"uid": uid, "item_id": top_item}, **ctx)
            report["lineage_top_item"] = json.loads(lineage_str)

    # 3. Feature freshness
    if uid:
        freshness_str = _get_feature_freshness({"uid": uid}, **ctx)
        report["feature_freshness"] = json.loads(freshness_str)

    # 4. Pipeline health
    health_str = _get_pipeline_health({}, **ctx)
    report["pipeline_health"] = json.loads(health_str)

    return json.dumps(report, ensure_ascii=False, default=str)


registry.register(
    name="diagnose_recommendation_chain",
    toolset="diagnose",
    schema={
        "name": "diagnose_recommendation_chain",
        "description": (
            "综合诊断工具：串联快照→特征溯源→特征时效→链路健康，输出完整请求诊断报告。"
            "当需要全面诊断某次推荐请求时优先使用此工具。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "req_id": {"type": "string", "description": "推荐请求ID，格式 req_xxx"},
            },
            "required": ["req_id"],
        },
    },
    handler=_diagnose_recommendation_chain,
)


# ── "olap" toolset ─────────────────────────────────────────────────────────────


def _get_ranking_trace(args: dict, **ctx) -> str:
    """
    Query ClickHouse ranking_trace table for a specific request.
    Returns stage-level item counts and filter reasons.
    """
    req_id = args["req_id"]
    sql = (
        f"SELECT stage, count() AS item_count, groupArray(filter_reason) AS reasons "
        f"FROM ranking_trace WHERE req_id = '{req_id}' GROUP BY stage ORDER BY stage"
    )
    rows = _clickhouse_query(sql)
    return json.dumps({"req_id": req_id, "stages": rows}, ensure_ascii=False, default=str)


registry.register(
    name="get_ranking_trace",
    toolset="olap",
    schema={
        "name": "get_ranking_trace",
        "description": (
            "查询 ClickHouse ranking_trace 表，返回某请求在召回→粗排→精排各 stage "
            "的物料数量和过滤原因。需要 ClickHouse 服务可用（Step 6 之后）。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "req_id": {"type": "string", "description": "推荐请求ID，格式 req_xxx"},
            },
            "required": ["req_id"],
        },
    },
    handler=_get_ranking_trace,
)


def _get_filter_reason_stats(args: dict, **ctx) -> str:
    """Aggregate filter reason distribution for a user over a time range."""
    uid = args["uid"]
    start_ts = args.get("start_ts", 0)
    end_ts = args.get("end_ts", int(time.time()))
    sql = (
        f"SELECT stage, filter_reason, count() AS cnt "
        f"FROM ranking_trace "
        f"WHERE uid = '{uid}' AND ts >= {start_ts} AND ts <= {end_ts} "
        f"GROUP BY stage, filter_reason ORDER BY cnt DESC LIMIT 50"
    )
    rows = _clickhouse_query(sql)
    return json.dumps({"uid": uid, "filter_stats": rows}, ensure_ascii=False, default=str)


registry.register(
    name="get_filter_reason_stats",
    toolset="olap",
    schema={
        "name": "get_filter_reason_stats",
        "description": "统计指定用户近期各排序 stage 的过滤原因分布，用于诊断哪类商品被系统过滤。",
        "parameters": {
            "type": "object",
            "properties": {
                "uid": {"type": "string", "description": "用户ID，格式 Uxxxxxx"},
                "start_ts": {"type": "integer", "description": "起始时间戳（Unix 秒），默认 0"},
                "end_ts": {"type": "integer", "description": "结束时间戳（Unix 秒），默认当前时间"},
            },
            "required": ["uid"],
        },
    },
    handler=_get_filter_reason_stats,
)


def _get_user_rec_quality(args: dict, **ctx) -> str:
    """Query recommendation quality metrics (CTR, cart rate, purchase rate) for a user."""
    uid = args["uid"]
    sql = (
        f"SELECT "
        f"  countIf(click_label=1) / count() AS ctr, "
        f"  countIf(engage_label=1) / count() AS engage_rate, "
        f"  countIf(buy_label=1) / count() AS buy_rate, "
        f"  count() AS total_impressions "
        f"FROM ranking_trace WHERE uid = '{uid}'"
    )
    rows = _clickhouse_query(sql)
    metrics = rows[0] if rows else {}
    return json.dumps({"uid": uid, "rec_quality": metrics}, ensure_ascii=False, default=str)


registry.register(
    name="get_user_rec_quality",
    toolset="olap",
    schema={
        "name": "get_user_rec_quality",
        "description": "综合评估指定用户的推荐质量趋势：CTR、加购率、购买转化率。",
        "parameters": {
            "type": "object",
            "properties": {
                "uid": {"type": "string", "description": "用户ID，格式 Uxxxxxx"},
            },
            "required": ["uid"],
        },
    },
    handler=_get_user_rec_quality,
)


# ── "health" toolset ───────────────────────────────────────────────────────────


def _get_pipeline_health(args: dict, **ctx) -> str:
    """
    Check real-time pipeline health:
    1. Flink job states via REST API
    2. Kafka consumer group lag (Flink's internal metrics)
    """
    health: dict[str, Any] = {"timestamp": int(time.time())}

    # Flink jobs
    flink_data = _flink_rest("/jobs/overview")
    if "error" in flink_data:
        health["flink"] = {"status": "unreachable", "error": flink_data["error"]}
    else:
        jobs = flink_data.get("jobs", [])
        running = [j for j in jobs if j.get("state") == "RUNNING"]
        failed = [j for j in jobs if j.get("state") in ("FAILED", "FAILING")]
        health["flink"] = {
            "total_jobs": len(jobs),
            "running": len(running),
            "failed": len(failed),
            "jobs": [{"name": j.get("name"), "state": j.get("state")} for j in jobs],
        }

    # Kafka consumer group lag from Flink metrics (best-effort)
    # Flink exposes kafka consumer lag via its metrics REST endpoint
    metrics_data = _flink_rest("/jobs/metrics?get=numRecordsIn")
    health["flink_metrics_available"] = "error" not in metrics_data

    return json.dumps(health, ensure_ascii=False, default=str)


registry.register(
    name="get_pipeline_health",
    toolset="health",
    schema={
        "name": "get_pipeline_health",
        "description": (
            "检查实时链路健康状态：Flink Job 运行状态（通过 Flink REST API）。"
            "当怀疑数据链路异常、特征停止更新时调用。"
        ),
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    handler=_get_pipeline_health,
)


def _get_feature_freshness(args: dict, **ctx) -> str:
    """
    Check how stale a user's features are by inspecting the update_ts
    stored alongside user features in Redis.
    """
    rc: redis.Redis = ctx["rc"]
    uid = args["uid"]
    try:
        # update_ts stored as a field in the user feature hash
        raw_ts = rc.hget(f"feat:user:{uid}", "update_ts")
        now = int(time.time())
        if raw_ts is None:
            return json.dumps({
                "uid": uid,
                "update_ts": None,
                "age_seconds": None,
                "status": "unknown",
            }, ensure_ascii=False)

        update_ts = int(float(raw_ts))
        age = now - update_ts
        # Mark as stale if older than 10 minutes
        status = "fresh" if age < 600 else "stale"
        return json.dumps({
            "uid": uid,
            "update_ts": update_ts,
            "age_seconds": age,
            "status": status,
        }, ensure_ascii=False)
    except Exception as e:
        logger.warning("[tools] get_feature_freshness uid=%s: %s", uid, e)
        return json.dumps({"uid": uid, "error": str(e)}, ensure_ascii=False)


registry.register(
    name="get_feature_freshness",
    toolset="health",
    schema={
        "name": "get_feature_freshness",
        "description": (
            "检查 Redis 中用户特征的 update_ts 距当前时间差，判断特征是否过期（超过10分钟视为 stale）。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "uid": {"type": "string", "description": "用户ID，格式 Uxxxxxx"},
            },
            "required": ["uid"],
        },
    },
    handler=_get_feature_freshness,
)


# ── Legacy compatibility (kept for existing tests) ────────────────────────────
# The old TOOL_SPECS and dispatch_tool are superseded by the registry but
# are re-exported so existing tests don't break.

TOOL_SPECS: list[dict] = registry.get_definitions()


def dispatch_tool(name: str, args: dict, rc: redis.Redis) -> str:
    """Legacy shim — delegates to registry.dispatch."""
    return registry.dispatch(name, args, rc=rc)


# Old tests import these as module-level functions that return dicts (not JSON).
# These shims decode the JSON from the registry handler.

def get_rec_snapshot(req_id: str, *, rc: redis.Redis) -> dict:
    return json.loads(_get_rec_snapshot({"req_id": req_id}, rc=rc))


def get_feature_contributions(req_id: str, item_id: str, *, rc: redis.Redis) -> dict:
    return json.loads(_get_feature_contributions({"req_id": req_id, "item_id": item_id}, rc=rc))


def get_user_features(uid: str, *, rc: redis.Redis) -> dict:
    return json.loads(_get_user_features({"uid": uid}, rc=rc))


def get_user_sequence(uid: str, *, rc: redis.Redis) -> dict:
    return json.loads(_get_user_sequence({"uid": uid}, rc=rc))

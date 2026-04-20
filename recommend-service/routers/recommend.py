"""
recommend.py
------------
POST /recommend      — fetch features, run RuleModel, write snapshot, return ranked list
POST /recommend/inject — directly update Redis counters to simulate a Flink-processed event
"""

import logging
import time
import uuid
from typing import Optional

import redis
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from feature.feature_fetcher import (
    _get_redis,
    _hgetall_safe,
    build_candidate_pool,
    fetch_user_features,
)
from feature.snapshot_writer import write_snapshot
from models.rule_model import RuleModel

router = APIRouter()
logger = logging.getLogger(__name__)
_model = RuleModel()


# ── Dependency ────────────────────────────────────────────────────────────────

def get_redis_client() -> redis.Redis:
    return _get_redis()


# ── Request / Response schemas ────────────────────────────────────────────────

class RecommendRequest(BaseModel):
    uid: str
    top_k: int = Field(default=10, ge=1, le=100)


class InjectRequest(BaseModel):
    uid: str
    item_id: str
    bhv_type: str  # supported values: "click" | "fav" | "cart" | "buy"


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/recommend")
def recommend(req: RecommendRequest, r: redis.Redis = Depends(get_redis_client)):
    req_id = f"req_{uuid.uuid4().hex[:8]}"

    # 1. Build candidate pool
    candidates = build_candidate_pool(req.uid, req.top_k, r=r)

    # 2. Score & rank
    scored = _model.predict(req.uid, candidates, r=r)
    top_items = scored[: req.top_k]

    # 3. Write snapshot (best-effort, non-blocking)——尝试写入但不阻塞，可能不保证数据持久化或成功
    user_features = fetch_user_features(req.uid, r=r)
    scores_payload = [s.to_dict() for s in top_items]
    write_snapshot(
        req_id=req_id,
        uid=req.uid,
        user_features=user_features,
        candidates=candidates,
        scores=scores_payload,
        r=r,
    )

    return {
        "req_id": req_id,
        "uid": req.uid,
        "items": scores_payload,
    }


@router.post("/recommend/inject")
def inject(req: InjectRequest, r: redis.Redis = Depends(get_redis_client)):
    """
    Directly update Redis counters to simulate a real-time behaviour event
    processed by Flink.  This lets callers observe ranking changes without
    waiting for an actual Flink pipeline run.

    Supported bhv_type values and their Redis effects:
      click  → feat:user:{uid}.total_click++
               feat:item:{item_id}.total_detail_click++
               seq:click:{uid}: ZADD ts item_id + ZREMRANGEBYRANK (keep 50)
      fav    → feat:user:{uid}.total_fav++
               feat:item:{item_id}.total_fav++
      cart   → feat:user:{uid}.total_cart++
               feat:item:{item_id}.total_cart++
      buy    → feat:user:{uid}.total_buy++
               feat:item:{item_id}.total_buy++
               Recompute conversion_rate = total_buy / total_detail_click
    """
    uid = req.uid
    item_id = req.item_id
    bhv_type = req.bhv_type
    ts_ms = int(time.time() * 1000)

    user_key = f"feat:user:{uid}"
    item_key = f"feat:item:{item_id}"

    try:
        pipe = r.pipeline()

        if bhv_type == "click":
            pipe.hincrby(user_key, "total_click", 1)
            pipe.hincrby(item_key, "total_detail_click", 1)
            # Update click sequence
            seq_key = f"seq:click:{uid}"
            pipe.zadd(seq_key, {item_id: ts_ms})
            pipe.zremrangebyrank(seq_key, 0, -51)   # keep newest 50：移除有序集合中按排名（score 升序排列）位于 【start.stop] 区间内的所有成员
            pipe.expire(seq_key, 7 * 86400)

        elif bhv_type == "fav":
            pipe.hincrby(user_key, "total_fav", 1)
            pipe.hincrby(item_key, "total_fav", 1)

        elif bhv_type == "cart":
            pipe.hincrby(user_key, "total_cart", 1)
            pipe.hincrby(item_key, "total_cart", 1)

        elif bhv_type == "buy":
            pipe.hincrby(user_key, "total_buy", 1)
            pipe.hincrby(item_key, "total_buy", 1)

        pipe.execute()

        # Recompute conversion_rate for buy (outside pipeline as it needs current values)
        if bhv_type == "buy":
            item_data = _hgetall_safe(r, item_key)
            total_buy = int(item_data.get("total_buy", 0))
            total_dc  = int(item_data.get("total_detail_click", 0))
            cvr = total_buy / total_dc if total_dc > 0 else 0.0
            r.hset(item_key, "conversion_rate", f"{cvr:.6f}")

    except Exception as e:
        logger.warning("inject failed uid=%s item_id=%s bhv_type=%s — %s", uid, item_id, bhv_type, e)
        # inject is best-effort; don't fail the caller

    return {"status": "ok", "uid": uid, "item_id": item_id, "bhv_type": bhv_type}

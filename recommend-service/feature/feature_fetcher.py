"""
feature_fetcher.py
------------------
Reads all required features from Redis for a given (uid, item_id) pair.

Redis key layout (written by Flink jobs):
  feat:user:{uid}             Hash  — UserFeatureFunction  (Job3)
  feat:user:stat:{uid}        Hash  — UserWindowStatFunction (Job3)
  feat:item:{item_id}         Hash  — ItemFeatureFunction   (Job3)
  feat:item:stat:{item_id}    Hash  — ItemWindowStatFunction (Job3)
  cross:{uid}:{cat_id}        Hash  — CrossFeatureFunction  (Job3)
  feat:user_item:{uid}:{cat}  Hash  — WindowAggJob          (Job2)
  seq:click:{uid}             ZSet  — SequenceFeatureJob    (Job4)
  seq:cart:{uid}              ZSet  — SequenceFeatureJob    (Job4)
  seq:buy:{uid}               ZSet  — SequenceFeatureJob    (Job4)
  dim:item:{item_id}          Hash  — init_dim_data.py (fallback for cold items)
"""

import logging
import os
import random
from typing import List, Optional

import redis

logger = logging.getLogger(__name__)

from .feature_schema import (
    CrossFeatures,
    FeatureBundle,
    ItemFeatures,
    UserFeatures,
    UserItemWindowFeatures,
)

# Redis connection config via environment variables: REDIS_HOST (default: localhost), REDIS_PORT (default: 6379)
_REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
_REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

# Sequence fetch length for ranking
_SEQ_FETCH_LEN = 20
# Candidate pool multiplier
_POOL_MULTIPLIER = 5
# Max SCAN count per iteration when sampling dim keys
_SCAN_COUNT = 200


def _get_redis() -> redis.Redis:
    return redis.Redis(host=_REDIS_HOST, port=_REDIS_PORT, decode_responses=True)


# ── Low-level helpers ─────────────────────────────────────────────────────────

def _hgetall_safe(r: redis.Redis, key: str) -> dict:
    try:
        return r.hgetall(key) or {}
    except Exception as e:
        logger.warning("[feature_fetcher] HGETALL failed for key=%s: %s", key, e)
        return {}


def _int(d: dict, k: str, default: int = 0) -> int:
    try:
        return int(d.get(k, default))
    except (ValueError, TypeError):
        return default


def _float(d: dict, k: str, default: float = 0.0) -> float:
    try:
        v = d.get(k)
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_user_features(uid: str, r: Optional[redis.Redis] = None) -> UserFeatures:
    """Fetch all user-level features from Redis.  Never raises."""
    rc = r or _get_redis()

    uf = _hgetall_safe(rc, f"feat:user:{uid}")
    stat = _hgetall_safe(rc, f"feat:user:stat:{uid}")

    # Recent click sequence (ZSet, newest first)
    try:
        seq = rc.zrevrange(f"seq:click:{uid}", 0, _SEQ_FETCH_LEN - 1)
    except Exception as e:
        logger.warning("Failed to read seq:click:%s — %s", uid, e)
        seq = []

    return UserFeatures(
        uid=uid,
        total_pv=_int(uf, "total_pv"),
        total_click=_int(uf, "total_click"),
        total_cart=_int(uf, "total_cart"),
        total_fav=_int(uf, "total_fav"),
        total_buy=_int(uf, "total_buy"),
        active_days=_int(uf, "active_days"),
        user_age=_int(uf, "user_age"),
        user_city=uf.get("user_city", ""),
        user_level=_int(uf, "user_level"),
        click_5min=_float(stat, "click_5min"),
        click_1h=_float(stat, "click_1h"),
        pv_5min=_float(stat, "pv_5min"),
        pv_1h=_float(stat, "pv_1h"),
        click_seq=list(seq),
    )


def fetch_item_features(item_id: str, r: Optional[redis.Redis] = None) -> ItemFeatures:
    """Fetch all item-level features from Redis.  Never raises."""
    rc = r or _get_redis()

    itf = _hgetall_safe(rc, f"feat:item:{item_id}")
    stat = _hgetall_safe(rc, f"feat:item:stat:{item_id}")

    # Cold-item fallback: read dim:item:{item_id} for brand/price/category
    if not itf:
        dim = _hgetall_safe(rc, f"dim:item:{item_id}")
        category_id = _int(dim, "category_id")
        item_brand = dim.get("brand", "")
        item_price = _float(dim, "price")
    else:
        category_id = _int(itf, "category_id")
        item_brand = itf.get("item_brand", "")
        item_price = _float(itf, "item_price")

    return ItemFeatures(
        item_id=item_id,
        total_detail_click=_int(itf, "total_detail_click"),
        total_buy=_int(itf, "total_buy"),
        total_cart=_int(itf, "total_cart"),
        total_fav=_int(itf, "total_fav"),
        uv=_int(itf, "uv"),
        conversion_rate=_float(itf, "conversion_rate"),
        category_id=category_id,
        item_brand=item_brand,
        item_price=item_price,
        click_5min=_float(stat, "click_5min"),
        click_1h=_float(stat, "click_1h"),
    )


def fetch_cross_features(uid: str, cat_id: int, r: Optional[redis.Redis] = None) -> CrossFeatures:
    """Fetch user × category cross features (exponential-decay counts)."""
    rc = r or _get_redis()
    d = _hgetall_safe(rc, f"cross:{uid}:{cat_id}")
    return CrossFeatures(
        pv_cnt=_float(d, "pv_cnt"),
        click_cnt=_float(d, "click_cnt"),
        cart_cnt=_float(d, "cart_cnt"),
        buy_cnt=_float(d, "buy_cnt"),
    )


def fetch_user_item_window(uid: str, cat_id: int, r: Optional[redis.Redis] = None) -> UserItemWindowFeatures:
    """Fetch home-channel 1-min window features for (user, category)."""
    rc = r or _get_redis()
    d = _hgetall_safe(rc, f"feat:user_item:{uid}:{cat_id}")
    return UserItemWindowFeatures(
        show_cnt=_int(d, "show_cnt"),
        click_cnt=_int(d, "click_cnt"),
        cart_cnt=_int(d, "cart_cnt"),
        buy_cnt=_int(d, "buy_cnt"),
    )


def fetch_feature_bundle(uid: str, item_id: str, r: Optional[redis.Redis] = None) -> FeatureBundle:
    """Assemble the full feature bundle for one (user, item) pair."""
    rc = r or _get_redis()
    user = fetch_user_features(uid, r=rc)
    item = fetch_item_features(item_id, r=rc)
    cross = fetch_cross_features(uid, item.category_id, r=rc)
    ui_window = fetch_user_item_window(uid, item.category_id, r=rc)
    return FeatureBundle(user=user, item=item, cross=cross, user_item_window=ui_window)


def build_candidate_pool(uid: str, top_k: int, r: Optional[redis.Redis] = None) -> List[str]:
    """
    Build a candidate item list for ranking.

    Strategy:
      1. Take items from the user's click / cart / buy history sequences.
      2. Supplement with randomly sampled dim:item:* keys up to top_k * POOL_MULTIPLIER.
      Deduplication is applied; order is preserved (history first).
    """
    rc = r or _get_redis()
    target_size = top_k * _POOL_MULTIPLIER

    seen: set = set()
    candidates: List[str] = []

    def _add(item_id: str):
        if item_id and item_id not in seen:
            seen.add(item_id)
            candidates.append(item_id)

    # Pull from behavioral sequences
    for seq_key in (f"seq:click:{uid}", f"seq:cart:{uid}", f"seq:buy:{uid}"):
        try:
            items = rc.zrevrange(seq_key, 0, _SEQ_FETCH_LEN - 1)
            for iid in items:
                _add(iid)
        except Exception:
            pass  # best-effort; partial seq failure is acceptable

    # Supplement with random dim:item:* keys
    if len(candidates) < target_size:
        needed = target_size - len(candidates)
        try:
            sampled: List[str] = []
            cursor = 0
            iterations = 0
            while len(sampled) < needed * 3 and iterations < 20:
                cursor, keys = rc.scan(cursor, match="dim:item:*", count=_SCAN_COUNT)
                for k in keys:
                    # key format: "dim:item:I0000001"
                    iid = k.split(":", 2)[-1]
                    if iid not in seen:
                        sampled.append(iid)
                if cursor == 0:
                    break
                iterations += 1
            random.shuffle(sampled)
            for iid in sampled[:needed]:
                _add(iid)
        except Exception as e:
            logger.warning("SCAN dim:item:* failed, candidate pool may be small — %s", e)

    return candidates[:target_size]

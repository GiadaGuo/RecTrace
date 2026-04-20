"""
snapshot_writer.py
------------------
Writes a recommendation snapshot to Redis for later traceability by the Agent.

Key format : snapshot:rec:{req_id}  (Hash, TTL 86400s / 24 hours)
Fields     : uid, ts, user_features_json, candidates_json, scores_json
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional

import redis

from .feature_schema import UserFeatures

_TTL_SECONDS = 86_400  # 24 hours

logger = logging.getLogger(__name__)


def write_snapshot(
    req_id: str,
    uid: str,
    user_features: UserFeatures,
    candidates: List[str],
    scores: List[Dict[str, Any]],
    r: Optional[redis.Redis] = None,
) -> None:
    """
    Persist a recommendation snapshot.

    Parameters
    ----------
    req_id         : unique request id (e.g. "req_a1b2c3d4")
    uid            : user id
    user_features  : UserFeatures dataclass (serialised as JSON)
    candidates     : full candidate item_id list before ranking
    scores         : ranked list of {"item_id", "score", "feature_contributions"}
    r              : optional Redis client (injected for testing)
    """
    from feature.feature_fetcher import _get_redis  # avoid circular at module level

    rc = r or _get_redis()
    key = f"snapshot:rec:{req_id}"

    payload = {
        "uid": uid,
        "ts": str(int(time.time() * 1000)),
        "user_features_json": json.dumps(
            {k: v for k, v in user_features.__dict__.items() if k != "click_seq"},
            ensure_ascii=False,
        ),
        "candidates_json": json.dumps(candidates, ensure_ascii=False),
        "scores_json": json.dumps(scores, ensure_ascii=False),
    }

    try:
        pipe = rc.pipeline()
        pipe.hset(key, mapping=payload)
        pipe.expire(key, _TTL_SECONDS)
        pipe.execute()
    except Exception as e:
        logger.warning("Snapshot write failed for req_id=%s — %s", req_id, e)
        # snapshot is best-effort; never block the recommend response

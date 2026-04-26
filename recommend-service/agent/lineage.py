"""
lineage.py
----------
Core logic for run_lineage_trace: given a uid and item_id, reconstruct
the recommendation decision path by combining:
  - The most recent rec snapshot that includes the item
  - The user's click sequence at snapshot time

Redis keys used:
  snapshot:rec:{req_id}   Hash  — written by snapshot_writer.py
  seq:click:{uid}         ZSet  — written by SequenceFeatureJob (Job4)
"""

import json
import logging
from typing import Any

import redis

logger = logging.getLogger(__name__)

# Number of behaviour items to include in the lineage path
_SEQ_FETCH_LEN = 10


def run_lineage_trace(uid: str, item_id: str, rc: redis.Redis) -> dict[str, Any]:
    """
    Trace the full recommendation decision path for a (uid, item_id) pair.

    Steps
    -----
    1. Scan snapshot:rec:* keys to find the most recent snapshot for `uid`
       that contains `item_id` in its scored results.
    2. Extract feature_contributions for that item from the snapshot.
    3. Fetch the user's recent click sequence from seq:click:{uid}.
    4. Return a combined dict; return empty dict sections on missing data.

    Never raises — caller always gets a dict (may be empty on data absence).
    """
    result: dict[str, Any] = {
        "uid": uid,
        "item_id": item_id,
        "req_id": None,
        "ts": None,
        "seq_items": [],
        "contributing_features": {},
    }

    # ── Step 1 & 2: Find the most recent snapshot containing item_id ──────────
    try:
        best_snapshot: dict | None = None
        best_ts: int = -1

        cursor = 0
        while True:
            cursor, keys = rc.scan(cursor, match="snapshot:rec:*", count=100)
            for key in keys:
                try:
                    raw = rc.hgetall(key)
                    if not raw:
                        continue
                    # Filter by uid
                    if raw.get("uid") != uid:
                        continue
                    scores_raw = raw.get("scores_json")
                    if not scores_raw:
                        continue
                    scores = json.loads(scores_raw)
                    matched = [s for s in scores if s.get("item_id") == item_id]
                    if not matched:
                        continue
                    ts_val = int(raw.get("ts", 0))
                    if ts_val > best_ts:
                        best_ts = ts_val
                        best_snapshot = {
                            "key": key,
                            "req_id": key.split(":", 2)[-1],
                            "ts": ts_val,
                            "score_entry": matched[0],
                        }
                except Exception as inner_e:
                    logger.warning("[lineage] Error parsing key %s: %s", key, inner_e)
            if cursor == 0:
                break

        if best_snapshot:
            result["req_id"] = best_snapshot["req_id"]
            result["ts"] = best_snapshot["ts"]
            result["contributing_features"] = best_snapshot["score_entry"].get(
                "feature_contributions", {}
            )
    except Exception as e:
        logger.warning("[lineage] Snapshot scan failed for uid=%s item=%s: %s", uid, item_id, e)

    # ── Step 3: Fetch recent click sequence ───────────────────────────────────
    try:
        seq = rc.zrevrange(f"seq:click:{uid}", 0, _SEQ_FETCH_LEN - 1)
        result["seq_items"] = list(seq)
    except Exception as e:
        logger.warning("[lineage] Failed to read seq:click:%s: %s", uid, e)

    return result

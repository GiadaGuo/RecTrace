"""
memory/session_memory.py
------------------------
Short-term memory: stores per-session conversation summaries in Redis.

Redis key:  agent:{user_id}:{session_id}:summary  (STRING, TTL 24h)

The session summary is a concise LLM-generated text block that captures
the current conversation context.  It is injected into the system prompt
at the start of each request to give the model continuity.

During Phase 1 (single-user) the summary is generated lazily on ``save()``.
A real summarisation LLM call is only made if the full history exceeds the
configured token threshold (SUMMARY_THRESHOLD_CHARS).
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import redis

from .provider import MemoryProvider

logger = logging.getLogger(__name__)

_REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
_TTL_SECONDS = int(os.environ.get("SESSION_MEMORY_TTL", str(24 * 3600)))
_SUMMARY_THRESHOLD_CHARS = int(os.environ.get("SUMMARY_THRESHOLD_CHARS", "2000"))


def _build_key(user_id: str, session_id: str) -> str:
    return f"agent:{user_id}:{session_id}:summary"


class SessionMemory(MemoryProvider):
    """
    Short-term Redis-backed session summary.

    Parameters
    ----------
    user_id    : str
    session_id : str
    rc         : redis.Redis — optional shared client; created internally if None
    """

    def __init__(
        self,
        user_id: str,
        session_id: str,
        rc: Optional[redis.Redis] = None,
    ) -> None:
        super().__init__(user_id, session_id)
        self._rc = rc or redis.Redis.from_url(_REDIS_URL, decode_responses=True)
        self._key = _build_key(user_id, session_id)

    # ── MemoryProvider interface ───────────────────────────────────────────────

    def load(self, query: Optional[str] = None) -> str:
        """Return the stored session summary, or empty string."""
        try:
            value = self._rc.get(self._key)
            return value or ""
        except Exception as e:
            logger.warning("[SessionMemory] load failed: %s", e)
            return ""

    def save(self, content: str, metadata: Optional[dict] = None) -> None:
        """Store a session summary (with TTL refresh)."""
        if not content.strip():
            return
        try:
            self._rc.set(self._key, content, ex=_TTL_SECONDS)
        except Exception as e:
            logger.warning("[SessionMemory] save failed: %s", e)

    def clear(self) -> None:
        try:
            self._rc.delete(self._key)
        except Exception as e:
            logger.warning("[SessionMemory] clear failed: %s", e)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def append_turn(self, role: str, content: str) -> None:
        """
        Append a conversation turn to a raw history key, then refresh TTL.

        History key:  agent:{user_id}:{session_id}:history  (LIST)
        Each element is "role|content".
        """
        history_key = f"agent:{self.user_id}:{self.session_id}:history"
        entry = f"{role}|{content}"
        try:
            self._rc.rpush(history_key, entry)
            self._rc.expire(history_key, _TTL_SECONDS)
        except Exception as e:
            logger.warning("[SessionMemory] append_turn failed: %s", e)

    def get_history(self) -> list[dict]:
        """Return the raw turn list as [{"role": ..., "content": ...}]."""
        history_key = f"agent:{self.user_id}:{self.session_id}:history"
        try:
            entries = self._rc.lrange(history_key, 0, -1)
            result = []
            for e in entries:
                if "|" in e:
                    role, _, content = e.partition("|")
                    result.append({"role": role, "content": content})
            return result
        except Exception as e:
            logger.warning("[SessionMemory] get_history failed: %s", e)
            return []

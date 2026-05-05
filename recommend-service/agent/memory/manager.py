"""
memory/manager.py
-----------------
MemoryManager orchestrates the three memory layers and produces the unified
context block injected into the system prompt.

Layers (in injection order):
  1. SessionMemory  — recent session summary (short-term, Redis)
  2. ChromaMemory   — semantically relevant past cases (long-term, ChromaDB)
  3. SkillMemory    — matching skill playbooks (from SKILL.md files)

Usage::

    mm = MemoryManager(user_id="default", session_id="abc123", rc=redis_client)
    context = mm.get_context(query="用户特征过期")
    # → formatted string block, ready for system prompt injection

    mm.save_session_summary("本次诊断了用户U001的推荐链路，特征过期问题已定位。")
    mm.save_case("用户U001特征过期导致推荐质量下降，解决方案：...", tags=["freshness"])
"""

from __future__ import annotations

import logging
from typing import Optional

import redis

from .session_memory import SessionMemory
from .chroma_memory import ChromaMemory
from .skill_memory import SkillMemory

logger = logging.getLogger(__name__)


class MemoryManager:
    """
    Unified entry point for all memory operations.

    Parameters
    ----------
    user_id    : str
    session_id : str
    rc         : redis.Redis — optional, forwarded to SessionMemory
    """

    def __init__(
        self,
        user_id: str,
        session_id: str,
        rc: Optional[redis.Redis] = None,
    ) -> None:
        self.user_id = user_id
        self.session_id = session_id
        self._session = SessionMemory(user_id, session_id, rc=rc)
        self._chroma = ChromaMemory(user_id, session_id)
        self._skill = SkillMemory(user_id, session_id)

    # ── Context assembly ──────────────────────────────────────────────────────

    def get_context(self, query: Optional[str] = None) -> str:
        """
        Build the memory context block for injection into the system prompt.

        Loads all three layers and concatenates non-empty results.
        Returns empty string if all layers are empty.
        """
        blocks: list[str] = []

        session_text = self._session.load(query)
        if session_text:
            blocks.append(f"## 当前会话摘要\n{session_text}")

        try:
            chroma_text = self._chroma.load(query)
            if chroma_text:
                blocks.append(chroma_text)
        except Exception as e:
            logger.debug("[MemoryManager] ChromaDB load skipped: %s", e)

        try:
            skill_text = self._skill.load(query)
            if skill_text:
                blocks.append(skill_text)
        except Exception as e:
            logger.debug("[MemoryManager] SkillMemory load skipped: %s", e)

        return "\n\n".join(blocks)

    # ── Session layer ─────────────────────────────────────────────────────────

    def save_session_summary(self, summary: str) -> None:
        """Persist a session summary to Redis."""
        self._session.save(summary)

    def append_turn(self, role: str, content: str) -> None:
        """Append a conversation turn to the session history."""
        self._session.append_turn(role, content)

    def get_history(self) -> list[dict]:
        """Return the current session turn history."""
        return self._session.get_history()

    # ── Long-term layer ───────────────────────────────────────────────────────

    def save_case(self, content: str, metadata: Optional[dict] = None) -> None:
        """
        Store a diagnostic case in ChromaDB for long-term semantic retrieval.
        Silently skips if ChromaDB is unavailable.
        """
        try:
            self._chroma.save(content, metadata)
        except Exception as e:
            logger.debug("[MemoryManager] ChromaDB save skipped: %s", e)

    # ── Skill layer ───────────────────────────────────────────────────────────

    def save_skill(
        self,
        name: str,
        description: str,
        body: str,
        tags: list[str] | None = None,
    ) -> None:
        """Write a user-specific skill playbook file."""
        self._skill.save_user_skill(name, description, body, tags)

    def list_skills(self) -> list[dict]:
        """Return metadata for all available skills."""
        return self._skill.list_skills()

    # ── Cleanup ───────────────────────────────────────────────────────────────

    def clear_session(self) -> None:
        """Clear short-term session memory."""
        self._session.clear()

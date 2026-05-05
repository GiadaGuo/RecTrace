"""
memory/provider.py
------------------
Abstract base class for all memory provider implementations.

Each concrete provider covers one "layer" of the three-layer memory system:
  SessionMemory  — short-term, per-session, stored in Redis
  ChromaMemory   — long-term semantic, stored in ChromaDB
  SkillMemory    — reusable skill/solution store, loaded from SKILL.md files

All providers share the same interface so MemoryManager can operate them
uniformly.  The user_id + session_id pair is passed at construction time and
stored as instance attributes for scoped storage access.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional


class MemoryProvider(ABC):
    """
    Abstract base for memory providers.

    Parameters
    ----------
    user_id    : str  — namespace for user-level storage
    session_id : str  — namespace for session-level storage
    """

    def __init__(self, user_id: str, session_id: str) -> None:
        self.user_id = user_id
        self.session_id = session_id

    # ── Required interface ────────────────────────────────────────────────────

    @abstractmethod
    def load(self, query: Optional[str] = None) -> str:
        """
        Load memory contents relevant to ``query`` (or all contents if None).

        Returns a human-readable string block ready to inject into the system
        prompt context.  Returns empty string if nothing is stored.
        """
        ...

    @abstractmethod
    def save(self, content: str, metadata: Optional[dict] = None) -> None:
        """Persist ``content`` to this memory layer."""
        ...

    @abstractmethod
    def clear(self) -> None:
        """Remove all memory for the current user / session scope."""
        ...

    # ── Optional hook ─────────────────────────────────────────────────────────

    def close(self) -> None:
        """Release any open connections / resources.  Override if needed."""

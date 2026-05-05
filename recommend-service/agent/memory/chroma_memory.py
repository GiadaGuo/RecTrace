"""
memory/chroma_memory.py
-----------------------
Long-term semantic memory: stores diagnostic cases and insights in ChromaDB.

Collection naming:
  agent_{user_id}_cases  — per-user case history (long-term)

Each document stores a diagnostic session summary with metadata:
  session_id, user_id, timestamp, req_id (if applicable), tags

Phase 1: single-user, collection is "agent_default_cases".
Phase 3: collection per user_id for tenant isolation.

ChromaDB connection configured via env:
  CHROMA_HOST  (default: localhost)
  CHROMA_PORT  (default: 8000)
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Optional

from .provider import MemoryProvider

logger = logging.getLogger(__name__)

_CHROMA_HOST = os.environ.get("CHROMA_HOST", "localhost")
_CHROMA_PORT = int(os.environ.get("CHROMA_PORT", "8000"))
_TOP_K = int(os.environ.get("CHROMA_MEMORY_TOP_K", "3"))


def _get_client():
    """Lazy import chromadb to keep it optional during development."""
    import chromadb
    return chromadb.HttpClient(host=_CHROMA_HOST, port=_CHROMA_PORT)


class ChromaMemory(MemoryProvider):
    """
    Long-term semantic memory backed by ChromaDB.

    Stores full diagnostic session summaries as documents.
    On ``load(query)``, performs a semantic similarity search for the top-k
    most relevant past cases to include as context.
    """

    def __init__(self, user_id: str, session_id: str) -> None:
        super().__init__(user_id, session_id)
        self._collection_name = f"agent_{user_id}_cases"
        self._client = None
        self._collection = None

    # ── Lazy init ─────────────────────────────────────────────────────────────

    def _ensure_collection(self):
        if self._collection is not None:
            return
        try:
            client = _get_client()
            self._collection = client.get_or_create_collection(
                name=self._collection_name,
                metadata={"hnsw:space": "cosine"},
            )
            self._client = client
        except Exception as e:
            logger.warning("[ChromaMemory] cannot connect to ChromaDB: %s", e)
            raise

    # ── MemoryProvider interface ───────────────────────────────────────────────

    def load(self, query: Optional[str] = None) -> str:
        """
        Semantic search for past cases relevant to ``query``.
        Returns formatted string with top-k results, or empty string on error.
        """
        try:
            self._ensure_collection()
        except Exception:
            return ""

        if self._collection.count() == 0:
            return ""

        if not query:
            # Return the most recent cases (no semantic filter)
            results = self._collection.get(
                limit=_TOP_K,
                include=["documents", "metadatas"],
            )
        else:
            results = self._collection.query(
                query_texts=[query],
                n_results=min(_TOP_K, self._collection.count()),
                include=["documents", "metadatas", "distances"],
            )

        docs = results.get("documents", [])
        # query returns nested list, get returns flat list
        if docs and isinstance(docs[0], list):
            docs = docs[0]

        if not docs:
            return ""

        lines = ["## 相关历史诊断案例"]
        for i, doc in enumerate(docs, 1):
            lines.append(f"\n### 案例 {i}\n{doc}")
        return "\n".join(lines)

    def save(self, content: str, metadata: Optional[dict] = None) -> None:
        """Store a new diagnostic case document."""
        if not content.strip():
            return
        try:
            self._ensure_collection()
        except Exception:
            return

        doc_id = str(uuid.uuid4())
        meta = {
            "user_id": self.user_id,
            "session_id": self.session_id,
            "timestamp": int(time.time()),
        }
        if metadata:
            meta.update(metadata)

        try:
            self._collection.add(
                documents=[content],
                ids=[doc_id],
                metadatas=[meta],
            )
        except Exception as e:
            logger.warning("[ChromaMemory] save failed: %s", e)

    def clear(self) -> None:
        """Delete all documents for this user's collection."""
        try:
            self._ensure_collection()
            client = _get_client()
            client.delete_collection(self._collection_name)
            self._collection = None
        except Exception as e:
            logger.warning("[ChromaMemory] clear failed: %s", e)

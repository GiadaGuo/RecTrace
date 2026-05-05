"""
feedback_loop.py
----------------
Session retrospective and trajectory storage for the learning loop.

Responsibilities:
  1. Record each tool call event during the session as a trajectory step.
  2. On session end (``close()``), write the full trajectory to a JSONL file.
  3. Generate a session summary and save it to MemoryManager (short-term + long-term).
  4. Call SkillExtractor to check if a reusable skill can be distilled.
  5. Expose ``on_rl_reward(reward)`` hook — Phase 3 RLHF expansion point.

Trajectory format (JSONL, one JSON object per line):
  {
    "user_id": "default",
    "session_id": "abc123",
    "timestamp": 1715000000,
    "conversations": [                        # ShareGPT format
      {"from": "human", "value": "..."},
      {"from": "gpt",   "value": "..."},
      ...
    ],
    "tool_calls": [                           # structured tool call log
      {"name": "...", "args": {...}, "result": "...", "ts": 1715000001}
    ],
    "reward": null,                           # filled in by on_rl_reward() for RLHF
    "skill_extracted": null                   # skill name if SkillExtractor wrote one
  }

Storage path:
  TRAJECTORY_DIR / {user_id} / {session_id}.jsonl
  Default: ./data/trajectories/  (env TRAJECTORY_DIR)
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

_TRAJECTORY_DIR = Path(os.environ.get("TRAJECTORY_DIR", "./data/trajectories"))


class FeedbackLoop:
    """
    Per-session feedback loop recorder.

    Usage::

        loop = FeedbackLoop(user_id="default", session_id="abc123", rc=redis_client)
        loop.record_user_message("查询用户U001的特征")
        loop.record_tool_call("get_user_features", {"uid": "U001"}, '{"age": 25}')
        loop.record_assistant_message("该用户年龄25岁，特征正常。")
        loop.close()  # saves trajectory + triggers skill extraction

    Parameters
    ----------
    user_id    : str
    session_id : str
    rc         : redis.Redis — optional, forwarded to MemoryManager
    """

    def __init__(
        self,
        user_id: str,
        session_id: str,
        rc=None,
    ) -> None:
        self.user_id = user_id
        self.session_id = session_id
        self._rc = rc

        self._conversations: list[dict] = []   # ShareGPT format turns
        self._tool_calls: list[dict] = []       # structured tool call log
        self._reward: Optional[float] = None
        self._closed = False
        self._start_ts = int(time.time())

    # ── Event recording ───────────────────────────────────────────────────────

    def record_user_message(self, content: str) -> None:
        self._conversations.append({"from": "human", "value": content})

    def record_assistant_message(self, content: str) -> None:
        self._conversations.append({"from": "gpt", "value": content})

    def record_tool_call(self, name: str, args: dict, result: str) -> None:
        """Record a tool invocation event."""
        self._tool_calls.append({
            "name": name,
            "args": args,
            "result": result[:500],   # truncate long results for storage efficiency
            "ts": int(time.time()),
        })
        # Mirror into conversations as a "function" turn for ShareGPT compatibility
        self._conversations.append({
            "from": "function_call",
            "value": json.dumps({"name": name, "args": args}, ensure_ascii=False),
        })
        self._conversations.append({
            "from": "observation",
            "value": result[:500],
        })

    # ── RL reward hook ────────────────────────────────────────────────────────

    def on_rl_reward(self, reward: float) -> None:
        """
        Set the reward signal for this trajectory.

        Phase 3 expansion point: veRL / TRL training pipelines can call this
        (e.g. from a human feedback API endpoint) to annotate the trajectory.
        The reward is written to the JSONL file and used during offline RL training.
        """
        self._reward = reward
        logger.debug("[FeedbackLoop] reward=%.3f set for session=%s", reward, self.session_id)

    # ── Session close ─────────────────────────────────────────────────────────

    def close(
        self,
        session_summary: Optional[str] = None,
        *,
        extract_skill: bool = True,
        save_to_memory: bool = True,
    ) -> dict:
        """
        Finalise the session: save trajectory, update memory, extract skill.

        Returns the trajectory dict for inspection / testing.
        """
        if self._closed:
            logger.debug("[FeedbackLoop] already closed, skipping")
            return {}

        self._closed = True
        skill_name: Optional[str] = None

        # 1. Build session summary if not provided
        if not session_summary:
            session_summary = self._auto_summary()

        # 2. Skill extraction
        if extract_skill and self._tool_calls:
            try:
                from .skill_extractor import SkillExtractor
                extractor = SkillExtractor(self.user_id, self.session_id)
                skill = extractor.extract_skills(session_summary, self._tool_calls)
                if skill:
                    skill_name = skill.get("name")
            except Exception as e:
                logger.warning("[FeedbackLoop] skill extraction error: %s", e)

        # 3. Save to memory layers
        if save_to_memory and session_summary.strip():
            try:
                from .memory.manager import MemoryManager
                mm = MemoryManager(self.user_id, self.session_id, rc=self._rc)
                mm.save_session_summary(session_summary)
                # Save as a long-term case if there were tool calls (meaningful session)
                if self._tool_calls:
                    case_text = self._build_case_document(session_summary)
                    mm.save_case(case_text, metadata={"session_id": self.session_id})
            except Exception as e:
                logger.warning("[FeedbackLoop] memory save error: %s", e)

        # 4. Write trajectory JSONL
        trajectory = {
            "user_id": self.user_id,
            "session_id": self.session_id,
            "timestamp": self._start_ts,
            "conversations": self._conversations,
            "tool_calls": self._tool_calls,
            "reward": self._reward,
            "skill_extracted": skill_name,
            "summary": session_summary,
        }
        self._write_trajectory(trajectory)

        return trajectory

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _auto_summary(self) -> str:
        """Generate a minimal summary from recorded turns without LLM call."""
        human_turns = [t["value"] for t in self._conversations if t["from"] == "human"]
        tool_names = list({tc["name"] for tc in self._tool_calls})
        gpt_turns = [t["value"] for t in self._conversations if t["from"] == "gpt"]

        parts = []
        if human_turns:
            parts.append(f"用户问题：{human_turns[0][:100]}")
        if tool_names:
            parts.append(f"使用工具：{', '.join(tool_names)}")
        if gpt_turns:
            parts.append(f"最终回答摘要：{gpt_turns[-1][:150]}")
        return "；".join(parts) if parts else ""

    def _build_case_document(self, summary: str) -> str:
        """Build the long-term case document stored in ChromaDB."""
        tool_lines = []
        for tc in self._tool_calls:
            tool_lines.append(f"- {tc['name']}({json.dumps(tc['args'], ensure_ascii=False)})")
        tools_block = "\n".join(tool_lines) if tool_lines else "(无工具调用)"
        return (
            f"## 诊断案例\n\n**摘要：** {summary}\n\n"
            f"**调用工具：**\n{tools_block}\n\n"
            f"**会话ID：** {self.session_id}\n"
        )

    def _write_trajectory(self, trajectory: dict) -> None:
        """Append trajectory as a single JSON line to the JSONL file."""
        try:
            output_dir = _TRAJECTORY_DIR / self.user_id
            output_dir.mkdir(parents=True, exist_ok=True)
            filepath = output_dir / f"{self.session_id}.jsonl"
            with filepath.open("a", encoding="utf-8") as f:
                f.write(json.dumps(trajectory, ensure_ascii=False, default=str) + "\n")
            logger.info("[FeedbackLoop] trajectory written: %s", filepath)
        except Exception as e:
            logger.warning("[FeedbackLoop] trajectory write failed: %s", e)

    # ── Properties (read-only) ────────────────────────────────────────────────

    @property
    def turn_count(self) -> int:
        return len([t for t in self._conversations if t["from"] == "human"])

    @property
    def tool_call_count(self) -> int:
        return len(self._tool_calls)

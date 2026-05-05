"""
skill_extractor.py
------------------
Learning loop core: extracts reusable skill playbooks from completed Agent
trajectories and persists them to the user's skill directory.

Extraction strategy (lightweight, LLM-assisted):
  1. FeedbackLoop calls ``extract_skills()`` after a session ends.
  2. The extractor reads the session's trajectory (conversation turns + tool results).
  3. A compact LLM prompt asks the model to decide if a reusable pattern exists.
  4. If the model returns a skill, it is written to:
       agent/skills/users/{user_id}/{skill_name}.md
     via SkillMemory.save_user_skill().

The LLM call is *optional*: if the model is unavailable the extractor silently
skips skill extraction (no exception propagates to the caller).

RL expansion point: ``extract_skills()`` also returns the structured skill dict
so FeedbackLoop can include it in the trajectory JSONL for future RLHF training.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Optional

from .memory.skill_memory import SkillMemory
from .zhipu_client import invoke as llm_invoke

logger = logging.getLogger(__name__)


_EXTRACTION_PROMPT = """\
你是一个推荐系统专家，正在分析 AI 助手完成的一次诊断会话。

请判断这次会话是否包含可复用的诊断模式或解决方案。
如果有，用以下 JSON 格式输出一个技能条目（只返回 JSON，不要其他内容）：

{{
  "name": "技能名（英文下划线格式，唯一标识）",
  "description": "一句话描述（中文，< 50字）",
  "tags": ["tag1", "tag2"],
  "body": "完整的诊断步骤（Markdown 格式，包含工具调用示例）"
}}

如果没有可复用的模式，只输出：null

以下是会话摘要：
{session_summary}

会话中使用的工具调用序列：
{tool_calls_summary}
"""


class SkillExtractor:
    """
    Extracts reusable skill playbooks from agent trajectories.

    Parameters
    ----------
    user_id    : str
    session_id : str
    """

    def __init__(self, user_id: str, session_id: str) -> None:
        self.user_id = user_id
        self.session_id = session_id
        self._skill_memory = SkillMemory(user_id, session_id)

    def extract_skills(
        self,
        session_summary: str,
        tool_calls: list[dict],
        *,
        force: bool = False,
    ) -> Optional[dict]:
        """
        Analyse a completed session and extract a skill if a reusable pattern exists.

        Parameters
        ----------
        session_summary : str   — condensed description of what the session accomplished
        tool_calls      : list  — list of {"name": ..., "args": ..., "result": ...} dicts
        force           : bool  — skip the LLM call and always attempt to write (for testing)

        Returns the extracted skill dict, or None if no skill was found.
        """
        if not session_summary.strip():
            return None

        tool_calls_summary = self._format_tool_calls(tool_calls)

        if force:
            # In forced mode, construct a minimal skill from the summary
            return self._write_skill({
                "name": f"extracted_{self.session_id[:8]}",
                "description": session_summary[:50],
                "tags": ["auto_extracted"],
                "body": f"## 自动提取的诊断流程\n\n{session_summary}\n\n### 使用的工具\n{tool_calls_summary}",
            })

        # LLM-assisted extraction
        prompt = _EXTRACTION_PROMPT.format(
            session_summary=session_summary,
            tool_calls_summary=tool_calls_summary,
        )
        try:
            response = llm_invoke(
                [{"role": "user", "content": prompt}],
                tools=None,
            )
            raw_text = response.content if hasattr(response, "content") else str(response)
            skill_dict = self._parse_skill_json(raw_text)
            if skill_dict is None:
                logger.debug("[SkillExtractor] LLM decided no skill to extract")
                return None
            return self._write_skill(skill_dict)
        except Exception as e:
            logger.warning("[SkillExtractor] extraction failed (skipping): %s", e)
            return None

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _format_tool_calls(self, tool_calls: list[dict]) -> str:
        if not tool_calls:
            return "(无工具调用)"
        lines = []
        for i, tc in enumerate(tool_calls, 1):
            name = tc.get("name", "?")
            args = tc.get("args", {})
            result_preview = str(tc.get("result", ""))[:100]
            lines.append(f"{i}. {name}({json.dumps(args, ensure_ascii=False)}) → {result_preview}")
        return "\n".join(lines)

    def _parse_skill_json(self, text: str) -> Optional[dict]:
        """Extract JSON from the LLM response, handling markdown code blocks."""
        text = text.strip()
        if text.lower() == "null" or not text:
            return None
        # Strip markdown code fence if present
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
        try:
            data = json.loads(text.strip())
            if not isinstance(data, dict):
                return None
            # Validate required fields
            if not all(k in data for k in ("name", "description", "body")):
                return None
            return data
        except json.JSONDecodeError as e:
            logger.debug("[SkillExtractor] JSON parse error: %s | text=%s", e, text[:200])
            return None

    def _write_skill(self, skill_dict: dict) -> dict:
        """Write the skill to disk and return the dict."""
        self._skill_memory.save_user_skill(
            name=skill_dict["name"],
            description=skill_dict["description"],
            body=skill_dict["body"],
            tags=skill_dict.get("tags", []),
            version=skill_dict.get("version", 1),
        )
        logger.info("[SkillExtractor] saved skill '%s' for user=%s",
                    skill_dict["name"], self.user_id)
        return skill_dict

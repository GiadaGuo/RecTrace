"""
memory/skill_memory.py
----------------------
Skill memory: loads reusable diagnostic playbooks from SKILL.md files.

Directory layout:
  agent/skills/global/          — built-in skills (shipped with the service)
  agent/skills/users/{user_id}/ — user-specific skills extracted by SkillExtractor

Each skill file is a Markdown file with YAML frontmatter:

    ---
    name: diagnose_cold_start
    description: 诊断冷启动用户推荐效果差的标准流程
    tags: [cold_start, diagnose]
    version: 1
    ---

    ## 诊断步骤
    1. 调用 get_user_features 确认用户特征是否稀疏 ...

SkillMemory reads all relevant files and injects matching skill bodies
as context.  Matching is by tag overlap or keyword search on description.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Optional

from .provider import MemoryProvider

logger = logging.getLogger(__name__)

# Base directory: agent/skills/
_SKILLS_BASE = Path(__file__).parent.parent / "skills"


def _parse_frontmatter(text: str) -> tuple[dict, str]:
    """
    Split YAML frontmatter from Markdown body.
    Returns (metadata_dict, body_str).  Falls back to ({}, text) on parse error.
    """
    import yaml

    fm_match = re.match(r"^---\s*\n(.*?)\n---\s*\n(.*)", text, re.DOTALL)
    if not fm_match:
        return {}, text
    try:
        meta = yaml.safe_load(fm_match.group(1)) or {}
    except Exception:
        meta = {}
    body = fm_match.group(2).strip()
    return meta, body


def _load_skill_files(directory: Path) -> list[dict]:
    """Load all .md files in a directory as skill dicts."""
    skills = []
    if not directory.exists():
        return skills
    for path in sorted(directory.glob("*.md")):
        try:
            text = path.read_text(encoding="utf-8")
            meta, body = _parse_frontmatter(text)
            skills.append({
                "name": meta.get("name", path.stem),
                "description": meta.get("description", ""),
                "tags": meta.get("tags", []),
                "version": meta.get("version", 1),
                "body": body,
                "path": str(path),
            })
        except Exception as e:
            logger.warning("[SkillMemory] failed to load %s: %s", path, e)
    return skills


class SkillMemory(MemoryProvider):
    """
    Read-only memory provider that surfaces relevant skill playbooks.

    ``save()`` is a no-op — skills are written by SkillExtractor, not by
    MemoryManager directly.
    """

    def __init__(self, user_id: str, session_id: str) -> None:
        super().__init__(user_id, session_id)
        self._global_dir = _SKILLS_BASE / "global"
        self._user_dir = _SKILLS_BASE / "users" / user_id

    # ── MemoryProvider interface ───────────────────────────────────────────────

    def load(self, query: Optional[str] = None) -> str:
        """
        Load matching skill playbooks.

        If query is None, return all skills.
        If query is given, filter by tag overlap or keyword match on name/description.
        """
        global_skills = _load_skill_files(self._global_dir)
        user_skills = _load_skill_files(self._user_dir)
        all_skills = global_skills + user_skills

        if not all_skills:
            return ""

        if query:
            query_lower = query.lower()
            matched = []
            for skill in all_skills:
                tags_str = " ".join(skill.get("tags", []))
                searchable = f"{skill['name']} {skill['description']} {tags_str}".lower()
                if any(word in searchable for word in query_lower.split()):
                    matched.append(skill)
            if not matched:
                # Fall back to all skills if no match
                matched = all_skills
        else:
            matched = all_skills

        lines = ["## 可用技能手册"]
        for skill in matched:
            lines.append(f"\n### {skill['name']}\n{skill['description']}\n\n{skill['body']}")
        return "\n".join(lines)

    def save(self, content: str, metadata: Optional[dict] = None) -> None:
        """No-op: skills are written by SkillExtractor."""

    def clear(self) -> None:
        """No-op: do not delete skill files from MemoryManager."""

    # ── Helpers ───────────────────────────────────────────────────────────────

    def list_skills(self) -> list[dict]:
        """Return all skill metadata (without body text)."""
        all_skills = (
            _load_skill_files(self._global_dir) +
            _load_skill_files(self._user_dir)
        )
        return [
            {k: v for k, v in s.items() if k != "body"}
            for s in all_skills
        ]

    def save_user_skill(self, name: str, description: str, body: str,
                        tags: list[str] | None = None, version: int = 1) -> Path:
        """Write a skill file to the user-specific skills directory."""
        self._user_dir.mkdir(parents=True, exist_ok=True)
        tags_str = str(tags or [])
        content = (
            f"---\nname: {name}\ndescription: {description}\n"
            f"tags: {tags_str}\nversion: {version}\n---\n\n{body}\n"
        )
        path = self._user_dir / f"{name}.md"
        path.write_text(content, encoding="utf-8")
        return path

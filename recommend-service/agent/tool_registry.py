"""
tool_registry.py
----------------
Lightweight tool registry for the agent's function-calling layer.

Inspired by hermes-agent's tools/registry.py but stripped to the minimum
needed for this project:
  - No TTL caching on check_fn
  - No threading locks (single-process FastAPI service)
  - No auto-discovery (tools register themselves at import time)

Usage:
    from agent.tool_registry import registry

    registry.register(
        name="my_tool",
        toolset="my_toolset",
        schema={"name": "my_tool", "description": "...", "parameters": {...}},
        handler=lambda args, **ctx: json.dumps({"result": "..."}),
    )
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class ToolEntry:
    """Metadata for a single registered tool."""
    name: str
    toolset: str
    schema: dict          # OpenAI function schema (name/description/parameters)
    handler: Callable     # (args: dict, **ctx) -> str  (must return JSON string)
    description: str = ""


class ToolRegistry:
    """
    Central registry for all agent tools.

    ctx kwargs passed through dispatch:
      rc         — redis.Redis client
      user_id    — authenticated user id (default "default")
      session_id — current session id
    """

    def __init__(self) -> None:
        self._tools: dict[str, ToolEntry] = {}

    def register(
        self,
        name: str,
        toolset: str,
        schema: dict,
        handler: Callable,
        description: str = "",
    ) -> None:
        """Register a tool. Silently overwrites if name already exists."""
        self._tools[name] = ToolEntry(
            name=name,
            toolset=toolset,
            schema=schema,
            handler=handler,
            description=description or schema.get("description", ""),
        )
        logger.debug("[tool_registry] registered tool: %s (toolset=%s)", name, toolset)

    def dispatch(self, name: str, args: dict, **ctx: Any) -> str:
        """
        Execute a named tool.

        ctx is forwarded to the handler — handlers may pick up rc, user_id,
        session_id etc. as kwargs.

        Returns a JSON string. Returns '{"error": "..."}' on unknown name.
        """
        entry = self._tools.get(name)
        if entry is None:
            return json.dumps({"error": f"Unknown tool: {name}"}, ensure_ascii=False)
        try:
            result = entry.handler(args, **ctx)
            # Handlers must return str; coerce dict as a safety net
            if isinstance(result, (dict, list)):
                return json.dumps(result, ensure_ascii=False, default=str)
            return result
        except Exception as e:
            logger.warning("[tool_registry] dispatch %s failed: %s", name, e)
            return json.dumps({"error": str(e)}, ensure_ascii=False)

    def get_definitions(
        self,
        toolsets: Optional[list[str]] = None,
    ) -> list[dict]:
        """
        Return OpenAI-format tool definitions.

        toolsets — if provided, only include tools from these toolsets.
        """
        result = []
        for entry in self._tools.values():
            if toolsets and entry.toolset not in toolsets:
                continue
            result.append({"type": "function", "function": entry.schema})
        return result

    def get_toolset_names(self) -> list[str]:
        """Return sorted list of registered toolset names."""
        return sorted({e.toolset for e in self._tools.values()})

    def tool_names(self) -> list[str]:
        """Return sorted list of all registered tool names."""
        return sorted(self._tools.keys())

    def __contains__(self, name: str) -> bool:
        return name in self._tools


# Module-level singleton — imported by tools.py and other tool modules
registry = ToolRegistry()

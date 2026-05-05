"""
todo_tool.py
------------
In-memory task planning store for the Agent, plus tool registrations for the
LLM to manage its own task list during multi-step reasoning.

TodoStore holds a flat ordered list of TodoItem objects keyed by (user_id,
session_id).  Status transitions: pending → in_progress → completed | cancelled.

Tools registered (toolset "todo"):
  todo_add          — add a new task to the list
  todo_update       — update the status of an existing task
  todo_list         — return the current task list
  todo_clear        — clear all tasks for the current session (reset)
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field, asdict
from threading import Lock
from typing import ClassVar

from .tool_registry import registry

logger = logging.getLogger(__name__)


# ── Data model ────────────────────────────────────────────────────────────────

VALID_STATUSES = {"pending", "in_progress", "completed", "cancelled"}


@dataclass
class TodoItem:
    task_id: str
    content: str
    status: str = "pending"    # pending | in_progress | completed | cancelled
    priority: int = 0          # 0 = normal, -1 = low, 1 = high


# ── Store ─────────────────────────────────────────────────────────────────────

class TodoStore:
    """
    Keyed by (user_id, session_id).  Thread-safe at the store level.
    """

    _instances: ClassVar[dict[tuple[str, str], "TodoStore"]] = {}
    _class_lock: ClassVar[Lock] = Lock()

    def __init__(self) -> None:
        self._lock = Lock()
        self._tasks: list[TodoItem] = []

    # ── factory ──────────────────────────────────────────────────────────────

    @classmethod
    def for_session(cls, user_id: str, session_id: str) -> "TodoStore":
        key = (user_id, session_id)
        with cls._class_lock:
            if key not in cls._instances:
                cls._instances[key] = cls()
            return cls._instances[key]

    @classmethod
    def evict(cls, user_id: str, session_id: str) -> None:
        """Remove a store instance (e.g. when session ends)."""
        with cls._class_lock:
            cls._instances.pop((user_id, session_id), None)

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def add(self, content: str, priority: int = 0) -> TodoItem:
        item = TodoItem(
            task_id=str(uuid.uuid4())[:8],
            content=content,
            priority=priority,
        )
        with self._lock:
            self._tasks.append(item)
        return item

    def update(self, task_id: str, status: str) -> TodoItem | None:
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid status '{status}'. Valid: {VALID_STATUSES}")
        with self._lock:
            for item in self._tasks:
                if item.task_id == task_id:
                    item.status = status
                    return item
        return None

    def list_tasks(self, status_filter: str | None = None) -> list[TodoItem]:
        with self._lock:
            tasks = list(self._tasks)
        if status_filter:
            tasks = [t for t in tasks if t.status == status_filter]
        return tasks

    def clear(self) -> int:
        with self._lock:
            n = len(self._tasks)
            self._tasks.clear()
        return n

    def to_dict_list(self) -> list[dict]:
        return [asdict(t) for t in self.list_tasks()]


# ── Tool handlers ─────────────────────────────────────────────────────────────

def _todo_add(args: dict, **ctx) -> str:
    user_id = ctx.get("user_id", "default")
    session_id = ctx.get("session_id", "default")
    store = TodoStore.for_session(user_id, session_id)
    content = args["content"]
    priority = int(args.get("priority", 0))
    item = store.add(content, priority)
    logger.debug("[todo] add task_id=%s user=%s session=%s", item.task_id, user_id, session_id)
    return json.dumps({"task_id": item.task_id, "status": item.status}, ensure_ascii=False)


registry.register(
    name="todo_add",
    toolset="todo",
    schema={
        "name": "todo_add",
        "description": "向当前会话的任务列表中添加一条新任务，返回 task_id。",
        "parameters": {
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "任务描述"},
                "priority": {
                    "type": "integer",
                    "description": "优先级：1=高，0=普通，-1=低。默认 0。",
                },
            },
            "required": ["content"],
        },
    },
    handler=_todo_add,
)


def _todo_update(args: dict, **ctx) -> str:
    user_id = ctx.get("user_id", "default")
    session_id = ctx.get("session_id", "default")
    store = TodoStore.for_session(user_id, session_id)
    task_id = args["task_id"]
    status = args["status"]
    try:
        item = store.update(task_id, status)
    except ValueError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)
    if item is None:
        return json.dumps({"error": f"task_id '{task_id}' not found"}, ensure_ascii=False)
    return json.dumps({"task_id": item.task_id, "status": item.status}, ensure_ascii=False)


registry.register(
    name="todo_update",
    toolset="todo",
    schema={
        "name": "todo_update",
        "description": "更新某个任务的状态（pending/in_progress/completed/cancelled）。",
        "parameters": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string", "description": "由 todo_add 返回的任务 ID"},
                "status": {
                    "type": "string",
                    "enum": ["pending", "in_progress", "completed", "cancelled"],
                    "description": "新状态",
                },
            },
            "required": ["task_id", "status"],
        },
    },
    handler=_todo_update,
)


def _todo_list(args: dict, **ctx) -> str:
    user_id = ctx.get("user_id", "default")
    session_id = ctx.get("session_id", "default")
    store = TodoStore.for_session(user_id, session_id)
    status_filter = args.get("status")
    tasks = store.to_dict_list() if not status_filter else [
        t for t in store.to_dict_list() if t["status"] == status_filter
    ]
    return json.dumps({"tasks": tasks}, ensure_ascii=False)


registry.register(
    name="todo_list",
    toolset="todo",
    schema={
        "name": "todo_list",
        "description": "查看当前会话的任务列表，可按状态过滤。",
        "parameters": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["pending", "in_progress", "completed", "cancelled"],
                    "description": "按状态过滤，省略则返回全部。",
                },
            },
            "required": [],
        },
    },
    handler=_todo_list,
)


def _todo_clear(args: dict, **ctx) -> str:
    user_id = ctx.get("user_id", "default")
    session_id = ctx.get("session_id", "default")
    store = TodoStore.for_session(user_id, session_id)
    n = store.clear()
    return json.dumps({"cleared": n}, ensure_ascii=False)


registry.register(
    name="todo_clear",
    toolset="todo",
    schema={
        "name": "todo_clear",
        "description": "清空当前会话的全部任务（重置任务列表）。",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    handler=_todo_clear,
)

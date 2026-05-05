"""
orchestrator.py
---------------
LangGraph StateGraph that wires together the LLM (zhipu_client), the tool
registry, iteration budget, and memory context into a ReAct loop.

Graph topology:
  START → agent (call_model) ─┬─ tool_calls non-empty → tools ─→ agent
                              └─ tool_calls empty      → END

Changes from original:
  - Uses IterationBudget (env AGENT_MAX_ITERATIONS, default 30) instead of
    hardcoded recursion_limit=10.
  - AgentState carries user_id + session_id for tool context isolation.
  - Memory context block injected after SystemMessage at conversation start.
  - All tool dispatches pass user_id + session_id through registry.dispatch().
  - ``stream()`` and ``run()`` accept keyword args user_id / session_id.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Sequence

import redis
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage
from langgraph.graph import END, StateGraph
from langgraph.graph.message import MessagesState

from .iteration_budget import IterationBudget
from .prompts import (
    LLM_ERROR_TEMPLATE,
    RECURSION_LIMIT_MSG,
    SYSTEM_PROMPT,
    TOOL_CALL_PROGRESS_TEMPLATE,
    TOOL_RESULT_FALLBACK_TEMPLATE,
    TOOL_RESULT_HEADER_TEMPLATE,
    TOOL_RESULT_ITEM_TEMPLATE,
)
from .tool_registry import registry
# Side-effect imports: registers all tool + todo handlers onto registry at startup
from . import tools as _tools_module  # noqa: F401
from . import todo_tool as _todo_module  # noqa: F401
from .zhipu_client import invoke as llm_invoke, parse_tool_call

logger = logging.getLogger(__name__)


# ── State definition ──────────────────────────────────────────────────────────


class AgentState(MessagesState):
    """
    Extends MessagesState with runtime context for tool dispatch.

    rc         — Redis client
    user_id    — caller identity (Phase 1: always "default")
    session_id — conversation session UUID
    budget     — shared IterationBudget instance for this request
    """
    rc: redis.Redis
    user_id: str
    session_id: str
    budget: IterationBudget


# ── Memory context helper ─────────────────────────────────────────────────────


def _build_memory_block(user_id: str, session_id: str) -> str | None:
    """
    Try to load short-term memory context for the current session.
    Returns a formatted string to inject as the second message, or None.

    Importing MemoryManager here (lazy) avoids a circular import at module
    init time and makes the memory system optional during early development.
    """
    try:
        from .memory.manager import MemoryManager
        mm = MemoryManager(user_id=user_id, session_id=session_id)
        context = mm.get_context()
        return context if context else None
    except ImportError:
        return None
    except Exception as e:
        logger.debug("[orchestrator] memory context unavailable: %s", e)
        return None


# ── Node functions ────────────────────────────────────────────────────────────


def call_model(state: AgentState) -> dict:
    """
    Consume one iteration budget unit, then call the LLM.
    If the budget is exhausted, immediately return a forced-finish message.
    """
    budget: IterationBudget = state.get("budget") or IterationBudget()

    if not budget.consume():
        logger.warning("[orchestrator] IterationBudget exhausted (user=%s session=%s)",
                       state.get("user_id"), state.get("session_id"))
        return {"messages": [AIMessage(content=RECURSION_LIMIT_MSG)]}

    messages = list(state["messages"])
    user_id = state.get("user_id", "default")
    session_id = state.get("session_id", "default")

    # Inject system prompt (first position)
    if not messages or not isinstance(messages[0], SystemMessage):
        messages.insert(0, SystemMessage(content=SYSTEM_PROMPT))

    # Inject memory context block right after system prompt (once, at start)
    if len(messages) <= 2:
        mem_block = _build_memory_block(user_id, session_id)
        if mem_block:
            # Insert as an assistant message so it looks like prior knowledge
            messages.insert(1, AIMessage(content=mem_block))

    # Convert to OpenAI-compatible dicts for zai-sdk
    oai_messages = _lc_messages_to_oai(messages)

    # Build tool definitions (all toolsets)
    tool_defs = registry.get_definitions()

    try:
        ai_msg = llm_invoke(oai_messages, tools=tool_defs)
    except Exception as e:
        logger.warning("[orchestrator] LLM invoke failed: %s", e)
        return {"messages": [AIMessage(content=LLM_ERROR_TEMPLATE.format(e=e))]}

    # Build LangChain AIMessage with normalized tool_calls
    tool_calls_data = []
    if ai_msg.tool_calls:
        for tc in ai_msg.tool_calls:
            name, args = parse_tool_call(tc)
            tool_calls_data.append({
                "id": tc.id if hasattr(tc, "id") and tc.id else f"call_{len(tool_calls_data)}",
                "name": name,
                "args": args,
            })

    lc_ai = AIMessage(
        content=ai_msg.content or "",
        tool_calls=tool_calls_data,
    )
    lc_ai.additional_kwargs["_raw_tool_calls"] = ai_msg.tool_calls
    return {"messages": [lc_ai]}


def tool_node(state: AgentState) -> dict:
    """Execute all tool_calls from the last AIMessage and return ToolMessages."""
    last_ai: AIMessage = state["messages"][-1]
    rc: redis.Redis = state.get("rc")
    user_id: str = state.get("user_id", "default")
    session_id: str = state.get("session_id", "default")
    budget: IterationBudget | None = state.get("budget")

    tool_messages: list[ToolMessage] = []
    for tc in last_ai.tool_calls:
        tool_name = tc["name"]
        tool_args = tc["args"]
        tool_id = tc["id"]

        logger.debug("[orchestrator] executing tool: %s(%s) user=%s", tool_name, tool_args, user_id)
        result_str = registry.dispatch(
            tool_name, tool_args,
            rc=rc, user_id=user_id, session_id=session_id,
        )

        # execute_code gets a free budget refund (not yet implemented, reserved)
        if tool_name == "execute_code" and budget is not None:
            budget.refund(1)

        tool_messages.append(
            ToolMessage(content=result_str, tool_call_id=tool_id, name=tool_name)
        )

    return {"messages": tool_messages}


# ── Conditional edge ──────────────────────────────────────────────────────────


def should_continue(state: AgentState) -> str:
    last_msg = state["messages"][-1]
    if isinstance(last_msg, AIMessage) and last_msg.tool_calls:
        return "tools"
    return END


# ── Graph construction ────────────────────────────────────────────────────────


def _build_graph() -> StateGraph:
    graph = StateGraph(AgentState)
    graph.add_node("agent", call_model)
    graph.add_node("tools", tool_node)
    graph.set_entry_point("agent")
    graph.add_conditional_edges("agent", should_continue)
    graph.add_edge("tools", "agent")
    return graph


# Compile once at module level
_react_app = _build_graph().compile()


# ── Public API ────────────────────────────────────────────────────────────────


def stream(
    messages: Sequence[BaseMessage],
    rc: redis.Redis,
    *,
    user_id: str = "default",
    session_id: str = "default",
):
    """
    Stream the ReAct loop with intermediate tool-call progress and final answer.

    Yields str chunks:
      - "[TOOL] tool_name(args)"  — for each tool call step
      - final assistant text after all tool calls complete
    """
    budget = IterationBudget()
    seen_ids: set[str] = set()

    for chunk in _react_app.stream(
        {
            "messages": list(messages),
            "rc": rc,
            "user_id": user_id,
            "session_id": session_id,
            "budget": budget,
        },
        # LangGraph recursion limit kept as hard safety net above budget limit
        config={"recursion_limit": budget._max + 5},
        stream_mode="values",
    ):
        last = chunk["messages"][-1]

        # Tool-call step: yield progress notification
        if isinstance(last, AIMessage) and last.tool_calls:
            msg_id = last.id or ""
            if msg_id not in seen_ids:
                seen_ids.add(msg_id)
                if last.content:
                    yield last.content
                for tc in last.tool_calls:
                    args_preview = ", ".join(
                        f"{k}={v!r}" for k, v in tc["args"].items()
                    )
                    yield TOOL_CALL_PROGRESS_TEMPLATE.format(
                        tool_name=tc["name"], args_preview=args_preview
                    )

        # Tool-result step: yield formatted result
        elif isinstance(last, ToolMessage):
            msg_id = last.tool_call_id or ""
            if msg_id not in seen_ids:
                seen_ids.add(msg_id)
                try:
                    result_data = json.loads(last.content)
                    if isinstance(result_data, dict) and result_data:
                        lines = [TOOL_RESULT_HEADER_TEMPLATE.format(name=last.name)]
                        for k, v in result_data.items():
                            lines.append(TOOL_RESULT_ITEM_TEMPLATE.format(k=k, v=v))
                        yield "\n".join(lines) + "\n"
                    else:
                        yield TOOL_RESULT_FALLBACK_TEMPLATE.format(
                            name=last.name, content=last.content
                        )
                except Exception:
                    yield TOOL_RESULT_FALLBACK_TEMPLATE.format(
                        name=last.name, content=last.content
                    )

        # Final answer
        elif isinstance(last, AIMessage) and last.content and not last.tool_calls:
            msg_id = last.id or ""
            if msg_id not in seen_ids:
                seen_ids.add(msg_id)
                yield last.content


def run(
    messages: Sequence[BaseMessage],
    rc: redis.Redis,
    *,
    user_id: str = "default",
    session_id: str = "default",
) -> str:
    """
    Run the full ReAct loop synchronously and return the final answer string.
    """
    budget = IterationBudget()
    try:
        result = _react_app.invoke(
            {
                "messages": list(messages),
                "rc": rc,
                "user_id": user_id,
                "session_id": session_id,
                "budget": budget,
            },
            config={"recursion_limit": budget._max + 5},
        )
    except Exception as e:
        err_msg = str(e)
        if "recursion" in err_msg.lower():
            logger.warning("[orchestrator] Recursion limit reached, returning last message")
            for msg in reversed(messages):
                if isinstance(msg, AIMessage) and msg.content:
                    return msg.content
            return RECURSION_LIMIT_MSG
        raise

    final_messages = result.get("messages", [])
    for msg in reversed(final_messages):
        if isinstance(msg, AIMessage) and msg.content:
            return msg.content
    return ""


# ── Helpers ────────────────────────────────────────────────────────────────────


def _lc_messages_to_oai(messages: Sequence[BaseMessage]) -> list[dict]:
    """Convert LangChain message objects to OpenAI-compatible dicts."""
    result = []
    for msg in messages:
        if isinstance(msg, SystemMessage):
            result.append({"role": "system", "content": msg.content})
        elif isinstance(msg, HumanMessage):
            result.append({"role": "user", "content": msg.content})
        elif isinstance(msg, AIMessage):
            entry: dict[str, Any] = {"role": "assistant", "content": msg.content or ""}
            if msg.tool_calls:
                entry["tool_calls"] = [
                    {
                        "id": tc["id"],
                        "type": "function",
                        "function": {
                            "name": tc["name"],
                            "arguments": json.dumps(tc["args"], ensure_ascii=False),
                        },
                    }
                    for tc in msg.tool_calls
                ]
            result.append(entry)
        elif isinstance(msg, ToolMessage):
            result.append({
                "role": "tool",
                "tool_call_id": msg.tool_call_id,
                "content": msg.content,
            })
        else:
            result.append({"role": "user", "content": str(msg.content)})
    return result

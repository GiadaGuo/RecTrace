"""
orchestrator.py
---------------
LangGraph StateGraph that wires together the LLM (zhipu_client) and
the tool functions (tools.py) into a ReAct loop.

Graph topology:
  START → agent (call_model) ─┬─ tool_calls non-empty → tools ─→ agent
                              └─ tool_calls empty      → END

Recursion limit prevents infinite loops; on limit breach the last
assistant message is returned gracefully instead of raising RecursionError.
"""

import json
import logging
import re
from typing import Any, Sequence

import redis
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage
from langgraph.graph import END, StateGraph
from langgraph.graph.message import MessagesState

from .prompts import (
    LLM_ERROR_TEMPLATE,
    RECURSION_LIMIT_MSG,
    SYSTEM_PROMPT,
    TOOL_CALL_PROGRESS_TEMPLATE,
    TOOL_RESULT_FALLBACK_TEMPLATE,
    TOOL_RESULT_HEADER_TEMPLATE,
    TOOL_RESULT_ITEM_TEMPLATE,
)
from .tools import TOOL_SPECS, dispatch_tool
from .zhipu_client import invoke as llm_invoke, parse_tool_call

logger = logging.getLogger(__name__)

# Maximum ReAct iterations before forced stop
_RECURSION_LIMIT = 10

# (SYSTEM_PROMPT moved to agent/prompts.py)


# ── State definition ──────────────────────────────────────────────────────────


class AgentState(MessagesState):
    """Extends MessagesState with a redis client reference for tool dispatch."""
    rc: redis.Redis


# ── Node functions ────────────────────────────────────────────────────────────


def call_model(state: AgentState) -> dict:
    """
    Call the LLM with the current message history and TOOL_SPECS.
    Returns the LLM's response as an AIMessage (or dict that LangGraph
    will merge into state).
    """
    messages = list(state["messages"])

    # Inject system prompt if not already present
    if not messages or not isinstance(messages[0], SystemMessage):
        messages.insert(0, SystemMessage(content=SYSTEM_PROMPT))

    # Convert LangChain messages → OpenAI-compatible dicts for zai-sdk
    oai_messages = _lc_messages_to_oai(messages)

    try:
        ai_msg = llm_invoke(oai_messages, tools=TOOL_SPECS)
    except Exception as e:
        logger.warning("[orchestrator] LLM invoke failed: %s", e)
        return {"messages": [AIMessage(content=LLM_ERROR_TEMPLATE.format(e=e))]}

    # Wrap into LangChain AIMessage, preserving tool_calls
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
    # Store the raw zai-sdk message for should_continue to inspect
    lc_ai.additional_kwargs["_raw_tool_calls"] = ai_msg.tool_calls

    return {"messages": [lc_ai]}


def tool_node(state: AgentState) -> dict:
    """
    Execute all tool_calls from the last AIMessage and return ToolMessages.
    """
    last_ai: AIMessage = state["messages"][-1]
    rc: redis.Redis = state.get("rc")

    tool_messages: list[ToolMessage] = []
    for tc in last_ai.tool_calls:
        tool_name = tc["name"]
        tool_args = tc["args"]
        tool_id = tc["id"]

        logger.debug("[orchestrator] executing tool: %s(%s)", tool_name, tool_args)
        result_str = dispatch_tool(tool_name, tool_args, rc=rc)

        tool_messages.append(
            ToolMessage(content=result_str, tool_call_id=tool_id, name=tool_name)
        )

    return {"messages": tool_messages}


# ── Conditional edge ──────────────────────────────────────────────────────────


def should_continue(state: AgentState) -> str:
    """
    Route: if the last AIMessage has tool_calls, go to 'tools' node;
    otherwise, end the conversation.
    """
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


def stream(messages: Sequence[BaseMessage], rc: redis.Redis):
    """
    Stream the ReAct loop with intermediate tool-call progress and final answer.

    Yields:
      - "[TOOL] tool_name(args)" for each tool call step (so the frontend
        can show the agent is thinking / calling tools)
      - final assistant text content after all tool calls complete
      - intermediate AIMessage content (when the model outputs text before
        deciding to call tools) is skipped to avoid partial / truncated output
    """
    seen_ids: set[str] = set()

    for chunk in _react_app.stream(
        {"messages": list(messages), "rc": rc},
        config={"recursion_limit": _RECURSION_LIMIT},
        stream_mode="values",
    ):
        last = chunk["messages"][-1]

        # --- Tool-call step: yield progress notification ---
        if isinstance(last, AIMessage) and last.tool_calls:
            msg_id = last.id or ""
            if msg_id not in seen_ids:
                seen_ids.add(msg_id)

                # 先输出开场白
                if last.content:
                    yield last.content

                for tc in last.tool_calls:
                    tool_name = tc["name"]
                    args_preview = ", ".join(
                        f"{k}={v!r}" for k, v in tc["args"].items()
                    )
                    # 不以换行符开头，避免 SSE 空行问题
                    yield TOOL_CALL_PROGRESS_TEMPLATE.format(tool_name=tool_name, args_preview=args_preview)

        # --- Tool-result step: yield formatted tool result ---
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
                        yield TOOL_RESULT_FALLBACK_TEMPLATE.format(name=last.name, content=last.content)
                except Exception:
                    yield TOOL_RESULT_FALLBACK_TEMPLATE.format(name=last.name, content=last.content)

        # --- Final answer (AIMessage without tool_calls) ---
        elif isinstance(last, AIMessage) and last.content and not last.tool_calls:
            msg_id = last.id or ""
            if msg_id not in seen_ids:
                seen_ids.add(msg_id)
                yield last.content


def run(messages: Sequence[BaseMessage], rc: redis.Redis) -> str:
    """
    Run the ReAct loop with the given message history and Redis client.

    Returns the final assistant text answer.
    On recursion-limit breach, returns the last available assistant message.
    """
    try:
        result = _react_app.invoke(
            {"messages": list(messages), "rc": rc},
            config={"recursion_limit": _RECURSION_LIMIT},
        )
    except Exception as e:
        # LangGraph may raise GraphRecursionError on limit breach
        err_msg = str(e)
        if "recursion" in err_msg.lower():
            logger.warning("[orchestrator] Recursion limit reached, returning last message")
            # Extract last assistant content from messages so far
            for msg in reversed(messages):
                if isinstance(msg, AIMessage) and msg.content:
                    return msg.content
            return RECURSION_LIMIT_MSG
        raise

    final_messages = result.get("messages", [])
    # Find the last AIMessage with non-empty content
    for msg in reversed(final_messages):
        if isinstance(msg, AIMessage) and msg.content:
            return msg.content
    return ""


# ── Helpers ────────────────────────────────────────────────────────────────────


def _lc_messages_to_oai(messages: Sequence[BaseMessage]) -> list[dict]:
    """Convert LangChain message objects to OpenAI-compatible dicts for zai-sdk."""
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
                            "arguments": __import__("json").dumps(
                                tc["args"], ensure_ascii=False
                            ),
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
            # Fallback: treat as user message
            result.append({"role": "user", "content": str(msg.content)})
    return result

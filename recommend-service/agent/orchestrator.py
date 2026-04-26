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

import logging
import re
from typing import Any, Sequence

import redis
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langgraph.graph import END, StateGraph
from langgraph.graph.message import MessagesState

from .tools import TOOL_SPECS, dispatch_tool
from .zhipu_client import invoke as llm_invoke, parse_tool_call

logger = logging.getLogger(__name__)

# Maximum ReAct iterations before forced stop
_RECURSION_LIMIT = 10

# System prompt injected at the start of every conversation
_SYSTEM_PROMPT = """你是一个推荐系统的智能分析助手。你可以调用以下工具来回答用户关于推荐结果的问题：
- get_rec_snapshot：查看某次推荐请求的快照数据
- get_feature_contributions：查看某商品的特征贡献分
- run_lineage_trace：追溯某商品被推荐的决策路径
- get_user_sequence：查看用户的点击行为序列
- get_user_features：查看用户的实时特征

请根据用户的问题选择合适的工具，并用中文给出清晰的回答。"""


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
    if not messages or messages[0].type != "system":
        messages.insert(0, HumanMessage(content=_SYSTEM_PROMPT))
        # Tag it so we don't re-inject (LangGraph treats it as a HumanMessage
        # but we store system content there for simplicity with zai-sdk).

    # Convert LangChain messages → OpenAI-compatible dicts for zai-sdk
    oai_messages = _lc_messages_to_oai(messages)

    try:
        ai_msg = llm_invoke(oai_messages, tools=TOOL_SPECS)
    except Exception as e:
        logger.warning("[orchestrator] LLM invoke failed: %s", e)
        return {"messages": [AIMessage(content=f"抱歉，模型调用出现异常：{e}")]}

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
            return "抱歉，推理轮次已达上限，请尝试简化问题。"
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
        if isinstance(msg, HumanMessage):
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

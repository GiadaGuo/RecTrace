"""
zhipu_client.py
---------------
Thin wrapper around the ZhipuAI (zai-sdk) ChatGLM API.

Exposes a single `invoke(messages, tools)` function that calls
chat.completions.create and returns the raw response message object.
The caller checks `.tool_calls` on the returned message to decide whether
to execute tools or return the final answer.

Environment variables:
    ZHIPU_API_KEY   — ZhipuAI API key (required)
    ZHIPU_MODEL     — model name (default: glm-5.1)
    ZHIPU_BASE_URL  — API base URL (optional, zai-sdk defaults to Chinese endpoint)
"""

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

_MODEL = os.environ.get("ZHIPU_MODEL", "glm-5.1")


def _get_client():
    """Lazily create ZhipuAiClient (Chinese region endpoint)."""
    try:
        from zai import ZhipuAiClient
    except ImportError as exc:
        raise ImportError(
            "zai-sdk is required for the Agent module. "
            "Install with: pip install zai-sdk"
        ) from exc

    api_key = os.environ.get("ZHIPU_API_KEY", "")
    base_url = os.environ.get("ZHIPU_BASE_URL", "")
    kwargs = {"api_key": api_key}
    if base_url:
        kwargs["base_url"] = base_url
    return ZhipuAiClient(**kwargs)


def invoke(messages: list, tools: list | None = None) -> Any:
    """
    Call ChatGLM API and return the response message object.

    Parameters
    ----------
    messages : list
        OpenAI-compatible message dicts, e.g.
        [{"role": "user", "content": "..."}]
    tools : list | None
        TOOL_SPECS in OpenAI Function Calling format (type:object wrapped).

    Returns
    -------
    message object with attributes:
        .content      — str, final text answer (may be None mid-loop)
        .tool_calls   — list of tool call objects, or None
        .role         — "assistant"
    """
    client = _get_client()

    kwargs: dict = {
        "model": _MODEL,
        "messages": messages,
    }
    if tools:
        kwargs["tools"] = tools
        kwargs["tool_choice"] = "auto"

    logger.debug("[zhipu_client] invoke model=%s messages=%d", _MODEL, len(messages))

    response = client.chat.completions.create(**kwargs)
    message = response.choices[0].message
    logger.debug(
        "[zhipu_client] tool_calls=%s",
        bool(message.tool_calls),
    )
    return message


def parse_tool_call(tool_call) -> tuple[str, dict]:
    """
    Extract (function_name, arguments_dict) from a tool_call object.

    Handles both dict-style and attribute-style tool_call objects.
    """
    try:
        name = tool_call.function.name
        args = json.loads(tool_call.function.arguments)
    except AttributeError:
        # dict-style fallback
        name = tool_call["function"]["name"]
        args = json.loads(tool_call["function"]["arguments"])
    return name, args

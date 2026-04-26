"""
test_orchestrator.py
--------------------
Tests for agent/orchestrator.py: LangGraph ReAct loop logic.

All tests mock the zhipu_client to avoid real LLM calls.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import fakeredis
from langchain_core.messages import AIMessage, HumanMessage

from agent.orchestrator import should_continue


# ── should_continue routing logic ─────────────────────────────────────────────

class TestShouldContinue:
    def test_routes_to_tools_when_tool_calls_present(self):
        """When the last AIMessage has tool_calls, should_continue returns 'tools'."""
        state = {
            "messages": [
                HumanMessage(content="Why was I0000001 recommended?"),
                AIMessage(
                    content="",
                    tool_calls=[{
                        "id": "call_0",
                        "name": "get_rec_snapshot",
                        "args": {"req_id": "req_test01"},
                    }],
                ),
            ]
        }
        result = should_continue(state)
        assert result == "tools"

    def test_routes_to_end_when_no_tool_calls(self):
        """When the last AIMessage has no tool_calls (empty list), should_continue returns END."""
        from langgraph.graph import END

        state = {
            "messages": [
                HumanMessage(content="Hello"),
                AIMessage(content="你好！有什么可以帮你的？"),
            ]
        }
        result = should_continue(state)
        assert result == END


# ── Recursion limit handling ──────────────────────────────────────────────────

class TestRecursionLimit:
    def test_does_not_raise_recursion_error_on_limit(self):
        """
        When the ReAct loop exceeds recursion_limit, the orchestrator
        should return a fallback message, not raise RecursionError.

        We test this by patching the compiled graph to force a
        GraphRecursionError, then verifying run() handles it.
        """
        from agent import orchestrator
        from langgraph.errors import GraphRecursionError

        original_app = orchestrator._react_app

        class FakeGraph:
            def invoke(self, state, config=None):
                raise GraphRecursionError("Recursion limit of 10 reached")

        orchestrator._react_app = FakeGraph()

        try:
            fr = fakeredis.FakeRedis(decode_responses=True)
            messages = [HumanMessage(content="test")]
            result = orchestrator.run(messages, rc=fr)
            # Should return a fallback string, not raise
            assert isinstance(result, str)
            assert len(result) > 0
        finally:
            orchestrator._react_app = original_app

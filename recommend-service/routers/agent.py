"""
agent.py
--------
POST /agent/chat — Agent conversational endpoint powered by LangGraph ReAct loop.
GET  /lineage/{uid}/{item_id} — Direct lineage trace query.
"""

import logging
from typing import Optional

import redis
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from langchain_core.messages import HumanMessage
from pydantic import BaseModel

from agent.orchestrator import stream as orchestrator_stream
from agent.lineage import run_lineage_trace
from feature.feature_fetcher import _get_redis

router = APIRouter()
logger = logging.getLogger(__name__)


# ── Dependency ────────────────────────────────────────────────────────────────

def get_redis_client() -> redis.Redis:
    return _get_redis()


# ── Request / Response schemas ────────────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None


class ChatResponse(BaseModel):
    reply: str
    session_id: Optional[str] = None


class LineageResponse(BaseModel):
    uid: str
    item_id: str
    req_id: Optional[str] = None
    ts: Optional[str] = None
    seq_items: list = []
    contributing_features: dict = {}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/agent/chat")
def agent_chat(req: ChatRequest, r: redis.Redis = Depends(get_redis_client)):
    """
    Agent conversational endpoint (SSE).

    Accepts a user message, runs it through the LangGraph ReAct loop
    (which may invoke tool calls), and streams the assistant reply as
    Server-Sent Events.  Each event carries a text chunk; the final
    event is ``data: [DONE]``.
    """
    messages = [HumanMessage(content=req.message)]

    def event_generator():
        has_content = False
        for text in orchestrator_stream(messages, rc=r):
            has_content = True
            # SSE 规范：多行内容需要每行都以 data: 开头
            for line in text.split("\n"):
                yield f"data: {line}\n"
            yield "\n"  # 事件结束空行
        if not has_content:
            yield "data: 抱歉，未能获取到完整回答，请重试。\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.get("/lineage/{uid}/{item_id}", response_model=LineageResponse)
def lineage(uid: str, item_id: str, r: redis.Redis = Depends(get_redis_client)):
    """
    Direct lineage trace: reconstruct the recommendation decision path
    for a (uid, item_id) pair without going through the Agent loop.
    """
    result = run_lineage_trace(uid, item_id, r)
    return LineageResponse(**result)

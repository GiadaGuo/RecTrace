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
from langchain_core.messages import HumanMessage
from pydantic import BaseModel

from agent.orchestrator import run as orchestrator_run
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

@router.post("/agent/chat", response_model=ChatResponse)
def agent_chat(req: ChatRequest, r: redis.Redis = Depends(get_redis_client)):
    """
    Agent conversational endpoint.

    Accepts a user message, runs it through the LangGraph ReAct loop
    (which may invoke tool calls), and returns the final assistant answer.
    """
    messages = [HumanMessage(content=req.message)]
    reply = orchestrator_run(messages, rc=r)

    return ChatResponse(reply=reply, session_id=req.session_id)


@router.get("/lineage/{uid}/{item_id}", response_model=LineageResponse)
def lineage(uid: str, item_id: str, r: redis.Redis = Depends(get_redis_client)):
    """
    Direct lineage trace: reconstruct the recommendation decision path
    for a (uid, item_id) pair without going through the Agent loop.
    """
    result = run_lineage_trace(uid, item_id, r)
    return LineageResponse(**result)

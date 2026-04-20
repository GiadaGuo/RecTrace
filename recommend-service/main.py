"""
main.py
-------
FastAPI application entry point for recommend-service.

Start locally:
    cd recommend-service
    uvicorn main:app --reload --port 8000

Environment variables:
    REDIS_HOST  — Redis hostname (default: localhost)
    REDIS_PORT  — Redis port    (default: 6379)
"""

from fastapi import FastAPI

from routers.recommend import router as recommend_router

app = FastAPI(
    title="Recommend Service",
    description="Real-time recommendation service backed by Flink feature pipeline",
    version="1.0.0",
)

app.include_router(recommend_router, tags=["recommend"])


@app.get("/health")
def health():
    return {"status": "ok"}

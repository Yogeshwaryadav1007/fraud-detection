"""API Gateway — edge concerns only: auth, rate limiting, routing, correlation.

In production Istio + an ingress gateway do TLS, mTLS and retries; this service
keeps the business-facing contract stable and shields internal service names.
"""
from __future__ import annotations

import time
import uuid

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from libs.common import observability as obs
from libs.common.config import settings
from libs.common.stores import redis

log = obs.setup("api-gateway", metrics_port=9100)
app = FastAPI(title="Fraud Platform Gateway", version="1.0.0")

UPSTREAM = {
    "ingestion": "http://ingestion:8080",
    "cases": "http://case-management:8080",
    "decisions": "http://risk-scoring:8080",
}
RATE_LIMIT_PER_MIN = 600


@app.middleware("http")
async def edge(request: Request, call_next):
    t0 = time.perf_counter()
    request.state.request_id = request.headers.get("x-request-id", uuid.uuid4().hex)
    response = await call_next(request)
    response.headers["x-request-id"] = request.state.request_id
    response.headers["x-response-time-ms"] = f"{(time.perf_counter()-t0)*1000:.1f}"
    return response


async def guard(api_key: str | None) -> str:
    if api_key not in settings.API_KEYS.split(","):
        raise HTTPException(401, "invalid api key")
    r = await redis()
    bucket = f"rl:{api_key}:{int(time.time() // 60)}"
    n = await r.incr(bucket)
    if n == 1:
        await r.expire(bucket, 90)
    if n > RATE_LIMIT_PER_MIN:
        raise HTTPException(429, "rate limit exceeded")
    return api_key


@app.get("/health/live")
async def live():
    return {"status": "ok"}


@app.post("/v1/transactions")
async def proxy_txn(request: Request, x_api_key: str | None = Header(default=None)):
    await guard(x_api_key)
    body = await request.body()
    async with httpx.AsyncClient(timeout=3.0) as c:
        r = await c.post(
            f"{UPSTREAM['ingestion']}/v1/transactions",
            content=body,
            headers={"content-type": "application/json",
                     "x-api-key": x_api_key or "",
                     "x-request-id": request.state.request_id,
                     "idempotency-key": request.headers.get("idempotency-key", "")},
        )
    return JSONResponse(status_code=r.status_code, content=r.json())


@app.get("/v1/cases")
async def proxy_cases(request: Request, x_api_key: str | None = Header(default=None)):
    await guard(x_api_key)
    async with httpx.AsyncClient(timeout=5.0) as c:
        r = await c.get(f"{UPSTREAM['cases']}/v1/cases",
                        params=dict(request.query_params))
    return JSONResponse(status_code=r.status_code, content=r.json())

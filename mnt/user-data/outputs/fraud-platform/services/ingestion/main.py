"""Transaction Ingestion — the only write door into the pipeline.

Responsibilities: authenticate, validate, deduplicate (idempotency key),
persist the raw record, publish to txn.raw.v1, return 202 fast.
"""
from __future__ import annotations

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from libs.common import observability as obs
from libs.common.bus import Producer
from libs.common.config import settings
from libs.common.schemas import TransactionRequest
from libs.common.stores import close_all, pg, redis
from libs.common.topics import TRANSACTIONS_RAW

log = obs.setup("ingestion", metrics_port=9101)
producer = Producer()

DEDUPE_TTL = 24 * 3600


@asynccontextmanager
async def lifespan(_: FastAPI):
    await producer.start()
    await pg()
    yield
    await producer.stop()
    await close_all()


app = FastAPI(title="Transaction Ingestion", version="1.0.0", lifespan=lifespan)


def _auth(api_key: str | None) -> None:
    if api_key not in settings.API_KEYS.split(","):
        raise HTTPException(status_code=401, detail="invalid api key")


@app.get("/health/live")
async def live():
    return {"status": "ok"}


@app.get("/health/ready")
async def ready():
    r = await redis()
    await r.ping()
    return {"status": "ready"}


@app.post("/v1/transactions", status_code=202)
async def ingest(
    txn: TransactionRequest,
    request: Request,
    x_api_key: str | None = Header(default=None),
    idempotency_key: str | None = Header(default=None),
):
    t0 = time.perf_counter()
    _auth(x_api_key)
    r = await redis()

    key = idempotency_key or txn.transaction_id
    if not await r.set(f"idem:{key}", txn.transaction_id, ex=DEDUPE_TTL, nx=True):
        prior = await r.get(f"idem:{key}")
        obs.EVENTS.labels("ingestion", TRANSACTIONS_RAW, "duplicate").inc()
        return JSONResponse(
            status_code=200,
            content={"transaction_id": prior, "status": "DUPLICATE"},
        )

    with obs.timed("persist"):
        pool = await pg()
        await pool.execute(
            """INSERT INTO transactions
               (transaction_id, card_id, account_id, customer_id, merchant_id,
                merchant_category, merchant_country, amount, currency, channel,
                device_id, ip_address, latitude, longitude, occurred_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
               ON CONFLICT (transaction_id) DO NOTHING""",
            txn.transaction_id, txn.card_id, txn.account_id, txn.customer_id,
            txn.merchant_id, txn.merchant_category, txn.merchant_country,
            txn.amount, txn.currency, txn.channel.value, txn.device_id,
            txn.ip_address, txn.latitude, txn.longitude, txn.occurred_at,
        )

    trace_id = request.headers.get("x-request-id", txn.transaction_id)
    payload = txn.model_dump()
    payload["trace_id"] = trace_id

    with obs.timed("publish"):
        await producer.send(TRANSACTIONS_RAW, payload, key=txn.card_id)

    obs.EVENTS.labels("ingestion", TRANSACTIONS_RAW, "accepted").inc()
    log.info(
        "transaction accepted",
        extra={"transaction_id": txn.transaction_id, "trace_id": trace_id},
    )
    return {
        "transaction_id": txn.transaction_id,
        "status": "ACCEPTED",
        "ingest_latency_ms": round((time.perf_counter() - t0) * 1000, 2),
    }

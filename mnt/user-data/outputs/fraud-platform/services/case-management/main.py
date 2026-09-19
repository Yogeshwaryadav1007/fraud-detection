"""Case Management — analyst queue API plus the consumer that fills it."""
from __future__ import annotations

import asyncio
import json
import os
import threading
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, Query

from libs.common import observability as obs
from libs.common.bus import Consumer, Producer, run_worker
from libs.common.schemas import AuditEvent, CaseEvent
from libs.common.stores import pg
from libs.common.topics import AUDIT, CASES

log = obs.setup("case-management", metrics_port=9109)
producer = Producer()


# ------------------------------------------------------------------ consumer
async def handle(_topic: str, value: dict) -> None:
    case = CaseEvent.model_validate(value)
    pool = await pg()
    await pool.execute(
        """INSERT INTO cases
           (case_id, transaction_id, customer_id, priority, risk_score,
            status, reasons, created_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8)
           ON CONFLICT (transaction_id) DO NOTHING""",
        case.case_id, case.transaction_id, case.customer_id, case.priority,
        case.risk_score, case.status,
        json.dumps([r.model_dump() for r in case.reasons], default=str),
        case.created_at,
    )
    obs.EVENTS.labels("case-management", CASES, "created").inc()


# ------------------------------------------------------------------ API
@asynccontextmanager
async def lifespan(_: FastAPI):
    await producer.start()
    yield
    await producer.stop()


app = FastAPI(title="Case Management", version="1.0.0", lifespan=lifespan)


@app.get("/health/live")
async def live():
    return {"status": "ok"}


@app.get("/v1/cases")
async def list_cases(
    status: str = Query("OPEN"),
    priority: str | None = None,
    limit: int = Query(50, le=200),
):
    pool = await pg()
    rows = await pool.fetch(
        """SELECT * FROM cases
           WHERE status = $1 AND ($2::text IS NULL OR priority = $2)
           ORDER BY risk_score DESC, created_at ASC LIMIT $3""",
        status, priority, limit,
    )
    return {"count": len(rows), "cases": [dict(r) for r in rows]}


@app.get("/v1/cases/{case_id}")
async def get_case(case_id: str):
    pool = await pg()
    row = await pool.fetchrow("SELECT * FROM cases WHERE case_id = $1", case_id)
    if not row:
        raise HTTPException(404, "case not found")
    return dict(row)


@app.patch("/v1/cases/{case_id}")
async def update_case(case_id: str, status: str, analyst: str, note: str = ""):
    if status not in ("OPEN", "IN_REVIEW", "CLOSED"):
        raise HTTPException(400, "invalid status")
    pool = await pg()
    row = await pool.fetchrow(
        """UPDATE cases SET status=$2, analyst=$3, note=$4, updated_at=now()
           WHERE case_id=$1 RETURNING transaction_id""",
        case_id, status, analyst, note,
    )
    if not row:
        raise HTTPException(404, "case not found")
    await producer.send(AUDIT, AuditEvent(
        transaction_id=row["transaction_id"], event_type="CASE_UPDATED",
        actor=analyst, payload={"case_id": case_id, "status": status,
                                "note": note}), key=row["transaction_id"])
    return {"case_id": case_id, "status": status}


def _consumer_thread() -> None:
    asyncio.run(run_worker(Consumer(CASES, group="case-management"),
                           Producer(), handle))


if __name__ == "__main__":
    threading.Thread(target=_consumer_thread, daemon=True).start()
    uvicorn.run(app, host=os.getenv("HOST", "127.0.0.1"), port=8080)

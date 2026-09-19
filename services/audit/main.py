"""Audit — append-only, hash-chained, WORM-friendly.

Each row stores the SHA-256 of (prev_hash || canonical payload). Tampering with
any historical row breaks every hash after it, which is what regulators want to
see. Rows are also mirrored to Elasticsearch for investigator search.
"""
from __future__ import annotations

import asyncio
import hashlib
import json

import httpx

from libs.common import observability as obs
from libs.common.bus import Consumer, Producer, run_worker
from libs.common.config import settings
from libs.common.schemas import AuditEvent
from libs.common.stores import pg, redis
from libs.common.topics import AUDIT

log = obs.setup("audit", metrics_port=9107)
producer = Producer()
HEAD_KEY = "audit:chain:head"


def chain_hash(prev: str | None, payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(f"{prev or ''}|{canonical}".encode()).hexdigest()


async def index_es(ev: AuditEvent) -> None:
    try:
        async with httpx.AsyncClient(timeout=2.0) as c:
            await c.put(
                f"{settings.ELASTIC_URL}/fraud-audit-000001/_doc/{ev.audit_id}",
                json=json.loads(ev.model_dump_json()),
            )
    except Exception as exc:                                # noqa: BLE001
        log.warning("elasticsearch mirror failed: %s", exc)


async def handle(topic: str, value: dict) -> None:
    ev = (AuditEvent.model_validate(value) if topic == AUDIT
          else AuditEvent(transaction_id=value["transaction_id"],
                          event_type="DECISION_PUBLISHED", payload=value))

    r = await redis()
    prev = await r.get(HEAD_KEY)
    ev.prev_hash = prev
    ev.hash = chain_hash(prev, ev.payload)

    pool = await pg()
    await pool.execute(
        """INSERT INTO audit_log
           (audit_id, transaction_id, event_type, actor, payload,
            prev_hash, hash, occurred_at)
           VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8)
           ON CONFLICT (audit_id) DO NOTHING""",
        ev.audit_id, ev.transaction_id, ev.event_type, ev.actor,
        json.dumps(ev.payload, default=str), ev.prev_hash, ev.hash,
        ev.occurred_at,
    )
    await r.set(HEAD_KEY, ev.hash)
    await index_es(ev)
    obs.EVENTS.labels("audit", topic, "ok").inc()


def main() -> None:
    asyncio.run(run_worker(Consumer(AUDIT, group="audit"), producer, handle))


if __name__ == "__main__":
    main()

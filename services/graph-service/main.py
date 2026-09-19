"""Graph Service — relationship risk over Neo4j.

Writes the transaction into the graph, then runs a bounded set of pattern
queries under a hard timeout. Graph work is the slowest leg of the pipeline, so
it is budgeted: exceed the budget and we emit a partial signal rather than
stalling the decision.
"""
from __future__ import annotations

import asyncio
import time

import queries as Q

from libs.common import observability as obs
from libs.common.bus import Consumer, Producer, run_worker
from libs.common.schemas import EnrichedTransaction, Reason, Signal
from libs.common.stores import neo4j
from libs.common.topics import ANALYSIS_GRAPH, TRANSACTIONS_ENRICHED

log = obs.setup("graph-service", metrics_port=9105)
producer = Producer()
QUERY_BUDGET_S = 0.45


async def _run(session, cypher: str, **params) -> list[dict]:
    result = await session.run(cypher, **params)
    return [r.data() async for r in result]


async def analyse(e: EnrichedTransaction) -> tuple[float, list[Reason]]:
    t = e.transaction
    score = 0.0
    reasons: list[Reason] = []

    async with neo4j().session() as s:
        await _run(s, Q.UPSERT,
                   card_id=t.card_id, account_id=t.account_id,
                   customer_id=t.customer_id, merchant_id=t.merchant_id,
                   mcc=t.merchant_category, country=t.merchant_country,
                   transaction_id=t.transaction_id, amount=float(t.amount),
                   currency=t.currency, occurred_at=t.occurred_at.isoformat(),
                   device_id=t.device_id, ip_address=t.ip_address)

        shared_dev, shared_ip, prox, mule, ring = await asyncio.gather(
            _run(s, Q.SHARED_DEVICE, card_id=t.card_id),
            _run(s, Q.SHARED_IP, card_id=t.card_id),
            _run(s, Q.FRAUD_PROXIMITY, card_id=t.card_id),
            _run(s, Q.MULE_FAN_IN, account_id=t.account_id),
            _run(s, Q.COMMUNITY_SIZE, card_id=t.card_id),
            return_exceptions=True,
        )

    def ok(x): return isinstance(x, list) and x

    if ok(shared_dev):
        top = shared_dev[0]
        pts = min(15 + 5 * top["card_count"], 45)
        score += pts
        reasons.append(Reason(code="G001",
            description=f"Device shared by {top['card_count']} different cards",
            weight=pts, evidence=top))

    if ok(shared_ip):
        top = shared_ip[0]
        pts = min(10 + 3 * top["card_count"], 30)
        score += pts
        reasons.append(Reason(code="G002",
            description=f"IP address shared by {top['card_count']} cards",
            weight=pts, evidence=top))

    if ok(prox):
        hops = prox[0]["hops"]
        pts = {1: 50, 2: 35, 3: 20}.get(hops, 10)
        score += pts
        reasons.append(Reason(code="G003",
            description=f"{hops} hop(s) away from a confirmed fraudulent card",
            weight=pts, evidence=prox[0]))

    if ok(mule):
        score += 30
        reasons.append(Reason(code="G004",
            description="Account shows mule-like fan-in from many sources",
            weight=30, evidence=mule[0]))

    if ok(ring) and ring[0].get("ring_size", 0) >= 15:
        pts = 20
        score += pts
        reasons.append(Reason(code="G005",
            description=f"Card sits in a dense cluster of {ring[0]['ring_size']} cards",
            weight=pts, evidence=ring[0]))

    return min(score, 100.0), sorted(reasons, key=lambda r: -r.weight)


async def handle(_topic: str, value: dict) -> None:
    t0 = time.perf_counter()
    e = EnrichedTransaction.model_validate(value)
    degraded = False
    try:
        with obs.timed("graph"):
            score, reasons = await asyncio.wait_for(analyse(e), QUERY_BUDGET_S)
    except (asyncio.TimeoutError, Exception) as exc:        # noqa: BLE001
        obs.DEGRADED.labels("GRAPH").inc()
        log.warning("graph analysis degraded: %s", exc)
        score, reasons, degraded = 0.0, [], True

    signal = Signal(
        transaction_id=e.transaction.transaction_id,
        source="GRAPH",
        score=score,
        reasons=reasons,
        latency_ms=round((time.perf_counter() - t0) * 1000, 3),
        degraded=degraded,
        trace_id=e.trace_id,
    )
    await producer.send(ANALYSIS_GRAPH, signal, key=signal.transaction_id)
    obs.EVENTS.labels("graph-service", ANALYSIS_GRAPH, "ok").inc()


def main() -> None:
    asyncio.run(run_worker(
        Consumer(TRANSACTIONS_ENRICHED, group="graph-service"), producer, handle))


if __name__ == "__main__":
    main()

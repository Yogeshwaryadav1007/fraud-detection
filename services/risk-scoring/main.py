"""Risk Scoring — the fan-in point and the only place a decision is made.

Three analysers publish independently, so this service buffers partial results
in Redis keyed by transaction_id. It decides as soon as all three arrive, or
when DECISION_TIMEOUT_MS elapses — whichever comes first. A timeout does not
block the transaction; it decides on what it has and records the gap in
`missing_signals`, which is what makes the score auditable.
"""
from __future__ import annotations

import asyncio
import json
import time

from libs.common import observability as obs
from libs.common.bus import Consumer, Producer, run_worker
from libs.common.config import settings
from libs.common.schemas import (
    AuditEvent,
    CaseEvent,
    Decision,
    NotificationEvent,
    Reason,
    RiskDecision,
)
from libs.common.stores import redis
from libs.common.topics import (
    ANALYSIS_GRAPH,
    ANALYSIS_ML,
    ANALYSIS_RULES,
    AUDIT,
    CASES,
    DECISIONS,
    NOTIFICATIONS,
    TRANSACTIONS_ENRICHED,
)

log = obs.setup("risk-scoring", metrics_port=9106)
producer = Producer()

BUFFER_TTL = 300
EXPECTED = {"RULES", "ML", "GRAPH"}


# ------------------------------------------------------------------ scoring
def combine(parts: dict[str, dict]) -> tuple[float, list[str]]:
    """Weighted blend that renormalises when a signal is missing, so an outage
    in one analyser shifts weight to the survivors instead of deflating the
    score and silently approving fraud."""
    weights = {"RULES": settings.W_RULES, "ML": settings.W_ML,
               "GRAPH": settings.W_GRAPH}
    present = {k: v for k, v in parts.items() if k in weights}
    missing = sorted(EXPECTED - set(present))
    denom = sum(weights[k] for k in present) or 1.0
    score = sum(weights[k] * float(present[k]["score"]) for k in present) / denom

    # A single unambiguous rule hit must be able to carry the decision alone.
    hard = max((r["weight"] for r in present.get("RULES", {}).get("reasons", [])
                if r["weight"] >= 50), default=0)
    if hard:
        score = max(score, 90.0)

    if missing:
        score = min(score * 1.05 + 3, 100)     # small uncertainty premium
    return round(min(score, 100.0), 2), missing


def to_decision(score: float) -> Decision:
    if score <= settings.APPROVE_MAX:
        return Decision.APPROVE
    if score <= settings.CHALLENGE_MAX:
        return Decision.CHALLENGE
    if score <= settings.REVIEW_MAX:
        return Decision.REVIEW
    return Decision.DECLINE


# ------------------------------------------------------------------ buffer
async def remember_context(value: dict) -> None:
    r = await redis()
    t = value["transaction"]
    await r.hset(f"agg:{t['transaction_id']}", "ctx", json.dumps({
        "customer_id": t["customer_id"], "card_id": t["card_id"],
        "amount": float(t["amount"]), "currency": t["currency"],
        "trace_id": value.get("trace_id"), "t0": time.time(),
    }))
    await r.expire(f"agg:{t['transaction_id']}", BUFFER_TTL)


async def handle(topic: str, value: dict) -> None:
    if topic == TRANSACTIONS_ENRICHED:
        await remember_context(value)
        asyncio.create_task(_deadline(value["transaction"]["transaction_id"]))
        return

    txn_id = value["transaction_id"]
    r = await redis()
    await r.hset(f"agg:{txn_id}", value["source"], json.dumps(value))
    await r.expire(f"agg:{txn_id}", BUFFER_TTL)

    fields = await r.hkeys(f"agg:{txn_id}")
    if EXPECTED.issubset(set(fields)):
        await finalize(txn_id, reason="complete")


async def _deadline(txn_id: str) -> None:
    await asyncio.sleep(settings.DECISION_TIMEOUT_MS / 1000)
    try:
        await finalize(txn_id, reason="timeout")
    except Exception:
        log.exception("deadline finalize failed", extra={"transaction_id": txn_id})


async def finalize(txn_id: str, reason: str) -> None:
    r = await redis()
    # Atomic claim: whoever flips this key first owns the decision.
    if not await r.set(f"decided:{txn_id}", reason, ex=BUFFER_TTL, nx=True):
        return

    raw = await r.hgetall(f"agg:{txn_id}")
    if "ctx" not in raw:
        return
    ctx = json.loads(raw.pop("ctx"))
    parts = {k: json.loads(v) for k, v in raw.items()}

    score, missing = combine(parts)
    decision = to_decision(score)

    reasons = [Reason.model_validate(x)
               for p in parts.values() for x in p.get("reasons", [])]
    reasons.sort(key=lambda x: -x.weight)

    rd = RiskDecision(
        transaction_id=txn_id,
        customer_id=ctx["customer_id"], card_id=ctx["card_id"],
        amount=ctx["amount"], currency=ctx["currency"],
        risk_score=score, decision=decision,
        rules_score=parts.get("RULES", {}).get("score"),
        ml_score=parts.get("ML", {}).get("score"),
        graph_score=parts.get("GRAPH", {}).get("score"),
        reasons=reasons[:8], missing_signals=missing,
        total_latency_ms=round((time.time() - ctx["t0"]) * 1000, 2),
        trace_id=ctx.get("trace_id"),
    )

    await producer.send(DECISIONS, rd, key=txn_id)
    await producer.send(AUDIT, AuditEvent(
        transaction_id=txn_id, event_type="DECISION_MADE",
        payload=json.loads(rd.model_dump_json())), key=txn_id)

    if decision in (Decision.CHALLENGE, Decision.DECLINE):
        await producer.send(NOTIFICATIONS, NotificationEvent(
            transaction_id=txn_id, customer_id=ctx["customer_id"],
            template="step_up_auth" if decision is Decision.CHALLENGE
                     else "txn_declined",
            payload={"amount": ctx["amount"], "currency": ctx["currency"],
                     "risk_score": score}), key=ctx["customer_id"])

    if decision in (Decision.REVIEW, Decision.DECLINE):
        await producer.send(CASES, CaseEvent(
            transaction_id=txn_id, customer_id=ctx["customer_id"],
            priority="P1" if decision is Decision.DECLINE else "P2",
            risk_score=score, reasons=rd.reasons), key=txn_id)

    obs.DECISIONS.labels(decision.value).inc()
    obs.SCORE.observe(score)
    log.info("decision", extra={"transaction_id": txn_id,
                                "decision": decision.value,
                                "risk_score": score,
                                "trace_id": ctx.get("trace_id")})
    await r.delete(f"agg:{txn_id}")


def main() -> None:
    consumer = Consumer(
        TRANSACTIONS_ENRICHED, ANALYSIS_RULES, ANALYSIS_ML, ANALYSIS_GRAPH,
        group="risk-scoring",
    )
    asyncio.run(run_worker(consumer, producer, handle))


if __name__ == "__main__":
    main()

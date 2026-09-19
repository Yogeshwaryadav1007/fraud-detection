"""Rule Engine — deterministic, explainable, sub-millisecond."""
from __future__ import annotations

import asyncio
import time

from rules import evaluate

from libs.common import observability as obs
from libs.common.bus import Consumer, Producer, run_worker
from libs.common.schemas import EnrichedTransaction, Signal
from libs.common.topics import ANALYSIS_RULES, TRANSACTIONS_ENRICHED

log = obs.setup("rule-engine", metrics_port=9103)
producer = Producer()


async def handle(_topic: str, value: dict) -> None:
    t0 = time.perf_counter()
    enriched = EnrichedTransaction.model_validate(value)
    with obs.timed("evaluate"):
        score, reasons = evaluate(enriched)

    signal = Signal(
        transaction_id=enriched.transaction.transaction_id,
        source="RULES",
        score=score,
        reasons=reasons[:10],
        latency_ms=round((time.perf_counter() - t0) * 1000, 3),
        trace_id=enriched.trace_id,
    )
    await producer.send(ANALYSIS_RULES, signal, key=signal.transaction_id)
    obs.EVENTS.labels("rule-engine", ANALYSIS_RULES, "ok").inc()


def main() -> None:
    asyncio.run(run_worker(
        Consumer(TRANSACTIONS_ENRICHED, group="rule-engine"), producer, handle))


if __name__ == "__main__":
    main()

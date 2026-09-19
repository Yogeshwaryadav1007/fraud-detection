"""ML Service — unsupervised anomaly detection with per-feature attribution.

Model: IsolationForest wrapped in a StandardScaler pipeline, trained offline by
ml/train.py. If the artefact is missing the service degrades to a heuristic and
flags `degraded=true` so risk-scoring can widen its confidence band instead of
silently trusting a fake score.
"""
from __future__ import annotations

import asyncio
import time

import joblib
import numpy as np

from features import FEATURE_NAMES, vectorize
from libs.common import observability as obs
from libs.common.config import settings
from libs.common.bus import Consumer, Producer, run_worker
from libs.common.schemas import EnrichedTransaction, Reason, Signal
from libs.common.topics import ANALYSIS_ML, TRANSACTIONS_ENRICHED

log = obs.setup("ml-service", metrics_port=9104)
producer = Producer()

MODEL = None
MODEL_VERSION = "fallback-v0"
BASELINE: np.ndarray | None = None


def load_model() -> None:
    global MODEL, MODEL_VERSION, BASELINE
    try:
        bundle = joblib.load(settings.MODEL_PATH)
        MODEL = bundle["pipeline"]
        MODEL_VERSION = bundle.get("version", "unknown")
        BASELINE = np.asarray(bundle["baseline_median"], dtype=float)
        log.info("model loaded: %s", MODEL_VERSION)
    except Exception as exc:                                # noqa: BLE001
        log.warning("model unavailable (%s) — running degraded", exc)


def _to_score(raw: float) -> float:
    """IsolationForest decision_function: positive = normal, negative = outlier.
    Squash into 0-100 where higher means more anomalous."""
    return float(np.clip(50.0 - raw * 120.0, 0.0, 100.0))


def attribute(x: np.ndarray, k: int = 4) -> list[Reason]:
    """Cheap, deterministic attribution: which features deviate most from the
    training median, in units of the scaler's own standard deviation."""
    if MODEL is None or BASELINE is None:
        return []
    scaler = MODEL.named_steps["scaler"]
    z = (x - BASELINE) / np.where(scaler.scale_ == 0, 1, scaler.scale_)
    order = np.argsort(-np.abs(z))[:k]
    return [
        Reason(
            code=f"ML_{FEATURE_NAMES[i].upper()}",
            description=f"{FEATURE_NAMES[i]} deviates {z[i]:+.1f} sigma from the norm",
            weight=round(float(abs(z[i])), 2),
            evidence={"feature": FEATURE_NAMES[i],
                      "value": round(float(x[i]), 3),
                      "baseline": round(float(BASELINE[i]), 3),
                      "z": round(float(z[i]), 2)},
        )
        for i in order if abs(z[i]) > 1.0
    ]


def heuristic(e: EnrichedTransaction) -> float:
    """Used only when the model artefact is missing."""
    s = 0.0
    s += min(e.velocity.txn_count_1h * 2.0, 30)
    if e.profile.avg_ticket:
        s += min(float(e.transaction.amount) / e.profile.avg_ticket * 8, 40)
    s += 15 if e.is_new_device else 0
    s += 10 if e.is_foreign else 0
    return min(s, 100.0)


async def handle(_topic: str, value: dict) -> None:
    t0 = time.perf_counter()
    e = EnrichedTransaction.model_validate(value)

    with obs.timed("inference"):
        if MODEL is not None:
            x = np.asarray(vectorize(e), dtype=float)
            raw = float(MODEL.decision_function(x.reshape(1, -1))[0])
            score, reasons, degraded = _to_score(raw), attribute(x), False
        else:
            obs.DEGRADED.labels("ML").inc()
            score, reasons, degraded = heuristic(e), [], True

    signal = Signal(
        transaction_id=e.transaction.transaction_id,
        source="ML",
        score=score,
        reasons=reasons,
        latency_ms=round((time.perf_counter() - t0) * 1000, 3),
        degraded=degraded,
        trace_id=e.trace_id,
    )
    await producer.send(ANALYSIS_ML, signal, key=signal.transaction_id)
    obs.EVENTS.labels("ml-service", ANALYSIS_ML, "ok").inc()


def main() -> None:
    load_model()
    asyncio.run(run_worker(
        Consumer(TRANSACTIONS_ENRICHED, group="ml-service"), producer, handle))


if __name__ == "__main__":
    main()

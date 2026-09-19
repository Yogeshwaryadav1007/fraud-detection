"""Structured JSON logs, Prometheus metrics, optional OpenTelemetry tracing."""
from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import contextmanager

from prometheus_client import Counter, Histogram, start_http_server

from .config import settings

# ------------------------------------------------------------------ metrics
EVENTS = Counter(
    "fraud_events_total", "Events handled", ["service", "topic", "outcome"]
)
LATENCY = Histogram(
    "fraud_stage_latency_seconds",
    "Per-stage processing latency",
    ["service", "stage"],
    buckets=(.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5),
)
DECISIONS = Counter(
    "fraud_decisions_total", "Final decisions", ["decision"]
)
SCORE = Histogram(
    "fraud_risk_score", "Distribution of final risk scores",
    buckets=(0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100),
)
DEGRADED = Counter(
    "fraud_degraded_total", "Times a scorer fell back to a default", ["source"]
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "service": settings.SERVICE_NAME,
            "env": settings.ENV,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for k in ("transaction_id", "trace_id", "decision", "risk_score"):
            if hasattr(record, k):
                base[k] = getattr(record, k)
        if record.exc_info:
            base["exc"] = self.formatException(record.exc_info)
        return json.dumps(base)


def setup(service_name: str, metrics_port: int | None = None) -> logging.Logger:
    settings.SERVICE_NAME = service_name
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(settings.LOG_LEVEL)

    try:
        start_http_server(metrics_port or settings.METRICS_PORT)
    except OSError:
        pass  # already bound (e.g. uvicorn reload)

    if settings.TRACING_ENABLED:
        _init_tracing(service_name)
    return logging.getLogger(service_name)


def _init_tracing(service_name: str) -> None:
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        provider = TracerProvider(
            resource=Resource.create({"service.name": service_name})
        )
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.OTEL_ENDPOINT,
                                                insecure=True))
        )
        trace.set_tracer_provider(provider)
    except Exception:                                       # noqa: BLE001
        logging.getLogger(__name__).warning("tracing unavailable, continuing")


@contextmanager
def timed(stage: str):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        LATENCY.labels(settings.SERVICE_NAME, stage).observe(
            time.perf_counter() - t0
        )

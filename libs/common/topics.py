"""Single source of truth for Kafka topic names, partitions and retention."""
from dataclasses import dataclass


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    replication: int
    retention_ms: int
    key: str          # what the partition key is
    purpose: str


TRANSACTIONS_RAW = "txn.raw.v1"
TRANSACTIONS_ENRICHED = "txn.enriched.v1"
ANALYSIS_RULES = "analysis.rules.v1"
ANALYSIS_ML = "analysis.ml.v1"
ANALYSIS_GRAPH = "analysis.graph.v1"
DECISIONS = "decision.final.v1"
NOTIFICATIONS = "notification.outbound.v1"
CASES = "case.created.v1"
AUDIT = "audit.event.v1"
DLQ = "dlq.v1"

DAY = 86_400_000

CATALOG = [
    TopicSpec(TRANSACTIONS_RAW, 12, 3, 7 * DAY, "card_id",
              "Validated transactions straight out of ingestion."),
    TopicSpec(TRANSACTIONS_ENRICHED, 12, 3, 7 * DAY, "card_id",
              "Transaction + velocity counters + customer profile + geo."),
    TopicSpec(ANALYSIS_RULES, 12, 3, 3 * DAY, "transaction_id",
              "Deterministic rule hits and partial score."),
    TopicSpec(ANALYSIS_ML, 12, 3, 3 * DAY, "transaction_id",
              "Anomaly probability + top SHAP-style feature contributions."),
    TopicSpec(ANALYSIS_GRAPH, 12, 3, 3 * DAY, "transaction_id",
              "Ring/shared-device/shared-IP findings from Neo4j."),
    TopicSpec(DECISIONS, 12, 3, 30 * DAY, "transaction_id",
              "Final explainable score + APPROVE/CHALLENGE/REVIEW/DECLINE."),
    TopicSpec(NOTIFICATIONS, 6, 3, 3 * DAY, "customer_id",
              "Outbound push/SMS/email fan-out."),
    TopicSpec(CASES, 6, 3, 90 * DAY, "case_id",
              "Investigation cases for the analyst queue."),
    TopicSpec(AUDIT, 6, 3, 365 * DAY, "transaction_id",
              "Immutable, hash-chained audit trail. Compacted never; append only."),
    TopicSpec(DLQ, 6, 3, 30 * DAY, "transaction_id",
              "Poison messages from any consumer, with error metadata."),
]

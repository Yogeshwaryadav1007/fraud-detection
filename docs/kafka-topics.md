# Kafka Topology

## Catalog

| Topic | Partitions | RF | Retention | Key | Producer → Consumers |
|---|---|---|---|---|---|
| `txn.raw.v1` | 12 | 3 | 7d | `card_id` | ingestion → enrichment |
| `txn.enriched.v1` | 12 | 3 | 7d | `card_id` | enrichment → rule-engine, ml-service, graph-service, risk-scoring |
| `analysis.rules.v1` | 12 | 3 | 3d | `transaction_id` | rule-engine → risk-scoring |
| `analysis.ml.v1` | 12 | 3 | 3d | `transaction_id` | ml-service → risk-scoring |
| `analysis.graph.v1` | 12 | 3 | 3d | `transaction_id` | graph-service → risk-scoring |
| `decision.final.v1` | 12 | 3 | 30d | `transaction_id` | risk-scoring → audit, downstream |
| `notification.outbound.v1` | 6 | 3 | 3d | `customer_id` | risk-scoring → notification |
| `case.created.v1` | 6 | 3 | 90d | `case_id` | risk-scoring → case-management |
| `audit.event.v1` | 6 | 3 | 365d | `transaction_id` | all → audit |
| `dlq.v1` | 6 | 3 | 30d | `transaction_id` | all → manual triage |

## Why the key changes mid-pipeline

Before enrichment, **order within a card matters**. Velocity counters are
stateful; two transactions on the same card processed out of order produce
wrong counts. Keying on `card_id` puts them on one partition, which Kafka
orders.

After enrichment, **order stops mattering and co-location starts**. The three
analyser outputs for one transaction must reach the same risk-scoring consumer
instance, so the key becomes `transaction_id`.

## Sizing

12 partitions supports roughly 12 concurrent consumers per group. At 5,000
tps and ~2 ms of rule evaluation per event, one consumer handles ~500 eps, so
12 gives headroom of about 2.4x. Partition count can only grow, never shrink,
so start higher than you think you need.

## Delivery semantics

At-least-once. Producers use `acks=all` with idempotence enabled; consumers
commit only after the handler returns. Every consumer must therefore be
idempotent:
- risk-scoring: `SET NX` on `decided:{txn_id}` makes duplicate finalisation a no-op
- ingestion, audit, case-management: `ON CONFLICT DO NOTHING`
- notification: per-customer rate limit caps duplicate sends

Exactly-once via Kafka transactions was considered and rejected — the
throughput cost is roughly 20 to 30% and idempotent handlers give the same
observable guarantee here.

## Consumer groups

`fraud.enrichment` · `fraud.rule-engine` · `fraud.ml-service` ·
`fraud.graph-service` · `fraud.risk-scoring` · `fraud.notification` ·
`fraud.case-management` · `fraud.audit`

Each analyser is its own group, so all three receive every enriched event
independently. This is the fan-out that makes parallel scoring possible.

## Failure handling

A handler exception publishes the original payload plus error metadata to
`dlq.v1` and commits the offset. The partition keeps moving. DLQ depth is
alerted on; replay is a manual, reviewed operation.

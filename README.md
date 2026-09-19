# Real-Time Fraud Detection Platform

A microservices replacement for a batch fraud-detection system. A transaction
enters through the API Gateway, is enriched with velocity and profile features,
scored in parallel by a rule engine, an anomaly model and a graph traversal,
then combined into a single explainable risk score and a decision:
**APPROVE · CHALLENGE · REVIEW · DECLINE**.

## Pipeline

```
                                 ┌──────────────┐
  acquirer ──▶ API Gateway ──▶  Ingestion  ──▶ txn.raw.v1
                                 └──────────────┘
                                        │
                                  Enrichment  (Redis velocity + Postgres profile)
                                        │
                                 txn.enriched.v1
                     ┌──────────────────┼──────────────────┐
                     ▼                  ▼                  ▼
              Rule Engine          ML Service        Graph Service
              (15 rules)        (IsolationForest)      (Neo4j)
                     │                  │                  │
              analysis.rules     analysis.ml       analysis.graph
                     └──────────────────┼──────────────────┘
                                        ▼
                                  Risk Scoring
                          (weighted blend + 800 ms deadline)
                                        │
                     ┌──────────────────┼──────────────────┐
                     ▼                  ▼                  ▼
               Notification      Case Management         Audit
                                                   (hash-chained, WORM)
```

## Quick start

```bash
cp .env.example .env
make up          # docker compose up + create topics  (~2 min first run)
make seed        # customer profiles + a demo fraud ring in Neo4j
make train       # train the anomaly model
make loadtest    # 200 rps for 60 seconds
```

| Service | URL |
|---|---|
| API Gateway | http://localhost:8000 |
| Case Management | http://localhost:8002/v1/cases |
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090 |
| Jaeger | http://localhost:16686 |
| Neo4j Browser | http://localhost:7474 (neo4j/fraudgraph) |

Send one transaction:

```bash
curl -X POST localhost:8000/v1/transactions \
  -H 'content-type: application/json' -H 'x-api-key: demo-key-1' \
  -d '{"card_id":"card_ring_5","account_id":"acc_1","customer_id":"cust_1",
       "merchant_id":"mer_9","merchant_country":"KP","amount":95000,
       "currency":"INR","channel":"ECOM","device_id":"dev_shared_1",
       "ip_address":"203.0.113.77","latitude":23.36,"longitude":85.33}'
```

That transaction hits a sanctioned country, a shared device in a known ring,
and an amount far above the customer's average — it should come back DECLINE
with a populated `reasons` array.

## Layout

```
libs/common/       schemas, topic catalog, Kafka wrapper, metrics, store pools
services/          10 services, one directory each
ml/train.py        offline model training
neo4j/             constraints + indexes
db/schema.sql      partitioned tables, immutable audit trigger
k8s/               namespace, config, deployments, Istio, alerts
scripts/           topic creation, seeding, load generation
tests/             rule and scoring unit tests
docs/              15-day plan, architecture, API and Kafka contracts
```

## Read next

- **[docs/15-day-plan.md](docs/15-day-plan.md)** — the Day 1→15 build order,
  with the artefact and the completion check for each day.
- [docs/kafka-topics.md](docs/kafka-topics.md) — partitioning rationale and
  delivery semantics.
- [docs/api-contracts.md](docs/api-contracts.md) — REST surface.

## Known limitations

These are deliberate and should be stated, not hidden:

- The ML model trains on **synthetic data** from `ml/train.py`. The reported
  ROC-AUC of ~1.0 reflects a trivially separable generator, not real
  performance. Replace `synth()` with a historical extract before drawing any
  conclusion about detection quality.
- Score thresholds (30/60/85) are **unvalidated** against real fraud rates and
  need tuning against a labelled holdout.
- The notification service's `deliver()` is a stub; wire it to FCM/Twilio/SES.
- Postgres, Kafka and Neo4j run single-node in compose. The `k8s/` manifests
  assume managed or operator-deployed clusters.
- `k8s/02-deployments.yaml` contains one fully-specified Deployment as the
  template; the other nine follow the same shape with the name and metrics port
  changed.

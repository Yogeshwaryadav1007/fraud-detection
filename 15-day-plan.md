# Real-Time Fraud Detection Platform — Day 1 to Day 15

Each day lists the artefact you must produce, the file it lives in, and the
check that proves the day is done. Nothing is marked complete on the strength
of a diagram alone — every design day has a corresponding artefact in the repo.

**Repository:** `fraud-platform/`
**Time budget:** roughly 6 to 7 focused hours per day.

---

## Week 1 — Foundations and the core pipeline

### Day 1 — Domain, scope and the legacy teardown
**Build**
- `docs/architecture.md` sections 1 to 3: business context, what the legacy
  batch system does today, and the specific failures you are fixing.
- A one-page context diagram: acquirer → platform → core banking / analyst UI.
- Non-functional requirements written as numbers, not adjectives.

**Numbers to commit to now, because everything downstream depends on them**

| Requirement | Target |
|---|---|
| Peak throughput | 5,000 transactions/sec |
| p99 end-to-end decision latency | under 1 second |
| Availability | 99.95% (≈22 min/month) |
| False positive rate | under 2% of approved volume |
| Fraud recall | above 85% of confirmed fraud |
| Audit retention | 7 years, immutable |
| RPO / RTO | 5 min / 30 min |

**Done when:** a reader who has never seen the project can state, from your
document alone, why batch scoring is insufficient and what "real-time" means
numerically here.

---

### Day 2 — Service decomposition and event contracts
**Build**
- `libs/common/schemas.py` — every Pydantic model on the wire. ✅ *in repo*
- `libs/common/topics.py` — the topic catalog with partitions and retention. ✅
- `docs/api-contracts.md` — the REST surface.
- A C4 Level 2 container diagram (10 services, 5 datastores).

**The decomposition, and the reason for each split**

| Service | Why it is its own deployable |
|---|---|
| API Gateway | Edge concerns change on a different cadence from business logic |
| Ingestion | Must stay available and fast even when scorers are down |
| Enrichment | CPU-light, IO-heavy — scales on a different curve |
| Rule Engine | Changes daily; must ship without redeploying ML |
| ML Service | Needs GPU/memory profile and its own model release cycle |
| Graph Service | The slowest leg; must be isolated so it can't stall the rest |
| Risk Scoring | The only place policy lives; single audit point |
| Case Management | Stateful, analyst-facing, low traffic |
| Notification | Fan-out with third-party dependencies and retry storms |
| Audit | Write-once, compliance-owned, separate blast radius |

**Done when:** `pytest tests/` passes and every event a service emits has a
model that validates it.

---

### Day 3 — Kafka topology
**Build**
- `scripts/create_topics.sh` ✅
- `docs/kafka-topics.md` — the partitioning rationale.
- `libs/common/bus.py` — producer, consumer, DLQ wrapper. ✅

**Decisions to write down and defend**
- **Partition key is `card_id` for `txn.raw` and `txn.enriched`.** All
  transactions for one card land on one partition, so velocity counters are
  computed in order. Keying by `transaction_id` here would let two transactions
  on the same card be enriched out of order and produce wrong counters.
- **Partition key switches to `transaction_id` for `analysis.*`.** After
  enrichment, ordering no longer matters; what matters is that all three
  analyser outputs for one transaction can be aggregated, and they are keyed to
  co-locate.
- **Manual commit, commit-after-handle.** At-least-once delivery. Every
  consumer must therefore be idempotent — the Redis `nx` claim in risk-scoring
  and the `ON CONFLICT DO NOTHING` inserts are what make that true.
- **DLQ instead of infinite retry.** One poison message must never halt a
  partition.

**Done when:** `make topics` is idempotent and `docker compose up` produces a
message that travels from ingestion to risk-scoring.

---

### Day 4 — Ingestion and the API Gateway
**Build**
- `services/ingestion/main.py` ✅
- `services/api-gateway/main.py` ✅

**The three things ingestion must get right**
1. **Idempotency.** Acquirers retry. A Redis `SET NX` on the idempotency key
   returns `DUPLICATE` instead of double-charging the pipeline.
2. **Fail open at the edge, never fail silent.** Ingestion returns `202` as
   soon as the event is durably in Kafka, not after scoring.
3. **Correlation ID from the first hop.** `x-request-id` becomes `trace_id` and
   rides every downstream event, which is what makes a single transaction
   traceable across ten services.

**Done when:** `curl -X POST localhost:8000/v1/transactions` returns `202`, and
sending the same `idempotency-key` twice returns `DUPLICATE`.

---

### Day 5 — Enrichment and the feature store
**Build**
- `services/enrichment/main.py` ✅

**The Redis data structures, and why each one**

| Structure | Key | Purpose |
|---|---|---|
| Sorted set | `vel:z:{card}` | Sliding-window counts. Score is the timestamp, so a single `ZRANGEBYSCORE` gives any window; `ZREMRANGEBYSCORE` trims it. |
| HyperLogLog | `vel:merch:{card}` | Distinct merchants in 24h at ~12 KB regardless of volume. Exact counts would need unbounded sets. |
| Hash | `last:{card}` | Previous timestamp and coordinates, for the impossible-travel calculation. |
| Set | `seen:dev:{card}` | Device history, for the new-device flag. |
| String w/ TTL | `prof:{customer}` | 15-minute cache of the Postgres profile. |

All three lookups run under `asyncio.gather`, so enrichment latency is the
slowest single call, not their sum.

**Done when:** an enriched event carries non-zero velocity counters after you
send five transactions on the same card.

---

## Week 2 — Detection, decision and the platform around them

### Day 6 — Rule Engine
**Build**
- `services/rule-engine/rules.py` — 15 rules as data ✅
- `services/rule-engine/main.py` ✅
- `tests/test_rules.py` — positive and negative case per rule ✅

**Design point worth defending at the board:** rules are a `list[Rule]` of pure
predicates, not `if` statements inside the consumer. That is what lets a fraud
analyst add a rule without touching the pipeline, lets each rule be unit
tested in isolation, and — via the `try/except` in `evaluate` — guarantees a
malformed rule degrades one signal instead of taking down the service.

**Done when:** `pytest tests/test_rules.py` is green and a sanctioned-country
transaction alone produces a DECLINE.

---

### Day 7 — ML Service
**Build**
- `services/ml-service/features.py` — the 22-feature contract ✅
- `ml/train.py` ✅
- `services/ml-service/main.py` ✅

**Run:** `python ml/train.py --rows 200000 --out ml/model.joblib`

**Be honest about this in the presentation.** `ml/train.py` ships a synthetic
generator so the pipeline runs on day one. On synthetic data it reports
ROC-AUC ≈ 1.0, which means nothing — the generator makes fraud trivially
separable. The number to present is the architecture around the model, not the
score: a shared feature module that makes training/serving skew structurally
impossible, a versioned artefact bundle, attribution output, and an explicit
`degraded` flag when the artefact is missing. Before any real deployment,
replace `synth()` with a historical extract and re-measure PR-AUC, which is the
metric that matters at a 1.5% base rate.

**Why IsolationForest rather than a supervised classifier:** labels arrive
60 to 90 days late via chargebacks, so a supervised model is always training on
a stale definition of fraud. Unsupervised detection catches novel patterns on
day one. The production answer is both — this model plus a supervised
gradient-boosted model once labels mature.

**Done when:** `ml/model.joblib` exists and ml-service logs a real version
string instead of `fallback-v0`.

---

### Day 8 — Graph Service and Neo4j
**Build**
- `neo4j/init.cypher` — constraints and indexes ✅
- `services/graph-service/queries.py` — 7 Cypher queries ✅
- `services/graph-service/main.py` ✅
- `scripts/seed.py` — seeds a 12-card ring ✅

**Graph model:** `Customer -OWNS-> Account -HAS_CARD-> Card -MADE-> Transaction
-AT_MERCHANT-> Merchant`, with `Card -USED_DEVICE-> Device` and
`Card -USED_IP-> IP` as the shared-entity edges that actually reveal rings.

**The hard constraint:** graph traversal is the slowest leg. It runs under a
450 ms `asyncio.wait_for` budget. Exceeding it emits a zero-score signal with
`degraded=true` rather than stalling the decision — and risk-scoring then
renormalises the remaining weights instead of silently scoring the transaction
lower.

**Done when:** `make seed` then a transaction on `card_ring_5` returns G001 and
G003 findings.

---

### Day 9 — Risk Scoring and explainability
**Build**
- `services/risk-scoring/main.py` ✅
- `tests/test_scoring.py` ✅

**The aggregation problem and its solution.** Three analysers publish
independently with different latencies. Risk-scoring buffers partial results in
a Redis hash keyed by `transaction_id`, and decides on whichever comes first:
all three signals arrive, or the 800 ms deadline fires. A `SET NX` on
`decided:{txn_id}` makes the race safe — exactly one path wins.

**Three scoring rules you should be ready to justify:**
1. **Renormalise on missing signals.** If graph is down, its weight is
   redistributed rather than treated as a zero score. Treating absent as zero
   would mean an outage silently approves fraud.
2. **Uncertainty premium.** A decision made on partial information gets a small
   upward adjustment. Incomplete information should bias toward friction, not
   toward approval.
3. **Hard rules override the blend.** A sanctioned-country hit forces a score
   of at least 90 regardless of what ML and graph say.

**Thresholds:** ≤30 APPROVE · ≤60 CHALLENGE · ≤85 REVIEW · >85 DECLINE.
These are policy, not code — they live in the ConfigMap and change without a
deploy.

**Done when:** `pytest tests/test_scoring.py` is green and a decision event
carries `reasons` explaining the score in plain language.

---

### Day 10 — Notification, Case Management, Audit
**Build**
- `services/notification/main.py` — rate limited, retries with backoff ✅
- `services/case-management/main.py` — analyst queue API ✅
- `services/audit/main.py` — hash-chained ✅
- `db/schema.sql` ✅

**The audit design is the compliance centrepiece.** Each row stores
`SHA256(prev_hash || canonical_payload)`. Altering any historical row breaks
every hash after it, so tampering is detectable rather than merely discouraged.
A Postgres trigger raises on `UPDATE` and `DELETE`, so immutability is enforced
by the database and not by application discipline. Rows mirror to Elasticsearch
for investigator search.

**Done when:** `GET /v1/cases?status=OPEN` returns cases created by DECLINE
decisions, and `UPDATE audit_log` raises an exception.

---

## Week 3 — Production readiness and delivery

### Day 11 — Containers and Kubernetes
**Build**
- `Dockerfile` — one parameterised multi-stage image ✅
- `docker-compose.yml` — full local stack ✅
- `k8s/00-namespace.yaml`, `01-config.yaml`, `02-deployments.yaml` ✅

**Container hardening to point at:** non-root user 10001, read-only root
filesystem, all capabilities dropped, `seccompProfile: RuntimeDefault`, pinned
base image, distinct liveness and readiness probes, `maxUnavailable: 0` on
rollout, PodDisruptionBudget, zone spread.

**Autoscaling:** CPU alone is the wrong signal for a queue consumer — a backed
up service can sit at 40% CPU while lag explodes. The HPA therefore scales on
`kafka_consumergroup_lag` as well, scales up aggressively (100% per 30 s) and
down slowly (25% per 60 s).

**Done when:** `make up` brings the stack to healthy and `kubectl apply -f k8s/`
validates with `--dry-run=server`.

---

### Day 12 — Istio, security, compliance
**Build**
- `k8s/03-istio.yaml` ✅
- `docs/architecture.md` security and compliance sections.

**Security layers, outermost inward:** WAF and TLS at the ingress gateway ·
API key plus per-key rate limit at the gateway · STRICT mTLS between every pod ·
`AuthorizationPolicy` restricting who may call ingestion · default-deny
NetworkPolicy · secrets from Vault via ExternalSecrets, never in Git ·
PII encrypted at rest, card numbers tokenised and never logged ·
RBAC with a dedicated ServiceAccount per service.

**Compliance mapping to state explicitly:**

| Regulation | Where it is satisfied |
|---|---|
| PCI-DSS 3.4 | PAN never enters the platform; only `card_id` tokens |
| PCI-DSS 10.x | Hash-chained `audit_log`, 7-year retention |
| GDPR Art. 22 | `reasons[]` on every decision gives the right to explanation |
| GDPR Art. 17 | Erasure via crypto-shredding of the per-customer key |
| RBI / AML | Case management trail, SAR-ready export |
| Model governance | Versioned artefact, metrics recorded per version |

**Done when:** Istio shows 100% mTLS in Kiali and the compliance table maps
every control to a file in the repo.

---

### Day 13 — Observability and SLOs
**Build**
- `k8s/prometheus.yml`, `k8s/04-alerts.yaml` ✅
- `libs/common/observability.py` — metrics, JSON logs, OTel ✅
- A Grafana dashboard JSON with four rows.

**The four golden dashboards:** business (decision mix, decline rate, score
distribution) · pipeline (per-stage latency, Kafka lag, DLQ depth) ·
infrastructure (CPU, memory, pod restarts) · model health (degraded counter,
feature drift, score distribution shift).

**The alert that matters most** is `DeclineRateSpike`. A bad rule or a bad model
rollout does not crash anything — it quietly declines good customers. Latency
alerts will not catch that; the decline-rate ratio will.

**Done when:** a load test shows traffic on every dashboard and one trace in
Jaeger spans gateway → ingestion → enrichment → all three scorers → decision.

---

### Day 14 — CI/CD and disaster recovery
**Build**
- `.github/workflows/ci.yml` ✅
- `docs/architecture.md` DR section with a tested runbook.

**Pipeline:** lint and type check → unit tests against live Redis/Postgres →
`pip-audit`, `gitleaks`, `bandit` → matrix build of 10 images → Trivy scan with
`exit-code: 1` on HIGH → staging deploy with smoke test → manual approval →
10% canary → 15-minute error-budget watch → promote.

**DR posture:** RPO 5 min / RTO 30 min. Kafka MirrorMaker 2 replicates to a
secondary region · Postgres streaming replication with PITR · Neo4j nightly
full plus hourly incremental · Redis is deliberately treated as disposable,
because velocity counters rebuild from the Kafka log within minutes.

**The failure modes to walk through, each with its documented response:**
Kafka broker loss (RF=3 absorbs it) · Neo4j down (graph degrades, pipeline
continues) · model artefact corrupt (fallback heuristic plus `degraded` flag) ·
Redis flush (counters rebuild by replay) · full region loss (DNS failover, warm
standby).

**Done when:** the pipeline is green end to end and you have personally run the
Neo4j-down scenario and watched decisions keep flowing.

---

### Day 15 — Architecture document and board presentation
**Build**
- `docs/architecture.md` complete, roughly 40 to 60 pages with diagrams.
- An 18-slide board deck.

**Deck structure, with the time you get for each**

| # | Slide | Minutes |
|---|---|---|
| 1 | Title | 0.5 |
| 2 | The problem: current fraud losses and batch latency | 2 |
| 3 | What we are building, in one sentence | 1 |
| 4 | Architecture overview diagram | 3 |
| 5 | Transaction journey: 900 ms, end to end | 3 |
| 6 | Three detectors, and why one is never enough | 3 |
| 7 | Explainability: a real decision, in plain English | 2 |
| 8 | Graph detection: the ring we found | 2 |
| 9 | Scale and performance numbers | 2 |
| 10 | Reliability: what happens when a component dies | 2 |
| 11 | Security layers | 2 |
| 12 | Regulatory compliance mapping | 2 |
| 13 | Observability and SLOs | 1.5 |
| 14 | CI/CD and release safety | 1.5 |
| 15 | Disaster recovery | 1.5 |
| 16 | Cost model | 2 |
| 17 | Risks and open questions | 2 |
| 18 | Roadmap and the decision we need from you | 2 |

**Slide 17 is the one that earns credibility.** Name the real risks: the model
is trained on synthetic data until a historical extract is available; the graph
service is the latency bottleneck and the first thing to degrade under load;
thresholds are unvalidated against production fraud rates; the false-positive
target of 2% is an assumption, not a measurement. A board that hears only good
news stops believing the good news.

**Done when:** you can run the full demo — load test, live dashboard, one
DECLINE with its explanation, one Neo4j kill showing graceful degradation — in
under eight minutes without touching a terminal you have not rehearsed.

---

## Sequencing notes

- Days 6, 7 and 8 are independent of each other. If you fall behind, build the
  Rule Engine properly and let the ML and Graph services ship thinner —
  deterministic rules carry the demo, and the architecture is what is being
  assessed.
- Day 9 depends on all three of 6, 7 and 8 emitting something. Stub the signals
  if needed rather than blocking.
- Days 11 to 14 can be compressed to two days if Week 2 overruns. Do not
  compress Day 15.
- Write `docs/architecture.md` incrementally from Day 1. Leaving it to Day 15
  is the single most common way this kind of sprint fails.

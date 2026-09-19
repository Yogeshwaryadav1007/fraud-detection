# API Contracts

Base URL: `https://api.fraud.example.com`
Auth: `X-API-Key` header. All endpoints accept and echo `X-Request-Id`.

---

## POST /v1/transactions
Submit a transaction for scoring. Returns as soon as the event is durable in
Kafka — scoring is asynchronous.

**Headers:** `X-API-Key` (required) · `Idempotency-Key` (recommended)

```json
{
  "card_id": "card_88421",
  "account_id": "acc_88421",
  "customer_id": "cust_88421",
  "merchant_id": "mer_413",
  "merchant_category": "5411",
  "merchant_country": "IN",
  "amount": 12500.00,
  "currency": "INR",
  "channel": "ECOM",
  "device_id": "dev_a91f",
  "ip_address": "203.0.113.77",
  "latitude": 23.3606,
  "longitude": 85.3346
}
```

**202 Accepted**
```json
{"transaction_id": "txn_a3f9...", "status": "ACCEPTED", "ingest_latency_ms": 4.2}
```

**200 OK** — replay of a seen `Idempotency-Key`
```json
{"transaction_id": "txn_a3f9...", "status": "DUPLICATE"}
```

**Errors:** `401` invalid key · `422` validation failure with field detail ·
`429` rate limit, with `Retry-After`

---

## GET /v1/decisions/{transaction_id}
```json
{
  "transaction_id": "txn_a3f9",
  "risk_score": 87.4,
  "decision": "DECLINE",
  "rules_score": 95.0,
  "ml_score": 71.2,
  "graph_score": 80.0,
  "reasons": [
    {"code": "R004", "description": "Impossible travel: >500 km in under 30 minutes",
     "weight": 40, "evidence": {"km": 1840.2, "seconds": 612}},
    {"code": "G001", "description": "Device shared by 7 different cards",
     "weight": 45, "evidence": {"device_id": "dev_shared_1", "card_count": 7}},
    {"code": "ML_AMOUNT_OVER_AVG",
     "description": "amount_over_avg deviates +4.2 sigma from the norm",
     "weight": 4.2, "evidence": {"value": 12.4, "baseline": 1.02, "z": 4.2}}
  ],
  "missing_signals": [],
  "total_latency_ms": 412.8
}
```

The `reasons` array is the GDPR Article 22 artefact. Every decision must be
explainable in language a customer-service agent can read aloud.

---

## GET /v1/cases
Query: `status` (OPEN·IN_REVIEW·CLOSED) · `priority` (P1·P2·P3) · `limit` (≤200)
Ordered by `risk_score DESC, created_at ASC` — highest risk, oldest first.

## PATCH /v1/cases/{case_id}
Body: `status`, `analyst`, `note`. Emits a `CASE_UPDATED` audit event.

---

## Health and metrics
Every service exposes `/health/live` and `/health/ready` on 8080, and Prometheus
metrics on its own port in the 9100–9109 range.

## Error envelope
```json
{"error": {"code": "VALIDATION_FAILED", "message": "amount must be > 0",
           "request_id": "req_7f2a", "timestamp": "2026-09-18T10:22:01Z"}}
```

## Rate limits
600 requests/minute per API key at the gateway, sliding 60-second window.
Exceeding it returns `429` with `Retry-After` in seconds.

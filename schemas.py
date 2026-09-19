"""Event contracts. These are the payloads that travel on Kafka."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


def now() -> datetime:
    return datetime.now(timezone.utc)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:20]}"


class Decision(str, Enum):
    APPROVE = "APPROVE"
    CHALLENGE = "CHALLENGE"
    REVIEW = "REVIEW"
    DECLINE = "DECLINE"


class Channel(str, Enum):
    CARD_PRESENT = "CARD_PRESENT"
    ECOM = "ECOM"
    ATM = "ATM"
    P2P = "P2P"
    WALLET = "WALLET"


# ---------------------------------------------------------------- inbound API
class TransactionRequest(BaseModel):
    """What the acquirer / core banking system POSTs to the gateway."""
    transaction_id: str = Field(default_factory=lambda: new_id("txn"))
    card_id: str
    account_id: str
    customer_id: str
    merchant_id: str
    merchant_category: str = "5999"
    merchant_country: str = "IN"
    amount: Decimal
    currency: str = "INR"
    channel: Channel = Channel.ECOM
    device_id: str | None = None
    ip_address: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    occurred_at: datetime = Field(default_factory=now)

    @field_validator("amount")
    @classmethod
    def positive(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("amount must be > 0")
        return v

    @field_validator("currency")
    @classmethod
    def iso(cls, v: str) -> str:
        if len(v) != 3:
            raise ValueError("currency must be ISO-4217 alpha-3")
        return v.upper()


# ---------------------------------------------------------------- pipeline
class VelocityCounters(BaseModel):
    txn_count_1m: int = 0
    txn_count_1h: int = 0
    txn_count_24h: int = 0
    amount_sum_1h: float = 0.0
    amount_sum_24h: float = 0.0
    distinct_merchants_24h: int = 0
    distinct_countries_24h: int = 0
    distinct_devices_24h: int = 0


class CustomerProfile(BaseModel):
    customer_id: str
    tenure_days: int = 0
    risk_band: Literal["LOW", "MEDIUM", "HIGH"] = "LOW"
    avg_ticket: float = 0.0
    std_ticket: float = 0.0
    home_country: str = "IN"
    kyc_verified: bool = True
    chargebacks_12m: int = 0


class EnrichedTransaction(BaseModel):
    transaction: TransactionRequest
    velocity: VelocityCounters = VelocityCounters()
    profile: CustomerProfile
    seconds_since_prev_txn: float | None = None
    km_from_prev_txn: float | None = None
    is_new_device: bool = False
    is_new_merchant: bool = True
    is_foreign: bool = False
    enriched_at: datetime = Field(default_factory=now)
    trace_id: str | None = None


class Signal(BaseModel):
    """Common envelope emitted by rule-engine / ml-service / graph-service."""
    transaction_id: str
    source: Literal["RULES", "ML", "GRAPH"]
    score: float = Field(ge=0, le=100)
    reasons: list[Reason] = []
    latency_ms: float = 0.0
    degraded: bool = False          # true when the service fell back
    produced_at: datetime = Field(default_factory=now)
    trace_id: str | None = None


class Reason(BaseModel):
    code: str
    description: str
    weight: float = 0.0
    evidence: dict[str, Any] = {}


class RiskDecision(BaseModel):
    transaction_id: str
    customer_id: str
    card_id: str
    amount: float
    currency: str
    risk_score: float
    decision: Decision
    rules_score: float | None = None
    ml_score: float | None = None
    graph_score: float | None = None
    reasons: list[Reason] = []
    missing_signals: list[str] = []
    model_version: str = "n/a"
    policy_version: str = "policy-v1"
    total_latency_ms: float = 0.0
    decided_at: datetime = Field(default_factory=now)
    trace_id: str | None = None


class NotificationEvent(BaseModel):
    notification_id: str = Field(default_factory=lambda: new_id("ntf"))
    transaction_id: str
    customer_id: str
    channel: Literal["PUSH", "SMS", "EMAIL"] = "PUSH"
    template: str
    payload: dict[str, Any] = {}
    created_at: datetime = Field(default_factory=now)


class CaseEvent(BaseModel):
    case_id: str = Field(default_factory=lambda: new_id("case"))
    transaction_id: str
    customer_id: str
    priority: Literal["P1", "P2", "P3"] = "P2"
    risk_score: float
    status: Literal["OPEN", "IN_REVIEW", "CLOSED"] = "OPEN"
    reasons: list[Reason] = []
    created_at: datetime = Field(default_factory=now)


class AuditEvent(BaseModel):
    audit_id: str = Field(default_factory=lambda: new_id("aud"))
    transaction_id: str
    event_type: str
    actor: str = "system"
    payload: dict[str, Any] = {}
    prev_hash: str | None = None
    hash: str | None = None
    occurred_at: datetime = Field(default_factory=now)


Signal.model_rebuild()

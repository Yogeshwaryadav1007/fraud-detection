"""Rule engine unit tests — every rule needs a positive and a negative case."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services/rule-engine"))

from libs.common.schemas import (
    Channel, CustomerProfile, EnrichedTransaction, TransactionRequest,
    VelocityCounters,
)
from rules import evaluate


def make(**over) -> EnrichedTransaction:
    txn_kw = dict(card_id="c1", account_id="a1", customer_id="u1",
                  merchant_id="m1", amount=1000, currency="INR",
                  channel=Channel.ECOM, merchant_country="IN",
                  occurred_at=datetime(2026, 1, 1, 12, tzinfo=timezone.utc))
    txn_kw.update(over.pop("txn", {}))
    base = dict(
        transaction=TransactionRequest(**txn_kw),
        velocity=VelocityCounters(**over.pop("vel", {})),
        profile=CustomerProfile(customer_id="u1", avg_ticket=1000,
                                std_ticket=350, tenure_days=500,
                                **over.pop("prof", {})),
    )
    base.update(over)
    return EnrichedTransaction(**base)


def codes(e) -> set[str]:
    return {r.code for r in evaluate(e)[1]}


def test_clean_transaction_scores_zero():
    score, reasons = evaluate(make())
    assert score == 0 and reasons == []


def test_r001_velocity_burst():
    assert "R001" in codes(make(vel={"txn_count_1m": 9}))
    assert "R001" not in codes(make(vel={"txn_count_1m": 3}))


def test_r003_amount_spike():
    assert "R003" in codes(make(txn={"amount": 9000}))
    assert "R003" not in codes(make(txn={"amount": 2000}))


def test_r004_impossible_travel():
    e = make(km_from_prev_txn=1200, seconds_since_prev_txn=600)
    assert "R004" in codes(e)
    slow = make(km_from_prev_txn=1200, seconds_since_prev_txn=40000)
    assert "R004" not in codes(slow)


def test_r007_sanctioned_country_is_decisive():
    score, reasons = evaluate(make(txn={"merchant_country": "KP"}))
    assert "R007" in {r.code for r in reasons}
    assert max(r.weight for r in reasons) >= 50


def test_score_is_capped_at_100():
    e = make(txn={"amount": 999999, "merchant_country": "KP",
                  "merchant_category": "7995"},
             vel={"txn_count_1m": 20, "txn_count_1h": 60,
                  "distinct_countries_24h": 5, "distinct_devices_24h": 9},
             km_from_prev_txn=5000, seconds_since_prev_txn=60,
             is_new_device=True)
    score, _ = evaluate(e)
    assert score == 100.0


def test_a_broken_rule_cannot_crash_the_engine(monkeypatch):
    import rules as R
    bad = R.Rule("RXXX", "boom", 10,
                 lambda e: (_ for _ in ()).throw(ValueError("boom")),
                 lambda e: {})
    monkeypatch.setattr(R, "RULES", R.RULES + [bad])
    score, _ = R.evaluate(make())
    assert score == 0

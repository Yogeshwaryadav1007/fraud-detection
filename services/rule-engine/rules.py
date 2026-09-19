"""Declarative rule set. Each rule is pure: EnrichedTransaction -> Reason | None.

Rules are data, not code branches scattered across the service — so they can be
version-controlled, unit-tested, and hot-reloaded from a YAML file later.
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

from libs.common.schemas import EnrichedTransaction, Reason

log = logging.getLogger(__name__)

HIGH_RISK_MCC = {"7995", "6051", "4829", "6211"}     # gambling, crypto, wire, securities
SANCTIONED = {"KP", "IR", "SY", "CU"}


@dataclass(frozen=True)
class Rule:
    code: str
    description: str
    weight: float                 # points added to the rules score (0-100 cap)
    predicate: Callable[[EnrichedTransaction], bool]
    evidence: Callable[[EnrichedTransaction], dict]


def _amt(e: EnrichedTransaction) -> float:
    return float(e.transaction.amount)


RULES: list[Rule] = [
    Rule("R001", "Velocity: more than 5 transactions in 60 seconds", 30,
         lambda e: e.velocity.txn_count_1m > 5,
         lambda e: {"txn_count_1m": e.velocity.txn_count_1m}),

    Rule("R002", "Velocity: more than 25 transactions in 1 hour", 20,
         lambda e: e.velocity.txn_count_1h > 25,
         lambda e: {"txn_count_1h": e.velocity.txn_count_1h}),

    Rule("R003", "Amount is more than 5x the customer's average ticket", 25,
         lambda e: e.profile.avg_ticket > 0 and _amt(e) > 5 * e.profile.avg_ticket,
         lambda e: {"amount": _amt(e), "avg_ticket": e.profile.avg_ticket}),

    Rule("R004", "Impossible travel: >500 km in under 30 minutes", 40,
         lambda e: (e.km_from_prev_txn or 0) > 500
                   and (e.seconds_since_prev_txn or 1e9) < 1800,
         lambda e: {"km": e.km_from_prev_txn, "seconds": e.seconds_since_prev_txn}),

    Rule("R005", "First transaction on a brand-new device above 25,000", 20,
         lambda e: e.is_new_device and _amt(e) > 25_000,
         lambda e: {"device_id": e.transaction.device_id, "amount": _amt(e)}),

    Rule("R006", "Merchant category is high risk", 15,
         lambda e: e.transaction.merchant_category in HIGH_RISK_MCC,
         lambda e: {"mcc": e.transaction.merchant_category}),

    Rule("R007", "Transaction country is sanctioned", 60,
         lambda e: e.transaction.merchant_country in SANCTIONED,
         lambda e: {"country": e.transaction.merchant_country}),

    Rule("R008", "Card used in 3 or more countries in 24 hours", 30,
         lambda e: e.velocity.distinct_countries_24h >= 3,
         lambda e: {"countries_24h": e.velocity.distinct_countries_24h}),

    Rule("R009", "Card used on 4 or more devices in 24 hours", 25,
         lambda e: e.velocity.distinct_devices_24h >= 4,
         lambda e: {"devices_24h": e.velocity.distinct_devices_24h}),

    Rule("R010", "Card-testing pattern: many small amounts in an hour", 25,
         lambda e: e.velocity.txn_count_1h >= 10 and _amt(e) < 100,
         lambda e: {"txn_count_1h": e.velocity.txn_count_1h, "amount": _amt(e)}),

    Rule("R011", "Foreign transaction on an unverified KYC account", 20,
         lambda e: e.is_foreign and not e.profile.kyc_verified,
         lambda e: {"country": e.transaction.merchant_country}),

    Rule("R012", "Account younger than 30 days spending above 50,000", 20,
         lambda e: e.profile.tenure_days < 30 and _amt(e) > 50_000,
         lambda e: {"tenure_days": e.profile.tenure_days, "amount": _amt(e)}),

    Rule("R013", "Customer has 2 or more chargebacks in the last 12 months", 15,
         lambda e: e.profile.chargebacks_12m >= 2,
         lambda e: {"chargebacks_12m": e.profile.chargebacks_12m}),

    Rule("R014", "24h spend exceeds 10x the average ticket", 20,
         lambda e: e.profile.avg_ticket > 0
                   and e.velocity.amount_sum_24h > 10 * e.profile.avg_ticket,
         lambda e: {"amount_sum_24h": e.velocity.amount_sum_24h}),

    Rule("R015", "ATM withdrawal at a new merchant in a foreign country", 25,
         lambda e: e.transaction.channel.value == "ATM" and e.is_foreign,
         lambda e: {"channel": "ATM", "country": e.transaction.merchant_country}),
]


def evaluate(e: EnrichedTransaction) -> tuple[float, list[Reason]]:
    hits: list[Reason] = []
    score = 0.0
    for rule in RULES:
        try:
            if rule.predicate(e):
                hits.append(Reason(code=rule.code, description=rule.description,
                                   weight=rule.weight, evidence=rule.evidence(e)))
                score += rule.weight
        except Exception:
            log.exception("rule evaluation failed: %s", rule.code)
            continue          # a broken rule must never stop the pipeline
    return min(score, 100.0), sorted(hits, key=lambda r: -r.weight)

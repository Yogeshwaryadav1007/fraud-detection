"""Feature extraction shared by training and serving — this file is the contract
that prevents training/serving skew. Change it in one place only.
"""
from __future__ import annotations

import math

from libs.common.schemas import EnrichedTransaction

FEATURE_NAMES = [
    "amount_log",
    "amount_over_avg",
    "amount_zscore",
    "txn_count_1m",
    "txn_count_1h",
    "txn_count_24h",
    "amount_sum_24h_log",
    "distinct_merchants_24h",
    "distinct_countries_24h",
    "distinct_devices_24h",
    "seconds_since_prev_log",
    "km_from_prev_log",
    "is_new_device",
    "is_new_merchant",
    "is_foreign",
    "tenure_days_log",
    "kyc_verified",
    "chargebacks_12m",
    "hour_sin",
    "hour_cos",
    "is_ecom",
    "is_atm",
]


def _log1p(x: float | None) -> float:
    return math.log1p(max(float(x or 0.0), 0.0))


def vectorize(e: EnrichedTransaction) -> list[float]:
    t, v, p = e.transaction, e.velocity, e.profile
    amount = float(t.amount)
    hour = t.occurred_at.hour
    std = p.std_ticket or 1.0
    return [
        _log1p(amount),
        amount / p.avg_ticket if p.avg_ticket else 0.0,
        (amount - p.avg_ticket) / std,
        v.txn_count_1m,
        v.txn_count_1h,
        v.txn_count_24h,
        _log1p(v.amount_sum_24h),
        v.distinct_merchants_24h,
        v.distinct_countries_24h,
        v.distinct_devices_24h,
        _log1p(e.seconds_since_prev_txn if e.seconds_since_prev_txn is not None else 86400),
        _log1p(e.km_from_prev_txn),
        float(e.is_new_device),
        float(e.is_new_merchant),
        float(e.is_foreign),
        _log1p(p.tenure_days),
        float(p.kyc_verified),
        float(p.chargebacks_12m),
        math.sin(2 * math.pi * hour / 24),
        math.cos(2 * math.pi * hour / 24),
        float(t.channel.value == "ECOM"),
        float(t.channel.value == "ATM"),
    ]

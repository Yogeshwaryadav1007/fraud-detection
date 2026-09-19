"""Offline training for the anomaly model.

Usage:
    python ml/train.py --rows 200000 --contamination 0.02 --out ml/model.joblib

Produces a joblib bundle: {pipeline, version, feature_names, baseline_median,
metrics}. The bundle is what ml-service loads; nothing else is shared between
training and serving except services/ml-service/features.py.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services/ml-service"))

from features import FEATURE_NAMES, vectorize                    # noqa: E402
from libs.common.schemas import (                                # noqa: E402
    Channel, CustomerProfile, EnrichedTransaction, TransactionRequest,
    VelocityCounters,
)

RNG = np.random.default_rng(42)


def synth(n: int, fraud_rate: float = 0.015):
    """Synthetic generator so the pipeline is runnable on day one. Swap this for
    a real historical extract before any production training run."""
    X, y = [], []
    base = datetime.now(timezone.utc) - timedelta(days=30)
    for i in range(n):
        fraud = RNG.random() < fraud_rate
        avg = float(RNG.lognormal(7.0, 0.6))
        amount = (avg * RNG.lognormal(0, 0.4) if not fraud
                  else avg * RNG.uniform(4, 30))
        txn = TransactionRequest(
            transaction_id=f"txn_{i}", card_id=f"card_{i % 5000}",
            account_id=f"acc_{i % 5000}", customer_id=f"cust_{i % 5000}",
            merchant_id=f"mer_{RNG.integers(0, 900)}",
            merchant_category=str(RNG.choice(["5411", "5812", "7995", "5999"])),
            merchant_country=str(RNG.choice(["IN", "US", "AE", "SG"],
                                            p=[.8, .1, .05, .05])),
            amount=round(amount, 2), currency="INR",
            channel=Channel(RNG.choice(["ECOM", "CARD_PRESENT", "ATM"])),
            device_id=f"dev_{RNG.integers(0, 4000)}",
            occurred_at=base + timedelta(seconds=int(RNG.integers(0, 2_592_000))),
        )
        vel = VelocityCounters(
            txn_count_1m=int(RNG.poisson(6 if fraud else 0.3)),
            txn_count_1h=int(RNG.poisson(22 if fraud else 2)),
            txn_count_24h=int(RNG.poisson(45 if fraud else 9)),
            amount_sum_24h=float(amount * RNG.uniform(1, 6)),
            distinct_merchants_24h=int(RNG.poisson(12 if fraud else 3)),
            distinct_countries_24h=int(RNG.poisson(2.5 if fraud else 0.2)) + 1,
            distinct_devices_24h=int(RNG.poisson(4 if fraud else 1)) + 1,
        )
        prof = CustomerProfile(
            customer_id=txn.customer_id,
            tenure_days=int(RNG.integers(1, 3000)),
            avg_ticket=round(avg, 2), std_ticket=round(avg * 0.35, 2),
            kyc_verified=bool(RNG.random() > (0.3 if fraud else 0.02)),
            chargebacks_12m=int(RNG.poisson(1.2 if fraud else 0.05)),
        )
        e = EnrichedTransaction(
            transaction=txn, velocity=vel, profile=prof,
            seconds_since_prev_txn=float(RNG.exponential(60 if fraud else 7200)),
            km_from_prev_txn=float(RNG.exponential(900 if fraud else 15)),
            is_new_device=bool(RNG.random() < (0.7 if fraud else 0.08)),
            is_new_merchant=bool(RNG.random() < (0.8 if fraud else 0.3)),
            is_foreign=txn.merchant_country != "IN",
        )
        X.append(vectorize(e))
        y.append(int(fraud))
    return np.asarray(X, float), np.asarray(y, int)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=120_000)
    ap.add_argument("--contamination", type=float, default=0.015)
    ap.add_argument("--out", default="ml/model.joblib")
    args = ap.parse_args()

    print(f"generating {args.rows:,} rows ...")
    X, y = synth(args.rows)

    split = int(len(X) * 0.8)
    X_tr, X_te, y_te = X[:split], X[split:], y[split:]

    # Train only on the presumed-clean majority: this is unsupervised detection.
    X_clean = X_tr[y[:split] == 0]

    pipe = Pipeline([
        ("scaler", StandardScaler()),
        ("iforest", IsolationForest(
            n_estimators=300, contamination=args.contamination,
            max_samples="auto", random_state=42, n_jobs=-1)),
    ])
    print(f"fitting on {len(X_clean):,} clean rows ...")
    pipe.fit(X_clean)

    scores = -pipe.decision_function(X_te)          # higher = more anomalous
    auc = roc_auc_score(y_te, scores)
    ap_score = average_precision_score(y_te, scores)
    print(f"ROC-AUC={auc:.4f}  PR-AUC={ap_score:.4f}")

    version = "if-" + hashlib.sha256(
        f"{args.rows}{args.contamination}{datetime.now().date()}".encode()
    ).hexdigest()[:10]

    bundle = {
        "pipeline": pipe,
        "version": version,
        "feature_names": FEATURE_NAMES,
        "baseline_median": np.median(X_clean, axis=0).tolist(),
        "metrics": {"roc_auc": auc, "pr_auc": ap_score,
                    "rows": int(args.rows),
                    "trained_at": datetime.now(timezone.utc).isoformat()},
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, args.out)
    Path(args.out).with_suffix(".json").write_text(
        json.dumps({k: v for k, v in bundle.items() if k != "pipeline"},
                   indent=2, default=str))
    print(f"saved {args.out}  version={version}")


if __name__ == "__main__":
    main()

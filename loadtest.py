"""Closed-loop load generator. Reports throughput and client-observed latency.

    python scripts/loadtest.py --rps 500 --seconds 60 --fraud-rate 0.03
"""
from __future__ import annotations

import argparse
import asyncio
import random
import statistics
import time
import uuid

import httpx

URL = "http://localhost:8000/v1/transactions"
KEY = "demo-key-1"
lat: list[float] = []
errors = 0


def payload(fraud: bool) -> dict:
    cust = random.randint(0, 4999)
    amount = round(random.lognormvariate(7, 0.6) * (12 if fraud else 1), 2)
    return {
        "transaction_id": f"txn_{uuid.uuid4().hex[:18]}",
        "card_id": f"card_ring_{random.randint(1,12)}" if fraud else f"card_{cust}",
        "account_id": f"acc_{cust}", "customer_id": f"cust_{cust}",
        "merchant_id": f"mer_{random.randint(0,900)}",
        "merchant_category": "7995" if fraud else "5411",
        "merchant_country": random.choice(["KP", "US"]) if fraud else "IN",
        "amount": amount, "currency": "INR",
        "channel": "ECOM",
        "device_id": "dev_shared_1" if fraud else f"dev_{random.randint(0,4000)}",
        "ip_address": "203.0.113.77" if fraud else f"10.0.{random.randint(0,255)}.{random.randint(1,254)}",
        "latitude": 23.36, "longitude": 85.33,
    }


async def fire(client: httpx.AsyncClient, fraud_rate: float) -> None:
    global errors
    t0 = time.perf_counter()
    try:
        r = await client.post(URL, json=payload(random.random() < fraud_rate),
                              headers={"x-api-key": KEY})
        if r.status_code >= 400:
            errors += 1
    except Exception:                                       # noqa: BLE001
        errors += 1
    lat.append((time.perf_counter() - t0) * 1000)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rps", type=int, default=200)
    ap.add_argument("--seconds", type=int, default=30)
    ap.add_argument("--fraud-rate", type=float, default=0.03)
    a = ap.parse_args()

    limits = httpx.Limits(max_connections=1000)
    async with httpx.AsyncClient(timeout=10.0, limits=limits) as client:
        start = time.perf_counter()
        for tick in range(a.seconds):
            batch = [fire(client, a.fraud_rate) for _ in range(a.rps)]
            await asyncio.gather(*batch)
            elapsed = time.perf_counter() - start
            print(f"t={tick+1:>3}s sent={len(lat):>7} errors={errors} "
                  f"rate={len(lat)/elapsed:,.0f}/s")
            await asyncio.sleep(max(0, (tick + 1) - elapsed))

    s = sorted(lat)
    print("\n--- client-observed latency (ms) ---")
    print(f"count  {len(s):,}")
    print(f"mean   {statistics.mean(s):.1f}")
    print(f"p50    {s[len(s)//2]:.1f}")
    print(f"p95    {s[int(len(s)*0.95)]:.1f}")
    print(f"p99    {s[int(len(s)*0.99)]:.1f}")
    print(f"errors {errors}")


if __name__ == "__main__":
    asyncio.run(main())

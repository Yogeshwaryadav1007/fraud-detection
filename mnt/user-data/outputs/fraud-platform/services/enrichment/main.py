"""Enrichment — turns a bare transaction into a feature-complete event.

Redis holds the hot velocity counters (sliding windows via sorted sets).
Postgres holds the slow-moving customer profile.
Output: txn.enriched.v1
"""
from __future__ import annotations

import asyncio
import math
import time
from datetime import datetime, timezone

from libs.common import observability as obs
from libs.common.bus import Consumer, Producer, run_worker
from libs.common.schemas import (
    CustomerProfile,
    EnrichedTransaction,
    TransactionRequest,
    VelocityCounters,
)
from libs.common.stores import pg, redis
from libs.common.topics import TRANSACTIONS_ENRICHED, TRANSACTIONS_RAW

log = obs.setup("enrichment", metrics_port=9102)
producer = Producer()

WINDOW_24H = 86_400


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    R = 6371.0
    dlat, dlon = math.radians(b[0] - a[0]), math.radians(b[1] - a[1])
    x = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(a[0])) * math.cos(math.radians(b[0]))
         * math.sin(dlon / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(x))


async def velocity(card_id: str, txn: TransactionRequest, ts: float) -> VelocityCounters:
    r = await redis()
    zkey = f"vel:z:{card_id}"
    pipe = r.pipeline()
    pipe.zadd(zkey, {f"{txn.transaction_id}:{txn.amount}": ts})
    pipe.zremrangebyscore(zkey, 0, ts - WINDOW_24H)
    pipe.expire(zkey, WINDOW_24H)
    pipe.zrangebyscore(zkey, ts - 60, ts)
    pipe.zrangebyscore(zkey, ts - 3600, ts)
    pipe.zrangebyscore(zkey, ts - WINDOW_24H, ts)
    for dim, val in (("merch", txn.merchant_id),
                     ("ctry", txn.merchant_country),
                     ("dev", txn.device_id or "none")):
        pipe.pfadd(f"vel:{dim}:{card_id}", val)
        pipe.expire(f"vel:{dim}:{card_id}", WINDOW_24H)
        pipe.pfcount(f"vel:{dim}:{card_id}")
    res = await pipe.execute()

    m1, m1h, m24 = res[3], res[4], res[5]

    def total(members: list[str]) -> float:
        out = 0.0
        for m in members:
            try:
                out += float(m.rsplit(":", 1)[1])
            except (IndexError, ValueError):
                continue
        return out

    return VelocityCounters(
        txn_count_1m=len(m1),
        txn_count_1h=len(m1h),
        txn_count_24h=len(m24),
        amount_sum_1h=round(total(m1h), 2),
        amount_sum_24h=round(total(m24), 2),
        distinct_merchants_24h=res[8],
        distinct_countries_24h=res[11],
        distinct_devices_24h=res[14],
    )


async def profile(customer_id: str) -> CustomerProfile:
    r = await redis()
    cached = await r.get(f"prof:{customer_id}")
    if cached:
        return CustomerProfile.model_validate_json(cached)

    pool = await pg()
    row = await pool.fetchrow(
        """SELECT customer_id, tenure_days, risk_band, avg_ticket, std_ticket,
                  home_country, kyc_verified, chargebacks_12m
           FROM customer_profiles WHERE customer_id = $1""",
        customer_id,
    )
    prof = CustomerProfile(**dict(row)) if row else CustomerProfile(
        customer_id=customer_id, risk_band="MEDIUM"
    )
    await r.set(f"prof:{customer_id}", prof.model_dump_json(), ex=900)
    return prof


async def last_seen(card_id: str, txn: TransactionRequest, ts: float):
    """Returns (seconds_since_prev, km_from_prev, is_new_device, is_new_merchant)."""
    r = await redis()
    prev = await r.hgetall(f"last:{card_id}")
    gap = km = None
    if prev.get("ts"):
        gap = round(ts - float(prev["ts"]), 3)
        if txn.latitude is not None and prev.get("lat"):
            km = round(haversine_km(
                (float(prev["lat"]), float(prev["lon"])),
                (txn.latitude, txn.longitude or 0.0)), 2)

    new_dev = bool(txn.device_id) and not await r.sismember(
        f"seen:dev:{card_id}", txn.device_id)
    new_mer = not await r.sismember(f"seen:mer:{card_id}", txn.merchant_id)

    pipe = r.pipeline()
    pipe.hset(f"last:{card_id}", mapping={
        "ts": ts,
        "lat": txn.latitude if txn.latitude is not None else "",
        "lon": txn.longitude if txn.longitude is not None else "",
        "merchant": txn.merchant_id,
    })
    pipe.expire(f"last:{card_id}", 30 * 86_400)
    if txn.device_id:
        pipe.sadd(f"seen:dev:{card_id}", txn.device_id)
        pipe.expire(f"seen:dev:{card_id}", 180 * 86_400)
    pipe.sadd(f"seen:mer:{card_id}", txn.merchant_id)
    pipe.expire(f"seen:mer:{card_id}", 180 * 86_400)
    await pipe.execute()
    return gap, km, new_dev, new_mer


async def handle(_topic: str, value: dict) -> None:
    t0 = time.perf_counter()
    trace_id = value.pop("trace_id", None)
    txn = TransactionRequest.model_validate(value)
    ts = txn.occurred_at.replace(tzinfo=timezone.utc).timestamp()

    with obs.timed("enrich"):
        vel, prof, seen = await asyncio.gather(
            velocity(txn.card_id, txn, ts),
            profile(txn.customer_id),
            last_seen(txn.card_id, txn, ts),
        )
    gap, km, new_dev, new_mer = seen

    enriched = EnrichedTransaction(
        transaction=txn,
        velocity=vel,
        profile=prof,
        seconds_since_prev_txn=gap,
        km_from_prev_txn=km,
        is_new_device=new_dev,
        is_new_merchant=new_mer,
        is_foreign=txn.merchant_country != prof.home_country,
        trace_id=trace_id,
    )
    await producer.send(TRANSACTIONS_ENRICHED, enriched, key=txn.card_id)
    obs.EVENTS.labels("enrichment", TRANSACTIONS_ENRICHED, "ok").inc()
    log.info("enriched", extra={"transaction_id": txn.transaction_id,
                                "trace_id": trace_id})


def main() -> None:
    consumer = Consumer(TRANSACTIONS_RAW, group="enrichment")
    asyncio.run(run_worker(consumer, producer, handle))


if __name__ == "__main__":
    main()

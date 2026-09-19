"""Seeds customer profiles in Postgres and a small fraud ring in Neo4j so the
graph service has something to find on a fresh environment."""
from __future__ import annotations

import asyncio
import random

from libs.common.stores import close_all, neo4j, pg

random.seed(7)


async def seed_pg(n: int = 5000) -> None:
    pool = await pg()
    rows = []
    for i in range(n):
        avg = round(random.lognormvariate(7.0, 0.6), 2)
        rows.append((
            f"cust_{i}", random.randint(1, 3000),
            random.choice(["LOW"] * 8 + ["MEDIUM"] * 3 + ["HIGH"]),
            avg, round(avg * 0.35, 2), "IN",
            random.random() > 0.03, min(int(random.expovariate(8)), 5),
        ))
    await pool.executemany(
        """INSERT INTO customer_profiles
           (customer_id, tenure_days, risk_band, avg_ticket, std_ticket,
            home_country, kyc_verified, chargebacks_12m)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
           ON CONFLICT (customer_id) DO NOTHING""", rows)
    print(f"seeded {n} customer profiles")


async def seed_graph() -> None:
    async with neo4j().session() as s:
        await s.run("""
            UNWIND range(1, 12) AS i
            MERGE (c:Card {id: 'card_ring_' + toString(i)})
              SET c.flagged_fraud = (i <= 3)
            MERGE (d:Device {id: 'dev_shared_1'})
            MERGE (c)-[u:USED_DEVICE]->(d)
              SET u.first_seen = datetime(), u.last_seen = datetime(), u.count = 5
            MERGE (ip:IP {address: '203.0.113.77'})
            MERGE (c)-[v:USED_IP]->(ip)
              SET v.first_seen = datetime(), v.last_seen = datetime(), v.count = 9
        """)
    print("seeded a 12-card fraud ring sharing one device and one IP")


async def main() -> None:
    await seed_pg()
    await seed_graph()
    await close_all()


if __name__ == "__main__":
    asyncio.run(main())

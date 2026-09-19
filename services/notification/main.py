"""Notification — fan-out with per-customer rate limiting and retry backoff."""
from __future__ import annotations

import asyncio

from libs.common import observability as obs
from libs.common.bus import Consumer, Producer, run_worker
from libs.common.schemas import NotificationEvent
from libs.common.stores import redis
from libs.common.topics import NOTIFICATIONS

log = obs.setup("notification", metrics_port=9108)
producer = Producer()

TEMPLATES = {
    "step_up_auth": "Confirm the {currency} {amount} transaction on your card.",
    "txn_declined": "We blocked a {currency} {amount} transaction we flagged as risky.",
}
MAX_PER_HOUR = 5


async def rate_limited(customer_id: str) -> bool:
    r = await redis()
    key = f"ntf:rate:{customer_id}"
    n = await r.incr(key)
    if n == 1:
        await r.expire(key, 3600)
    return n > MAX_PER_HOUR


async def deliver(ev: NotificationEvent, body: str) -> None:
    """Stub for the real provider (FCM / Twilio / SES). Retries with backoff."""
    for attempt in range(3):
        try:
            await asyncio.sleep(0)          # provider call goes here
            log.info("notification sent", extra={"transaction_id": ev.transaction_id})
            return
        except Exception:                                   # noqa: BLE001
            await asyncio.sleep(2 ** attempt * 0.2)
    raise RuntimeError("delivery failed after 3 attempts")


async def handle(_topic: str, value: dict) -> None:
    ev = NotificationEvent.model_validate(value)
    if await rate_limited(ev.customer_id):
        obs.EVENTS.labels("notification", NOTIFICATIONS, "suppressed").inc()
        return
    body = TEMPLATES.get(ev.template, "Security alert on your account.").format(
        currency=ev.payload.get("currency", ""),
        amount=ev.payload.get("amount", ""),
    )
    await deliver(ev, body)
    obs.EVENTS.labels("notification", NOTIFICATIONS, "sent").inc()


def main() -> None:
    asyncio.run(run_worker(
        Consumer(NOTIFICATIONS, group="notification"), producer, handle))


if __name__ == "__main__":
    main()

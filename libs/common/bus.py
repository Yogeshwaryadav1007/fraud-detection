"""Thin async Kafka wrapper: JSON serialisation, DLQ, graceful shutdown."""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Callable
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import BaseModel

from .config import settings
from .topics import DLQ

log = logging.getLogger(__name__)


def _default(o: Any):
    if isinstance(o, BaseModel):
        return o.model_dump()
    if isinstance(o, datetime):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, Enum):
        return o.value
    raise TypeError(f"not serialisable: {type(o)}")


def dumps(v: Any) -> bytes:
    if isinstance(v, BaseModel):
        v = v.model_dump()
    return json.dumps(v, default=_default).encode()


class Producer:
    def __init__(self) -> None:
        self._p: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._p = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP,
            value_serializer=dumps,
            key_serializer=lambda k: k.encode() if k else None,
            acks="all",
            enable_idempotence=True,
            linger_ms=5,
            compression_type="lz4",
        )
        await self._p.start()
        log.info("producer connected to %s", settings.KAFKA_BOOTSTRAP)

    async def send(self, topic: str, value: Any, key: str | None = None) -> None:
        assert self._p, "producer not started"
        await self._p.send_and_wait(topic, value=value, key=key)

    async def stop(self) -> None:
        if self._p:
            await self._p.stop()


class Consumer:
    """Manual-commit consumer. Commit only after the handler succeeds."""

    def __init__(self, *topics: str, group: str) -> None:
        self.topics = topics
        self.group = f"{settings.KAFKA_GROUP_PREFIX}.{group}"
        self._c: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._c = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP,
            group_id=self.group,
            value_deserializer=lambda b: json.loads(b.decode()),
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=200,
        )
        await self._c.start()
        log.info("consumer %s subscribed to %s", self.group, self.topics)

    async def stream(self) -> AsyncIterator[tuple[str, dict]]:
        assert self._c, "consumer not started"
        async for msg in self._c:
            yield msg.topic, msg.value
            await self._c.commit()

    async def stop(self) -> None:
        if self._c:
            await self._c.stop()


async def run_worker(
    consumer: Consumer,
    producer: Producer,
    handler: Callable[[str, dict], Any],
) -> None:
    """Boilerplate loop used by every stateless consumer service."""
    await producer.start()
    await consumer.start()
    try:
        async for topic, value in consumer.stream():
            try:
                await handler(topic, value)
            except Exception as exc:
                log.exception("handler failed on %s", topic)
                await producer.send(
                    DLQ,
                    {
                        "source_topic": topic,
                        "service": settings.SERVICE_NAME,
                        "error": f"{type(exc).__name__}: {exc}",
                        "payload": value,
                    },
                    key=str(value.get("transaction_id") or "unknown"),
                )
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()
        await producer.stop()

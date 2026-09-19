"""Lazy singletons for Redis / Postgres / Neo4j so services stay boilerplate-free."""
from __future__ import annotations

import asyncpg
import redis.asyncio as aioredis
from neo4j import AsyncGraphDatabase

from .config import settings

_redis: aioredis.Redis | None = None
_pool: asyncpg.Pool | None = None
_neo = None


async def redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis


async def pg() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            settings.POSTGRES_DSN, min_size=2, max_size=10, command_timeout=5
        )
    return _pool


def neo4j():
    global _neo
    if _neo is None:
        _neo = AsyncGraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD),
            max_connection_pool_size=20,
        )
    return _neo


async def close_all() -> None:
    if _redis:
        await _redis.aclose()
    if _pool:
        await _pool.close()
    if _neo:
        await _neo.close()

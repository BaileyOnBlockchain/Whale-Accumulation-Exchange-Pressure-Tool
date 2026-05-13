from __future__ import annotations

import redis.asyncio as redis

from src.config import settings
from src.utils.logging import get_logger

log = get_logger(__name__)

_client: redis.Redis | None = None


def get_client() -> redis.Redis:
    global _client
    if _client is None:
        pool = redis.ConnectionPool.from_url(
            settings.redis_url,
            max_connections=settings.redis_max_connections,
            decode_responses=False,
        )
        _client = redis.Redis(connection_pool=pool)
    return _client


async def ping() -> bool:
    try:
        client = get_client()
        await client.ping()
        return True
    except Exception as exc:
        log.warning("redis_ping_failed", error=str(exc))
        return False

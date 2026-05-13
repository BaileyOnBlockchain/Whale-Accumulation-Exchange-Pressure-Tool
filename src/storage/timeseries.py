"""Time-series helpers for Redis — signal snapshots and flow event streams."""
from __future__ import annotations

import time
from typing import Any

import orjson

from src.config import key_accumulation_signal
from src.core.models import AccumulationSignal, Chain
from src.storage.redis_client import get_client
from src.utils.logging import get_logger

log = get_logger(__name__)

_SIGNAL_TTL = 300        # 5 min cache for live signal
_FLOW_STREAM_TTL = 2_592_000  # 30 days for flow history


async def cache_signal(signal: AccumulationSignal) -> None:
    """Cache a computed accumulation signal in Redis (TTL 5 min)."""
    r = get_client()
    key = key_accumulation_signal(signal.chain.value, signal.asset)
    payload = orjson.dumps(signal.model_dump())
    await r.set(key, payload, ex=_SIGNAL_TTL)


async def get_cached_signal(chain: str, asset: str) -> dict[str, Any] | None:
    """Return a cached signal dict or None if stale/missing."""
    r = get_client()
    key = key_accumulation_signal(chain.lower(), asset.upper())
    raw = await r.get(key)
    if raw:
        return orjson.loads(raw)
    return None


async def append_flow_event(
    chain: str, exchange: str, direction: str,
    amount_usd: float, timestamp_ms: int, tx_hash: str,
) -> None:
    """Append a flow event to the per-exchange time-series stream."""
    from src.config import key_exchange_flow
    r = get_client()
    stream_key = key_exchange_flow(chain.lower(), exchange.lower(), direction)
    await r.zadd(stream_key, {f"{tx_hash}:{amount_usd}": timestamp_ms})
    await r.expire(stream_key, _FLOW_STREAM_TTL)


async def get_flow_events(
    chain: str, exchange: str, direction: str,
    since_ms: int, until_ms: int | None = None,
) -> list[tuple[str, float]]:
    """Return flow events in a time range as (member, score) pairs."""
    from src.config import key_exchange_flow
    r = get_client()
    stream_key = key_exchange_flow(chain.lower(), exchange.lower(), direction)
    end = until_ms or int(time.time() * 1000)
    raw = await r.zrangebyscore(stream_key, since_ms, end, withscores=True)
    return [(m.decode() if isinstance(m, bytes) else m, s) for m, s in raw]

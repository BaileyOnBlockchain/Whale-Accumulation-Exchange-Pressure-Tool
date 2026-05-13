"""FastAPI HTTP server — REST API for whale accumulation intelligence.

Endpoints mirror the MCP tools for direct HTTP access:
  GET /health
  GET /signal/{chain}/{asset}
  GET /whales/{chain}/{asset}
  GET /exchange/{chain}/{asset}
  GET /label/{chain}/{address}
  GET /holders/{chain}/{asset}
  POST /corpus/refresh
  GET /corpus/stats
"""
from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI, Query
from fastapi.responses import ORJSONResponse

from src.config import settings
from src.corpus.store import get_store
from src.intelligence.accumulation_score import compute_accumulation_score
from src.intelligence.exchange_pressure import ExchangePressureTracker
from src.intelligence.whale_tracker import WhaleTracker
from src.storage.timeseries import cache_signal, get_cached_signal
from src.utils.logging import configure_logging, get_logger
from src.utils.metrics import start_metrics_server

configure_logging()
log = get_logger(__name__)

_whale_tracker: WhaleTracker | None = None
_exchange_tracker: ExchangePressureTracker | None = None


def _wt() -> WhaleTracker:
    global _whale_tracker
    if _whale_tracker is None:
        _whale_tracker = WhaleTracker()
    return _whale_tracker


def _et() -> ExchangePressureTracker:
    global _exchange_tracker
    if _exchange_tracker is None:
        _exchange_tracker = ExchangePressureTracker()
    return _exchange_tracker


@asynccontextmanager
async def lifespan(application: FastAPI):
    store = get_store()
    await store.initialize()
    from src.collectors.exchange_wallets import build_seed_labels
    seed = build_seed_labels()
    await store.upsert_labels_bulk(seed)
    log.info("http_server_started", seed_labels=len(seed))
    yield


app = FastAPI(
    title="Whale Accumulation & Exchange-Pressure Intelligence",
    description=(
        "On-chain whale accumulation and exchange-pressure intelligence. "
        "All address labels carry transparent provenance — every entity "
        "classification cites its exact source and methodology."
    ),
    version="1.0.0",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict[str, Any]:
    from src.storage.redis_client import ping
    redis_ok = await ping()
    store = get_store()
    stats = await store.corpus_stats()
    return {
        "status":      "ok" if redis_ok else "degraded",
        "redis":       "connected" if redis_ok else "unreachable",
        "corpus":      stats,
        "version":     "1.0.0",
    }


@app.get("/signal/{chain}/{asset}")
async def get_signal(
    chain: str,
    asset: str,
    lookback_hours: int = Query(24, ge=1, le=168),
    use_cache: bool = Query(True),
) -> dict[str, Any]:
    if use_cache:
        cached = await get_cached_signal(chain.lower(), asset.upper())
        if cached:
            return {**cached, "cached": True}

    whale_flow = await _wt().scan_eth_whales(lookback_hours)
    exchange_p = await _et().scan_eth_exchange_pressure(lookback_hours)
    signal = compute_accumulation_score(whale_flow, exchange_p)
    await cache_signal(signal)
    return {**signal.model_dump(), "cached": False}


@app.get("/whales/{chain}/{asset}")
async def get_whales(
    chain: str,
    asset: str,
    lookback_hours: int = Query(24, ge=1, le=168),
    min_delta_usd: float = Query(100_000),
) -> dict[str, Any]:
    wf = await _wt().scan_eth_whales(lookback_hours)
    return wf.model_dump()


@app.get("/exchange/{chain}/{asset}")
async def get_exchange(
    chain: str,
    asset: str,
    lookback_hours: int = Query(24, ge=1, le=168),
) -> dict[str, Any]:
    ep = await _et().scan_eth_exchange_pressure(lookback_hours)
    return ep.model_dump()


@app.get("/label/{chain}/{address}")
async def get_label(chain: str, address: str) -> dict[str, Any]:
    store = get_store()
    label = await store.get_label(address.lower(), chain.lower())
    if not label:
        return {"found": False, "address": address, "chain": chain}
    return {
        "found":       True,
        "address":     label.address,
        "chain":       label.chain.value,
        "entity_name": label.entity_name,
        "entity_type": label.entity_type.value,
        "wallet_role": label.wallet_role.value,
        "confidence":  round(label.confidence, 3),
        "sources":     [s.model_dump() for s in label.sources],
        "provenance":  label.provenance_summary(),
    }


@app.get("/holders/{chain}/{asset}")
async def get_holders(
    chain: str,
    asset: str,
    top_n: int = Query(50, ge=1, le=200),
) -> dict[str, Any]:
    store = get_store()
    whale_addrs = await store.get_whale_addresses(chain.lower())
    exch_labels = await store.get_exchange_addresses(chain.lower())
    all_addrs   = list({e.address for e in exch_labels} | set(whale_addrs))[:top_n]

    holders = []
    for rank, addr in enumerate(all_addrs, 1):
        lbl = await store.get_label(addr, chain.lower())
        bal_hist = await store.get_balance_history(addr, chain.lower(), asset.upper(), limit=1)
        holders.append({
            "rank":        rank,
            "address":     addr,
            "balance_usd": bal_hist[0]["balance_usd"] if bal_hist else 0.0,
            "entity_name": lbl.entity_name if lbl else f"{addr[:8]}…",
            "entity_type": lbl.entity_type.value if lbl else "unknown",
            "labeled":     lbl is not None,
        })

    holders.sort(key=lambda h: h["balance_usd"], reverse=True)
    return {"chain": chain, "asset": asset, "top_n": top_n, "holders": holders}


@app.post("/corpus/refresh")
async def corpus_refresh(
    sources: list[str] | None = Query(default=None),
    chain: str = Query(default="ethereum"),
) -> dict[str, Any]:
    store = get_store()
    from src.collectors.exchange_wallets import build_seed_labels
    seed = build_seed_labels()
    n = await store.upsert_labels_bulk(seed)
    stats = await store.corpus_stats()
    return {"status": "ok", "labels_seeded": n, "corpus_stats": stats}


@app.get("/corpus/stats")
async def corpus_stats() -> dict[str, Any]:
    store = get_store()
    return await store.corpus_stats()


def main() -> None:
    start_metrics_server()
    uvicorn.run(
        "src.server.http_server:app",
        host=settings.http_host,
        port=settings.http_port,
        log_level=settings.log_level.lower(),
        reload=False,
    )


if __name__ == "__main__":
    main()

"""Data ingestion pipeline daemon.

Continuously:
1. Seeds / refreshes the labeled corpus (hourly)
2. Scans whale balances and exchange reserves (every 5 min)
3. Computes and caches accumulation signals (every 60s)
4. Runs on-chain heuristics to expand the labeled corpus (daily)

Run: python -m src.pipeline
"""
from __future__ import annotations

import asyncio
import signal
import sys
import time

try:
    import uvloop
    _HAS_UVLOOP = True
except (ImportError, RuntimeError):
    _HAS_UVLOOP = False

from src.config import settings
from src.collectors.exchange_wallets import build_seed_labels
from src.corpus.store import get_store
from src.intelligence.accumulation_score import compute_accumulation_score
from src.intelligence.exchange_pressure import ExchangePressureTracker
from src.intelligence.whale_tracker import WhaleTracker
from src.storage.redis_client import get_client, ping
from src.storage.timeseries import cache_signal
from src.utils.logging import configure_logging, get_logger
from src.utils.metrics import start_metrics_server, corpus_refresh_total, corpus_labels_gauge

configure_logging()
log = get_logger(__name__)


class Pipeline:
    def __init__(self) -> None:
        self._running = True
        self._store = get_store()
        self._whale_tracker = WhaleTracker()
        self._exchange_tracker = ExchangePressureTracker()
        self._last_corpus_refresh = 0.0
        self._last_signal_compute = 0.0

    async def run(self) -> None:
        log.info("pipeline_starting", chains=settings.chains, assets=settings.assets)

        if not await ping():
            log.error("redis_not_reachable", url=settings.redis_url)
            sys.exit(1)

        await self._store.initialize()
        log.info("corpus_store_initialized", db_path=settings.corpus_db_path)

        # Initial corpus seed
        await self._refresh_corpus()

        start_metrics_server()
        log.info("pipeline_running")

        while self._running:
            now = time.time()
            try:
                # Refresh corpus every hour
                if now - self._last_corpus_refresh >= settings.corpus_refresh_interval_s:
                    await self._refresh_corpus()

                # Scan whale balances every 5 min
                if now - self._last_signal_compute >= settings.balance_scan_interval_s:
                    await self._scan_and_signal()

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error("pipeline_loop_error", error=str(exc))

            await asyncio.sleep(10)

    async def _refresh_corpus(self) -> None:
        log.info("corpus_refresh_starting")
        try:
            seed_labels = build_seed_labels()
            n = await self._store.upsert_labels_bulk(seed_labels)
            log.info("corpus_seeded", count=n)
            corpus_refresh_total.labels(source="exchange_wallet_seed").inc()

            # Optional: Arkham refresh
            if settings.arkham_api_key:
                from src.collectors.arkham import ArkhamCollector
                from src.collectors.exchange_wallets import get_known_exchange_addresses
                arkham = ArkhamCollector()
                if await arkham.is_available():
                    addrs = list(get_known_exchange_addresses("ethereum"))[:100]
                    ark_labels = await arkham.batch_lookup(addrs, "ethereum")
                    n_ark = await self._store.upsert_labels_bulk(ark_labels)
                    log.info("corpus_arkham_refreshed", count=n_ark)
                    corpus_refresh_total.labels(source="arkham_intelligence").inc()

            # Optional: Dune refresh
            if settings.dune_api_key:
                from src.collectors.dune import DuneCollector
                dune = DuneCollector()
                if await dune.is_available():
                    dune_labels = await dune.get_spellbook_labels("ethereum")
                    n_dune = await self._store.upsert_labels_bulk(dune_labels)
                    log.info("corpus_dune_refreshed", count=n_dune)
                    corpus_refresh_total.labels(source="dune_spellbook").inc()

            # Update corpus metrics
            stats = await self._store.corpus_stats()
            for entity_type, count in stats.get("by_entity_type", {}).items():
                corpus_labels_gauge.labels(chain="ethereum", entity_type=entity_type).set(count)

        except Exception as exc:
            log.error("corpus_refresh_error", error=str(exc))

        self._last_corpus_refresh = time.time()

    async def _scan_and_signal(self) -> None:
        for chain in settings.chains:
            try:
                if chain == "ethereum":
                    whale_flow, exchange_pressure = await asyncio.gather(
                        self._whale_tracker.scan_eth_whales(lookback_hours=24),
                        self._exchange_tracker.scan_eth_exchange_pressure(lookback_hours=24),
                    )
                    signal = compute_accumulation_score(whale_flow, exchange_pressure)
                    await cache_signal(signal)
                    log.info(
                        "signal_computed",
                        chain=chain, asset="ETH",
                        score=signal.score, rating=signal.rating,
                    )

                elif chain == "bitcoin":
                    whale_flow = await self._whale_tracker.scan_btc_whales(lookback_hours=24)
                    log.info(
                        "btc_whale_scan",
                        net_flow_usd=whale_flow.net_flow_usd,
                        tracked=whale_flow.tracked_count,
                    )

            except Exception as exc:
                log.error("scan_error", chain=chain, error=str(exc))

        self._last_signal_compute = time.time()

    def stop(self) -> None:
        self._running = False


async def run_pipeline() -> None:
    pipeline = Pipeline()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _shutdown(sig: int) -> None:
        log.info("shutdown_signal", signal=sig)
        pipeline.stop()
        stop_event.set()

    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _shutdown, sig)
        task = asyncio.create_task(pipeline.run())
        await stop_event.wait()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    except NotImplementedError:
        try:
            await pipeline.run()
        except (KeyboardInterrupt, asyncio.CancelledError):
            pipeline.stop()

    log.info("pipeline_stopped")


def main() -> None:
    if _HAS_UVLOOP:
        uvloop.install()
    asyncio.run(run_pipeline())


if __name__ == "__main__":
    main()

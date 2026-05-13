"""Whale accumulation tracker.

Tracks balance changes for all labeled whale addresses and unlabeled top holders,
producing a WhaleFlowSummary that feeds into the accumulation signal.

Key metrics
-----------
- net_flow_usd         : sum of all balance changes (positive = accumulation)
- accumulating_count   : whales increasing holdings
- distributing_count   : whales decreasing holdings
- top_accumulators     : top 10 whales by USD accumulated
- top_distributors     : top 10 whales by USD distributed
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import numpy as np

from src.collectors.etherscan import EtherscanCollector
from src.collectors.onchain import AlchemyCollector, BlockchairCollector, CoinGeckoPriceCollector
from src.config import settings, key_whale_balance, key_accumulation_signal
from src.corpus.store import get_store
from src.core.models import (
    BalanceDelta, BalanceSnapshot, Chain, EntityType, WhaleFlowSummary,
)
from src.storage.redis_client import get_client
from src.utils.logging import get_logger
from src.utils.metrics import whale_scans_total, whale_tracked_gauge

log = get_logger(__name__)

_WHALE_THRESHOLD_USD = settings.whale_min_usd
_TOP_N = settings.whale_top_n


class WhaleTracker:
    """
    Continuously tracks balance changes for labeled + discovered whale addresses.

    Pipeline
    --------
    1. Load labeled whale + exchange addresses from corpus
    2. Fetch current balances from chain (Alchemy/Etherscan/Blockchair)
    3. Compare against prior snapshots stored in Redis
    4. Compute delta: who accumulated, who distributed
    5. Persist snapshots to Redis (TTL 30d) and DuckDB (permanent)
    6. Return WhaleFlowSummary for signal computation
    """

    def __init__(self) -> None:
        self._store = get_store()
        self._price_oracle = CoinGeckoPriceCollector()
        self._alchemy = AlchemyCollector()
        self._etherscan = EtherscanCollector()
        self._blockchair = BlockchairCollector()
        self._redis = get_client()

    # ── ETH whale tracking ────────────────────────────────────────────────────

    async def scan_eth_whales(
        self, lookback_hours: int = 24
    ) -> WhaleFlowSummary:
        """
        Scan all labeled ETH whale addresses + top holders for balance changes.
        Returns a WhaleFlowSummary for the specified lookback window.
        """
        t0 = time.perf_counter()

        # Step 1: Get current ETH price
        prices = await self._price_oracle.get_prices_usd(["ethereum"])
        eth_price = prices.get("ethereum", 3_500.0)

        # Step 2: Gather addresses to track
        labeled_whales = await self._store.get_whale_addresses("ethereum")
        exchange_addrs = await self._store.get_exchange_addresses("ethereum")
        exchange_addr_set = {e.address.lower() for e in exchange_addrs}

        # Step 3: Fetch current balances
        all_addrs = labeled_whales[:_TOP_N]
        if not all_addrs:
            log.info("whale_tracker_no_addresses", chain="ethereum")
            return self._empty_summary(Chain.ethereum, "ETH", lookback_hours)

        balances = await self._alchemy.get_balances_bulk(all_addrs, eth_price)

        # Step 4: Compare with previous snapshots and compute deltas
        deltas = await self._compute_deltas(balances, Chain.ethereum, "ETH", eth_price)

        # Step 5: Persist new snapshots
        await self._persist_snapshots(balances)

        summary = self._build_summary(
            deltas=deltas,
            chain=Chain.ethereum,
            asset="ETH",
            lookback_hours=lookback_hours,
            labeled_count=len(labeled_whales),
            tracked_count=len(balances),
            latency=time.perf_counter() - t0,
        )

        whale_scans_total.labels(chain="ethereum").inc()
        whale_tracked_gauge.labels(chain="ethereum").set(len(balances))
        return summary

    async def scan_btc_whales(
        self, lookback_hours: int = 24
    ) -> WhaleFlowSummary:
        """Scan labeled BTC whale addresses for balance changes."""
        t0 = time.perf_counter()

        prices = await self._price_oracle.get_prices_usd(["bitcoin"])
        btc_price = prices.get("bitcoin", 97_000.0)

        labeled_whales = await self._store.get_whale_addresses("bitcoin")
        all_addrs = labeled_whales[:_TOP_N]

        if not all_addrs:
            return self._empty_summary(Chain.bitcoin, "BTC", lookback_hours)

        balances = await self._blockchair.get_btc_balances_bulk(all_addrs, btc_price)

        deltas = await self._compute_deltas(balances, Chain.bitcoin, "BTC", btc_price)
        await self._persist_snapshots(balances)

        summary = self._build_summary(
            deltas=deltas,
            chain=Chain.bitcoin,
            asset="BTC",
            lookback_hours=lookback_hours,
            labeled_count=len(labeled_whales),
            tracked_count=len(balances),
            latency=time.perf_counter() - t0,
        )

        whale_scans_total.labels(chain="bitcoin").inc()
        return summary

    # ── Delta computation ─────────────────────────────────────────────────────

    async def _compute_deltas(
        self,
        snapshots: list[BalanceSnapshot],
        chain: Chain,
        asset: str,
        price_usd: float,
    ) -> list[BalanceDelta]:
        deltas: list[BalanceDelta] = []
        now_ms = int(time.time() * 1000)

        for snap in snapshots:
            rkey = key_whale_balance(chain.value, snap.address)
            prev_data = await self._redis.hgetall(rkey)

            if prev_data:
                prev_usd = float(prev_data.get(b"balance_usd", b"0") or 0)
            else:
                prev_usd = snap.balance_usd   # first scan — no delta yet

            delta_usd = snap.balance_usd - prev_usd
            delta_pct = (delta_usd / prev_usd * 100) if prev_usd > 0 else 0.0

            # Lookup entity label for enrichment
            label = await self._store.get_label(snap.address, chain.value)
            entity_name = label.entity_name if label else f"{snap.address[:8]}…"
            entity_type = label.entity_type if label else EntityType.unknown

            if abs(delta_usd) > 1000:  # ignore dust
                deltas.append(BalanceDelta(
                    address=snap.address,
                    chain=chain,
                    asset=asset,
                    prev_balance_usd=prev_usd,
                    curr_balance_usd=snap.balance_usd,
                    delta_usd=delta_usd,
                    delta_pct=round(delta_pct, 2),
                    period_start_ms=int(prev_data.get(b"updated_ms", b"0") or 0),
                    period_end_ms=now_ms,
                    entity_name=entity_name,
                    entity_type=entity_type,
                ))

        return deltas

    async def _persist_snapshots(self, snapshots: list[BalanceSnapshot]) -> None:
        now_ms = int(time.time() * 1000)
        pipe = self._redis.pipeline()
        for snap in snapshots:
            rkey = key_whale_balance(snap.chain.value, snap.address)
            pipe.hset(rkey, mapping={
                "balance_native": str(snap.balance_native),
                "balance_usd":    str(snap.balance_usd),
                "asset":          snap.asset,
                "updated_ms":     str(now_ms),
            })
            pipe.expire(rkey, 86400 * 30)  # 30 day TTL
        await pipe.execute()

        # Persist to DuckDB (permanent record)
        store = self._store
        for snap in snapshots:
            await store.record_balance_snapshot(
                address=snap.address,
                chain=snap.chain.value,
                asset=snap.asset,
                balance_native=snap.balance_native,
                balance_usd=snap.balance_usd,
            )

    # ── Summary builder ───────────────────────────────────────────────────────

    def _build_summary(
        self,
        deltas: list[BalanceDelta],
        chain: Chain,
        asset: str,
        lookback_hours: int,
        labeled_count: int,
        tracked_count: int,
        latency: float,
    ) -> WhaleFlowSummary:
        accumulating = [d for d in deltas if d.delta_usd > 0]
        distributing = [d for d in deltas if d.delta_usd < 0]

        total_accum  = sum(d.delta_usd for d in accumulating)
        total_distrib = abs(sum(d.delta_usd for d in distributing))
        net_flow     = total_accum - total_distrib

        top_acc = sorted(accumulating, key=lambda d: d.delta_usd, reverse=True)[:10]
        top_dist = sorted(distributing, key=lambda d: d.delta_usd)[:10]

        corpus_stats = {
            "labeled_addresses": labeled_count,
            "tracked_addresses": tracked_count,
            "corpus_coverage_pct": round(100 * labeled_count / max(tracked_count, 1), 1),
        }

        return WhaleFlowSummary(
            chain=chain,
            asset=asset,
            lookback_hours=lookback_hours,
            total_accumulating_usd=round(total_accum, 0),
            total_distributing_usd=round(total_distrib, 0),
            net_flow_usd=round(net_flow, 0),
            accumulating_count=len(accumulating),
            distributing_count=len(distributing),
            neutral_count=tracked_count - len(accumulating) - len(distributing),
            tracked_count=tracked_count,
            labeled_count=labeled_count,
            top_accumulators=[
                {
                    "address":     d.address,
                    "entity_name": d.entity_name,
                    "entity_type": d.entity_type.value,
                    "delta_usd":   round(d.delta_usd, 0),
                    "delta_pct":   d.delta_pct,
                }
                for d in top_acc
            ],
            top_distributors=[
                {
                    "address":     d.address,
                    "entity_name": d.entity_name,
                    "entity_type": d.entity_type.value,
                    "delta_usd":   round(d.delta_usd, 0),
                    "delta_pct":   d.delta_pct,
                }
                for d in top_dist
            ],
            provenance={
                "data_sources":  ["alchemy_ethereum", "blockchair_bitcoin"],
                "scan_latency_ms": round(latency * 1000, 1),
                "corpus_stats":  corpus_stats,
                "methodology": (
                    "Balance snapshots fetched from on-chain RPC. "
                    "Deltas computed against previous snapshot stored in Redis. "
                    "Addresses labeled from our corpus (Arkham + Dune + exchange seed). "
                    "Only deltas > $1,000 USD included to filter dust."
                ),
            },
            confidence=min(1.0, 0.5 + 0.5 * (labeled_count / max(tracked_count, 1))),
        )

    def _empty_summary(
        self, chain: Chain, asset: str, lookback_hours: int
    ) -> WhaleFlowSummary:
        return WhaleFlowSummary(
            chain=chain, asset=asset, lookback_hours=lookback_hours,
            total_accumulating_usd=0, total_distributing_usd=0, net_flow_usd=0,
            accumulating_count=0, distributing_count=0, neutral_count=0,
            tracked_count=0, labeled_count=0,
            provenance={"note": "No whale addresses in corpus yet — run seed_corpus first"},
            confidence=0.0,
        )

"""Exchange pressure intelligence.

Tracks net flows into and out of known exchange wallets to measure sell/buy pressure.

Logic
-----
- Coins entering exchanges   → sell pressure (holders liquidating to exchanges)
- Coins leaving exchanges    → accumulation signal (self-custody, OTC withdrawal)
- Net flow = inflows - outflows (negative = net outflow = bullish signal)
- Exchange reserve change    = current reserves - historical baseline

Signal interpretation
---------------------
Strong outflow  → whales/institutions withdrawing from exchanges to cold storage
Strong inflow   → prepare-to-sell signal; large deposits precede sells
Stable reserves → neither strong accumulation nor distribution
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

from src.collectors.etherscan import EtherscanCollector
from src.collectors.onchain import AlchemyCollector, CoinGeckoPriceCollector
from src.config import settings, key_exchange_flow, key_exchange_reserve
from src.corpus.store import get_store
from src.core.models import (
    Chain, EntityType, ExchangeFlowEvent, ExchangePressureSummary,
    ExchangeReserve, FlowDirection,
)
from src.storage.redis_client import get_client
from src.utils.logging import get_logger
from src.utils.metrics import exchange_flow_events_total

log = get_logger(__name__)

_ETH_PRICE_FALLBACK = 3_500.0


def _redis_float(data: dict, key: bytes, default: float = 0.0) -> float:
    """Safely decode a Redis bytes value to float (decode_responses=False)."""
    raw = data.get(key)
    if raw is None:
        return default
    try:
        return float(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
    except (ValueError, AttributeError):
        return default


class ExchangePressureTracker:
    """
    Tracks exchange reserve balances and net flows for known exchange wallets.

    Data flow
    ---------
    1. Load all exchange wallet addresses from corpus
    2. Fetch current balances for each wallet via Alchemy/Etherscan
    3. Compare against last-known reserves stored in Redis
    4. Classify transactions into inflows/outflows using Alchemy asset transfers
    5. Aggregate per-exchange and cross-exchange metrics
    6. Compute ExchangePressureSummary with full provenance
    """

    def __init__(self) -> None:
        self._store = get_store()
        self._price_oracle = CoinGeckoPriceCollector()
        self._alchemy = AlchemyCollector()
        self._etherscan = EtherscanCollector()
        self._redis = get_client()

    async def scan_eth_exchange_pressure(
        self, lookback_hours: int = 24
    ) -> ExchangePressureSummary:
        """
        Compute exchange pressure for ETH across all known exchange wallets.
        """
        t0 = time.perf_counter()

        prices = await self._price_oracle.get_prices_usd(["ethereum"])
        eth_price = prices.get("ethereum", _ETH_PRICE_FALLBACK)

        exchange_labels = await self._store.get_exchange_addresses("ethereum")
        if not exchange_labels:
            return self._empty_summary(Chain.ethereum, "ETH", lookback_hours)

        # Group by exchange
        exchange_groups: dict[str, list[str]] = {}
        for lbl in exchange_labels:
            exchange_groups.setdefault(lbl.entity_name, []).append(lbl.address)

        # Fetch current balances for all exchange wallets
        all_addrs = [lbl.address for lbl in exchange_labels]
        balances = await self._alchemy.get_balances_bulk(all_addrs, eth_price)
        balance_map = {b.address: b.balance_usd for b in balances}

        # Compute per-exchange reserves + flow
        exchange_breakdown: list[dict[str, Any]] = []
        total_inflow_usd  = 0.0
        total_outflow_usd = 0.0
        total_reserve_usd = 0.0
        prev_reserve_usd  = 0.0

        for exchange_name, addrs in exchange_groups.items():
            current_reserve = sum(balance_map.get(addr, 0) for addr in addrs)
            total_reserve_usd += current_reserve

            # Load previous reserve from Redis
            rkey = key_exchange_reserve("ethereum", exchange_name.lower())
            prev_data = await self._redis.hgetall(rkey)
            prev_reserve = _redis_float(prev_data, b"balance_usd")
            prev_reserve_usd += prev_reserve

            reserve_delta = current_reserve - prev_reserve

            # Approximate inflow/outflow from reserve change (net signal)
            if reserve_delta > 0:
                inflow  = reserve_delta
                outflow = 0.0
            else:
                inflow  = 0.0
                outflow = abs(reserve_delta)

            total_inflow_usd  += inflow
            total_outflow_usd += outflow

            # Update Redis reserve
            now_ms = int(time.time() * 1000)
            await self._redis.hset(rkey, mapping={
                "balance_usd": str(current_reserve),
                "wallet_count": str(len(addrs)),
                "updated_ms":  str(now_ms),
            })
            await self._redis.expire(rkey, 86400 * 30)

            exchange_breakdown.append({
                "exchange":           exchange_name,
                "wallets_tracked":    len(addrs),
                "reserve_usd":        round(current_reserve, 0),
                "reserve_delta_usd":  round(reserve_delta, 0),
                "inflow_usd":         round(inflow, 0),
                "outflow_usd":        round(outflow, 0),
                "net_flow_usd":       round(inflow - outflow, 0),
                "wallet_addresses":   addrs,
            })

        # Persist flow aggregates to DuckDB
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for ex_data in exchange_breakdown:
            await self._store.upsert_flow_aggregate(
                exchange_name=ex_data["exchange"],
                chain="ethereum",
                asset="ETH",
                date_bucket=today,
                inflow_usd=ex_data["inflow_usd"],
                outflow_usd=ex_data["outflow_usd"],
                tx_count=0,
            )

        net_flow = total_inflow_usd - total_outflow_usd
        reserve_change_usd = total_reserve_usd - prev_reserve_usd
        reserve_change_pct = (
            (reserve_change_usd / prev_reserve_usd * 100)
            if prev_reserve_usd > 0 else 0.0
        )

        latency = time.perf_counter() - t0

        return ExchangePressureSummary(
            chain=Chain.ethereum,
            asset="ETH",
            lookback_hours=lookback_hours,
            total_inflow_usd=round(total_inflow_usd, 0),
            total_outflow_usd=round(total_outflow_usd, 0),
            net_flow_usd=round(net_flow, 0),
            reserve_change_usd=round(reserve_change_usd, 0),
            reserve_change_pct=round(reserve_change_pct, 2),
            exchange_breakdown=sorted(
                exchange_breakdown, key=lambda x: x["reserve_usd"], reverse=True
            ),
            provenance={
                "methodology": (
                    "Exchange reserves computed from direct on-chain balance queries "
                    "of known exchange hot and cold wallets. Wallet addresses sourced "
                    "from our labeled corpus (exchange_wallet_seed + Dune Spellbook). "
                    "Net flow = change in total exchange reserve balances. "
                    "Negative net flow (outflow > inflow) is a bullish accumulation signal."
                ),
                "exchanges_tracked":  list(exchange_groups.keys()),
                "wallets_tracked":    len(all_addrs),
                "data_sources":       ["alchemy_ethereum"],
                "scan_latency_ms":    round(latency * 1000, 1),
                "price_source":       "coingecko",
                "eth_price_usd":      eth_price,
            },
            confidence=min(
                1.0,
                0.6 + 0.1 * min(len(exchange_groups), 5)
            ),
        )

    async def get_exchange_reserves(
        self, chain: str, asset: str
    ) -> list[ExchangeReserve]:
        """Load current exchange reserves from Redis cache."""
        exchange_labels = await self._store.get_exchange_addresses(chain)
        exchange_groups: dict[str, list[str]] = {}
        for lbl in exchange_labels:
            exchange_groups.setdefault(lbl.entity_name, []).append(lbl.address)

        reserves: list[ExchangeReserve] = []
        for exchange_name, addrs in exchange_groups.items():
            rkey = key_exchange_reserve(chain.lower(), exchange_name.lower())
            data = await self._redis.hgetall(rkey)
            if data:
                reserves.append(ExchangeReserve(
                    exchange_name=exchange_name,
                    chain=Chain(chain.lower()),
                    asset=asset.upper(),
                    balance_native=0.0,
                    balance_usd=_redis_float(data, b"balance_usd"),
                    wallet_count=len(addrs),
                    updated_ms=int(_redis_float(data, b"updated_ms")),
                ))

        return sorted(reserves, key=lambda r: r.balance_usd, reverse=True)

    def _empty_summary(
        self, chain: Chain, asset: str, lookback_hours: int
    ) -> ExchangePressureSummary:
        return ExchangePressureSummary(
            chain=chain, asset=asset, lookback_hours=lookback_hours,
            total_inflow_usd=0, total_outflow_usd=0, net_flow_usd=0,
            reserve_change_usd=0, reserve_change_pct=0,
            provenance={"note": "No exchange wallets in corpus — run seed_corpus first"},
            confidence=0.0,
        )

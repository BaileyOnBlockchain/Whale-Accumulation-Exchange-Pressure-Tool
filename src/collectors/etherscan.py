"""Etherscan collector — on-chain balance data and address labels for Ethereum.

Two responsibilities:
1. Fetch ETH/ERC-20 balances for tracked addresses (whale balance snapshots).
2. Pull Etherscan public name tags as a labeling source.

Free tier: 5 requests/second, 100k requests/day.
No API key needed for basic balance queries.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

from src.collectors.base import BaseHTTPCollector
from src.config import settings, SOURCE_ETHERSCAN_LABELS
from src.core.models import Chain, EntityLabel, EntityType, WalletRole
from src.corpus.provenance import from_etherscan_labels
from src.utils.logging import get_logger

log = get_logger(__name__)

_BASE = "https://api.etherscan.io/api"
_WEI_TO_ETH = 1e-18
_GWEI_TO_ETH = 1e-9


class EtherscanCollector(BaseHTTPCollector):
    source_name = SOURCE_ETHERSCAN_LABELS

    def __init__(self, api_key: str = "") -> None:
        super().__init__(api_key=api_key or settings.etherscan_api_key, rate_limit_rps=5.0)

    def _params(self, extra: dict[str, Any]) -> dict[str, Any]:
        p = {"apikey": self._api_key or "YourApiKeyToken"}
        p.update(extra)
        return p

    async def is_available(self) -> bool:
        try:
            data = await self.get(_BASE, params=self._params({
                "module": "stats", "action": "ethsupply",
            }))
            return data.get("status") == "1"
        except Exception:
            return False

    # ── Balance queries ───────────────────────────────────────────────────────

    async def get_eth_balance(self, address: str) -> float:
        """Return ETH balance in whole ETH units."""
        try:
            data = await self.get(_BASE, params=self._params({
                "module": "account",
                "action": "balance",
                "address": address,
                "tag":     "latest",
            }))
            if data.get("status") == "1":
                return int(data["result"]) * _WEI_TO_ETH
        except Exception as exc:
            log.debug("etherscan_balance_error", address=address, error=str(exc))
        return 0.0

    async def get_eth_balances_bulk(
        self, addresses: list[str]
    ) -> dict[str, float]:
        """Batch balance query — Etherscan supports up to 20 addresses per call."""
        results: dict[str, float] = {}
        for i in range(0, len(addresses), 20):
            batch = addresses[i:i + 20]
            try:
                data = await self.get(_BASE, params=self._params({
                    "module":  "account",
                    "action":  "balancemulti",
                    "address": ",".join(batch),
                    "tag":     "latest",
                }))
                if data.get("status") == "1":
                    for entry in data["result"]:
                        results[entry["account"].lower()] = (
                            int(entry["balance"]) * _WEI_TO_ETH
                        )
            except Exception as exc:
                log.warning("etherscan_bulk_balance_error",
                            batch_size=len(batch), error=str(exc))
            await asyncio.sleep(0.25)
        return results

    async def get_erc20_balance(
        self, address: str, contract_address: str, decimals: int = 18
    ) -> float:
        """Return ERC-20 token balance in whole token units."""
        try:
            data = await self.get(_BASE, params=self._params({
                "module":          "account",
                "action":          "tokenbalance",
                "address":         address,
                "contractaddress": contract_address,
                "tag":             "latest",
            }))
            if data.get("status") == "1":
                return int(data["result"]) / (10 ** decimals)
        except Exception as exc:
            log.debug("etherscan_erc20_error", address=address, error=str(exc))
        return 0.0

    # ── Transaction history ───────────────────────────────────────────────────

    async def get_recent_txns(
        self, address: str, page: int = 1, offset: int = 100
    ) -> list[dict[str, Any]]:
        """Fetch recent normal (ETH) transactions for an address."""
        try:
            data = await self.get(_BASE, params=self._params({
                "module":     "account",
                "action":     "txlist",
                "address":    address,
                "startblock": 0,
                "endblock":   99999999,
                "page":       page,
                "offset":     offset,
                "sort":       "desc",
            }))
            if data.get("status") == "1":
                return data.get("result", [])
        except Exception as exc:
            log.debug("etherscan_txns_error", address=address, error=str(exc))
        return []

    async def get_internal_txns(
        self, address: str, page: int = 1, offset: int = 100
    ) -> list[dict[str, Any]]:
        """Fetch recent internal (contract call) transactions."""
        try:
            data = await self.get(_BASE, params=self._params({
                "module":     "account",
                "action":     "txlistinternal",
                "address":    address,
                "page":       page,
                "offset":     offset,
                "sort":       "desc",
            }))
            if data.get("status") == "1":
                return data.get("result", [])
        except Exception as exc:
            log.debug("etherscan_internal_error", address=address, error=str(exc))
        return []

    # ── Address analytics ─────────────────────────────────────────────────────

    async def get_address_stats(self, address: str) -> dict[str, Any]:
        """
        Compute basic on-chain stats for an address needed by heuristics:
        tx_per_day, avg_tx_amount_eth, has_dex_activity.
        """
        txns = await self.get_recent_txns(address, offset=200)
        if not txns:
            return {
                "tx_count": 0, "tx_per_day": 0.0,
                "avg_tx_value_eth": 0.0,
                "has_dex_activity": False,
                "has_defi_positions": False,
                "oldest_tx_ms": 0,
                "newest_tx_ms": 0,
            }

        DEX_CONTRACTS = {
            "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",  # Uniswap v2 router
            "0xe592427a0aece92de3edee1f18e0157c05861564",  # Uniswap v3 router
            "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f",  # Sushiswap router
            "0x1111111254fb6c44bac0bed2854e76f90643097d",  # 1inch
        }

        timestamps = [int(t["timeStamp"]) for t in txns if t.get("timeStamp")]
        if len(timestamps) < 2:
            return {
                "tx_count": len(txns), "tx_per_day": 0.0,
                "avg_tx_value_eth": 0.0, "has_dex_activity": False,
                "has_defi_positions": False,
                "oldest_tx_ms": min(timestamps) * 1000 if timestamps else 0,
                "newest_tx_ms": max(timestamps) * 1000 if timestamps else 0,
            }

        span_days = (max(timestamps) - min(timestamps)) / 86400
        tx_per_day = len(txns) / max(span_days, 1.0)

        values_eth = [
            int(t.get("value", 0)) * _WEI_TO_ETH
            for t in txns if int(t.get("value", 0)) > 0
        ]
        avg_value = sum(values_eth) / len(values_eth) if values_eth else 0.0

        to_addrs = {(t.get("to") or "").lower() for t in txns}
        has_dex = bool(to_addrs & DEX_CONTRACTS)

        return {
            "tx_count":          len(txns),
            "tx_per_day":        round(tx_per_day, 2),
            "avg_tx_value_eth":  round(avg_value, 6),
            "has_dex_activity":  has_dex,
            "has_defi_positions": has_dex,
            "oldest_tx_ms":      min(timestamps) * 1000,
            "newest_tx_ms":      max(timestamps) * 1000,
        }

    # ── Top holder discovery ──────────────────────────────────────────────────

    async def get_top_eth_holders(self, limit: int = 200) -> list[dict[str, Any]]:
        """
        Fetch top ETH holders from Etherscan's accounts leaderboard.
        Note: Etherscan returns this as HTML; we use their labeled API endpoint.
        """
        try:
            data = await self.get(_BASE, params=self._params({
                "module": "account",
                "action": "listaccounts",
                "page":   1,
                "offset": min(limit, 10000),
            }))
            if data.get("status") == "1":
                return data.get("result", [])
        except Exception as exc:
            log.warning("etherscan_top_holders_failed", error=str(exc))
        return []

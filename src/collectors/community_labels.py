"""Community-curated address label collector — free Arkham replacement.

Fetches entity labels from two zero-cost GitHub-hosted datasets:

  1. brianleect/etherscan-labels  — 30k+ labeled Ethereum addresses
     (mirrors Etherscan public name tags, community-maintained)
  2. Built-in known-exchange seed (offline fallback, zero network calls)

No API key required. Called on every corpus refresh cycle.
"""
from __future__ import annotations

import time
from typing import Any

import httpx

from src.collectors.base import BaseHTTPCollector
from src.config import SOURCE_COMMUNITY_LABELS
from src.core.models import Chain, EntityLabel, EntityType, WalletRole
from src.corpus.provenance import from_community_list
from src.utils.logging import get_logger

log = get_logger(__name__)

# Raw JSON — no auth, no rate limit beyond GitHub's anonymous 60 req/hr per IP
_BRIAN_ETH_URL = (
    "https://raw.githubusercontent.com/brianleect/etherscan-labels"
    "/main/data/etherscan/combined/combinedAllLabels.json"
)
_BRIAN_REPO_URL = "https://github.com/brianleect/etherscan-labels"

# ── Label → EntityType mapping ────────────────────────────────────────────────
# Any label substring match routes to the entity type.
# Order matters: first match wins.

_EXCHANGE_LABELS: frozenset[str] = frozenset({
    "exchange", "cex", "binance", "coinbase", "kraken", "gemini", "huobi",
    "okex", "okx", "bybit", "kucoin", "bitstamp", "bitfinex", "ftx",
    "gate-io", "gateio", "bittrex", "poloniex", "shapeshift", "changelly",
    "crypto.com", "cryptocom", "bitmex", "deribit", "mexc", "bitget",
    "phemex", "lbank", "ascendex", "bitmart", "whitebit", "hotbit",
    "upbit", "bithumb", "korbit", "coinone", "zaif", "liquid", "btcbox",
    "robinhood", "etoro", "revolut", "bitpanda", "bitvavo", "independentreserve",
})

_BRIDGE_LABELS: frozenset[str] = frozenset({
    "bridge", "cross-chain", "layer-2", "l2", "hop-protocol", "across",
    "stargate", "multichain", "anyswap", "celer", "synapse",
})

_DEFI_LABELS: frozenset[str] = frozenset({
    "dex", "defi", "uniswap", "sushiswap", "curve", "balancer", "aave",
    "compound", "maker", "yearn", "convex", "lido", "frax", "bancor",
    "kyber", "1inch", "paraswap", "cowswap", "cow-protocol",
    "gmx", "perpetual", "dydx", "synthetix",
})

_MINER_LABELS: frozenset[str] = frozenset({
    "miner", "mining", "pool", "ethermine", "f2pool", "sparkpool",
    "antpool", "nanopool",
})

_WHALE_LABELS: frozenset[str] = frozenset({"whale"})


def _classify(name: str, labels: list[str]) -> EntityType | None:
    """Return entity type from name + label list, or None to skip."""
    # Check label tags first (more reliable than name substring matching)
    combined = {lbl.lower() for lbl in labels}
    combined.add(name.lower())

    for token in combined:
        if any(ex in token for ex in _EXCHANGE_LABELS):
            return EntityType.exchange
    for token in combined:
        if any(br in token for br in _BRIDGE_LABELS):
            return EntityType.bridge
    for token in combined:
        if any(df in token for df in _DEFI_LABELS):
            return EntityType.defi
    for token in combined:
        if any(mn in token for mn in _MINER_LABELS):
            return EntityType.miner
    for token in combined:
        if any(wh in token for wh in _WHALE_LABELS):
            return EntityType.whale

    return None


def _wallet_role(entity_type: EntityType, labels: list[str]) -> WalletRole:
    lbl_set = {l.lower() for l in labels}
    if entity_type == EntityType.exchange:
        if "cold-wallet" in lbl_set:
            return WalletRole.cold_wallet
        return WalletRole.hot_wallet
    if entity_type == EntityType.whale:
        return WalletRole.accumulation
    return WalletRole.unknown


class GitHubLabelsCollector(BaseHTTPCollector):
    """Fetches community-curated address labels from GitHub — no API key needed."""

    source_name = SOURCE_COMMUNITY_LABELS

    def __init__(self) -> None:
        # Generous rate limit — it's a single bulk fetch, not per-address calls
        super().__init__(api_key="", rate_limit_rps=10.0)

    def _default_headers(self) -> dict[str, str]:
        headers = super()._default_headers()
        # GitHub raw CDN works fine with default headers; add Accept for clarity
        headers["Accept"] = "application/json"
        return headers

    async def is_available(self) -> bool:
        return True  # No API key gating — always attempt

    async def get_eth_labels(self) -> list[EntityLabel]:
        """
        Fetch ~30k labeled Ethereum addresses from brianleect/etherscan-labels.
        Returns EntityLabel objects for all addresses with a recognised entity type.
        Silently returns [] on any network/parse failure.
        """
        try:
            client = await self._get_client()
            resp = await client.get(_BRIAN_ETH_URL, timeout=httpx.Timeout(60.0, connect=15.0))
            resp.raise_for_status()
            raw: dict[str, Any] = resp.json()
        except Exception as exc:
            log.warning("community_labels_fetch_failed", url=_BRIAN_ETH_URL, error=str(exc))
            return []

        now_ms = int(time.time() * 1000)
        labels: list[EntityLabel] = []

        for address, meta in raw.items():
            if not isinstance(meta, dict):
                continue
            name: str = (meta.get("name") or "").strip()
            tag_list: list[str] = meta.get("labels") or []
            if not name and not tag_list:
                continue

            entity_type = _classify(name, tag_list)
            if entity_type is None:
                continue

            addr = address.lower()
            src = from_community_list(
                list_name="brianleect/etherscan-labels",
                list_url=_BRIAN_REPO_URL,
                label=name or tag_list[0],
            )

            labels.append(EntityLabel(
                address=addr,
                chain=Chain.ethereum,
                entity_name=name or tag_list[0],
                entity_type=entity_type,
                wallet_role=_wallet_role(entity_type, tag_list),
                sources=[src],
                confidence=src.confidence,
                first_seen_ms=now_ms,
                last_updated_ms=now_ms,
            ))

        log.info("community_labels_loaded",
                 total_parsed=len(raw), labeled=len(labels), source="brianleect/etherscan-labels")
        return labels

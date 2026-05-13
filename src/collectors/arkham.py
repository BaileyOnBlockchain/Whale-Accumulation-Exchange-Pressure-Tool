"""Arkham Intelligence collector — entity labels from the public API.

Arkham provides entity-level attribution for EVM addresses. Their free tier
supports ~10 req/min. With an API key the limits are higher.

Every label returned by this collector carries a ProvenanceRecord that cites:
  - The Arkham API endpoint used
  - The address explorer URL for independent verification
  - Arkham's methodology (entity clustering from on-chain + off-chain signals)

API docs: https://codex.arkhamintelligence.com
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

from src.collectors.base import BaseHTTPCollector
from src.config import settings, SOURCE_ARKHAM
from src.core.models import Chain, EntityLabel, EntityType, WalletRole
from src.corpus.provenance import from_arkham, aggregate_confidence
from src.utils.logging import get_logger

log = get_logger(__name__)

_BASE = "https://api.arkhamintelligence.com"

_ENTITY_TYPE_MAP: dict[str, EntityType] = {
    "exchange":    EntityType.exchange,
    "cex":         EntityType.exchange,
    "whale":       EntityType.whale,
    "institution": EntityType.institution,
    "defi":        EntityType.defi,
    "miner":       EntityType.miner,
    "bridge":      EntityType.bridge,
}


class ArkhamCollector(BaseHTTPCollector):
    source_name = SOURCE_ARKHAM

    def __init__(self, api_key: str = "") -> None:
        super().__init__(api_key=api_key or settings.arkham_api_key, rate_limit_rps=2.0)

    def _default_headers(self) -> dict[str, str]:
        headers = super()._default_headers()
        if self._api_key:
            headers["API-Key"] = self._api_key
        return headers

    async def is_available(self) -> bool:
        if not self._api_key:
            log.info("arkham_no_api_key", note="Free-tier public lookups only")
            return True
        try:
            await self.get(f"{_BASE}/intelligence/address/0xbe0eb53f46cd790cd13851d5eff43d12404d33e8")
            return True
        except Exception as exc:
            log.warning("arkham_unavailable", error=str(exc))
            return False

    async def get_entity(self, address: str, chain: str = "ethereum") -> EntityLabel | None:
        """
        Look up a single address in Arkham Intelligence.

        Returns an EntityLabel with Arkham provenance, or None if not found.
        """
        try:
            data = await self.get(
                f"{_BASE}/intelligence/address/{address.lower()}",
                params={"chain": chain},
            )
        except Exception as exc:
            log.debug("arkham_lookup_failed", address=address, error=str(exc))
            return None

        return self._parse_entity(data, address.lower(), chain)

    def _parse_entity(
        self, data: dict[str, Any], address: str, chain: str
    ) -> EntityLabel | None:
        # Arkham response shape: {entity: {name, type, ...}, arkhamEntity: {...}}
        entity = data.get("arkhamEntity") or data.get("entity") or {}
        if not entity:
            return None

        name = entity.get("name") or entity.get("id") or ""
        if not name:
            return None

        raw_type = (entity.get("type") or "").lower()
        entity_type = _ENTITY_TYPE_MAP.get(raw_type, EntityType.unknown)

        try:
            chain_obj = Chain(chain.lower())
        except ValueError:
            chain_obj = Chain.ethereum

        src = from_arkham(
            entity_name=name,
            entity_type=raw_type or "unknown",
            address=address,
        )

        return EntityLabel(
            address=address,
            chain=chain_obj,
            entity_name=name,
            entity_type=entity_type,
            wallet_role=WalletRole.unknown,
            sources=[src],
            confidence=src.confidence,
            last_updated_ms=int(time.time() * 1000),
        )

    async def batch_lookup(
        self, addresses: list[str], chain: str = "ethereum"
    ) -> list[EntityLabel]:
        """
        Look up multiple addresses with rate limiting.
        Returns only addresses where Arkham has a label.
        """
        results: list[EntityLabel] = []
        for addr in addresses:
            label = await self.get_entity(addr, chain)
            if label:
                results.append(label)
            await asyncio.sleep(0.5)   # respect 2 req/s default
        log.info("arkham_batch_complete",
                 queried=len(addresses), labeled=len(results), chain=chain)
        return results

    async def search_entity(self, name: str) -> list[dict[str, Any]]:
        """
        Search Arkham for entity addresses by name.
        Useful for expanding our corpus (e.g., find all Coinbase wallets).
        """
        try:
            data = await self.get(
                f"{_BASE}/intelligence/search",
                params={"q": name, "limit": 50},
            )
            return data.get("addresses", [])
        except Exception as exc:
            log.debug("arkham_search_failed", query=name, error=str(exc))
            return []

"""Dune Analytics Spellbook collector — community-maintained address labels.

The Dune Spellbook `labels.addresses` table is a community-curated, peer-reviewed
dataset of labeled on-chain addresses. Entries are merged via open GitHub PRs
(github.com/duneanalytics/spellbook), making the provenance fully auditable.

Queries used
------------
- labels.addresses WHERE category IN ('cex','whale','defi','bridge')
- cex.addresses — dedicated CEX wallet list
- All queries are public and rerunnable on dune.com

Free tier: 2,500 credits/month, no API key needed for public queries.
API key unlocks larger result sets and faster execution.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from src.collectors.base import BaseHTTPCollector
from src.config import settings, SOURCE_DUNE_SPELLBOOK
from src.core.models import Chain, EntityLabel, EntityType, WalletRole
from src.corpus.provenance import from_dune_spellbook
from src.utils.logging import get_logger

log = get_logger(__name__)

_BASE = "https://api.dune.com/api/v1"

# Public Dune Spellbook queries — latest cached results available via GET
# https://dune.com/queries/<id>
_QUERY_CEX_LABELS  = "2059912"   # cex.addresses ethereum — community maintained
_QUERY_ETH_LABELS  = "1982060"   # ethereum labels.addresses (blockchain='ethereum')

_CATEGORY_TO_ENTITY: dict[str, EntityType] = {
    "cex":         EntityType.exchange,
    "exchange":    EntityType.exchange,
    "whale":       EntityType.whale,
    "defi":        EntityType.defi,
    "bridge":      EntityType.bridge,
    "miner":       EntityType.miner,
    "nft":         EntityType.defi,
    "dao":         EntityType.institution,
}


class DuneCollector(BaseHTTPCollector):
    source_name = SOURCE_DUNE_SPELLBOOK

    def __init__(self, api_key: str = "") -> None:
        super().__init__(api_key=api_key or settings.dune_api_key, rate_limit_rps=1.0)

    def _default_headers(self) -> dict[str, str]:
        headers = super()._default_headers()
        if self._api_key:
            headers["X-Dune-API-Key"] = self._api_key
        return headers

    async def is_available(self) -> bool:
        return bool(self._api_key)

    async def get_cex_labels(self, blockchain: str = "ethereum") -> list[EntityLabel]:
        """
        Fetch exchange wallet labels from Dune's community cex.addresses spellbook.
        Uses GET /results to pull the latest cached run — no execution credits needed.

        Spellbook source:
          https://github.com/duneanalytics/spellbook/tree/main/models/cex
        """
        if not self._api_key:
            return []
        rows = await self._get_query_results(_QUERY_CEX_LABELS)
        labels = [l for r in rows if (l := self._row_to_label(r, blockchain))]
        log.info("dune_cex_labels_fetched", count=len(labels), blockchain=blockchain)
        return labels

    async def get_spellbook_labels(
        self,
        blockchain: str = "ethereum",
        categories: list[str] | None = None,
        limit: int = 5000,
    ) -> list[EntityLabel]:
        """
        Fetch labeled addresses from Dune Spellbook labels.addresses.
        Pulls the latest cached results of the public ETH labels query.
        """
        if not self._api_key:
            return []
        rows = await self._get_query_results(_QUERY_ETH_LABELS, limit=limit)
        labels = [l for r in rows if (l := self._row_to_label(r, blockchain))]
        log.info("dune_spellbook_labels_fetched", count=len(labels), blockchain=blockchain)
        return labels

    async def _get_query_results(
        self, query_id: str, limit: int = 10000
    ) -> list[dict[str, Any]]:
        """Fetch the latest cached results for a saved Dune query (no execution)."""
        try:
            data = await self.get(
                f"{_BASE}/query/{query_id}/results",
                params={"limit": limit},
            )
            return data.get("result", {}).get("rows", [])
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                log.debug("dune_query_not_found", query_id=query_id)
            else:
                log.warning("dune_results_failed", query_id=query_id,
                            status=exc.response.status_code, error=str(exc))
            return []
        except Exception as exc:
            log.warning("dune_results_failed", query_id=query_id, error=str(exc))
            return []

    def _row_to_label(
        self, row: dict[str, Any], blockchain: str
    ) -> EntityLabel | None:
        address = (row.get("address") or "").lower()
        name = row.get("name") or row.get("label") or ""
        category = (row.get("category") or row.get("type") or "").lower()

        if not address or not name:
            return None

        try:
            chain = Chain(blockchain.lower())
        except ValueError:
            return None

        entity_type = _CATEGORY_TO_ENTITY.get(category, EntityType.unknown)
        if entity_type == EntityType.unknown:
            return None

        src = from_dune_spellbook(
            label=name,
            category=category,
            query_id=_QUERY_CEX_LABELS,
        )

        role = WalletRole.hot_wallet if entity_type == EntityType.exchange else WalletRole.unknown

        return EntityLabel(
            address=address,
            chain=chain,
            entity_name=name,
            entity_type=entity_type,
            wallet_role=role,
            sources=[src],
            confidence=src.confidence,
            last_updated_ms=int(time.time() * 1000),
        )

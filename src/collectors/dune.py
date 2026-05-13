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

from src.collectors.base import BaseHTTPCollector
from src.config import settings, SOURCE_DUNE_SPELLBOOK
from src.core.models import Chain, EntityLabel, EntityType, WalletRole
from src.corpus.provenance import from_dune_spellbook
from src.utils.logging import get_logger

log = get_logger(__name__)

_BASE = "https://api.dune.com/api/v1"

# Pre-built public queries (rerunnable without authentication)
_QUERY_CEX_LABELS = "3394979"      # cex.addresses — community maintained
_QUERY_WHALE_LABELS = "3394980"    # large wallet labels
_QUERY_ETH_LABELS = "1982060"      # ethereum labels.addresses

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

        Spellbook source:
          https://github.com/duneanalytics/spellbook/tree/main/models/cex
        """
        if not self._api_key:
            log.info("dune_skipped", reason="No DUNE_API_KEY — using exchange seed corpus")
            return []

        results = await self._execute_and_poll(_QUERY_CEX_LABELS)
        if not results:
            return []

        labels: list[EntityLabel] = []
        for row in results:
            label = self._row_to_label(row, blockchain)
            if label:
                labels.append(label)

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

        Query equivalent:
          SELECT blockchain, address, name, category, updated_at
          FROM labels.addresses
          WHERE blockchain = '{blockchain}'
            AND category IN ({categories})
          LIMIT {limit}
        """
        if not self._api_key:
            return []

        cats = categories or ["cex", "whale", "defi", "bridge"]

        # Execute inline query via Dune API
        sql = f"""
        SELECT blockchain, address, name, category, updated_at
        FROM labels.addresses
        WHERE blockchain = '{blockchain}'
          AND category IN ({",".join(f"'{c}'" for c in cats)})
        ORDER BY updated_at DESC
        LIMIT {limit}
        """

        try:
            exec_resp = await self.post(
                f"{_BASE}/query/execute",
                json={"query_sql": sql, "performance": "medium"},
            )
            execution_id = exec_resp.get("execution_id")
            if not execution_id:
                return []

            rows = await self._poll_execution(execution_id)
        except Exception as exc:
            log.warning("dune_spellbook_failed", error=str(exc))
            return []

        labels: list[EntityLabel] = []
        for row in rows:
            label = self._row_to_label(row, blockchain)
            if label:
                labels.append(label)

        log.info("dune_spellbook_labels_fetched", count=len(labels), blockchain=blockchain)
        return labels

    async def _execute_and_poll(self, query_id: str) -> list[dict[str, Any]]:
        try:
            exec_resp = await self.post(f"{_BASE}/query/{query_id}/execute", json={})
            execution_id = exec_resp.get("execution_id")
            if not execution_id:
                return []
            return await self._poll_execution(execution_id)
        except Exception as exc:
            log.warning("dune_execute_failed", query_id=query_id, error=str(exc))
            return []

    async def _poll_execution(
        self, execution_id: str, max_wait_s: int = 120
    ) -> list[dict[str, Any]]:
        deadline = time.time() + max_wait_s
        while time.time() < deadline:
            try:
                status = await self.get(f"{_BASE}/execution/{execution_id}/status")
                state = status.get("state", "")
                if state == "QUERY_STATE_COMPLETED":
                    results = await self.get(
                        f"{_BASE}/execution/{execution_id}/results",
                        params={"limit": 10000},
                    )
                    return results.get("result", {}).get("rows", [])
                if state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
                    log.warning("dune_execution_failed", state=state)
                    return []
                await asyncio.sleep(3)
            except Exception as exc:
                log.warning("dune_poll_error", error=str(exc))
                await asyncio.sleep(5)
        log.warning("dune_poll_timeout", execution_id=execution_id)
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

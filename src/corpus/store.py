"""DuckDB-backed label corpus — the authoritative store of labeled addresses.

Schema
------
entity_labels     : One row per address. Core identity + aggregate confidence.
provenance_records: One row per (address, source). Full attribution chain.
balance_history   : Point-in-time balance snapshots for tracked addresses.
flow_aggregates   : Pre-aggregated daily net flows per exchange.

Why DuckDB
----------
Columnar storage makes filtering 500k+ addresses fast (e.g., WHERE chain='ethereum'
AND entity_type='exchange'). SQLite would scan row-by-row. Analytics queries
(SUM, AVG, window functions) are orders of magnitude faster.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import duckdb
import orjson

from src.config import settings
from src.core.models import Chain, EntityLabel, EntityType, ProvenanceRecord, WalletRole
from src.corpus.provenance import aggregate_confidence, build_provenance_dict
from src.utils.logging import get_logger

log = get_logger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS entity_labels (
    address          TEXT NOT NULL,
    chain            TEXT NOT NULL,
    entity_name      TEXT NOT NULL,
    entity_type      TEXT NOT NULL,
    wallet_role      TEXT NOT NULL DEFAULT 'unknown',
    confidence       FLOAT NOT NULL DEFAULT 0.0,
    source_count     INTEGER NOT NULL DEFAULT 0,
    first_seen_ms    BIGINT NOT NULL DEFAULT 0,
    last_updated_ms  BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (address, chain)
);

CREATE TABLE IF NOT EXISTS provenance_records (
    address         TEXT NOT NULL,
    chain           TEXT NOT NULL,
    source_name     TEXT NOT NULL,
    source_label    TEXT NOT NULL,
    source_url      TEXT NOT NULL DEFAULT '',
    methodology     TEXT NOT NULL DEFAULT '',
    confidence      FLOAT NOT NULL DEFAULT 0.0,
    fetched_at_ms   BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (address, chain, source_name)
);

CREATE TABLE IF NOT EXISTS balance_history (
    address         TEXT NOT NULL,
    chain           TEXT NOT NULL,
    asset           TEXT NOT NULL,
    balance_native  DOUBLE NOT NULL,
    balance_usd     DOUBLE NOT NULL,
    block_height    BIGINT NOT NULL DEFAULT 0,
    snapshot_ms     BIGINT NOT NULL,
    PRIMARY KEY (address, chain, asset, snapshot_ms)
);

CREATE TABLE IF NOT EXISTS flow_aggregates (
    exchange_name   TEXT NOT NULL,
    chain           TEXT NOT NULL,
    asset           TEXT NOT NULL,
    date_bucket     TEXT NOT NULL,   -- YYYY-MM-DD
    inflow_usd      DOUBLE NOT NULL DEFAULT 0.0,
    outflow_usd     DOUBLE NOT NULL DEFAULT 0.0,
    net_flow_usd    DOUBLE NOT NULL DEFAULT 0.0,
    tx_count        INTEGER NOT NULL DEFAULT 0,
    updated_ms      BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (exchange_name, chain, asset, date_bucket)
);
"""

_CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_labels_chain_type  ON entity_labels(chain, entity_type);
CREATE INDEX IF NOT EXISTS idx_labels_chain       ON entity_labels(chain);
CREATE INDEX IF NOT EXISTS idx_prov_address_chain ON provenance_records(address, chain);
CREATE INDEX IF NOT EXISTS idx_balance_chain_ms   ON balance_history(chain, asset, snapshot_ms DESC);
CREATE INDEX IF NOT EXISTS idx_flow_chain_date    ON flow_aggregates(chain, asset, date_bucket DESC);
"""


class CorpusStore:
    def __init__(self, db_path: str = settings.corpus_db_path) -> None:
        self._db_path = db_path
        self._lock = asyncio.Lock()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        import os
        os.makedirs(os.path.dirname(self._db_path) or ".", exist_ok=True)
        return duckdb.connect(self._db_path)

    async def initialize(self) -> None:
        await asyncio.to_thread(self._init_sync)
        log.info("corpus_store_initialized", db_path=self._db_path)

    def _init_sync(self) -> None:
        con = self._connect()
        con.execute(_SCHEMA)
        con.execute(_CREATE_INDEXES)
        con.close()

    # ── Upsert ────────────────────────────────────────────────────────────────

    async def upsert_label(self, label: EntityLabel) -> None:
        await asyncio.to_thread(self._upsert_label_sync, label)

    def _upsert_label_sync(self, label: EntityLabel) -> None:
        now_ms = int(time.time() * 1000)
        con = self._connect()
        try:
            con.execute("BEGIN")
            existing = con.execute(
                "SELECT first_seen_ms FROM entity_labels WHERE address=? AND chain=?",
                [label.address.lower(), label.chain.value],
            ).fetchone()

            first_seen = existing[0] if existing else now_ms

            con.execute(
                """
                INSERT INTO entity_labels
                    (address, chain, entity_name, entity_type, wallet_role,
                     confidence, source_count, first_seen_ms, last_updated_ms)
                VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT (address, chain) DO UPDATE SET
                    entity_name     = excluded.entity_name,
                    entity_type     = excluded.entity_type,
                    wallet_role     = excluded.wallet_role,
                    confidence      = excluded.confidence,
                    source_count    = excluded.source_count,
                    last_updated_ms = excluded.last_updated_ms
                """,
                [
                    label.address.lower(),
                    label.chain.value,
                    label.entity_name,
                    label.entity_type.value,
                    label.wallet_role.value,
                    label.confidence,
                    len(label.sources),
                    first_seen,
                    now_ms,
                ],
            )

            # Delete old provenance for this address+chain, then re-insert all sources
            con.execute(
                "DELETE FROM provenance_records WHERE address=? AND chain=?",
                [label.address.lower(), label.chain.value],
            )
            for src in label.sources:
                con.execute(
                    """
                    INSERT INTO provenance_records
                        (address, chain, source_name, source_label, source_url,
                         methodology, confidence, fetched_at_ms)
                    VALUES (?,?,?,?,?,?,?,?)
                    ON CONFLICT (address, chain, source_name) DO UPDATE SET
                        source_label  = excluded.source_label,
                        source_url    = excluded.source_url,
                        methodology   = excluded.methodology,
                        confidence    = excluded.confidence,
                        fetched_at_ms = excluded.fetched_at_ms
                    """,
                    [
                        label.address.lower(),
                        label.chain.value,
                        src.source_name,
                        src.source_label,
                        src.source_url,
                        src.methodology,
                        src.confidence,
                        src.fetched_at_ms,
                    ],
                )
            con.execute("COMMIT")
        except Exception:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            con.close()

    async def upsert_labels_bulk(self, labels: list[EntityLabel]) -> int:
        return await asyncio.to_thread(self._upsert_bulk_sync, labels)

    def _upsert_bulk_sync(self, labels: list[EntityLabel]) -> int:
        count = 0
        for label in labels:
            try:
                self._upsert_label_sync(label)
                count += 1
            except Exception as exc:
                log.warning("corpus_upsert_error", address=label.address, error=str(exc))
        return count

    # ── Lookup ────────────────────────────────────────────────────────────────

    async def get_label(self, address: str, chain: str) -> EntityLabel | None:
        return await asyncio.to_thread(self._get_label_sync, address.lower(), chain.lower())

    def _get_label_sync(self, address: str, chain: str) -> EntityLabel | None:
        con = self._connect()
        try:
            row = con.execute(
                "SELECT * FROM entity_labels WHERE address=? AND chain=?",
                [address, chain],
            ).fetchone()
            if not row:
                return None

            cols = [d[0] for d in con.description]
            data = dict(zip(cols, row))

            prov_rows = con.execute(
                "SELECT * FROM provenance_records WHERE address=? AND chain=? ORDER BY fetched_at_ms DESC",
                [address, chain],
            ).fetchall()
            prov_cols = [d[0] for d in con.description]

            sources = []
            for pr in prov_rows:
                pd = dict(zip(prov_cols, pr))
                sources.append(ProvenanceRecord(
                    source_name=pd["source_name"],
                    source_label=pd["source_label"],
                    source_url=pd["source_url"],
                    methodology=pd["methodology"],
                    confidence=pd["confidence"],
                    fetched_at_ms=pd["fetched_at_ms"],
                ))

            return EntityLabel(
                address=data["address"],
                chain=Chain(data["chain"]),
                entity_name=data["entity_name"],
                entity_type=EntityType(data["entity_type"]),
                wallet_role=WalletRole(data["wallet_role"]),
                sources=sources,
                confidence=data["confidence"],
                first_seen_ms=data["first_seen_ms"],
                last_updated_ms=data["last_updated_ms"],
            )
        finally:
            con.close()

    async def get_exchange_addresses(
        self, chain: str, exchange_name: str | None = None
    ) -> list[EntityLabel]:
        return await asyncio.to_thread(
            self._get_exchange_addresses_sync, chain.lower(), exchange_name
        )

    def _get_exchange_addresses_sync(
        self, chain: str, exchange_name: str | None
    ) -> list[EntityLabel]:
        con = self._connect()
        try:
            if exchange_name:
                rows = con.execute(
                    "SELECT address FROM entity_labels WHERE chain=? AND entity_type='exchange' "
                    "AND lower(entity_name)=lower(?)",
                    [chain, exchange_name],
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT address FROM entity_labels WHERE chain=? AND entity_type='exchange'",
                    [chain],
                ).fetchall()
            addresses = [r[0] for r in rows]
        finally:
            con.close()

        labels = []
        for addr in addresses:
            lbl = self._get_label_sync(addr, chain)
            if lbl:
                labels.append(lbl)
        return labels

    async def get_whale_addresses(self, chain: str) -> list[str]:
        return await asyncio.to_thread(self._get_whale_addresses_sync, chain.lower())

    def _get_whale_addresses_sync(self, chain: str) -> list[str]:
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT address FROM entity_labels WHERE chain=? AND entity_type='whale' "
                "ORDER BY confidence DESC",
                [chain],
            ).fetchall()
            return [r[0] for r in rows]
        finally:
            con.close()

    async def corpus_stats(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._corpus_stats_sync)

    def _corpus_stats_sync(self) -> dict[str, Any]:
        con = self._connect()
        try:
            total = con.execute("SELECT COUNT(*) FROM entity_labels").fetchone()[0]
            by_type = dict(con.execute(
                "SELECT entity_type, COUNT(*) FROM entity_labels GROUP BY entity_type"
            ).fetchall())
            by_chain = dict(con.execute(
                "SELECT chain, COUNT(*) FROM entity_labels GROUP BY chain"
            ).fetchall())
            prov_count = con.execute("SELECT COUNT(*) FROM provenance_records").fetchone()[0]
            by_source = dict(con.execute(
                "SELECT source_name, COUNT(*) FROM provenance_records GROUP BY source_name"
            ).fetchall())
            return {
                "total_labels":      total,
                "provenance_records": prov_count,
                "by_entity_type":    by_type,
                "by_chain":          by_chain,
                "by_source":         by_source,
            }
        finally:
            con.close()

    # ── Balance history ───────────────────────────────────────────────────────

    async def record_balance_snapshot(
        self,
        address: str,
        chain: str,
        asset: str,
        balance_native: float,
        balance_usd: float,
        block_height: int = 0,
    ) -> None:
        await asyncio.to_thread(
            self._record_balance_sync,
            address.lower(), chain.lower(), asset.upper(),
            balance_native, balance_usd, block_height,
        )

    def _record_balance_sync(
        self, address: str, chain: str, asset: str,
        balance_native: float, balance_usd: float, block_height: int,
    ) -> None:
        now_ms = int(time.time() * 1000)
        con = self._connect()
        try:
            con.execute(
                """
                INSERT INTO balance_history
                    (address, chain, asset, balance_native, balance_usd, block_height, snapshot_ms)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT (address, chain, asset, snapshot_ms) DO NOTHING
                """,
                [address, chain, asset, balance_native, balance_usd, block_height, now_ms],
            )
        finally:
            con.close()

    async def get_balance_history(
        self, address: str, chain: str, asset: str, limit: int = 30
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(
            self._get_balance_history_sync,
            address.lower(), chain.lower(), asset.upper(), limit,
        )

    def _get_balance_history_sync(
        self, address: str, chain: str, asset: str, limit: int
    ) -> list[dict[str, Any]]:
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT balance_native, balance_usd, block_height, snapshot_ms "
                "FROM balance_history WHERE address=? AND chain=? AND asset=? "
                "ORDER BY snapshot_ms DESC LIMIT ?",
                [address, chain, asset, limit],
            ).fetchall()
            return [
                {"balance_native": r[0], "balance_usd": r[1],
                 "block_height": r[2], "snapshot_ms": r[3]}
                for r in rows
            ]
        finally:
            con.close()

    # ── Flow aggregates ───────────────────────────────────────────────────────

    async def upsert_flow_aggregate(
        self,
        exchange_name: str,
        chain: str,
        asset: str,
        date_bucket: str,
        inflow_usd: float,
        outflow_usd: float,
        tx_count: int,
    ) -> None:
        await asyncio.to_thread(
            self._upsert_flow_sync,
            exchange_name, chain.lower(), asset.upper(),
            date_bucket, inflow_usd, outflow_usd, tx_count,
        )

    def _upsert_flow_sync(
        self, exchange_name: str, chain: str, asset: str,
        date_bucket: str, inflow_usd: float, outflow_usd: float, tx_count: int,
    ) -> None:
        now_ms = int(time.time() * 1000)
        con = self._connect()
        try:
            con.execute(
                """
                INSERT INTO flow_aggregates
                    (exchange_name, chain, asset, date_bucket,
                     inflow_usd, outflow_usd, net_flow_usd, tx_count, updated_ms)
                VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT (exchange_name, chain, asset, date_bucket) DO UPDATE SET
                    inflow_usd  = excluded.inflow_usd,
                    outflow_usd = excluded.outflow_usd,
                    net_flow_usd= excluded.net_flow_usd,
                    tx_count    = excluded.tx_count,
                    updated_ms  = excluded.updated_ms
                """,
                [
                    exchange_name, chain, asset, date_bucket,
                    inflow_usd, outflow_usd, inflow_usd - outflow_usd,
                    tx_count, now_ms,
                ],
            )
        finally:
            con.close()

    async def get_flow_aggregates(
        self, chain: str, asset: str, days: int = 30
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(
            self._get_flow_aggregates_sync, chain.lower(), asset.upper(), days
        )

    def _get_flow_aggregates_sync(
        self, chain: str, asset: str, days: int
    ) -> list[dict[str, Any]]:
        from datetime import date, timedelta
        cutoff = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
        con = self._connect()
        try:
            rows = con.execute(
                """
                SELECT exchange_name, date_bucket,
                       SUM(inflow_usd) AS inflow_usd,
                       SUM(outflow_usd) AS outflow_usd,
                       SUM(net_flow_usd) AS net_flow_usd,
                       SUM(tx_count) AS tx_count
                FROM flow_aggregates
                WHERE chain=? AND asset=? AND date_bucket >= ?
                GROUP BY exchange_name, date_bucket
                ORDER BY date_bucket DESC
                """,
                [chain, asset, cutoff],
            ).fetchall()
            cols = ["exchange_name", "date_bucket", "inflow_usd", "outflow_usd",
                    "net_flow_usd", "tx_count"]
            return [dict(zip(cols, r)) for r in rows]
        finally:
            con.close()


# Singleton
_store: CorpusStore | None = None


def get_store() -> CorpusStore:
    global _store
    if _store is None:
        _store = CorpusStore()
    return _store

"""Shared Pydantic models for the whale accumulation pipeline."""
from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel


# ── Enums ─────────────────────────────────────────────────────────────────────

class Chain(str, Enum):
    ethereum = "ethereum"
    bitcoin  = "bitcoin"
    solana   = "solana"
    arbitrum = "arbitrum"
    optimism = "optimism"
    base     = "base"
    polygon  = "polygon"

class EntityType(str, Enum):
    exchange    = "exchange"
    whale       = "whale"
    institution = "institution"
    defi        = "defi"
    miner       = "miner"
    bridge      = "bridge"
    unknown     = "unknown"

class WalletRole(str, Enum):
    hot_wallet    = "hot_wallet"
    cold_wallet   = "cold_wallet"
    deposit       = "deposit"
    withdrawal    = "withdrawal"
    custody       = "custody"
    accumulation  = "accumulation"
    unknown       = "unknown"

class FlowDirection(str, Enum):
    inflow  = "inflow"   # funds entering exchange
    outflow = "outflow"  # funds leaving exchange

class AccumulationRating(str, Enum):
    strong_accumulation   = "Strong Accumulation"
    moderate_accumulation = "Moderate Accumulation"
    neutral               = "Neutral"
    moderate_distribution = "Moderate Distribution"
    strong_distribution   = "Strong Distribution"


# ── Provenance ────────────────────────────────────────────────────────────────

class ProvenanceRecord(BaseModel):
    """Single attribution record for an address label."""
    source_name:   str           # e.g. "arkham_intelligence", "dune_spellbook"
    source_label:  str           # raw label text from that source
    source_url:    str = ""      # URL or query link for verification
    methodology:   str = ""      # how we derived this label
    confidence:    float = 1.0   # 0.0–1.0
    fetched_at_ms: int = 0       # unix ms when this record was fetched


class EntityLabel(BaseModel):
    """A labeled address in our corpus."""
    address:     str
    chain:       Chain
    entity_name: str
    entity_type: EntityType
    wallet_role: WalletRole = WalletRole.unknown
    sources:     list[ProvenanceRecord] = []
    confidence:  float = 1.0    # aggregate across all sources
    first_seen_ms:   int = 0
    last_updated_ms: int = 0

    @property
    def is_exchange(self) -> bool:
        return self.entity_type == EntityType.exchange

    @property
    def is_whale(self) -> bool:
        return self.entity_type == EntityType.whale

    @property
    def source_names(self) -> list[str]:
        return [s.source_name for s in self.sources]

    def provenance_summary(self) -> dict[str, Any]:
        return {
            "sources":    self.source_names,
            "confidence": round(self.confidence, 3),
            "source_count": len(self.sources),
            "labels":     [s.source_label for s in self.sources],
            "methodologies": list({s.methodology for s in self.sources if s.methodology}),
        }


# ── Balance snapshot ──────────────────────────────────────────────────────────

class BalanceSnapshot(BaseModel):
    """Point-in-time balance for a tracked address."""
    address:        str
    chain:          Chain
    balance_native: float   # in chain-native units (ETH, BTC, etc.)
    balance_usd:    float
    asset:          str     # "ETH", "BTC", etc.
    block_height:   int = 0
    snapshot_ms:    int = 0


class BalanceDelta(BaseModel):
    """Change in balance between two snapshots."""
    address:           str
    chain:             Chain
    asset:             str
    prev_balance_usd:  float
    curr_balance_usd:  float
    delta_usd:         float
    delta_pct:         float
    period_start_ms:   int
    period_end_ms:     int
    entity_name:       str = ""
    entity_type:       EntityType = EntityType.unknown


# ── Exchange flow ─────────────────────────────────────────────────────────────

class ExchangeFlowEvent(BaseModel):
    """Single inflow or outflow event involving a known exchange wallet."""
    tx_hash:        str
    chain:          Chain
    asset:          str
    direction:      FlowDirection
    exchange_name:  str
    exchange_wallet: str
    counterparty:   str
    amount_native:  float
    amount_usd:     float
    timestamp_ms:   int
    block_height:   int = 0

    def to_redis_fields(self) -> dict[str, str]:
        return {
            "tx_hash":          self.tx_hash,
            "asset":            self.asset,
            "direction":        self.direction.value,
            "exchange_name":    self.exchange_name,
            "exchange_wallet":  self.exchange_wallet,
            "counterparty":     self.counterparty,
            "amount_native":    str(self.amount_native),
            "amount_usd":       str(self.amount_usd),
            "timestamp_ms":     str(self.timestamp_ms),
        }


class ExchangeReserve(BaseModel):
    """Current on-chain reserve for a single exchange."""
    exchange_name:  str
    chain:          Chain
    asset:          str
    balance_native: float
    balance_usd:    float
    wallet_count:   int
    updated_ms:     int


# ── Signals ───────────────────────────────────────────────────────────────────

class WhaleFlowSummary(BaseModel):
    """Aggregate whale balance changes over a time window."""
    chain:              Chain
    asset:              str
    lookback_hours:     int

    total_accumulating_usd:   float
    total_distributing_usd:   float
    net_flow_usd:             float    # positive = net accumulation

    accumulating_count:  int
    distributing_count:  int
    neutral_count:       int
    tracked_count:       int
    labeled_count:       int

    top_accumulators:    list[dict[str, Any]] = []
    top_distributors:    list[dict[str, Any]] = []

    provenance: dict[str, Any] = {}
    confidence: float = 0.0


class ExchangePressureSummary(BaseModel):
    """Aggregate exchange flow metrics over a time window."""
    chain:          Chain
    asset:          str
    lookback_hours: int

    total_inflow_usd:   float   # coins entering exchanges (sell pressure)
    total_outflow_usd:  float   # coins leaving exchanges (accumulation signal)
    net_flow_usd:       float   # negative = net outflow (bullish)

    reserve_change_usd: float   # change in total exchange reserves
    reserve_change_pct: float

    exchange_breakdown: list[dict[str, Any]] = []

    provenance: dict[str, Any] = {}
    confidence: float = 0.0


class AccumulationSignal(BaseModel):
    """
    Composite on-chain accumulation signal.

    Score: -100 (maximum distribution) to +100 (maximum accumulation).
    Positive = whales accumulating, coins leaving exchanges.
    Negative = whales distributing, coins piling into exchanges.
    """
    chain:  Chain
    asset:  str

    score:  float   # -100 to +100
    rating: str     # AccumulationRating label

    components: dict[str, float]    # score breakdown by component
    evidence:   dict[str, Any]

    whale_flow:      WhaleFlowSummary | None = None
    exchange_pressure: ExchangePressureSummary | None = None

    provenance: dict[str, Any] = {}
    confidence: float = 0.0
    freshness:  str = ""


# ── HTTP / MCP response wrappers ──────────────────────────────────────────────

class LabelLookupResponse(BaseModel):
    address:    str
    chain:      str
    found:      bool
    labels:     list[dict[str, Any]] = []
    provenance: dict[str, Any] = {}
    confidence: float = 0.0
    freshness:  str = ""
    evidence:   dict[str, Any] = {}


class TopHoldersResponse(BaseModel):
    chain:      str
    asset:      str
    top_n:      int
    holders:    list[dict[str, Any]] = []
    labeled_pct: float = 0.0
    provenance: dict[str, Any] = {}
    confidence: float = 0.0
    freshness:  str = ""
    evidence:   dict[str, Any] = {}

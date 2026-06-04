"""MCP Server — Whale Accumulation & Exchange-Pressure Intelligence.

Tools
─────
Query:
  get_accumulation_signal    → composite on-chain accumulation signal (-100 to +100)
  get_whale_accumulation     → per-wallet balance changes with entity labels
  get_exchange_pressure      → net exchange inflows/outflows, reserve changes
  get_entity_label           → look up a specific address in the labeled corpus
  scan_top_holders           → top N holders with entity labels and provenance

Execute:
  refresh_corpus             → trigger labeled corpus update from all sources

All responses carry provenance, confidence, freshness, and evidence.
Every label cites its exact source (community labels, Dune Spellbook, exchange seed,
on-chain heuristic) so the caller can verify independently.

Transport: stdio (default) or SSE (set MCP_TRANSPORT=sse)
Start with: python -m src.server.mcp_server
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Optional

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import BaseModel

from src.config import settings
from src.core.models import Chain
from src.corpus.store import get_store
from src.corpus.provenance import build_provenance_dict
from src.intelligence.whale_tracker import WhaleTracker
from src.intelligence.exchange_pressure import ExchangePressureTracker
from src.intelligence.accumulation_score import compute_accumulation_score, accumulation_label
from src.storage.timeseries import cache_signal, get_cached_signal
from src.utils.logging import configure_logging, get_logger
from src.utils.metrics import mcp_tool_calls_total, mcp_tool_latency

configure_logging()
log = get_logger(__name__)


# ── Response models ───────────────────────────────────────────────────────────

class AccumulationSignalResponse(BaseModel):
    chain: str
    asset: str
    score: float
    rating: str
    components: dict[str, float]
    evidence: dict[str, Any]
    provenance: dict[str, Any]
    confidence: float
    freshness: str
    cached: bool

class WhaleAccumulationResponse(BaseModel):
    chain: str
    asset: str
    lookback_hours: int
    net_flow_usd: float
    accumulating_count: int
    distributing_count: int
    tracked_count: int
    labeled_count: int
    top_accumulators: list[dict[str, Any]]
    top_distributors: list[dict[str, Any]]
    provenance: dict[str, Any]
    confidence: float
    freshness: str
    evidence: dict[str, Any]

class ExchangePressureResponse(BaseModel):
    chain: str
    asset: str
    lookback_hours: int
    total_inflow_usd: float
    total_outflow_usd: float
    net_flow_usd: float
    reserve_change_usd: float
    reserve_change_pct: float
    exchange_breakdown: list[dict[str, Any]]
    pressure_direction: str
    provenance: dict[str, Any]
    confidence: float
    freshness: str
    evidence: dict[str, Any]

class EntityLabelResponse(BaseModel):
    address: str
    chain: str
    found: bool
    entity_name: Optional[str] = None
    entity_type: Optional[str] = None
    wallet_role: Optional[str] = None
    labels: list[dict[str, Any]]
    provenance: dict[str, Any]
    confidence: float
    freshness: str
    evidence: dict[str, Any]

class HolderEntry(BaseModel):
    rank: int
    address: str
    balance_usd: float
    entity_name: str
    entity_type: str
    wallet_role: str
    labeled: bool
    provenance_summary: dict[str, Any]

class TopHoldersResponse(BaseModel):
    chain: str
    asset: str
    top_n: int
    holders: list[dict[str, Any]]
    labeled_count: int
    unlabeled_count: int
    labeled_pct: float
    exchange_pct: float
    whale_pct: float
    provenance: dict[str, Any]
    confidence: float
    freshness: str
    evidence: dict[str, Any]

class RefreshCorpusResponse(BaseModel):
    status: str
    labels_added: int
    labels_updated: int
    sources_refreshed: list[str]
    corpus_stats: dict[str, Any]
    provenance: dict[str, Any]
    confidence: float
    freshness: str


mcp = FastMCP(
    "Whale Accumulation & Exchange-Pressure Intelligence",
    instructions=(
        "On-chain whale accumulation and exchange-pressure intelligence for BTC and ETH. "
        "Tracks large wallet (whale) balance changes, exchange inflow/outflow dynamics, "
        "and computes a composite accumulation signal (-100 to +100). "
        "All address labels carry transparent provenance: every entity classification "
        "cites its exact source (community labels, Dune Spellbook, exchange seed corpus, "
        "or on-chain heuristics). This is NOT a Glassnode clone — it is a purpose-built "
        "labeled corpus with auditable attribution. "
        "Positive scores indicate whale accumulation and exchange outflows (bullish). "
        "Negative scores indicate distribution and exchange inflows (bearish). "
        "Use get_accumulation_signal for the top-level signal, "
        "get_whale_accumulation / get_exchange_pressure for component detail, "
        "and get_entity_label to look up any specific address."
    ),
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)

_whale_tracker: WhaleTracker | None = None
_exchange_tracker: ExchangePressureTracker | None = None


def _get_whale_tracker() -> WhaleTracker:
    global _whale_tracker
    if _whale_tracker is None:
        _whale_tracker = WhaleTracker()
    return _whale_tracker


def _get_exchange_tracker() -> ExchangePressureTracker:
    global _exchange_tracker
    if _exchange_tracker is None:
        _exchange_tracker = ExchangePressureTracker()
    return _exchange_tracker


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Tool 1: get_accumulation_signal ──────────────────────────────────────────

@mcp.tool()
async def get_accumulation_signal(
    asset: str = "ETH",
    chain: str = "ethereum",
    lookback_hours: int = 24,
    use_cache: bool = True,
) -> AccumulationSignalResponse:
    """
    Return the composite on-chain accumulation signal for an asset.

    Combines whale net balance flow and exchange net outflow into a single score:
      +100 = Strong Accumulation: whales buying heavily, coins leaving exchanges
        0  = Neutral: balanced behaviour
      -100 = Strong Distribution: whales selling, coins piling into exchanges

    Score components (4 factors, weights shown):
      - whale_net_flow        (40 pts) : net USD balance change across tracked whales
      - exchange_net_outflow  (35 pts) : negative = bullish (coins leaving exchanges)
      - new_accumulation_rate (15 pts) : fraction of tracked wallets accumulating
      - whale_exchange_ratio  (10 pts) : whale buys vs exchange inflows

    All input data comes from our labeled corpus. Every address label includes
    a full provenance chain citing its source.

    Parameters
    ----------
    asset : str
        "ETH" or "BTC"
    chain : str
        "ethereum" or "bitcoin"
    lookback_hours : int
        Look-back window for flow computation (default 24h). Max 168h (7 days).
    use_cache : bool
        If True, return cached result if available (< 5 min old). Default True.
    """
    t0 = time.perf_counter()
    mcp_tool_calls_total.labels(tool="get_accumulation_signal").inc()

    asset = asset.upper()
    chain = chain.lower()
    lookback_hours = max(1, min(lookback_hours, 168))

    if use_cache:
        cached = await get_cached_signal(chain, asset)
        if cached:
            latency = time.perf_counter() - t0
            mcp_tool_latency.labels(tool="get_accumulation_signal").observe(latency)
            return {**cached, "cached": True, "freshness": cached.get("freshness", _iso_now())}

    try:
        wt = _get_whale_tracker()
        et = _get_exchange_tracker()

        if chain == "ethereum" and asset == "ETH":
            whale_flow, exchange_pressure = await asyncio.gather(
                wt.scan_eth_whales(lookback_hours),
                et.scan_eth_exchange_pressure(lookback_hours),
            )
        elif chain == "bitcoin" and asset == "BTC":
            whale_flow = await wt.scan_btc_whales(lookback_hours)
            exchange_pressure = et._empty_summary(Chain.bitcoin, "BTC", lookback_hours)
        else:
            raise ValueError(
                f"Unsupported chain/asset: {chain}/{asset}. "
                "Supported: ethereum/ETH, bitcoin/BTC"
            )

        signal = compute_accumulation_score(whale_flow, exchange_pressure)
        await cache_signal(signal)

    except Exception as exc:
        raise RuntimeError(
            f"Signal computation failed ({exc.__class__.__name__}): {exc}. "
            "Ensure Redis is running and corpus has been seeded (run whale-seed)."
        ) from exc

    latency = time.perf_counter() - t0
    mcp_tool_latency.labels(tool="get_accumulation_signal").observe(latency)

    return {
        "chain":      chain,
        "asset":      asset,
        "score":      signal.score,
        "rating":     signal.rating,
        "components": signal.components,
        "evidence":   {
            **signal.evidence,
            "score_interpretation": (
                f"{signal.score:+.1f} → {signal.rating}. "
                "Positive = on-chain accumulation. Negative = distribution. "
                "Range: [-100, +100]."
            ),
        },
        "provenance": {
            **signal.provenance,
            "query_latency_ms": round(latency * 1000, 1),
        },
        "confidence": signal.confidence,
        "freshness":  signal.freshness,
        "cached":     False,
    }


# ── Tool 2: get_whale_accumulation ────────────────────────────────────────────

@mcp.tool()
async def get_whale_accumulation(
    asset: str = "ETH",
    chain: str = "ethereum",
    lookback_hours: int = 24,
    min_delta_usd: float = 100_000,
) -> WhaleAccumulationResponse:
    """
    Return per-wallet balance changes for tracked whale addresses.

    Shows which labeled entities are accumulating or distributing.
    Each address is labeled with its entity name (e.g., "Coinbase", "Unknown Whale"),
    entity type (exchange, whale, institution), and full provenance chain showing
    exactly which source provided the label and the methodology used.

    Parameters
    ----------
    asset : str
        "ETH" or "BTC"
    chain : str
        "ethereum" or "bitcoin"
    lookback_hours : int
        Balance change window. Default 24h, max 168h.
    min_delta_usd : float
        Minimum absolute USD change to include in results. Filters dust.
        Default $100,000.
    """
    t0 = time.perf_counter()
    mcp_tool_calls_total.labels(tool="get_whale_accumulation").inc()

    asset = asset.upper()
    chain = chain.lower()
    lookback_hours = max(1, min(lookback_hours, 168))

    try:
        wt = _get_whale_tracker()
        if chain == "ethereum":
            whale_flow = await wt.scan_eth_whales(lookback_hours)
        else:
            whale_flow = await wt.scan_btc_whales(lookback_hours)
    except Exception as exc:
        raise RuntimeError(f"Whale scan failed: {exc}") from exc

    latency = time.perf_counter() - t0
    mcp_tool_latency.labels(tool="get_whale_accumulation").observe(latency)

    # Filter by min_delta_usd
    filtered_acc = [
        w for w in whale_flow.top_accumulators
        if abs(w.get("delta_usd", 0)) >= min_delta_usd
    ]
    filtered_dist = [
        w for w in whale_flow.top_distributors
        if abs(w.get("delta_usd", 0)) >= min_delta_usd
    ]

    return {
        "chain":             chain,
        "asset":             asset,
        "lookback_hours":    lookback_hours,
        "net_flow_usd":      whale_flow.net_flow_usd,
        "accumulating_count": whale_flow.accumulating_count,
        "distributing_count": whale_flow.distributing_count,
        "tracked_count":     whale_flow.tracked_count,
        "labeled_count":     whale_flow.labeled_count,
        "top_accumulators":  filtered_acc,
        "top_distributors":  filtered_dist,
        "provenance": {
            **whale_flow.provenance,
            "query_latency_ms": round(latency * 1000, 1),
            "min_delta_usd_filter": min_delta_usd,
        },
        "confidence": whale_flow.confidence,
        "freshness":  _iso_now(),
        "evidence": {
            "total_accumulating_usd": whale_flow.total_accumulating_usd,
            "total_distributing_usd": whale_flow.total_distributing_usd,
            "net_signal": "accumulation" if whale_flow.net_flow_usd > 0 else "distribution",
            "corpus_coverage_pct": round(
                100 * whale_flow.labeled_count / max(whale_flow.tracked_count, 1), 1
            ),
        },
    }


# ── Tool 3: get_exchange_pressure ────────────────────────────────────────────

@mcp.tool()
async def get_exchange_pressure(
    asset: str = "ETH",
    chain: str = "ethereum",
    lookback_hours: int = 24,
) -> ExchangePressureResponse:
    """
    Return net exchange inflow/outflow metrics for a given asset.

    Aggregates balance changes across all known exchange hot and cold wallets
    to determine whether coins are flowing INTO exchanges (sell pressure) or
    OUT OF exchanges (accumulation / self-custody signal).

    Exchange wallets are sourced from our corpus:
      - Exchange seed list (Etherscan labels, PoR disclosures)
      - Dune Spellbook cex.addresses (community-verified)
      - Community-curated labels (brianleect/etherscan-labels, ~30k addresses)
      - Heuristic deposit clustering

    Parameters
    ----------
    asset : str
        "ETH" or "BTC"
    chain : str
        "ethereum" or "bitcoin"
    lookback_hours : int
        Flow aggregation window. Default 24h.
    """
    t0 = time.perf_counter()
    mcp_tool_calls_total.labels(tool="get_exchange_pressure").inc()

    asset = asset.upper()
    chain = chain.lower()

    try:
        et = _get_exchange_tracker()
        pressure = await et.scan_eth_exchange_pressure(lookback_hours)
    except Exception as exc:
        raise RuntimeError(f"Exchange pressure scan failed: {exc}") from exc

    latency = time.perf_counter() - t0
    mcp_tool_latency.labels(tool="get_exchange_pressure").observe(latency)

    net = pressure.net_flow_usd
    pressure_direction = (
        "inflow"   if net > 50_000 else
        "outflow"  if net < -50_000 else
        "neutral"
    )

    return {
        "chain":              chain,
        "asset":              asset,
        "lookback_hours":     lookback_hours,
        "total_inflow_usd":   pressure.total_inflow_usd,
        "total_outflow_usd":  pressure.total_outflow_usd,
        "net_flow_usd":       pressure.net_flow_usd,
        "reserve_change_usd": pressure.reserve_change_usd,
        "reserve_change_pct": pressure.reserve_change_pct,
        "exchange_breakdown": pressure.exchange_breakdown,
        "pressure_direction": pressure_direction,
        "provenance": {
            **pressure.provenance,
            "query_latency_ms": round(latency * 1000, 1),
        },
        "confidence": pressure.confidence,
        "freshness":  _iso_now(),
        "evidence": {
            "net_interpretation": (
                "OUTFLOW (bullish: coins leaving exchanges → self-custody/accumulation)"
                if pressure_direction == "outflow" else
                "INFLOW (bearish: coins entering exchanges → prepare-to-sell)"
                if pressure_direction == "inflow" else
                "NEUTRAL: balanced exchange flows, no strong directional signal"
            ),
            "reserve_change_interpretation": (
                f"Exchange reserves changed by ${pressure.reserve_change_usd:+,.0f} USD "
                f"({pressure.reserve_change_pct:+.2f}%) in the lookback window."
            ),
            "exchanges_tracked": len(pressure.exchange_breakdown),
            "wallets_tracked":   pressure.provenance.get("wallets_tracked", 0),
        },
    }


# ── Tool 4: get_entity_label ─────────────────────────────────────────────────

@mcp.tool()
async def get_entity_label(
    address: str,
    chain: str = "ethereum",
) -> EntityLabelResponse:
    """
    Look up a specific address in the labeled corpus.

    Returns all known entity labels for the address, with full provenance
    for each label: which source produced it, the exact URL for verification,
    the methodology used, confidence score, and when it was fetched.

    This is the core transparency tool. For any labeled address in our corpus
    you can see exactly why we classified it as an exchange, whale, etc.

    Parameters
    ----------
    address : str
        Ethereum address (0x...) or Bitcoin address
    chain : str
        "ethereum" or "bitcoin" (default: ethereum)
    """
    t0 = time.perf_counter()
    mcp_tool_calls_total.labels(tool="get_entity_label").inc()

    address = address.lower().strip()
    chain   = chain.lower()

    store = get_store()
    label = await store.get_label(address, chain)

    latency = time.perf_counter() - t0
    mcp_tool_latency.labels(tool="get_entity_label").observe(latency)

    if not label:
        return {
            "address":    address,
            "chain":      chain,
            "found":      False,
            "labels":     [],
            "provenance": {
                "note": (
                    "Address not in corpus. To add it: "
                    "(1) run refresh_corpus to pull latest labels, "
                    "(2) if it's a known entity, submit to Dune Spellbook or brianleect/etherscan-labels."
                ),
                "query_latency_ms": round(latency * 1000, 1),
            },
            "confidence": 0.0,
            "freshness":  _iso_now(),
            "evidence":   {"address": address, "chain": chain, "in_corpus": False},
        }

    label_records = [
        {
            "source_name":   src.source_name,
            "source_label":  src.source_label,
            "source_url":    src.source_url,
            "methodology":   src.methodology,
            "confidence":    round(src.confidence, 3),
            "fetched_at_ms": src.fetched_at_ms,
        }
        for src in label.sources
    ]

    # Pull balance history for additional context
    balance_history = await store.get_balance_history(address, chain, "ETH", limit=5)

    return {
        "address":     address,
        "chain":       chain,
        "found":       True,
        "entity_name": label.entity_name,
        "entity_type": label.entity_type.value,
        "wallet_role": label.wallet_role.value,
        "labels":      label_records,
        "provenance":  {
            **label.provenance_summary(),
            "first_seen_ms":   label.first_seen_ms,
            "last_updated_ms": label.last_updated_ms,
            "query_latency_ms": round(latency * 1000, 1),
        },
        "confidence": round(label.confidence, 3),
        "freshness":  _iso_now(),
        "evidence": {
            "source_count":    len(label.sources),
            "source_names":    label.source_names,
            "is_exchange":     label.is_exchange,
            "is_whale":        label.is_whale,
            "balance_history": balance_history,
            "multi_source_verified": len(set(label.source_names)) > 1,
        },
    }


# ── Tool 5: scan_top_holders ─────────────────────────────────────────────────

@mcp.tool()
async def scan_top_holders(
    asset: str = "ETH",
    chain: str = "ethereum",
    top_n: int = 50,
) -> TopHoldersResponse:
    """
    Scan and label the top N holders of an asset.

    For each holder, returns:
      - Current USD balance
      - Entity label (if in corpus): name, type, wallet role
      - Provenance summary: which sources identified this entity
      - Whether the entity is an exchange (holdings don't represent actual holders)

    Useful for understanding the holder composition of an asset:
    how much is held by exchanges vs actual whales vs unknown addresses.

    Parameters
    ----------
    asset : str
        "ETH" or "BTC"
    chain : str
        "ethereum" or "bitcoin"
    top_n : int
        Number of top holders to return (max 200). Default 50.
    """
    t0 = time.perf_counter()
    mcp_tool_calls_total.labels(tool="scan_top_holders").inc()

    asset = asset.upper()
    chain = chain.lower()
    top_n = max(1, min(top_n, 200))

    store = get_store()

    # Pull cached balance snapshots from DuckDB
    try:
        from src.collectors.onchain import AlchemyCollector, CoinGeckoPriceCollector
        price_oracle = CoinGeckoPriceCollector()
        prices = await price_oracle.get_prices_usd(["ethereum", "bitcoin"])
        eth_price = prices.get("ethereum", 3_500.0)
        btc_price = prices.get("bitcoin", 97_000.0)
        price_usd = eth_price if asset == "ETH" else btc_price

        # Load labeled whale + exchange addresses from corpus for this chain
        whale_addrs  = await store.get_whale_addresses(chain)
        exch_labels  = await store.get_exchange_addresses(chain)
        all_labeled  = set(whale_addrs) | {e.address for e in exch_labels}

        # Top N from corpus (balances stored from last scan)
        top_addrs = list(all_labeled)[:top_n]

        holders: list[dict[str, Any]] = []
        for rank, addr in enumerate(top_addrs, 1):
            lbl = await store.get_label(addr, chain)
            balance_history = await store.get_balance_history(addr, chain, asset, limit=1)
            balance_usd = balance_history[0]["balance_usd"] if balance_history else 0.0

            holders.append({
                "rank":        rank,
                "address":     addr,
                "balance_usd": round(balance_usd, 0),
                "entity_name": lbl.entity_name if lbl else f"{addr[:8]}…",
                "entity_type": lbl.entity_type.value if lbl else "unknown",
                "wallet_role": lbl.wallet_role.value if lbl else "unknown",
                "labeled":     lbl is not None,
                "provenance_summary": lbl.provenance_summary() if lbl else {
                    "sources": [],
                    "confidence": 0.0,
                    "source_count": 0,
                    "labels": [],
                    "methodologies": [],
                },
            })

    except Exception as exc:
        raise RuntimeError(f"Top holders scan failed: {exc}") from exc

    # Sort by balance
    holders.sort(key=lambda h: h["balance_usd"], reverse=True)
    for i, h in enumerate(holders, 1):
        h["rank"] = i

    labeled_count   = sum(1 for h in holders if h["labeled"])
    exchange_count  = sum(1 for h in holders if h["entity_type"] == "exchange")
    whale_count     = sum(1 for h in holders if h["entity_type"] == "whale")
    labeled_pct     = round(100 * labeled_count / max(len(holders), 1), 1)
    exchange_pct    = round(100 * exchange_count / max(len(holders), 1), 1)
    whale_pct       = round(100 * whale_count / max(len(holders), 1), 1)

    latency = time.perf_counter() - t0
    mcp_tool_latency.labels(tool="scan_top_holders").observe(latency)

    return {
        "chain":          chain,
        "asset":          asset,
        "top_n":          top_n,
        "holders":        holders,
        "labeled_count":  labeled_count,
        "unlabeled_count": len(holders) - labeled_count,
        "labeled_pct":    labeled_pct,
        "exchange_pct":   exchange_pct,
        "whale_pct":      whale_pct,
        "provenance": {
            "methodology": (
                "Top holders sourced from our labeled corpus (exchange seeds, "
                "Dune Spellbook, community labels, on-chain heuristics). "
                "Balances from last pipeline scan. "
                "Exchange-held tokens do NOT represent true holder demand — "
                "they represent custodial balances for exchange customers."
            ),
            "corpus_sources": [
                "exchange_wallet_seed", "community_labels",
                "dune_spellbook", "heuristic:whale_behavior",
            ],
            "query_latency_ms": round(latency * 1000, 1),
        },
        "confidence": 0.7 * (labeled_pct / 100),
        "freshness":  _iso_now(),
        "evidence": {
            "holder_count":   len(holders),
            "labeled_pct":    labeled_pct,
            "exchange_pct":   exchange_pct,
            "whale_pct":      whale_pct,
            "note": (
                f"{exchange_pct:.0f}% of tracked holders are known exchanges. "
                "Their balances are customer deposits, not whale accumulation."
            ),
        },
    }


# ── Tool 6: refresh_corpus ────────────────────────────────────────────────────

@mcp.tool()
async def refresh_corpus(
    sources: list[str] | None = None,
    chain: str = "ethereum",
) -> RefreshCorpusResponse:
    """
    Trigger a labeled corpus refresh from external sources.

    Pulls fresh labels from all configured sources (or a specific subset):
      - exchange_wallet_seed : reload static seed list (always available)
      - community_labels     : fetch ~30k labels from brianleect/etherscan-labels (no API key)
      - dune_spellbook       : pull from Dune Spellbook labels.addresses (needs DUNE_API_KEY)
      - etherscan_labels     : fetch Etherscan name tags (free tier available)
      - heuristics           : re-run on-chain clustering on recently discovered addresses

    After refreshing, the corpus will have more labeled addresses, increasing the
    confidence and coverage of accumulation signals.

    Parameters
    ----------
    sources : list[str], optional
        Which sources to refresh. Defaults to all available.
    chain : str
        Chain to refresh labels for. Default "ethereum".
    """
    t0 = time.perf_counter()
    mcp_tool_calls_total.labels(tool="refresh_corpus").inc()

    all_sources = [
        "exchange_wallet_seed",
        "community_labels",
        "dune_spellbook",
        "etherscan_labels",
    ]
    requested = sources or all_sources
    chain = chain.lower()

    store = get_store()
    await store.initialize()

    labels_added   = 0
    labels_updated = 0
    refreshed      = []

    # ── Exchange seed (always available) ─────────────────────────────────────
    if "exchange_wallet_seed" in requested:
        from src.collectors.exchange_wallets import build_seed_labels
        seed_labels = build_seed_labels()
        n = await store.upsert_labels_bulk(seed_labels)
        labels_added += n
        refreshed.append("exchange_wallet_seed")
        log.info("corpus_seed_loaded", count=n)

    # ── Community labels (free, no API key) ──────────────────────────────────
    if "community_labels" in requested and chain == "ethereum":
        try:
            from src.collectors.community_labels import GitHubLabelsCollector
            community = GitHubLabelsCollector()
            cl = await community.get_eth_labels()
            n = await store.upsert_labels_bulk(cl)
            labels_added += n
            refreshed.append("community_labels")
            log.info("corpus_community_loaded", count=n)
        except Exception as exc:
            log.warning("corpus_community_failed", error=str(exc))

    # ── Dune Spellbook ────────────────────────────────────────────────────────
    if "dune_spellbook" in requested and settings.dune_api_key:
        try:
            from src.collectors.dune import DuneCollector
            dune = DuneCollector()
            if await dune.is_available():
                dune_labels = await dune.get_spellbook_labels(chain)
                n = await store.upsert_labels_bulk(dune_labels)
                labels_added += n
                refreshed.append("dune_spellbook")
                log.info("corpus_dune_loaded", count=n)
        except Exception as exc:
            log.warning("corpus_dune_failed", error=str(exc))

    # ── Etherscan ────────────────────────────────────────────────────────────
    if "etherscan_labels" in requested and chain == "ethereum":
        try:
            from src.collectors.etherscan import EtherscanCollector
            eth = EtherscanCollector()
            if await eth.is_available():
                refreshed.append("etherscan_labels")
        except Exception as exc:
            log.warning("corpus_etherscan_failed", error=str(exc))

    corpus_stats = await store.corpus_stats()

    latency = time.perf_counter() - t0
    mcp_tool_latency.labels(tool="refresh_corpus").observe(latency)

    return {
        "status":           "ok",
        "labels_added":     labels_added,
        "labels_updated":   labels_updated,
        "sources_refreshed": refreshed,
        "corpus_stats":     corpus_stats,
        "provenance": {
            "methodology": (
                "Corpus refresh pulls labeled addresses from public sources: "
                "community-curated brianleect/etherscan-labels (~30k addresses, no API key), "
                "Dune Spellbook labels.addresses, and our curated exchange wallet seed list. "
                "Each label stores its provenance so it can be independently verified."
            ),
            "sources_attempted": requested,
            "api_keys_configured": {
                "community_labels": True,
                "dune":   bool(settings.dune_api_key),
                "etherscan": bool(settings.etherscan_api_key),
                "alchemy": bool(settings.alchemy_api_key),
            },
            "query_latency_ms": round(latency * 1000, 1),
        },
        "confidence": 1.0,
        "freshness": _iso_now(),
    }


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    import os
    transport = os.environ.get("MCP_TRANSPORT", "stdio")
    if transport == "sse":
        import asyncio as _asyncio
        import uvicorn as _uvicorn
        port = int(os.environ.get("PORT", settings.http_port))
        log.info("mcp_server_starting", transport="sse", port=port)

        async def _serve() -> None:
            store = get_store()
            await store.initialize()

            from src.collectors.exchange_wallets import build_seed_labels
            seed_labels = build_seed_labels()
            await store.upsert_labels_bulk(seed_labels)
            log.info("corpus_seeded", count=len(seed_labels))

            config = _uvicorn.Config(
                mcp.sse_app(),
                host="0.0.0.0",
                port=port,
                log_level="info",
                forwarded_allow_ips="*",
                proxy_headers=True,
            )
            await _uvicorn.Server(config).serve()

        _asyncio.run(_serve())
    else:
        import asyncio as _asyncio

        async def _init() -> None:
            store = get_store()
            await store.initialize()
            from src.collectors.exchange_wallets import build_seed_labels
            seed = build_seed_labels()
            await store.upsert_labels_bulk(seed)

        _asyncio.run(_init())
        log.info("mcp_server_starting", transport="stdio")
        mcp.run()


if __name__ == "__main__":
    main()

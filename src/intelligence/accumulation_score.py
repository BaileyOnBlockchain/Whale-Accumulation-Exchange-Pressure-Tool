"""Composite on-chain accumulation signal.

Combines whale flow and exchange pressure into a single score (-100 to +100).

Score interpretation
--------------------
 +100  Strong Accumulation : whales aggressively buying, exchange reserves draining
 +30   Moderate Accumulation: net accumulation trend, moderate outflows
   0   Neutral             : balanced inflows/outflows, no clear trend
 -30   Moderate Distribution: more sending to exchanges than receiving
 -100  Strong Distribution : whales offloading, exchange reserves swelling

Component weights (sum = 100)
------------------------------
- whale_flow    (40) : net USD change across tracked whale wallets
- exchange_flow (35) : net exchange reserve change (outflow = positive)
- new_accum     (15) : new addresses accumulating (demand expansion)
- ratio         (10) : whale-to-exchange vs exchange-to-whale ratio
"""
from __future__ import annotations

import math
import time
from typing import Any

from src.config import (
    ACCUMULATION_LABELS,
    SCORE_EXCHANGE_FLOW_MAX,
    SCORE_NEW_ACCUM_MAX,
    SCORE_RATIO_MAX,
    SCORE_WHALE_FLOW_MAX,
    key_accumulation_signal,
    settings,
)
from src.core.models import (
    AccumulationSignal, AccumulationRating, Chain,
    ExchangePressureSummary, WhaleFlowSummary,
)
from src.utils.logging import get_logger

log = get_logger(__name__)


def accumulation_label(score: float) -> str:
    for threshold, label in ACCUMULATION_LABELS:
        if score >= threshold:
            return label
    return "Strong Distribution"


def compute_accumulation_score(
    whale_flow: WhaleFlowSummary,
    exchange_pressure: ExchangePressureSummary,
) -> AccumulationSignal:
    """
    Compute the composite accumulation signal from whale flow and exchange pressure.

    Returns an AccumulationSignal with:
    - score       : -100 to +100
    - rating      : human-readable label
    - components  : breakdown by factor
    - evidence    : raw numbers for transparency
    - provenance  : methodology and data sources
    - confidence  : 0–1 based on corpus coverage and data freshness
    """

    # ── Component 1: Whale Net Flow (weight 40) ───────────────────────────────
    # Positive when whales are accumulating (net_flow_usd > 0)
    # Scale: $1B net accumulation ≈ full score
    scale_1b = 1_000_000_000.0
    whale_component = (
        whale_flow.net_flow_usd / scale_1b
    ) * SCORE_WHALE_FLOW_MAX
    whale_component = max(-SCORE_WHALE_FLOW_MAX, min(SCORE_WHALE_FLOW_MAX, whale_component))

    # ── Component 2: Exchange Reserve Change (weight 35) ─────────────────────
    # Negative net_flow_usd in exchange_pressure = outflow = bullish
    # We flip sign: exchange outflow → positive score
    scale_500m = 500_000_000.0
    exch_net = -exchange_pressure.net_flow_usd   # flip: outflow is positive
    exch_component = (exch_net / scale_500m) * SCORE_EXCHANGE_FLOW_MAX
    exch_component = max(-SCORE_EXCHANGE_FLOW_MAX, min(SCORE_EXCHANGE_FLOW_MAX, exch_component))

    # ── Component 3: New Accumulation Ratio (weight 15) ──────────────────────
    # How many tracked addresses are accumulating vs total tracked
    tracked = max(whale_flow.tracked_count, 1)
    accum_ratio = whale_flow.accumulating_count / tracked
    distrib_ratio = whale_flow.distributing_count / tracked
    net_ratio = accum_ratio - distrib_ratio   # -1 to +1
    new_accum_component = net_ratio * SCORE_NEW_ACCUM_MAX

    # ── Component 4: Wallet/Exchange Ratio (weight 10) ────────────────────────
    # Ratio of total whale accumulation vs total exchange inflow
    exchange_inflow  = max(exchange_pressure.total_inflow_usd, 1.0)
    whale_accum      = whale_flow.total_accumulating_usd
    ratio = math.log(whale_accum / exchange_inflow + 1) if whale_accum > 0 else 0
    ratio_component = min(SCORE_RATIO_MAX, ratio * 3)
    if exchange_pressure.total_inflow_usd > whale_flow.total_accumulating_usd:
        ratio_component = -ratio_component   # more inflows than whale buys → negative

    # ── Composite ─────────────────────────────────────────────────────────────
    raw_score = (
        whale_component
        + exch_component
        + new_accum_component
        + ratio_component
    )
    # Clamp to [-100, +100]
    score = max(-100.0, min(100.0, raw_score))

    components = {
        "whale_net_flow":        round(whale_component, 2),
        "exchange_net_outflow":  round(exch_component, 2),
        "new_accumulation_rate": round(new_accum_component, 2),
        "whale_exchange_ratio":  round(ratio_component, 2),
    }

    # ── Confidence ────────────────────────────────────────────────────────────
    # Combine confidence from both inputs; reduce if corpus is sparse
    raw_confidence = (
        whale_flow.confidence * 0.55
        + exchange_pressure.confidence * 0.45
    )
    # Penalise if almost no addresses are labeled
    coverage = whale_flow.labeled_count / max(whale_flow.tracked_count, 1)
    confidence = raw_confidence * (0.5 + 0.5 * coverage)

    # ── Evidence ──────────────────────────────────────────────────────────────
    evidence: dict[str, Any] = {
        "whale_net_flow_usd":           round(whale_flow.net_flow_usd, 0),
        "whale_accumulating_count":     whale_flow.accumulating_count,
        "whale_distributing_count":     whale_flow.distributing_count,
        "whale_tracked_count":          whale_flow.tracked_count,
        "whale_labeled_count":          whale_flow.labeled_count,
        "exchange_inflow_usd":          round(exchange_pressure.total_inflow_usd, 0),
        "exchange_outflow_usd":         round(exchange_pressure.total_outflow_usd, 0),
        "exchange_net_flow_usd":        round(exchange_pressure.net_flow_usd, 0),
        "exchange_reserve_change_usd":  round(exchange_pressure.reserve_change_usd, 0),
        "exchange_reserve_change_pct":  round(exchange_pressure.reserve_change_pct, 2),
        "exchanges_tracked":            len(exchange_pressure.exchange_breakdown),
    }

    provenance: dict[str, Any] = {
        "methodology": (
            "Composite signal combining 4 on-chain components: "
            "(1) whale net balance flow (+/-$1B scale → 40 pts), "
            "(2) exchange reserve net change (outflow = bullish, +/-$500M scale → 35 pts), "
            "(3) accumulating-vs-distributing wallet ratio (→ 15 pts), "
            "(4) whale accumulation vs exchange inflow ratio (→ 10 pts). "
            "All input data is derived from labeled addresses in our own corpus — "
            "community-curated labels, Dune Spellbook, exchange seed wallets, on-chain heuristics."
        ),
        "corpus_sources": [
            "exchange_wallet_seed",
            "community_curated",
            "dune_spellbook",
            "etherscan_labels",
            "heuristic:deposit_clustering",
            "heuristic:whale_behavior",
        ],
        "score_range": "[-100, +100]: positive = accumulation, negative = distribution",
        "component_weights": {
            "whale_net_flow":        SCORE_WHALE_FLOW_MAX,
            "exchange_net_outflow":  SCORE_EXCHANGE_FLOW_MAX,
            "new_accumulation_rate": SCORE_NEW_ACCUM_MAX,
            "whale_exchange_ratio":  SCORE_RATIO_MAX,
        },
    }

    return AccumulationSignal(
        chain=whale_flow.chain,
        asset=whale_flow.asset,
        score=round(score, 2),
        rating=accumulation_label(score),
        components=components,
        evidence=evidence,
        whale_flow=whale_flow,
        exchange_pressure=exchange_pressure,
        provenance=provenance,
        confidence=round(min(1.0, confidence), 3),
        freshness=_iso_now(),
    )


def _iso_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()

"""On-chain clustering heuristics — our proprietary label expansion engine.

These heuristics let us grow the labeled corpus beyond what any single public
source provides, while keeping every new label fully traceable.

Heuristics implemented
----------------------
1. deposit_clustering   : Identify exchange deposit addresses by output pattern.
2. hot_cold_detection   : Classify wallets as hot/cold by velocity and balance ratio.
3. whale_behavior       : Score wallets by accumulation behaviour (balance, velocity, DeFi).
4. co_spend_clustering  : (Bitcoin) Group addresses that appear in same tx inputs.
5. consolidation_sweep  : Detect exchange consolidation wallets (many small → one large).
"""
from __future__ import annotations

import math
import time
from typing import Any

from src.core.models import EntityLabel, EntityType, WalletRole, Chain
from src.corpus.provenance import (
    from_heuristic_deposit_clustering,
    from_heuristic_hot_cold,
    from_heuristic_whale_behavior,
    aggregate_confidence,
)
from src.utils.logging import get_logger

log = get_logger(__name__)


# ── Deposit Address Clustering ─────────────────────────────────────────────────

def classify_deposit_address(
    address: str,
    chain: Chain,
    outbound_transfers: list[dict[str, Any]],
    known_exchange_wallets: set[str],
) -> EntityLabel | None:
    """
    An address is a deposit address if ≥80% of its outbound value flows
    exclusively to known exchange wallets. Exchange deposit addresses are
    identifiable because users send to them and the exchange sweeps forward.

    Parameters
    ----------
    outbound_transfers : list of {to_address, amount_usd, timestamp_ms}
    known_exchange_wallets : set of known exchange hot/cold wallet addresses
    """
    if not outbound_transfers:
        return None

    exchange_bound_usd = sum(
        t["amount_usd"] for t in outbound_transfers
        if t["to_address"].lower() in known_exchange_wallets
    )
    total_out_usd = sum(t["amount_usd"] for t in outbound_transfers)

    if total_out_usd < 1000:
        return None

    ratio = exchange_bound_usd / total_out_usd
    if ratio < 0.80:
        return None

    # Determine which exchange owns this deposit address
    exchange_counts: dict[str, int] = {}
    for t in outbound_transfers:
        dest = t["to_address"].lower()
        if dest in known_exchange_wallets:
            exchange_counts[dest] = exchange_counts.get(dest, 0) + 1

    if not exchange_counts:
        return None

    # The exchange with the most deposits
    primary_dest = max(exchange_counts, key=lambda k: exchange_counts[k])

    src = from_heuristic_deposit_clustering(
        parent_exchange=primary_dest[:10],
        deposit_count=len([t for t in outbound_transfers
                           if t["to_address"].lower() in known_exchange_wallets]),
        total_deposited_usd=exchange_bound_usd,
        last_deposit_ms=max(t["timestamp_ms"] for t in outbound_transfers),
    )

    return EntityLabel(
        address=address.lower(),
        chain=chain,
        entity_name=f"Exchange Deposit ({primary_dest[:8]}…)",
        entity_type=EntityType.exchange,
        wallet_role=WalletRole.deposit,
        sources=[src],
        confidence=src.confidence,
        first_seen_ms=min(t["timestamp_ms"] for t in outbound_transfers),
        last_updated_ms=int(time.time() * 1000),
    )


# ── Hot / Cold Wallet Detection ────────────────────────────────────────────────

def classify_hot_cold(
    address: str,
    chain: Chain,
    parent_exchange: str,
    balance_usd: float,
    tx_per_day: float,
    avg_tx_amount_usd: float,
    inbound_from_cold: bool,
    outbound_to_customers: bool,
) -> EntityLabel | None:
    """
    Hot wallet: high velocity, moderate balance, sends to customer addresses.
    Cold wallet: low velocity, very large balance, receives from hot wallets.

    Returns None if signals are insufficient to classify with confidence.
    """
    is_hot_signal = (
        tx_per_day > 50
        and outbound_to_customers
        and not inbound_from_cold
    )
    is_cold_signal = (
        balance_usd > 10_000_000
        and tx_per_day < 5
        and inbound_from_cold
        and not outbound_to_customers
    )

    if not is_hot_signal and not is_cold_signal:
        return None

    role = WalletRole.hot_wallet if is_hot_signal else WalletRole.cold_wallet
    confidence_base = 0.85 if is_hot_signal else 0.90

    # Higher confidence when both signals are strongly positive
    if is_hot_signal and tx_per_day > 200:
        confidence_base = min(0.97, confidence_base + 0.07)
    if is_cold_signal and balance_usd > 100_000_000:
        confidence_base = min(0.97, confidence_base + 0.05)

    evidence = {
        "tx_per_day":              round(tx_per_day, 1),
        "avg_tx_amount_usd":       round(avg_tx_amount_usd, 0),
        "balance_usd":             round(balance_usd, 0),
        "inbound_from_cold":       inbound_from_cold,
        "outbound_to_customers":   outbound_to_customers,
        "confidence":              confidence_base,
    }

    src = from_heuristic_hot_cold(role.value, evidence)

    return EntityLabel(
        address=address.lower(),
        chain=chain,
        entity_name=parent_exchange,
        entity_type=EntityType.exchange,
        wallet_role=role,
        sources=[src],
        confidence=confidence_base,
        last_updated_ms=int(time.time() * 1000),
    )


# ── Whale Behavior Scoring ─────────────────────────────────────────────────────

def classify_whale(
    address: str,
    chain: Chain,
    balance_usd: float,
    tx_per_day: float,
    has_dex_activity: bool,
    has_defi_positions: bool,
    entity_name_hint: str = "",
    whale_min_usd: float = 1_000_000.0,
) -> EntityLabel | None:
    """
    Score an address as a whale based on balance size and behavioral fingerprint.

    Institutional / cold whales:
      - Large balance (> $1M)
      - Low transaction velocity (< 5 tx/day)
      - No DEX activity (suggests OTC or custodial)

    Active whales:
      - Large balance
      - Moderate velocity
      - Some DeFi/DEX interaction (smart money, protocol whale)
    """
    if balance_usd < whale_min_usd:
        return None

    src = from_heuristic_whale_behavior(balance_usd, tx_per_day, has_dex_activity)

    # Subtype hinting
    if has_dex_activity or has_defi_positions:
        role = WalletRole.unknown    # active, not pure accumulation
        sub_name = "Active Whale"
    else:
        role = WalletRole.accumulation
        sub_name = "Accumulation Wallet"

    name = entity_name_hint or f"{sub_name} ({address[:8]}…)"

    return EntityLabel(
        address=address.lower(),
        chain=chain,
        entity_name=name,
        entity_type=EntityType.whale,
        wallet_role=role,
        sources=[src],
        confidence=src.confidence,
        last_updated_ms=int(time.time() * 1000),
    )


# ── Consolidation Sweep Detection ─────────────────────────────────────────────

def is_consolidation_wallet(
    inbound_tx_count: int,
    outbound_tx_count: int,
    avg_inbound_usd: float,
    avg_outbound_usd: float,
) -> tuple[bool, float]:
    """
    Consolidation wallets aggregate many small deposits into fewer large outputs.
    Pattern: many inbound (small) → few outbound (large). Ratio > 10x.

    Returns (is_consolidation, confidence).
    """
    if inbound_tx_count < 10 or outbound_tx_count == 0:
        return False, 0.0

    fan_in_ratio = inbound_tx_count / outbound_tx_count
    size_ratio = avg_outbound_usd / avg_inbound_usd if avg_inbound_usd > 0 else 0

    if fan_in_ratio < 5 or size_ratio < 5:
        return False, 0.0

    # Confidence scales with how extreme the ratio is
    confidence = min(0.95, 0.60 + math.log10(fan_in_ratio) * 0.12)
    return True, confidence


# ── Bitcoin Co-Spend Clustering ───────────────────────────────────────────────

def co_spend_groups(transactions: list[dict[str, Any]]) -> list[set[str]]:
    """
    Bitcoin common-input ownership heuristic: addresses that appear together
    as inputs to the same transaction are controlled by the same entity.

    Parameters
    ----------
    transactions : list of {tx_hash, inputs: [address, ...]}

    Returns
    -------
    List of address groups (sets) that are likely controlled by the same wallet.
    Uses union-find to merge overlapping groups.
    """
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        if x not in parent:
            parent[x] = x
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(a: str, b: str) -> None:
        parent[find(a)] = find(b)

    for tx in transactions:
        inputs = tx.get("inputs", [])
        if len(inputs) < 2:
            continue
        for addr in inputs[1:]:
            union(inputs[0], addr)

    groups: dict[str, set[str]] = {}
    for addr in parent:
        root = find(addr)
        groups.setdefault(root, set()).add(addr)

    return [g for g in groups.values() if len(g) >= 2]


# ── Batch Classification ──────────────────────────────────────────────────────

def run_whale_sweep(
    addresses: list[dict[str, Any]],
    chain: Chain,
    whale_min_usd: float = 1_000_000.0,
) -> list[EntityLabel]:
    """
    Classify a list of address stats as whales.
    Each address dict must have: address, balance_usd, tx_per_day,
                                  has_dex_activity, has_defi_positions.
    """
    results: list[EntityLabel] = []
    for addr_data in addresses:
        label = classify_whale(
            address=addr_data["address"],
            chain=chain,
            balance_usd=addr_data.get("balance_usd", 0),
            tx_per_day=addr_data.get("tx_per_day", 0),
            has_dex_activity=addr_data.get("has_dex_activity", False),
            has_defi_positions=addr_data.get("has_defi_positions", False),
            entity_name_hint=addr_data.get("entity_name", ""),
            whale_min_usd=whale_min_usd,
        )
        if label:
            results.append(label)

    log.info("whale_sweep_complete", count=len(results), chain=chain.value)
    return results

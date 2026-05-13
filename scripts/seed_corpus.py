"""
Seed the labeled corpus from all available public sources.

Run this once before starting the pipeline to populate the corpus with
exchange wallet labels and any API-backed labels.

Usage:
  python -m scripts.seed_corpus
  python -m scripts.seed_corpus --arkham --dune --etherscan
"""
from __future__ import annotations

import asyncio
import sys
import time

import typer

try:
    import uvloop
    uvloop.install()
except (ImportError, RuntimeError):
    pass

from src.config import settings
from src.corpus.store import get_store
from src.utils.logging import configure_logging, get_logger

configure_logging()
log = get_logger(__name__)

app = typer.Typer()


@app.command()
def main(
    arkham:     bool = typer.Option(False, help="Pull Arkham Intelligence entity labels"),
    dune:       bool = typer.Option(False, help="Pull Dune Spellbook labels"),
    etherscan:  bool = typer.Option(False, help="Pull Etherscan name tags"),
    chain:      str  = typer.Option("ethereum", help="Chain to seed (ethereum|bitcoin)"),
    verbose:    bool = typer.Option(False, help="Verbose output"),
) -> None:
    asyncio.run(_seed(
        use_arkham=arkham,
        use_dune=dune,
        use_etherscan=etherscan,
        chain=chain,
        verbose=verbose,
    ))


async def _seed(
    use_arkham: bool,
    use_dune: bool,
    use_etherscan: bool,
    chain: str,
    verbose: bool,
) -> None:
    t0 = time.perf_counter()
    store = get_store()
    await store.initialize()

    total = 0

    # ── Step 1: Exchange wallet seed (always, no API key needed) ─────────────
    print("▸ Loading exchange wallet seed corpus...")
    from src.collectors.exchange_wallets import build_seed_labels
    seed_labels = build_seed_labels()
    n = await store.upsert_labels_bulk(seed_labels)
    total += n
    print(f"  ✓ {n} exchange wallet labels loaded")

    # ── Step 2: Arkham Intelligence ───────────────────────────────────────────
    if use_arkham or settings.arkham_api_key:
        print("▸ Fetching Arkham Intelligence labels...")
        try:
            from src.collectors.arkham import ArkhamCollector
            from src.collectors.exchange_wallets import get_known_exchange_addresses
            arkham = ArkhamCollector()
            if await arkham.is_available():
                known = list(get_known_exchange_addresses(chain))
                labels = await arkham.batch_lookup(known, chain)
                n = await store.upsert_labels_bulk(labels)
                total += n
                print(f"  ✓ {n} Arkham labels loaded")
            else:
                print("  ⚠ Arkham API key invalid or rate-limited")
        except Exception as exc:
            print(f"  ✗ Arkham failed: {exc}")
    elif use_arkham:
        print("  ⚠ ARKHAM_API_KEY not set — skipping")

    # ── Step 3: Dune Spellbook ────────────────────────────────────────────────
    if use_dune or settings.dune_api_key:
        print("▸ Fetching Dune Spellbook labels...")
        try:
            from src.collectors.dune import DuneCollector
            dune = DuneCollector()
            if await dune.is_available():
                labels = await dune.get_spellbook_labels(chain)
                n = await store.upsert_labels_bulk(labels)
                total += n
                print(f"  ✓ {n} Dune Spellbook labels loaded")
            else:
                print("  ⚠ DUNE_API_KEY not set or query failed")
        except Exception as exc:
            print(f"  ✗ Dune failed: {exc}")
    elif use_dune:
        print("  ⚠ DUNE_API_KEY not set — skipping")

    # ── Step 4: Corpus stats ──────────────────────────────────────────────────
    stats = await store.corpus_stats()
    elapsed = time.perf_counter() - t0

    print(f"\n{'─'*50}")
    print(f"Corpus seeding complete in {elapsed:.1f}s")
    print(f"  Total labels:        {stats['total_labels']}")
    print(f"  Provenance records:  {stats['provenance_records']}")
    print(f"  By entity type:      {stats['by_entity_type']}")
    print(f"  By chain:            {stats['by_chain']}")
    print(f"  By source:           {stats['by_source']}")
    print(f"{'─'*50}")
    print()
    print("Next steps:")
    print("  1. Start the pipeline:   python -m src.pipeline")
    print("  2. Start the MCP server: python -m src.server.mcp_server")
    print("  3. Start the HTTP API:   python -m src.server.http_server")


if __name__ == "__main__":
    app()

# Whale Accumulation & Exchange-Pressure Intelligence

An MCP server that tracks on-chain whale behavior and exchange flows for ETH and BTC, producing a composite accumulation signal with fully transparent, auditable provenance on every address label.

**Signal range:** −100 (strong distribution) → +100 (strong accumulation)

---

## What it does

The pipeline continuously monitors two signals:

| Signal | What it measures | Bullish when |
|---|---|---|
| **Whale net flow** | Balance changes across labeled whale wallets | Whales are accumulating (net +) |
| **Exchange pressure** | CEX inflows vs outflows across hot + cold wallets | Coins leave exchanges (outflow) |

These combine into the composite score. Every address in the corpus cites its exact source — Arkham Intelligence, Dune Spellbook, Etherscan labels, or an on-chain heuristic — so you can verify any classification independently.

---

## MCP Tools

Connect this to Claude Desktop, Claude Code, or any MCP client.

| Tool | Description |
|---|---|
| `get_accumulation_signal` | Composite score (−100 to +100) + component breakdown |
| `get_whale_accumulation` | Per-wallet balance deltas with entity labels + provenance |
| `get_exchange_pressure` | Net CEX inflow/outflow, reserve changes, per-exchange breakdown |
| `get_entity_label` | Look up any address — returns all labels and their sources |
| `scan_top_holders` | Top N holders with entity classification and corpus coverage stats |
| `refresh_corpus` | Pull fresh labels from Arkham, Dune Spellbook, Etherscan |

All responses include `confidence`, `freshness`, `provenance`, and `evidence` fields.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Pipeline (background, async)                               │
│  ├─ WhaleTracker     — scans top ETH/BTC holder balances    │
│  ├─ ExchangeTracker  — monitors CEX hot/cold wallet flows   │
│  └─ CorpusRefresher  — pulls labels every CORPUS_REFRESH_S  │
└───────────────────┬─────────────────────────────────────────┘
                    │ writes to
          ┌─────────┴──────────┐
          │  Redis             │  ← cached signals (TTL 5 min)
          │  DuckDB            │  ← labeled corpus + balance history
          └─────────┬──────────┘
                    │ reads from
┌───────────────────┴─────────────────────────────────────────┐
│  MCP Server (stdio or SSE)                                  │
│  6 tools — get_accumulation_signal, get_whale_accumulation  │
│            get_exchange_pressure, get_entity_label          │
│            scan_top_holders, refresh_corpus                 │
└─────────────────────────────────────────────────────────────┘
```

**Label sources (corpus):**
- Exchange seed list — curated hot/cold wallets for Binance, Coinbase, Kraken, OKX, etc.
- [Arkham Intelligence](https://platform.arkhamintelligence.com) — entity labels via public API
- [Dune Spellbook](https://dune.com/spellbook) — `labels.addresses` table
- Etherscan name tags — free-tier address labels
- On-chain heuristics — deposit clustering, hot/cold wallet detection, whale behavior patterns

---

## Quickstart (local stdio)

**Prerequisites:** Python 3.11+, Redis running on `localhost:6379`

```bash
git clone https://github.com/YOUR_USERNAME/whale-accumulation-intelligence.git
cd whale-accumulation-intelligence

pip install -e .

cp .env.example .env
# Edit .env — add your API keys (all optional, free-tier fallbacks exist)

# Seed the labeled corpus
whale-seed

# Start the pipeline (background process)
whale-pipeline &

# Run the MCP server (stdio — for Claude Desktop / Claude Code)
whale-mcp
```

**Claude Desktop config** (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "whale-intelligence": {
      "command": "whale-mcp",
      "env": {
        "REDIS_URL": "redis://localhost:6379/0"
      }
    }
  }
}
```

---

## Deploy on Railway (SSE)

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/new/template)

The `Procfile` runs both the pipeline and MCP server in one dyno over SSE:

```
web: MCP_TRANSPORT=sse python -m src.main
```

Set your environment variables in the Railway dashboard. Redis is available as a Railway plugin.

**Claude Code remote MCP config:**

```json
{
  "mcpServers": {
    "whale-intelligence": {
      "url": "https://YOUR_APP.railway.app/sse"
    }
  }
}
```

---

## Docker Compose

Runs pipeline + HTTP API + Redis as separate services:

```bash
cp .env.example .env
# fill in API keys

docker compose -f docker/docker-compose.yml up -d
```

Services:
- `redis` — persistent cache and signal store (port 6380)
- `pipeline` — continuous whale + exchange scanner (Prometheus metrics on 9091)
- `api` — HTTP REST API (port 8001, `/health` endpoint)

The MCP server runs as a stdio subprocess from your MCP client — not as a persistent Docker service. See comments in `docker/docker-compose.yml`.

---

## API Keys

All keys are optional. The pipeline degrades gracefully and uses free-tier fallbacks.

| Key | Source | Free tier | Used for |
|---|---|---|---|
| `ARKHAM_API_KEY` | [platform.arkhamintelligence.com](https://platform.arkhamintelligence.com) | 10 req/min | Entity labels |
| `DUNE_API_KEY` | [dune.com/settings/api](https://dune.com/settings/api) | Available | Spellbook address labels |
| `ETHERSCAN_API_KEY` | [etherscan.io/apis](https://etherscan.io/apis) | 5 req/sec | On-chain balance data |
| `ALCHEMY_API_KEY` | [alchemy.com](https://www.alchemy.com) | 300M CU/mo | Ethereum RPC |
| `BLOCKCHAIR_API_KEY` | [blockchair.com/api](https://blockchair.com/api) | 30 req/min | Bitcoin on-chain data |
| `COINGECKO_API_KEY` | [coingecko.com/api](https://www.coingecko.com/en/api) | 30 req/min | USD price oracle |
| `HELIUS_API_KEY` | [helius.dev](https://www.helius.dev) | 100k credits/mo | Solana on-chain data |

---

## Configuration

All settings are environment variables (see `.env.example`):

| Variable | Default | Description |
|---|---|---|
| `WHALE_MIN_USD` | `1000000` | Minimum USD balance to qualify as a whale |
| `WHALE_TOP_N` | `200` | Number of top holders tracked per asset |
| `BALANCE_SCAN_INTERVAL_S` | `300` | How often to re-scan whale balances (seconds) |
| `CORPUS_REFRESH_INTERVAL_S` | `3600` | How often to pull fresh labels from sources |
| `FLOW_LOOKBACK_MINUTES` | `1440` | Exchange pressure lookback window (24 h default) |
| `TRACKED_ASSETS` | `ETH,BTC,USDT,USDC` | Assets to track |
| `HTTP_PORT` | `8001` | HTTP API port |
| `METRICS_PORT` | `9091` | Prometheus metrics port |

---

## Score Components

The composite signal (−100 to +100) is the weighted sum of four factors:

| Component | Weight | Bullish condition |
|---|---|---|
| Whale net flow | 40 pts | Net positive balance change across tracked whales |
| Exchange net outflow | 35 pts | More coins leaving exchanges than entering |
| New accumulation rate | 15 pts | High fraction of tracked wallets accumulating |
| Whale/exchange ratio | 10 pts | Whale buys outpace exchange inflows |

Rating thresholds: **Strong Accumulation** ≥70 · **Moderate Accumulation** ≥30 · **Neutral** ≥−30 · **Moderate Distribution** ≥−70 · **Strong Distribution** <−70

---

## Stack

- **MCP framework** — [FastMCP](https://github.com/jlowin/fastmcp) (stdio + SSE transports)
- **Pipeline** — asyncio + [uvloop](https://github.com/MagicStack/uvloop)
- **Cache / signals** — Redis (async)
- **Corpus store** — DuckDB (embedded, file-backed)
- **HTTP API** — FastAPI + Uvicorn
- **Config** — Pydantic Settings
- **Logging** — structlog (JSON)
- **Metrics** — Prometheus client
- **HTTP client** — httpx (async, with retry logic)

---

## License

MIT

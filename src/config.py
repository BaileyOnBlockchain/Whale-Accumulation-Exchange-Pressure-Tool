from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_max_connections: int = 50

    # Corpus
    corpus_db_path: str = "./data/corpus.duckdb"

    # Tracking scope
    tracked_chains: str = "ethereum,bitcoin"
    tracked_assets: str = "ETH,BTC,USDT,USDC"

    # Whale classification
    whale_min_usd: float = 1_000_000.0
    whale_top_n: int = 200

    # Pipeline intervals (seconds)
    balance_scan_interval_s: int = 300
    corpus_refresh_interval_s: int = 3600
    signal_recompute_interval_s: int = 60

    # Exchange pressure
    flow_lookback_minutes: int = 1440

    # HTTP
    http_host: str = "0.0.0.0"
    http_port: int = 8001

    # Logging
    log_level: str = "INFO"

    # Metrics
    metrics_port: int = 9091

    # External API keys (all optional)
    arkham_api_key: str = ""
    dune_api_key: str = ""
    etherscan_api_key: str = ""
    alchemy_api_key: str = ""
    blockchair_api_key: str = ""
    helius_api_key: str = ""
    coingecko_api_key: str = ""

    @property
    def chains(self) -> list[str]:
        return [c.strip().lower() for c in self.tracked_chains.split(",")]

    @property
    def assets(self) -> list[str]:
        return [a.strip().upper() for a in self.tracked_assets.split(",")]


settings = Settings()

# ── Chain constants ────────────────────────────────────────────────────────────

CHAIN_NATIVE_ASSET: dict[str, str] = {
    "ethereum": "ETH",
    "bitcoin":  "BTC",
    "solana":   "SOL",
    "arbitrum": "ETH",
    "optimism": "ETH",
    "base":     "ETH",
    "polygon":  "MATIC",
}

# ── Accumulation score component weights (must sum to 100) ────────────────────

SCORE_WHALE_FLOW_MAX    = 40.0
SCORE_EXCHANGE_FLOW_MAX = 35.0
SCORE_NEW_ACCUM_MAX     = 15.0
SCORE_RATIO_MAX         = 10.0

ACCUMULATION_LABELS = [
    ( 70, "Strong Accumulation"),
    ( 30, "Moderate Accumulation"),
    (-30, "Neutral"),
    (-70, "Moderate Distribution"),
    (-101, "Strong Distribution"),
]

# ── Redis key templates ────────────────────────────────────────────────────────

def key_whale_balance(chain: str, address: str) -> str:
    return f"whale:balance:{chain}:{address.lower()}"

def key_exchange_flow(chain: str, exchange: str, direction: str) -> str:
    return f"flow:exchange:{chain}:{exchange}:{direction}"

def key_exchange_reserve(chain: str, exchange: str) -> str:
    return f"exchange:reserve:{chain}:{exchange}"

def key_accumulation_signal(chain: str, asset: str) -> str:
    return f"signal:{chain}:{asset.upper()}"

def key_corpus_meta(source: str) -> str:
    return f"corpus:meta:{source}"

def key_pipeline_state(chain: str) -> str:
    return f"pipeline:{chain}:state"

# ── Provenance source names ────────────────────────────────────────────────────

SOURCE_EXCHANGE_SEED    = "exchange_wallet_seed"
SOURCE_ARKHAM           = "arkham_intelligence"
SOURCE_DUNE_SPELLBOOK   = "dune_spellbook"
SOURCE_ETHERSCAN_LABELS = "etherscan_labels"
SOURCE_HEURISTIC_DEPOSIT   = "heuristic:deposit_clustering"
SOURCE_HEURISTIC_HOT_COLD  = "heuristic:hot_cold_detection"
SOURCE_HEURISTIC_WHALE     = "heuristic:whale_behavior"
SOURCE_COMMUNITY_LABELS    = "community_curated"

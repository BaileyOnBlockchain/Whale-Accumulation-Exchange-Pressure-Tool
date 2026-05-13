from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from src.config import settings

# ── MCP server ────────────────────────────────────────────────────────────────
mcp_tool_calls_total = Counter(
    "whale_mcp_tool_calls_total",
    "Total MCP tool calls",
    ["tool"],
)
mcp_tool_latency = Histogram(
    "whale_mcp_tool_latency_seconds",
    "MCP tool call latency",
    ["tool"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

# ── Whale tracker ─────────────────────────────────────────────────────────────
whale_scans_total = Counter(
    "whale_scans_total",
    "Total whale balance scans",
    ["chain"],
)
whale_tracked_gauge = Gauge(
    "whale_addresses_tracked",
    "Number of whale addresses currently tracked",
    ["chain"],
)

# ── Exchange pressure ─────────────────────────────────────────────────────────
exchange_flow_events_total = Counter(
    "whale_exchange_flow_events_total",
    "Total exchange flow events detected",
    ["chain", "exchange", "direction"],
)
exchange_reserve_gauge = Gauge(
    "whale_exchange_reserve_usd",
    "Exchange reserve balance in USD",
    ["chain", "exchange"],
)

# ── Corpus ────────────────────────────────────────────────────────────────────
corpus_labels_gauge = Gauge(
    "whale_corpus_labels_total",
    "Total labeled addresses in corpus",
    ["chain", "entity_type"],
)
corpus_refresh_total = Counter(
    "whale_corpus_refresh_total",
    "Total corpus refresh cycles",
    ["source"],
)


def start_metrics_server() -> None:
    try:
        start_http_server(settings.metrics_port)
    except OSError:
        pass   # port already in use (e.g. test environment)

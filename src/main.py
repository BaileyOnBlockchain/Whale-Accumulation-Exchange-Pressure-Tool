"""Entry point that starts pipeline + MCP server in the same process.

Useful for Railway single-service deployments. For production, run the
pipeline and MCP server as separate services (see docker-compose.yml).
"""
from __future__ import annotations

import asyncio
import os

try:
    import uvloop
    uvloop.install()
except (ImportError, RuntimeError):
    pass

from src.config import settings
from src.utils.logging import configure_logging, get_logger

configure_logging()
log = get_logger(__name__)


async def _main() -> None:
    from src.corpus.store import get_store
    from src.collectors.exchange_wallets import build_seed_labels
    from src.storage.redis_client import ping
    from src.utils.metrics import start_metrics_server

    if not await ping():
        log.warning("redis_not_reachable", url=settings.redis_url,
                    note="Starting without Redis — signals will not be cached")

    store = get_store()
    await store.initialize()
    seed = build_seed_labels()
    await store.upsert_labels_bulk(seed)
    log.info("corpus_seeded", count=len(seed))

    start_metrics_server()

    transport = os.environ.get("MCP_TRANSPORT", "stdio")
    if transport == "sse":
        from src.server.mcp_server import mcp
        import uvicorn
        port = int(os.environ.get("PORT", settings.http_port))
        config = uvicorn.Config(
            mcp.sse_app(),
            host="0.0.0.0",
            port=port,
            log_level="info",
            forwarded_allow_ips="*",
            proxy_headers=True,
        )
        await uvicorn.Server(config).serve()
    else:
        from src.server.mcp_server import mcp
        mcp.run()


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()

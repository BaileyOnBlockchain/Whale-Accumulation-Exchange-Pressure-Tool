"""Abstract base class for HTTP-based data collectors."""
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any

import httpx

from src.utils.logging import get_logger

log = get_logger(__name__)

_BACKOFF = [1, 2, 4, 8, 16, 32, 60]
_DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=10.0)


class BaseHTTPCollector(ABC):
    """Rate-limited, auto-retrying async HTTP collector."""

    source_name: str = ""

    def __init__(self, api_key: str = "", rate_limit_rps: float = 5.0) -> None:
        self._api_key = api_key
        self._rate_limit_rps = rate_limit_rps
        self._semaphore = asyncio.Semaphore(max(1, int(rate_limit_rps)))
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=_DEFAULT_TIMEOUT,
                headers=self._default_headers(),
                follow_redirects=True,
            )
        return self._client

    def _default_headers(self) -> dict[str, str]:
        return {"Accept": "application/json", "User-Agent": "whale-accumulation-intel/1.0"}

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def get(self, url: str, params: dict[str, Any] | None = None,
                  headers: dict[str, str] | None = None) -> dict[str, Any]:
        client = await self._get_client()
        backoff_idx = 0

        while True:
            async with self._semaphore:
                try:
                    resp = await client.get(url, params=params, headers=headers)
                    if resp.status_code == 429:
                        wait = int(resp.headers.get("Retry-After", _BACKOFF[backoff_idx]))
                        log.warning("rate_limited", source=self.source_name, wait_s=wait)
                        await asyncio.sleep(wait)
                        backoff_idx = min(backoff_idx + 1, len(_BACKOFF) - 1)
                        continue
                    resp.raise_for_status()
                    return resp.json()
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code >= 500:
                        wait = _BACKOFF[min(backoff_idx, len(_BACKOFF) - 1)]
                        backoff_idx += 1
                        log.warning("server_error", source=self.source_name,
                                    status=exc.response.status_code, retry_in=wait)
                        await asyncio.sleep(wait)
                        continue
                    raise
                except (httpx.ConnectError, httpx.TimeoutException) as exc:
                    wait = _BACKOFF[min(backoff_idx, len(_BACKOFF) - 1)]
                    backoff_idx += 1
                    log.warning("connection_error", source=self.source_name,
                                error=str(exc), retry_in=wait)
                    await asyncio.sleep(wait)
                    if backoff_idx >= len(_BACKOFF):
                        raise

    async def post(self, url: str, json: dict[str, Any] | None = None,
                   headers: dict[str, str] | None = None) -> dict[str, Any]:
        client = await self._get_client()
        resp = await client.post(url, json=json, headers=headers)
        resp.raise_for_status()
        return resp.json()

    @abstractmethod
    async def is_available(self) -> bool:
        """Return True if this collector can operate (API key valid, endpoint live)."""
        ...

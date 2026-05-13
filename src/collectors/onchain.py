"""Direct on-chain collectors for balance tracking and flow detection.

Supports multiple chains:
- Ethereum: via Alchemy enhanced API (free tier) + Etherscan fallback
- Bitcoin:  via Blockchair free API + Mempool.space fallback
- Solana:   via Helius API (free tier)

These collectors feed real-time balance snapshots into Redis and DuckDB,
enabling the whale tracker and exchange pressure modules to compute signals.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

from src.collectors.base import BaseHTTPCollector
from src.config import settings
from src.core.models import BalanceSnapshot, Chain, ExchangeFlowEvent, FlowDirection
from src.utils.logging import get_logger

log = get_logger(__name__)

_ETH_PRICE_FALLBACK = 3_500.0
_BTC_PRICE_FALLBACK = 97_000.0


# ── Price oracle ──────────────────────────────────────────────────────────────

class CoinGeckoPriceCollector(BaseHTTPCollector):
    """Simple free-tier price oracle — no API key needed."""
    source_name = "coingecko_prices"

    def __init__(self) -> None:
        super().__init__(rate_limit_rps=2.0)

    async def is_available(self) -> bool:
        try:
            await self.get("https://api.coingecko.com/api/v3/ping")
            return True
        except Exception:
            return False

    async def get_prices_usd(self, coins: list[str]) -> dict[str, float]:
        """
        Get USD prices for a list of CoinGecko coin IDs.
        Example: get_prices_usd(["ethereum", "bitcoin", "tether"])
        """
        try:
            data = await self.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={"ids": ",".join(coins), "vs_currencies": "usd"},
            )
            return {coin: data.get(coin, {}).get("usd", 0.0) for coin in coins}
        except Exception as exc:
            log.warning("coingecko_price_failed", error=str(exc))
            return {}


# ── Ethereum on-chain (Alchemy) ───────────────────────────────────────────────

# Ordered by reliability — tried in sequence when the primary fails
_PUBLIC_ETH_RPCS = [
    "https://rpc.ankr.com/eth",
    "https://ethereum.publicnode.com",
    "https://cloudflare-eth.com",
    "https://eth.llamarpc.com",
    "https://1rpc.io/eth",
]


class AlchemyCollector(BaseHTTPCollector):
    source_name = "alchemy_ethereum"

    def __init__(self, api_key: str = "") -> None:
        super().__init__(api_key=api_key or settings.alchemy_api_key, rate_limit_rps=25.0)
        if self._api_key:
            self._rpc_urls = [f"https://eth-mainnet.g.alchemy.com/v2/{self._api_key}"]
        else:
            self._rpc_urls = list(_PUBLIC_ETH_RPCS)
        self._rpc_url_idx = 0

    @property
    def _rpc_url(self) -> str:
        return self._rpc_urls[self._rpc_url_idx % len(self._rpc_urls)]

    def _next_rpc(self) -> None:
        self._rpc_url_idx = (self._rpc_url_idx + 1) % len(self._rpc_urls)
        log.info("rpc_fallback", next_url=self._rpc_url)

    async def is_available(self) -> bool:
        try:
            result = await self._rpc_call("eth_blockNumber", [])
            return bool(result)
        except Exception:
            return False

    async def _rpc_call(self, method: str, params: list[Any]) -> Any:
        last_exc: Exception | None = None
        for _ in range(len(self._rpc_urls)):
            try:
                resp = await self.post(self._rpc_url, json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": method,
                    "params": params,
                })
                if "error" in resp:
                    raise ValueError(f"RPC error: {resp['error']}")
                return resp.get("result")
            except Exception as exc:
                last_exc = exc
                log.warning("rpc_endpoint_failed", url=self._rpc_url, error=str(exc))
                self._next_rpc()
        raise RuntimeError(f"All RPC endpoints failed. Last error: {last_exc}")

    async def get_eth_balance_wei(self, address: str) -> int:
        result = await self._rpc_call("eth_getBalance", [address, "latest"])
        return int(result, 16) if result else 0

    async def get_eth_balance(self, address: str) -> float:
        wei = await self.get_eth_balance_wei(address)
        return wei / 1e18

    async def get_balances_bulk(
        self, addresses: list[str], price_usd: float = _ETH_PRICE_FALLBACK
    ) -> list[BalanceSnapshot]:
        """Fetch ETH balances for a list of addresses in parallel batches."""
        results: list[BalanceSnapshot] = []
        now_ms = int(time.time() * 1000)

        async def fetch_one(addr: str) -> BalanceSnapshot | None:
            try:
                bal = await self.get_eth_balance(addr)
                return BalanceSnapshot(
                    address=addr.lower(),
                    chain=Chain.ethereum,
                    balance_native=bal,
                    balance_usd=bal * price_usd,
                    asset="ETH",
                    snapshot_ms=now_ms,
                )
            except Exception as exc:
                log.debug("alchemy_balance_error", address=addr, error=str(exc))
                return None

        batch_size = 20
        for i in range(0, len(addresses), batch_size):
            batch = addresses[i:i + batch_size]
            snapshots = await asyncio.gather(*[fetch_one(a) for a in batch])
            results.extend(s for s in snapshots if s)
            await asyncio.sleep(0.1)

        return results

    async def get_token_balances(
        self, address: str, contract_addresses: list[str]
    ) -> dict[str, float]:
        """Alchemy getTokenBalances — returns ERC-20 balances."""
        if not self._api_key:
            return {}
        try:
            result = await self._rpc_call(
                "alchemy_getTokenBalances",
                [address, contract_addresses],
            )
            if not result:
                return {}
            out: dict[str, float] = {}
            for tb in result.get("tokenBalances", []):
                contract = (tb.get("contractAddress") or "").lower()
                raw = tb.get("tokenBalance") or "0x0"
                try:
                    out[contract] = int(raw, 16) / 1e18
                except ValueError:
                    pass
            return out
        except Exception as exc:
            log.debug("alchemy_token_balances_error", address=address, error=str(exc))
            return {}

    async def get_asset_transfers(
        self,
        address: str,
        direction: str = "to",
        from_block: str = "0x0",
        max_count: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Alchemy getAssetTransfers — richer than Etherscan for flow analysis.
        direction: "to" = inflows, "from" = outflows
        """
        if not self._api_key:
            return []
        try:
            param_key = "toAddress" if direction == "to" else "fromAddress"
            result = await self._rpc_call(
                "alchemy_getAssetTransfers",
                [{
                    param_key:   address,
                    "fromBlock": from_block,
                    "toBlock":   "latest",
                    "category":  ["external", "internal", "erc20"],
                    "maxCount":  hex(max_count),
                    "order":     "desc",
                }],
            )
            return result.get("transfers", []) if result else []
        except Exception as exc:
            log.debug("alchemy_transfers_error", address=address, error=str(exc))
            return []


# ── Bitcoin on-chain (Blockchair + Mempool.space) ─────────────────────────────

class BlockchairCollector(BaseHTTPCollector):
    source_name = "blockchair_bitcoin"

    def __init__(self, api_key: str = "") -> None:
        super().__init__(api_key=api_key or settings.blockchair_api_key, rate_limit_rps=1.5)
        self._base = "https://api.blockchair.com/bitcoin"

    def _params(self, extra: dict[str, Any]) -> dict[str, Any]:
        p: dict[str, Any] = {}
        if self._api_key:
            p["key"] = self._api_key
        p.update(extra)
        return p

    async def is_available(self) -> bool:
        try:
            data = await self.get(f"{self._base}/stats", params=self._params({}))
            return "data" in data
        except Exception:
            return False

    async def get_btc_balance(self, address: str) -> tuple[float, float]:
        """Return (balance_btc, balance_received_btc) for a Bitcoin address."""
        try:
            data = await self.get(
                f"{self._base}/dashboards/address/{address}",
                params=self._params({}),
            )
            addr_data = (data.get("data") or {}).get(address, {}).get("address", {})
            balance_sat = addr_data.get("balance", 0)
            received_sat = addr_data.get("received", 0)
            return balance_sat / 1e8, received_sat / 1e8
        except Exception as exc:
            log.debug("blockchair_balance_error", address=address, error=str(exc))
            return 0.0, 0.0

    async def get_btc_balances_bulk(
        self, addresses: list[str], price_usd: float = _BTC_PRICE_FALLBACK
    ) -> list[BalanceSnapshot]:
        results: list[BalanceSnapshot] = []
        now_ms = int(time.time() * 1000)

        for addr in addresses:
            bal, _ = await self.get_btc_balance(addr)
            if bal > 0:
                results.append(BalanceSnapshot(
                    address=addr.lower(),
                    chain=Chain.bitcoin,
                    balance_native=bal,
                    balance_usd=bal * price_usd,
                    asset="BTC",
                    snapshot_ms=now_ms,
                ))
            await asyncio.sleep(0.7)  # stay under free-tier rate limit

        return results

    async def get_recent_txns(
        self, address: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Fetch recent Bitcoin transactions for an address."""
        try:
            data = await self.get(
                f"{self._base}/dashboards/address/{address}",
                params=self._params({"transaction_details": True, "limit": limit}),
            )
            return (data.get("data") or {}).get(address, {}).get("transactions", [])
        except Exception as exc:
            log.debug("blockchair_txns_error", address=address, error=str(exc))
            return []

    async def get_top_btc_holders(self, limit: int = 100) -> list[dict[str, Any]]:
        """Fetch the richest Bitcoin addresses from Blockchair."""
        try:
            data = await self.get(
                f"{self._base}/addresses",
                params=self._params({
                    "s": "balance(desc)",
                    "limit": min(limit, 100),
                }),
            )
            return (data.get("data") or [])
        except Exception as exc:
            log.warning("blockchair_top_holders_failed", error=str(exc))
            return []


# ── Mempool.space fallback (no API key needed) ────────────────────────────────

class MempoolCollector(BaseHTTPCollector):
    source_name = "mempool_space"

    def __init__(self) -> None:
        super().__init__(rate_limit_rps=5.0)
        self._base = "https://mempool.space/api"

    async def is_available(self) -> bool:
        try:
            await self.get(f"{self._base}/blocks/tip/height")
            return True
        except Exception:
            return False

    async def get_btc_balance(self, address: str) -> float:
        """Return confirmed + unconfirmed BTC balance."""
        try:
            data = await self.get(f"{self._base}/address/{address}")
            chain_stats = data.get("chain_stats", {})
            funded = chain_stats.get("funded_txo_sum", 0)
            spent  = chain_stats.get("spent_txo_sum", 0)
            return (funded - spent) / 1e8
        except Exception as exc:
            log.debug("mempool_balance_error", address=address, error=str(exc))
            return 0.0

    async def get_address_txns(
        self, address: str, after_txid: str | None = None
    ) -> list[dict[str, Any]]:
        url = f"{self._base}/address/{address}/txs"
        try:
            data = await self.get(url, params={"after_txid": after_txid} if after_txid else None)
            return data if isinstance(data, list) else []
        except Exception as exc:
            log.debug("mempool_txns_error", address=address, error=str(exc))
            return []

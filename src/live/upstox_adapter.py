"""Upstox broker adapter stub.

Targets WebSocket V3 for market data streaming and REST V3 for
historical/options endpoints. This is a stub — real implementation
requires Upstox API credentials and the full SDK integration.

References:
- Market Data Feed V3: https://upstox.com/developer/api-documentation/v3/get-market-data-feed/
- Historical Candle V3: https://upstox.com/developer/api-documentation/v3/get-historical-candle-data/
- Option Chain: https://upstox.com/developer/api-documentation/get-pc-option-chain/
"""

from __future__ import annotations

import os
from typing import Callable

import httpx
import structlog

from src.models import BrokerAdapter, OptionChainEntry, OptionChainSnapshot

log = structlog.get_logger()

UPSTOX_WS_V3_URL = "wss://api.upstox.com/v3/feed/market-data-feed"
UPSTOX_REST_V3_BASE = "https://api.upstox.com/v3"
UPSTOX_REST_V2_BASE = "https://api.upstox.com/v2"


class UpstoxAdapter(BrokerAdapter):
    """Upstox broker adapter (stub implementation).

    Real integration requires:
    1. OAuth2 access token from Upstox developer portal
    2. WebSocket V3 subscription for live 5s bars
    3. REST V3 for historical candle data
    4. REST V2 for option chain data
    """

    def __init__(self):
        self.access_token = os.environ.get("UPSTOX_ACCESS_TOKEN", "")
        self.api_key = os.environ.get("UPSTOX_API_KEY", "")
        self._ws = None
        self._subscriptions: dict[str, Callable] = {}
        self._client = httpx.AsyncClient(
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Api-Key": self.api_key,
                "Accept": "application/json",
            },
            timeout=30.0,
        )

    async def get_ltp(self, symbol: str) -> float:
        """Get last traded price for a symbol."""
        resp = await self._client.get(
            f"{UPSTOX_REST_V2_BASE}/market-quote/ltp",
            params={"instrument_key": symbol},
        )
        resp.raise_for_status()
        data = resp.json()
        # Extract LTP from response (structure depends on Upstox API)
        quotes = data.get("data", {})
        for key, val in quotes.items():
            return float(val.get("last_price", 0))
        return 0.0

    async def get_option_chain(
        self, underlying: str, expiry: str
    ) -> OptionChainSnapshot:
        """Get option chain snapshot."""
        resp = await self._client.get(
            f"{UPSTOX_REST_V2_BASE}/option/chain",
            params={"instrument_key": underlying, "expiry_date": expiry},
        )
        resp.raise_for_status()
        data = resp.json()

        entries = []
        for item in data.get("data", []):
            for side in ["call_options", "put_options"]:
                opt = item.get(side, {})
                market = opt.get("market_data", {})
                if not market:
                    continue
                entries.append(
                    OptionChainEntry(
                        strike=float(item.get("strike_price", 0)),
                        option_type="CE" if side == "call_options" else "PE",
                        ltp=float(market.get("ltp", 0)),
                        bid=float(market.get("bid_price", 0)),
                        ask=float(market.get("ask_price", 0)),
                        volume=int(market.get("volume", 0)),
                        oi=int(market.get("oi", 0)),
                        iv=float(opt.get("option_greeks", {}).get("iv", 0)),
                    )
                )

        return OptionChainSnapshot(
            underlying=underlying,
            timestamp=0,  # would use actual timestamp from response
            expiry=expiry,
            entries=tuple(entries),
        )

    async def place_order(
        self,
        symbol: str,
        qty: int,
        side: str,
        order_type: str,
        price: float = 0.0,
    ) -> str:
        """Place an order. Returns order ID."""
        payload = {
            "instrument_token": symbol,
            "quantity": qty,
            "transaction_type": side,
            "order_type": order_type,
            "product": "I",  # Intraday
            "validity": "DAY",
        }
        if price > 0:
            payload["price"] = price

        resp = await self._client.post(
            f"{UPSTOX_REST_V2_BASE}/order/place", json=payload
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", {}).get("order_id", "")

    async def get_order_status(self, order_id: str) -> dict:
        """Get order status."""
        resp = await self._client.get(
            f"{UPSTOX_REST_V2_BASE}/order/details",
            params={"order_id": order_id},
        )
        resp.raise_for_status()
        return resp.json().get("data", {})

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        resp = await self._client.delete(
            f"{UPSTOX_REST_V2_BASE}/order/cancel",
            params={"order_id": order_id},
        )
        return resp.status_code == 200

    async def subscribe_market_data(self, symbols: list[str], callback) -> None:
        """Subscribe to live market data via WebSocket V3.

        Note: This is a stub. Real implementation would:
        1. Open a WebSocket connection to UPSTOX_WS_V3_URL
        2. Send subscription messages for the requested symbols
        3. Parse incoming binary protobuf messages
        4. Convert to bar dicts and invoke callback
        """
        log.info("upstox_subscribe_stub", symbols=symbols)
        for sym in symbols:
            self._subscriptions[sym] = callback

    async def unsubscribe_market_data(self, symbols: list[str]) -> None:
        """Unsubscribe from market data."""
        for sym in symbols:
            self._subscriptions.pop(sym, None)
        log.info("upstox_unsubscribe_stub", symbols=symbols)

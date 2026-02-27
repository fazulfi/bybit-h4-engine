from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import aiohttp

from app.config import load_settings


BASE_URL = "https://api.bybit.com"


class BybitREST:
    def __init__(self) -> None:
        self.settings = load_settings(require_keys=False)
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        return self._session

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        retries: int = 3,
    ) -> Dict[str, Any]:
        url = f"{BASE_URL}{path}"

        for attempt in range(1, retries + 1):
            try:
                session = await self._get_session()
                async with session.request(method, url, params=params) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        raise RuntimeError(f"HTTP {resp.status}: {text}")

                    data = await resp.json()

                    if data.get("retCode") != 0:
                        raise RuntimeError(f"Bybit error: {data}")

                    return data

            except Exception as e:
                if attempt == retries:
                    raise
                await asyncio.sleep(0.5 * attempt)  # simple backoff

        raise RuntimeError("Unreachable")

    # =====================================
    # PUBLIC ENDPOINTS
    # =====================================

    async def get_instruments_linear(self) -> List[Dict[str, Any]]:
        """
        Get all linear (USDT) perpetual instruments.
        """
        data = await self._request(
            "GET",
            "/v5/market/instruments-info",
            params={
                "category": self.settings.category,
            },
        )
        return data["result"]["list"]

    async def get_tickers_linear(self) -> List[Dict[str, Any]]:
        """
        Get all linear tickers (contains 24h turnover).
        """
        data = await self._request(
            "GET",
            "/v5/market/tickers",
            params={
                "category": self.settings.category,
            },
        )
        return data["result"]["list"]

    async def get_kline(
        self,
        symbol: str,
        interval: str,
        limit: int = 200,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get kline data.
        interval: "240" for H4
        start/end in ms (optional)
        """
        params: Dict[str, Any] = {
            "category": self.settings.category,
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }

        if start:
            params["start"] = start
        if end:
            params["end"] = end

        data = await self._request(
            "GET",
            "/v5/market/kline",
            params=params,
        )

        # Bybit returns list of lists
        # [ startTime, open, high, low, close, volume, turnover ]
        raw = data["result"]["list"]

        klines = []
        for row in raw:
            klines.append(
                {
                    "start": int(row[0]),  # ms
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                }
            )

        # Bybit returns newest first → reverse to oldest first
        klines.reverse()
        return klines

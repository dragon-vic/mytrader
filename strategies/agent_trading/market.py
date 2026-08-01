from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any
from typing import Protocol

import aiohttp

from strategies.agent_trading.lifecycle import MarketInstrument


BINANCE_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"


class MarketSnapshotter(Protocol):
    async def capture(
        self,
        instruments: tuple[MarketInstrument, ...],
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class SnapshotWindow:
    interval: str = "1m"
    minutes: int = 120


class RestMarketSnapshotter:
    def __init__(self, window: SnapshotWindow = SnapshotWindow()) -> None:
        if window.minutes <= 0 or window.minutes > 1000:
            raise ValueError("snapshot minutes must be between 1 and 1000")
        self.window = window
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> RestMarketSnapshotter:
        if self.session is not None:
            raise RuntimeError("RestMarketSnapshotter is already started")
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        return self

    async def __aexit__(self, *_args) -> None:
        if self.session is not None:
            await self.session.close()
            self.session = None

    # 披露完成后只拉取一次；不同交易场所并行请求，避免分析 Agent 等待实时行情。
    async def capture(
        self,
        instruments: tuple[MarketInstrument, ...],
    ) -> dict[str, Any]:
        if self.session is None:
            raise RuntimeError("RestMarketSnapshotter is not started")
        captured_ms = time.time_ns() // 1_000_000
        series = await asyncio.gather(
            *(self._capture_one(item, captured_ms) for item in instruments),
        )
        return {
            "captured_at_ms": captured_ms,
            "interval": self.window.interval,
            "lookback_minutes": self.window.minutes,
            "series": list(series),
        }

    async def _capture_one(
        self,
        instrument: MarketInstrument,
        captured_ms: int,
    ) -> dict[str, Any]:
        if instrument.venue == "BINANCE":
            candles = await self._binance(instrument.market_symbol)
        elif instrument.venue == "HYPERLIQUID":
            candles = await self._hyperliquid(instrument.market_symbol, captured_ms)
        else:
            raise ValueError(f"unsupported snapshot venue: {instrument.venue}")
        return {
            "instrument_id": instrument.instrument_id,
            "venue": instrument.venue,
            "market_symbol": instrument.market_symbol,
            "candles": candles,
        }

    async def _binance(self, symbol: str) -> list[dict[str, Any]]:
        assert self.session is not None
        params = {
            "symbol": symbol,
            "interval": self.window.interval,
            "limit": str(self.window.minutes),
        }
        async with self.session.get(BINANCE_KLINES_URL, params=params) as response:
            response.raise_for_status()
            payload = await response.json()
        if not isinstance(payload, list):
            raise TypeError("Binance kline response must be an array")
        return [
            {
                "open_time_ms": row[0],
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4],
                "volume": row[5],
                "close_time_ms": row[6],
            }
            for row in payload
        ]

    async def _hyperliquid(
        self,
        symbol: str,
        captured_ms: int,
    ) -> list[dict[str, Any]]:
        assert self.session is not None
        body = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": self.window.interval,
                "startTime": captured_ms - self.window.minutes * 60_000,
                "endTime": captured_ms,
            },
        }
        async with self.session.post(HYPERLIQUID_INFO_URL, json=body) as response:
            response.raise_for_status()
            payload = await response.json()
        if not isinstance(payload, list):
            raise TypeError("Hyperliquid candle response must be an array")
        return [
            {
                "open_time_ms": row["t"],
                "open": row["o"],
                "high": row["h"],
                "low": row["l"],
                "close": row["c"],
                "volume": row["v"],
                "close_time_ms": row["T"],
            }
            for row in payload
        ]

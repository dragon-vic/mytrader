from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from nautilus_trader.core.nautilus_pyo3 import AggressorSide
from nautilus_trader.core.nautilus_pyo3 import Bar
from nautilus_trader.core.nautilus_pyo3 import Price
from nautilus_trader.core.nautilus_pyo3 import Quantity
from nautilus_trader.core.nautilus_pyo3 import TradeId
from nautilus_trader.core.nautilus_pyo3 import TradeTick

from utils.config_loader import ROOT
from utils.config_loader import proxy_url
from utils.instrument_factory import InstrumentFactory


# 把配置里的周期字符串转成 Binance REST interval。
def kline_interval(timeframe: str) -> str:
    unit = timeframe[-1]
    value = timeframe[:-1]
    if unit not in {"s", "m", "h", "d", "w", "M"}:
        raise ValueError(f"Unsupported Binance timeframe: {timeframe}")
    return f"{value}{unit}"


# 管理当前 set 的行情拉取、CSV 路径和 NT bar 转换。
class MarketDataStore:
    def __init__(self, settings: dict[str, Any], run_type: str = "backtest") -> None:
        self.settings = settings
        self.factory = InstrumentFactory(settings, run_type)
        self.markets = self.factory.markets

    # 生成指定市场的原始 OHLCV 文件路径。
    def raw_ohlcv_path(self, market: dict[str, Any]) -> Path:
        instrument = str(market["instrument_symbol"]).replace("-", "_").lower()
        filename = f"{market['exchange']}_{instrument}_{market['timeframe']}_ohlcv.csv"
        return ROOT / self.settings["project"]["data_dir"] / "raw" / filename

    # 通过 Binance REST 拉取某个市场的 kline。
    async def fetch_ohlcv_async(self, market: dict[str, Any]) -> pd.DataFrame:
        interval = kline_interval(market["timeframe"])
        limit = int(market["limit"])
        end_time = None
        klines = []
        url = "https://fapi.binance.com/fapi/v1/klines"
        proxies = {"http": proxy_url(self.settings), "https": proxy_url(self.settings)} if proxy_url(self.settings) else None

        for _ in range(int(market.get("batches", 1))):
            params = {"symbol": self.factory.raw_symbol(market), "interval": interval, "limit": limit}
            if end_time is not None:
                params["endTime"] = end_time
            response = await asyncio.to_thread(
                requests.get,
                url,
                params=params,
                proxies=proxies,
                timeout=10,
            )
            response.raise_for_status()
            batch = response.json()
            if not batch:
                break
            klines = batch + klines
            end_time = int(batch[0][0]) - 1
            if len(batch) < limit:
                break

        if not klines:
            raise RuntimeError("Binance returned no klines.")

        return pd.DataFrame(
            [
                {
                    "timestamp": pd.to_datetime(k[0], unit="ms", utc=True),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                }
                for k in klines
            ],
        )

    # 同步入口包装，方便 fetch_data.py 直接调用。
    def fetch_ohlcv(self, market: dict[str, Any]) -> pd.DataFrame:
        if market["exchange"].lower() != "binance":
            raise ValueError("This minimal project currently supports Binance only.")
        return asyncio.run(self.fetch_ohlcv_async(market))

    # 拉取 set 里声明的额外行情数据，目前用于 funding rate。
    def fetch_extra_data(self) -> list[Path]:
        params = self.settings["strategy"].get("params", {})
        paths = []
        if "funding_csv_path" in params:
            market = self.markets[0]
            df = self.fetch_funding_rates(market, int(self.settings.get("data", {}).get("funding_limit", 1000)))
            path = ROOT / params["funding_csv_path"]
            self.save_funding_rates(df, path)
            paths.append(path)
        return paths

    # 通过 Binance 官方 REST 拉取 U 本位永续资金费历史。
    def fetch_funding_rates(self, market: dict[str, Any], limit: int) -> pd.DataFrame:
        params = self.settings["strategy"].get("params", {})
        proxy = proxy_url(self.settings)
        response = requests.get(
            f"{params.get('funding_api_base_url', 'https://fapi.binance.com')}/fapi/v1/fundingRate",
            params={"symbol": self.factory.raw_symbol(market), "limit": limit},
            proxies={"http": proxy, "https": proxy} if proxy else None,
            timeout=10,
        )
        response.raise_for_status()
        return pd.DataFrame(response.json()).sort_values("fundingTime")

    # 保存 funding rate CSV，字段保持 Binance 原始命名，供策略直接读取。
    def save_funding_rates(self, df: pd.DataFrame, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)

    # 保存 OHLCV 到 CSV。
    def save_ohlcv(self, df: pd.DataFrame, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        out = df.copy()
        out["timestamp"] = out["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        out.to_csv(path, index=False)

    # 从 CSV 读取 OHLCV 并恢复 UTC 时间戳。
    def load_ohlcv(self, path: Path) -> pd.DataFrame:
        df = pd.read_csv(path)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df

    # 把普通 OHLCV 数据转成 PyO3 Bar 对象。
    def ohlcv_to_bars(self, df: pd.DataFrame, market: dict[str, Any]):
        instrument = self.factory.instrument(market)
        bar_type = self.factory.bar_type(market)
        bars = []
        for row in df.to_dict("records"):
            ts_ns = int(pd.Timestamp(row["timestamp"]).timestamp() * 1_000_000_000)
            bars.append(
                Bar(
                    bar_type=bar_type,
                    open=Price(Decimal(str(row["open"])), instrument.price_precision),
                    high=Price(Decimal(str(row["high"])), instrument.price_precision),
                    low=Price(Decimal(str(row["low"])), instrument.price_precision),
                    close=Price(Decimal(str(row["close"])), instrument.price_precision),
                    volume=Quantity(Decimal(str(row["volume"])), instrument.size_precision),
                    ts_event=ts_ns,
                    ts_init=ts_ns,
                ),
            )
        return bars

    # 读取指定市场的 CSV 并转成 NT Bar。
    def load_bars(self, market: dict[str, Any]):
        return self.ohlcv_to_bars(self.load_ohlcv(self.raw_ohlcv_path(market)), market)

    # 从 trade tick parquet 构建 NT TradeTick。
    def load_trade_ticks(self, path: Path) -> list[TradeTick]:
        df = pd.read_parquet(
            path,
            columns=["symbol", "timestamp_ms", "price", "quantity", "buyer_maker", "trade_id"],
        )
        by_symbol = {
            self.factory.raw_symbol(market): self.factory.instrument(market)
            for market in self.markets
        }
        df = df[df["symbol"].isin(by_symbol)].sort_values(["timestamp_ms", "trade_id"])
        ticks = []
        for row in df.to_dict("records"):
            instrument = by_symbol[row["symbol"]]
            ts_ns = int(row["timestamp_ms"]) * 1_000_000
            side = AggressorSide.SELLER if bool(row["buyer_maker"]) else AggressorSide.BUYER
            ticks.append(
                TradeTick(
                    instrument_id=instrument.id,
                    price=Price(Decimal(str(row["price"])), instrument.price_precision),
                    size=Quantity(Decimal(str(row["quantity"])), instrument.size_precision),
                    aggressor_side=side,
                    trade_id=TradeId(str(row["trade_id"])),
                    ts_event=ts_ns,
                    ts_init=ts_ns,
                ),
            )
        return ticks

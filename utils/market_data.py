from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.adapters.binance.common.enums import BinanceKlineInterval
from nautilus_trader.adapters.binance.common.urls import get_http_base_url
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient
from nautilus_trader.adapters.binance.http.market import BinanceMarketHttpAPI
from nautilus_trader.common.component import LiveClock
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.persistence.wranglers import BarDataWrangler

from utils.config_loader import ROOT
from utils.config_loader import proxy_url
from utils.instrument_factory import InstrumentFactory


# 把配置里的周期字符串转成 NT 的 Binance kline interval。
def kline_interval(timeframe: str) -> BinanceKlineInterval:
    for item in BinanceKlineInterval:
        if item.value == timeframe:
            return item
    raise ValueError(f"Unsupported Binance timeframe: {timeframe}")


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

    def account_type(self) -> BinanceAccountType:
        if self.settings["mode"] == "backtest":
            value = self.settings["backtest"]["venue_account"]["account_type"]
        else:
            client = self.settings["strategy"]["params"]["instrument_client"]
            source = self.settings["node"]["data"]["clients"][client]
            value = source["account_type"]
        return getattr(BinanceAccountType, value)

    # 通过 NT 的 Binance adapter 异步拉取某个市场的 kline。
    async def fetch_ohlcv_async(self, market: dict[str, Any]) -> pd.DataFrame:
        account_type = self.account_type()
        client = BinanceHttpClient(
            clock=LiveClock(),
            api_key=None,
            api_secret=None,
            base_url=get_http_base_url(account_type, BinanceEnvironment.LIVE, is_us=False),
            proxy_url=proxy_url(self.settings),
        )
        interval = kline_interval(market["timeframe"])
        limit = int(market["limit"])
        end_time = None
        klines = []

        for _ in range(int(market.get("batches", 1))):
            batch = await BinanceMarketHttpAPI(client=client, account_type=account_type).query_klines(
                symbol=self.factory.raw_symbol(market),
                interval=interval,
                limit=limit,
                end_time=end_time,
            )
            if not batch:
                break
            klines = batch + klines
            end_time = batch[0].open_time - 1
            if len(batch) < limit:
                break

        if not klines:
            raise RuntimeError("Binance returned no klines.")

        return pd.DataFrame(
            [
                {
                    "timestamp": pd.to_datetime(k.open_time, unit="ms", utc=True),
                    "open": float(k.open),
                    "high": float(k.high),
                    "low": float(k.low),
                    "close": float(k.close),
                    "volume": float(k.volume),
                }
                for k in klines
            ],
        )

    # 同步入口包装，方便数据拉取脚本直接调用。
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
            df = self.fetch_funding_rates(market, int(self.settings["node"]["data"].get("funding_limit", 1000)))
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

    # 把普通 OHLCV 数据转成 NT 回测引擎需要的 Bar 对象。
    def ohlcv_to_bars(self, df: pd.DataFrame, market: dict[str, Any]):
        data = df.copy()
        data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
        data = data.set_index("timestamp")
        return BarDataWrangler(
            self.factory.bar_type(market),
            self.factory.instrument(market),
        ).process(data[["open", "high", "low", "close", "volume"]])

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

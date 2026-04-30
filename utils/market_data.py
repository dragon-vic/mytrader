from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pandas as pd

from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.adapters.binance.common.enums import BinanceKlineInterval
from nautilus_trader.adapters.binance.common.urls import get_http_base_url
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient
from nautilus_trader.adapters.binance.http.market import BinanceMarketHttpAPI
from nautilus_trader.common.component import LiveClock
from nautilus_trader.persistence.wranglers import BarDataWrangler

from utils.config_loader import ROOT
from utils.config_loader import market_configs
from utils.config_loader import proxy_url
from utils.instrument_factory import binance_raw_symbol
from utils.instrument_factory import make_bar_type
from utils.instrument_factory import make_instrument


# 生成指定市场的原始 OHLCV 文件路径。
def raw_ohlcv_path(settings: dict[str, Any], market: dict[str, Any] | None = None) -> Path:
    market = market or market_configs(settings)[0]
    instrument = str(market["instrument_symbol"]).replace("-", "_").lower()
    filename = f"{market['exchange']}_{instrument}_{market['timeframe']}_ohlcv.csv"
    return ROOT / settings["project"]["data_dir"] / "raw" / filename


# 把配置里的周期字符串转成 NT 的 Binance kline interval。
def kline_interval(timeframe: str) -> BinanceKlineInterval:
    for item in BinanceKlineInterval:
        if item.value == timeframe:
            return item
    raise ValueError(f"Unsupported Binance timeframe: {timeframe}")


# 通过 NT 的 Binance adapter 异步拉取某个市场的 kline。
async def fetch_ohlcv_async(
    settings: dict[str, Any],
    market: dict[str, Any] | None = None,
) -> pd.DataFrame:
    market = market or market_configs(settings)[0]
    account_type = getattr(BinanceAccountType, settings["live"]["account_type"])
    client = BinanceHttpClient(
        clock=LiveClock(),
        api_key=None,
        api_secret=None,
        base_url=get_http_base_url(account_type, BinanceEnvironment.LIVE, is_us=False),
        proxy_url=proxy_url(settings),
    )
    interval = kline_interval(market["timeframe"])
    limit = int(market["limit"])
    end_time = None
    klines = []

    for _ in range(int(market.get("batches", 1))):
        batch = await BinanceMarketHttpAPI(client=client, account_type=account_type).query_klines(
            symbol=binance_raw_symbol(settings, market),
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


# 同步入口包装，方便 fetch_data.py 直接调用。
def fetch_ohlcv(settings: dict[str, Any], market: dict[str, Any] | None = None) -> pd.DataFrame:
    market = market or market_configs(settings)[0]
    if market["exchange"].lower() != "binance":
        raise ValueError("This minimal project currently supports Binance only.")
    return asyncio.run(fetch_ohlcv_async(settings, market))


# 保存 OHLCV 到 CSV。
def save_ohlcv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    out["timestamp"] = out["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    out.to_csv(path, index=False)


# 从 CSV 读取 OHLCV 并恢复 UTC 时间戳。
def load_ohlcv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


# 把普通 OHLCV 数据转成 NT 回测引擎需要的 Bar 对象。
def ohlcv_to_bars(
    df: pd.DataFrame,
    settings: dict[str, Any],
    market: dict[str, Any] | None = None,
):
    instrument = make_instrument(settings, market)
    data = df.copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
    data = data.set_index("timestamp")
    return BarDataWrangler(make_bar_type(settings, market), instrument).process(
        data[["open", "high", "low", "close", "volume"]],
    )

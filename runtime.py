from __future__ import annotations

import asyncio
import importlib
import os
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from dotenv import load_dotenv

from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.adapters.binance.common.enums import BinanceKlineInterval
from nautilus_trader.adapters.binance.common.urls import get_http_base_url
from nautilus_trader.adapters.binance.config import BinanceDataClientConfig
from nautilus_trader.adapters.binance.config import BinanceExecClientConfig
from nautilus_trader.adapters.binance.config import BinanceInstrumentProviderConfig
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient
from nautilus_trader.adapters.binance.http.market import BinanceMarketHttpAPI
from nautilus_trader.common.component import LiveClock
from nautilus_trader.config import CacheConfig
from nautilus_trader.config import RoutingConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.persistence.wranglers import BarDataWrangler

ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_NAME = "ema_cross_1"
BINANCE_CLIENT_NAME = "BINANCE"


# 加载一个具名 set，让每个策略保留自己的市场和参数。
def load_settings(config_name: str | None = None) -> dict[str, Any]:
    name = config_name or DEFAULT_CONFIG_NAME
    path = ROOT / "config" / f"{name}.yaml"
    with path.open("r", encoding="utf-8") as f:
        settings = yaml.safe_load(f)
    settings["project"]["config_name"] = name
    return settings


# 所有生成的数据、报告和日志都放在项目目录内。
def ensure_dirs(settings: dict[str, Any]) -> None:
    for path in (
        ROOT / settings["project"]["data_dir"] / "raw",
        reports_dir(settings),
        ROOT / "logs",
    ):
        path.mkdir(parents=True, exist_ok=True)


# 返回当前 set 的报告目录。
def reports_dir(settings: dict[str, Any]) -> Path:
    return ROOT / settings["project"]["reports_dir"] / settings["project"]["config_name"]


# 返回当前 set 的市场列表；旧 set 只有 market，新 set 可以有 markets。
def market_configs(settings: dict[str, Any]) -> list[dict[str, Any]]:
    return settings.get("markets") or [settings["market"]]


# 返回当前 set 涉及的全部 NT venue。
def venue_ids(settings: dict[str, Any]) -> frozenset[str]:
    return frozenset(market["venue"] for market in market_configs(settings))


# 构建 NT cache 配置，当前只收紧 bar/tick 内存容量。
def cache_config(settings: dict[str, Any]) -> CacheConfig:
    capacity = int(settings.get("runtime", {}).get("cache_capacity", 1000))
    return CacheConfig(
        tick_capacity=capacity,
        bar_capacity=capacity,
        drop_instruments_on_reset=False,
    )


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


# 读取当前 set 的代理地址。
def proxy_url(settings: dict[str, Any]) -> str | None:
    market = market_configs(settings)[0]
    return settings.get("live", {}).get("proxy_url") or market.get("proxy_url")


# 返回 Binance 原生 symbol，合约回测文件名和 NT symbol 可以不同。
def binance_raw_symbol(settings: dict[str, Any], market: dict[str, Any] | None = None) -> str:
    market = market or market_configs(settings)[0]
    return market.get("raw_symbol") or market["instrument_symbol"]


# 构建 NT 的 Binance market API，account_type 决定现货或合约接口。
def market_api(settings: dict[str, Any], env: BinanceEnvironment) -> BinanceMarketHttpAPI:
    acct = getattr(BinanceAccountType, settings["live"]["account_type"])
    client = BinanceHttpClient(
        clock=LiveClock(),
        api_key=None,
        api_secret=None,
        base_url=get_http_base_url(acct, env, is_us=False),
        proxy_url=proxy_url(settings),
    )
    return BinanceMarketHttpAPI(client=client, account_type=acct)


# 通过 NT 的 Binance adapter 异步拉取某个市场的 kline。
async def fetch_ohlcv_async(
    settings: dict[str, Any],
    market: dict[str, Any] | None = None,
) -> pd.DataFrame:
    market = market or market_configs(settings)[0]
    api = market_api(settings, BinanceEnvironment.LIVE)
    interval = kline_interval(market["timeframe"])
    limit = int(market["limit"])
    end_time = None
    klines = []

    for _ in range(int(market.get("batches", 1))):
        batch = await api.query_klines(
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


# 把 1m/1h/1d 这种周期转成 NT BarType 需要的 bar spec。
def timeframe_to_bar_spec(timeframe: str) -> str:
    unit = timeframe[-1]
    value = int(timeframe[:-1])
    mapping = {"m": "MINUTE", "h": "HOUR", "d": "DAY"}
    if unit not in mapping:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return f"{value}-{mapping[unit]}"


# 构建指定市场的 NT instrument id。
def instrument_id(settings: dict[str, Any], market: dict[str, Any] | None = None) -> InstrumentId:
    market = market or market_configs(settings)[0]
    return InstrumentId(Symbol(market["instrument_symbol"]), Venue(market["venue"]))


# 配置值为空时返回 None，否则构建 NT Money。
def optional_money(value: Any, currency: Currency) -> Money | None:
    return Money(Decimal(str(value)), currency) if value is not None else None


# 构建现货 CurrencyPair instrument。
def make_currency_pair(
    settings: dict[str, Any],
    market: dict[str, Any] | None = None,
) -> CurrencyPair:
    market = market or market_configs(settings)[0]
    cfg = settings["instrument"]
    base = Currency.from_str(market.get("base_currency", cfg["base_currency"]))
    quote = Currency.from_str(market.get("quote_currency", cfg["quote_currency"]))
    return CurrencyPair(
        instrument_id=instrument_id(settings, market),
        raw_symbol=Symbol(binance_raw_symbol(settings, market)),
        base_currency=base,
        quote_currency=quote,
        price_precision=int(cfg["price_precision"]),
        size_precision=int(cfg["size_precision"]),
        price_increment=Price.from_str(str(cfg["price_increment"])),
        size_increment=Quantity.from_str(str(cfg["size_increment"])),
        lot_size=Quantity.from_str(str(cfg["size_increment"])),
        max_quantity=Quantity.from_str(str(cfg["max_quantity"])),
        min_quantity=Quantity.from_str(str(cfg["min_quantity"])),
        max_notional=optional_money(cfg.get("max_notional"), quote),
        min_notional=optional_money(cfg.get("min_notional"), quote),
        max_price=Price.from_str(str(cfg.get("max_price", "10000000"))),
        min_price=Price.from_str(str(cfg["price_increment"])),
        margin_init=Decimal(str(cfg.get("margin_init", "0"))),
        margin_maint=Decimal(str(cfg.get("margin_maint", "0"))),
        maker_fee=Decimal(str(cfg["maker_fee"])),
        taker_fee=Decimal(str(cfg["taker_fee"])),
        ts_event=0,
        ts_init=0,
    )


# 构建 U 本位等永续合约 CryptoPerpetual instrument。
def make_crypto_perpetual(
    settings: dict[str, Any],
    market: dict[str, Any] | None = None,
) -> CryptoPerpetual:
    market = market or market_configs(settings)[0]
    cfg = settings["instrument"]
    base = Currency.from_str(market.get("base_currency", cfg["base_currency"]))
    quote = Currency.from_str(market.get("quote_currency", cfg["quote_currency"]))
    settlement = Currency.from_str(market.get("settlement_currency", cfg["settlement_currency"]))
    return CryptoPerpetual(
        instrument_id=instrument_id(settings, market),
        raw_symbol=Symbol(binance_raw_symbol(settings, market)),
        base_currency=base,
        quote_currency=quote,
        settlement_currency=settlement,
        is_inverse=bool(cfg.get("is_inverse", False)),
        price_precision=int(cfg["price_precision"]),
        size_precision=int(cfg["size_precision"]),
        price_increment=Price.from_str(str(cfg["price_increment"])),
        size_increment=Quantity.from_str(str(cfg["size_increment"])),
        ts_event=0,
        ts_init=0,
        multiplier=Quantity.from_str(str(cfg.get("multiplier", "1"))),
        lot_size=Quantity.from_str(str(cfg["size_increment"])),
        max_quantity=Quantity.from_str(str(cfg["max_quantity"])),
        min_quantity=Quantity.from_str(str(cfg["min_quantity"])),
        max_notional=optional_money(cfg.get("max_notional"), quote),
        min_notional=optional_money(cfg.get("min_notional"), quote),
        max_price=Price.from_str(str(cfg.get("max_price", "10000000"))),
        min_price=Price.from_str(str(cfg["price_increment"])),
        margin_init=Decimal(str(cfg.get("margin_init", "1"))),
        margin_maint=Decimal(str(cfg.get("margin_maint", "1"))),
        maker_fee=Decimal(str(cfg["maker_fee"])),
        taker_fee=Decimal(str(cfg["taker_fee"])),
    )


# 根据 set 里的 instrument.kind 选择现货或永续合约 instrument。
def make_instrument(settings: dict[str, Any], market: dict[str, Any] | None = None) -> Instrument:
    kind = settings["instrument"].get("kind", "spot")
    if kind == "spot":
        return make_currency_pair(settings, market)
    if kind == "perpetual":
        return make_crypto_perpetual(settings, market)
    raise ValueError(f"Unsupported instrument kind: {kind}")


# 构建当前 set 的全部 instrument。
def make_instruments(settings: dict[str, Any]) -> list[Instrument]:
    return [make_instrument(settings, market) for market in market_configs(settings)]


# 构建指定市场对应的 NT BarType。
def make_bar_type(settings: dict[str, Any], market: dict[str, Any] | None = None) -> BarType:
    market = market or market_configs(settings)[0]
    spec = timeframe_to_bar_spec(market["timeframe"])
    return BarType.from_str(f"{market['instrument_symbol']}.{market['venue']}-{spec}-LAST-EXTERNAL")


# 构建当前 set 的全部 BarType。
def make_bar_types(settings: dict[str, Any]) -> list[BarType]:
    return [make_bar_type(settings, market) for market in market_configs(settings)]


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


# 构建 Binance live data/exec client 共用的 instrument provider 配置。
def instrument_provider(settings: dict[str, Any]) -> BinanceInstrumentProviderConfig:
    return BinanceInstrumentProviderConfig(
        load_all=False,
        load_ids=frozenset(instrument.id for instrument in make_instruments(settings)),
    )

# 构建 Binance live data client 配置。
def binance_data_config(settings: dict[str, Any]) -> BinanceDataClientConfig:
    return BinanceDataClientConfig(
        account_type=getattr(BinanceAccountType, settings["live"]["account_type"]),
        environment=getattr(BinanceEnvironment, settings["live"]["environment"]),
        proxy_url=proxy_url(settings),
        instrument_provider=instrument_provider(settings),
        routing=RoutingConfig(default=True, venues=venue_ids(settings)),
    )

# 构建 Binance live exec client 配置；没有 key 时返回 None。
def binance_exec_config(settings: dict[str, Any]) -> BinanceExecClientConfig | None:
    load_dotenv(ROOT / ".env")

    api_key = os.getenv(settings["live"]["api_key_env"])
    api_secret = os.getenv(settings["live"]["api_secret_env"])
    if not api_key or not api_secret:
        return None
    return BinanceExecClientConfig(
        api_key=api_key,
        api_secret=api_secret,
        account_type=getattr(BinanceAccountType, settings["live"]["account_type"]),
        environment=getattr(BinanceEnvironment, settings["live"]["environment"]),
        proxy_url=proxy_url(settings),
        instrument_provider=instrument_provider(settings),
        routing=RoutingConfig(default=True, venues=venue_ids(settings)),
    )


# 根据 set 里的策略类配置动态构建策略实例。
def build_strategy(settings: dict[str, Any]):
    strategy = settings["strategy"]
    module = importlib.import_module(strategy["module"])
    strategy_cls = getattr(module, strategy["class"])
    config_cls = getattr(module, strategy["config_class"])
    if "markets" in settings:
        instruments = make_instruments(settings)
        config = config_cls(
            instrument_ids=[instrument.id for instrument in instruments],
            bar_types=make_bar_types(settings),
            trade_notional=Decimal(str(strategy["trade_notional"])),
            **strategy.get("params", {}),
        )
    else:
        instrument = make_instrument(settings)
        config = config_cls(
            instrument_id=instrument.id,
            bar_type=make_bar_type(settings),
            trade_size=Decimal(str(strategy["trade_size"])),
            **strategy.get("params", {}),
        )
    return strategy_cls(config)

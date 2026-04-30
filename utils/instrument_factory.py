from __future__ import annotations

from decimal import Decimal
from typing import Any

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

from utils.config_loader import market_configs


# 返回 Binance 原生 symbol，合约回测文件名和 NT symbol 可以不同。
def binance_raw_symbol(settings: dict[str, Any], market: dict[str, Any] | None = None) -> str:
    market = market or market_configs(settings)[0]
    return market.get("raw_symbol") or market["instrument_symbol"]


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

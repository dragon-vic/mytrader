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

from utils.arguments import TIMEFRAME_UNITS
from utils.config_loader import market_configs


# 把 1s/1m/1h/1d 这种周期转成 NT BarType 需要的 bar spec。
def timeframe_to_bar_spec(timeframe: str) -> str:
    unit = timeframe[-1]
    value = int(timeframe[:-1])
    if unit not in TIMEFRAME_UNITS:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return f"{value}-{TIMEFRAME_UNITS[unit]}"


# 配置值为空时返回 None，否则构建 NT Money。
def optional_money(value: Any, currency: Currency) -> Money | None:
    return Money(Decimal(str(value)), currency) if value is not None else None


# 根据 set 构建 NT instrument 和 bar type。
class InstrumentFactory:
    def __init__(self, settings: dict[str, Any], run_type: str = "live") -> None:
        self.settings = settings
        self.markets = market_configs(settings)
        if run_type == "backtest":
            self.cfg = dict(settings.get("backtest", {}).get("instrument", {}))
            self.cfg.update(settings.get("instrument", {}))
        else:
            self.cfg = dict(settings.get("instrument", {}))

    # 返回 Binance 原生 symbol，文件名和 NT symbol 可以不同。
    def raw_symbol(self, market: dict[str, Any]) -> str:
        return market.get("raw_symbol") or market["instrument_symbol"]

    # 构建指定市场的 NT instrument id。
    def instrument_id(self, market: dict[str, Any]) -> InstrumentId:
        return InstrumentId(Symbol(market["instrument_symbol"]), Venue(market["venue"]))

    # 构建现货 CurrencyPair instrument。
    def currency_pair(self, market: dict[str, Any]) -> CurrencyPair:
        base = Currency.from_str(market["base_currency"])
        quote = Currency.from_str(market["quote_currency"])
        return CurrencyPair(
            instrument_id=self.instrument_id(market),
            raw_symbol=Symbol(self.raw_symbol(market)),
            base_currency=base,
            quote_currency=quote,
            price_precision=int(self.cfg["price_precision"]),
            size_precision=int(self.cfg["size_precision"]),
            price_increment=Price.from_str(str(self.cfg["price_increment"])),
            size_increment=Quantity.from_str(str(self.cfg["size_increment"])),
            lot_size=Quantity.from_str(str(self.cfg["size_increment"])),
            max_quantity=Quantity.from_str(str(self.cfg["max_quantity"])),
            min_quantity=Quantity.from_str(str(self.cfg["min_quantity"])),
            max_notional=optional_money(self.cfg.get("max_notional"), quote),
            min_notional=optional_money(self.cfg.get("min_notional"), quote),
            max_price=Price.from_str(str(self.cfg["max_price"])),
            min_price=Price.from_str(str(self.cfg["price_increment"])),
            margin_init=Decimal(str(self.cfg["margin_init"])),
            margin_maint=Decimal(str(self.cfg["margin_maint"])),
            maker_fee=Decimal(str(self.cfg["maker_fee"])),
            taker_fee=Decimal(str(self.cfg["taker_fee"])),
            ts_event=0,
            ts_init=0,
        )

    # 构建 U 本位永续合约 CryptoPerpetual instrument。
    def crypto_perpetual(self, market: dict[str, Any]) -> CryptoPerpetual:
        base = Currency.from_str(market["base_currency"])
        quote = Currency.from_str(market["quote_currency"])
        settlement = Currency.from_str(market["settlement_currency"])
        return CryptoPerpetual(
            instrument_id=self.instrument_id(market),
            raw_symbol=Symbol(self.raw_symbol(market)),
            base_currency=base,
            quote_currency=quote,
            settlement_currency=settlement,
            is_inverse=bool(self.cfg["is_inverse"]),
            price_precision=int(self.cfg["price_precision"]),
            size_precision=int(self.cfg["size_precision"]),
            price_increment=Price.from_str(str(self.cfg["price_increment"])),
            size_increment=Quantity.from_str(str(self.cfg["size_increment"])),
            ts_event=0,
            ts_init=0,
            multiplier=Quantity.from_str(str(self.cfg["multiplier"])),
            lot_size=Quantity.from_str(str(self.cfg["size_increment"])),
            max_quantity=Quantity.from_str(str(self.cfg["max_quantity"])),
            min_quantity=Quantity.from_str(str(self.cfg["min_quantity"])),
            max_notional=optional_money(self.cfg.get("max_notional"), quote),
            min_notional=optional_money(self.cfg.get("min_notional"), quote),
            max_price=Price.from_str(str(self.cfg["max_price"])),
            min_price=Price.from_str(str(self.cfg["price_increment"])),
            margin_init=Decimal(str(self.cfg["margin_init"])),
            margin_maint=Decimal(str(self.cfg["margin_maint"])),
            maker_fee=Decimal(str(self.cfg["maker_fee"])),
            taker_fee=Decimal(str(self.cfg["taker_fee"])),
        )

    # 根据 instrument.kind 选择现货或永续合约。
    def instrument(self, market: dict[str, Any]) -> Instrument:
        kind = self.cfg["kind"]
        if kind == "spot":
            return self.currency_pair(market)
        if kind == "perpetual":
            return self.crypto_perpetual(market)
        raise ValueError(f"Unsupported instrument kind: {kind}")

    # 构建当前 set 的全部 instrument。
    def instruments(self) -> list[Instrument]:
        return [self.instrument(market) for market in self.markets]

    # 构建指定市场对应的 NT BarType。
    def bar_type(self, market: dict[str, Any]) -> BarType:
        spec = timeframe_to_bar_spec(market["timeframe"])
        aggregation_source = "INTERNAL" if market["timeframe"].endswith("s") else "EXTERNAL"
        return BarType.from_str(f"{market['instrument_symbol']}.{market['venue']}-{spec}-LAST-{aggregation_source}")

    # 构建当前 set 的全部 BarType。
    def bar_types(self) -> list[BarType]:
        return [self.bar_type(market) for market in self.markets]

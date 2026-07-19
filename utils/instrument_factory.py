from __future__ import annotations

from decimal import Decimal
from typing import Any

from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


def optional_money(value: Any, currency: Currency) -> Money | None:
    return Money(Decimal(str(value)), currency) if value is not None else None


# 从一个规范化 backtest venue 构造合成 instrument。
class InstrumentFactory:
    def __init__(self, venue: dict[str, Any]) -> None:
        self.markets = venue["markets"]
        self.cfg = venue["instrument"]
        self.kind = venue["instrument_kind"]
        self._instruments = [self.instrument(market) for market in self.markets]

    def raw_symbol(self, market: dict[str, Any]) -> str:
        return str(market["raw_symbol"])

    def instrument_id(self, market: dict[str, Any]) -> InstrumentId:
        return InstrumentId.from_str(market["instrument_id"])

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
            lot_size=Quantity.from_str(str(self.cfg["lot_size"])),
            max_quantity=Quantity.from_str(str(self.cfg["max_quantity"])),
            min_quantity=Quantity.from_str(str(self.cfg["min_quantity"])),
            max_notional=optional_money(self.cfg["max_notional"], quote),
            min_notional=optional_money(self.cfg["min_notional"], quote),
            max_price=Price.from_str(str(self.cfg["max_price"])),
            min_price=Price.from_str(str(self.cfg["min_price"])),
            margin_init=Decimal(str(self.cfg["margin_init"])),
            margin_maint=Decimal(str(self.cfg["margin_maint"])),
            maker_fee=Decimal(str(self.cfg["maker_fee"])),
            taker_fee=Decimal(str(self.cfg["taker_fee"])),
            ts_event=0,
            ts_init=0,
        )

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
            multiplier=Quantity.from_str(str(self.cfg["multiplier"])),
            lot_size=Quantity.from_str(str(self.cfg["lot_size"])),
            max_quantity=Quantity.from_str(str(self.cfg["max_quantity"])),
            min_quantity=Quantity.from_str(str(self.cfg["min_quantity"])),
            max_notional=optional_money(self.cfg["max_notional"], quote),
            min_notional=optional_money(self.cfg["min_notional"], quote),
            max_price=Price.from_str(str(self.cfg["max_price"])),
            min_price=Price.from_str(str(self.cfg["min_price"])),
            margin_init=Decimal(str(self.cfg["margin_init"])),
            margin_maint=Decimal(str(self.cfg["margin_maint"])),
            maker_fee=Decimal(str(self.cfg["maker_fee"])),
            taker_fee=Decimal(str(self.cfg["taker_fee"])),
            ts_event=0,
            ts_init=0,
        )

    def instrument(self, market: dict[str, Any]) -> Instrument:
        if self.kind == "spot":
            return self.currency_pair(market)
        if self.kind == "perpetual":
            return self.crypto_perpetual(market)
        raise ValueError(f"unsupported backtest instrument_kind: {self.kind}")

    def instruments(self) -> list[Instrument]:
        return list(self._instruments)

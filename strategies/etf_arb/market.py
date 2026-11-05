from dataclasses import dataclass, field

from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.identifiers import InstrumentId

from .baseline import FairValue, MINUTE


@dataclass
class Market:
    instrument_id: InstrumentId
    bid: float = 0.
    ask: float = 0.
    bid_size: float = 0.
    ask_size: float = 0.
    quote_ns: int = 0
    depth_ns: int = 0
    last_fill_ns: int = 0
    funding_ns: int = 0
    funding_seen_ns: int = 0
    last_funding_ns: int = 0
    funding_history_ready: bool = False
    funding_rate: float = 0.
    book: OrderBook = field(init=False)

    def __post_init__(self):
        self.book = OrderBook(self.instrument_id, BookType.L2_MBP)

    def fresh(self, now: int, maximum_ms: int) -> bool:
        return self.quote_ns > self.last_fill_ns and 0 <= now - self.quote_ns <= maximum_ms * 1_000_000 and 0 < self.bid <= self.ask and min(self.bid_size, self.ask_size) > 0

    def funding_blocked(self, now: int, minutes: int, max_age_ms: int) -> bool:
        if not self.funding_history_ready or not self.funding_ns or not 0 <= now - self.funding_seen_ns <= max_age_ms * 1_000_000:
            return True
        window = minutes * MINUTE
        return abs(now - self.funding_ns) <= window or (self.last_funding_ns > 0 and 0 <= now - self.last_funding_ns <= window)

    def levels(self, buy: bool, now: int, max_age_ms: int) -> list[tuple[float, float]]:
        best, size = (self.ask, self.ask_size) if buy else (self.bid, self.bid_size)
        book_best = self.book.best_ask_price() if buy else self.book.best_bid_price()
        if self.depth_ns <= self.last_fill_ns or not 0 <= now - self.depth_ns <= max_age_ms * 1_000_000 or book_best is None or float(book_best) != best:
            return [(best, size)]
        levels = self.book.asks() if buy else self.book.bids()
        result = [(float(level.price), float(level.size())) for level in levels[:5]]
        if result:
            result[0] = best, min(size, result[0][1])
        return result


def sweep(levels: list[tuple[float, float]], quantity: float) -> tuple[float, float] | None:
    remaining, value, worst = quantity, 0., 0.
    for price, size in levels:
        take = min(size, remaining)
        value += take * price
        remaining -= take
        worst = price
        if remaining <= max(1e-10, quantity * 1e-10):
            return value / quantity, worst
    return None


def entry_gap(fair: FairValue, etf: Market, under: Market, side: int) -> float:
    etf_price = etf.bid if side > 0 else etf.ask
    under_buy = side * fair.multiple > 0
    under_price = under.ask if under_buy else under.bid
    theoretical = fair.price(under_price)
    return side * (etf_price / theoretical - 1) if theoretical > 0 else float('-inf')


def exit_gap(fair: FairValue, etf: Market, under: Market, side: int) -> float:
    etf_price = etf.ask if side > 0 else etf.bid
    under_buy = side * fair.multiple < 0
    under_price = under.ask if under_buy else under.bid
    theoretical = fair.price(under_price)
    return side * (etf_price / theoretical - 1) if theoretical > 0 else float('inf')

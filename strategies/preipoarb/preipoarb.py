from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from math import sqrt

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy


@dataclass
class ArbLeg:
    instrument_id: InstrumentId
    price: Decimal


@dataclass
class ArbPos:
    buy_id: InstrumentId
    sell_id: InstrumentId
    buy_px: Decimal
    sell_px: Decimal
    edge_bps: Decimal
    mean_bps: float
    z_score: float
    opened_ns: int


@dataclass
class SpreadState:
    buy: ArbLeg
    sell: ArbLeg
    edge_bps: Decimal
    mean_bps: float
    std_bps: float
    z_score: float
    samples: int


class SpreadWindow:
    def __init__(self) -> None:
        self.points: deque[tuple[int, float]] = deque()
        self.total = 0.0
        self.total_sq = 0.0

    def add(self, ts_ns: int, value: float, lookback_ns: int) -> None:
        self.points.append((ts_ns, value))
        self.total += value
        self.total_sq += value * value
        cutoff = ts_ns - lookback_ns
        while self.points and self.points[0][0] < cutoff:
            _, old = self.points.popleft()
            self.total -= old
            self.total_sq -= old * old

    def stats(self) -> tuple[int, float, float]:
        count = len(self.points)
        if count == 0:
            return 0, 0.0, 0.0
        mean = self.total / count
        variance = max(self.total_sq / count - mean * mean, 0.0)
        return count, mean, sqrt(variance)


class PreipoArbConfig(StrategyConfig, frozen=True):
    instruments: list[str]
    assets: list[str]
    spread_mode: str
    one_sided: bool
    entry_bps: Decimal
    exit_bps: Decimal
    lookback_sec: float
    min_spread_samples: int
    entry_z: Decimal
    exit_z: Decimal
    min_entry_bps: Decimal
    trade_notional: Decimal
    max_hold_sec: float
    min_hold_sec: float
    cooldown_sec: float
    max_quote_age_sec: float
    slippage_bps: Decimal
    fee_bps: dict[str, float]
    dry_run: bool


class PreipoArbStrategy(Strategy):
    def __init__(self, config: PreipoArbConfig) -> None:
        super().__init__(config)
        self.instruments = [InstrumentId.from_str(value) for value in config.instruments]
        self.assets = [asset.upper() for asset in config.assets]
        self.spread_mode = config.spread_mode
        self.one_sided = bool(config.one_sided)
        self.entry_bps = Decimal(str(config.entry_bps))
        self.exit_bps = Decimal(str(config.exit_bps))
        self.lookback_ns = int(float(config.lookback_sec) * 1_000_000_000)
        self.min_spread_samples = int(config.min_spread_samples)
        self.entry_z = float(config.entry_z)
        self.exit_z = float(config.exit_z)
        self.min_entry_bps = Decimal(str(config.min_entry_bps))
        self.notional = Decimal(str(config.trade_notional))
        self.max_hold_ns = int(float(config.max_hold_sec) * 1_000_000_000)
        self.min_hold_ns = int(float(config.min_hold_sec) * 1_000_000_000)
        self.cooldown_ns = int(float(config.cooldown_sec) * 1_000_000_000)
        self.max_quote_age_ns = int(float(config.max_quote_age_sec) * 1_000_000_000)
        self.slippage_bps = Decimal(str(config.slippage_bps))
        self.fee_bps = {key.upper(): Decimal(str(value)) for key, value in config.fee_bps.items()}
        self.quotes: dict[InstrumentId, QuoteTick] = {}
        self.windows: dict[tuple[str, InstrumentId, InstrumentId], SpreadWindow] = {}
        self.positions: dict[str, ArbPos] = {}
        self.last_close_ns: dict[str, int] = {}

    def on_start(self) -> None:
        if self.spread_mode not in {"threshold", "mean_deviation"}:
            raise RuntimeError("spread_mode must be threshold or mean_deviation")
        if self.spread_mode == "threshold" and self.entry_bps <= self.exit_bps:
            raise RuntimeError("entry_bps must be greater than exit_bps")
        if self.spread_mode == "mean_deviation":
            if self.lookback_ns <= 0:
                raise RuntimeError("lookback_sec must be positive")
            if self.min_spread_samples <= 1:
                raise RuntimeError("min_spread_samples must be greater than 1")
            if self.entry_z <= self.exit_z:
                raise RuntimeError("entry_z must be greater than exit_z")
        if self.notional <= 0:
            raise RuntimeError("trade_notional must be positive")
        if self.config.min_hold_sec < 0:
            raise RuntimeError("min_hold_sec must be positive")
        if self.config.cooldown_sec < 0:
            raise RuntimeError("cooldown_sec must be positive")
        if self.config.max_quote_age_sec <= 0:
            raise RuntimeError("max_quote_age_sec must be positive")
        for instrument_id in self.instruments:
            self.subscribe_quote_ticks(instrument_id)
        self.log.info(
            f"preipo_arb started assets={','.join(self.assets)} instruments={len(self.instruments)} "
            f"mode={self.spread_mode} one_sided={self.one_sided} "
            f"entry_bps={self.entry_bps} exit_bps={self.exit_bps} "
            f"lookback={self.config.lookback_sec}s entry_z={self.entry_z} exit_z={self.exit_z} "
            f"notional={self.notional} dry_run={self.config.dry_run}",
        )

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self.quotes[tick.instrument_id] = tick
        asset = self._asset(tick.instrument_id)
        if asset is None:
            return
        states = self._update_spreads(asset)
        if asset in self.positions:
            self._maybe_close(asset, states)
        else:
            self._maybe_open(asset, states)

    def on_stop(self) -> None:
        for instrument_id in self.instruments:
            self.unsubscribe_quote_ticks(instrument_id)

    def _asset(self, instrument_id: InstrumentId) -> str | None:
        symbol = str(instrument_id.symbol).upper().replace("PF_", "").replace("VNTL-", "")
        symbol = symbol.replace("OPENAIX", "OPENAI").replace("ANTHROPICX", "ANTHROPIC")
        for asset in self.assets:
            if symbol.startswith(asset):
                return asset
        return None

    def _venue(self, instrument_id: InstrumentId) -> str:
        return str(instrument_id.venue).upper()

    def _fee(self, instrument_id: InstrumentId) -> Decimal:
        return self.fee_bps[self._venue(instrument_id)]

    def _valid_quotes(self, asset: str) -> tuple[list[ArbLeg], list[ArbLeg]]:
        bids: list[ArbLeg] = []
        asks: list[ArbLeg] = []
        now_ns = self.clock.timestamp_ns()
        for instrument_id, quote in self.quotes.items():
            if self._asset(instrument_id) != asset:
                continue
            if now_ns - quote.ts_event > self.max_quote_age_ns:
                continue
            bid = Decimal(str(quote.bid_price))
            ask = Decimal(str(quote.ask_price))
            if bid > 0:
                bids.append(ArbLeg(instrument_id, bid))
            if ask > 0:
                asks.append(ArbLeg(instrument_id, ask))
        return bids, asks

    def _best(self, asset: str) -> tuple[ArbLeg, ArbLeg] | None:
        bids, asks = self._valid_quotes(asset)
        if not bids or not asks:
            return None
        buy = min(asks, key=lambda leg: leg.price)
        sell = max(bids, key=lambda leg: leg.price)
        if buy.instrument_id == sell.instrument_id:
            return None
        return buy, sell

    def _net_bps(self, buy: ArbLeg, sell: ArbLeg) -> Decimal:
        gross = (sell.price - buy.price) / buy.price * Decimal("10000")
        costs = self._fee(buy.instrument_id) + self._fee(sell.instrument_id) + self.slippage_bps * Decimal("2")
        return gross - costs

    def _update_spreads(self, asset: str) -> dict[tuple[InstrumentId, InstrumentId], SpreadState]:
        states: dict[tuple[InstrumentId, InstrumentId], SpreadState] = {}
        if self.spread_mode != "mean_deviation":
            return states

        bids, asks = self._valid_quotes(asset)
        now_ns = self.clock.timestamp_ns()
        for buy in asks:
            for sell in bids:
                if buy.instrument_id == sell.instrument_id:
                    continue
                edge = self._net_bps(buy, sell)
                key = (asset, buy.instrument_id, sell.instrument_id)
                window = self.windows.setdefault(key, SpreadWindow())
                window.add(now_ns, float(edge), self.lookback_ns)
                samples, mean, std = window.stats()
                if samples < self.min_spread_samples or std <= 0:
                    continue
                z_score = (float(edge) - mean) / std
                states[(buy.instrument_id, sell.instrument_id)] = SpreadState(
                    buy=buy,
                    sell=sell,
                    edge_bps=edge,
                    mean_bps=mean,
                    std_bps=std,
                    z_score=z_score,
                    samples=samples,
                )
        return states

    def _maybe_open(self, asset: str, states: dict[tuple[InstrumentId, InstrumentId], SpreadState]) -> None:
        last_close_ns = self.last_close_ns.get(asset)
        now_ns = self.clock.timestamp_ns()
        if last_close_ns is not None and now_ns - last_close_ns < self.cooldown_ns:
            return

        if self.spread_mode == "threshold":
            self._maybe_open_threshold(asset, now_ns)
            return

        candidates = [
            state
            for state in states.values()
            if self._can_open(state)
        ]
        if not candidates:
            return
        state = max(candidates, key=lambda item: item.z_score)
        self.positions[asset] = ArbPos(
            buy_id=state.buy.instrument_id,
            sell_id=state.sell.instrument_id,
            buy_px=state.buy.price,
            sell_px=state.sell.price,
            edge_bps=state.edge_bps,
            mean_bps=state.mean_bps,
            z_score=state.z_score,
            opened_ns=now_ns,
        )
        self.log.info(
            f"open {asset} edge={state.edge_bps:.2f}bps mean={state.mean_bps:.2f}bps "
            f"z={state.z_score:.2f} buy={state.buy.instrument_id}@{state.buy.price} "
            f"sell={state.sell.instrument_id}@{state.sell.price}",
        )
        if not self.config.dry_run:
            self._submit_pair(state.buy.instrument_id, OrderSide.BUY, state.buy.price)
            self._submit_pair(state.sell.instrument_id, OrderSide.SELL, state.sell.price)

    def _can_open(self, state: SpreadState) -> bool:
        if state.z_score < self.entry_z:
            return False
        if state.edge_bps < self.min_entry_bps:
            return False
        return not self.one_sided or state.edge_bps > 0

    def _maybe_open_threshold(self, asset: str, now_ns: int) -> None:
        best = self._best(asset)
        if best is None:
            return
        buy, sell = best
        net = self._net_bps(buy, sell)
        if net < self.entry_bps:
            return
        self.positions[asset] = ArbPos(
            buy_id=buy.instrument_id,
            sell_id=sell.instrument_id,
            buy_px=buy.price,
            sell_px=sell.price,
            edge_bps=net,
            mean_bps=float(net),
            z_score=0.0,
            opened_ns=now_ns,
        )
        self.log.info(
            f"open {asset} edge={net:.2f}bps buy={buy.instrument_id}@{buy.price} "
            f"sell={sell.instrument_id}@{sell.price}",
        )
        if not self.config.dry_run:
            self._submit_pair(buy.instrument_id, OrderSide.BUY, buy.price)
            self._submit_pair(sell.instrument_id, OrderSide.SELL, sell.price)

    def _maybe_close(self, asset: str, states: dict[tuple[InstrumentId, InstrumentId], SpreadState]) -> None:
        pos = self.positions[asset]
        buy_quote = self.quotes.get(pos.buy_id)
        sell_quote = self.quotes.get(pos.sell_id)
        if buy_quote is None or sell_quote is None:
            return
        now_ns = self.clock.timestamp_ns()
        if now_ns - buy_quote.ts_event > self.max_quote_age_ns:
            return
        if now_ns - sell_quote.ts_event > self.max_quote_age_ns:
            return

        age_ns = now_ns - pos.opened_ns
        expired = age_ns >= self.max_hold_ns
        if age_ns < self.min_hold_ns and not expired:
            return

        close_buy = ArbLeg(pos.sell_id, Decimal(str(sell_quote.ask_price)))
        close_sell = ArbLeg(pos.buy_id, Decimal(str(buy_quote.bid_price)))
        close_edge = self._net_bps(close_buy, close_sell)

        if self.spread_mode == "threshold":
            should_close = close_edge <= self.exit_bps or expired
            signal = f"close_edge={close_edge:.2f}bps"
        else:
            state = states.get((pos.buy_id, pos.sell_id))
            if state is None and not expired:
                return
            z_score = state.z_score if state is not None else float("nan")
            should_close = z_score <= self.exit_z or expired
            signal = f"edge_z={z_score:.2f} close_edge={close_edge:.2f}bps"

        if not should_close:
            return
        self.log.info(
            f"close {asset} {signal} expired={expired} "
            f"sell_long={pos.buy_id}@{close_sell.price} buy_short={pos.sell_id}@{close_buy.price}",
        )
        if not self.config.dry_run:
            self._submit_pair(pos.buy_id, OrderSide.SELL, close_sell.price)
            self._submit_pair(pos.sell_id, OrderSide.BUY, close_buy.price)
        self.last_close_ns[asset] = now_ns
        del self.positions[asset]

    def _submit_pair(self, instrument_id: InstrumentId, side: OrderSide, price: Decimal) -> None:
        instrument = self.cache.instrument(instrument_id)
        quantity = instrument.make_qty(self.notional / price)
        order = self.order_factory.market(
            instrument_id=instrument_id,
            order_side=side,
            quantity=quantity,
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)

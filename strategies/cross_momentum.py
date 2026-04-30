from __future__ import annotations

from collections import deque
from decimal import Decimal

import pandas as pd

from nautilus_trader.config import PositiveInt
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy


class CrossMomentumConfig(StrategyConfig, frozen=True):
    instrument_ids: list[InstrumentId]
    bar_types: list[BarType]
    trade_notional: Decimal
    benchmark_index: int = 0
    lookback_bars: PositiveInt = 3
    long_count: PositiveInt = 2
    short_count: PositiveInt = 2
    min_abs_score: float = 0.0
    request_bars: bool = True
    warmup_days: PositiveInt = 7
    close_positions_on_stop: bool = True


class CrossMomentum(Strategy):
    def __init__(self, config: CrossMomentumConfig) -> None:
        if len(config.instrument_ids) != len(config.bar_types):
            raise ValueError("instrument_ids and bar_types must have the same length")
        super().__init__(config)
        self.instruments: dict[InstrumentId, Instrument] = {}
        self.bar_to_instrument = dict(zip(config.bar_types, config.instrument_ids, strict=True))
        self.closes = {
            instrument_id: deque(maxlen=config.lookback_bars + 1)
            for instrument_id in config.instrument_ids
        }
        self.current_bar_ts = 0
        self.seen_this_ts: set[InstrumentId] = set()
        self.last_bar_ts = {instrument_id: 0 for instrument_id in config.instrument_ids}
        self.cache_warmed = False

    # 注册多个 bar 流，先请求历史 bar 预热，再订阅实时 bar。
    def on_start(self) -> None:
        for instrument_id in self.config.instrument_ids:
            instrument = self.cache.instrument(instrument_id)
            if instrument is None:
                self.log.error(f"Instrument not found: {instrument_id}")
                self.stop()
                return
            self.instruments[instrument_id] = instrument

        for bar_type in self.config.bar_types:
            if self.config.request_bars:
                self.request_bars(
                    bar_type,
                    start=self._clock.utc_now() - pd.Timedelta(days=self.config.warmup_days),
                )
            self.subscribe_bars(bar_type)

    # 收齐同一小时的多币种 bar 后，按相对 BTC 动量调仓。
    def on_bar(self, bar: Bar) -> None:
        self._warmup_from_cache()
        instrument_id = self.bar_to_instrument.get(bar.bar_type)
        if instrument_id is None:
            return

        if bar.ts_event != self.current_bar_ts:
            self.current_bar_ts = bar.ts_event
            self.seen_this_ts.clear()

        self._append_close(bar)
        self.seen_this_ts.add(instrument_id)

        if len(self.seen_this_ts) < len(self.config.instrument_ids):
            return
        if not all(len(values) > self.config.lookback_bars for values in self.closes.values()):
            return

        self._rebalance()

    # 从 NT cache 读取历史 bars 做预热，不覆盖 NT 的历史数据回调。
    def _warmup_from_cache(self) -> None:
        if self.cache_warmed:
            return

        for bar_type in self.config.bar_types:
            if not self.cache.has_bars(bar_type):
                continue
            for cached_bar in self.cache.bars(bar_type):
                self._append_close(cached_bar)

        self.cache_warmed = all(
            len(values) > self.config.lookback_bars
            for values in self.closes.values()
        )

    # 把 bar 的收盘价写入本地 deque，并跳过重复或倒序历史数据。
    def _append_close(self, bar: Bar) -> None:
        instrument_id = self.bar_to_instrument.get(bar.bar_type)
        if instrument_id is None:
            return
        if bar.ts_event <= self.last_bar_ts[instrument_id]:
            return
        self.closes[instrument_id].append(float(bar.close))
        self.last_bar_ts[instrument_id] = bar.ts_event

    # 用每个币相对 BTC 的动量分数生成多空目标。
    def _rebalance(self) -> None:
        benchmark = self.config.instrument_ids[self.config.benchmark_index]
        benchmark_return = self._return(benchmark)
        scores = {
            instrument_id: self._return(instrument_id) - benchmark_return
            for instrument_id in self.config.instrument_ids
            if instrument_id != benchmark
        }

        ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
        longs = {
            instrument_id
            for instrument_id, score in ranked[: self.config.long_count]
            if abs(score) >= self.config.min_abs_score
        }
        shorts = {
            instrument_id
            for instrument_id, score in ranked[-self.config.short_count :]
            if abs(score) >= self.config.min_abs_score
        }

        for instrument_id in scores:
            if instrument_id in longs:
                self._target(instrument_id, OrderSide.BUY)
            elif instrument_id in shorts:
                self._target(instrument_id, OrderSide.SELL)
            else:
                self.close_all_positions(instrument_id)

    # 计算 lookback 区间收益率。
    def _return(self, instrument_id: InstrumentId) -> float:
        values = self.closes[instrument_id]
        return values[-1] / values[0] - 1.0

    # 把单个 instrument 调整到目标方向。
    def _target(self, instrument_id: InstrumentId, side: OrderSide) -> None:
        if side == OrderSide.BUY:
            if self.portfolio.is_net_long(instrument_id):
                return
            if self.portfolio.is_net_short(instrument_id):
                self.close_all_positions(instrument_id)
            self._market(instrument_id, side)
            return

        if self.portfolio.is_net_short(instrument_id):
            return
        if self.portfolio.is_net_long(instrument_id):
            self.close_all_positions(instrument_id)
        self._market(instrument_id, side)

    # 按固定名义金额换算数量后提交市价单。
    def _market(self, instrument_id: InstrumentId, side: OrderSide) -> None:
        instrument = self.instruments[instrument_id]
        last_price = Decimal(str(self.closes[instrument_id][-1]))
        quantity = instrument.make_qty(self.config.trade_notional / last_price)
        order = self.order_factory.market(
            instrument_id=instrument_id,
            order_side=side,
            quantity=quantity,
            time_in_force=TimeInForce.GTC,
        )
        suffix = "LONG" if side == OrderSide.BUY else "SHORT"
        self.submit_order(order, PositionId(f"{instrument_id}-{suffix}"))

    # 停止时取消订单、按配置平仓并取消订阅。
    def on_stop(self) -> None:
        for instrument_id in self.config.instrument_ids:
            self.cancel_all_orders(instrument_id)
            if self.config.close_positions_on_stop:
                self.close_all_positions(instrument_id)
        for bar_type in self.config.bar_types:
            self.unsubscribe_bars(bar_type)

    # 重置本地行情缓存。
    def on_reset(self) -> None:
        for values in self.closes.values():
            values.clear()
        self.current_bar_ts = 0
        self.seen_this_ts.clear()
        for instrument_id in self.last_bar_ts:
            self.last_bar_ts[instrument_id] = 0
        self.cache_warmed = False

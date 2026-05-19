from __future__ import annotations

from decimal import Decimal

from nautilus_trader.common.events import TimeEvent
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from utils.arguments import NODE_STOP_TOPIC


class PolyTestConfig(StrategyConfig, frozen=True):
    instrument_ids: list[InstrumentId]
    bar_types: list[BarType]
    labels: list[str]
    max_ticks: int
    timeout_sec: int


class PolyTestStrategy(Strategy):
    def __init__(self, config: PolyTestConfig) -> None:
        super().__init__(config)
        self.quote_count = 0
        self.trade_count = 0
        self.stopped = False
        self.labels = dict(zip(config.instrument_ids, config.labels, strict=True))

    # 启动后订阅 Polymarket quote/trade ticks。
    def on_start(self) -> None:
        self._log_account()
        for instrument_id in self.config.instrument_ids:
            self.subscribe_quote_ticks(instrument_id)
            self.subscribe_trade_ticks(instrument_id)
        self.clock.set_time_alert_ns(
            "polytest_timeout",
            self.clock.timestamp_ns() + int(self.config.timeout_sec * 1_000_000_000),
            callback=self._on_time,
        )
        labels = ", ".join(self.labels.values())
        self.log.info(f"polytest启动，订阅{len(self.config.instrument_ids)}个市场: {labels}")

    # 打印盘口 tick，达到数量后停止 node。
    def on_quote_tick(self, tick: QuoteTick) -> None:
        self.quote_count += 1
        spread = Decimal(str(tick.ask_price)) - Decimal(str(tick.bid_price))
        self.log.info(
            f"盘口 n={self.quote_count} market={self._label(tick.instrument_id)} "
            f"bid={tick.bid_price}x{tick.bid_size} ask={tick.ask_price}x{tick.ask_size} spread={spread}"
        )
        self._stop_if_done()

    # 打印成交 tick，达到数量后停止 node。
    def on_trade_tick(self, tick: TradeTick) -> None:
        self.trade_count += 1
        self.log.info(
            f"成交 n={self.trade_count} market={self._label(tick.instrument_id)} "
            f"price={tick.price} size={tick.size} side={tick.aggressor_side}"
        )
        self._stop_if_done()

    # 打印启动时已经加载到账户和持仓摘要。
    def _log_account(self) -> None:
        accounts = list(self.cache.accounts())
        positions = list(self.cache.positions_open())
        if not accounts:
            self.log.info(f"账户摘要 accounts=0 open_positions={len(positions)}，exec 账户可能尚未完成加载")
            return
        for account in accounts:
            self.log.info(
                f"账户摘要 account={account.id} type={account.type} "
                f"base={account.base_currency} open_positions={len(positions)} "
                f"balances={account.balances_total()}"
            )

    # 返回短市场名，避免日志里刷长 condition/token。
    def _label(self, instrument_id: InstrumentId) -> str:
        return self.labels[instrument_id]

    # 超时也停止，避免 smoke test 长时间挂住。
    def _on_time(self, _event: TimeEvent) -> None:
        self.log.info(f"polytest超时停止，quote_ticks={self.quote_count}, trade_ticks={self.trade_count}")
        self._request_stop()

    # 达到 tick 数量就请求 live node 停止。
    def _stop_if_done(self) -> None:
        if self.quote_count + self.trade_count >= self.config.max_ticks:
            self._request_stop()

    # 通过 live.py 注册的控制 topic 停止 node。
    def _request_stop(self) -> None:
        if self.stopped:
            return
        self.stopped = True
        self.msgbus.publish(NODE_STOP_TOPIC, {"source": "polytest"})

    # 停止时取消订阅。
    def on_stop(self) -> None:
        for instrument_id in self.config.instrument_ids:
            self.unsubscribe_quote_ticks(instrument_id)
            self.unsubscribe_trade_ticks(instrument_id)

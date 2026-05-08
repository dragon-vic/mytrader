from __future__ import annotations

from decimal import Decimal

from external.data_engine import EXTERNAL_SIGNAL_CLIENT_NAME
from external.data_engine import ExternalSignal
from external.data_engine import external_signal_type
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import PositionEvent
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy


NODE_STOP_TOPIC = "controls.node.stop"


class OrcaShortConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_notional: Decimal
    use_trade_ticks: bool = True
    close_positions_on_stop: bool = True


class OrcaShort(Strategy):
    def __init__(self, config: OrcaShortConfig) -> None:
        super().__init__(config)
        self.instrument: Instrument | None = None
        self.last_price: Decimal | None = None
        self.cleaned_existing_position = False
        self.short_submitted = False
        self.stopping_from_external = False

    # 启动时订阅行情和外部信号，真正下单等第一笔行情到达后执行。
    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            raise RuntimeError(f"Instrument not found: {self.config.instrument_id}")

        if self.config.use_trade_ticks:
            self.subscribe_trade_ticks(self.config.instrument_id)
        else:
            self.subscribe_bars(self.config.bar_type)
        self.subscribe_data(external_signal_type(), client_id=ClientId(EXTERNAL_SIGNAL_CLIENT_NAME))
        self.log.info(f"OrcaShort started: notional={self.config.trade_notional}")

    # tick 到达后更新参考价，并推动启动清仓和开空流程。
    def on_trade_tick(self, tick: TradeTick) -> None:
        self.last_price = Decimal(str(tick.price))
        self._run_entry_flow()

    # bar 模式用于回测或没有 tick 的场景，逻辑和 tick 一样。
    def on_bar(self, bar: Bar) -> None:
        self.last_price = Decimal(str(bar.close))
        self._run_entry_flow()

    # 外部信号到达后先平仓，仓位干净后请求 live.py 停止 node。
    def on_data(self, data: ExternalSignal) -> None:
        self.log.info(f"External signal received, stopping strategy: {data}")
        self.stopping_from_external = True
        self.cancel_all_orders(self.config.instrument_id)
        self._close_open_positions()
        if not self._open_positions():
            self._request_node_stop()

    # 清仓成交后，如果已经干净并且还没开目标空单，就继续开空。
    def on_position_event(self, event: PositionEvent) -> None:
        self.log.info(f"position_event={event}")
        if self.stopping_from_external:
            if not self._open_positions():
                self._request_node_stop()
            return
        if self.cleaned_existing_position and not self.short_submitted and not self._open_positions():
            self._submit_short()

    # 第一次进入时先把旧仓位清干净，确认空仓后再开 1000U 空单。
    def _run_entry_flow(self) -> None:
        if self.stopping_from_external or self.short_submitted:
            return
        if not self.cleaned_existing_position:
            self.cancel_all_orders(self.config.instrument_id)
            self.cleaned_existing_position = True
            if self._open_positions():
                self.log.info("Existing ORCA position found, closing before opening target short.")
                self._close_open_positions()
                return
            self.log.info("No existing ORCA position found, opening target short.")
        if not self._open_positions():
            self._submit_short()

    # 从 NT cache 读取当前未平仓仓位，避免 portfolio 启动同步期状态不一致。
    def _open_positions(self) -> list:
        return self.cache.positions_open(instrument_id=self.config.instrument_id)

    # 显式关闭 cache 里的 ORCA 仓位，包括不是本策略打开的启动前旧仓。
    def _close_open_positions(self) -> None:
        for position in self._open_positions():
            self.log.info(
                f"Closing cached ORCA position: id={position.id}, "
                f"side={position.side}, qty={position.quantity}, strategy_id={position.strategy_id}"
            )
            self.close_position(position, reduce_only=False)

    # 向 NT msgbus 发布统一停止请求，让 live.py 处理 node 生命周期。
    def _request_node_stop(self) -> None:
        self.log.info("Requesting TradingNode stop.")
        self.msgbus.publish(
            NODE_STOP_TOPIC,
            {
                "strategy_id": str(self.id),
                "reason": "external_signal",
                "ts_event": self.clock.timestamp_ns(),
            },
        )

    # 按最新价把 1000U 名义金额换算成 ORCA 数量并提交市价空单。
    def _submit_short(self) -> None:
        if self.last_price is None:
            raise RuntimeError("Cannot submit short before receiving market price")
        if self.instrument is None:
            raise RuntimeError(f"Instrument not found: {self.config.instrument_id}")

        quantity = self.instrument.make_qty(self.config.trade_notional / self.last_price)
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.SELL,
            quantity=quantity,
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)
        self.short_submitted = True
        self.log.info(
            f"Submitted ORCA short: notional={self.config.trade_notional}, "
            f"price={self.last_price}, quantity={quantity}"
        )

    # 停止时撤单，并按配置平掉策略持仓。
    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        if self.config.close_positions_on_stop:
            self._close_open_positions()
        if self.config.use_trade_ticks:
            self.unsubscribe_trade_ticks(self.config.instrument_id)
        else:
            self.unsubscribe_bars(self.config.bar_type)
        self.unsubscribe_data(external_signal_type(), client_id=ClientId(EXTERNAL_SIGNAL_CLIENT_NAME))

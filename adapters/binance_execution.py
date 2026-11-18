"""保留未知订单，查询真实成交后再经NT报告链路更新订单和持仓。"""
from decimal import Decimal

from nautilus_trader.adapters.binance.common.enums import BinanceKeyType
from nautilus_trader.adapters.binance.factories import (
    BinanceLiveExecClientFactory, _resolve_environment, get_api_key, get_api_secret,
    get_cached_binance_http_client, get_cached_binance_futures_instrument_provider, get_ws_base_url,
)
from nautilus_trader.adapters.binance.futures.execution import BinanceFuturesExecutionClient
from nautilus_trader.adapters.binance.http.error import BinanceError
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.objects import Quantity

from utils.execution import OrderResolution, uncertain_order_error


class ConfirmedBinanceFuturesClient(BinanceFuturesExecutionClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.order_queries = set()
        self.uncertain_orders = set()
        self.submit_errors = {}
        self.native_submit_methods = self._submit_order_method
        self._submit_order_method = {kind: self._submit_with_error for kind in self.native_submit_methods}

    async def _submit_with_error(self, order, position_side, price_match, close_position):
        try:
            await self.native_submit_methods[order.order_type](order, position_side, price_match, close_position)
            self.submit_errors.pop(order.client_order_id, None)
        except Exception as exc:
            self.submit_errors[order.client_order_id] = exc
            raise

    def generate_order_rejected(self, strategy_id, instrument_id, client_order_id, reason,
                                ts_event, due_post_only=False):
        error = self.submit_errors.pop(client_order_id, None)
        status = error.status if isinstance(error, BinanceError) else None
        detail = error.message if isinstance(error, BinanceError) else reason
        if not due_post_only and uncertain_order_error(detail, status):
            if client_order_id not in self.uncertain_orders:
                self._log.warning(f'ORDER_UNRESOLVED client_order_id={client_order_id}')
                self.uncertain_orders.add(client_order_id)
            return
        super().generate_order_rejected(strategy_id, instrument_id, client_order_id, reason,
                                        ts_event, due_post_only)

    async def generate_order_status_report(self, command):
        # 查询失败保持未知，禁止使用NT查询重试耗尽后生成拒单的路径。
        try:
            row = await self._http_account.query_order(
                symbol=command.instrument_id.symbol.value,
                orig_client_order_id=command.client_order_id.value if command.client_order_id else None,
                order_id=int(command.venue_order_id.value) if command.venue_order_id else None)
            if row is None:
                return None
            return row.parse_to_order_status_report(
                account_id=self.account_id, instrument_id=command.instrument_id, report_id=UUID4(),
                enum_parser=self._enum_parser, treat_expired_as_canceled=self._treat_expired_as_canceled,
                ts_init=self._clock.timestamp_ns())
        except Exception as exc:
            self._log.warning(f'ORDER_QUERY_UNRESOLVED client_order_id={command.client_order_id} error={type(exc).__name__}')
            return None

    async def _query_order(self, command):
        key = command.client_order_id or command.venue_order_id
        if key in self.order_queries:
            return
        self.order_queries.add(key)
        try:
            report = await self.generate_order_status_report(command)
            if report is None:
                return
            order = self._cache.order(command.client_order_id)
            if order is None:
                return
            if report.filled_qty.as_decimal() > order.filled_qty.as_decimal():
                rows = await self._http_account.query_user_trades(
                    symbol=command.instrument_id.symbol.value, order_id=int(report.venue_order_id.value), limit=1000)
                rows = [row for row in rows if str(row.orderId) == report.venue_order_id.value]
                if sum((Decimal(row.qty) for row in rows), Decimal(0)) != report.filled_qty.as_decimal():
                    self._log.warning(f'ORDER_FILLS_INCOMPLETE client_order_id={command.client_order_id}')
                    return
                # 先建立交易所订单映射，真实trade_id成交依次入账，最后确认终态。
                if order.venue_order_id is None:
                    accepted = OrderStatusReport(account_id=report.account_id, instrument_id=report.instrument_id,
                        client_order_id=report.client_order_id, venue_order_id=report.venue_order_id,
                        order_side=report.order_side, order_type=report.order_type, time_in_force=report.time_in_force,
                        order_status=OrderStatus.ACCEPTED, quantity=report.quantity,
                        filled_qty=Quantity(0, report.filled_qty.precision), report_id=UUID4(),
                        ts_accepted=report.ts_accepted, ts_last=report.ts_last, ts_init=self._clock.timestamp_ns())
                    self._send_order_status_report(accepted)
                for row in sorted(rows, key=lambda trade: (trade.time, trade.id)):
                    fill = row.parse_to_fill_report(self.account_id, command.instrument_id, UUID4(),
                                                    self._clock.timestamp_ns(), self._use_position_ids)
                    fill.client_order_id = command.client_order_id
                    self._send_fill_report(fill)
            if order.filled_qty.as_decimal() != report.filled_qty.as_decimal():
                self._log.warning(f'ORDER_ACCOUNTING_UNRESOLVED client_order_id={command.client_order_id}')
                return
            self._send_order_status_report(report)
            if report.order_status in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED):
                self._msgbus.publish(f'events.order_resolution.{order.strategy_id}', OrderResolution(
                    str(command.client_order_id), report.order_status.name, report.filled_qty.as_decimal()))
            self.uncertain_orders.discard(command.client_order_id)
            self.submit_errors.pop(command.client_order_id, None)
        except Exception as exc:
            self.last_reconciliation_error = type(exc).__name__
            self._log.warning(f'ORDER_RECONCILIATION_PENDING client_order_id={command.client_order_id} error={type(exc).__name__}')
        finally:
            self.order_queries.discard(key)


class ConfirmedBinanceExecFactory(BinanceLiveExecClientFactory):
    @staticmethod
    def create(loop, name, config, msgbus, cache, clock):
        if config.account_type.is_spot_or_margin:
            return BinanceLiveExecClientFactory.create(loop, name, config, msgbus, cache, clock)
        if config.key_type == BinanceKeyType.RSA:
            raise ValueError('Binance execution requires HMAC or Ed25519 keys')
        environment = _resolve_environment(config.environment)
        key = config.api_key or get_api_key(config.account_type, environment)
        secret = config.api_secret or get_api_secret(config.account_type, environment)
        client = get_cached_binance_http_client(clock=clock, account_type=config.account_type,
            api_key=key, api_secret=secret, key_type=config.key_type, base_url=config.base_url_http,
            environment=environment, is_us=config.us, proxy_url=config.proxy_url)
        provider = get_cached_binance_futures_instrument_provider(client=client, clock=clock,
            account_type=config.account_type, config=config.instrument_provider, venue=config.venue)
        return ConfirmedBinanceFuturesClient(loop=loop, name=name, config=config, msgbus=msgbus,
            cache=cache, clock=clock, client=client, instrument_provider=provider,
            base_url_ws=config.base_url_ws or get_ws_base_url(config.account_type, environment, config.us),
            account_type=config.account_type, environment=environment, api_key=key, api_secret=secret)

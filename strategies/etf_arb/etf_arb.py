from __future__ import annotations

import asyncio
import json
import math
import queue
import threading
from collections import deque
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path

from nautilus_trader.adapters.binance.futures.types import BinanceFuturesMarkPriceUpdate
from nautilus_trader.model.data import Bar, BarType, CustomData, DataType
from nautilus_trader.model.enums import BookType, OrderSide
from nautilus_trader.model.identifiers import ClientId, ClientOrderId
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.actor import Actor
from adapters.binance import market_order_limits
from utils.constants import NODE_STOP_TOPIC
from utils.live_control import NodeStopRequest

from .baseline import Baseline, DAY, FairValue, HOUR, MINUTE, MarketCalendar
from .config import EtfArbConfig, PairConfig
from .market import Market, entry_gap, exit_gap, sweep
from .runtime_lock import RuntimeLock
from .recorder import EventRecorder
from .close_reference import read_cache, refresh as refresh_references
from .state import read_state, write_state
from .capital import CapitalReader, account_fill, capital_headroom
from .special_events import VenueMonitor, load_events, specification


@dataclass
class Stage:
    pair: str
    side: int
    fair: FairValue
    started_ns: int
    close_ns: int
    budget: float
    batches: int = 0
    phase: str = 'opening'
    quantities: list[float] = field(default_factory=lambda: [0., 0.])
    exit_reason: str = ''
    exit_started_ns: int = 0
    repair_deadline_ns: int = 0
    next_attempt_ns: int = 0
    pause_after_exit: bool = False
    available_at_start: float = 0.
    capital_checked_ns: int = 0
    average_prices: list[float] = field(default_factory=lambda: [0., 0.])
    realized_pnl: float = 0.
    fees: float = 0.
    reconciled_epoch_ns: int = 0
    native_limits: list[float | None] = field(default_factory=lambda: [None, None])
    adding_error: str = ''
    native_leverages: list[float] = field(default_factory=lambda: [1., 1.])


@dataclass
class Pending:
    instrument: str
    leg: int
    quantity: float
    sent_ns: int
    action: str
    filled: float = 0.
    terminal: bool = False
    checked_ns: int = 0
    expected_price: float | None = None
    reserved_margin: float = 0.


@dataclass
class ExecutionPlan:
    quantities: list[float]
    expected_prices: list[float]
    observed_gap: float
    expected_gap: float
    requested_gross: float
    expected_gross: float


class EtfArbStrategy(Strategy):
    def __init__(self, config: EtfArbConfig):
        super().__init__(config)
        self.pairs = {p.name: p for p in config.pairs}
        self.by_instrument = {}
        for pair in config.pairs:
            for instrument in (pair.etf, pair.underlying):
                self.by_instrument.setdefault(instrument, []).append(pair.name)
        self.markets = {iid: Market(iid) for iid in self.by_instrument}
        self.instruments = {}
        self.market_limits = {}
        self.baselines = {}
        self.calendar = None
        self.stage: Stage | None = None
        self.orders: dict[str, Pending] = {}
        self.confirmations: dict[str, tuple[int, FairValue, int]] = {}
        self.paused: set[str] = set()
        self.ready = False
        self.dispatching = False
        self.stopping = False
        self.exiting = False
        self.state_path = Path(config.state_path)
        self.audit = None
        self.runtime_lock = None
        self.state_failed = False
        self.startup_failed = False
        self.fault_reason = ''
        self.node_stop_requested = False
        self.owner_thread = None
        self.event_loop = None
        self.driving = False
        self.seen_fills = set()
        self.bars: dict = {iid: {} for iid in self.markets}
        self.bootstrap_remaining = set()
        self.bootstrap_requested = {}
        self.bootstrap_active = None
        self.bootstrap_next_ns = 0
        self.segment_minute = -1
        self.segment_now = None
        self.blocked_minute = -1
        self.calendar_blocked = True
        self.data_client = ClientId(config.data_client)
        self.exec_client = ClientId(config.exec_client)
        self.stats = {'quotes': 0, 'depth': 0, 'confirmations': 0, 'stages': 0, 'batches': 0}
        self.funding_mailbox = queue.SimpleQueue()
        self.background_stop = threading.Event()
        self.background_worker = None
        self.reference_worker = None
        self.reference_mailbox = queue.SimpleQueue()
        self.reference_errors = {}
        self.started_ns = 0
        self.last_health_ns = 0
        self.quote_ages_ms = deque(maxlen=4096)
        self.recorder = None
        self.capture_above = {name: False for name in self.pairs}
        self.last_capture_state_ns = 0
        self.capture_issue = None
        self.last_capture_issue_ns = 0
        self.add_wait_reason = None
        self.capital_reader = None
        self.capital_token = 0
        self.capital_requested_ns = 0
        self.capital_retry_ns = 0
        self.capital_inflight = False
        self.capital_snapshot = None
        self.last_capital_snapshot = None
        self.capital_error = None
        self.capital_purpose = 'new_stage'
        self.venue_monitor = None
        self.venue_checked_ns = 0
        self.venue_error = None
        self.venue_rows = {}
        self.venue_specs_match = {}
        self.venue_recovered_ns = {}
        self.events = ()
        self.pair_epochs = {name: 0 for name in self.pairs}

    def on_start(self):
        self.owner_thread = threading.get_ident()
        if isinstance(self.clock, LiveClock):
            self.event_loop = asyncio.get_running_loop()
        try:
            self._start_strategy()
        except Exception as exc:
            self.startup_failed = True
            self.fault_reason = f'startup_failed:{type(exc).__name__}:{exc}'
            self.stopping = True
            self.background_stop.set()
            self.log.exception('ETF startup failed', exc)
            self._record('startup_failed', reason=self.fault_reason)
            self._close_resources()
            # 让NT先完成STARTING状态，再按协议停止，保留原始检查点供排查。
            if self.event_loop is not None:
                self.event_loop.call_soon(self._abort_startup)
            else:
                self.clock.set_time_alert_ns('etf.startup_abort', self.clock.timestamp_ns() + 1,
                                             callback=self._abort_startup)

    def _abort_startup(self, _event=None):
        self._assert_thread()
        Actor.stop(self)
        self._request_node_stop()

    def _start_strategy(self):
        now = self.clock.timestamp_ns()
        self.started_ns = now
        self.calendar = MarketCalendar(now)
        self.baselines = {p.name: Baseline(p.multiple, self.calendar, self.config) for p in self.pairs.values()}
        for iid in self.markets:
            instrument = self.cache.instrument(iid)
            if instrument is None:
                raise RuntimeError(f'missing instrument {iid}')
            self.instruments[iid] = instrument
            self.market_limits[iid] = market_order_limits(instrument)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.runtime_lock = RuntimeLock(self.state_path.with_suffix('.lock'))
        self.audit = Path(self.config.audit_path).open('a', encoding='utf-8', buffering=1)
        self.recorder = EventRecorder(Path(self.config.audit_path).parent / 'capture', self.markets, self.config,
                                      {'strategy_config': json.loads(self.config.json()),
                                       'strategy_id': str(self.id), 'source': 'NT strategy callbacks',
                                       'instruments': {str(i): {'price_increment': str(v.price_increment),
                                                               'size_increment': str(v.size_increment),
                                                               'min_quantity': str(v.min_quantity),
                                                               'min_notional': str(v.min_notional)}
                                                       for i, v in self.instruments.items()},
                                       'price_encoding': 'float64; instrument precision in exchange specification'})
        self._recover()
        symbols = [self._symbol(iid) for iid in self.markets]
        events = load_events(self.config.special_events_path, symbols)
        self._validate_events(events, now)
        self.events = events
        self._reset_event_baselines(now)
        self._save()
        if self.config.bootstrap:
            self._apply_references(read_cache(self.config.reference_path))
        self.capital_reader = CapitalReader(self.config.account_environment, self._capital_wakeup)
        self.capital_reader.worker.start()
        self.venue_monitor = VenueMonitor(self.config.special_events_path, symbols, self.config.account_environment,
                                           self.config.venue_poll_seconds)
        self.venue_monitor.worker.start()
        self.bootstrap_remaining = set(self.markets) if self.config.bootstrap else set()
        for iid in self.markets:
            self.subscribe_quote_ticks(iid, client_id=self.data_client)
            self.subscribe_order_book_deltas(iid, BookType.L2_MBP, depth=5,
                                             client_id=self.data_client, params={'update_speed': 100})
            self.subscribe_data(DataType(BinanceFuturesMarkPriceUpdate, metadata={'instrument_id': iid}),
                                client_id=self.data_client)
            bar_type = BarType.from_str(f'{iid}-1-MINUTE-LAST-EXTERNAL')
            self.subscribe_bars(bar_type, client_id=self.data_client)
        self._pump_bootstrap(now)
        self.ready = not self.bootstrap_remaining
        if self.config.bootstrap:
            self.background_worker = threading.Thread(target=self._funding_history, name='etf-funding-history', daemon=True)
            self.background_worker.start()
            self.reference_worker = threading.Thread(target=self._reference_history, name='etf-close-reference', daemon=True)
            self.reference_worker.start()
        self.clock.set_timer_ns('etf.watchdog', self.config.watchdog_ms * 1_000_000, now, 0,
                                callback=self._timer_event)
        self._record('started', pairs=list(self.pairs), recovered=self.stage is not None)

    def _assert_thread(self):
        if self.owner_thread != threading.get_ident():
            raise RuntimeError('ETF state mutation outside its event loop thread')

    # Rust LiveClock在线程池触发，先回到NT事件循环再访问状态。
    def _timer_event(self, event):
        if self.stopping:
            return
        if self.event_loop is not None:
            if not self.event_loop.is_closed():
                try:
                    self.event_loop.call_soon_threadsafe(self._handle_timer, event)
                except RuntimeError:
                    if not self.stopping:
                        raise
        else:
            self._handle_timer(event)

    def _handle_timer(self, event):
        self._assert_thread()
        if self.stopping:
            return
        try:
            if event.name == 'etf.watchdog':
                self._watchdog(event)
            else:
                name, _, token = event.name.removeprefix('etf.confirm.').rpartition('.')
                candidate = self.confirmations.get(name)
                if candidate and token == str(candidate[2]):
                    self._confirm(name)
        except Exception as exc:
            self.log.exception('ETF timer callback failed', exc)
            self._fail_strategy(f'timer_error:{type(exc).__name__}')

    def _fail_strategy(self, reason):
        self._assert_thread()
        if not self.fault_reason:
            self.fault_reason = reason
            self.confirmations.clear()
            self._record('strategy_fault', reason=reason)
        if self.stopping:
            return
        if self.stage:
            self._exit(reason, pause=True)
        else:
            self._request_node_stop()

    def _request_node_stop(self):
        if self.node_stop_requested:
            return
        self.node_stop_requested = True
        self._record('node_stop_requested', reason=self.fault_reason)
        self.msgbus.publish(NODE_STOP_TOPIC, NodeStopRequest(source='etf_arb', reason=self.fault_reason))

    def _apply_references(self, snapshot):
        self.reference_errors = snapshot.get('errors', {})
        for name, data in snapshot.get('pairs', {}).items():
            if name in self.baselines:
                epoch = self.pair_epochs[name]
                rows = [r for r in data['rows'] if self._reference_after(r, epoch)]
                self.baselines[name].set_references(rows, data['checked_ns'])

    def _reference_after(self, row, epoch):
        if not epoch:
            return True
        import pandas as pd
        session = self.calendar.days.get(pd.Timestamp(row['date']).date())
        return session is not None and session[0] >= epoch

    def _symbol(self, iid):
        return str(iid.symbol).removesuffix('-PERP')

    def _capital_wakeup(self):
        if self.event_loop is not None and not self.event_loop.is_closed() and not self.stopping:
            try:
                self.event_loop.call_soon_threadsafe(self._receive_capital)
            except RuntimeError:
                if not self.stopping:
                    raise

    def _request_capital(self, now, purpose='new_stage'):
        if self.capital_inflight or now < self.capital_retry_ns:
            return
        if (purpose == 'new_stage' and self.capital_snapshot is not None and
                now - self.capital_requested_ns < self.config.capital_timeout_ms * 1_000_000):
            return
        if not self.capital_reader.worker.is_alive():
            raise RuntimeError('native capital worker stopped')
        self.capital_token += 1
        self.capital_snapshot = None
        self.capital_error = None
        self.capital_inflight = True
        self.capital_requested_ns = now
        self.capital_purpose = purpose
        self.capital_reader.request(self.capital_token)
        self._record('capital_requested', token=self.capital_token, purpose=purpose)

    def _receive_capital(self):
        self._assert_thread()
        if self.stopping or self.capital_reader is None:
            return
        while not self.capital_reader.results.empty():
            token, snapshot, error = self.capital_reader.results.get_nowait()
            if token != self.capital_token:
                continue
            if (error is None and self.capital_purpose == 'new_stage' and
                    self.clock.timestamp_ns() - self.capital_requested_ns >= self.config.capital_timeout_ms * 1_000_000):
                snapshot, error = None, 'capital_read_timeout'
            self.capital_inflight = False
            self.capital_snapshot, self.capital_error = snapshot, error
            if error:
                self.capital_retry_ns = self.clock.timestamp_ns() + 5_000_000_000
                self._record('capital_error', reason=error, token=token)
            else:
                self.last_capital_snapshot = snapshot
                self._record('capital_updated', token=token, available=snapshot.available, wallet=snapshot.wallet,
                             requested_ns=snapshot.requested_ns, received_ns=snapshot.received_ns)
                if self.capital_purpose == 'event_reconciliation' and self.stage and not self._pending():
                    positions = [snapshot.positions.get(self._symbol(iid), {}) for iid in self._ids(self.stage.pair)]
                    if not any(float(p.get('openOrderInitialMargin', 0)) for p in positions):
                        quantities = [float(p.get('positionAmt', 0)) for p in positions]
                        cached = self._position_quantities(self.stage.pair)
                        if any(abs(actual - local) >= float(self.instruments[iid].size_increment) / 2
                               for actual, local, iid in zip(quantities, cached, self._ids(self.stage.pair))):
                            self.stage.adding_error = 'event_position_cache_reconciliation_required'
                            self.capital_retry_ns = self.clock.timestamp_ns() + 30_000_000_000
                            self._record('event_position_mismatch', pair=self.stage.pair, native=quantities, cached=cached)
                            self._save()
                            continue
                        self.stage.quantities = quantities
                        self.stage.average_prices = [float(p.get('entryPrice', 0)) for p in positions]
                        self.stage.reconciled_epoch_ns = self.pair_epochs[self.stage.pair]
                        self._record('event_positions_reconciled', pair=self.stage.pair,
                                     quantities=self.stage.quantities, epoch_ns=self.stage.reconciled_epoch_ns)
                        self._save()
                        self.capital_snapshot = None
                        self._drive()
        now = self.clock.timestamp_ns()
        for name, (_, _, triggered) in list(self.confirmations.items()):
            if now - triggered >= self.config.confirmation_ms * 1_000_000:
                self._confirm(name)

    def _available_capital(self):
        if self.stage:
            return capital_headroom(self.stage, self.orders, [self.markets[i] for i in self._ids(self.stage.pair)])
        return self.capital_snapshot.available if self.capital_snapshot else None

    def _capital_status(self, now):
        latest = self.last_capital_snapshot
        stamp = self.stage.capital_checked_ns if self.stage else (
            latest.received_ns if latest else None)
        return {'source': 'Binance REST /fapi/v2/account USDT availableBalance',
                'checked_ns': stamp, 'age_seconds': (now - stamp) / 1e9 if stamp else None,
                'stage_available_at_start': self.stage.available_at_start if self.stage else None,
                'available_at_check': latest.available if latest else None,
                'local_headroom': self._available_capital(), 'inflight': self.capital_inflight,
                'worker_alive': self.capital_reader.worker.is_alive() if self.capital_reader else False,
                'error': self.capital_error, 'refresh_policy': 'once_per_stage',
                'realized_pnl': self.stage.realized_pnl if self.stage else None,
                'fees': self.stage.fees if self.stage else None}

    def _validate_events(self, events, now):
        new = {e.id: e for e in events}
        for event in self.events:
            if event.block_ns > now:
                continue
            replacement = new.get(event.id)
            if (replacement is None or replacement.block_ns != event.block_ns or replacement.symbols != event.symbols or
                    replacement.flatten_ns != event.flatten_ns or
                    (event.reset_ns is not None and replacement.reset_ns != event.reset_ns)):
                raise ValueError('active_event_removed_or_rewritten')

    def _reset_event_baselines(self, now):
        for name, pair in self.pairs.items():
            symbols = {self._symbol(i) for i in self._ids(name)}
            epoch = max([self.pair_epochs[name]] + [e.reset_ns for e in self.events
                        if e.reset_ns is not None and now >= e.reset_ns and symbols.intersection(e.symbols)])
            if epoch > self.pair_epochs[name]:
                self.pair_epochs[name] = epoch
                self.baselines[name] = Baseline(pair.multiple, self.calendar, self.config)
                self.confirmations.pop(name, None)
                self._record('event_baseline_reset', pair=name, epoch_ns=epoch)
                self._save()

    def _venue_reason(self, name, now):
        if self.venue_error or not self.venue_checked_ns or not 0 <= now - self.venue_checked_ns <= self.config.venue_max_age_seconds * 1_000_000_000:
            return 'venue_status_unavailable'
        for iid in self._ids(name):
            symbol = self._symbol(iid)
            row = self.venue_rows[symbol]
            if row['status'] != 'TRADING':
                return f'venue_status:{symbol}:{row["status"]}'
            if not self.venue_specs_match.get(symbol, False):
                return f'venue_spec_changed:{symbol}'
            delivery = int(row['deliveryDate']) * 1_000_000
            if now >= delivery - self.config.close_block_minutes * MINUTE:
                return f'venue_delivery:{symbol}'
            recovered = self.venue_recovered_ns.get(symbol, 0)
            if recovered and (now - recovered < 5_000_000_000 or self.markets[iid].quote_ns <= recovered):
                return f'venue_recovering:{symbol}'
        anchor = self.baselines[name].anchor
        epoch = self.pair_epochs[name]
        anchor_ns = anchor[0] if anchor and anchor[0] >= epoch + self.config.anchor_window_minutes * MINUTE else None
        return next((reason for event in self.events
                     if now >= event.block_ns and (reason := event.reason([self._symbol(i) for i in self._ids(name)], now,
                                                 anchor_ns))), None)

    def _poll_venue(self, now):
        if self.venue_monitor:
            while not self.venue_monitor.results.empty():
                checked, events, rows, error = self.venue_monitor.results.get_nowait()
                if error:
                    if error != self.venue_error:
                        self._record('venue_error', reason=error)
                    self.venue_error = error
                    continue
                try:
                    self._validate_events(events, now)
                except ValueError as exc:
                    self.venue_error = str(exc)
                    continue
                for symbol, row in rows.items():
                    before = self.venue_rows.get(symbol)
                    if before and before['status'] != 'TRADING' and row['status'] == 'TRADING':
                        self.venue_recovered_ns[symbol] = now
                    if before is None or row['status'] != before['status'] or specification(row) != specification(before):
                        self._record('venue_changed', symbol=symbol, status=row['status'], specification=specification(row))
                self.venue_specs_match = {self._symbol(iid): specification(rows[self._symbol(iid)]) == specification(instrument.info)
                                          for iid, instrument in self.instruments.items()}
                changed = events != self.events
                self.venue_checked_ns, self.events, self.venue_rows, self.venue_error = checked, events, rows, None
                if changed:
                    self._record('special_events_updated', events=[asdict(e) for e in self.events])
                    self._save()
        self._reset_event_baselines(now)
        if self.stage:
            symbols = {self._symbol(i) for i in self._ids(self.stage.pair)}
            event = next((e for e in self.events if now >= e.flatten_ns and symbols.intersection(e.symbols)
                          and e.reason(symbols, now, self.stage.fair.anchor_ns)), None)
            reason = self._venue_reason(self.stage.pair, now)
            if event:
                self._exit(f'special_event:{event.id}')
            elif reason and (reason.startswith('venue_spec_changed:') or reason.startswith('venue_delivery:')):
                self._exit(reason, pause=True)
            if (self.stage.exit_reason and not self._pending() and
                    self.pair_epochs[self.stage.pair] > self.stage.reconciled_epoch_ns):
                self._request_capital(now, purpose='event_reconciliation')

    def _reference_history(self):
        while not self.background_stop.is_set():
            try:
                snapshot = refresh_references(self.config.reference_path, self.config.pairs, self.background_stop)
                self.reference_mailbox.put((snapshot, None))
            except (OSError, ValueError) as exc:
                self.reference_mailbox.put((None, type(exc).__name__))
            if self.background_stop.wait(600):
                return

    # 补齐启动前最近一次资金费时点；HTTP始终在后台线程运行。
    def _funding_history(self):
        import requests
        with requests.Session() as session:
            for iid in self.markets:
                while not self.background_stop.is_set():
                    try:
                        response = session.get('https://fapi.binance.com/fapi/v1/fundingRate',
                                               params={'symbol': str(iid.symbol).removesuffix('-PERP'), 'limit': 1}, timeout=10)
                        response.raise_for_status()
                        rows = response.json()
                        if not rows:
                            raise ValueError('empty funding history')
                        self.funding_mailbox.put((iid, int(rows[-1]['fundingTime']) * 1_000_000, None))
                        break
                    except (requests.RequestException, ValueError, KeyError) as exc:
                        self.funding_mailbox.put((iid, None, type(exc).__name__))
                        if self.background_stop.wait(5):
                            return

    def on_historical_data(self, data):
        self._assert_thread()
        if isinstance(data, Bar):
            self._store_bar(data)

    def _request_history(self, iid, now):
        import pandas as pd
        self.bootstrap_requested[iid] = now
        self.bootstrap_active = iid
        self.bootstrap_next_ns = now + 15_000_000_000
        self.request_bars(BarType.from_str(f'{iid}-1-MINUTE-LAST-EXTERNAL'),
                          start=pd.Timestamp(now - (self.config.history_days + 2) * DAY, tz='UTC'),
                          end=pd.Timestamp(now // MINUTE * MINUTE - 1, tz='UTC'),
                          limit=499, client_id=self.data_client,
                          callback=lambda _request, instrument=iid: self._bootstrap_done(instrument))

    # 串行分页并限制新合约启动频率，给实时执行保留REST请求额度。
    def _pump_bootstrap(self, now):
        if not self.bootstrap_remaining:
            return
        if self.bootstrap_active is not None:
            if now - self.bootstrap_requested[self.bootstrap_active] >= 5 * MINUTE:
                self._record('bootstrap_retry', instrument=str(self.bootstrap_active))
                self._request_history(self.bootstrap_active, now)
        elif now >= self.bootstrap_next_ns:
            self._request_history(min(self.bootstrap_remaining, key=str), now)

    def _store_bar(self, bar: Bar):
        iid = bar.bar_type.instrument_id
        minute = bar.ts_event // MINUTE * MINUTE
        self.bars[iid][minute] = (float(bar.close), float(bar.volume) > 0)
        return iid, minute

    def _bootstrap_done(self, instrument):
        self._assert_thread()
        if instrument not in self.bootstrap_remaining:
            return
        self._record('bootstrap_instrument', instrument=str(instrument), bars=len(self.bars[instrument]))
        self.bootstrap_remaining.discard(instrument)
        self.bootstrap_active = None
        if self.bootstrap_remaining:
            return
        now = self.clock.timestamp_ns()
        for pair in self.pairs.values():
            etf, under = self.bars[pair.etf], self.bars[pair.underlying]
            baseline = self.baselines[pair.name]
            for minute in sorted(etf.keys() & under.keys()):
                if minute + MINUTE > now or minute < self.pair_epochs[pair.name]:
                    continue
                ep, ev = etf[minute]
                up, uv = under[minute]
                baseline.minute(minute, ep, up, ev and uv)
            if baseline.fair(now) is None:
                self._record('baseline_anchor_missing', pair=pair.name)
        self.bars = {iid: {t: v for t, v in values.items() if t >= now // MINUTE * MINUTE - MINUTE}
                     for iid, values in self.bars.items()}
        self.ready = True
        self._record('baseline_ready', baselines={name: b.describe()
                                                 for name, b in self.baselines.items()})

    def on_bar(self, bar: Bar):
        self._assert_thread()
        iid, minute = self._store_bar(bar)
        if not self.ready:
            return
        for name in self.by_instrument[iid]:
            if minute < self.pair_epochs[name]:
                continue
            pair = self.pairs[name]
            e, u = self.bars[pair.etf].get(minute), self.bars[pair.underlying].get(minute)
            if e and u:
                baseline = self.baselines[name]
                revision = baseline.revision
                baseline.minute(minute, e[0], u[0], e[1] and u[1])
                if revision != baseline.revision:
                    self._record('baseline_updated', pair=name, baseline=baseline.describe())
        for old in list(self.bars[iid]):
            if old < minute - 2 * MINUTE:
                del self.bars[iid][old]

    def on_quote_tick(self, tick):
        self._assert_thread()
        market = self.markets[tick.instrument_id]
        if tick.ts_event < market.quote_ns:
            return
        market.bid, market.ask = float(tick.bid_price), float(tick.ask_price)
        market.bid_size, market.ask_size = float(tick.bid_size), float(tick.ask_size)
        market.quote_ns = tick.ts_event
        now = self.clock.timestamp_ns()
        self.quote_ages_ms.append((now - tick.ts_event) / 1e6)
        self.stats['quotes'] += 1
        if self.recorder:
            self.recorder.market('q', tick.instrument_id, now, tick.ts_event, tick.ts_init,
                                 (market.bid, market.ask, market.bid_size, market.ask_size))
        self._market_changed(tick.instrument_id)

    def on_order_book_deltas(self, deltas):
        self._assert_thread()
        market = self.markets[deltas.instrument_id]
        if deltas.ts_event < market.depth_ns:
            return
        market.book.apply_deltas(deltas)
        market.depth_ns = deltas.ts_event
        self.stats['depth'] += 1
        if self.recorder:
            bids = tuple((float(level.price), float(level.size())) for level in market.book.bids()[:5])
            asks = tuple((float(level.price), float(level.size())) for level in market.book.asks()[:5])
            self.recorder.market('d', deltas.instrument_id, self.clock.timestamp_ns(), deltas.ts_event,
                                 deltas.ts_init, (bids, asks, deltas.sequence))
        if self.stage and deltas.instrument_id in self._ids(self.stage.pair):
            self._drive()

    def on_data(self, data):
        self._assert_thread()
        item = data.data if isinstance(data, CustomData) else data
        if not isinstance(item, BinanceFuturesMarkPriceUpdate):
            return
        market = self.markets[item.instrument_id]
        if item.ts_event < market.funding_seen_ns:
            return
        if market.funding_ns and item.next_funding_ns > market.funding_ns:
            market.last_funding_ns = market.funding_ns
            market.funding_history_ready = True
        market.funding_ns = item.next_funding_ns
        market.funding_seen_ns = item.ts_event
        market.funding_rate = float(item.funding_rate)
        market.mark = float(item.mark)
        if self.recorder:
            self.recorder.market('m', item.instrument_id, self.clock.timestamp_ns(), item.ts_event, item.ts_init,
                                 (float(item.mark), float(item.index), float(item.estimated_settle),
                                  market.funding_rate, market.funding_ns))

    # 信号采集独立于可交易性：禁开窗口、其他组合占用和100ms内消失的信号也保留。
    def _observe_capture(self, name, now, gap):
        above = gap >= self.config.entry_gap
        if above and name not in self.recorder.active:
            self.recorder.begin(name, self._ids(name), now, self._capture_state(name, now))
        if above != self.capture_above[name]:
            self.capture_above[name] = above
            self.recorder.event('threshold_cross' if above else 'threshold_below', now,
                                pair=name, gap=gap, state=self._capture_state(name, now))
        if name in self.recorder.active and (gap > self.config.exit_gap or not self._fresh(name, now)):
            self.recorder.touch(name, now)

    def _capture_state(self, name, now):
        stage = self.stage if self.stage and self.stage.pair == name else None
        fair = stage.fair if stage else self._fair(name, now)
        markets = [self.markets[i] for i in self._ids(name)]
        gaps = [entry_gap(fair, *markets, side) for side in (1, -1)] if fair else None
        target = min(self.config.exit_gap_cap, self.config.exit_gap +
                     ((now - stage.started_ns) // HOUR) * self.config.exit_hour_step) if stage else self.config.exit_gap
        return {'pair': name, 'time_ns': now, 'ready': self.ready, 'paused': name in self.paused,
                'active_pair': self.stage.pair if self.stage else None,
                'fair': asdict(fair) if fair else None, 'baseline_segment': self.baselines[name].segment_key,
                'entry_gaps': gaps, 'exit_gap': exit_gap(fair, *markets, stage.side) if stage else None,
                'exit_target': target, 'fresh': self._fresh(name, now),
                'calendar_blocked': self.calendar.entry_blocked(now, self.config),
                'funding_blocked': [m.funding_blocked(now, self.config.funding_block_minutes,
                                                     self.config.max_funding_age_ms) for m in markets],
                'available_usdt': self._available_capital(), 'capital': self._capital_status(now),
                'venue_reason': self._venue_reason(name, now), 'event_epoch_ns': self.pair_epochs[name],
                'confirmation': self.confirmations[name][2] if name in self.confirmations else None,
                'stage': asdict(stage) if stage else None,
                'pending': {k: asdict(v) for k, v in self.orders.items() if not v.terminal} if stage else {},
                'markets': [{'instrument': str(m.instrument_id), 'bid': m.bid, 'ask': m.ask,
                             'bid_size': m.bid_size, 'ask_size': m.ask_size, 'quote_ns': m.quote_ns,
                             'depth_ns': m.depth_ns, 'funding_seen_ns': m.funding_seen_ns,
                             'mark': m.mark,
                             'funding_rate': m.funding_rate, 'next_funding_ns': m.funding_ns,
                             'last_funding_ns': m.last_funding_ns} for m in markets]}

    def _decision(self, name, kind, **values):
        if self.recorder:
            now = self.clock.timestamp_ns()
            self.recorder.event(kind, now, pair=name, state=self._capture_state(name, now), **values)

    def _ids(self, name):
        pair = self.pairs[name]
        return pair.etf, pair.underlying

    def _fresh(self, name, now):
        return all(self.markets[iid].fresh(now, self.config.max_quote_age_ms) for iid in self._ids(name))

    def _blocked(self, name, now):
        minute = now // MINUTE
        if minute != self.blocked_minute:
            self.blocked_minute = minute
            self.calendar_blocked = self.calendar.entry_blocked(now, self.config)
        return bool(self._venue_reason(name, now)) or self.calendar_blocked or any(
            self.markets[iid].funding_blocked(now, self.config.funding_block_minutes, self.config.max_funding_age_ms)
            for iid in self._ids(name))

    def _fair(self, name, now):
        baseline = self.baselines[name]
        value = baseline.fair(now)
        if now // MINUTE != self.segment_minute:
            self.segment_minute = now // MINUTE
            self.segment_now = self.calendar.segment(now)
        if value is not None and baseline.segment_key != self.segment_now:
            value = FairValue(value.e0, value.u0, value.multiple, 0., value.anchor_ns)
        return value

    def _market_changed(self, iid):
        if self.stopping:
            return
        if not self.ready:
            if self.stage and iid in self._ids(self.stage.pair):
                self._drive()
            return
        now = self.clock.timestamp_ns()
        for name in self.by_instrument[iid]:
            fair = self.stage.fair if self.stage and self.stage.pair == name else self._fair(name, now)
            if fair is None:
                continue
            etf, under = (self.markets[i] for i in self._ids(name))
            if min(etf.bid, etf.ask, under.bid, under.ask) <= 0:
                continue
            plus, minus = entry_gap(fair, etf, under, 1), entry_gap(fair, etf, under, -1)
            if self.recorder:
                self._observe_capture(name, now, max(plus, minus))
            if self.stage or self.exiting or self.fault_reason or name in self.paused or name in self.confirmations or not self._fresh(name, now) or self._blocked(name, now):
                continue
            side = 1 if plus >= minus else -1
            if max(plus, minus) < self.config.entry_gap:
                continue
            if now < self.capital_retry_ns:
                continue
            if not self.confirmations:
                self._request_capital(now)
            self.confirmations[name] = side, fair, now
            self._decision(name, 'confirmation_started', side=side, deadline_ns=now + self.config.confirmation_ms * 1_000_000)
            self.clock.set_time_alert_ns(f'etf.confirm.{name}.{now}', now + self.config.confirmation_ms * 1_000_000,
                                         callback=self._timer_event)
        if self.stage and iid in self._ids(self.stage.pair):
            self._drive()

    def _confirm(self, name):
        self._assert_thread()
        candidate = self.confirmations.get(name)
        if candidate is None or self.stage or self.stopping or self.exiting or self.fault_reason or name in self.paused:
            self.confirmations.pop(name, None)
            if candidate:
                self._decision(name, 'confirmation_skipped', reason='stage_occupied_or_stopped_or_paused')
            return
        side, fair, triggered = candidate
        now = self.clock.timestamp_ns()
        if not self._fresh(name, now) or self._blocked(name, now) or self._fair(name, now) != fair:
            self.confirmations.pop(name, None)
            self._decision(name, 'confirmation_failed', reason='freshness_window_or_baseline_changed')
            return
        etf, under = (self.markets[i] for i in self._ids(name))
        if entry_gap(fair, etf, under, side) < self.config.entry_gap:
            self.confirmations.pop(name, None)
            self._decision(name, 'confirmation_failed', reason='gap_below_threshold')
            return
        elapsed = now - self.capital_requested_ns
        if self.capital_inflight and elapsed < self.config.capital_timeout_ms * 1_000_000:
            return
        self.confirmations.pop(name, None)
        self.stats['confirmations'] += 1
        snapshot = self.capital_snapshot
        if snapshot is None or elapsed >= self.config.capital_timeout_ms * 1_000_000:
            if elapsed >= self.config.capital_timeout_ms * 1_000_000:
                self.capital_error = 'capital_read_timeout'
                self.capital_retry_ns = now + 5_000_000_000
            self._decision(name, 'confirmation_failed', reason=self.capital_error or 'capital_unavailable_or_timed_out')
            return
        for iid in self.markets:
            symbol = self._symbol(iid)
            native_config = snapshot.configs.get(symbol, {})
            position = snapshot.positions.get(symbol, {})
            if iid in self._ids(name) and (int(native_config.get('leverage', 0)) < 1 or
                                          native_config.get('marginType') != 'CROSSED'):
                self._decision(name, 'confirmation_failed', reason='native_leverage_or_margin_mode', symbol=symbol)
                return
            if float(position.get('positionAmt', 0)) or float(position.get('openOrderInitialMargin', 0)):
                self._decision(name, 'confirmation_failed', reason='native_configured_instrument_occupied', symbol=symbol)
                return
        leverage = float(self.config.gross_leverage)
        budget = snapshot.available * float(self.config.capital_fraction) * leverage
        leverages = [min(leverage, float(snapshot.configs[self._symbol(iid)]['leverage'])) for iid in self._ids(name)]
        limits = []
        for iid in self._ids(name):
            limit = float(snapshot.configs[self._symbol(iid)]['maxNotionalValue'])
            if limit <= 0 or math.isnan(limit):
                self._decision(name, 'confirmation_failed', reason='invalid_native_notional_limit')
                return
            limits.append(limit if math.isfinite(limit) else None)
        unit_values = [max(etf.ask, etf.mark), abs(fair.slope) * max(under.ask, under.mark)]
        unit_margin = sum(v / lev for v, lev in zip(unit_values, leverages))
        margin_fraction = unit_margin / sum(unit_values)
        budget = min(budget, snapshot.available * float(self.config.capital_fraction) / margin_fraction)
        budget = min([budget] + [limit * sum(unit_values) / value
                                for limit, value in zip(limits, unit_values) if limit is not None])
        gross = min(2 * float(self.config.batch_notional), budget,
                    snapshot.available / (margin_fraction + 2 * self.config.taker_fee))
        plan = self._pair_plan(name, side, fair, gross, native_limits=limits)
        if plan is None:
            self._decision(name, 'confirmation_failed', reason='depth_size_cost_or_slippage', gross=gross)
            return
        self.stage = Stage(name, side, fair, now, self.calendar.next_close(now), budget,
                           available_at_start=snapshot.available, capital_checked_ns=snapshot.received_ns,
                           reconciled_epoch_ns=self.pair_epochs[name], native_limits=limits, native_leverages=leverages)
        self.capital_snapshot = None
        self.stats['stages'] += 1
        for other in list(self.confirmations):
            self._decision(other, 'confirmation_canceled', reason='another_pair_acquired_capital')
            timer = f'etf.confirm.{other}.{self.confirmations[other][2]}'
            if timer in self.clock.timer_names:
                self.clock.cancel_timer(timer)
        self.confirmations.clear()
        self._record('stage_open', pair=name, side=side, trigger_ns=triggered, confirmed_ns=now,
                     gap=entry_gap(fair, etf, under, side), budget=budget, fair=asdict(fair),
                     available_at_start=snapshot.available, capital_checked_ns=snapshot.received_ns,
                     native_leverages=leverages, native_limits=limits)
        self._batch(plan)

    def _quantity(self, iid, value):
        step = float(self.instruments[iid].size_increment)
        return math.copysign(math.floor(abs(value) / step + 1e-10) * step, value)

    def _pair_plan(self, name, side, fair, gross, close_target=None, native_limits=None):
        ids = self._ids(name)
        markets = [self.markets[i] for i in ids]
        signs = [-side, side * (1 if fair.multiple > 0 else -1)]
        closing = close_target is not None
        limits = self.stage.native_limits if self.stage else (native_limits or [None, None])
        if closing:
            signs = [-sign for sign in signs]
        now = self.clock.timestamp_ns()
        levels = [m.levels(sign > 0, now, self.config.max_depth_age_ms) for m, sign in zip(markets, signs)]
        observed_gap = (exit_gap if closing else entry_gap)(fair, *markets, side)
        if (closing and observed_gap > close_target) or (not closing and observed_gap < self.config.entry_gap):
            return None
        slope = abs(fair.slope)
        maximum = min(gross / (levels[0][0][0] + slope * levels[1][0][0]),
                      float(self.config.batch_notional) / max(levels[0][0][0], markets[0].ask, markets[0].mark),
                      float(self.config.batch_notional) / max(levels[1][0][0], markets[1].ask, markets[1].mark) / slope,
                      self.config.execution_depth_fraction * sum(q for _, q in levels[0]),
                      self.config.execution_depth_fraction * sum(q for _, q in levels[1]) / slope)
        if closing:
            maximum = min(maximum, abs(self.stage.quantities[0]))
        for leg, iid in enumerate(ids):
            instrument = self.instruments[iid]
            ratio = 1 if leg == 0 else slope
            maximum = min(maximum, self.market_limits[iid][1] / ratio)
            if instrument.max_notional:
                maximum = min(maximum, float(instrument.max_notional) / levels[leg][0][0] / ratio)
            if not closing and limits[leg] is not None:
                held = self.stage.quantities[leg] if self.stage else 0.
                cap_price = max(levels[leg][0][0], markets[leg].ask, markets[leg].mark)
                remaining = limits[leg] - abs(held) * cap_price
                maximum = min(maximum, max(0., remaining) / cap_price / ratio)
        # 二分缩量直到可见盘口的预计价差损耗和费用满足要求。
        best = None
        lo, hi = 0., maximum
        for attempt in range(18):
            size = hi if attempt == 0 else (lo + hi) / 2
            etf_qty = self._quantity(ids[0], signs[0] * size)
            prior = self.stage.quantities if self.stage and self.stage.pair == name else (0., 0.)
            target_under = self._quantity(ids[1], -(prior[0] + etf_qty) * fair.slope)
            quantities = [etf_qty, self._quantity(ids[1], target_under - prior[1])]
            if not quantities[0] or any(q * sign < 0 for q, sign in zip(quantities, signs)) or (not closing and not all(quantities)):
                break
            fills = [sweep(book, abs(q)) if q else (book[0][0], book[0][0]) for book, q in zip(levels, quantities)]
            if any(f is None for f in fills):
                hi = size
                continue
            if min(fair.price(fills[1][0]), fair.price(fills[1][1])) <= 0:
                hi = size
                continue
            average = side * (fills[0][0] / fair.price(fills[1][0]) - 1)
            total = sum(abs(q) * f[0] for q, f in zip(quantities, fills))
            profit = abs(quantities[0]) * fair.price(fills[1][0]) * (average - self.config.exit_gap)
            funding = max(0., sum(q * f[0] * m.funding_rate for q, f, m in zip(quantities, fills, markets)))
            gap_cost = average - observed_gap if closing else observed_gap - average
            valid = (gap_cost <= self.config.max_execution_gap_slippage + 1e-12
                     and total <= gross + 1e-6
                     and all(abs(q) * max(f[0], m.ask, m.mark) <= float(self.config.batch_notional) + 1e-6
                             for q, f, m in zip(quantities, fills, markets))
                     and all(abs(q) <= self.config.execution_depth_fraction * sum(size for _, size in book) + 1e-9
                             for q, book in zip(quantities, levels))
                     and (average <= close_target + 1e-12 if closing else
                          profit - 2 * self.config.taker_fee * total - funding > self.config.minimum_net_edge * total))
            if closing:
                valid &= all((not q or q * held < 0) and abs(q) <= abs(held) + 1e-9
                             for q, held in zip(quantities, self.stage.quantities))
            else:
                valid &= sum(abs(q) * (m.bid + m.ask) / 2 for q, m in zip(quantities, markets)) <= gross + 1e-6
                valid &= all(limit is None or abs(held + q) * max(fill[0], m.ask, m.mark) <= limit + 1e-6
                             for limit, held, q, fill, m in zip(limits, prior, quantities, fills, markets))
            if valid:
                for iid, q, fill in zip(ids, quantities, fills):
                    if closing and not q:
                        continue
                    instrument = self.instruments[iid]
                    if abs(q) < self.market_limits[iid][0] or abs(q) > self.market_limits[iid][1]:
                        valid = False
                    if not closing and instrument.min_notional and abs(q) * fill[0] < float(instrument.min_notional):
                        valid = False
            if valid:
                best = ExecutionPlan(quantities, [f[0] for f in fills], observed_gap, average, gross, total)
                lo = size
                if size == maximum:
                    break
            else:
                hi = size
            if hi - lo < float(self.instruments[ids[0]].size_increment):
                break
        return best

    def _batch(self, plan):
        if self.fault_reason or self.stage.exit_reason:
            return
        self.add_wait_reason = None
        self._decision(self.stage.pair, 'batch_plan', order_type='MARKET', **asdict(plan))
        self.stage.batches += 1
        self.stage.phase = 'batch'
        self.stats['batches'] += 1
        self._submit(plan.quantities, 'entry', expected_prices=plan.expected_prices)

    def _pending(self):
        return any(not order.terminal for order in self.orders.values())

    def _flat(self):
        return all(abs(q) < float(self.instruments[iid].size_increment) / 2
                   for iid, q in zip(self._ids(self.stage.pair), self.stage.quantities))

    def _drive(self):
        self._assert_thread()
        if self.stage is None or self.dispatching or self.driving or self.stopping or self.exiting:
            return
        self.driving = True
        try:
            self._advance_stage()
        finally:
            self.driving = False

    def _advance_stage(self):
        now, stage = self.clock.timestamp_ns(), self.stage
        if now >= stage.close_ns - self.config.close_flatten_minutes * MINUTE:
            self._exit('scheduled_close')
        if self._pending() or now < stage.next_attempt_ns:
            return
        if stage.phase == 'opening' and not stage.exit_reason:
            return
        if self._flat():
            self._finish()
            return
        if stage.exit_reason:
            self._close()
            return
        if self._trim_halted_excess(now):
            return
        fresh = self._fresh(stage.pair, now)
        if stage.phase in ['batch', 'repair', 'exit', 'exit_repair']:
            ids = self._ids(stage.pair)
            under = ids[1]
            target = self._quantity(under, -stage.quantities[0] * stage.fair.slope)
            delta = self._quantity(under, target - stage.quantities[1])
            if abs(delta) >= float(self.instruments[under].size_increment) / 2:
                if not stage.repair_deadline_ns:
                    stage.repair_deadline_ns = now + self.config.hedge_timeout_ms * 1_000_000
                if now >= stage.repair_deadline_ns:
                    self._exit('hedge_timeout', pause=True)
                    self._close()
                elif fresh:
                    reducing = stage.phase in ['exit', 'exit_repair']
                    leg = 1
                    if reducing and not (delta * stage.quantities[1] < 0 and abs(delta) <= abs(stage.quantities[1]) + 1e-9):
                        leg = 0
                        delta = self._quantity(ids[0], -stage.quantities[1] / stage.fair.slope) - stage.quantities[0]
                    reduce_only = delta * stage.quantities[leg] < 0 and abs(delta) <= abs(stage.quantities[leg]) + 1e-9
                    plan = self._single_plan(leg, delta, reducing or reduce_only)
                    if plan:
                        stage.phase = 'exit_repair' if reducing else 'repair'
                        quantities, prices = plan
                        self._decision(stage.pair, 'repair_plan', quantities=quantities, expected_prices=prices, reducing=reducing)
                        self._submit(quantities, 'exit_repair' if reducing else 'repair', expected_prices=prices)
                return
            stage.phase = 'holding'
            stage.repair_deadline_ns = 0
        if not fresh:
            self._add_wait('stale_or_consumed_quote')
            return
        etf, under = (self.markets[i] for i in self._ids(stage.pair))
        target = min(self.config.exit_gap_cap,
                     self.config.exit_gap + ((now - stage.started_ns) // HOUR) * self.config.exit_hour_step)
        if exit_gap(stage.fair, etf, under, stage.side) <= target:
            plan = self._pair_plan(stage.pair, stage.side, stage.fair, 2 * float(self.config.batch_notional), close_target=target)
            if plan:
                stage.phase = 'exit'
                self.add_wait_reason = None
                self._decision(stage.pair, 'exit_plan', target=target, **asdict(plan))
                self._submit(plan.quantities, 'exit', expected_prices=plan.expected_prices)
            else:
                self._add_wait('exit_depth_size_or_slippage')
            return
        if self._blocked(stage.pair, now):
            self._add_wait('time_or_funding_window')
            return
        if stage.adding_error:
            self._add_wait(stage.adding_error)
            return
        if entry_gap(stage.fair, etf, under, stage.side) < self.config.entry_gap:
            self._add_wait('between_entry_and_exit')
            return
        used = sum(abs(q) * max(m.ask, m.mark) for q, m in zip(stage.quantities, (etf, under)))
        free = self._available_capital()
        unit_values = [max(etf.ask, etf.mark), abs(stage.fair.slope) * max(under.ask, under.mark)]
        margin_fraction = sum(v / lev for v, lev in zip(unit_values, stage.native_leverages)) / sum(unit_values)
        gross = min(2 * float(self.config.batch_notional), max(0., stage.budget - used),
                    free / (margin_fraction + 2 * self.config.taker_fee))
        plan = self._pair_plan(stage.pair, stage.side, stage.fair, gross)
        if plan:
            self._batch(plan)
        else:
            self._add_wait('depth_size_cost_or_slippage')

    def _add_wait(self, reason):
        if reason != self.add_wait_reason:
            self.add_wait_reason = reason
            self._decision(self.stage.pair, 'adding_wait', reason=reason)

    def _exit(self, reason, pause=False):
        stage = self.stage
        if stage is None:
            return
        changed = False
        if not stage.exit_reason or (stage.exit_reason == 'reversion' and reason != 'reversion'):
            stage.exit_reason = reason
            stage.exit_started_ns = self.clock.timestamp_ns()
            changed = True
            self._record('exit_requested', pair=stage.pair, reason=reason)
        changed |= pause and not stage.pause_after_exit
        stage.pause_after_exit |= pause
        if changed:
            self._save()

    def _single_plan(self, leg, delta, reducing):
        stage = self.stage
        ids = self._ids(stage.pair)
        iid, market = ids[leg], self.markets[ids[leg]]
        book = market.levels(delta > 0, self.clock.timestamp_ns(), self.config.max_depth_age_ms)
        instrument = self.instruments[iid]
        maximum = min(abs(delta), self.market_limits[iid][1],
                      self.config.execution_depth_fraction * sum(q for _, q in book),
                      float(self.config.batch_notional) / max(book[0][0], market.ask, market.mark))
        if not reducing:
            maximum = min(maximum, self._available_capital() /
                          (1 / stage.native_leverages[leg] + 2 * self.config.taker_fee) / book[0][0])
            if stage.native_limits[leg] is not None:
                maximum = min(maximum, max(0., stage.native_limits[leg] / max(book[0][0], market.ask, market.mark) - abs(stage.quantities[leg])))
        theory = stage.fair.price((self.markets[ids[1]].bid + self.markets[ids[1]].ask) / 2)
        if theory <= 0:
            return None
        best, lo, hi = None, 0., maximum
        for attempt in range(18):
            size = hi if attempt == 0 else (lo + hi) / 2
            qty = self._quantity(iid, math.copysign(size, delta))
            if abs(qty) < self.market_limits[iid][0]:
                break
            fill = sweep(book, abs(qty))
            etf_equivalent = abs(qty) / abs(stage.fair.slope) if leg == 1 else abs(qty)
            valid = (fill and abs(qty) * max(fill[0], market.ask, market.mark) <= float(self.config.batch_notional) + 1e-6
                     and abs(qty) * abs(fill[0] - book[0][0]) <= etf_equivalent * theory * self.config.max_execution_gap_slippage + 1e-10)
            if valid and not reducing and instrument.min_notional:
                valid = abs(qty) * fill[0] >= float(instrument.min_notional)
            if valid:
                quantities, prices = [0., 0.], [None, None]
                quantities[leg], prices[leg] = qty, fill[0]
                best = quantities, prices
                lo = size
                if size == maximum:
                    break
            else:
                hi = size
            if hi - lo < float(instrument.size_increment):
                break
        return best

    def _trim_halted_excess(self, now):
        stage = self.stage
        ids = self._ids(stage.pair)
        tradable = [self.venue_rows.get(self._symbol(i), {}).get('status') == 'TRADING' for i in ids]
        if not all(tradable):
            quantities, prices = [0., 0.], [None, None]
            for leg, allowed in enumerate(tradable):
                if not allowed or not stage.quantities[leg] or not self.markets[ids[leg]].fresh(now, self.config.max_quote_age_ms):
                    continue
                held = stage.quantities[leg]
                other = stage.quantities[1 - leg]
                matched = -other / stage.fair.slope if leg == 0 else -other * stage.fair.slope
                excess = abs(held) - (abs(matched) if held * matched > 0 else 0.)
                if excess <= 0:
                    continue
                plan = self._single_plan(leg, -math.copysign(excess, held), True)
                if plan:
                    quantities, prices = plan
                    break
            if any(quantities):
                stage.phase = 'exit_repair'
                self._submit(quantities, 'exit_repair', expected_prices=prices)
            else:
                self._add_wait('halted_leg_preserving_matched_hedge')
            return True
        return False

    def _close(self):
        if self.stage is None or self._pending():
            return
        stage, now = self.stage, self.clock.timestamp_ns()
        stage.phase = 'exit'
        ids = self._ids(stage.pair)
        if self.pair_epochs[stage.pair] > stage.reconciled_epoch_ns:
            self._add_wait('event_position_reconciliation_required')
            return
        if any(not self.venue_specs_match.get(self._symbol(i), False) for i in ids):
            self._add_wait('venue_specification_review_required')
            return
        if self._trim_halted_excess(now):
            return
        quantities, prices = [], []
        bounded = now - stage.exit_started_ns < self.config.hedge_timeout_ms * 1_000_000
        values = [abs(q) * max(self.markets[i].ask, self.markets[i].mark) for i, q in zip(ids, stage.quantities)]
        if any(q and self.markets[i].ask <= 0 for i, q in zip(ids, stage.quantities)):
            self._add_wait('exit_price_unavailable')
            return
        fraction = min(1., float(self.config.batch_notional) / max(values)) if max(values) > 0 else 1.
        for iid, held in zip(ids, stage.quantities):
            amount = min(abs(held) * fraction, self.market_limits[iid][1])
            market = self.markets[iid]
            if bounded and market.fresh(now, self.config.max_quote_age_ms):
                book = market.levels(held < 0, now, self.config.max_depth_age_ms)
                amount = min(amount, self.config.execution_depth_fraction * sum(q for _, q in book))
            quantities.append(self._quantity(iid, -math.copysign(amount, held)))
            prices.append(None)
        self._submit(quantities, 'exit', expected_prices=prices)

    def _submit(self, quantities, action, expected_prices=None):
        self._assert_thread()
        if self.stage is None or self._pending():
            raise RuntimeError('order submission requires one active stage and no pending batch')
        now = self.clock.timestamp_ns()
        headroom = self._available_capital()
        created = []
        for leg, (iid, signed) in enumerate(zip(self._ids(self.stage.pair), quantities)):
            signed = self._quantity(iid, signed)
            if abs(signed) < float(self.instruments[iid].size_increment) / 2:
                continue
            instrument = self.instruments[iid]
            kwargs = dict(instrument_id=iid, order_side=OrderSide.BUY if signed > 0 else OrderSide.SELL,
                          quantity=instrument.make_qty(abs(signed)),
                          reduce_only=action in ['exit', 'exit_repair'] or (action == 'repair' and signed * self.stage.quantities[leg] < 0 and abs(signed) <= abs(self.stage.quantities[leg]) + 1e-9),
                          tags=[f'etf:{self.stage.pair}', f'batch:{self.stage.batches}', action])
            order = self.order_factory.market(**kwargs)
            self.orders[str(order.client_order_id)] = Pending(str(iid), leg, signed, now, action,
                expected_price=expected_prices[leg] if expected_prices else None,
                reserved_margin=0. if kwargs['reduce_only'] else abs(signed) * max(
                    self.markets[iid].ask, self.markets[iid].mark,
                    expected_prices[leg] if expected_prices and expected_prices[leg] else 0.) *
                    (1 / self.stage.native_leverages[leg] + 2 * self.config.taker_fee))
            created.append(order)
        if not created:
            self.stage.next_attempt_ns = now + self.config.retry_interval_ms * 1_000_000
            return
        required = sum(self.orders[str(o.client_order_id)].reserved_margin for o in created)
        if required > headroom + 1e-6:
            for order in created:
                self.orders.pop(str(order.client_order_id))
            self.stage.next_attempt_ns = now + self.config.retry_interval_ms * 1_000_000
            self._add_wait('local_capital_reservation_insufficient')
            return
        if self.fault_reason and not all(order.is_reduce_only for order in created):
            for order in created:
                self.orders.pop(str(order.client_order_id))
            return
        saved = self._save()
        if not saved and not all(order.is_reduce_only for order in created):
            for order in created:
                self.orders.pop(str(order.client_order_id))
            self._drive()
            return
        self.dispatching = True
        attempted = 0
        submitted = []
        try:
            for order in created:
                attempted += 1
                self.submit_order(order, client_id=self.exec_client)
                submitted.append(order)
        except Exception as exc:
            for order in created[attempted:]:
                self.orders[str(order.client_order_id)].terminal = True
            failed = created[attempted - 1]
            if self.cache.order(failed.client_order_id) is None:
                self.orders[str(failed.client_order_id)].terminal = True
            self.log.exception('ETF order submission failed', exc)
            self._fail_strategy(f'submit_error:{type(exc).__name__}')
        finally:
            self.dispatching = False
        if submitted:
            self._record('orders_sent', pair=self.stage.pair, action=action, batch=self.stage.batches,
                     orders=[str(o.client_order_id) for o in submitted],
                     details=[{'id': str(o.client_order_id), 'instrument': str(o.instrument_id),
                               'side': str(o.side), 'order_type': str(o.order_type), 'quantity': str(o.quantity), 'price': str(o.price) if hasattr(o, 'price') else None,
                               'reduce_only': o.is_reduce_only, 'time_ns': now} for o in submitted])
        self._drive()

    def on_order_filled(self, event):
        self._assert_thread()
        key = str(event.client_order_id)
        pending = self.orders.get(key)
        unique = key, str(event.trade_id)
        if pending is None or self.stage is None or unique in self.seen_fills:
            return
        self.seen_fills.add(unique)
        self.markets[event.instrument_id].last_fill_ns = max(self.markets[event.instrument_id].last_fill_ns, event.ts_event)
        size = float(event.last_qty)
        pending.filled = float(Decimal(str(pending.filled)) + event.last_qty.as_decimal())
        signed = size if event.order_side == OrderSide.BUY else -size
        commission = max(0., float(event.commission)) if str(event.commission.currency) == 'USDT' else size * float(event.last_px) * self.config.taker_fee
        account_fill(self.stage, pending.leg, signed, float(event.last_px), commission)
        self.stage.quantities[pending.leg] = float(Decimal(str(self.stage.quantities[pending.leg])) + Decimal(str(signed)))
        pending.terminal |= pending.filled >= abs(pending.quantity) - 1e-9
        self._save()
        self._record('fill', order=key, qty=signed, price=float(event.last_px), action=pending.action,
                     exchange_time_ns=event.ts_event, init_ns=event.ts_init, trade_id=str(event.trade_id),
                     commission=str(event.commission), liquidity_side=str(event.liquidity_side),
                     expected_price=pending.expected_price,
                     slippage_bps=(float(event.last_px) / pending.expected_price - 1) * math.copysign(10000, signed)
                                  if pending.expected_price else None)
        self._drive()

    def _terminal(self, event, failed=False):
        self._assert_thread()
        pending = self.orders.get(str(event.client_order_id))
        if pending is None or self.stage is None or pending.terminal:
            return
        pending.terminal = True
        self.stage.next_attempt_ns = self.clock.timestamp_ns() + self.config.retry_interval_ms * 1_000_000 if pending.action != 'entry' else 0
        self._record('order_terminal', order=str(event.client_order_id), event=type(event).__name__,
                     exchange_time_ns=event.ts_event, reason=getattr(event, 'reason', None))
        if failed and pending.action == 'entry':
            self.stage.adding_error = 'entry_rejected_waiting_for_reversion'
        if failed and self.venue_monitor:
            self.venue_monitor.refresh.set()
        self._save()
        self._drive()

    def on_order_accepted(self, event):
        self._assert_thread()
        if str(event.client_order_id) in self.orders:
            self._record('order_accepted', order=str(event.client_order_id), venue_order_id=str(event.venue_order_id),
                         exchange_time_ns=event.ts_event, init_ns=event.ts_init)

    def on_order_rejected(self, event):
        self._terminal(event, True)

    def on_order_denied(self, event):
        self._terminal(event, True)

    def on_order_expired(self, event):
        self._terminal(event)

    def on_order_canceled(self, event):
        self._terminal(event)

    def on_order_cancel_rejected(self, event):
        self._assert_thread()
        order = self.cache.order(event.client_order_id)
        if order and order.is_closed:
            pending = self.orders.get(str(event.client_order_id))
            if pending:
                pending.terminal = True
            self._drive()
        elif order and order.venue_order_id:
            self.query_order(order, client_id=self.exec_client)

    def _watchdog(self, _event):
        self._assert_thread()
        if self.stopping:
            return
        now = self.clock.timestamp_ns()
        self._poll_venue(now)
        self._receive_capital()
        self._pump_bootstrap(now)
        if now - self.last_health_ns >= 30_000_000_000:
            self.last_health_ns = now
            self._health(now)
        while not self.funding_mailbox.empty():
            iid, stamp, error = self.funding_mailbox.get_nowait()
            if error:
                self._record('funding_history_error', instrument=str(iid), error=error)
            else:
                market = self.markets[iid]
                market.last_funding_ns = max(stamp, market.last_funding_ns)
                market.funding_history_ready = True
        while not self.reference_mailbox.empty():
            snapshot, error = self.reference_mailbox.get_nowait()
            if error:
                self.reference_errors = {'cache': error}
                self._record('reference_update_error', error=error)
            else:
                self._apply_references(snapshot)
                self._record('reference_updated', checked_ns=snapshot.get('checked_ns'), errors=snapshot.get('errors'),
                             latest={name: data['rows'][-1]['date'] for name, data in snapshot.get('pairs', {}).items() if data['rows']})
        for key, pending in list(self.orders.items()):
            if pending.terminal or now - pending.sent_ns < self.config.order_timeout_ms * 1_000_000:
                continue
            if now - pending.checked_ns < self.config.order_timeout_ms * 1_000_000:
                continue
            pending.checked_ns = now
            order = self.cache.order(ClientOrderId(key))
            if order is not None:
                if order.is_closed:
                    pending.terminal = True
                else:
                    self.query_order(order, client_id=self.exec_client)
                    if not order.is_pending_cancel:
                        self.cancel_order(order, client_id=self.exec_client)
            self._add_wait('order_resolution_pending')
        self._drive()
        if self.fault_reason and self.stage is None:
            self._request_node_stop()

        if self.recorder:
            held = self.stage.pair if self.stage else None
            self.recorder.end_due(now, held)
            if now - self.last_capture_state_ns >= 1_000_000_000:
                self.last_capture_state_ns = now
                for name in self.recorder.active:
                    self._decision(name, 'state_sample')
            self.recorder.flush()
            issue = self.recorder.error, self.recorder.dropped
            if issue != self.capture_issue and (issue[0] or issue[1]) and (
                    self.capture_issue is None or now - self.last_capture_issue_ns >= 30_000_000_000):
                self.capture_issue = issue
                self.last_capture_issue_ns = now
                self.log.error(f'capture_issue error={issue[0]} dropped={issue[1]}; trading continues')

    def _health(self, now):
        free = self._available_capital()
        ages = sorted(self.quote_ages_ms)
        pairs = {}
        for name in self.pairs:
            fair = self._fair(name, now)
            etf, under = (self.markets[i] for i in self._ids(name))
            pairs[name] = {'anchor_ready': fair is not None, 'correction': fair.baseline if fair else None,
                           'baseline': self.baselines[name].describe(),
                           'fresh': self._fresh(name, now), 'entry_blocked': self._blocked(name, now),
                           'gap': max(entry_gap(fair, etf, under, 1), entry_gap(fair, etf, under, -1))
                                  if fair and etf.bid > 0 and under.bid > 0 else None}
        self._record('health', ready=self.ready, bootstrap_remaining=[str(i) for i in self.bootstrap_remaining],
                     available_usdt=free, capital=self._capital_status(now),
                     venue={'checked_ns': self.venue_checked_ns, 'error': self.venue_error,
                            'worker_alive': self.venue_monitor.worker.is_alive() if self.venue_monitor else False,
                            'events': [asdict(e) for e in self.events]}, paused=sorted(self.paused),
                     stage=self.stage.pair if self.stage else None, pending=sum(not p.terminal for p in self.orders.values()),
                     stats=dict(self.stats), pairs=pairs, capture=self.recorder.status() if self.recorder else None,
                     fault=self.fault_reason, state_failed=self.state_failed,
                     references={'worker_alive': self.reference_worker.is_alive() if self.reference_worker else False,
                                 'errors': self.reference_errors},
                     quote_age_ms={'p50': ages[len(ages)//2], 'p99': ages[min(len(ages)-1, int(len(ages)*.99))], 'max': ages[-1]} if ages else None,
                     markets={str(i): {'quote_age_ms': (now-m.quote_ns)/1e6 if m.quote_ns else None,
                                      'depth_age_ms': (now-m.depth_ns)/1e6 if m.depth_ns else None,
                                      'funding_ready': m.funding_history_ready,
                                      'funding_age_ms': (now-m.funding_seen_ns)/1e6 if m.funding_seen_ns else None}
                              for i, m in self.markets.items()})

    def _finish(self):
        self._assert_thread()
        stage = self.stage
        if stage is None:
            return
        if self._pending() or not self._flat():
            raise RuntimeError('stage completion requires no pending orders and flat quantities')
        actual = self._position_quantities(stage.pair)
        if any(abs(q) >= float(self.instruments[iid].size_increment) / 2
               for iid, q in zip(self._ids(stage.pair), actual)):
            stage.quantities = actual
            self._exit('position_mismatch', pause=True)
            return
        if stage.pause_after_exit:
            self.paused.add(stage.pair)
        self._record('stage_closed', pair=stage.pair, batches=stage.batches,
                     reason=stage.exit_reason or ('entry_rejected' if stage.pause_after_exit else 'reversion'),
                     duration_ms=(self.clock.timestamp_ns() - stage.started_ns) / 1e6, paused=stage.pause_after_exit)
        self.stage = None
        self.orders.clear()
        self.seen_fills.clear()
        self._save()

    def _position_quantities(self, name):
        return [float(sum((p.signed_decimal_qty() for p in self.cache.positions_open(instrument_id=iid, strategy_id=self.id)), Decimal(0)))
                for iid in self._ids(name)]

    def _record(self, kind, **values):
        record = {'time_ns': self.clock.timestamp_ns(), 'kind': kind, **values}
        if self.recorder:
            name = values.get('pair') or (self.stage.pair if self.stage else None)
            captured = dict(values)
            if name in self.pairs:
                captured['state'] = self._capture_state(name, record['time_ns'])
            self.recorder.event(kind, record['time_ns'], **captured)
        if self.audit:
            try:
                self.audit.write(json.dumps(record, ensure_ascii=False, separators=(',', ':')) + '\n')
            except OSError as exc:
                stream, self.audit = self.audit, None
                self.log.exception('ETF audit write failed', exc)
                try:
                    stream.close()
                except OSError:
                    pass
                if not self.stopping:
                    self._fail_strategy('audit_write_failed')
        if kind in ['started', 'stage_open', 'stage_closed', 'exit_requested', 'baseline_anchor_missing']:
            self.log.info(json.dumps(record, ensure_ascii=False))

    def _save(self):
        self._assert_thread()
        if self.state_failed:
            return False
        payload = {'version': 1, 'strategy_id': str(self.id), 'stage': asdict(self.stage) if self.stage else None,
                   'orders': {key: asdict(value) for key, value in self.orders.items()},
                   'paused': sorted(self.paused), 'seen_fills': list(self.seen_fills),
                   'fault': self.fault_reason, 'stopped': self.stopping,
                   'event_epochs': self.pair_epochs, 'special_events': [asdict(e) for e in self.events]}
        try:
            write_state(self.state_path, payload)
            return True
        except (OSError, ValueError, TypeError) as exc:
            self.state_failed = True
            self.log.exception('ETF checkpoint write failed', exc)
            self._fail_strategy(f'checkpoint_failed:{type(exc).__name__}')
            return False

    def _recover(self):
        positions = {iid: float(sum((p.signed_decimal_qty() for p in self.cache.positions_open(instrument_id=iid)), Decimal(0)))
                     for iid in self.markets}
        live_orders = [o for iid in self.markets for o in self.cache.orders_open(instrument_id=iid)]
        checkpoint = read_state(self.state_path)
        if checkpoint:
            payload = checkpoint
            if payload['version'] != 1 or payload['strategy_id'] != str(self.id):
                raise RuntimeError('state identity mismatch')
            self.paused = set(payload['paused'])
            from .special_events import SpecialEvent
            self.events = tuple(SpecialEvent(**{**e, 'symbols': tuple(e['symbols'])}) for e in payload.get('special_events', []))
            self.pair_epochs.update(payload.get('event_epochs', {}))
            if payload['stage']:
                saved = payload['stage']
                saved['fair'] = FairValue(**saved['fair'])
                self.stage = Stage(**saved)
                if self.stage.pair not in self.pairs:
                    raise RuntimeError('recovered pair absent from configuration')
                ids = self._ids(self.stage.pair)
                if any(abs(q) > 1e-9 for iid, q in positions.items() if iid not in ids):
                    raise RuntimeError('unexpected positions outside recovered pair')
                self.stage.quantities = [positions[iid] for iid in ids]
                self.stage.exit_reason = 'restart_recovery'
                self.stage.phase = 'exit'
                self.stage.exit_started_ns = self.clock.timestamp_ns()
                self.stage.repair_deadline_ns = 0
                self.stage.pause_after_exit = True
                self.stage.next_attempt_ns = 0
                self.orders = {str(o.client_order_id): Pending(str(o.instrument_id), ids.index(o.instrument_id),
                               float(o.quantity) * (1 if o.side == OrderSide.BUY else -1), self.clock.timestamp_ns(),
                               'recovery', filled=float(o.filled_qty)) for o in live_orders}
                for order in live_orders:
                    self.cancel_order(order, client_id=self.exec_client)
                self._save()
                return
        if any(abs(q) > 1e-9 for q in positions.values()) or live_orders:
            raise RuntimeError('startup requires flat configured instruments or matching recovery state')
        self._save()

    def on_stop(self):
        self._assert_thread()
        self.stopping = True
        self.background_stop.set()
        for name in list(self.clock.timer_names):
            if name.startswith('etf.'):
                self.clock.cancel_timer(name)
        if self.startup_failed:
            self._close_resources()
            return
        if self.stage:
            self.stage.quantities = self._position_quantities(self.stage.pair)
            if self._flat() and not self.cache.orders_open(strategy_id=self.id):
                self._record('stage_closed', pair=self.stage.pair, batches=self.stage.batches,
                             reason=self.stage.exit_reason or 'managed_stop',
                             duration_ms=(self.clock.timestamp_ns() - self.stage.started_ns) / 1e6,
                             paused=self.stage.pause_after_exit)
                if self.stage.pause_after_exit:
                    self.paused.add(self.stage.pair)
                self.stage = None
                self.orders.clear()
        self._save()
        self._record('stopped', stats=self.stats, remaining=asdict(self.stage) if self.stage else None)
        self._close_resources()

    def _close_resources(self):
        if self.capital_reader:
            self.capital_reader.close()
        if self.venue_monitor:
            self.venue_monitor.close()
        if self.recorder:
            status = self.recorder.close(self.clock.timestamp_ns())
            self.recorder = None
            self._record('capture_stopped', status=status)
        if self.audit:
            stream, self.audit = self.audit, None
            try:
                stream.close()
            except OSError as exc:
                self.log.exception('ETF audit close failed', exc)
        if self.runtime_lock:
            self.runtime_lock.close()
            self.runtime_lock = None

    def on_market_exit(self):
        self._assert_thread()
        self.exiting = True
        self.confirmations.clear()
        if self.startup_failed:
            return
        if self.stage:
            self._exit('managed_stop')

    # NT自带退出检查也由LiveClock触发，退出收尾必须回到同一线程。
    def _check_market_exit(self, event):
        if self.stopping:
            return
        if self.event_loop is not None and threading.get_ident() != self.owner_thread:
            try:
                self.event_loop.call_soon_threadsafe(self._check_market_exit, event)
            except RuntimeError:
                if not self.stopping:
                    raise
            return
        self._assert_thread()
        super()._check_market_exit(event)

"""后台维护公司行动事件表和交易所状态，行情线程仅读取快照。"""
import json
import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import requests
import yaml


def timestamp(value):
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None:
        raise ValueError('special event timestamp requires timezone')
    return int(stamp.timestamp() * 1e9)


@dataclass(frozen=True)
class SpecialEvent:
    id: str
    symbols: tuple[str, ...]
    block_ns: int
    flatten_ns: int
    reset_ns: int | None
    resume_ns: int | None
    source: str
    exchange_source: str | None

    def reason(self, symbols, now, anchor_ns):
        if now < self.block_ns or not set(self.symbols).intersection(symbols):
            return None
        if self.reset_ns is None or self.resume_ns is None or now < self.resume_ns:
            return f'special_event:{self.id}'
        if anchor_ns is None or anchor_ns <= self.reset_ns:
            return f'event_baseline_pending:{self.id}'
        return None


def load_events(path, symbols):
    data = yaml.safe_load(Path(path).read_text(encoding='utf-8'))
    if set(data) != {'version', 'events'} or data['version'] != 1:
        raise ValueError('invalid special event file version or fields')
    events = []
    fields = {'id', 'symbols', 'block_at', 'flatten_at', 'cash_effective_at',
              'reset_after', 'resume_after', 'source', 'exchange_source'}
    for row in data['events']:
        if set(row) != fields or not row['id'] or not row['source']:
            raise ValueError('invalid special event fields')
        if not row['symbols'] or not set(row['symbols']) <= set(symbols):
            raise ValueError('special event contains unknown symbols')
        block, flatten, cash = (timestamp(row[k]) for k in ('block_at', 'flatten_at', 'cash_effective_at'))
        reset = timestamp(row['reset_after']) if row['reset_after'] else None
        resume = timestamp(row['resume_after']) if row['resume_after'] else None
        if not block <= flatten <= cash or (reset is None) != (resume is None):
            raise ValueError('invalid special event timing')
        if reset is not None and (not row['exchange_source'] or not cash <= reset <= resume):
            raise ValueError('event resumption requires exchange source and a post-adjustment baseline')
        events.append(SpecialEvent(row['id'], tuple(row['symbols']), block, flatten, reset, resume,
                                   row['source'], row['exchange_source']))
    if len({e.id for e in events}) != len(events):
        raise ValueError('duplicate special event IDs')
    return tuple(events)


def specification(info):
    filters = {r['filterType']: {k: v for k, v in r.items() if v is not None} for r in info['filters']
               if r['filterType'] in {'PRICE_FILTER', 'LOT_SIZE', 'MARKET_LOT_SIZE', 'MIN_NOTIONAL'}}
    return json.dumps({'filters': filters, 'contractType': info['contractType'],
                       'marginAsset': info['marginAsset']}, sort_keys=True, separators=(',', ':'))


class VenueMonitor:
    def __init__(self, path, symbols, environment, interval_seconds):
        self.path, self.symbols = path, set(symbols)
        self.url = {'live': 'https://fapi.binance.com',
                    'testnet': 'https://testnet.binancefuture.com'}[environment]
        self.interval = interval_seconds
        self.results = queue.Queue(maxsize=1)
        self.stop = threading.Event()
        self.refresh = threading.Event()
        self.worker = threading.Thread(target=self._run, name='etf-venue-monitor', daemon=True)

    def _run(self):
        last_request = 0.
        with requests.Session() as session:
            while not self.stop.is_set():
                if self.stop.wait(max(0., 1 - (time.monotonic() - last_request))):
                    return
                last_request = time.monotonic()
                try:
                    events = load_events(self.path, self.symbols)
                    response = session.get(self.url + '/fapi/v1/exchangeInfo', timeout=(3, 5))
                    if response.status_code != 200:
                        raise RuntimeError(f'exchangeInfo HTTP {response.status_code}')
                    rows = {r['symbol']: r for r in response.json()['symbols'] if r['symbol'] in self.symbols}
                    if rows.keys() != self.symbols:
                        raise ValueError('configured symbol absent from exchangeInfo')
                    result = (time.time_ns(), events, rows, None)
                except (OSError, ValueError, KeyError, TypeError, RuntimeError, requests.RequestException) as exc:
                    result = (time.time_ns(), None, None, type(exc).__name__)
                if self.stop.is_set():
                    return
                try:
                    self.results.get_nowait()
                except queue.Empty:
                    pass
                self.results.put_nowait(result)
                self.refresh.wait(self.interval)
                self.refresh.clear()

    def close(self):
        self.stop.set()
        self.refresh.set()

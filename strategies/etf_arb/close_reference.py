"""美股ETF与标的实际收盘参考；网络和磁盘操作只在后台线程执行。"""
import json
import math
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

from .baseline import MarketCalendar


def read_cache(path):
    file = Path(path)
    return json.loads(file.read_text(encoding='utf8')) if file.exists() else {'pairs': {}}


def _closes(session, symbol, calendar, now):
    response = session.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}',
                           params={'period1': now // 1_000_000_000 - 120 * 86400,
                                   'period2': now // 1_000_000_000, 'interval': '1d'},
                           headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
    response.raise_for_status()
    data = response.json()['chart']['result'][0]
    meta = data['meta']
    if meta['symbol'] != symbol or meta['currency'] != 'USD' or meta['exchangeTimezoneName'] != 'America/New_York':
        raise ValueError(f'{symbol}: unexpected cash market')
    if meta['dataGranularity'] != '1d':
        raise ValueError(f'{symbol}: expected daily closing prices')
    prices = {}
    for stamp, price in zip(data['timestamp'], data['indicators']['quote'][0]['close'], strict=True):
        date = pd.Timestamp(stamp, unit='s', tz='UTC').tz_convert('America/New_York').date()
        market = calendar.days.get(date)
        # 日线接口包含当日盘中价格，只接收已结束的美股交易日。
        if market is None or market[1] > now or price is None:
            continue
        if not math.isfinite(price) or price <= 0:
            raise ValueError(f'{symbol}: invalid close on {date}')
        prices[date.isoformat()] = float(price)
    if not prices:
        raise ValueError(f'{symbol}: no completed daily prices')
    return prices


def refresh(path, pairs, stop=None):
    cache = read_cache(path)
    errors, prices = {}, {}
    now = time.time_ns()
    calendar = MarketCalendar(now)
    cutoff = (datetime.fromtimestamp(now / 1e9, timezone.utc) - timedelta(days=120)).strftime('%Y-%m-%d')
    with requests.Session() as session:
        for pair in pairs:
            if stop and stop.is_set():
                break
            try:
                symbols = [str(i.symbol).removesuffix('USDT-PERP') for i in (pair.etf, pair.underlying)]
                for symbol in symbols:
                    if symbol not in prices:
                        prices[symbol] = _closes(session, symbol, calendar, now)
                etf, underlying = (prices[symbol] for symbol in symbols)
                dates = sorted(etf.keys() & underlying.keys())
                if not dates:
                    raise ValueError(f'{pair.name}: no matching cash closing dates')
                old = {r['date']: r for r in cache['pairs'].get(pair.name, {}).get('rows', [])}
                checked = time.time_ns()
                for date in dates:
                    row = {'date': date, 'etf_close': etf[date], 'underlying_close': underlying[date]}
                    previous = old.get(date)
                    same = previous and all(previous[k] == row[k] for k in ('etf_close', 'underlying_close'))
                    row['available_ns'] = previous['available_ns'] if same else checked
                    old[date] = row
                cache['pairs'][pair.name] = {'source': 'Yahoo cash close', 'symbols': symbols, 'checked_ns': checked,
                                             'rows': [r for date, r in sorted(old.items()) if date >= cutoff]}
            except (requests.RequestException, ValueError, KeyError, IndexError, TypeError) as exc:
                errors[pair.name] = f'{type(exc).__name__}: {exc}'
    cache['checked_ns'] = time.time_ns()
    cache['errors'] = errors
    if stop and stop.is_set():
        return cache
    file = Path(path)
    file.parent.mkdir(parents=True, exist_ok=True)
    temp = file.with_suffix('.tmp')
    temp.write_text(json.dumps(cache, separators=(',', ':')), encoding='utf8')
    temp.replace(file)
    return cache

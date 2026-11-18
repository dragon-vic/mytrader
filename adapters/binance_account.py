"""Binance U本位原始账户快照。网络请求由调用方的后台线程执行。"""
import hashlib
import hmac
import math
import time
from dataclasses import dataclass
from urllib.parse import urlencode

import requests
from utils.background import BackgroundReader

from .binance import credentials


@dataclass(frozen=True)
class FuturesAccountSnapshot:
    requested_ns: int
    received_ns: int
    available: float
    wallet: float
    positions: dict
    configs: dict


class BinanceAccountReader:
    def __init__(self, environment):
        self.key, self.secret = credentials(environment)
        self.url = {'live': 'https://fapi.binance.com',
                    'testnet': 'https://testnet.binancefuture.com'}[environment]
        self.session = requests.Session()
        self.session.headers['X-MBX-APIKEY'] = self.key
        self.offset_ms = 0
        self.time_synced_at = 0.
        self.rate_limit_until = 0.

    # 异常只包含路径与错误类型，避免签名URL进入日志。
    def _get(self, path, signed=True):
        for attempt in range(2):
            if time.monotonic() < self.rate_limit_until:
                raise RuntimeError(f'Binance GET {path}: rate_limit_backoff')
            params = {}
            if signed:
                params = {'timestamp': time.time_ns() // 1_000_000 + self.offset_ms, 'recvWindow': 5000}
                query = urlencode(params)
                params['signature'] = hmac.new(self.secret.encode(), query.encode(), hashlib.sha256).hexdigest()
            try:
                response = self.session.get(self.url + path, params=params, timeout=(3, 5))
            except requests.RequestException as exc:
                raise RuntimeError(f'Binance GET {path}: {type(exc).__name__}') from None
            if response.status_code in (418, 429):
                default = 120 if response.status_code == 418 else 60
                try:
                    delay = float(response.headers.get('Retry-After', default))
                except ValueError:
                    delay = default
                self.rate_limit_until = time.monotonic() + (max(1., delay) if math.isfinite(delay) else default)
                raise RuntimeError(f'Binance GET {path}: HTTP {response.status_code} rate_limit_backoff')
            try:
                data = response.json()
            except ValueError:
                raise ValueError(f'Binance GET {path}: invalid JSON') from None
            if response.status_code == 200:
                return data
            if signed and attempt == 0 and isinstance(data, dict) and data.get('code') == -1021:
                self._sync_time()
                continue
            raise RuntimeError(f'Binance GET {path}: HTTP {response.status_code}')

    def _sync_time(self):
        requested = time.time_ns()
        remote = self._get('/fapi/v1/time', signed=False)
        self.offset_ms = int(remote['serverTime']) - (requested + time.time_ns()) // 2_000_000
        self.time_synced_at = time.monotonic()

    def read(self):
        requested = time.time_ns()
        if not self.time_synced_at or time.monotonic() - self.time_synced_at >= 900:
            self._sync_time()
        data = self._get('/fapi/v2/account')
        received = time.time_ns()
        if data['multiAssetsMargin'] is not False:
            raise ValueError('native capital requires single-asset margin mode')
        if data['canTrade'] is not True:
            raise ValueError('Binance account canTrade=false')
        asset = next(r for r in data['assets'] if r['asset'] == 'USDT')
        available, wallet = float(asset['availableBalance']), float(asset['walletBalance'])
        if not all(math.isfinite(v) for v in (available, wallet)):
            raise ValueError('invalid native USDT balance')
        positions, configs = {}, {}
        if not data['positions']:
            raise ValueError('native account positions missing')
        for row in data['positions']:
            if row['positionSide'] != 'BOTH':
                raise ValueError('native account contains hedge-mode positions')
            if type(row['isolated']) is not bool:
                raise ValueError('invalid native margin type')
            positions[row['symbol']] = row
            configs[row['symbol']] = {'symbol': row['symbol'], 'leverage': row['leverage'],
                                      'marginType': 'ISOLATED' if row['isolated'] else 'CROSSED',
                                      'maxNotionalValue': row['maxNotional']}
        return FuturesAccountSnapshot(requested, received, max(0., available), wallet, positions, configs)

    def close(self):
        self.session.close()


class NativeAccountWorker(BackgroundReader):
    def __init__(self, environment, notify):
        super().__init__(lambda: BinanceAccountReader(environment), notify, 'native-account-reader')

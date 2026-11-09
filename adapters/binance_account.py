"""Binance U本位原始账户快照。网络请求由调用方的后台线程执行。"""
import hashlib
import hmac
import math
import time
from dataclasses import dataclass
from urllib.parse import urlencode

import requests

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

    # 异常只包含路径与错误类型，避免签名URL进入日志。
    def _get(self, path, signed=True):
        params = {}
        if signed:
            params = {'timestamp': time.time_ns() // 1_000_000 + self.offset_ms, 'recvWindow': 5000}
            query = urlencode(params)
            params['signature'] = hmac.new(self.secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        try:
            response = self.session.get(self.url + path, params=params, timeout=(3, 5))
        except requests.RequestException as exc:
            raise RuntimeError(f'Binance GET {path}: {type(exc).__name__}') from None
        if response.status_code != 200:
            raise RuntimeError(f'Binance GET {path}: HTTP {response.status_code}')
        try:
            return response.json()
        except ValueError:
            raise ValueError(f'Binance GET {path}: invalid JSON') from None

    def read(self):
        requested = time.time_ns()
        remote = self._get('/fapi/v1/time', signed=False)
        self.offset_ms = int(remote['serverTime']) - (requested + time.time_ns()) // 2_000_000
        if self._get('/fapi/v1/multiAssetsMargin')['multiAssetsMargin'] is not False:
            raise ValueError('native capital requires single-asset margin mode')
        if self._get('/fapi/v1/positionSide/dual')['dualSidePosition'] is not False:
            raise ValueError('native capital requires one-way position mode')
        configs = {r['symbol']: r for r in self._get('/fapi/v1/symbolConfig')}
        data = self._get('/fapi/v2/account')
        received = time.time_ns()
        if data['canTrade'] is not True:
            raise ValueError('Binance account canTrade=false')
        asset = next(r for r in data['assets'] if r['asset'] == 'USDT')
        available, wallet = float(asset['availableBalance']), float(asset['walletBalance'])
        if not all(math.isfinite(v) for v in (available, wallet)):
            raise ValueError('invalid native USDT balance')
        positions = {}
        for row in data['positions']:
            if row['positionSide'] != 'BOTH':
                raise ValueError('native account contains hedge-mode positions')
            positions[row['symbol']] = row
        return FuturesAccountSnapshot(requested, received, max(0., available), wallet, positions, configs)

    def close(self):
        self.session.close()

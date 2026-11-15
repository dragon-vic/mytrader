"""跨策略企业微信通知，调用线程只投递消息快照。"""
from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from pathlib import Path

import requests
from dotenv import dotenv_values

from utils.constants import PROJECT_ROOT


class WeComNotifier:
    def __init__(self, delivery_path: str | Path, *, capacity: int = 64):
        self.delivery_path = Path(delivery_path)
        self.queue = queue.Queue(maxsize=capacity)
        self.closed = threading.Event()
        self.lock = threading.Lock()
        self.sent = 0
        self.failed = 0
        self.rejected = 0
        self.last_error = None
        self.log = logging.getLogger(__name__)
        self.worker = threading.Thread(target=self._run, name='wecom-notifications', daemon=True)
        self.worker.start()

    def send(self, event_id: str, message: str) -> bool:
        if not self.lock.acquire(blocking=False):
            self.rejected += 1
            return False
        try:
            if self.closed.is_set() or not message.strip() or len(message.encode('utf-8')) > 2048:
                self.rejected += 1
                return False
            try:
                self.queue.put_nowait((event_id, message))
            except queue.Full:
                self.rejected += 1
                return False
        finally:
            self.lock.release()
        return True

    def status(self) -> dict:
        return {'queued': self.queue.qsize(), 'sent': self.sent, 'failed': self.failed,
                'rejected': self.rejected, 'last_error': self.last_error,
                'worker_alive': self.worker.is_alive()}

    def close(self, timeout=5.):
        self.closed.set()
        self.worker.join(timeout=timeout)
        if self.worker.is_alive():
            self.last_error = 'shutdown_delivery_timeout'
            self.log.error('WeCom shutdown delivery timeout queued=%s', self.queue.qsize())

    def _run(self):
        session = requests.Session()
        client = None
        try:
            while not self.closed.is_set() or not self.queue.empty():
                try:
                    event_id, message = self.queue.get(timeout=.2)
                except queue.Empty:
                    continue
                attempts, error = 0, None
                try:
                    if client is None:
                        client = _WeComClient(session)
                    for attempt in range(3):
                        attempts += 1
                        try:
                            client.send(message)
                            break
                        except (RuntimeError, ValueError, KeyError, TypeError):
                            if attempt == 2:
                                raise
                            time.sleep(2 ** attempt)
                except Exception as exc:
                    # 第三方异常可能包含带凭据的URL，只记录异常类型及自有错误。
                    error = str(exc) if isinstance(exc, _WeComError) else type(exc).__name__
                    self.log.error('WeCom notification failed event=%s error=%s', event_id, error)
                with self.lock:
                    self.sent += error is None
                    self.failed += error is not None
                    if error:
                        self.last_error = error
                try:
                    self.delivery_path.parent.mkdir(parents=True, exist_ok=True)
                    with self.delivery_path.open('a', encoding='utf-8') as stream:
                        stream.write(json.dumps({'time_ns': time.time_ns(), 'event_id': event_id,
                                                 'status': 'failed' if error else 'sent',
                                                 'attempts': attempts, 'error': error}, ensure_ascii=False) + '\n')
                except OSError as exc:
                    with self.lock:
                        self.last_error = f'delivery_log:{type(exc).__name__}'
                    self.log.error('WeCom delivery log failed error=%s', type(exc).__name__)
                finally:
                    self.queue.task_done()
        finally:
            session.close()


class _WeComError(RuntimeError):
    pass


class _WeComClient:
    def __init__(self, session):
        values = {**dotenv_values(PROJECT_ROOT / '.env'), **os.environ}
        required = ('WECOM_CORP_ID', 'WECOM_AGENT_ID', 'WECOM_APP_SECRET', 'WECOM_ALLOWED_USER_IDS')
        missing = [key for key in required if not values.get(key)]
        if missing:
            raise _WeComError(f'missing configuration {", ".join(missing)}')
        self.corp = values['WECOM_CORP_ID']
        self.agent = int(values['WECOM_AGENT_ID'])
        self.secret = values['WECOM_APP_SECRET']
        users = list(dict.fromkeys(user.strip() for user in values['WECOM_ALLOWED_USER_IDS'].split(',') if user.strip()))
        if not users or '@all' in users or any('|' in user for user in users):
            raise _WeComError('invalid WECOM_ALLOWED_USER_IDS')
        self.users = '|'.join(users)
        self.session = session
        self.token = None
        self.token_deadline = 0.

    def _request(self, path, params, payload=None):
        try:
            response = self.session.request('GET' if payload is None else 'POST',
                'https://qyapi.weixin.qq.com/cgi-bin/' + path,
                params=params, json=payload, timeout=(3, 5))
            if response.status_code != 200:
                raise _WeComError(f'{path} HTTP {response.status_code}')
            data = response.json()
        except requests.RequestException as exc:
            raise _WeComError(f'{path} {type(exc).__name__}') from None
        except ValueError:
            raise _WeComError(f'{path} invalid JSON') from None
        if not isinstance(data, dict):
            raise _WeComError(f'{path} invalid response')
        return data

    def send(self, text):
        if time.monotonic() >= self.token_deadline:
            result = self._request('gettoken', {'corpid': self.corp, 'corpsecret': self.secret})
            if result.get('errcode', 0):
                raise _WeComError(f'gettoken errcode={result["errcode"]}')
            self.token = result['access_token']
            self.token_deadline = time.monotonic() + max(0., float(result['expires_in']) - 60)
        result = self._request('message/send', {'access_token': self.token}, {
            'touser': self.users, 'agentid': self.agent, 'msgtype': 'text', 'text': {'content': text},
            'enable_duplicate_check': 1, 'duplicate_check_interval': 1800,
        })
        if 'errcode' not in result:
            raise _WeComError('message/send missing errcode')
        if result.get('errcode') in (40014, 42001):
            self.token_deadline = 0.
        if result.get('errcode', 0):
            raise _WeComError(f'message/send errcode={result["errcode"]}')
        if any(result.get(key) for key in ('invaliduser', 'unlicenseduser')):
            raise _WeComError('message/send recipient rejected')

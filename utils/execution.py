"""执行重试与结果未知的分类，独立于具体策略。"""
import re
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class OrderResolution:
    client_order_id: str
    status: str
    filled: Decimal


def uncertain_order_error(reason, status=None):
    text = str(reason).lower()
    if status is not None and (status >= 500 or status == 408):
        return True
    ambiguous = any(word in text for word in ('unknown', 'timeout', 'timed out', 'connection', 'network',
                                               'socket', 'disconnected', 'duplicate'))
    codes = {int(value) for value in re.findall(r'(?<!\d)-\d{4}(?!\d)', text)}
    if ambiguous or codes.intersection({-1000, -1001, -1006, -1007}):
        return True
    if status is None and re.search(r'\b50[0-9]\b', text):
        return True
    if text.strip() == 'rejected' or codes or (status is not None and 400 <= status < 500):
        return False
    return True


def retry_delay_ns(attempt):
    return (1, 2, 5)[min(max(0, attempt - 1), 2)] * 1_000_000_000


def order_alert_due(sent_ns, last_alert_ns, now):
    return now - sent_ns >= 60_000_000_000 and (not last_alert_ns or now - last_alert_ns >= 300_000_000_000)

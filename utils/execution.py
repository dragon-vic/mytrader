"""订单批次提交、未知结果和核单退避。"""
import re
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class OrderResolution:
    client_order_id: str
    status: str
    filled: Decimal


@dataclass(kw_only=True)
class OrderCheck:
    checked_ns: int = 0
    resolution_attempts: int = 0
    alert_ns: int = 0

    def poll(self, sent_ns: int, now: int, timeout_ns: int = 0) -> bool | None:
        """None 表示继续等待，布尔值表示可以核单及是否同时告警。"""
        if now - sent_ns < timeout_ns or now - self.checked_ns < retry_delay_ns(self.resolution_attempts + 1):
            return None
        self.checked_ns = now
        self.resolution_attempts += 1
        alert = order_alert_due(sent_ns, self.alert_ns, now)
        if alert:
            self.alert_ns = now
        return alert


@dataclass(frozen=True)
class BatchSubmission:
    submitted: list
    error: Exception | None = None


def submit_order_batch(strategy, orders, pending, *, client_id=None) -> BatchSubmission:
    """发单前由策略登记并保存全批 pending 与预留，提交异常后由策略处理敞口。"""
    entries = [pending[str(order.client_order_id)] for order in orders]
    submitted = []
    error = None
    for index, order in enumerate(orders):
        try:
            strategy.submit_order(order, client_id=client_id)
        except Exception as exc:
            # NT先写缓存再投递。缓存中的订单可能已经发出，交给原单核对。
            if strategy.cache.order(order.client_order_id) is None:
                entries[index].terminal = True
            error = exc
            break
        submitted.append(order)
        # NT可以同步回报拒单并正常返回。已确认未足额成交时停止批次余腿。
        if entries[index].terminal and order.is_closed and order.filled_qty < order.quantity:
            break
    else:
        return BatchSubmission(submitted)
    for entry in entries[index + 1:]:
        entry.terminal = True
    return BatchSubmission(submitted, error)


def margin_with_fees(quantity, price, leverage, taker_fee):
    return abs(quantity) * price * (1 / leverage + 2 * taker_fee)


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

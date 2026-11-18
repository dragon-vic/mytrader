import asyncio
import threading
from functools import wraps

from nautilus_trader.common.component import LiveClock
from nautilus_trader.trading.strategy import Strategy
from utils.constants import NODE_STOP_TOPIC
from utils.live_control import NodeStopRequest


def guarded_callback(method):
    @wraps(method)
    def invoke(self, *args, **kwargs):
        self._assert_thread()
        try:
            return method(self, *args, **kwargs)
        except Exception as exc:
            self.log.exception(f'Strategy callback failed name={method.__name__}', exc)
            self.on_callback_error(method.__name__, exc)
    return invoke


class LiveStrategy(Strategy):
    def on_callback_error(self, name, error):
        self.msgbus.publish(NODE_STOP_TOPIC, NodeStopRequest(
            source=str(self.id), reason=f'callback_error:{name}:{type(error).__name__}'))

    def bind_owner(self):
        self.owner_thread = threading.get_ident()
        self.event_loop = asyncio.get_running_loop() if isinstance(self.clock, LiveClock) else None

    def _assert_thread(self):
        if self.owner_thread != threading.get_ident():
            raise RuntimeError('strategy state mutation outside its event loop thread')

    def dispatch_to_owner(self, callback, *args):
        if self.event_loop is None or self.owner_thread == threading.get_ident():
            callback(*args)
        elif not self.event_loop.is_closed():
            try:
                self.event_loop.call_soon_threadsafe(callback, *args)
            except RuntimeError:
                if not self.event_loop.is_closed():
                    raise

    def cancel_timer_if_active(self, name):
        # LiveClock到期移除计时器与回调回到owner之间存在时间窗口。
        try:
            self.clock.cancel_timer(name)
        except KeyError:
            if name in self.clock.timer_names:
                raise

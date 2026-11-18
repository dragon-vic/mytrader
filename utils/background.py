import queue
import threading


class BackgroundReader:
    def __init__(self, factory, notify, name):
        self.factory, self.notify = factory, notify
        self.requests = queue.Queue(maxsize=1)
        self.results = queue.Queue(maxsize=1)
        self.stop = threading.Event()
        self.notification_error = None
        self.worker = threading.Thread(target=self._run, name=name, daemon=True)

    def request(self, token):
        self.requests.put_nowait(token)

    def _run(self):
        reader = None
        try:
            while not self.stop.is_set():
                try:
                    token = self.requests.get(timeout=.2)
                except queue.Empty:
                    continue
                try:
                    if reader is None:
                        reader = self.factory()
                    value, error = reader.read(), None
                except Exception as exc:
                    value, error = None, f'{type(exc).__name__}:{exc}' if isinstance(exc, (RuntimeError, ValueError)) else type(exc).__name__
                if self.stop.is_set():
                    return
                self.results.put_nowait((token, value, error))
                try:
                    self.notify()
                except Exception as exc:
                    # 结果仍在邮箱，调用方的周期轮询可继续接收。
                    self.notification_error = type(exc).__name__
        finally:
            if reader:
                reader.close()

    def close(self):
        self.stop.set()

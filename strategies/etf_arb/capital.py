"""每阶段一次账户读取，阶段内使用成交与在途预留记账。"""
import queue
import threading

from adapters.binance_account import BinanceAccountReader


class CapitalReader:
    def __init__(self, environment, notify):
        self.environment, self.notify = environment, notify
        self.requests = queue.Queue(maxsize=1)
        self.results = queue.Queue(maxsize=1)
        self.stop = threading.Event()
        self.worker = threading.Thread(target=self._run, name='etf-native-capital', daemon=True)

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
                        reader = BinanceAccountReader(self.environment)
                    snapshot, error = reader.read(), None
                except (RuntimeError, ValueError, KeyError, StopIteration, TypeError) as exc:
                    snapshot, error = None, f'{type(exc).__name__}:{exc}'
                if self.stop.is_set():
                    return
                self.results.put((token, snapshot, error))
                self.notify()
        finally:
            if reader:
                reader.close()

    def close(self):
        self.stop.set()


def capital_headroom(stage, orders, markets):
    pnl, margin = 0., 0.
    for quantity, average, market, leverage in zip(stage.quantities, stage.average_prices, markets, stage.native_leverages):
        if not quantity:
            continue
        if min(market.bid, market.ask) <= 0:
            return 0.
        price = market.bid if quantity > 0 else market.ask
        pnl += quantity * (price - average)
        margin += abs(quantity) * max(average, market.ask, market.mark) / leverage
    reserved = sum(p.reserved_margin * max(0., 1 - p.filled / abs(p.quantity))
                   for p in orders.values() if not p.terminal)
    return max(0., stage.available_at_start + stage.realized_pnl - stage.fees + min(0., pnl) - margin - reserved)


def account_fill(stage, leg, signed, price, fee):
    held, average = stage.quantities[leg], stage.average_prices[leg]
    if held * signed < 0:
        stage.realized_pnl += min(abs(signed), abs(held)) * (price - average) * (1 if held > 0 else -1)
    if held * signed >= 0:
        stage.average_prices[leg] = (abs(held) * average + abs(signed) * price) / (abs(held) + abs(signed))
    elif abs(signed) > abs(held):
        stage.average_prices[leg] = price
    elif abs(signed) == abs(held):
        stage.average_prices[leg] = 0.
    stage.fees += fee

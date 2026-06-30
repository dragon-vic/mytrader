from __future__ import annotations

from collections import defaultdict

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from utils.arguments import NODE_STOP_TOPIC


class QuoteDelayConfig(StrategyConfig, frozen=True):
    instruments: list[InstrumentId]
    run_seconds: float
    summary_interval_sec: float


class QuoteDelayStrategy(Strategy):
    def __init__(self, config: QuoteDelayConfig) -> None:
        super().__init__(config)
        self.instruments = [InstrumentId.from_str(str(item)) for item in config.instruments]
        self.run_ns = int(float(config.run_seconds) * 1_000_000_000)
        self.summary_ns = int(float(config.summary_interval_sec) * 1_000_000_000)
        self.last_summary_ns = 0
        self.delay_values: dict[str, list[int]] = defaultdict(list)
        self.age_values: dict[str, list[int]] = defaultdict(list)
        self.total_counts: dict[str, int] = defaultdict(int)

    def on_start(self) -> None:
        for instrument_id in self.instruments:
            self.subscribe_quote_ticks(instrument_id)
        now_ns = self.clock.timestamp_ns()
        self.last_summary_ns = now_ns
        self.clock.set_time_alert_ns(
            "quote_delay_test_stop",
            now_ns + self.run_ns,
            callback=lambda _event: self._request_stop(),
            allow_past=True,
        )
        self.log.info(
            f"quote_delay_test started instruments={','.join(str(item) for item in self.instruments)} "
            f"run_seconds={self.run_ns / 1_000_000_000:.1f}",
        )

    def on_quote_tick(self, tick: QuoteTick) -> None:
        now_ns = self.clock.timestamp_ns()
        key = str(tick.instrument_id)
        self.delay_values[key].append(now_ns - int(tick.ts_init))
        self.age_values[key].append(now_ns - int(tick.ts_event))
        self.total_counts[key] += 1
        if now_ns - self.last_summary_ns >= self.summary_ns:
            self._log_summary("interval")
            self.delay_values.clear()
            self.age_values.clear()
            self.last_summary_ns = now_ns

    def on_stop(self) -> None:
        self._log_summary("final")
        for instrument_id in self.instruments:
            self.unsubscribe_quote_ticks(instrument_id)

    def _request_stop(self) -> None:
        self.msgbus.publish(NODE_STOP_TOPIC, {"source": "quote_delay_test", "reason": "timer"})

    def _log_summary(self, label: str) -> None:
        keys = sorted(set(self.total_counts) | {str(item) for item in self.instruments})
        for key in keys:
            delays = self.delay_values.get(key, [])
            ages = self.age_values.get(key, [])
            self.log.info(
                f"quote_delay_{label} {key} interval_n={len(delays)} total_n={self.total_counts.get(key, 0)} "
                f"on_delay_ms={self._stats(delays)} event_age_ms={self._stats(ages)}",
            )

    def _stats(self, values: list[int]) -> str:
        if not values:
            return "-"
        ordered = sorted(values)
        return (
            f"p50={self._pct(ordered, 0.50):.3f} "
            f"p95={self._pct(ordered, 0.95):.3f} "
            f"p99={self._pct(ordered, 0.99):.3f} "
            f"max={ordered[-1] / 1_000_000:.3f}"
        )

    def _pct(self, ordered: list[int], pct: float) -> float:
        idx = min(max(int((len(ordered) - 1) * pct), 0), len(ordered) - 1)
        return ordered[idx] / 1_000_000

from bisect import bisect_right
from collections import deque
from dataclasses import dataclass
from functools import lru_cache

import exchange_calendars as xcals
import numpy as np
import pandas as pd

from .config import EtfArbConfig

SECOND = 1_000_000_000
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
DAY = 24 * HOUR


class MarketCalendar:
    def __init__(self, now_ns: int):
        year = pd.Timestamp(now_ns, tz='UTC').year
        calendar = xcals.get_calendar('XNYS', start=f'{year-1}-01-01', end=f'{year+2}-12-31')
        schedule = calendar.schedule
        self.opens = [int(t.value) for t in schedule['open']]
        self.closes = [int(t.value) for t in schedule['close']]
        self.days = {t.tz_convert('America/New_York').date(): (o, c)
                     for t, o, c in zip(schedule['open'], self.opens, self.closes)}

    def previous_close(self, now: int) -> int:
        index = bisect_right(self.closes, now) - 1
        if index < 0 or now >= self.closes[-1]:
            raise ValueError('market calendar out of range')
        return self.closes[index]

    def next_close(self, now: int) -> int:
        index = bisect_right(self.closes, now)
        if index >= len(self.closes):
            raise ValueError('market calendar out of range')
        return self.closes[index]

    def entry_blocked(self, now: int, config: EtfArbConfig) -> bool:
        opening = bisect_right(self.opens, now) - 1
        if opening >= 0 and now < self.opens[opening] + config.open_block_minutes * MINUTE:
            return True
        return self.next_close(now) - now <= config.close_block_minutes * MINUTE

    @lru_cache(maxsize=65536)
    def segment(self, now: int) -> tuple[str, int]:
        local = pd.Timestamp(now, tz='UTC').tz_convert('America/New_York')
        midnight = local.normalize()
        minutes = local.hour * 60 + local.minute
        weekday = local.dayofweek
        if weekday == 5 or (weekday == 4 and minutes >= 1200) or (weekday == 6 and minutes < 1200):
            friday = midnight - pd.DateOffset(days=(weekday - 4) % 7)
            return 'weekend', int(friday.replace(hour=20).value)
        session = self.days.get(local.date())
        if session is None and minutes < 1200:
            return 'holiday', int(midnight.value)
        if minutes >= 1200:
            return 'overnight', int(midnight.replace(hour=20).value)
        if minutes < 240:
            previous = midnight - pd.DateOffset(days=1)
            return 'overnight', int(previous.replace(hour=20).value)
        if session is None:
            return 'holiday', int(midnight.value)
        opening, closing = session
        if now < opening:
            return 'premarket', int(midnight.replace(hour=4).value)
        if now < closing:
            return 'regular', opening
        return 'afterhours', closing


@dataclass(frozen=True)
class FairValue:
    e0: float
    u0: float
    multiple: int
    baseline: float
    anchor_ns: int

    @property
    def slope(self) -> float:
        return self.multiple * self.e0 * (1 + self.baseline) / self.u0

    def price(self, underlying: float) -> float:
        return self.e0 * (1 + self.multiple * (underlying / self.u0 - 1)) * (1 + self.baseline)


class Baseline:
    def __init__(self, multiple: int, calendar: MarketCalendar, config: EtfArbConfig):
        self.multiple, self.calendar, self.config = multiple, calendar, config
        self.anchor: tuple[int, float, float] | None = None
        self.segment_key: tuple[str, int] | None = None
        self.samples: list[tuple[float, float]] = []
        self.history: dict[str, list[tuple[int, float]]] = {}
        self.correction = 0.
        self.calibrated = False
        self.last_minute = -1
        self.raw_anchor: tuple[int, float, float] | None = None
        self.robust_anchor: tuple[int, float, float] | None = None
        self.close_samples = deque(maxlen=config.anchor_window_minutes)
        self.contract_closes: dict[int, tuple[float, float]] = {}
        self.references: dict[int, dict] = {}
        self.reference_checked_ns = 0
        self.anchor_source = 'contract'
        self.anchor_adjustment = 0.
        self.normal_pair_basis: float | None = None
        self.reference_anchor: float | None = None
        self.reference_gap: float | None = None
        self.reference_status = 'not_loaded'
        self.revision = 0

    def set_references(self, rows: list[dict], checked_ns: int) -> None:
        refs = {}
        for row in rows:
            session = self.calendar.days.get(pd.Timestamp(row['date']).date())
            if session:
                refs[session[1]] = row
        self.references = refs
        self.reference_checked_ns = checked_ns

    # 只在时段初始化及第五分钟定稿；中途到达的美股收盘参考等待下一时段。
    def _select_anchor(self, now: int) -> None:
        if self.robust_anchor is None:
            return
        stamp, etf, underlying = self.robust_anchor
        self.anchor = self.robust_anchor
        self.anchor_source = 'contract_robust'
        self.normal_pair_basis = None
        self.reference_anchor = None
        self.reference_gap = None
        reference = self.references.get(stamp)
        self.reference_status = 'current_close_missing' if reference is None else 'history_unstable'
        if reference and reference['available_ns'] > now:
            reference = None
            self.reference_status = 'publication_pending'
        if reference:
            history = []
            for t, (ep, up) in sorted(self.contract_closes.items()):
                if t >= stamp or stamp - t > self.config.history_days * DAY:
                    continue
                row = self.references.get(t)
                if row and row['available_ns'] <= now:
                    theoretical = row['etf_close'] * (1 + self.multiple * (up / row['underlying_close'] - 1))
                    if theoretical > 0:
                        history.append(ep / theoretical - 1)
            values = history[-self.config.anchor_basis_days:]
            if len(values) >= 3:
                normal = float(np.median(values))
                spread = float(np.quantile(values, .75) - np.quantile(values, .25))
                if abs(normal) <= self.config.anchor_basis_cap and spread <= self.config.calibration_history_range:
                    self.normal_pair_basis = normal
                    target = reference['etf_close'] * (1 + self.multiple * (underlying / reference['underlying_close'] - 1)) * (1 + normal)
                    if target > 0:
                        self.reference_anchor = target
                        self.reference_gap = target / etf - 1
                        self.reference_status = 'validated'
                        if abs(self.reference_gap) > self.config.anchor_reference_cap:
                            self.reference_status = 'close_conflict'
                        elif abs(self.reference_gap) > self.config.calibration_cap:
                            self.anchor = stamp, target, underlying
                            self.anchor_source = 'cash_close_checked'
        self.anchor_adjustment = self.anchor[1] / self.raw_anchor[1] - 1 if self.raw_anchor else 0.
        self.revision += 1

    def describe(self) -> dict:
        return {'anchor': self.anchor, 'raw_anchor': self.raw_anchor, 'source': self.anchor_source,
                'anchor_adjustment': self.anchor_adjustment, 'normal_pair_basis': self.normal_pair_basis,
                'session_correction': self.correction, 'segment': self.segment_key,
                'reference_status': self.reference_status, 'reference_checked_ns': self.reference_checked_ns,
                'reference_anchor': self.reference_anchor, 'reference_gap': self.reference_gap,
                'calibrated': self.calibrated}

    def advance(self, now: int) -> None:
        key = self.calendar.segment(now)
        if key != self.segment_key:
            self.segment_key = key
            self.samples = []
            self.correction = 0.
            self.calibrated = False
            self._select_anchor(now)
        label, start = key
        if self.calibrated or now < start + 5 * MINUTE:
            return
        self.calibrated = True
        self._select_anchor(now)
        if len(self.samples) < 4:
            return
        fair = self.fair(now)
        if fair is None:
            return
        values = [etf / fair.price(underlying) - 1 for etf, underlying in self.samples if fair.price(underlying) > 0]
        if len(values) < 4:
            return
        med = float(np.median(values))
        iqr = float(np.quantile(values, .75) - np.quantile(values, .25))
        if iqr > self.config.calibration_iqr or abs(med) > self.config.calibration_cap:
            return
        prior = [(t, v) for t, v in self.history.get(label, []) if start - t <= self.config.history_days * DAY]
        values = [v for _, v in prior[-3:]]
        if len(values) >= 2 and np.ptp(values) <= self.config.calibration_history_range and abs(med - np.median(values)) <= self.config.calibration_history_distance:
            w = self.config.calibration_history_weight
            self.correction = float(w * np.median(values) + (1 - w) * med)
        self.history[label] = (prior + [(start + 5 * MINUTE, med)])[-64:]

    def minute(self, start: int, etf: float, underlying: float, valid: bool) -> None:
        if start <= self.last_minute:
            return
        self.last_minute = start
        self.advance(start)
        fair = self.fair(start)
        if valid and fair and start < self.segment_key[1] + 5 * MINUTE:
            theory = fair.e0 * (1 + self.multiple * (underlying / fair.u0 - 1))
            if theory > 0:
                self.samples.append((etf, underlying))
        self.close_samples.append((start, etf, underlying, valid))
        end = start + MINUTE
        # 零成交分钟的官方close仍是截至收盘的最后成交价；只在校准样本中排除零成交。
        if self.calendar.previous_close(end) == end and etf > 0 and underlying > 0:
            values = []
            for t, ep, up, good in self.close_samples:
                if not good or t < end - self.config.anchor_window_minutes * MINUTE:
                    continue
                if self.raw_anchor:
                    _, e0, u0 = self.raw_anchor
                    fair_then = e0 * (1 + self.multiple * (up / u0 - 1))
                    fair_close = e0 * (1 + self.multiple * (underlying / u0 - 1))
                    if fair_then > 0 and fair_close > 0:
                        values.append(ep / fair_then * fair_close)
            robust = float(np.median(values)) if len(values) >= self.config.anchor_window_minutes * 2 / 3 else etf
            self.raw_anchor = end, etf, underlying
            self.robust_anchor = end, robust, underlying
            self.anchor = self.robust_anchor
            self.contract_closes[end] = etf, underlying
            self.contract_closes = {t: v for t, v in self.contract_closes.items() if end - t <= self.config.history_days * DAY}
        self.advance(end)

    def fair(self, now: int) -> FairValue | None:
        if self.anchor is None or self.anchor[0] != self.calendar.previous_close(now):
            return None
        stamp, e0, u0 = self.anchor
        return FairValue(e0, u0, self.multiple, self.correction, stamp)

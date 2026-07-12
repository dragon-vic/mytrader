from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd


LOCAL_DENIALS_FILE = "local_denials.csv"


class BacktestMarginPool:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.reserved: dict[str, dict[str, Decimal]] = {}
        self.used: dict[str, dict[str, Decimal]] = {}
        self.denials: list[dict[str, Any]] = []

    # 两腿一次性检查并预占，避免同一事件内多个策略重复使用余额。
    def reserve(
        self,
        owner: str,
        required: dict[str, Decimal],
        totals: dict[str, Decimal],
        ts_ns: int,
    ) -> bool:
        if owner in self.reserved:
            raise RuntimeError(f"margin reservation already exists: {owner}")
        for venue, amount in required.items():
            used = self._venue_sum(self.used, venue)
            reserved = self._venue_sum(self.reserved, venue)
            available = totals[venue] - used - reserved
            if available < amount:
                self.denials.append(
                    {
                        "ts_ns": ts_ns,
                        "strategy_id": owner,
                        "venue": venue,
                        "total": totals[venue],
                        "used": used,
                        "reserved": reserved,
                        "available": available,
                        "required": amount,
                        "reason": "insufficient_margin",
                    },
                )
                return False
        self.reserved[owner] = dict(required)
        return True

    def release(self, owner: str) -> None:
        self.reserved.pop(owner, None)

    # 成交后把 pending 预占转成按实际成交价计算的已用保证金。
    def commit_open(self, owner: str, margins: dict[str, Decimal]) -> None:
        self.release(owner)
        current = self.used.setdefault(owner, {})
        for venue, amount in margins.items():
            current[venue] = current.get(venue, Decimal("0")) + amount

    def commit_reduce(self, owner: str, before_qty: Decimal, reduced_qty: Decimal) -> None:
        if before_qty <= 0:
            raise RuntimeError("before_qty must be positive")
        ratio = max((before_qty - reduced_qty) / before_qty, Decimal("0"))
        current = self.used.get(owner)
        if current is None:
            return
        for venue in current:
            current[venue] *= ratio
        if ratio == 0:
            self.used.pop(owner, None)

    def write_denials(self, output_dir: Path) -> None:
        path = output_dir / LOCAL_DENIALS_FILE
        if not self.denials:
            if path.exists():
                path.unlink()
            return
        pd.DataFrame(self.denials).to_csv(path, index=False, encoding="utf-8-sig")

    @staticmethod
    def _venue_sum(values: dict[str, dict[str, Decimal]], venue: str) -> Decimal:
        return sum((row.get(venue, Decimal("0")) for row in values.values()), Decimal("0"))


MARGIN_POOL = BacktestMarginPool()

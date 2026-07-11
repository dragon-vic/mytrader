from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds


BEIJING_TZ = timezone(timedelta(hours=8))
COLLECTOR_COLUMNS = ("ts_local_ns", "ts_exchange_ms", "venue", "symbol", "bid", "ask", "bid_size", "ask_size")


def quote_event_ns(row: dict[str, object]) -> int:
    return int(row["ts_exchange_ms"]) * 1_000_000


def _hour_keys(start_ns: int, end_ns: int) -> list[str]:
    start = datetime.fromtimestamp(start_ns / 1_000_000_000, BEIJING_TZ).replace(minute=0, second=0, microsecond=0)
    end = datetime.fromtimestamp(end_ns / 1_000_000_000, BEIJING_TZ).replace(minute=0, second=0, microsecond=0)
    keys = []
    current = start
    while current <= end:
        keys.append(current.strftime("%Y%m%d%H"))
        current += timedelta(hours=1)
    return keys


# 从 collector 的 merged/raw 文件读取指定时间段 quote。
def load_warmup_quotes(base_dir: Path, asset: str, start_ns: int, end_ns: int) -> list[dict[str, object]]:
    merged_dir = base_dir / "quote_merged"
    raw_dir = base_dir / "quote_raw"
    paths: list[Path] = []
    for key in _hour_keys(start_ns, end_ns):
        merged = merged_dir / asset / f"bidask1-{key}.parquet"
        if merged.exists():
            paths.append(merged)
        hour_dir = raw_dir / asset / key
        if hour_dir.exists():
            paths.extend(sorted(hour_dir.glob("*.parquet")))
    paths = sorted(set(paths), key=str)
    if not paths:
        raise RuntimeError("no bidask1 collector parquet files found for initial window")

    dataset = ds.dataset([str(path) for path in paths], format="parquet")
    filt = (
        (pc.field("ts_local_ns") >= pa.scalar(start_ns, pa.int64()))
        & (pc.field("ts_local_ns") <= pa.scalar(end_ns, pa.int64()))
        & (pc.field("ts_exchange_ms") > pa.scalar(0, pa.int64()))
        & pc.field("symbol").isin([asset])
    )
    rows = dataset.to_table(columns=list(COLLECTOR_COLUMNS), filter=filt).to_pylist()
    if not rows:
        raise RuntimeError("no bidask1 collector rows found for initial window")
    return sorted(rows, key=quote_event_ns)

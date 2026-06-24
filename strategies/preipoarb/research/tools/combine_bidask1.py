from __future__ import annotations

from pathlib import Path

import pandas as pd


CORE_COLUMNS = ["ts_local_ns", "venue", "symbol", "bid", "ask"]
OPTIONAL_COLUMNS = ["ts_exchange_ms", "raw_symbol", "bid_size", "ask_size", "sequence"]


# 扫描研究目录中符合 collector schema 的 parquet，合并为统一 hourly bidask1 数据。
def combine_bidask1(search_root: Path, output_root: Path) -> pd.DataFrame:
    output_root.mkdir(parents=True, exist_ok=True)
    files = [
        path
        for path in search_root.rglob("*.parquet")
        if output_root not in path.parents and is_bidask1_file(path)
    ]
    frames = [read_bidask1(path) for path in files]
    data = pd.concat(frames, ignore_index=True)
    data = data.dropna(subset=["ts_local_ns", "venue", "symbol", "bid", "ask"])
    data = data.drop_duplicates(subset=CORE_COLUMNS, keep="last")
    data["hour"] = pd.to_datetime(data["ts_local_ns"], unit="ns", utc=True).dt.tz_convert("Asia/Shanghai").dt.strftime("%Y%m%d%H")
    rows = []
    for hour, hour_data in data.groupby("hour", sort=True):
        hour_data = hour_data.drop(columns=["hour"]).sort_values(["ts_local_ns", "symbol", "venue"])
        file = output_root / "merged" / f"bidask1-{hour}.parquet"
        file.parent.mkdir(parents=True, exist_ok=True)
        hour_data.to_parquet(file, index=False)
        ts = pd.to_datetime(hour_data["ts_local_ns"], unit="ns", utc=True).dt.tz_convert("Asia/Shanghai")
        rows.append(
            {
                "hour": hour,
                "rows": len(hour_data),
                "start": ts.min(),
                "end": ts.max(),
                "file": str(file),
            },
        )
    manifest = pd.DataFrame(rows)
    manifest.to_csv(output_root / "manifest.csv", index=False)
    return manifest


def is_bidask1_file(path: Path) -> bool:
    try:
        pd.read_parquet(path, columns=CORE_COLUMNS)
    except Exception:
        return False
    return True


def read_bidask1(path: Path) -> pd.DataFrame:
    columns = CORE_COLUMNS.copy()
    try:
        sample = pd.read_parquet(path, nrows=0)
        columns.extend(column for column in OPTIONAL_COLUMNS if column in sample.columns)
    except TypeError:
        sample = pd.read_parquet(path)
        columns.extend(column for column in OPTIONAL_COLUMNS if column in sample.columns)
        return normalize(sample[columns])
    return normalize(pd.read_parquet(path, columns=columns))


def normalize(frame: pd.DataFrame) -> pd.DataFrame:
    for column in OPTIONAL_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    frame = frame[CORE_COLUMNS + OPTIONAL_COLUMNS].copy()
    frame["venue"] = frame["venue"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    return frame


def main() -> None:
    manifest = combine_bidask1(
        search_root=Path("strategies/preipoarb/research"),
        output_root=Path("strategies/preipoarb/research/bidask1-combined"),
    )
    print(manifest[["hour", "rows", "start", "end"]].to_string(index=False))


if __name__ == "__main__":
    main()

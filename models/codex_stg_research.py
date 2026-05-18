from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PREDICTIONS_PATH = ROOT / "models" / "funding_return_outputs" / "predictions.parquet"
OUT_DIR = ROOT / "models" / "codex_stg_outputs"
SIGNAL_PATH = ROOT / "data" / "signal" / "codex_stg" / "CODEX_STG-SIGNAL-20260301.parquet"

HORIZON_MS = 500
ENTRY_BEFORE_MS = 500
EXIT_AFTER_MS = 500
MIN_RATE_BPS = 50.0
MIN_SCORE_BPS = 10.0
FEE_BPS = 8.0
BAD_SYMBOLS = {"DODOX", "STORJ", "SIREN", "RIVER", "BARD", "PIPPIN", "ORCA"}


def bps_stats(frame: pd.DataFrame) -> dict[str, float]:
    values = frame["actual_net_bps"]
    return {
        "trades": int(len(frame)),
        "avg_bps": float(values.mean()) if len(values) else np.nan,
        "median_bps": float(values.median()) if len(values) else np.nan,
        "win_pct": float((values > 0).mean() * 100.0) if len(values) else np.nan,
        "sum_bps": float(values.sum()),
        "p25_bps": float(values.quantile(0.25)) if len(values) else np.nan,
        "p75_bps": float(values.quantile(0.75)) if len(values) else np.nan,
    }


def md_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_empty_"
    rows = [
        "| " + " | ".join(frame.columns) + " |",
        "| " + " | ".join(["---"] * len(frame.columns)) + " |",
    ]
    for _, row in frame.iterrows():
        vals = []
        for val in row.tolist():
            if isinstance(val, float):
                vals.append("" if np.isnan(val) else f"{val:.2f}")
            else:
                vals.append(str(val))
        rows.append("| " + " | ".join(vals) + " |")
    return "\n".join(rows)


def build_signals() -> pd.DataFrame:
    if not PREDICTIONS_PATH.exists():
        raise RuntimeError(f"missing predictions file: {PREDICTIONS_PATH}")

    df = pd.read_parquet(PREDICTIONS_PATH)
    df = df[(df["horizon_ms"] == HORIZON_MS) & (df["split"].isin(["valid", "test"]))].copy()
    df["funding_utc"] = pd.to_datetime(df["funding_utc"], utc=True)
    df["node"] = df["funding_utc"].dt.floor("min")
    df["predicted_cost_bps"] = df["hgb_p90"].astype(float)
    df["score_bps"] = df["abs_rate_bps"].astype(float) - df["predicted_cost_bps"] - FEE_BPS
    df["actual_net_bps"] = df[f"net_{HORIZON_MS}ms"].astype(float)

    mask = (
        (df["abs_rate_bps"].astype(float) >= MIN_RATE_BPS)
        & (df["score_bps"] >= MIN_SCORE_BPS)
        & (~df["base"].isin(BAD_SYMBOLS))
    )
    candidates = df[mask].copy()
    candidates = candidates.sort_values(["node", "score_bps", "abs_rate_bps"], ascending=[True, False, False])
    signals = candidates.groupby("node", sort=False).head(1).copy()
    signals = signals.sort_values("funding_time").reset_index(drop=True)

    signals["event_id"] = signals["symbol"].astype(str) + ":" + signals["funding_time"].astype("int64").astype(str)
    signals["instrument_id"] = signals["symbol"].astype(str) + "-PERP.BINANCE"
    signals["entry_time_ms"] = signals["funding_time"].astype("int64") - ENTRY_BEFORE_MS
    signals["exit_time_ms"] = signals["funding_time"].astype("int64") + EXIT_AFTER_MS
    signals["estimated_funding_income"] = signals["abs_rate_bps"].astype(float) / 10000.0
    keep = [
        "event_id",
        "symbol",
        "base",
        "instrument_id",
        "funding_time",
        "funding_utc",
        "split",
        "side",
        "rate_bps",
        "abs_rate_bps",
        "score_bps",
        "predicted_cost_bps",
        "actual_net_bps",
        "entry_time_ms",
        "exit_time_ms",
        "estimated_funding_income",
        "rate_bucket",
        "interval_h",
        "rank_abs",
        "node_1h_count",
        "tick_count_pre",
        "usdt_pre",
    ]
    return signals[keep]


def write_summary(signals: pd.DataFrame) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    for name, part in [("valid", signals[signals["split"] == "valid"]), ("test", signals[signals["split"] == "test"]), ("all", signals)]:
        item = bps_stats(part)
        item["split"] = name
        rows.append(item)
    stats = pd.DataFrame(rows)[["split", "trades", "avg_bps", "median_bps", "win_pct", "sum_bps", "p25_bps", "p75_bps"]]

    monthly = signals.copy()
    monthly["month"] = pd.to_datetime(monthly["funding_utc"], utc=True).dt.strftime("%Y-%m")
    monthly = monthly.groupby("month").apply(bps_stats, include_groups=False).apply(pd.Series).reset_index()

    top_symbols = (
        signals.groupby("base")
        .agg(
            trades=("event_id", "count"),
            avg_bps=("actual_net_bps", "mean"),
            sum_bps=("actual_net_bps", "sum"),
            win_pct=("actual_net_bps", lambda s: (s > 0).mean() * 100.0),
        )
        .sort_values("sum_bps", ascending=False)
        .reset_index()
        .head(20)
    )

    lines = [
        "# codex_stg research",
        "",
        "信号来源：funding 费率、funding 前 tick 流动性、历史同类事件价格冲击模型。",
        "",
        f"- 交易规则：每个 funding 节点取预测收益最高的 1 个事件",
        f"- 开平仓：t-{ENTRY_BEFORE_MS}ms 开仓，t+{EXIT_AFTER_MS}ms 平仓",
        f"- 过滤：abs(rate) >= {MIN_RATE_BPS:.0f}bps，预测净收益 >= {MIN_SCORE_BPS:.0f}bps，剔除历史差币",
        f"- 手续费假设：{FEE_BPS:.0f}bps",
        "",
        "## 分段结果",
        "",
        md_table(stats),
        "",
        "## 月度结果",
        "",
        md_table(monthly[["month", "trades", "avg_bps", "sum_bps", "win_pct", "p25_bps", "p75_bps"]]),
        "",
        "## 贡献最高标的",
        "",
        md_table(top_symbols),
        "",
    ]
    (OUT_DIR / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    stats.to_parquet(OUT_DIR / "stats.parquet", index=False)
    monthly.to_parquet(OUT_DIR / "monthly.parquet", index=False)
    top_symbols.to_parquet(OUT_DIR / "top_symbols.parquet", index=False)


def main() -> None:
    signals = build_signals()
    SIGNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    signals.to_parquet(SIGNAL_PATH, index=False)
    write_summary(signals)
    print(SIGNAL_PATH.resolve())
    print((OUT_DIR / "summary.md").resolve())
    print(signals[["split", "base", "funding_utc", "abs_rate_bps", "score_bps", "actual_net_bps"]].tail(10).to_string(index=False))


if __name__ == "__main__":
    main()

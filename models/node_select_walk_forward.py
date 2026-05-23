from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "8")

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import models.node_select_2000_optimize as opt


OUT_DIR = ROOT / "models" / "node_select_2000_opt_outputs"


def row_stats(name: str, score: str, top_k: int, threshold: float, valid_stats: dict[str, float], test_stats: dict[str, float], month: pd.Timestamp) -> dict[str, object]:
    return {
        "month": month.strftime("%Y-%m"),
        "variant": name,
        "score": score,
        "top_k": top_k,
        "threshold": threshold,
        **{f"valid_{key}": value for key, value in valid_stats.items()},
        **{f"test_{key}": value for key, value in test_stats.items()},
    }


def simple_eval(valid: pd.DataFrame, test: pd.DataFrame, month: pd.Timestamp) -> list[dict[str, object]]:
    rows = []
    for top_k in (1, 3):
        v = valid.assign(score_rate=valid["abs_rate_bps"])
        t = test.assign(score_rate=test["abs_rate_bps"])
        threshold, valid_stats = opt.choose_threshold(v, "score_rate", top_k)
        test_stats = opt.stat(t, opt.pick(t, "score_rate", top_k, threshold), top_k)
        rows.append(row_stats("simple_max_rate", "score_rate", top_k, threshold, valid_stats, test_stats, month))
    return rows


def xgb_eval(train: pd.DataFrame, valid: pd.DataFrame, test: pd.DataFrame, month: pd.Timestamp) -> list[dict[str, object]]:
    rows = []
    scored: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
    for variant in ("funding_hist", "market_hist", "pre_move", "market_pre"):
        numeric, categorical = opt.feature_sets()[variant]
        reg, clf = opt.fit_models(train, numeric, categorical, "sq")
        valid_s = opt.add_scores(valid, reg, clf, numeric, categorical)
        test_s = opt.add_scores(test, reg, clf, numeric, categorical)
        valid_s = add_groups(valid_s)
        test_s = add_groups(test_s)
        scored[variant] = (valid_s, test_s)
        for score_col in ("score_mul", "score_adj20"):
            for top_k in (1, 3):
                threshold, valid_stats = opt.choose_threshold(valid_s, score_col, top_k)
                test_stats = opt.stat(test_s, opt.pick(test_s, score_col, top_k, threshold), top_k)
                rows.append(row_stats(f"xgb_{variant}", score_col, top_k, threshold, valid_stats, test_stats, month))
                if variant != "funding_hist":
                    continue
                for group_col in ("node_type", "cand_bucket", "rate_bucket", "hist_bucket", "base"):
                    thresholds, valid_pick = choose_group_threshold(valid_s, score_col, top_k, group_col)
                    test_pick = apply_group_threshold(test_s, score_col, top_k, group_col, thresholds)
                    valid_stats = opt.stat(valid_s, valid_pick, top_k)
                    test_stats = opt.stat(test_s, test_pick, top_k)
                    name = f"xgb_{group_col}_threshold"
                    rows.append(row_stats(name, score_col, top_k, float(np.nan), valid_stats, test_stats, month))
    rows.extend(ensemble_eval(scored["funding_hist"], scored["market_hist"], month, "xgb_ensemble"))
    rows.extend(ensemble_eval(scored["pre_move"], scored["market_pre"], month, "xgb_pre_ensemble"))
    rows.extend(dynamic_rows(rows, month))
    return rows


def ensemble_eval(
    funding_parts: tuple[pd.DataFrame, pd.DataFrame],
    market_parts: tuple[pd.DataFrame, pd.DataFrame],
    month: pd.Timestamp,
    name: str,
) -> list[dict[str, object]]:
    rows = []
    for valid, test, market_valid, market_test in [(funding_parts[0], funding_parts[1], market_parts[0], market_parts[1])]:
        valid_e = add_ensemble(valid, market_valid)
        test_e = add_ensemble(test, market_test)
        for score_col in ("ens_avg_mul", "ens_min_mul", "ens_avg_adj20", "ens_rank_mul"):
            for top_k in (1, 3):
                threshold, valid_stats = opt.choose_threshold(valid_e, score_col, top_k)
                test_stats = opt.stat(test_e, opt.pick(test_e, score_col, top_k, threshold), top_k)
                rows.append(row_stats(name, score_col, top_k, threshold, valid_stats, test_stats, month))
        for top_k in (1, 3):
            f_threshold, m_threshold, valid_pick = choose_dual_threshold(valid_e, top_k)
            test_pick = apply_dual_threshold(test_e, top_k, f_threshold, m_threshold)
            valid_stats = opt.stat(valid_e, valid_pick, top_k)
            test_stats = opt.stat(test_e, test_pick, top_k)
            rows.append(row_stats(f"{name}_dual_veto", "score_mul+market_mul", top_k, f_threshold, valid_stats, test_stats, month))
    return rows


def add_ensemble(base: pd.DataFrame, other: pd.DataFrame) -> pd.DataFrame:
    cols = ["event_key", "score_mul", "score_adj20"]
    joined = base.merge(other[cols], on="event_key", suffixes=("", "_mkt"), how="left")
    joined["ens_avg_mul"] = (joined["score_mul"] + joined["score_mul_mkt"]) / 2.0
    joined["ens_min_mul"] = joined[["score_mul", "score_mul_mkt"]].min(axis=1)
    joined["ens_avg_adj20"] = (joined["score_adj20"] + joined["score_adj20_mkt"]) / 2.0
    joined["fund_rank"] = joined.groupby("node")["score_mul"].rank(method="average", pct=True)
    joined["mkt_rank"] = joined.groupby("node")["score_mul_mkt"].rank(method="average", pct=True)
    joined["ens_rank_mul"] = (joined["fund_rank"] + joined["mkt_rank"]) / 2.0
    return joined


def choose_dual_threshold(frame: pd.DataFrame, top_k: int) -> tuple[float, float, pd.DataFrame]:
    base = top_candidates(frame, "ens_min_mul", top_k)
    f_vals = base["score_mul"].replace([np.inf, -np.inf], np.nan).dropna()
    m_vals = base["score_mul_mkt"].replace([np.inf, -np.inf], np.nan).dropna()
    if f_vals.empty or m_vals.empty:
        return float("inf"), float("inf"), base.iloc[0:0].copy()
    f_grid = np.unique(np.r_[np.arange(-10.0, 41.0, 2.5), f_vals.quantile(np.linspace(0.1, 0.9, 9)).to_numpy()])
    m_grid = np.unique(np.r_[np.arange(-10.0, 41.0, 2.5), m_vals.quantile(np.linspace(0.1, 0.9, 9)).to_numpy()])
    min_trades = max(5, int(len(base) * 0.18))
    best_sum = -np.inf
    best_f = float(f_vals.quantile(0.8))
    best_m = float(m_vals.quantile(0.8))
    best_pick = base.iloc[0:0].copy()
    for f_threshold in f_grid:
        for m_threshold in m_grid:
            picked = base[(base["score_mul"] >= f_threshold) & (base["score_mul_mkt"] >= m_threshold)]
            if len(picked) < min_trades:
                continue
            total = float(picked["actual_net"].sum())
            if total > best_sum:
                best_sum = total
                best_f = float(f_threshold)
                best_m = float(m_threshold)
                best_pick = picked.copy()
    return best_f, best_m, best_pick


def apply_dual_threshold(frame: pd.DataFrame, top_k: int, f_threshold: float, m_threshold: float) -> pd.DataFrame:
    base = top_candidates(frame, "ens_min_mul", top_k)
    return base[(base["score_mul"] >= f_threshold) & (base["score_mul_mkt"] >= m_threshold)].copy()


def dynamic_rows(rows: list[dict[str, object]], month: pd.Timestamp) -> list[dict[str, object]]:
    out = []
    candidates = [row for row in rows if str(row["variant"]).startswith("xgb_")]
    for top_k in (1, 3):
        part = [row for row in candidates if int(row["top_k"]) == top_k and int(row["valid_trades"]) >= max(10, 20 * top_k)]
        if not part:
            continue
        best = max(part, key=lambda row: float(row["valid_sum_bps"]))
        item = dict(best)
        item["month"] = month.strftime("%Y-%m")
        item["selected_rule"] = f"{best['variant']}:{best['score']}"
        item["variant"] = "xgb_dynamic_best"
        item["score"] = "selected_by_valid"
        out.append(item)
    return out


def add_groups(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["cand_bucket"] = np.select(
        [
            out["candidate_n"] <= 1,
            out["candidate_n"].between(2, 3),
            out["candidate_n"].between(4, 7),
        ],
        ["1", "2-3", "4-7"],
        default="8+",
    )
    out["hist_bucket"] = np.select(
        [
            out["symbol_hist_count"] < 3,
            out["symbol_hist_count"].between(3, 9),
            out["symbol_hist_count"].between(10, 29),
        ],
        ["0-2", "3-9", "10-29"],
        default="30+",
    )
    return out


def top_candidates(frame: pd.DataFrame, score_col: str, top_k: int) -> pd.DataFrame:
    ordered = frame.sort_values(["node", score_col, "abs_rate_bps"], ascending=[True, False, False])
    return ordered.groupby("node", sort=False).head(top_k).copy()


def best_threshold(rows: pd.DataFrame, score_col: str) -> float:
    values = rows[score_col].replace([np.inf, -np.inf], np.nan).dropna()
    if values.empty:
        return float("inf")
    grid = np.unique(
        np.r_[
            np.arange(-50.0, 101.0, 2.5),
            values.quantile(np.linspace(0.05, 0.95, 19)).to_numpy(),
        ],
    )
    min_trades = max(5, int(len(rows) * 0.20))
    best = float(values.quantile(0.95))
    best_sum = -np.inf
    for threshold in grid:
        chosen = rows[rows[score_col] >= threshold]
        if len(chosen) < min_trades:
            continue
        total = float(chosen["actual_net"].sum())
        if total > best_sum:
            best_sum = total
            best = float(threshold)
    return best


def choose_group_threshold(frame: pd.DataFrame, score_col: str, top_k: int, group_col: str) -> tuple[dict[str, float], pd.DataFrame]:
    base = top_candidates(frame, score_col, top_k)
    fallback = best_threshold(base, score_col)
    thresholds: dict[str, float] = {"__fallback__": fallback}
    for group, rows in base.groupby(group_col, sort=False):
        thresholds[str(group)] = best_threshold(rows, score_col) if len(rows) >= 20 else fallback
    return thresholds, filter_group(base, score_col, group_col, thresholds)


def filter_group(base: pd.DataFrame, score_col: str, group_col: str, thresholds: dict[str, float]) -> pd.DataFrame:
    fallback = thresholds["__fallback__"]
    threshold = base[group_col].astype(str).map(thresholds).fillna(fallback).astype(float)
    return base[base[score_col] >= threshold].copy()


def apply_group_threshold(frame: pd.DataFrame, score_col: str, top_k: int, group_col: str, thresholds: dict[str, float]) -> pd.DataFrame:
    return filter_group(top_candidates(frame, score_col, top_k), score_col, group_col, thresholds)


def factor_eval(train: pd.DataFrame, valid: pd.DataFrame, test: pd.DataFrame, month: pd.Timestamp) -> list[dict[str, object]]:
    rows = opt.factor_rows(train, valid, test)
    out = []
    for row in rows:
        if row["variant"] != "linear_factor":
            continue
        if row["score"] not in {"factor_logit", "factor_combo"}:
            continue
        out.append({"month": month.strftime("%Y-%m"), **row})
    return out


def aggregate(rows: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["variant", "score", "top_k"]
    return (
        rows.groupby(group_cols)
        .agg(
            months=("month", "nunique"),
            trades=("test_trades", "sum"),
            active_nodes=("test_active_nodes", "sum"),
            sum_bps=("test_sum_bps", "sum"),
            avg_bps=("test_sum_bps", lambda s: np.nan),
            win_months=("test_sum_bps", lambda s: int((s > 0).sum())),
            min_month_bps=("test_sum_bps", "min"),
            median_month_bps=("test_sum_bps", "median"),
            avg_win_pct=("test_win_pct", "mean"),
        )
        .reset_index()
    )


def add_avg(summary: pd.DataFrame) -> pd.DataFrame:
    out = summary.copy()
    out["avg_bps"] = out["sum_bps"] / out["trades"].replace(0, np.nan)
    out["win_month_pct"] = out["win_months"] / out["months"] * 100.0
    return out.sort_values(["top_k", "sum_bps"], ascending=[True, False])


def md_table(frame: pd.DataFrame) -> str:
    return opt.md_table(frame)


def write_summary(rows: pd.DataFrame, summary: pd.DataFrame) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows.to_parquet(OUT_DIR / "walk_forward_monthly.parquet", index=False)
    summary.to_parquet(OUT_DIR / "walk_forward_summary.parquet", index=False)
    cols = [
        "variant",
        "score",
        "top_k",
        "months",
        "trades",
        "sum_bps",
        "avg_bps",
        "win_month_pct",
        "min_month_bps",
        "median_month_bps",
        "avg_win_pct",
    ]
    top1 = summary[summary["top_k"] == 1].head(12)
    top3 = summary[summary["top_k"] == 3].head(12)
    lines = [
        "# 2000ms Walk Forward",
        "",
        "每个月：用更早数据训练，用上个月验证集选阈值，用本月做测试。",
        "",
        "## Top1",
        "",
        md_table(top1[cols].round(2)),
        "",
        "## Top3",
        "",
        md_table(top3[cols].round(2)),
        "",
    ]
    (OUT_DIR / "walk_forward_summary.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    df = opt.load_frame()
    months = pd.date_range("2025-07-01", "2026-05-01", freq="MS", tz="UTC")
    rows = []
    for month in months:
        valid_start = month - pd.DateOffset(months=1)
        test_end = month + pd.DateOffset(months=1)
        train = df[df["funding_utc"] < valid_start].copy()
        valid = df[(df["funding_utc"] >= valid_start) & (df["funding_utc"] < month)].copy()
        test = df[(df["funding_utc"] >= month) & (df["funding_utc"] < test_end)].copy()
        if len(train) < 500 or valid.empty or test.empty:
            continue
        rows.extend(simple_eval(valid, test, month))
        rows.extend(xgb_eval(train, valid, test, month))
        rows.extend(factor_eval(train, valid, test, month))
        print(month.strftime("%Y-%m"), len(train), len(valid), len(test))
    monthly = pd.DataFrame(rows)
    summary = add_avg(aggregate(monthly))
    write_summary(monthly, summary)
    print((OUT_DIR / "walk_forward_summary.md").resolve())
    print(summary.head(20).round(2).to_string(index=False))


if __name__ == "__main__":
    main()

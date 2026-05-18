from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
from joblib import dump
from sklearn.pipeline import Pipeline
from xgboost import XGBClassifier

import models.node_select_model as base


OUT_DIR = ROOT / "models" / "top1_select_outputs"
HORIZONS_MS = (500, 1000, 3000)


def add_top1(df: pd.DataFrame, ms: int) -> pd.DataFrame:
    out = df.copy()
    out["actual_rank"] = out.groupby("node")["actual_net"].rank(method="first", ascending=False).astype(int)
    out["is_top1"] = (out["actual_rank"] == 1).astype(int)
    top = out[out["actual_rank"] == 1].set_index("node")["actual_net"]
    second = out[out["actual_rank"] == 2].set_index("node")["actual_net"]
    gap = (top - second).rename("top_gap")
    out = out.join(gap, on="node")
    out["top_gap"] = out["top_gap"].fillna(0.0).clip(lower=0.0)
    out["node_weight"] = 1.0 + np.log1p(out["top_gap"] / 10.0)
    neg_count = (out["candidate_n"] - 1).clip(lower=1)
    out["sample_weight"] = np.where(out["is_top1"] == 1, out["node_weight"] * neg_count, out["node_weight"])
    denom = (out["candidate_n"] - 1).replace(0, 1)
    out["node_abs_rank"] = out.groupby("node")["abs_rate_bps"].rank(method="first", ascending=False)
    out["node_abs_pct"] = (out["candidate_n"] - out["node_abs_rank"]) / denom
    out["abs_gap_to_max"] = out["candidate_abs_max"] - out["abs_rate_bps"]
    out["abs_ratio_to_max"] = out["abs_rate_bps"] / out["candidate_abs_max"].replace(0, np.nan)
    out["node_usdt_rank"] = out.groupby("node")["usdt_pre"].rank(method="first", ascending=False)
    out["node_usdt_pct"] = (out["candidate_n"] - out["node_usdt_rank"]) / denom
    out["node_hist_p75_rank"] = out.groupby("node")[f"symbol_hist_p75_cost_{ms}ms"].rank(method="first", ascending=True)
    out["node_hist_p75_pct"] = (out["candidate_n"] - out["node_hist_p75_rank"]) / denom
    return out


def top1_features(ms: int) -> tuple[list[str], list[str]]:
    numeric, categorical = base.feature_cols(ms)
    numeric = [
        *numeric,
        "node_abs_rank",
        "node_abs_pct",
        "abs_gap_to_max",
        "abs_ratio_to_max",
        "node_usdt_rank",
        "node_usdt_pct",
        "node_hist_p75_rank",
        "node_hist_p75_pct",
    ]
    return numeric, categorical


def make_model(numeric: list[str], categorical: list[str]) -> Pipeline:
    clf = XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        n_estimators=550,
        learning_rate=0.025,
        max_depth=3,
        min_child_weight=4,
        subsample=0.85,
        colsample_bytree=0.85,
        reg_lambda=4.0,
        reg_alpha=0.2,
        random_state=31,
        n_jobs=8,
    )
    return Pipeline([("pre", base.preprocessor(numeric, categorical)), ("clf", clf)])


def pick_by_score(df: pd.DataFrame, score_col: str) -> pd.DataFrame:
    return df.sort_values(["node", score_col, "abs_rate_bps"], ascending=[True, False, False]).groupby("node").head(1).copy()


def rank_stats(source: pd.DataFrame, picked: pd.DataFrame) -> dict[str, float]:
    rows = []
    for row in picked.itertuples(index=False):
        group = source[source["node"] == row.node].sort_values("actual_net", ascending=False).reset_index(drop=True)
        rank = int(group.index[group["event_key"].eq(row.event_key)][0]) + 1
        n = len(group)
        pct = 100.0 if n == 1 else (n - rank) / (n - 1) * 100.0
        rows.append(
            {
                "rank": rank,
                "pct": pct,
                "actual_net": float(row.actual_net),
                "best_net": float(group["actual_net"].iloc[0]),
            },
        )
    stats = pd.DataFrame(rows)
    return {
        "nodes": int(len(stats)),
        "hit_best_pct": float((stats["rank"] == 1).mean() * 100.0),
        "avg_rank": float(stats["rank"].mean()),
        "avg_percentile": float(stats["pct"].mean()),
        "sum_bps": float(stats["actual_net"].sum()),
        "best_sum_bps": float(stats["best_net"].sum()),
        "capture_pct": float(stats["actual_net"].sum() / stats["best_net"].sum() * 100.0),
    }


def print_row(row: dict[str, object]) -> None:
    cols = [
        "horizon",
        "model",
        "nodes",
        "hit_best_pct",
        "avg_rank",
        "avg_percentile",
        "sum_bps",
        "best_sum_bps",
        "capture_pct",
    ]
    print(" | ".join(str(row[col]) if not isinstance(row[col], float) else f"{row[col]:.2f}" for col in cols))


def train_one(ms: int) -> list[dict[str, object]]:
    df = add_top1(base.load_frame(ms), ms)
    train_end, valid_end = base.split_data(df)
    train = df[(df["funding_utc"] < train_end) & (df["candidate_n"] > 1)].copy()
    test = df[(df["funding_utc"] >= valid_end) & (df["candidate_n"] > 1)].copy()
    numeric, categorical = top1_features(ms)
    model = make_model(numeric, categorical)
    model.fit(
        base.model_frame(train, numeric, categorical),
        train["is_top1"].to_numpy(dtype=int),
        clf__sample_weight=train["sample_weight"].to_numpy(dtype=float),
    )

    scored = test.copy()
    scored["top1_prob"] = model.predict_proba(base.model_frame(scored, numeric, categorical))[:, 1]
    top1_pick = pick_by_score(scored, "top1_prob")
    rate_pick = pick_by_score(scored, "abs_rate_bps")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dump(
        {
            "horizon_ms": ms,
            "train_end": train_end,
            "valid_end": valid_end,
            "numeric": numeric,
            "categorical": categorical,
            "model": model,
        },
        OUT_DIR / f"top1_{ms}ms.joblib",
    )

    rows = []
    for name, picked in [("simple_max_rate", rate_pick), ("top1_classifier", top1_pick)]:
        item = rank_stats(test, picked)
        item.update({"horizon": f"t+{ms / 1000:g}s", "model": name})
        rows.append(item)
    return rows


def main() -> None:
    print("horizon | model | nodes | hit_best_pct | avg_rank | avg_percentile | sum_bps | best_sum_bps | capture_pct")
    for ms in HORIZONS_MS:
        for row in train_one(ms):
            print_row(row)


if __name__ == "__main__":
    main()

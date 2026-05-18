from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "8")

import numpy as np
import pandas as pd
from xgboost import XGBClassifier
from xgboost import XGBRegressor
from joblib import dump
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler


ROOT = Path(__file__).resolve().parents[1]
FEATURE_PATH = ROOT / "models" / "funding_return_outputs" / "event_features.parquet"
OUT_DIR = ROOT / "models" / "node_select_outputs"
HORIZON_MS = 1000
MIN_RATE_BPS = 30.0


def split_data(df: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp]:
    times = df["funding_utc"].sort_values().reset_index(drop=True)
    return times.iloc[int(len(times) * 0.70)], times.iloc[int(len(times) * 0.85)]


def feature_cols(ms: int) -> tuple[list[str], list[str]]:
    numeric = [
        "abs_rate_bps",
        "interval_h",
        "rank_abs",
        "node_1h_count",
        "node_30_count",
        "node_50_count",
        "node_top_abs",
        "node_mean_abs",
        "candidate_n",
        "candidate_abs_mean",
        "candidate_abs_max",
        "log_usdt_pre",
        "log_tick_pre",
        "pre_cost_bps",
        "pre_vol_bps",
        "entry_err_ms",
        "symbol_hist_count",
        f"symbol_hist_mean_cost_{ms}ms",
        f"symbol_hist_p75_cost_{ms}ms",
        "is_bad_symbol",
        "is_negative_funding",
    ]
    categorical = ["base", "rate_bucket", "node_type", "side"]
    return numeric, categorical


def load_frame(ms: int) -> pd.DataFrame:
    df = pd.read_parquet(FEATURE_PATH)
    df["funding_utc"] = pd.to_datetime(df["funding_utc"], utc=True)
    df = df[df["abs_rate_bps"].astype(float) >= MIN_RATE_BPS].copy()
    df["node"] = df["funding_utc"].dt.floor("min")
    df["actual_net"] = df[f"net_{ms}ms"].astype(float)
    df["positive"] = (df["actual_net"] > 0).astype(int)
    df["candidate_n"] = df.groupby("node")["event_key"].transform("count")
    df["candidate_abs_mean"] = df.groupby("node")["abs_rate_bps"].transform("mean")
    df["candidate_abs_max"] = df.groupby("node")["abs_rate_bps"].transform("max")
    return df.sort_values(["funding_utc", "symbol"]).reset_index(drop=True)


def model_frame(df: pd.DataFrame, numeric: list[str], categorical: list[str]) -> pd.DataFrame:
    out = df[numeric + categorical].copy()
    out[numeric] = out[numeric].replace([np.inf, -np.inf], np.nan)
    for col in categorical:
        out[col] = out[col].astype(str).fillna("missing")
    return out


def preprocessor(numeric: list[str], categorical: list[str]) -> ColumnTransformer:
    return ColumnTransformer(
        [
            ("num", Pipeline([("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())]), numeric),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False, min_frequency=5), categorical),
        ],
        verbose_feature_names_out=False,
    )


def make_reg(numeric: list[str], categorical: list[str]) -> Pipeline:
    reg = XGBRegressor(
        objective="reg:absoluteerror",
        n_estimators=450,
        learning_rate=0.03,
        max_depth=3,
        min_child_weight=8,
        subsample=0.85,
        colsample_bytree=0.85,
        reg_lambda=5.0,
        reg_alpha=0.2,
        random_state=20,
        n_jobs=8,
    )
    return Pipeline([("pre", preprocessor(numeric, categorical)), ("reg", reg)])


def make_clf(numeric: list[str], categorical: list[str]) -> Pipeline:
    clf = XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        n_estimators=350,
        learning_rate=0.035,
        max_depth=3,
        min_child_weight=8,
        subsample=0.85,
        colsample_bytree=0.85,
        reg_lambda=5.0,
        reg_alpha=0.2,
        random_state=19,
        n_jobs=8,
    )
    return Pipeline([("pre", preprocessor(numeric, categorical)), ("clf", clf)])


def score_nodes(df: pd.DataFrame, reg: Pipeline, clf: Pipeline, numeric: list[str], categorical: list[str]) -> pd.DataFrame:
    scored = df.copy()
    x = model_frame(scored, numeric, categorical)
    scored["pred_net"] = reg.predict(x)
    scored["pred_pos_prob"] = clf.predict_proba(x)[:, 1]
    scored = scored.sort_values(["node", "pred_net", "pred_pos_prob"], ascending=[True, False, False])
    best = scored.groupby("node", sort=False).head(1).copy()
    true_idx = df.groupby("node")["actual_net"].idxmax()
    true_best = df.loc[true_idx, ["node", "base", "actual_net"]].rename(
        columns={"base": "true_base", "actual_net": "true_best_net"},
    )
    best = best.merge(true_best, on="node", how="left")
    best["hit_best"] = best["base"] == best["true_base"]
    best["node_positive"] = best["true_best_net"] > 0
    best["chosen_positive"] = best["actual_net"] > 0
    return best


def choose_threshold(nodes: pd.DataFrame) -> float:
    min_trades = max(20, int(len(nodes) * 0.25))
    best_threshold = 0.0
    best_sum = -np.inf
    grid = np.unique(np.r_[np.arange(-50, 101, 2.5), nodes["pred_net"].quantile(np.linspace(0.05, 0.95, 19)).to_numpy()])
    for threshold in grid:
        chosen = nodes[nodes["pred_net"] >= threshold]
        if len(chosen) < min_trades:
            continue
        total = chosen["actual_net"].sum()
        if total > best_sum:
            best_sum = total
            best_threshold = float(threshold)
    return best_threshold


def node_stats(nodes: pd.DataFrame, threshold: float) -> dict[str, float]:
    chosen = nodes[nodes["pred_net"] >= threshold]
    return {
        "nodes": int(len(nodes)),
        "trades": int(len(chosen)),
        "hit_best_pct": float(nodes["hit_best"].mean() * 100.0),
        "node_positive_pct": float(nodes["node_positive"].mean() * 100.0),
        "binary_accuracy_pct": float(((nodes["pred_net"] >= threshold) == nodes["node_positive"]).mean() * 100.0),
        "chosen_win_pct": float((chosen["actual_net"] > 0).mean() * 100.0) if len(chosen) else np.nan,
        "chosen_avg_bps": float(chosen["actual_net"].mean()) if len(chosen) else np.nan,
        "chosen_sum_bps": float(chosen["actual_net"].sum()),
        "oracle_sum_bps": float(nodes["true_best_net"].sum()),
        "all_selected_sum_bps": float(nodes["actual_net"].sum()),
    }


def print_stats(name: str, stats: dict[str, float]) -> None:
    print(
        f"{name}: nodes={stats['nodes']} trades={stats['trades']} "
        f"hit_best={stats['hit_best_pct']:.2f}% binary_acc={stats['binary_accuracy_pct']:.2f}% "
        f"win={stats['chosen_win_pct']:.2f}% avg={stats['chosen_avg_bps']:.2f}bps "
        f"sum={stats['chosen_sum_bps']:.2f}bps oracle={stats['oracle_sum_bps']:.2f}bps",
    )


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = load_frame(HORIZON_MS)
    train_end, valid_end = split_data(df)
    train = df[df["funding_utc"] < train_end].copy()
    valid = df[(df["funding_utc"] >= train_end) & (df["funding_utc"] < valid_end)].copy()
    test = df[df["funding_utc"] >= valid_end].copy()

    numeric, categorical = feature_cols(HORIZON_MS)
    x_train = model_frame(train, numeric, categorical)
    reg = make_reg(numeric, categorical)
    clf = make_clf(numeric, categorical)
    reg.fit(x_train, train["actual_net"].to_numpy(dtype=float))
    clf.fit(x_train, train["positive"].to_numpy(dtype=int))

    valid_nodes = score_nodes(valid, reg, clf, numeric, categorical)
    threshold = choose_threshold(valid_nodes)
    test_nodes = score_nodes(test, reg, clf, numeric, categorical)

    dump(
        {
            "horizon_ms": HORIZON_MS,
            "min_rate_bps": MIN_RATE_BPS,
            "threshold": threshold,
            "train_end": train_end,
            "valid_end": valid_end,
            "numeric": numeric,
            "categorical": categorical,
            "reg": reg,
            "clf": clf,
        },
        OUT_DIR / "node_select_1000ms.joblib",
    )
    (OUT_DIR / "threshold.txt").write_text(
        f"horizon_ms={HORIZON_MS}\nmin_rate_bps={MIN_RATE_BPS:.2f}\nthreshold={threshold:.4f}\n",
        encoding="utf-8",
    )

    print(f"train_events={len(train)} valid_events={len(valid)} test_events={len(test)}")
    print(f"train_end={train_end} valid_end={valid_end} threshold={threshold:.2f}bps")
    print_stats("valid", node_stats(valid_nodes, threshold))
    print_stats("test", node_stats(test_nodes, threshold))
    one = test_nodes[test_nodes["candidate_n"] == 1]
    many = test_nodes[test_nodes["candidate_n"] > 1]
    print_stats("test_candidate_1", node_stats(one, threshold))
    print_stats("test_candidate_gt1", node_stats(many, threshold))


if __name__ == "__main__":
    main()

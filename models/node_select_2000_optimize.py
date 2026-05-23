from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "8")

import joblib
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier
from xgboost import XGBRegressor

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import models.node_select_model as node


FEATURE_PATH = ROOT / "models" / "funding_return_outputs" / "event_features.parquet"
FUNDING_PATH = ROOT / "data" / "funding" / "ALL-Funding-20250101.parquet"
KLINE_DIR = ROOT / "data" / "klines"
OUT_DIR = ROOT / "models" / "node_select_2000_opt_outputs"
MODEL_DIR = ROOT / "models" / "node_select_outputs"
HORIZON_MS = 2000
MIN_RATE_BPS = 30.0
TEST_START = pd.Timestamp("2026-01-01", tz="UTC")
RATE_BUCKET_ORD = {"30-50": 0, "50-75": 1, "75-100": 2, "100-150": 3, "150+": 4}


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


def load_frame() -> pd.DataFrame:
    df = pd.read_parquet(FEATURE_PATH)
    df["funding_utc"] = pd.to_datetime(df["funding_utc"], utc=True)
    df = df[df["abs_rate_bps"].astype(float) >= MIN_RATE_BPS].copy()
    df["node"] = df["funding_utc"].dt.floor("min")
    df["actual_net"] = df[f"net_{HORIZON_MS}ms"].astype(float)
    df["positive"] = (df["actual_net"] > 0).astype(int)
    df["candidate_n"] = df.groupby("node")["event_key"].transform("count")
    df["candidate_abs_mean"] = df.groupby("node")["abs_rate_bps"].transform("mean")
    df["candidate_abs_max"] = df.groupby("node")["abs_rate_bps"].transform("max")
    df["candidate_abs_std"] = df.groupby("node")["abs_rate_bps"].transform("std").fillna(0.0)
    denom = (df["candidate_n"] - 1).clip(lower=1)
    df["rate_bucket_ord"] = df["rate_bucket"].map(RATE_BUCKET_ORD).astype(float)
    df["node_abs_rank"] = df.groupby("node")["abs_rate_bps"].rank(method="first", ascending=False)
    df["node_abs_pct"] = (df["candidate_n"] - df["node_abs_rank"]) / denom
    df["abs_gap_to_max"] = df["candidate_abs_max"] - df["abs_rate_bps"]
    df["abs_ratio_to_max"] = df["abs_rate_bps"] / df["candidate_abs_max"].replace(0, np.nan)
    hist_col = f"symbol_hist_p75_cost_{HORIZON_MS}ms"
    df["node_hist_p75_rank"] = df.groupby("node")[hist_col].rank(method="first", ascending=True)
    df["node_hist_p75_pct"] = (df["candidate_n"] - df["node_hist_p75_rank"]) / denom
    df = add_funding_history(df)
    df = add_market_features(df)
    return df.sort_values(["funding_utc", "symbol"]).reset_index(drop=True)


def add_funding_history(df: pd.DataFrame) -> pd.DataFrame:
    fund = pd.read_parquet(FUNDING_PATH).sort_values(["symbol", "funding_utc"]).reset_index(drop=True)
    fund["funding_utc"] = pd.to_datetime(fund["funding_utc"], utc=True)
    fund["funding_hist_count"] = fund.groupby("symbol").cumcount()
    grp = fund.groupby("symbol", group_keys=False)
    fund["funding_prev_rate_bps"] = grp["rate_bps"].shift(1)
    fund["funding_prev_abs_bps"] = grp["abs_rate_bps"].shift(1)
    fund["funding_hist_abs_mean"] = grp["abs_rate_bps"].transform(lambda s: s.expanding().mean().shift(1))
    fund["funding_hist_abs_p90"] = grp["abs_rate_bps"].transform(lambda s: s.expanding().quantile(0.90).shift(1))
    fund["funding_roll3_abs_mean"] = grp["abs_rate_bps"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean())
    fund["funding_roll10_abs_mean"] = grp["abs_rate_bps"].transform(lambda s: s.shift(1).rolling(10, min_periods=1).mean())
    fund["funding_roll10_abs_max"] = grp["abs_rate_bps"].transform(lambda s: s.shift(1).rolling(10, min_periods=1).max())
    cols = [
        "symbol",
        "funding_time",
        "funding_hist_count",
        "funding_prev_rate_bps",
        "funding_prev_abs_bps",
        "funding_hist_abs_mean",
        "funding_hist_abs_p90",
        "funding_roll3_abs_mean",
        "funding_roll10_abs_mean",
        "funding_roll10_abs_max",
    ]
    return df.merge(fund[cols], on=["symbol", "funding_time"], how="left")


def market_cols() -> list[str]:
    bases = ["BTC", "ETH", "SOL"]
    cols = []
    for base in bases:
        for win in (5, 15, 60):
            cols.extend(
                [
                    f"mkt_{base.lower()}_ret_{win}m",
                    f"mkt_{base.lower()}_vol_{win}m",
                    f"mkt_{base.lower()}_quote_{win}m",
                    f"mkt_{base.lower()}_imb_{win}m",
                ],
            )
    return cols


def add_market_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["mkt_key"] = out["funding_utc"] - pd.Timedelta(minutes=1)
    for base in ("BTC", "ETH", "SOL"):
        path = KLINE_DIR / f"{base}USDT-SPOT-1M-20250101.parquet"
        if not path.exists():
            continue
        feats = build_market_frame(path, base)
        out = pd.merge_asof(
            out.sort_values("mkt_key"),
            feats,
            left_on="mkt_key",
            right_on="open_utc",
            direction="backward",
        ).drop(columns=["open_utc"])
    return out.drop(columns=["mkt_key"]).sort_values(["funding_utc", "symbol"]).reset_index(drop=True)


def build_market_frame(path: Path, base: str) -> pd.DataFrame:
    df = pd.read_parquet(path).sort_values("open_time").reset_index(drop=True)
    open_time = pd.to_datetime(df["open_time"], errors="coerce").dt.tz_localize("Asia/Shanghai").dt.tz_convert("UTC")
    close = df["close"].astype(float)
    quote = df["quote_volume"].astype(float)
    buy_quote = df["taker_buy_quote_volume"].astype(float)
    ret1 = np.log(close).diff() * 10000.0
    out = pd.DataFrame({"open_utc": open_time})
    prefix = f"mkt_{base.lower()}"
    for win in (5, 15, 60):
        out[f"{prefix}_ret_{win}m"] = np.log(close / close.shift(win)) * 10000.0
        out[f"{prefix}_vol_{win}m"] = ret1.rolling(win, min_periods=max(3, win // 3)).std()
        out[f"{prefix}_quote_{win}m"] = np.log1p(quote.rolling(win, min_periods=1).sum())
        total_quote = quote.rolling(win, min_periods=1).sum()
        total_buy = buy_quote.rolling(win, min_periods=1).sum()
        out[f"{prefix}_imb_{win}m"] = (2.0 * total_buy / total_quote.replace(0, np.nan)) - 1.0
    return out.dropna(subset=["open_utc"]).sort_values("open_utc").reset_index(drop=True)


def split_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    pre = df[df["funding_utc"] < TEST_START].copy()
    valid_start = pre["funding_utc"].sort_values().iloc[int(len(pre) * 0.85)]
    train = pre[pre["funding_utc"] < valid_start].copy()
    valid = pre[pre["funding_utc"] >= valid_start].copy()
    test = df[df["funding_utc"] >= TEST_START].copy()
    return train, valid, test, valid_start


def feature_sets() -> dict[str, tuple[list[str], list[str]]]:
    legacy = [
        "rate_bps",
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
        "rate_bucket_ord",
        "symbol_hist_count",
        f"symbol_hist_mean_cost_{HORIZON_MS}ms",
        f"symbol_hist_p75_cost_{HORIZON_MS}ms",
        "is_negative_funding",
    ]
    ranked = [
        *legacy,
        "candidate_abs_std",
        "node_abs_rank",
        "node_abs_pct",
        "abs_gap_to_max",
        "abs_ratio_to_max",
        "node_hist_p75_rank",
        "node_hist_p75_pct",
        "is_bad_symbol",
    ]
    funding_hist = [
        *ranked,
        "funding_hist_count",
        "funding_prev_rate_bps",
        "funding_prev_abs_bps",
        "funding_hist_abs_mean",
        "funding_hist_abs_p90",
        "funding_roll3_abs_mean",
        "funding_roll10_abs_mean",
        "funding_roll10_abs_max",
    ]
    market_hist = [*funding_hist, *market_cols()]
    pre_move = [*funding_hist, "pre_cost_bps"]
    market_pre = [*market_hist, "pre_cost_bps"]
    thin = [
        "rate_bps",
        "abs_rate_bps",
        "interval_h",
        "node_30_count",
        "node_top_abs",
        "candidate_n",
        "candidate_abs_max",
        "rate_bucket_ord",
        "node_abs_rank",
        "node_abs_pct",
        "abs_gap_to_max",
        "abs_ratio_to_max",
        "symbol_hist_count",
        f"symbol_hist_p75_cost_{HORIZON_MS}ms",
        "node_hist_p75_pct",
        "is_negative_funding",
    ]
    return {
        "legacy_no_liq": (legacy, ["node_type", "side"]),
        "ranked_no_liq": (ranked, ["node_type", "side"]),
        "funding_hist": (funding_hist, ["node_type", "side"]),
        "market_hist": (market_hist, ["node_type", "side"]),
        "pre_move": (pre_move, ["node_type", "side"]),
        "market_pre": (market_pre, ["node_type", "side"]),
        "thin_no_liq": (thin, ["node_type", "side"]),
    }


def make_reg(loss: str) -> Pipeline:
    objective = "reg:absoluteerror" if loss == "mae" else "reg:squarederror"
    reg = XGBRegressor(
        objective=objective,
        n_estimators=520,
        learning_rate=0.025,
        max_depth=3,
        min_child_weight=6,
        subsample=0.85,
        colsample_bytree=0.85,
        reg_lambda=6.0,
        reg_alpha=0.2,
        random_state=42 if loss == "mae" else 43,
        n_jobs=8,
    )
    return reg


def make_clf() -> XGBClassifier:
    return XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        n_estimators=420,
        learning_rate=0.03,
        max_depth=3,
        min_child_weight=6,
        subsample=0.85,
        colsample_bytree=0.85,
        reg_lambda=6.0,
        reg_alpha=0.2,
        random_state=44,
        n_jobs=8,
    )


def fit_models(train: pd.DataFrame, numeric: list[str], categorical: list[str], loss: str) -> tuple[Pipeline, Pipeline]:
    pre = node.preprocessor(numeric, categorical)
    reg = Pipeline([("pre", pre), ("reg", make_reg(loss))])
    clf = Pipeline([("pre", node.preprocessor(numeric, categorical)), ("clf", make_clf())])
    x_train = node.model_frame(train, numeric, categorical)
    reg.fit(x_train, train["actual_net"].to_numpy(dtype=float))
    clf.fit(x_train, train["positive"].to_numpy(dtype=int))
    return reg, clf


def add_scores(df: pd.DataFrame, reg: Pipeline, clf: Pipeline, numeric: list[str], categorical: list[str]) -> pd.DataFrame:
    out = df.copy()
    x = node.model_frame(out, numeric, categorical)
    out["pred_net"] = reg.predict(x)
    out["pred_pos_prob"] = clf.predict_proba(x)[:, 1]
    out["score_net"] = out["pred_net"]
    out["score_adj10"] = out["pred_net"] + 10.0 * (out["pred_pos_prob"] - 0.5)
    out["score_adj20"] = out["pred_net"] + 20.0 * (out["pred_pos_prob"] - 0.5)
    out["score_mul"] = out["pred_net"] * (0.5 + out["pred_pos_prob"])
    return out


def pick(frame: pd.DataFrame, score_col: str, top_k: int, threshold: float) -> pd.DataFrame:
    ordered = frame.sort_values(["node", score_col, "abs_rate_bps"], ascending=[True, False, False])
    picked = ordered.groupby("node", sort=False).head(top_k).copy()
    return picked[picked[score_col] >= threshold].copy()


def stat(source: pd.DataFrame, picked: pd.DataFrame, top_k: int) -> dict[str, float]:
    oracle = (
        source.sort_values(["node", "actual_net"], ascending=[True, False])
        .groupby("node", sort=False)
        .head(top_k)
    )
    oracle_pos = oracle[oracle["actual_net"] > 0]
    values = picked["actual_net"]
    active_nodes = picked["node"].nunique()
    return {
        "nodes": int(source["node"].nunique()),
        "active_nodes": int(active_nodes),
        "trades": int(len(picked)),
        "win_pct": float((values > 0).mean() * 100.0) if len(values) else np.nan,
        "avg_bps": float(values.mean()) if len(values) else np.nan,
        "sum_bps": float(values.sum()),
        "oracle_sum_bps": float(oracle_pos["actual_net"].sum()),
        "capture_pct": float(values.sum() / oracle_pos["actual_net"].sum() * 100.0) if len(oracle_pos) else np.nan,
    }


def choose_threshold(valid: pd.DataFrame, score_col: str, top_k: int) -> tuple[float, dict[str, float]]:
    values = valid[score_col].replace([np.inf, -np.inf], np.nan).dropna()
    grid = np.unique(
        np.r_[
            np.arange(-50.0, 101.0, 2.5),
            values.quantile(np.linspace(0.05, 0.95, 37)).to_numpy(),
        ],
    )
    min_trades = max(30, int(valid["node"].nunique() * 0.18 * top_k))
    best_threshold = float(grid[0])
    best_stats: dict[str, float] | None = None
    best_sum = -np.inf
    for threshold in grid:
        picked = pick(valid, score_col, top_k, float(threshold))
        if len(picked) < min_trades:
            continue
        stats = stat(valid, picked, top_k)
        if stats["sum_bps"] > best_sum:
            best_sum = stats["sum_bps"]
            best_threshold = float(threshold)
            best_stats = stats
    if best_stats is None:
        picked = pick(valid, score_col, top_k, float(values.quantile(0.5)))
        best_threshold = float(values.quantile(0.5))
        best_stats = stat(valid, picked, top_k)
    return best_threshold, best_stats


def simple_rate_rows(valid: pd.DataFrame, test: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for top_k in (1, 3):
        threshold, valid_stats = choose_threshold(valid.assign(score_rate=valid["abs_rate_bps"]), "score_rate", top_k)
        test_pick = pick(test.assign(score_rate=test["abs_rate_bps"]), "score_rate", top_k, threshold)
        test_stats = stat(test, test_pick, top_k)
        rows.append(
            {
                "variant": "simple_max_rate",
                "loss": "none",
                "score": "abs_rate_bps",
                "top_k": top_k,
                "threshold": threshold,
                **{f"valid_{key}": value for key, value in valid_stats.items()},
                **{f"test_{key}": value for key, value in test_stats.items()},
            },
        )
    return rows


def pct_rank(df: pd.DataFrame, col: str, high_good: bool) -> pd.Series:
    ranked = df.groupby("node")[col].rank(method="average", pct=True, ascending=True)
    if not high_good:
        ranked = 1.0 - ranked + (1.0 / df.groupby("node")[col].transform("count").clip(lower=1))
    return ranked.replace([np.inf, -np.inf], np.nan).fillna(0.5).clip(0.0, 1.0)


def add_factor_scores(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    cost_p75 = f"symbol_hist_p75_cost_{HORIZON_MS}ms"
    cost_mean = f"symbol_hist_mean_cost_{HORIZON_MS}ms"
    out["factor_abs"] = pct_rank(out, "abs_rate_bps", True)
    out["factor_gap"] = pct_rank(out, "abs_gap_to_max", False)
    out["factor_cost_p75"] = pct_rank(out, cost_p75, False)
    out["factor_cost_mean"] = pct_rank(out, cost_mean, False)
    out["factor_hist_count"] = pct_rank(out, "symbol_hist_count", True)
    out["factor_funding_recent"] = pct_rank(out, "funding_roll3_abs_mean", True)
    out["factor_funding_hist"] = pct_rank(out, "funding_hist_abs_p90", True)
    out["factor_bad"] = 1.0 - out["is_bad_symbol"].astype(float)
    out["factor_manual"] = (
        30.0 * out["factor_abs"]
        + 15.0 * out["factor_gap"]
        + 20.0 * out["factor_cost_p75"]
        + 10.0 * out["factor_cost_mean"]
        + 10.0 * out["factor_hist_count"]
        + 10.0 * out["factor_funding_recent"]
        + 5.0 * out["factor_bad"]
    )
    return out


def factor_cols() -> list[str]:
    return [
        "factor_abs",
        "factor_gap",
        "factor_cost_p75",
        "factor_cost_mean",
        "factor_hist_count",
        "factor_funding_recent",
        "factor_funding_hist",
        "factor_bad",
        "abs_rate_bps",
        "candidate_n",
        "interval_h",
        "rate_bucket_ord",
        "symbol_hist_count",
        f"symbol_hist_p75_cost_{HORIZON_MS}ms",
        "funding_hist_abs_p90",
        "funding_roll3_abs_mean",
    ]


def factor_rows(train: pd.DataFrame, valid: pd.DataFrame, test: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    train_f = add_factor_scores(train)
    valid_f = add_factor_scores(valid)
    test_f = add_factor_scores(test)
    for top_k in (1, 3):
        threshold, valid_stats = choose_threshold(valid_f, "factor_manual", top_k)
        test_stats = stat(test_f, pick(test_f, "factor_manual", top_k, threshold), top_k)
        rows.append(
            {
                "variant": "manual_factor",
                "loss": "none",
                "score": "factor_manual",
                "top_k": top_k,
                "threshold": threshold,
                **{f"valid_{key}": value for key, value in valid_stats.items()},
                **{f"test_{key}": value for key, value in test_stats.items()},
            },
        )

    cols = factor_cols()
    reg = make_pipeline(SimpleImputer(strategy="median"), StandardScaler(), Ridge(alpha=20.0))
    clf = make_pipeline(
        SimpleImputer(strategy="median"),
        StandardScaler(),
        LogisticRegression(C=0.4, max_iter=1000, class_weight="balanced"),
    )
    reg.fit(train_f[cols], train_f["actual_net"].to_numpy(dtype=float))
    clf.fit(train_f[cols], train_f["positive"].to_numpy(dtype=int))
    for part in (valid_f, test_f):
        part["factor_ridge"] = reg.predict(part[cols])
        part["factor_logit"] = clf.predict_proba(part[cols])[:, 1]
        part["factor_combo"] = part["factor_ridge"] * (0.5 + part["factor_logit"])
    for score_col in ("factor_ridge", "factor_combo", "factor_logit"):
        for top_k in (1, 3):
            threshold, valid_stats = choose_threshold(valid_f, score_col, top_k)
            test_stats = stat(test_f, pick(test_f, score_col, top_k, threshold), top_k)
            rows.append(
                {
                    "variant": "linear_factor",
                    "loss": "ridge_logit",
                    "score": score_col,
                    "top_k": top_k,
                    "threshold": threshold,
                    **{f"valid_{key}": value for key, value in valid_stats.items()},
                    **{f"test_{key}": value for key, value in test_stats.items()},
                },
            )
    return rows


def pair_base_cols() -> list[str]:
    numeric, _ = feature_sets()["funding_hist"]
    return numeric


def pair_frame(df: pd.DataFrame, cols: list[str]) -> tuple[pd.DataFrame, np.ndarray]:
    parts = []
    labels = []
    clean = df[["node", "actual_net", *cols]].replace([np.inf, -np.inf], np.nan).copy()
    med = clean[cols].median(numeric_only=True)
    clean[cols] = clean[cols].fillna(med).fillna(0.0)
    for _, group in clean.groupby("node", sort=False):
        if len(group) < 2:
            continue
        vals = group[cols].to_numpy(dtype=float)
        y = group["actual_net"].to_numpy(dtype=float)
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                if y[i] == y[j]:
                    continue
                diff = vals[i] - vals[j]
                label = 1 if y[i] > y[j] else 0
                parts.append(diff)
                labels.append(label)
                parts.append(-diff)
                labels.append(1 - label)
    return pd.DataFrame(parts, columns=[f"d_{col}" for col in cols]), np.asarray(labels, dtype=int)


def add_pair_score(train: pd.DataFrame, part: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    pair_x, pair_y = pair_frame(train, cols)
    clf = XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        n_estimators=260,
        learning_rate=0.035,
        max_depth=3,
        min_child_weight=8,
        subsample=0.85,
        colsample_bytree=0.85,
        reg_lambda=6.0,
        reg_alpha=0.2,
        random_state=51,
        n_jobs=8,
    )
    clf.fit(pair_x, pair_y)
    out = part.copy()
    out["pair_score"] = 0.0
    med = train[cols].replace([np.inf, -np.inf], np.nan).median(numeric_only=True)
    for _, group in out.groupby("node", sort=False):
        idx = group.index.to_list()
        if len(idx) == 1:
            out.loc[idx[0], "pair_score"] = 0.5
            continue
        vals = group[cols].replace([np.inf, -np.inf], np.nan).fillna(med).fillna(0.0).to_numpy(dtype=float)
        score = np.zeros(len(idx), dtype=float)
        for i in range(len(idx)):
            diffs = vals[i] - vals
            probs = clf.predict_proba(pd.DataFrame(diffs, columns=pair_x.columns))[:, 1]
            score[i] = float((probs.sum() - 0.5) / max(len(idx) - 1, 1))
        out.loc[idx, "pair_score"] = score
    return out


def pair_rows(train: pd.DataFrame, valid: pd.DataFrame, test: pd.DataFrame) -> list[dict[str, Any]]:
    cols = pair_base_cols()
    valid_s = add_pair_score(train, valid, cols)
    test_s = add_pair_score(train, test, cols)
    rows: list[dict[str, Any]] = []
    for top_k in (1, 3):
        threshold, valid_stats = choose_threshold(valid_s, "pair_score", top_k)
        test_stats = stat(test_s, pick(test_s, "pair_score", top_k, threshold), top_k)
        rows.append(
            {
                "variant": "pairwise_rank",
                "loss": "pair_logit",
                "score": "pair_score",
                "top_k": top_k,
                "threshold": threshold,
                **{f"valid_{key}": value for key, value in valid_stats.items()},
                **{f"test_{key}": value for key, value in test_stats.items()},
            },
        )
    return rows


def evaluate_saved_model(df: pd.DataFrame) -> list[dict[str, Any]]:
    path = MODEL_DIR / "node_select_2000ms_low_api.joblib"
    if not path.exists():
        return []
    model = joblib.load(path)
    numeric = list(model["numeric"])
    categorical = list(model["categorical"])
    model_df = df.copy()
    for col in numeric:
        if col not in model_df.columns:
            model_df[col] = np.nan
    for col in categorical:
        if col not in model_df.columns:
            model_df[col] = "missing"
    scored = add_scores(model_df, model["reg"], model["clf"], numeric, categorical)
    threshold = float(model["threshold"])
    rows = []
    test = scored[scored["funding_utc"] >= TEST_START].copy()
    for top_k in (1, 3):
        test_pick = pick(test, "score_net", top_k, threshold)
        test_stats = stat(test, test_pick, top_k)
        rows.append(
            {
                "variant": "saved_node_2000_low_api",
                "loss": "saved",
                "score": "score_net",
                "top_k": top_k,
                "threshold": threshold,
                **{f"valid_{key}": np.nan for key in test_stats},
                **{f"test_{key}": value for key, value in test_stats.items()},
            },
        )
    return rows


def train_variants(train: pd.DataFrame, valid: pd.DataFrame, test: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = simple_rate_rows(valid, test)
    rows.extend(factor_rows(train, valid, test))
    rows.extend(pair_rows(train, valid, test))
    best: dict[str, Any] = {}
    for variant, (numeric, categorical) in feature_sets().items():
        for loss in ("mae", "sq"):
            reg, clf = fit_models(train, numeric, categorical, loss)
            valid_scored = add_scores(valid, reg, clf, numeric, categorical)
            test_scored = add_scores(test, reg, clf, numeric, categorical)
            for score_col in ("score_net", "score_adj10", "score_adj20", "score_mul"):
                for top_k in (1, 3):
                    threshold, valid_stats = choose_threshold(valid_scored, score_col, top_k)
                    test_pick = pick(test_scored, score_col, top_k, threshold)
                    test_stats = stat(test_scored, test_pick, top_k)
                    row = {
                        "variant": variant,
                        "loss": loss,
                        "score": score_col,
                        "top_k": top_k,
                        "threshold": threshold,
                        **{f"valid_{key}": value for key, value in valid_stats.items()},
                        **{f"test_{key}": value for key, value in test_stats.items()},
                    }
                    rows.append(row)
                    if top_k == 1 and (not best or row["valid_sum_bps"] > best["row"]["valid_sum_bps"]):
                        best = {
                            "row": row,
                            "variant": variant,
                            "loss": loss,
                            "score": score_col,
                            "numeric": numeric,
                            "categorical": categorical,
                            "reg": reg,
                            "clf": clf,
                        }
    return pd.DataFrame(rows), best


def save_models(df: pd.DataFrame, best: dict[str, Any], valid_start: pd.Timestamp) -> None:
    row = best["row"]
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    eval_path = MODEL_DIR / "node_select_2000ms_opt_eval.joblib"
    live_path = MODEL_DIR / "node_select_2000ms_opt_live.joblib"
    payload = {
        "horizon_ms": HORIZON_MS,
        "min_rate_bps": MIN_RATE_BPS,
        "threshold": float(row["threshold"]),
        "score": best["score"],
        "variant": best["variant"],
        "loss": best["loss"],
        "train_end": valid_start,
        "valid_end": TEST_START,
        "numeric": best["numeric"],
        "categorical": best["categorical"],
        "reg": best["reg"],
        "clf": best["clf"],
    }
    joblib.dump(payload, eval_path)

    live_reg, live_clf = fit_models(df, best["numeric"], best["categorical"], best["loss"])
    live_payload = dict(payload)
    live_payload["train_end"] = df["funding_utc"].max()
    live_payload["valid_end"] = df["funding_utc"].max()
    live_payload["reg"] = live_reg
    live_payload["clf"] = live_clf
    joblib.dump(live_payload, live_path)


def ensemble_scores(base: pd.DataFrame, other: pd.DataFrame) -> pd.DataFrame:
    joined = base.merge(other[["event_key", "score_mul", "score_adj20"]], on="event_key", suffixes=("", "_mkt"), how="left")
    joined["ens_min_mul"] = joined[["score_mul", "score_mul_mkt"]].min(axis=1)
    joined["ens_avg_mul"] = (joined["score_mul"] + joined["score_mul_mkt"]) / 2.0
    joined["ens_avg_adj20"] = (joined["score_adj20"] + joined["score_adj20_mkt"]) / 2.0
    return joined


def save_ensemble_model(train: pd.DataFrame, valid: pd.DataFrame, full: pd.DataFrame) -> None:
    specs = []
    scored_valid = []
    scored_full = []
    for name in ("pre_move", "market_pre"):
        numeric, categorical = feature_sets()[name]
        reg, clf = fit_models(train, numeric, categorical, "sq")
        specs.append(
            {
                "name": name,
                "horizon_ms": HORIZON_MS,
                "numeric": numeric,
                "categorical": categorical,
                "score": "score_adj20",
                "reg": reg,
                "clf": clf,
            },
        )
        scored_valid.append(add_scores(valid, reg, clf, numeric, categorical))

        live_reg, live_clf = fit_models(full, numeric, categorical, "sq")
        scored_full.append((name, numeric, categorical, live_reg, live_clf))

    valid_ens = ensemble_scores(scored_valid[0], scored_valid[1])
    threshold, valid_stats = choose_threshold(valid_ens, "ens_avg_adj20", 1)
    live_specs = [
        {
            "name": name,
            "horizon_ms": HORIZON_MS,
            "numeric": numeric,
            "categorical": categorical,
            "score": "score_adj20",
            "reg": reg,
            "clf": clf,
        }
        for name, numeric, categorical, reg, clf in scored_full
    ]
    payload = {
        "horizon_ms": HORIZON_MS,
        "min_rate_bps": MIN_RATE_BPS,
        "threshold": float(threshold),
        "score": "ens_avg_adj20",
        "variant": "pre_ensemble_avg_adj20",
        "valid_stats": valid_stats,
        "models": live_specs,
    }
    joblib.dump(payload, MODEL_DIR / "node_select_2000ms_ensemble_live.joblib")


def write_summary(metrics: pd.DataFrame, train: pd.DataFrame, valid: pd.DataFrame, test: pd.DataFrame, best: dict[str, Any], valid_start: pd.Timestamp) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    metrics.to_parquet(OUT_DIR / "metrics.parquet", index=False)
    top = metrics.sort_values(["top_k", "valid_sum_bps", "test_sum_bps"], ascending=[True, False, False])
    top1 = top[top["top_k"] == 1].head(12).copy()
    top3 = top[top["top_k"] == 3].head(12).copy()
    cols = [
        "variant",
        "loss",
        "score",
        "top_k",
        "threshold",
        "valid_trades",
        "valid_win_pct",
        "valid_sum_bps",
        "test_trades",
        "test_win_pct",
        "test_avg_bps",
        "test_sum_bps",
        "test_capture_pct",
    ]
    best_row = pd.DataFrame([best["row"]])[cols]
    baseline = metrics[
        metrics["variant"].isin(["simple_max_rate", "saved_node_2000_low_api"])
    ].sort_values(["top_k", "variant"])[cols]
    lines = [
        "# 2000ms Funding Node Selection",
        "",
        "口径：t-500ms 开仓，t+2000ms 平仓成交，收益 = abs(funding) - 价格成本 - 10bps。",
        "",
        f"- 事件池：abs(funding) >= {MIN_RATE_BPS:.0f}bps",
        f"- 训练集：{train['funding_utc'].min()} 到 {valid_start}",
        f"- 验证集：{valid_start} 到 {TEST_START}",
        f"- 测试集：{TEST_START} 到 {test['funding_utc'].max()}",
        f"- 训练/验证/测试事件数：{len(train)} / {len(valid)} / {len(test)}",
        "",
        "## Validation Best Single Model",
        "",
        md_table(best_row.round(2)),
        "",
        "## Baseline Comparison",
        "",
        md_table(baseline.round(2)),
        "",
        "## Top1 Validation-Selected Results",
        "",
        md_table(top1[cols].round(2)),
        "",
        "## Top3 Validation-Selected Results",
        "",
        md_table(top3[cols].round(2)),
        "",
        "## Notes",
        "",
        "- 表格按验证集收益排序；测试集只用于最后外样本对比。",
        "- `simple_max_rate` 是每个节点按 funding 最大直接选币并只用验证集选择 rate 阈值。",
        "- `saved_node_2000_low_api` 是已有落盘 XGB 模型在同一 2026 测试窗口上的表现。",
        "- 当前实盘配置使用 walk-forward Top1 最优的 `pre_ensemble_avg_adj20`，不是单一静态验证集最优模型。",
        "- top3 在 2026 外样本总收益更高，但月度回撤也更大；默认实盘先使用 top1。",
    ]
    (OUT_DIR / "summary.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    df = load_frame()
    train, valid, test, valid_start = split_frame(df)
    metrics, best = train_variants(train, valid, test)
    metrics = pd.concat([metrics, pd.DataFrame(evaluate_saved_model(df))], ignore_index=True)
    save_models(df, best, valid_start)
    save_ensemble_model(train, valid, df)
    write_summary(metrics, train, valid, test, best, valid_start)
    print((OUT_DIR / "summary.md").resolve())
    print(metrics.sort_values(["top_k", "valid_sum_bps"], ascending=[True, False]).head(20).round(2).to_string(index=False))


if __name__ == "__main__":
    main()

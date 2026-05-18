from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "8")

import numpy as np
import pandas as pd
from joblib import dump
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


ROOT = Path(__file__).resolve().parents[1]
FUNDING_PATH = ROOT / "data" / "funding" / "ALL-20250101.parquet"
TICK_PATH = ROOT / "data" / "tick" / "event" / "ALL-FUNDING-EVENTS-20250101.parquet"
OUT_DIR = ROOT / "models" / "funding_return_outputs"
FEE_BPS = 8.0
WINDOWS_MS = (500, 1000, 3000)
BAD_SYMBOLS = {"DODOX", "STORJ", "SIREN", "RIVER", "BARD", "PIPPIN", "ORCA"}


@dataclass(frozen=True)
class Split:
    train_end: pd.Timestamp
    valid_end: pd.Timestamp


def bucket_rate(v: float) -> str:
    if v < 50:
        return "30-50"
    if v < 75:
        return "50-75"
    if v < 100:
        return "75-100"
    if v < 150:
        return "100-150"
    return "150+"


def node_type(ts: pd.Timestamp) -> str:
    hour = int(ts.hour)
    if hour % 8 == 0:
        return "8h"
    if hour % 4 == 0:
        return "4h"
    return "1h"


def nearest_price(rows: pd.DataFrame, target_ms: int) -> tuple[float, int]:
    rel = rows["rel_ms"].to_numpy()
    px = rows["price"].to_numpy()
    before = np.flatnonzero(rel <= target_ms)
    if len(before):
        idx = int(before[-1])
    else:
        idx = int(np.argmin(np.abs(rel - target_ms)))
    return float(px[idx]), int(rel[idx] - target_ms)


def calc_vol_bps(prices: np.ndarray) -> float:
    if len(prices) < 3:
        return np.nan
    ret = np.diff(np.log(prices)) * 10000.0
    return float(np.nanstd(ret))


def build_ticks() -> pd.DataFrame:
    ticks = pd.read_parquet(TICK_PATH)
    ticks["funding_utc"] = pd.to_datetime(ticks["funding_utc"], utc=True, format="mixed")
    ticks["rel_ms"] = ticks["timestamp_ms"].astype("int64") - ticks["funding_time"].astype("int64")
    ticks["notional"] = ticks["price"] * ticks["quantity"]
    out: list[dict[str, object]] = []

    for event_key, rows in ticks.sort_values(["event_key", "timestamp_ms", "agg_trade_id"]).groupby("event_key", sort=False):
        first = rows.iloc[0]
        entry_px, entry_err = nearest_price(rows, -500)
        direction = 1.0 if first["side"] == "BUY" else -1.0
        item: dict[str, object] = {
            "event_key": event_key,
            "symbol": first["symbol"],
            "base": str(first["symbol"]).removesuffix("USDT"),
            "funding_time": int(first["funding_time"]),
            "funding_utc": first["funding_utc"],
            "rate_bps": float(first["rate_bps"]),
            "abs_rate_bps": float(first["abs_rate_bps"]),
            "side": first["side"],
            "direction": direction,
            "entry_px": entry_px,
            "entry_err_ms": entry_err,
            "tick_count_all": int(len(rows)),
            "usdt_all": float(rows["notional"].sum()),
        }

        pre = rows[(rows["rel_ms"] >= -3000) & (rows["rel_ms"] <= -500)]
        core = rows[(rows["rel_ms"] >= -500) & (rows["rel_ms"] <= 500)]
        item["tick_count_pre"] = int(len(pre))
        item["usdt_pre"] = float(pre["notional"].sum())
        item["tick_count_core"] = int(len(core))
        item["usdt_core"] = float(core["notional"].sum())
        if len(pre) >= 2:
            pre_start = float(pre.iloc[0]["price"])
            pre_end = float(pre.iloc[-1]["price"])
            item["pre_return_bps"] = direction * (pre_end - pre_start) / pre_start * 10000.0
            item["pre_cost_bps"] = -float(item["pre_return_bps"])
            item["pre_vol_bps"] = calc_vol_bps(pre["price"].to_numpy())
        else:
            item["pre_return_bps"] = np.nan
            item["pre_cost_bps"] = np.nan
            item["pre_vol_bps"] = np.nan

        for ms in WINDOWS_MS:
            exit_px, exit_err = nearest_price(rows, ms)
            ret_bps = direction * (exit_px - entry_px) / entry_px * 10000.0
            cost_bps = -ret_bps
            item[f"exit_px_{ms}ms"] = exit_px
            item[f"exit_err_{ms}ms"] = exit_err
            item[f"price_return_{ms}ms"] = ret_bps
            item[f"price_cost_{ms}ms"] = cost_bps
            item[f"net_{ms}ms"] = float(item["abs_rate_bps"]) - cost_bps - FEE_BPS
        out.append(item)

    return pd.DataFrame(out)


def build_funding_features(events: pd.DataFrame) -> pd.DataFrame:
    fund = pd.read_parquet(FUNDING_PATH).sort_values(["symbol", "funding_utc"]).reset_index(drop=True)
    fund["funding_utc"] = pd.to_datetime(fund["funding_utc"], utc=True)
    fund["node"] = fund["funding_utc"].dt.floor("h")
    fund["prev"] = fund.groupby("symbol")["funding_utc"].shift(1)
    fund["gap_h"] = (fund["funding_utc"] - fund["prev"]).dt.total_seconds() / 3600.0
    fund["interval_h"] = np.select(
        [
            fund["gap_h"].between(0.5, 1.5),
            fund["gap_h"].between(3.0, 5.0),
            fund["gap_h"].between(7.0, 9.0),
        ],
        [1, 4, 8],
        default=np.nan,
    )

    rank = fund.sort_values(["node", "abs_rate_bps"], ascending=[True, False]).copy()
    rank["rank_abs"] = rank.groupby("node").cumcount() + 1
    node = fund.groupby("node").agg(
        node_symbols=("symbol", "nunique"),
        node_30_count=("abs_rate_bps", lambda s: int((s >= 30).sum())),
        node_50_count=("abs_rate_bps", lambda s: int((s >= 50).sum())),
        node_top_abs=("abs_rate_bps", "max"),
        node_mean_abs=("abs_rate_bps", "mean"),
    )
    one_h = fund[fund["interval_h"] == 1].groupby("node")["symbol"].nunique().rename("node_1h_count")
    node = node.join(one_h, how="left").fillna({"node_1h_count": 0})

    cols = ["symbol", "funding_time", "interval_h", "rank_abs", "node"]
    merged = events.merge(rank[cols], on=["symbol", "funding_time"], how="left")
    merged = merged.merge(node, on="node", how="left")
    merged["rate_bucket"] = merged["abs_rate_bps"].map(bucket_rate)
    merged["node_type"] = merged["node"].map(node_type)
    merged["is_bad_symbol"] = merged["base"].isin(BAD_SYMBOLS).astype(int)
    merged["is_negative_funding"] = (merged["rate_bps"] < 0).astype(int)
    merged["log_usdt_pre"] = np.log1p(merged["usdt_pre"])
    merged["log_tick_pre"] = np.log1p(merged["tick_count_pre"])
    merged["log_usdt_core"] = np.log1p(merged["usdt_core"])
    merged["rank_abs"] = merged["rank_abs"].fillna(999)
    merged["interval_h"] = merged["interval_h"].fillna(0).astype(int)

    merged = merged.sort_values(["symbol", "funding_utc"]).reset_index(drop=True)
    for ms in WINDOWS_MS:
        target = f"price_cost_{ms}ms"
        hist = merged.groupby("symbol")[target].expanding().mean().reset_index(level=0, drop=True)
        merged[f"symbol_hist_mean_cost_{ms}ms"] = hist.groupby(merged["symbol"]).shift(1)
        q = (
            merged.groupby("symbol")[target]
            .expanding()
            .quantile(0.75)
            .reset_index(level=0, drop=True)
        )
        merged[f"symbol_hist_p75_cost_{ms}ms"] = q.groupby(merged["symbol"]).shift(1)
    merged["symbol_hist_count"] = merged.groupby("symbol").cumcount()

    return merged.sort_values("funding_utc").reset_index(drop=True)


def split_data(df: pd.DataFrame) -> Split:
    times = df["funding_utc"].sort_values().reset_index(drop=True)
    train_end = times.iloc[int(len(times) * 0.70)]
    valid_end = times.iloc[int(len(times) * 0.85)]
    return Split(train_end=train_end, valid_end=valid_end)


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


def model_frame(df: pd.DataFrame, numeric: list[str], categorical: list[str]) -> pd.DataFrame:
    cols = numeric + categorical
    out = df[cols].copy()
    out[numeric] = out[numeric].replace([np.inf, -np.inf], np.nan)
    for col in categorical:
        out[col] = out[col].astype(str).fillna("missing")
    return out


def make_model(kind: str, numeric: list[str], categorical: list[str]) -> Pipeline:
    pre = ColumnTransformer(
        [
            ("num", Pipeline([("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())]), numeric),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False, min_frequency=5), categorical),
        ],
        verbose_feature_names_out=False,
    )
    if kind == "ridge":
        reg = Ridge(alpha=20.0)
    elif kind == "hgb_p75":
        reg = HistGradientBoostingRegressor(
            loss="quantile",
            quantile=0.75,
            max_iter=300,
            learning_rate=0.04,
            max_leaf_nodes=15,
            l2_regularization=1.0,
            random_state=7,
        )
    elif kind == "hgb_p90":
        reg = HistGradientBoostingRegressor(
            loss="quantile",
            quantile=0.90,
            max_iter=300,
            learning_rate=0.04,
            max_leaf_nodes=15,
            l2_regularization=1.0,
            random_state=7,
        )
    else:
        reg = HistGradientBoostingRegressor(
            loss="squared_error",
            max_iter=300,
            learning_rate=0.04,
            max_leaf_nodes=15,
            l2_regularization=1.0,
            random_state=7,
        )
    return Pipeline([("pre", pre), ("reg", reg)])


def group_stats(train: pd.DataFrame, target: str) -> list[tuple[list[str], pd.DataFrame]]:
    keys = [
        ["base", "rate_bucket", "interval_h"],
        ["base", "rate_bucket"],
        ["rate_bucket", "interval_h", "node_type"],
        ["rate_bucket", "interval_h"],
        ["rate_bucket"],
        [],
    ]
    tables: list[tuple[list[str], pd.DataFrame]] = []
    for key in keys:
        if key:
            table = train.groupby(key)[target].agg(["count", "mean", "median", lambda s: s.quantile(0.75), lambda s: s.quantile(0.90)])
            table = table.rename(columns={"<lambda_0>": "p75", "<lambda_1>": "p90"}).reset_index()
        else:
            s = train[target]
            table = pd.DataFrame(
                {
                    "count": [len(s)],
                    "mean": [s.mean()],
                    "median": [s.median()],
                    "p75": [s.quantile(0.75)],
                    "p90": [s.quantile(0.90)],
                }
            )
        tables.append((key, table))
    return tables


def predict_group(part: pd.DataFrame, tables: list[tuple[list[str], pd.DataFrame]], min_count: int = 5) -> pd.DataFrame:
    pred = pd.DataFrame(index=part.index, columns=["group_mean", "group_p75", "group_p90", "group_key"])
    remain = pd.Series(True, index=part.index)
    for key, table in tables:
        if not remain.any():
            break
        if key:
            joined = part.loc[remain, key].merge(table, on=key, how="left")
            ok = joined["count"].fillna(0) >= min_count
            idx = part.loc[remain].index[ok.to_numpy()]
        else:
            joined = pd.concat([table] * int(remain.sum()), ignore_index=True)
            idx = part.loc[remain].index
            ok = pd.Series(True, index=joined.index)
        if len(idx) == 0:
            continue
        pred.loc[idx, "group_mean"] = joined.loc[ok, "mean"].to_numpy()
        pred.loc[idx, "group_p75"] = joined.loc[ok, "p75"].to_numpy()
        pred.loc[idx, "group_p90"] = joined.loc[ok, "p90"].to_numpy()
        pred.loc[idx, "group_key"] = "+".join(key) if key else "global"
        remain.loc[idx] = False
    return pred.astype({"group_mean": float, "group_p75": float, "group_p90": float})


def trade_stats(df: pd.DataFrame, net_col: str, mask: pd.Series) -> dict[str, float]:
    sub = df[mask].copy()
    if sub.empty:
        return {"trades": 0, "avg": np.nan, "median": np.nan, "win_pct": np.nan, "sum": 0.0, "p25": np.nan, "p75": np.nan}
    s = sub[net_col]
    return {
        "trades": int(len(sub)),
        "avg": float(s.mean()),
        "median": float(s.median()),
        "win_pct": float((s > 0).mean() * 100.0),
        "sum": float(s.sum()),
        "p25": float(s.quantile(0.25)),
        "p75": float(s.quantile(0.75)),
    }


def md_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_empty_"
    show = df.copy()
    cols = [str(col) for col in show.columns]
    rows = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in show.iterrows():
        vals = []
        for val in row.tolist():
            if isinstance(val, float):
                vals.append("" if np.isnan(val) else f"{val:.3f}")
            else:
                vals.append(str(val))
        rows.append("| " + " | ".join(vals) + " |")
    return "\n".join(rows)


def evaluate(part: pd.DataFrame, ms: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    target = f"price_cost_{ms}ms"
    net = f"net_{ms}ms"
    rows = []
    for name, pred in [
        ("group_mean", "group_mean"),
        ("group_p75", "group_p75"),
        ("ridge_mean", "ridge_mean"),
        ("hgb_mean", "hgb_mean"),
        ("hgb_p75", "hgb_p75"),
        ("hgb_p90", "hgb_p90"),
    ]:
        err = part[pred] - part[target]
        rows.append(
            {
                "horizon": f"{ms}ms",
                "model": name,
                "mae_cost": float(err.abs().mean()),
                "bias_cost": float(err.mean()),
                "pred_net_avg": float((part["abs_rate_bps"] - part[pred] - FEE_BPS).mean()),
            }
        )
    metric = pd.DataFrame(rows)
    rules = {
        "all_events": pd.Series(True, index=part.index),
        "pred_mean_net_gt0": (part["abs_rate_bps"] - part["group_mean"] - FEE_BPS) > 0,
        "pred_p75_net_gt0": (part["abs_rate_bps"] - part["group_p75"] - FEE_BPS) > 0,
        "ridge_net_gt0": (part["abs_rate_bps"] - part["ridge_mean"] - FEE_BPS) > 0,
        "hgb_mean_net_gt0": (part["abs_rate_bps"] - part["hgb_mean"] - FEE_BPS) > 0,
        "hgb_p75_net_gt0": (part["abs_rate_bps"] - part["hgb_p75"] - FEE_BPS) > 0,
        "hgb_p90_net_gt0": (part["abs_rate_bps"] - part["hgb_p90"] - FEE_BPS) > 0,
        "oracle_net_gt0": part[net] > 0,
    }
    stats = []
    for name, mask in rules.items():
        item = trade_stats(part, net, mask)
        item.update({"horizon": f"{ms}ms", "rule": name})
        stats.append(item)
    return metric, pd.DataFrame(stats)


def save_md(path: Path, split: Split, events: pd.DataFrame, metrics: pd.DataFrame, trades: pd.DataFrame, bucket: pd.DataFrame) -> None:
    lines = [
        "# Funding Return Model",
        "",
        "第一版模型目标：预测方向对齐后的价格成本，再计算 `收益 = abs(funding) - 价格成本 - 8bps`。",
        "",
        f"- 事件数：{len(events)}",
        f"- 训练结束：{split.train_end}",
        f"- 验证结束：{split.valid_end}",
        f"- 测试开始：{split.valid_end}",
        f"- 手续费假设：{FEE_BPS:.2f}bps",
        "",
        "## 价格成本预测误差",
        "",
        md_table(metrics.round(3)),
        "",
        "## 测试集交易规则表现",
        "",
        md_table(trades.round(3)),
        "",
        "## 测试集按 Funding 桶分布",
        "",
        md_table(bucket.round(3)),
        "",
        "## 使用方式",
        "",
        "`hgb_p75` 和 `group_p75` 是保守模型。实盘筛选时可以先看 `abs_rate_bps - hgb_p75 - 8 > 0`，它的目标是减少被价差吃掉的事件。",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    events_path = OUT_DIR / "event_features.parquet"
    if events_path.exists():
        events = pd.read_parquet(events_path)
        events["funding_utc"] = pd.to_datetime(events["funding_utc"], utc=True)
    else:
        events = build_funding_features(build_ticks())
        events.to_parquet(events_path, index=False)

    split = split_data(events)
    train = events[events["funding_utc"] < split.train_end].copy()
    valid = events[(events["funding_utc"] >= split.train_end) & (events["funding_utc"] < split.valid_end)].copy()
    test = events[events["funding_utc"] >= split.valid_end].copy()

    all_metrics = []
    all_trades = []
    pred_parts = []

    for ms in WINDOWS_MS:
        target = f"price_cost_{ms}ms"
        tables = group_stats(train, target)
        for key, table in tables:
            name = "global" if not key else "__".join(key)
            table.to_parquet(OUT_DIR / f"group_model_{ms}ms_{name}.parquet", index=False)

        numeric, categorical = feature_cols(ms)
        x_train = model_frame(train, numeric, categorical)
        y_train = train[target].to_numpy(dtype=float)
        models = {
            "ridge_mean": make_model("ridge", numeric, categorical),
            "hgb_mean": make_model("hgb_mean", numeric, categorical),
            "hgb_p75": make_model("hgb_p75", numeric, categorical),
            "hgb_p90": make_model("hgb_p90", numeric, categorical),
        }
        for model_name, model in models.items():
            model.fit(x_train, y_train)
            dump(model, OUT_DIR / f"{model_name}_{ms}ms.joblib")

        scored = []
        for name, part in [("valid", valid), ("test", test)]:
            part = part.copy()
            gp = predict_group(part, tables)
            part = pd.concat([part, gp], axis=1)
            x_part = model_frame(part, numeric, categorical)
            for model_name, model in models.items():
                part[model_name] = model.predict(x_part)
            part["split"] = name
            part["horizon_ms"] = ms
            scored.append(part)
        scored_df = pd.concat(scored, ignore_index=True)
        pred_parts.append(scored_df)

        metric, trades = evaluate(scored_df[scored_df["split"] == "test"].copy(), ms)
        all_metrics.append(metric)
        all_trades.append(trades)

    predictions = pd.concat(pred_parts, ignore_index=True)
    predictions.to_parquet(OUT_DIR / "predictions.parquet", index=False)

    metrics = pd.concat(all_metrics, ignore_index=True)
    trades = pd.concat(all_trades, ignore_index=True)
    metrics.to_parquet(OUT_DIR / "metrics.parquet", index=False)
    trades.to_parquet(OUT_DIR / "trade_rules.parquet", index=False)

    test_pred = predictions[predictions["split"] == "test"].copy()
    bucket_rows = []
    for ms in WINDOWS_MS:
        net = f"net_{ms}ms"
        sub = test_pred[test_pred["horizon_ms"] == ms]
        for b, g in sub.groupby("rate_bucket"):
            row = trade_stats(g, net, pd.Series(True, index=g.index))
            row.update({"horizon": f"{ms}ms", "rate_bucket": b})
            bucket_rows.append(row)
    bucket = pd.DataFrame(bucket_rows)
    bucket.to_parquet(OUT_DIR / "bucket_stats.parquet", index=False)

    save_md(OUT_DIR / "summary.md", split, events, metrics, trades, bucket)
    print((OUT_DIR / "summary.md").resolve())
    print(trades.round(3).to_string(index=False))


if __name__ == "__main__":
    main()

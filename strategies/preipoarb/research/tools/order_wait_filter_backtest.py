from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


LOCAL_TZ = "Asia/Shanghai"
LONG_EDGE = "long_edge"
SHORT_EDGE = "short_edge"


# 读取采集到的 bidask1，并用策略当前公式构造 long/short edge。
def load_quotes(quote_root: Path, assets: list[str]) -> pd.DataFrame:
    files = sorted((quote_root / "merged").glob("*.parquet")) + sorted((quote_root / "raw").glob("**/*.parquet"))
    frames = []
    columns = ["ts_exchange_ms", "venue", "symbol", "bid", "ask", "bid_size", "ask_size"]
    for path in files:
        frame = pd.read_parquet(path, columns=columns)
        frame = frame[frame["symbol"].isin(assets)]
        if not frame.empty:
            frames.append(frame)
    if not frames:
        raise RuntimeError(f"no quote data under {quote_root}")
    quotes = pd.concat(frames, ignore_index=True)
    quotes = quotes.dropna(subset=["ts_exchange_ms", "venue", "symbol", "bid", "ask"])
    quotes = quotes.drop_duplicates(["ts_exchange_ms", "venue", "symbol", "bid", "ask"]).sort_values("ts_exchange_ms")
    quotes["ts"] = pd.to_datetime(quotes["ts_exchange_ms"].astype("int64"), unit="ms", utc=True).dt.tz_convert(LOCAL_TZ)
    return quotes.reset_index(drop=True)


def build_edges(quotes: pd.DataFrame, asset: str, center_minutes: int) -> pd.DataFrame:
    part = quotes[quotes["symbol"].eq(asset)].sort_values("ts")
    binance = part[part["venue"].eq("BINANCE")][["ts", "bid", "ask", "bid_size", "ask_size"]].rename(
        columns={"bid": "binance_bid", "ask": "binance_ask", "bid_size": "binance_bid_size", "ask_size": "binance_ask_size"},
    )
    okx = part[part["venue"].eq("OKX")][["ts", "bid", "ask", "bid_size", "ask_size"]].rename(
        columns={"bid": "okx_bid", "ask": "okx_ask", "bid_size": "okx_bid_size", "ask_size": "okx_ask_size"},
    )
    events = pd.concat([binance.assign(source="BINANCE"), okx.assign(source="OKX")], ignore_index=True)
    events = events.drop_duplicates(["ts", "source"], keep="last").set_index("ts").sort_index()
    for column in ["binance_bid", "binance_ask", "binance_bid_size", "binance_ask_size", "okx_bid", "okx_ask", "okx_bid_size", "okx_ask_size"]:
        events[column] = events[column].ffill()
    events = events.dropna(subset=["binance_bid", "binance_ask", "okx_bid", "okx_ask"]).reset_index()
    events = events.groupby("ts", as_index=False).last().sort_values("ts")
    mid = (events["binance_bid"] + events["binance_ask"]) / 2
    events[LONG_EDGE] = (events["okx_ask"] - events["binance_bid"]) / mid * 10000
    events[SHORT_EDGE] = (events["okx_bid"] - events["binance_ask"]) / mid * 10000

    minute = events.set_index("ts")[[LONG_EDGE, SHORT_EDGE]].resample("1min").mean().ffill()
    means = minute.rolling(center_minutes, min_periods=1).mean().rename(columns={LONG_EDGE: "long_mean", SHORT_EDGE: "short_mean"})
    events = pd.merge_asof(events.sort_values("ts"), means.reset_index().sort_values("ts"), on="ts", direction="backward")
    return events.reset_index(drop=True)


def load_params(config_path: Path) -> tuple[dict[str, float], int]:
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    params = config["strategy"]["params"]
    bands = {asset: float(asset_params["grid_band_bps"]) for asset, asset_params in params["asset_grid_params"].items()}
    center_minutes = int(params["grid_center_sec"]) // 60
    return bands, center_minutes


def load_actions(path: Path) -> pd.DataFrame:
    actions = pd.read_csv(path)
    actions["signal_ts"] = pd.to_datetime(actions["signal_ts"], utc=True).dt.tz_convert(LOCAL_TZ)
    actions["filled_ts"] = pd.to_datetime(actions["filled_ts"], utc=True).dt.tz_convert(LOCAL_TZ)
    for column in ["signal_edge", "actual_edge", "edge_slippage", "mean", "qty"]:
        actions[column] = pd.to_numeric(actions[column], errors="coerce")
    return actions.sort_values("signal_ts").reset_index(drop=True)


def edge_passes(row: pd.Series, side: str, band_bps: float) -> bool:
    if side == SHORT_EDGE:
        return float(row[SHORT_EDGE]) >= float(row["short_mean"]) + band_bps
    return float(row[LONG_EDGE]) <= float(row["long_mean"]) - band_bps


# 用日志 edge 值反推触发 quote 的 exchange timestamp，避免直接使用策略收到 quote 后写日志的时间。
def match_signal_quote(frame: pd.DataFrame, action: pd.Series, search_ms: int) -> tuple[int, pd.Series]:
    signal_ts = action["signal_ts"]
    side = action["edge_side"]
    nearby = frame[frame["ts"].between(signal_ts - pd.Timedelta(milliseconds=search_ms), signal_ts + pd.Timedelta(milliseconds=search_ms))]
    if nearby.empty:
        idx = max(0, frame["ts"].searchsorted(signal_ts, side="right") - 1)
        return int(idx), frame.iloc[idx]
    score = (nearby[side].astype(float) - float(action["signal_edge"])).abs()
    score += (nearby["ts"] - signal_ts).dt.total_seconds().abs() * 0.01
    idx = int(score.idxmin())
    return idx, frame.loc[idx]


def quote_at(quotes: pd.DataFrame, asset: str, venue: str, ts: pd.Timestamp) -> pd.Series | None:
    part = quotes[(quotes["symbol"].eq(asset)) & (quotes["venue"].eq(venue))].sort_values("ts")
    idx = part["ts"].searchsorted(ts, side="left")
    if idx >= len(part):
        return None
    return part.iloc[int(idx)]


def simulated_actual_edge(quotes: pd.DataFrame, asset: str, side: str, submit_ts: pd.Timestamp, binance_delay_ms: int, okx_delay_ms: int) -> dict[str, object] | None:
    bn = quote_at(quotes, asset, "BINANCE", submit_ts + pd.Timedelta(milliseconds=binance_delay_ms))
    okx = quote_at(quotes, asset, "OKX", submit_ts + pd.Timedelta(milliseconds=okx_delay_ms))
    if bn is None or okx is None:
        return None
    binance_mid = (float(bn["bid"]) + float(bn["ask"])) / 2
    if side == SHORT_EDGE:
        actual_edge = (float(okx["bid"]) - float(bn["ask"])) / binance_mid * 10000
        binance_px = float(bn["ask"])
        okx_px = float(okx["bid"])
    else:
        actual_edge = (float(okx["ask"]) - float(bn["bid"])) / binance_mid * 10000
        binance_px = float(bn["bid"])
        okx_px = float(okx["ask"])
    return {
        "actual_edge": actual_edge,
        "binance_ts": bn["ts"],
        "okx_ts": okx["ts"],
        "binance_px": binance_px,
        "okx_px": okx_px,
        "binance_bid_size": float(bn["bid_size"]),
        "binance_ask_size": float(bn["ask_size"]),
        "okx_bid_size": float(okx["bid_size"]),
        "okx_ask_size": float(okx["ask_size"]),
    }


def signed_slippage(side: str, signal_edge: float, actual_edge: float) -> float:
    if side == SHORT_EDGE:
        return actual_edge - signal_edge
    return signal_edge - actual_edge


def run_order_filter(
    actions: pd.DataFrame,
    quotes: pd.DataFrame,
    edges: dict[str, pd.DataFrame],
    bands: dict[str, float],
    wait_ms: int,
    binance_delay_ms: int,
    okx_delay_ms: int,
    search_ms: int,
) -> pd.DataFrame:
    rows = []
    for action in actions.itertuples(index=False):
        item = pd.Series(action._asdict())
        asset = item["asset"]
        side = item["edge_side"]
        frame = edges[asset]
        signal_idx, signal_quote = match_signal_quote(frame, item, search_ms)
        target_ts = signal_quote["ts"] + pd.Timedelta(milliseconds=wait_ms)
        recheck_idx = frame["ts"].searchsorted(target_ts, side="left")
        if recheck_idx >= len(frame):
            status = "DROP_NO_QUOTE"
            recheck = None
        else:
            recheck = frame.iloc[int(recheck_idx)]
            status = "KEEP" if edge_passes(recheck, side, bands[asset]) else "DROP_SIGNAL_GONE"

        fill = None
        sim_actual = np.nan
        sim_slippage = np.nan
        if status == "KEEP" and recheck is not None:
            fill = simulated_actual_edge(quotes, asset, side, recheck["ts"], binance_delay_ms, okx_delay_ms)
            if fill is None:
                status = "DROP_NO_FILL_QUOTE"
            else:
                sim_actual = float(fill["actual_edge"])
                sim_slippage = signed_slippage(side, float(recheck[side]), sim_actual)

        rows.append(
            {
                "lot": item["lot"],
                "asset": asset,
                "action": item["action"],
                "side": "SHORT" if side == SHORT_EDGE else "LONG",
                "edge_side": side,
                "qty": item["qty"],
                "status": status,
                "log_signal_ts": item["signal_ts"],
                "signal_quote_ts": signal_quote["ts"],
                "wait_quote_ts": pd.NaT if recheck is None else recheck["ts"],
                "original_signal_edge": item["signal_edge"],
                "matched_signal_edge": float(signal_quote[side]),
                "wait_signal_edge": np.nan if recheck is None else float(recheck[side]),
                "wait_mean": np.nan if recheck is None else float(recheck["short_mean"] if side == SHORT_EDGE else recheck["long_mean"]),
                "original_actual_edge": item["actual_edge"],
                "sim_actual_edge": sim_actual,
                "original_slippage": item["edge_slippage"],
                "sim_slippage": sim_slippage,
                "original_minus_sim_slippage": np.nan if pd.isna(sim_slippage) else float(item["edge_slippage"]) - sim_slippage,
                "binance_ts": pd.NaT if fill is None else fill["binance_ts"],
                "okx_ts": pd.NaT if fill is None else fill["okx_ts"],
                "binance_px": np.nan if fill is None else fill["binance_px"],
                "okx_px": np.nan if fill is None else fill["okx_px"],
                "binance_bid_size": np.nan if fill is None else fill["binance_bid_size"],
                "binance_ask_size": np.nan if fill is None else fill["binance_ask_size"],
                "okx_bid_size": np.nan if fill is None else fill["okx_bid_size"],
                "okx_ask_size": np.nan if fill is None else fill["okx_ask_size"],
            },
        )
    return pd.DataFrame(rows)


def summarize(result: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for name, group in list(result.groupby("asset")) + [("TOTAL", result)]:
        kept = group[group["status"].eq("KEEP")]
        dropped = group[~group["status"].eq("KEEP")]
        rows.append(
            {
                "asset": name,
                "orders": len(group),
                "kept": len(kept),
                "dropped": len(dropped),
                "drop_rate": len(dropped) / len(group) if len(group) else np.nan,
                "original_slip_sum": group["original_slippage"].sum(),
                "kept_original_slip_sum": kept["original_slippage"].sum(),
                "kept_sim_slip_sum": kept["sim_slippage"].sum(),
                "dropped_original_slip_sum": dropped["original_slippage"].sum(),
                "dropped_large_bad_20bps": int((dropped["original_slippage"] <= -20).sum()),
                "dropped_large_bad_40bps": int((dropped["original_slippage"] <= -40).sum()),
                "kept_large_bad_20bps": int((kept["sim_slippage"] <= -20).sum()),
                "kept_large_bad_40bps": int((kept["sim_slippage"] <= -40).sum()),
            },
        )
    return pd.DataFrame(rows)


def main() -> None:
    root = Path("strategies/preipoarb")
    quote_root = root / "research" / "local_collector"
    actions_path = root / "research" / "analyst" / "charts" / "actions_from_log.csv"
    output_dir = root / "research" / "analyst" / "order_wait_filter_100ms"

    wait_ms = 100
    binance_delay_ms = 50
    okx_delay_ms = 100
    search_ms = 1500

    actions = load_actions(actions_path)
    assets = sorted(actions["asset"].unique())
    bands, center_minutes = load_params(root / "preipo_arb.yaml")
    quotes = load_quotes(quote_root, assets)
    edges = {asset: build_edges(quotes, asset, center_minutes) for asset in assets}
    result = run_order_filter(actions, quotes, edges, bands, wait_ms, binance_delay_ms, okx_delay_ms, search_ms)
    summary = summarize(result)

    output_dir.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_dir / "order_wait_filter_100ms.csv", index=False)
    summary.to_csv(output_dir / "summary.csv", index=False)

    print(f"wait={wait_ms}ms binance_delay={binance_delay_ms}ms okx_delay={okx_delay_ms}ms")
    print(summary.to_string(index=False, float_format=lambda value: f"{value:.2f}"))
    print()
    print("dropped orders:")
    dropped = result[result["status"].ne("KEEP")]
    columns = ["lot", "asset", "action", "side", "status", "original_slippage", "original_signal_edge", "wait_signal_edge", "wait_mean"]
    print(dropped[columns].to_string(index=False, float_format=lambda value: f"{value:.2f}"))
    print()
    print(f"wrote {output_dir / 'order_wait_filter_100ms.csv'}")


if __name__ == "__main__":
    main()

from __future__ import annotations

from dataclasses import dataclass
from math import floor
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


LONG_EDGE = "long_edge"
SHORT_EDGE = "short_edge"


@dataclass
class Position:
    side: str
    edge: float


@dataclass
class GridParams:
    band_bps: float
    step_bps: float
    confirm_quotes: int
    max_inventory: int
    min_capture_bps: float
    center_minutes: int
    min_window_minutes: int


# 读取 bidask1 parquet，并按策略当前公式构造 long/short 两条 edge 线。
def load_edges(quote_root: Path, assets: list[str], start: pd.Timestamp, end: pd.Timestamp, center_minutes: int) -> dict[str, pd.DataFrame]:
    files = sorted((quote_root / "merged").glob("*.parquet")) + sorted((quote_root / "raw").glob("**/*.parquet"))
    parts = []
    for file in files:
        frame = pd.read_parquet(file, columns=["ts_local_ns", "venue", "symbol", "bid", "ask", "bid_size", "ask_size"])
        parts.append(frame)
    raw = pd.concat(parts, ignore_index=True)
    raw = raw[raw["symbol"].isin(assets)].dropna(subset=["bid", "ask"]).copy()
    raw["ts"] = pd.to_datetime(raw["ts_local_ns"], unit="ns", utc=True).dt.tz_convert("Asia/Shanghai")
    raw = raw[(raw["ts"] >= start - pd.Timedelta(minutes=center_minutes + 30)) & (raw["ts"] <= end)]
    raw = raw.sort_values(["symbol", "ts", "venue"])
    return {asset: build_asset_edges(raw, asset, start, end, center_minutes) for asset in assets}


def build_asset_edges(raw: pd.DataFrame, asset: str, start: pd.Timestamp, end: pd.Timestamp, center_minutes: int) -> pd.DataFrame:
    asset_raw = raw[raw["symbol"] == asset].sort_values("ts")
    binance = asset_raw[asset_raw["venue"] == "BINANCE"][["ts", "bid", "ask", "bid_size", "ask_size"]].rename(
        columns={"bid": "binance_bid", "ask": "binance_ask", "bid_size": "binance_bid_size", "ask_size": "binance_ask_size"},
    )
    okx = asset_raw[asset_raw["venue"] == "OKX"][["ts", "bid", "ask", "bid_size", "ask_size"]].rename(
        columns={"bid": "okx_bid", "ask": "okx_ask", "bid_size": "okx_bid_size", "ask_size": "okx_ask_size"},
    )
    events = pd.concat([binance.assign(source="binance"), okx.assign(source="okx")], ignore_index=True)
    events = events.drop_duplicates(subset=["ts", "source"], keep="last").set_index("ts").sort_index()
    for column in ["binance_bid", "binance_ask", "binance_bid_size", "binance_ask_size", "okx_bid", "okx_ask", "okx_bid_size", "okx_ask_size"]:
        events[column] = events[column].ffill()
    events = events.dropna(subset=["binance_bid", "binance_ask", "okx_bid", "okx_ask"]).reset_index()
    events = events.groupby("ts", as_index=False).last().sort_values("ts")
    binance_mid = (events["binance_bid"] + events["binance_ask"]) / 2
    events[LONG_EDGE] = (events["okx_ask"] - events["binance_bid"]) / binance_mid * 10000
    events[SHORT_EDGE] = (events["okx_bid"] - events["binance_ask"]) / binance_mid * 10000

    minute = events.set_index("ts")[[LONG_EDGE, SHORT_EDGE]].resample("1min").mean().ffill()
    means = minute.rolling(center_minutes, min_periods=1).mean().rename(
        columns={LONG_EDGE: "long_mean", SHORT_EDGE: "short_mean"},
    )
    events = pd.merge_asof(events, means.reset_index().sort_values("ts"), on="ts", direction="backward")
    events["window_sec"] = (events["ts"] - minute.index[0]).dt.total_seconds()
    return events[(events["ts"] >= start) & (events["ts"] <= end)].reset_index(drop=True)


def run_delay_sweep(
    quote_root: Path,
    output_dir: Path,
    start: str,
    end: str,
    delays_ms: list[int],
    assets: list[str],
    params: GridParams,
) -> pd.DataFrame:
    output_dir.mkdir(parents=True, exist_ok=True)
    start_ts = pd.Timestamp(start, tz="Asia/Shanghai")
    end_ts = pd.Timestamp(end, tz="Asia/Shanghai")
    edges = load_edges(quote_root, assets, start_ts, end_ts, params.center_minutes)
    rows = []
    for delay_ms in delays_ms:
        actions = simulate(edges, delay_ms, params, end_ts)
        actions.to_csv(output_dir / f"actions_{delay_ms}ms.csv", index=False)
        rows.extend(summarize(actions, delay_ms))
    summary = pd.DataFrame(rows)
    summary.to_csv(output_dir / "delay_summary.csv", index=False)
    return summary


def run_single(
    quote_root: Path,
    output_dir: Path,
    start: str,
    end: str,
    delay_ms: int,
    assets: list[str],
    params: GridParams,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    output_dir.mkdir(parents=True, exist_ok=True)
    start_ts = pd.Timestamp(start, tz="Asia/Shanghai")
    end_ts = pd.Timestamp(end, tz="Asia/Shanghai")
    edges = load_edges(quote_root, assets, start_ts, end_ts, params.center_minutes)
    actions = simulate(edges, delay_ms, params, end_ts)
    actions.to_csv(output_dir / f"actions_{delay_ms}ms.csv", index=False)
    pd.DataFrame(summarize(actions, delay_ms)).to_csv(output_dir / f"summary_{delay_ms}ms.csv", index=False)
    return edges, actions


def simulate(edges: dict[str, pd.DataFrame], delay_ms: int, params: GridParams, end_ts: pd.Timestamp) -> pd.DataFrame:
    actions = []
    for asset, frame in edges.items():
        positions: list[Position] = []
        confirmations: dict[str, dict[str, int]] = {}
        blocked_until = pd.Timestamp("1970-01-01", tz="Asia/Shanghai")
        i = 0
        while i < len(frame):
            row = frame.iloc[i]
            if row.ts <= blocked_until:
                i += 1
                continue
            candidate = choose_candidate(asset, row, positions, confirmations, params)
            if candidate is None:
                i += 1
                continue
            side, close_idx, signal_edge = candidate
            fill_at = row.ts + pd.Timedelta(milliseconds=delay_ms)
            fill_idx = frame["ts"].searchsorted(fill_at, side="left")
            if fill_idx >= len(frame):
                break
            fill = frame.iloc[fill_idx]
            apply_action(actions, asset, row.ts, fill, side, signal_edge, positions, close_idx)
            confirmations.clear()
            blocked_until = fill.ts
            i = fill_idx + 1
        stop_positions(actions, asset, frame, positions, end_ts)
    return pd.DataFrame(actions).sort_values(["time", "asset"]).reset_index(drop=True)


def choose_candidate(
    asset: str,
    row: pd.Series,
    positions: list[Position],
    confirmations: dict[str, dict[str, int]],
    params: GridParams,
) -> tuple[str, int | None, float] | None:
    candidates = []
    inventory = inventory_of(positions)
    for side in [LONG_EDGE, SHORT_EDGE]:
        level = grid_level(row, side, params)
        if level is None:
            confirmations.pop(side, None)
            continue
        reducing = reduces_inventory(inventory, side)
        close_idx = best_close(row, side, positions, params) if reducing else None
        if reducing and close_idx is None:
            continue
        if not reducing and not can_open(row, side, inventory, positions, params):
            continue
        state = confirmations.setdefault(side, {"level": level, "count": 0})
        if state["level"] != level:
            state["level"] = level
            state["count"] = 0
        state["count"] += 1
        if state["count"] >= params.confirm_quotes:
            mean = row["short_mean"] if side == SHORT_EDGE else row["long_mean"]
            candidates.append((abs(float(row[side]) - float(mean)), side, close_idx, float(row[side])))
    if not candidates:
        return None
    _, side, close_idx, signal_edge = max(candidates, key=lambda item: item[0])
    return side, close_idx, signal_edge


def grid_level(row: pd.Series, side: str, params: GridParams) -> int | None:
    if row["window_sec"] < params.min_window_minutes * 60:
        return None
    edge = float(row[side])
    mean = float(row["short_mean"] if side == SHORT_EDGE else row["long_mean"])
    deviation = edge - mean
    if side == SHORT_EDGE and deviation >= params.band_bps:
        return int(floor((deviation - params.band_bps) / params.step_bps))
    if side == LONG_EDGE and deviation <= -params.band_bps:
        return int(floor((abs(deviation) - params.band_bps) / params.step_bps))
    return None


def inventory_of(positions: list[Position]) -> int:
    return sum(1 if position.side == LONG_EDGE else -1 for position in positions)


def reduces_inventory(inventory: int, side: str) -> bool:
    return (inventory > 0 and side == SHORT_EDGE) or (inventory < 0 and side == LONG_EDGE)


def best_close(row: pd.Series, side: str, positions: list[Position], params: GridParams) -> int | None:
    best_idx = None
    best_capture = -np.inf
    for idx, position in enumerate(positions):
        if position.side == side:
            continue
        capture = float(row[SHORT_EDGE]) - position.edge if position.side == LONG_EDGE else position.edge - float(row[LONG_EDGE])
        if capture >= params.min_capture_bps and capture > best_capture:
            best_idx = idx
            best_capture = capture
    return best_idx


def can_open(row: pd.Series, side: str, inventory: int, positions: list[Position], params: GridParams) -> bool:
    if side == LONG_EDGE and inventory >= params.max_inventory:
        return False
    if side == SHORT_EDGE and inventory <= -params.max_inventory:
        return False
    same_edges = [position.edge for position in positions if position.side == side]
    edge = float(row[side])
    if side == SHORT_EDGE and same_edges and edge < max(same_edges) + params.step_bps:
        return False
    if side == LONG_EDGE and same_edges and edge > min(same_edges) - params.step_bps:
        return False
    return True


def apply_action(
    actions: list[dict[str, object]],
    asset: str,
    signal_time: pd.Timestamp,
    fill: pd.Series,
    side: str,
    signal_edge: float,
    positions: list[Position],
    close_idx: int | None,
) -> None:
    before = inventory_of(positions)
    actual_edge = float(fill[side])
    action = "OPEN"
    qty = 0.01
    capture = np.nan
    if close_idx is not None:
        closed = positions.pop(close_idx)
        capture = actual_edge - closed.edge if closed.side == LONG_EDGE else closed.edge - actual_edge
        if abs(before) == 1:
            positions.append(Position(side, actual_edge))
            action = "FLIP"
            qty = 0.02
        else:
            action = "CLOSE"
    else:
        positions.append(Position(side, actual_edge))
    after = inventory_of(positions)
    slippage = actual_edge - signal_edge if side == SHORT_EDGE else signal_edge - actual_edge
    actions.append(
        {
            "time": fill.ts,
            "signal_time": signal_time,
            "asset": asset,
            "action": action,
            "edge_side": side,
            "qty": qty,
            "signal_edge": signal_edge,
            "actual_edge": actual_edge,
            "slippage": slippage,
            "inventory": f"{before}->{after}",
            "realized_capture": capture,
        },
    )


def stop_positions(actions: list[dict[str, object]], asset: str, frame: pd.DataFrame, positions: list[Position], end_ts: pd.Timestamp) -> None:
    if not positions:
        return
    final = frame[frame["ts"] <= end_ts].iloc[-1]
    for position in positions:
        side = SHORT_EDGE if position.side == LONG_EDGE else LONG_EDGE
        actual_edge = float(final[side])
        capture = actual_edge - position.edge if position.side == LONG_EDGE else position.edge - actual_edge
        actions.append(
            {
                "time": final.ts,
                "signal_time": pd.NaT,
                "asset": asset,
                "action": "STOP",
                "edge_side": side,
                "qty": 0.01,
                "signal_edge": np.nan,
                "actual_edge": actual_edge,
                "slippage": np.nan,
                "inventory": "-",
                "realized_capture": capture,
            },
        )


def summarize(actions: pd.DataFrame, delay_ms: int) -> list[dict[str, object]]:
    rows = []
    for asset, group in actions.groupby("asset"):
        rows.append(summary_row(group, delay_ms, asset))
    rows.append(summary_row(actions, delay_ms, "TOTAL"))
    return rows


def summary_row(actions: pd.DataFrame, delay_ms: int, asset: str) -> dict[str, object]:
    closed = actions[actions["realized_capture"].notna()]
    return {
        "delay_ms": delay_ms,
        "asset": asset,
        "actions": len(actions),
        "completed_positions": len(closed),
        "realized_bps": float(closed["realized_capture"].sum()),
        "avg_bps": float(closed["realized_capture"].mean()) if len(closed) else np.nan,
        "slippage_bps": float(actions["slippage"].dropna().sum()),
        "avg_slippage_bps": float(actions["slippage"].dropna().mean()) if actions["slippage"].notna().any() else np.nan,
    }


# 按 EDGE_CHART_STYLE.md 输出全周期 edge 点、3h 均线、信号线和交易标记。
def plot_backtest(edges: dict[str, pd.DataFrame], actions: pd.DataFrame, output_dir: Path, delay_ms: int, params: GridParams) -> list[Path]:
    paths = []
    for asset, frame in edges.items():
        asset_actions = actions[actions["asset"] == asset].copy()
        path = output_dir / f"backtest_{delay_ms}ms_{asset.lower()}_edge_orders.png"
        plot_asset(frame, asset_actions, asset, path, delay_ms, params)
        paths.append(path)
    return paths


def plot_asset(frame: pd.DataFrame, actions: pd.DataFrame, asset: str, path: Path, delay_ms: int, params: GridParams) -> None:
    minute = frame.set_index("ts")[[LONG_EDGE, SHORT_EDGE]].resample("1min").mean().ffill()
    means = minute.rolling(params.center_minutes, min_periods=1).mean()
    long_signal = means[LONG_EDGE] - params.band_bps
    short_signal = means[SHORT_EDGE] + params.band_bps

    fig, ax = plt.subplots(figsize=(32, 8.5), dpi=150)
    ax.scatter(frame["ts"], frame[LONG_EDGE], s=4, alpha=0.35, color="#2b6cb0", label="long edge")
    ax.scatter(frame["ts"], frame[SHORT_EDGE], s=4, alpha=0.35, color="#dd6b20", label="short edge")
    ax.plot(means.index, means[LONG_EDGE], color="#1a365d", linewidth=1.4, label="long 3h mean")
    ax.plot(means.index, means[SHORT_EDGE], color="#9c4221", linewidth=1.4, label="short 3h mean")
    ax.plot(long_signal.index, long_signal, color="#2f855a", linewidth=1.2, linestyle="--", label="long signal")
    ax.plot(short_signal.index, short_signal, color="#b83280", linewidth=1.2, linestyle="--", label="short signal")

    label_offsets = [18, -24, 32, -38, 48, -54]
    y_values = [frame[LONG_EDGE].min(), frame[LONG_EDGE].max(), frame[SHORT_EDGE].min(), frame[SHORT_EDGE].max(), long_signal.min(), long_signal.max(), short_signal.min(), short_signal.max()]
    for idx, row in actions.reset_index(drop=True).iterrows():
        side = row["edge_side"]
        is_long = side == LONG_EDGE
        is_close = row["action"] in {"CLOSE", "STOP"}
        marker = "^" if is_long else "v"
        color = "#2f855a" if is_long else "#c53030"
        face = "none" if is_close else color
        edge = float(row["actual_edge"])
        ax.scatter([row["time"]], [edge], s=95, marker=marker, facecolors=face, edgecolors=color, linewidths=1.7, zorder=5)
        text = f"{'LONG' if is_long else 'SHORT'} qty={float(row['qty']):.2f} {edge:.1f} bps"
        offset = label_offsets[idx % len(label_offsets)]
        ax.annotate(
            text,
            xy=(row["time"], edge),
            xytext=(0, offset),
            textcoords="offset points",
            ha="center",
            va="bottom" if offset > 0 else "top",
            fontsize=7.5,
            color=color,
            arrowprops={"arrowstyle": "-", "color": color, "lw": 0.6, "alpha": 0.75},
        )
        y_values.extend([edge, edge + offset * 0.18])

    ymin = min(value for value in y_values if pd.notna(value))
    ymax = max(value for value in y_values if pd.notna(value))
    pad = max(10, (ymax - ymin) * 0.08)
    ax.set_ylim(ymin - pad, ymax + pad)
    ax.set_xlim(frame["ts"].min() - pd.Timedelta(minutes=20), frame["ts"].max() + pd.Timedelta(minutes=20))
    ax.set_title(f"{asset} grid backtest ({delay_ms}ms fill delay)")
    ax.set_ylabel("edge bps")
    ax.grid(True, alpha=0.25)
    ax.legend(loc="upper left", ncol=3, fontsize=8)
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=4))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M", tz=frame["ts"].dt.tz))
    fig.autofmt_xdate(rotation=30, ha="right")
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path)
    plt.close(fig)


def main() -> None:
    quote_root = Path("strategies/preipoarb/research/bidask1-combined")
    manifest = pd.read_csv(quote_root / "manifest.csv")
    start = pd.to_datetime(manifest["start"]).min().tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S")
    end = pd.to_datetime(manifest["end"]).max().tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S")
    params = GridParams(
        band_bps=30,
        step_bps=25,
        confirm_quotes=2,
        max_inventory=4,
        min_capture_bps=0,
        center_minutes=180,
        min_window_minutes=120,
    )
    output_dir = Path("strategies/preipoarb/research/full_quote_backtest_100ms")
    edges, actions = run_single(
        quote_root=quote_root,
        output_dir=output_dir,
        start=start,
        end=end,
        delay_ms=100,
        assets=["OPENAI", "ANTHROPIC"],
        params=params,
    )
    summary = pd.DataFrame(summarize(actions, 100))
    plot_backtest(edges, actions, output_dir, 100, params)
    print(f"range {start} -> {end}")
    print(summary.to_string(index=False, float_format=lambda value: f"{value:.2f}"))


if __name__ == "__main__":
    main()

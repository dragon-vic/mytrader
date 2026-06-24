from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import time

import numpy as np
import pandas as pd


LONG_EDGE = "long_edge"
SHORT_EDGE = "short_edge"
LOCAL_TZ = "Asia/Shanghai"


@dataclass
class Pos:
    side: str
    qty: float
    bn_px: float
    okx_px: float
    edge: float
    signal_edge: float
    fees: float
    layers: int


@dataclass
class Params:
    band_bps: float = 30.0
    add_bps: float = 40.0
    base_qty: float = 0.5
    add_qty: float = 0.2
    max_layers: int = 2
    fee_bps: float = 5.0
    slip_bps: float = 2.0
    binance_delay_ms: int = 50
    okx_delay_ms: int = 100
    center_minutes: int = 180
    min_window_minutes: int = 120


class Quotes:
    def __init__(self, raw: pd.DataFrame) -> None:
        self.frames = {venue: frame.sort_values("ts").reset_index(drop=True) for venue, frame in raw.groupby("venue")}
        self.times = {venue: frame["ts"].astype("int64").to_numpy() for venue, frame in self.frames.items()}
        self.bid = {venue: frame["bid"].astype(float).to_numpy() for venue, frame in self.frames.items()}
        self.ask = {venue: frame["ask"].astype(float).to_numpy() for venue, frame in self.frames.items()}

    def at(self, venue: str, ts: pd.Timestamp) -> tuple[pd.Timestamp, float, float] | None:
        idx = np.searchsorted(self.times[venue], ts.value, side="left")
        if idx >= len(self.times[venue]):
            return None
        idx = int(idx)
        return pd.Timestamp(self.times[venue][idx], tz=LOCAL_TZ), float(self.bid[venue][idx]), float(self.ask[venue][idx])


# 读取 collector quote，使用 exchange timestamp 构造事件级 edge 和 3h 时间均线。
def load_edges(root: Path, asset: str, start: pd.Timestamp, params: Params) -> tuple[pd.DataFrame, pd.DataFrame]:
    warm_start = start - pd.Timedelta(minutes=params.center_minutes + 30)
    files = quote_files(root, warm_start)
    parts = []
    for path in files:
        frame = pd.read_parquet(path, columns=["ts_exchange_ms", "venue", "symbol", "bid", "ask", "bid_size", "ask_size"])
        frame = frame[frame["symbol"].eq(asset)]
        if not frame.empty:
            parts.append(frame)
    if not parts:
        raise RuntimeError(f"no quote data for {asset}")

    raw = pd.concat(parts, ignore_index=True).dropna(subset=["ts_exchange_ms", "bid", "ask"])
    raw["ts"] = pd.to_datetime(raw["ts_exchange_ms"].astype("int64"), unit="ms", utc=True).dt.tz_convert(LOCAL_TZ)
    raw = raw[raw["ts"] >= warm_start]
    raw = raw.drop_duplicates(["ts", "venue", "bid", "ask"]).sort_values(["ts", "venue"])

    binance = raw[raw["venue"].eq("BINANCE")][["ts", "bid", "ask", "bid_size", "ask_size"]].rename(
        columns={"bid": "bn_bid", "ask": "bn_ask", "bid_size": "bn_bid_size", "ask_size": "bn_ask_size"},
    )
    okx = raw[raw["venue"].eq("OKX")][["ts", "bid", "ask", "bid_size", "ask_size"]].rename(
        columns={"bid": "okx_bid", "ask": "okx_ask", "bid_size": "okx_bid_size", "ask_size": "okx_ask_size"},
    )
    events = pd.concat([binance.assign(source="BINANCE"), okx.assign(source="OKX")], ignore_index=True)
    events = events.drop_duplicates(["ts", "source"], keep="last").set_index("ts").sort_index()
    for col in ["bn_bid", "bn_ask", "bn_bid_size", "bn_ask_size", "okx_bid", "okx_ask", "okx_bid_size", "okx_ask_size"]:
        events[col] = events[col].ffill()
    events = events.dropna(subset=["bn_bid", "bn_ask", "okx_bid", "okx_ask"]).reset_index()
    events = events.groupby("ts", as_index=False).last().sort_values("ts")

    mid = (events["bn_bid"] + events["bn_ask"]) / 2
    events[LONG_EDGE] = (events["okx_ask"] - events["bn_bid"]) / mid * 10000
    events[SHORT_EDGE] = (events["okx_bid"] - events["bn_ask"]) / mid * 10000
    minute = events.set_index("ts")[[LONG_EDGE, SHORT_EDGE]].resample("1min").mean().ffill()
    means = minute.rolling(params.center_minutes, min_periods=1).mean().rename(
        columns={LONG_EDGE: "long_mean", SHORT_EDGE: "short_mean"},
    )
    events = pd.merge_asof(events, means.reset_index().sort_values("ts"), on="ts", direction="backward")
    events["window_sec"] = (events["ts"] - minute.index[0]).dt.total_seconds()
    return events[events["ts"] >= start].reset_index(drop=True), raw.reset_index(drop=True)


# 只读取回测开始前 warmup 以后小时文件，避免每次全量扫 collector。
def quote_files(root: Path, warm_start: pd.Timestamp) -> list[Path]:
    cutoff = warm_start.strftime("%Y%m%d%H")
    files = []
    for path in sorted((root / "merged").glob("bidask1-*.parquet")):
        match = re.search(r"bidask1-(\d{10})\.parquet$", path.name)
        if match and match.group(1) >= cutoff:
            files.append(path)
    for path in sorted((root / "raw").glob("**/*.parquet")):
        hour = path.parent.name
        if hour >= cutoff:
            files.append(path)
    return files


# 用双交易所不同延迟和每腿不利滑点，模拟一次两腿成交。
def fill_trade(quotes: Quotes, signal_ts: pd.Timestamp, side: str, params: Params) -> dict[str, object] | None:
    bn = quotes.at("BINANCE", signal_ts + pd.Timedelta(milliseconds=params.binance_delay_ms))
    okx = quotes.at("OKX", signal_ts + pd.Timedelta(milliseconds=params.okx_delay_ms))
    if bn is None or okx is None:
        return None
    bn_ts, bn_bid, bn_ask = bn
    okx_ts, okx_bid, okx_ask = okx

    slip = params.slip_bps / 10000
    if side == LONG_EDGE:
        bn_px = bn_bid * (1 - slip)  # sell Binance
        okx_px = okx_ask * (1 + slip)  # buy OKX
        edge = (okx_px - bn_px) / ((bn_bid + bn_ask) / 2) * 10000
    else:
        bn_px = bn_ask * (1 + slip)  # buy Binance
        okx_px = okx_bid * (1 - slip)  # sell OKX
        edge = (okx_px - bn_px) / ((bn_bid + bn_ask) / 2) * 10000
    return {"ts": max(bn_ts, okx_ts), "bn_px": bn_px, "okx_px": okx_px, "edge": edge}


def fee(qty: float, bn_px: float, okx_px: float, params: Params) -> float:
    return qty * abs(bn_px) * params.fee_bps / 10000 + qty * abs(okx_px) * params.fee_bps / 10000


def close_pnl(pos: Pos, qty: float, bn_px: float, okx_px: float, params: Params) -> tuple[float, float, float]:
    if pos.side == LONG_EDGE:
        gross = qty * ((okx_px - pos.okx_px) + (pos.bn_px - bn_px))
    else:
        gross = qty * ((bn_px - pos.bn_px) + (pos.okx_px - okx_px))
    open_fee = pos.fees * qty / pos.qty
    close_fee = fee(qty, bn_px, okx_px, params)
    return gross, open_fee + close_fee, gross - open_fee - close_fee


def add_pos(pos: Pos, qty: float, bn_px: float, okx_px: float, edge: float, signal_edge: float, trade_fee: float) -> Pos:
    new_qty = pos.qty + qty
    return Pos(
        side=pos.side,
        qty=new_qty,
        bn_px=(pos.bn_px * pos.qty + bn_px * qty) / new_qty,
        okx_px=(pos.okx_px * pos.qty + okx_px * qty) / new_qty,
        edge=(pos.edge * pos.qty + edge * qty) / new_qty,
        signal_edge=(pos.signal_edge * pos.qty + signal_edge * qty) / new_qty,
        fees=pos.fees + trade_fee,
        layers=pos.layers + 1,
    )


def should_add(pos: Pos, side: str, signal_edge: float, actual_edge: float, params: Params) -> bool:
    if pos.side != side:
        return False
    if pos.layers >= params.max_layers:
        return False
    if side == SHORT_EDGE:
        threshold = max(pos.signal_edge, pos.edge) + params.add_bps
        return signal_edge >= threshold and actual_edge >= threshold
    threshold = min(pos.signal_edge, pos.edge) - params.add_bps
    return signal_edge <= threshold and actual_edge <= threshold


def simulate(frame: pd.DataFrame, raw: pd.DataFrame, params: Params) -> pd.DataFrame:
    rows = []
    pos: Pos | None = None
    quotes = Quotes(raw)
    blocked_until = pd.Timestamp("1970-01-01", tz=LOCAL_TZ)

    candidates = signal_candidates(frame, params)
    for row in candidates.itertuples(index=False):
        ts = row.ts
        if ts <= blocked_until:
            continue
        signal = row.edge_side
        fill = fill_trade(quotes, ts, signal, params)
        if fill is None:
            break

        signal_edge = float(row.signal_edge)
        mean = float(row.mean)
        trade_qty = 0.0
        close_qty = 0.0
        open_qty = 0.0
        gross = fees = net = np.nan
        action = ""

        if pos is None:
            open_qty = params.base_qty
            trade_qty = open_qty
            pos = Pos(
                signal,
                open_qty,
                float(fill["bn_px"]),
                float(fill["okx_px"]),
                float(fill["edge"]),
                signal_edge,
                fee(open_qty, float(fill["bn_px"]), float(fill["okx_px"]), params),
                1,
            )
            action = "OPEN"
        elif pos.side == signal:
            if not should_add(pos, signal, signal_edge, float(fill["edge"]), params):
                continue
            open_qty = params.add_qty
            trade_qty = open_qty
            pos = add_pos(
                pos,
                open_qty,
                float(fill["bn_px"]),
                float(fill["okx_px"]),
                float(fill["edge"]),
                signal_edge,
                fee(open_qty, float(fill["bn_px"]), float(fill["okx_px"]), params),
            )
            action = "ADD"
        else:
            old = pos
            close_qty = old.qty
            open_qty = params.base_qty
            trade_qty = close_qty + open_qty
            gross, fees, net = close_pnl(old, close_qty, float(fill["bn_px"]), float(fill["okx_px"]), params)
            pos = Pos(
                signal,
                open_qty,
                float(fill["bn_px"]),
                float(fill["okx_px"]),
                float(fill["edge"]),
                signal_edge,
                fee(open_qty, float(fill["bn_px"]), float(fill["okx_px"]), params),
                1,
            )
            action = "FLIP"

        rows.append(
            {
                "signal_time": ts,
                "fill_time": fill["ts"],
                "action": action,
                "side": "LONG" if signal == LONG_EDGE else "SHORT",
                "edge_side": signal,
                "trade_qty": trade_qty,
                "close_qty": close_qty,
                "open_qty": open_qty,
                "position_qty": 0.0 if pos is None else pos.qty,
                "signal_edge": signal_edge,
                "actual_edge": float(fill["edge"]),
                "position_edge": np.nan if pos is None else pos.edge,
                "position_signal_edge": np.nan if pos is None else pos.signal_edge,
                "position_layers": 0 if pos is None else pos.layers,
                "mean": mean,
                "deviation": signal_edge - mean,
                "bn_px": float(fill["bn_px"]),
                "okx_px": float(fill["okx_px"]),
                "gross_pnl": gross,
                "fees": fees,
                "net_pnl": net,
            },
        )
        blocked_until = fill["ts"]

    if pos is not None and not frame.empty:
        final_side = SHORT_EDGE if pos.side == LONG_EDGE else LONG_EDGE
        fill = fill_trade(quotes, frame.iloc[-1]["ts"], final_side, params)
        if fill is not None:
            gross, fees, net = close_pnl(pos, pos.qty, float(fill["bn_px"]), float(fill["okx_px"]), params)
            rows.append(
                {
                    "signal_time": frame.iloc[-1]["ts"],
                    "fill_time": fill["ts"],
                    "action": "STOP",
                    "side": "LONG" if final_side == LONG_EDGE else "SHORT",
                    "edge_side": final_side,
                    "trade_qty": pos.qty,
                    "close_qty": pos.qty,
                    "open_qty": 0.0,
                    "position_qty": 0.0,
                    "signal_edge": np.nan,
                    "actual_edge": float(fill["edge"]),
                    "position_edge": np.nan,
                    "position_signal_edge": np.nan,
                    "position_layers": 0,
                    "mean": np.nan,
                    "deviation": np.nan,
                    "bn_px": float(fill["bn_px"]),
                    "okx_px": float(fill["okx_px"]),
                    "gross_pnl": gross,
                    "fees": fees,
                    "net_pnl": net,
                },
            )
    return pd.DataFrame(rows)


# 先向量化筛出满足 signal 的 quote，避免逐行扫全量盘口事件。
def signal_candidates(frame: pd.DataFrame, params: Params) -> pd.DataFrame:
    data = frame[frame["window_sec"] >= params.min_window_minutes * 60].copy()
    long_dev = data[LONG_EDGE] - data["long_mean"]
    short_dev = data[SHORT_EDGE] - data["short_mean"]
    long_hit = long_dev <= -params.band_bps
    short_hit = short_dev >= params.band_bps
    data = data[long_hit | short_hit].copy()
    if data.empty:
        return data

    long_dev = long_dev.loc[data.index]
    short_dev = short_dev.loc[data.index]
    choose_short = short_hit.loc[data.index] & (~long_hit.loc[data.index] | (short_dev.abs() >= long_dev.abs()))
    data["edge_side"] = np.where(choose_short, SHORT_EDGE, LONG_EDGE)
    data["signal_edge"] = np.where(choose_short, data[SHORT_EDGE], data[LONG_EDGE])
    data["mean"] = np.where(choose_short, data["short_mean"], data["long_mean"])
    return data.sort_values("ts").reset_index(drop=True)


def summarize(actions: pd.DataFrame) -> pd.DataFrame:
    closed = actions[actions["net_pnl"].notna()]
    return pd.DataFrame(
        [
            {
                "actions": len(actions),
                "open": int(actions["action"].eq("OPEN").sum()),
                "add": int(actions["action"].eq("ADD").sum()),
                "flip": int(actions["action"].eq("FLIP").sum()),
                "stop": int(actions["action"].eq("STOP").sum()),
                "closed_positions": len(closed),
                "gross_pnl": float(closed["gross_pnl"].sum()) if len(closed) else 0.0,
                "fees": float(closed["fees"].sum()) if len(closed) else 0.0,
                "net_pnl": float(closed["net_pnl"].sum()) if len(closed) else 0.0,
                "avg_net": float(closed["net_pnl"].mean()) if len(closed) else np.nan,
                "win_rate": float((closed["net_pnl"] > 0).mean()) if len(closed) else np.nan,
                "max_position_qty": float(actions["position_qty"].max()) if len(actions) else 0.0,
            },
        ],
    )


def main() -> None:
    started = time.perf_counter()
    params = Params()
    quote_root = Path("strategies/preipoarb/research/bidask1-live")
    report_orders = Path("strategies/preipoarb/report/live-20260622223523/orders.csv")
    output = Path("strategies/preipoarb/research/analyst/backtest_anth_one_layer")
    output.mkdir(parents=True, exist_ok=True)
    report = pd.read_csv(report_orders)
    start = pd.to_datetime(report["成交时间"].iloc[0]).tz_localize(LOCAL_TZ)

    loaded_at = time.perf_counter()
    frame, raw = load_edges(quote_root, "ANTHROPIC", start, params)
    simulated_at = time.perf_counter()
    actions = simulate(frame, raw, params)
    finished_at = time.perf_counter()
    summary = summarize(actions)
    actions.to_csv(output / "anth_one_layer_actions.csv", index=False)
    summary.to_csv(output / "anth_one_layer_summary.csv", index=False)

    print(f"range {frame['ts'].min()} -> {frame['ts'].max()} rows={len(frame)}")
    print(f"timing load_edges={simulated_at - loaded_at:.2f}s simulate={finished_at - simulated_at:.2f}s total={finished_at - started:.2f}s")
    print(summary.to_string(index=False, float_format=lambda value: f"{value:.4f}"))
    print(output / "anth_one_layer_actions.csv")
    print(output / "anth_one_layer_summary.csv")


if __name__ == "__main__":
    main()

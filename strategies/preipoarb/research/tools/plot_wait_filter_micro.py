from __future__ import annotations

import json
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


LOCAL_TZ = "Asia/Shanghai"
ROOT = Path("strategies/preipoarb/research")
ANALYST = ROOT / "analyst"
COLLECTOR = ROOT / "local_collector"
FILTER_DIR = ANALYST / "order_wait_filter_100ms"
OUT = FILTER_DIR / "micro_small_slip"


def read_filter() -> pd.DataFrame:
    data = pd.read_csv(FILTER_DIR / "order_wait_filter_100ms.csv")
    for column in ["log_signal_ts", "signal_quote_ts", "wait_quote_ts", "binance_ts", "okx_ts"]:
        data[column] = pd.to_datetime(data[column], utc=True, errors="coerce").dt.tz_convert(LOCAL_TZ)
    return data


def read_actions() -> pd.DataFrame:
    data = pd.read_csv(ANALYST / "charts" / "actions_from_log.csv")
    data["signal_ts"] = pd.to_datetime(data["signal_ts"], utc=True).dt.tz_convert(LOCAL_TZ)
    data["filled_ts"] = pd.to_datetime(data["filled_ts"], utc=True).dt.tz_convert(LOCAL_TZ)
    return data


def read_quotes(asset: str) -> pd.DataFrame:
    frames = []
    for path in sorted((COLLECTOR / "merged").glob("*.parquet")):
        frame = pd.read_parquet(path, columns=["ts_exchange_ms", "venue", "symbol", "bid", "ask", "bid_size", "ask_size"])
        frame = frame[frame["symbol"].eq(asset)]
        if not frame.empty:
            frames.append(frame)
    data = pd.concat(frames, ignore_index=True)
    data["ts"] = pd.to_datetime(data["ts_exchange_ms"], unit="ms", utc=True).dt.tz_convert(LOCAL_TZ)
    return data.drop_duplicates(["ts_exchange_ms", "venue", "bid", "ask"]).sort_values("ts").reset_index(drop=True)


def read_ticks(asset: str) -> pd.DataFrame:
    frames = []
    for path in sorted((COLLECTOR / "trade_merged").glob("*.parquet")):
        frame = pd.read_parquet(path)
        frame = frame[frame["symbol"].eq(asset)]
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=["ts", "venue", "price", "size", "side"])
    data = pd.concat(frames, ignore_index=True)
    data["ts"] = pd.to_datetime(data["ts_exchange_ms"], unit="ms", utc=True).dt.tz_convert(LOCAL_TZ)
    return data.drop_duplicates(["ts_exchange_ms", "venue", "price", "size", "trade_id"]).sort_values("ts").reset_index(drop=True)


def quote_window(quotes: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    rows = []
    for venue, part in quotes.groupby("venue"):
        part = part.sort_values("ts")
        prev = part[part["ts"] < start].tail(1).copy()
        if not prev.empty:
            prev["ts"] = start
            rows.append(prev)
        rows.append(part[part["ts"].between(start, end)].copy())
    return pd.concat(rows, ignore_index=True).sort_values(["venue", "ts"])


def active_quote(quotes: pd.DataFrame, venue: str, ts: pd.Timestamp, start: pd.Timestamp, end: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp, float, float] | None:
    part = quotes[quotes["venue"].eq(venue)].sort_values("ts")
    prev = part[part["ts"] <= ts].tail(1)
    if prev.empty:
        return None
    row = prev.iloc[0]
    next_quote = part[part["ts"] > row["ts"]].head(1)
    seg_start = max(row["ts"], start)
    seg_end = end if next_quote.empty else min(next_quote.iloc[0]["ts"], end)
    return seg_start, seg_end, float(row["bid"]), float(row["ask"])


def venue_from_instrument(instrument: str) -> str:
    return "BINANCE" if "BINANCE" in instrument else "OKX"


def tick_summary(ticks: pd.DataFrame, venue: str, ts: pd.Timestamp) -> str:
    part = ticks[ticks["venue"].eq(venue) & ticks["ts"].between(ts - pd.Timedelta(milliseconds=100), ts + pd.Timedelta(milliseconds=100))]
    total = part["size"].sum()
    buy = part[part["side"].eq("BUY")]["size"].sum() if "side" in part else 0.0
    sell = part[part["side"].eq("SELL")]["size"].sum() if "side" in part else 0.0
    return f"ticks +/-100ms total={total:.3g} buy={buy:.3g} sell={sell:.3g}"


def pad_ylim(ax, values: pd.Series) -> None:
    values = values.dropna()
    if values.empty:
        return
    lo = float(values.min())
    hi = float(values.max())
    pad = max(0.05, (hi - lo) * 0.28)
    ax.set_ylim(lo - pad, hi + pad)


def plot_one(row: pd.Series, action: pd.Series, quotes: pd.DataFrame, ticks: pd.DataFrame) -> Path:
    legs = json.loads(action["leg_json"])
    fill_times = [pd.to_datetime(leg["ts_ns"], unit="ns", utc=True).tz_convert(LOCAL_TZ) for leg in legs]
    start = min([row["signal_quote_ts"], row["wait_quote_ts"], *fill_times]) - pd.Timedelta(milliseconds=180)
    end = max([row["signal_quote_ts"], row["wait_quote_ts"], *fill_times]) + pd.Timedelta(milliseconds=260)
    q = quote_window(quotes, start, end)
    t = ticks[ticks["ts"].between(start, end)].copy()

    fig, axes = plt.subplots(2, 1, figsize=(14, 8), dpi=180, sharex=True)
    styles = {
        "BINANCE": {"bid": "#2563eb", "ask": "#93c5fd", "title": "Binance"},
        "OKX": {"bid": "#16a34a", "ask": "#86efac", "title": "OKX"},
    }
    for ax, venue in zip(axes, ["BINANCE", "OKX"], strict=True):
        style = styles[venue]
        part = q[q["venue"].eq(venue)].sort_values("ts")
        tick = t[t["venue"].eq(venue)].sort_values("ts")
        ax.step(part["ts"], part["bid"], where="post", color=style["bid"], linewidth=1.8, label=f"{venue} bid")
        ax.step(part["ts"], part["ask"], where="post", color=style["ask"], linewidth=1.8, label=f"{venue} ask")
        ax.scatter(tick["ts"], tick["price"], s=18, color=style["bid"], alpha=0.35, label=f"{venue} tick")

        for ts, color, label, yoff in [
            (row["signal_quote_ts"], "#dc2626", "signal quote", 8),
            (row["wait_quote_ts"], "#7c3aed", "100ms recheck", -18),
        ]:
            segment = active_quote(quotes, venue, ts, start, end)
            ax.axvline(ts, color=color, linestyle="--", linewidth=1.0, alpha=0.9)
            if segment is None:
                continue
            seg_start, seg_end, bid, ask = segment
            ax.hlines([bid, ask], seg_start, seg_end, colors=color, linewidth=3.0, zorder=4)
            ax.scatter([ts, ts], [bid, ask], s=28, color=color, zorder=6)
            ax.annotate(
                f"{label}\nbid {bid:.2f} ask {ask:.2f}",
                xy=(ts, ask),
                xytext=(8, yoff),
                textcoords="offset points",
                fontsize=8,
                color=color,
                bbox={"boxstyle": "round,pad=0.2", "fc": "white", "ec": color, "alpha": 0.82},
            )

        for leg in [leg for leg in legs if venue_from_instrument(leg["instrument"]) == venue]:
            ts = pd.to_datetime(leg["ts_ns"], unit="ns", utc=True).tz_convert(LOCAL_TZ)
            ax.scatter(ts, leg["avg_px"], s=145, marker="*", color=style["bid"], edgecolors="black", linewidth=0.8, zorder=7)
            ax.annotate(
                f"original fill\n{leg['side']} {leg['qty']:.3g}@{leg['avg_px']:.2f}",
                xy=(ts, leg["avg_px"]),
                xytext=(22, 28),
                textcoords="offset points",
                fontsize=8,
                color=style["bid"],
                arrowprops={"arrowstyle": "->", "color": style["bid"], "lw": 0.8},
                bbox={"boxstyle": "round,pad=0.22", "fc": "white", "ec": style["bid"], "alpha": 0.86},
            )

        ax.text(
            0.01,
            0.04,
            f"signal {tick_summary(ticks, venue, row['signal_quote_ts'])}\nrecheck {tick_summary(ticks, venue, row['wait_quote_ts'])}",
            transform=ax.transAxes,
            fontsize=8,
            color="#374151",
            bbox={"boxstyle": "round,pad=0.2", "fc": "white", "ec": "#9ca3af", "alpha": 0.75},
        )
        ax.set_ylabel(f"{style['title']} price")
        ax.grid(True, alpha=0.25)
        ax.legend(loc="best", fontsize=8, ncol=3)
        pad_ylim(ax, pd.concat([part["bid"], part["ask"], tick["price"]]))

    axes[-1].set_xlim(start, end)
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S.%f", tz=pd.Timestamp.now(tz=LOCAL_TZ).tz))
    fig.autofmt_xdate(rotation=25, ha="right")
    fig.suptitle(
        f"{row['asset']} lot={int(row['lot'])} {row['side']} {row['action']} filtered despite small slip "
        f"orig_slip={row['original_slippage']:.2f}bps | signal={row['original_signal_edge']:.2f} "
        f"recheck={row['wait_signal_edge']:.2f} mean={row['wait_mean']:.2f}"
    )
    fig.tight_layout()
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"{str(row['asset']).lower()}_lot{int(row['lot']):03d}_small_slip_{row['original_slippage']:.2f}bps.png".replace("-", "m")
    fig.savefig(path)
    plt.close(fig)
    return path


def main() -> None:
    filtered = read_filter()
    actions = read_actions()
    selected = filtered[(filtered["status"].ne("KEEP")) & (filtered["original_slippage"].abs().le(10))].copy()
    selected = selected.sort_values(["asset", "original_slippage"], ascending=[True, False]).reset_index(drop=True)
    OUT.mkdir(parents=True, exist_ok=True)
    selected.to_csv(OUT / "selected_small_slip_filtered.csv", index=False)

    paths = []
    for asset, group in selected.groupby("asset"):
        quotes = read_quotes(asset)
        ticks = read_ticks(asset)
        for _, row in group.iterrows():
            action = actions[actions["lot"].eq(row["lot"]) & actions["asset"].eq(row["asset"])].iloc[0]
            paths.append(plot_one(row, action, quotes, ticks))

    print(f"selected={len(selected)}")
    for path in paths:
        print(path)


if __name__ == "__main__":
    main()

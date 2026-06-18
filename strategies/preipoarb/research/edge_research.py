from __future__ import annotations

import time
import zipfile
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import requests
import seaborn as sns


STRATEGY_DIR = Path(__file__).resolve().parents[1]
RESEARCH_DIR = STRATEGY_DIR / "research"
RAW_DIR = RESEARCH_DIR / "raw"
OUT_TICK = RESEARCH_DIR / "data" / "tick"
OUT_SIGNAL = RESEARCH_DIR / "data" / "signal"
OUT_NOTES = RESEARCH_DIR / "notes"
OUT_PLOTS = RESEARCH_DIR / "plots"

ASSETS = {
    "OPENAI": {
        "binance": "OPENAIUSDT",
        "okx": "OPENAI-USDT-SWAP",
    },
    "ANTHROPIC": {
        "binance": "ANTHROPICUSDT",
        "okx": "ANTHROPIC-USDT-SWAP",
    },
}

FEES_BPS = {"binance": 5.0, "okx": 5.0}
SLIPPAGE_BPS = 2.0


FONT_FAMILY = ["Aptos", "Inter", "Segoe UI", "DejaVu Sans", "Arial", "sans-serif"]
TOKENS = {
    "surface": "#FCFCFD",
    "panel": "#FFFFFF",
    "ink": "#1F2430",
    "muted": "#6F768A",
    "grid": "#E6E8F0",
    "axis": "#D7DBE7",
}
BLUE = {"base": "#A3BEFA", "mid": "#5477C4", "dark": "#2E4780", "light": "#CEDFFE"}
ORANGE = {"base": "#F0986E", "mid": "#CC6F47", "dark": "#804126", "light": "#FFBDA1"}
OLIVE = {"base": "#A3D576", "mid": "#71B436", "dark": "#386411", "light": "#BEEB96"}


def use_chart_theme() -> None:
    sns.set_theme(
        style="whitegrid",
        rc={
            "figure.facecolor": TOKENS["surface"],
            "savefig.facecolor": TOKENS["surface"],
            "axes.facecolor": TOKENS["panel"],
            "axes.edgecolor": TOKENS["axis"],
            "axes.labelcolor": TOKENS["ink"],
            "grid.color": TOKENS["grid"],
            "grid.linewidth": 0.8,
            "font.family": "sans-serif",
            "font.sans-serif": FONT_FAMILY,
        },
    )


def add_header(fig, ax, title: str, subtitle: str) -> None:
    ax.set_title("")
    fig.subplots_adjust(top=0.84)
    left = ax.get_position().x0
    fig.text(left, 0.975, title, ha="left", va="top", fontsize=13, fontweight="semibold", color=TOKENS["ink"])
    fig.text(left, 0.925, subtitle, ha="left", va="top", fontsize=9, color=TOKENS["muted"])
    sns.despine(ax=ax)


def get(url: str) -> bytes:
    last_error = None
    for attempt in range(4):
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                return response.content
            last_error = RuntimeError(f"HTTP {response.status_code}: {url}")
            if response.status_code == 404:
                raise last_error
        except Exception as exc:
            last_error = exc
        time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(str(last_error))


def download(url: str, path: Path) -> bool:
    if path.exists() and path.stat().st_size > 0:
        return True
    try:
        content = get(url)
    except RuntimeError as exc:
        print(f"skip_download {url} reason={exc}", flush=True)
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    print(f"downloaded {path}", flush=True)
    return True


def date_range(start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    days = pd.date_range(start.normalize(), end.normalize(), freq="D", tz="UTC")
    return [pd.Timestamp(day) for day in days]


def fetch_zips(start: pd.Timestamp, end: pd.Timestamp) -> None:
    for day in date_range(start, end - pd.Timedelta(days=1)):
        date_text = day.strftime("%Y-%m-%d")
        okx_folder = day.strftime("%Y%m%d")
        for asset, symbols in ASSETS.items():
            b_symbol = symbols["binance"]
            b_url = (
                "https://data.binance.vision/data/futures/um/daily/aggTrades/"
                f"{b_symbol}/{b_symbol}-aggTrades-{date_text}.zip"
            )
            download(b_url, RAW_DIR / f"{b_symbol}-aggTrades-{date_text}.zip")

            o_symbol = symbols["okx"]
            o_url = (
                "https://www.okx.com/cdn/okex/traderecords/trades/daily/"
                f"{okx_folder}/{o_symbol}-trades-{date_text}.zip"
            )
            download(o_url, RAW_DIR / "okx" / f"{o_symbol}-trades-{date_text}.zip")


def read_binance_zip(path: Path, asset: str, symbol: str) -> pd.DataFrame:
    with zipfile.ZipFile(path) as zf:
        with zf.open(zf.namelist()[0]) as file:
            df = pd.read_csv(file)
    if df.empty:
        return df
    return pd.DataFrame({
        "exchange": "binance",
        "asset": asset,
        "symbol": symbol,
        "trade_id": df["agg_trade_id"].astype(str),
        "timestamp_ms": df["transact_time"].astype("int64"),
        "price": df["price"].astype("float64"),
        "size": df["quantity"].astype("float64"),
        "side": df["is_buyer_maker"].map(lambda value: "sell" if bool(value) else "buy"),
        "source": "binance_daily_aggTrades",
    })


def read_okx_zip(path: Path, asset: str, symbol: str) -> pd.DataFrame:
    with zipfile.ZipFile(path) as zf:
        with zf.open(zf.namelist()[0]) as file:
            df = pd.read_csv(file)
    if df.empty:
        return df
    return pd.DataFrame({
        "exchange": "okx",
        "asset": asset,
        "symbol": symbol,
        "trade_id": df["trade_id"].astype(str),
        "timestamp_ms": df["created_time"].astype("int64"),
        "price": df["price"].astype("float64"),
        "size": df["size"].astype("float64"),
        "side": df["side"].astype(str).str.lower(),
        "source": "okx_daily_instrument_trades",
    })


def load_zip_trades(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    frames = []
    for day in date_range(start, end - pd.Timedelta(days=1)):
        date_text = day.strftime("%Y-%m-%d")
        for asset, symbols in ASSETS.items():
            b_symbol = symbols["binance"]
            b_path = RAW_DIR / f"{b_symbol}-aggTrades-{date_text}.zip"
            if b_path.exists():
                frames.append(read_binance_zip(b_path, asset, b_symbol))

            o_symbol = symbols["okx"]
            o_path = RAW_DIR / "okx" / f"{o_symbol}-trades-{date_text}.zip"
            if o_path.exists():
                frames.append(read_okx_zip(o_path, asset, o_symbol))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def fetch_binance_rest(asset: str, symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    rows = []
    cursor = start_ms
    while cursor < end_ms:
        params = {"symbol": symbol, "startTime": cursor, "endTime": end_ms, "limit": 1000}
        data = requests.get("https://fapi.binance.com/fapi/v1/aggTrades", params=params, timeout=15).json()
        if not isinstance(data, list) or not data:
            break
        for item in data:
            rows.append({
                "exchange": "binance",
                "asset": asset,
                "symbol": symbol,
                "trade_id": str(item["a"]),
                "timestamp_ms": int(item["T"]),
                "price": float(item["p"]),
                "size": float(item["q"]),
                "side": "sell" if bool(item["m"]) else "buy",
                "source": "binance_fapi_aggTrades",
            })
        next_cursor = int(data[-1]["T"]) + 1
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        time.sleep(0.03)
    return pd.DataFrame(rows)


def fetch_okx_rest(asset: str, symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    rows = []
    after = None
    while True:
        params = {"instId": symbol, "limit": 100}
        if after is not None:
            params["after"] = after
        payload = requests.get("https://www.okx.com/api/v5/market/history-trades", params=params, timeout=15).json()
        data = payload.get("data") or []
        if not data:
            break
        min_ts = min(int(item["ts"]) for item in data)
        for item in data:
            ts = int(item["ts"])
            if start_ms <= ts <= end_ms:
                rows.append({
                    "exchange": "okx",
                    "asset": asset,
                    "symbol": symbol,
                    "trade_id": str(item["tradeId"]),
                    "timestamp_ms": ts,
                    "price": float(item["px"]),
                    "size": float(item["sz"]),
                    "side": str(item["side"]).lower(),
                    "source": "okx_history_trades_rest",
                })
        after = str(data[-1]["tradeId"])
        if min_ts < start_ms:
            break
        time.sleep(0.08)
    return pd.DataFrame(rows)


def assemble_trades(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    fetch_zips(start, end)
    frames = [load_zip_trades(start, end)]
    rest_start = max(start, end.normalize())
    start_ms = int(rest_start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    for asset, symbols in ASSETS.items():
        frames.append(fetch_binance_rest(asset, symbols["binance"], start_ms, end_ms))
        frames.append(fetch_okx_rest(asset, symbols["okx"], start_ms, end_ms))
    data = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True)
    data["ts_utc"] = pd.to_datetime(data["timestamp_ms"], unit="ms", utc=True)
    data = data[(data["ts_utc"] >= start) & (data["ts_utc"] <= end)]
    data = data.drop_duplicates(["exchange", "symbol", "trade_id"]).sort_values("timestamp_ms")
    return data.reset_index(drop=True)


def tick_price_frame(group: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, max_age_sec: int) -> pd.DataFrame:
    group = group.copy()
    group["ts_sec"] = group["ts_utc"].dt.floor("s")
    last = group.sort_values("timestamp_ms").groupby("ts_sec", as_index=False).tail(1)[["ts_sec", "price"]]
    last = last.rename(columns={"price": "tick_price"})
    last["price_time"] = last["ts_sec"]
    idx = pd.date_range(start.floor("s"), end.floor("s"), freq="s", tz="UTC", name="ts_sec")
    out = pd.DataFrame(index=idx).reset_index()
    out = out.merge(last, on="ts_sec", how="left")
    out["tick_price"] = out["tick_price"].ffill()
    out["price_time"] = out["price_time"].ffill()
    age = (out["ts_sec"] - out["price_time"]).dt.total_seconds()
    out["price_age_sec"] = age
    out.loc[age > max_age_sec, "tick_price"] = pd.NA
    return out


def build_tick_prices(trades: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, max_age_sec: int) -> pd.DataFrame:
    frames = []
    for (asset, exchange, symbol), group in trades.groupby(["asset", "exchange", "symbol"]):
        prices = tick_price_frame(group, start, end, max_age_sec)
        prices["asset"] = asset
        prices["exchange"] = exchange
        prices["symbol"] = symbol
        frames.append(prices)
    return pd.concat(frames, ignore_index=True)


def edge_series(prices: pd.DataFrame) -> pd.DataFrame:
    rows = []
    costs = FEES_BPS["binance"] + FEES_BPS["okx"] + SLIPPAGE_BPS * 2
    for asset, group in prices.groupby("asset"):
        wide = group.pivot(index="ts_sec", columns="exchange", values=["tick_price", "price_age_sec"])
        needed = [("tick_price", "binance"), ("tick_price", "okx")]
        if not all(col in wide.columns for col in needed):
            continue
        part = pd.DataFrame({
            "ts_sec": wide.index,
            "asset": asset,
            "binance_price": wide[("tick_price", "binance")].astype("float64"),
            "okx_price": wide[("tick_price", "okx")].astype("float64"),
            "binance_age_sec": wide[("price_age_sec", "binance")].astype("float64"),
            "okx_age_sec": wide[("price_age_sec", "okx")].astype("float64"),
        }).dropna()
        if part.empty:
            continue
        part["buy_binance_sell_okx"] = (part["okx_price"] - part["binance_price"]) / part["binance_price"] * 10000 - costs
        part["buy_okx_sell_binance"] = (part["binance_price"] - part["okx_price"]) / part["okx_price"] * 10000 - costs
        long = part.melt(
            id_vars=["ts_sec", "asset", "binance_price", "okx_price", "binance_age_sec", "okx_age_sec"],
            value_vars=["buy_binance_sell_okx", "buy_okx_sell_binance"],
            var_name="direction",
            value_name="edge_bps",
        )
        rows.append(long)
    return pd.concat(rows, ignore_index=True).sort_values(["asset", "direction", "ts_sec"])


def edge_bars(edge: pd.DataFrame, freq: str) -> pd.DataFrame:
    rows = []
    for (asset, direction), group in edge.groupby(["asset", "direction"]):
        part = group.set_index("ts_sec")["edge_bps"].resample(freq).ohlc().dropna()
        part["asset"] = asset
        part["direction"] = direction
        part["points"] = group.set_index("ts_sec")["edge_bps"].resample(freq).count().reindex(part.index)
        rows.append(part.reset_index())
    return pd.concat(rows, ignore_index=True)


def add_features(edge: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for (asset, direction), group in edge.groupby(["asset", "direction"]):
        part = group.sort_values("ts_sec").copy()
        series = part.set_index("ts_sec")["edge_bps"]
        med_30m = series.rolling("30min", min_periods=120).median()
        med_6h = series.rolling("6h", min_periods=1200).median()
        q90_30m = series.rolling("30min", min_periods=120).quantile(0.90)
        q10_30m = series.rolling("30min", min_periods=120).quantile(0.10)
        part["median_30m"] = med_30m.to_numpy()
        part["median_6h"] = med_6h.to_numpy()
        part["range_30m"] = (q90_30m - q10_30m).to_numpy()
        part["dist_to_zero"] = part["edge_bps"].abs()
        part["excursion_30m"] = part["edge_bps"] - part["median_30m"]
        part["drift_6h"] = part["median_30m"] - part["median_6h"]
        frames.append(part)
    return pd.concat(frames, ignore_index=True)


def summarize(edge: pd.DataFrame, prices: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, max_age_sec: int) -> str:
    lines = [
        "# PREIPO tick-implied edge research",
        "",
        f"- window: {start} to {end}",
        f"- edge definition: direct ratio of latest Binance and OKX tick prices; each exchange price forward-filled up to {max_age_sec}s",
        f"- costs in edge: Binance {FEES_BPS['binance']}bps + OKX {FEES_BPS['okx']}bps + slippage {SLIPPAGE_BPS}bps per leg",
        f"- tick-price rows: {len(prices):,}",
        f"- edge rows: {len(edge):,}",
        "",
        "## Edge distribution",
        "",
    ]
    stats = edge.groupby(["asset", "direction"])["edge_bps"].describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.90, 0.95])
    lines.append(stats.to_string())
    lines += ["", "## Volatility regimes", ""]
    featured = add_features(edge)
    regime = featured.groupby(["asset", "direction"]).agg(
        median_range_30m=("range_30m", "median"),
        p90_range_30m=("range_30m", lambda x: x.quantile(0.90)),
        median_abs_drift_6h=("drift_6h", lambda x: x.abs().median()),
        p90_abs_drift_6h=("drift_6h", lambda x: x.abs().quantile(0.90)),
    )
    lines.append(regime.to_string())
    return "\n".join(lines)


def plot_edge(edge: pd.DataFrame, bars: pd.DataFrame, tag: str, max_age_sec: int) -> list[Path]:
    use_chart_theme()
    paths = []
    for asset in sorted(edge["asset"].unique()):
        plot_df = bars[(bars["asset"] == asset) & (bars["direction"] == "buy_binance_sell_okx")].copy()
        if plot_df.empty:
            continue
        fig, ax = plt.subplots(figsize=(13, 6.8), dpi=150)
        ax.vlines(plot_df["ts_sec"], plot_df["low"], plot_df["high"], color=BLUE["light"], linewidth=0.8, alpha=0.65)
        ax.plot(plot_df["ts_sec"], plot_df["close"], color=BLUE["dark"], linewidth=1.0, label="5m close")
        ax.axhline(0, color=TOKENS["ink"], linewidth=1.0, linestyle=":")
        ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f} bps"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4, maxticks=8))
        ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
        ax.set_xlabel("")
        ax.set_ylabel("Net edge")
        ax.legend(loc="lower left", bbox_to_anchor=(0, 1.01), frameon=False)
        add_header(
            fig,
            ax,
            f"{asset} buy Binance / sell OKX edge moves in regimes",
            f"5-minute OHLC from latest tick price ratio, max price age {max_age_sec}s; edge includes fees and slippage.",
        )
        path = OUT_PLOTS / f"PREIPO-{asset}-EDGE-OHLC-{tag}.png"
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        paths.append(path)

    dist = edge[edge["direction"].eq("buy_binance_sell_okx")].copy()
    if not dist.empty:
        fig, ax = plt.subplots(figsize=(10.5, 6.2), dpi=150)
        palette = {"OPENAI": BLUE["base"], "ANTHROPIC": ORANGE["base"]}
        sns.kdeplot(data=dist, x="edge_bps", hue="asset", palette=palette, common_norm=False, fill=True, alpha=0.22, ax=ax)
        ax.axvline(0, color=TOKENS["ink"], linestyle=":", linewidth=1.0)
        ax.xaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f} bps"))
        ax.set_xlabel("Net edge")
        ax.set_ylabel("Density")
        add_header(
            fig,
            ax,
            "Edge distribution is asset-specific and fat-tailed",
            f"Latest tick price ratio, buy Binance / sell OKX direction, max price age {max_age_sec}s.",
        )
        path = OUT_PLOTS / f"PREIPO-EDGE-DISTRIBUTION-{tag}.png"
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        paths.append(path)
    return paths


def local_jump_trades(edge: pd.DataFrame) -> pd.DataFrame:
    notional = 100.0
    min_hold_sec = 60
    cooldown_sec = 300
    rows = []
    source = edge[edge["direction"].eq("buy_binance_sell_okx")].sort_values(["asset", "ts_sec"])
    for asset, part in source.groupby("asset"):
        pos = None
        cooldown_until = pd.Timestamp.min.tz_localize("UTC")
        needed = part[["asset", "ts_sec", "edge_bps", "median_30m", "range_30m"]].dropna()
        for row in needed.itertuples(index=False):
            if pos is not None:
                hold_sec = (row.ts_sec - pos["open_time"]).total_seconds()
                target = pos["base"] + 0.25 * (pos["entry_edge"] - pos["base"])
                if hold_sec >= min_hold_sec and (row.edge_bps <= target or hold_sec >= 7200):
                    pnl = (pos["entry_edge"] - row.edge_bps) / 10000 * notional
                    rows.append({
                        **pos,
                        "close_time": row.ts_sec,
                        "close_edge": row.edge_bps,
                        "hold_sec": hold_sec,
                        "pnl": pnl,
                        "reason": "time" if hold_sec >= 7200 else "revert",
                    })
                    pos = None
                    cooldown_until = row.ts_sec + pd.Timedelta(seconds=cooldown_sec)
                continue

            if row.ts_sec < cooldown_until:
                continue
            trigger = max(20.0, 1.5 * row.range_30m)
            if row.range_30m >= 12.0 and row.edge_bps - row.median_30m >= trigger and row.edge_bps > 0:
                pos = {
                    "asset": row.asset,
                    "open_time": row.ts_sec,
                    "entry_edge": row.edge_bps,
                    "base": row.median_30m,
                    "range30m": row.range_30m,
                    "trigger": trigger,
                }
    return pd.DataFrame(rows)


def plot_local_jump(edge: pd.DataFrame, trades: pd.DataFrame, tag: str) -> list[Path]:
    use_chart_theme()
    paths = []
    source = edge[edge["direction"].eq("buy_binance_sell_okx")]
    for asset, group in source.groupby("asset"):
        plot_df = (
            group.set_index("ts_sec")
            .resample("5min")
            .agg(edge=("edge_bps", "last"), base=("median_30m", "last"), range30=("range_30m", "last"))
            .dropna()
            .reset_index()
        )
        asset_trades = trades[trades["asset"].eq(asset)]
        fig, ax = plt.subplots(figsize=(13, 7.4), dpi=150)
        fig.subplots_adjust(top=0.80)
        ax.plot(plot_df["ts_sec"], plot_df["edge"], color=BLUE["dark"], linewidth=1.0, label="edge 5m close")
        ax.plot(plot_df["ts_sec"], plot_df["base"], color=ORANGE["dark"], linewidth=1.0, linestyle="--", label="30m median")
        ax.fill_between(
            plot_df["ts_sec"],
            plot_df["base"],
            plot_df["base"] + plot_df["range30"] * 1.5,
            color=ORANGE["light"],
            alpha=0.16,
            label="trigger zone",
        )
        if not asset_trades.empty:
            ax.scatter(
                asset_trades["open_time"],
                asset_trades["entry_edge"],
                s=34,
                color=OLIVE["mid"],
                edgecolor=OLIVE["dark"],
                linewidth=0.8,
                zorder=4,
                label="entry",
            )
            ax.scatter(
                asset_trades["close_time"],
                asset_trades["close_edge"],
                s=28,
                facecolors=TOKENS["panel"],
                edgecolors=ORANGE["dark"],
                linewidth=0.9,
                zorder=4,
                label="exit",
            )
        ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f} bps"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4, maxticks=8))
        ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
        ax.set_xlabel("")
        ax.set_ylabel("Net edge")
        ax.legend(loc="upper left", frameon=True, framealpha=0.92, ncol=2)
        left = ax.get_position().x0
        fig.text(
            left,
            0.975,
            f"{asset} local-jump entries on tick-price edge",
            ha="left",
            va="top",
            fontsize=13,
            fontweight="semibold",
            color=TOKENS["ink"],
        )
        fig.text(
            left,
            0.93,
            "Entry: edge above 30m median by max(20bps, 1.5x 30m range). Exit: give back 75% of jump or 2h timeout.",
            ha="left",
            va="top",
            fontsize=9,
            color=TOKENS["muted"],
        )
        sns.despine(ax=ax)
        path = OUT_PLOTS / f"PREIPO-{asset}-LOCAL-JUMP-BALANCED-{tag}.png"
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        paths.append(path)
    return paths


def main(start_text: str, end_text: str | None, max_age_sec: int) -> None:
    end = pd.Timestamp.utcnow().floor("s") if end_text is None else pd.Timestamp(end_text, tz="UTC")
    start = pd.Timestamp(start_text, tz="UTC")
    tag = f"{start:%Y%m%d}-{end:%Y%m%d}-age{max_age_sec}"

    trades = assemble_trades(start, end)
    trade_path = OUT_TICK / f"PREIPO-BINANCE-OKX-TRADES-RESEARCH-{tag}.parquet"
    trade_path.parent.mkdir(parents=True, exist_ok=True)
    trades.to_parquet(trade_path, index=False)

    prices = build_tick_prices(trades, start, end, max_age_sec)
    price_path = OUT_TICK / f"PREIPO-BINANCE-OKX-TICK-PRICES-{tag}.parquet"
    prices.to_parquet(price_path, index=False)

    edge = add_features(edge_series(prices))
    edge_path = OUT_SIGNAL / f"PREIPO-BINANCE-OKX-EDGE-APPROX-{tag}.parquet"
    edge_path.parent.mkdir(parents=True, exist_ok=True)
    edge.to_parquet(edge_path, index=False)

    bars = edge_bars(edge, "5min")
    bars_path = OUT_SIGNAL / f"PREIPO-BINANCE-OKX-EDGE-5M-{tag}.parquet"
    bars.to_parquet(bars_path, index=False)

    note = summarize(edge, prices, start, end, max_age_sec)
    note_path = OUT_NOTES / f"PREIPO-EDGE-RESEARCH-{tag}.md"
    note_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_text(note, encoding="utf-8")

    plot_paths = plot_edge(edge, bars, tag, max_age_sec)
    trades = local_jump_trades(edge)
    local_trade_path = OUT_SIGNAL / f"PREIPO-LOCAL-JUMP-BALANCED-TRADES-{tag}.parquet"
    trades.to_parquet(local_trade_path, index=False)
    plot_paths.extend(plot_local_jump(edge, trades, tag))
    print(f"trades={trade_path}")
    print(f"tick_prices={price_path}")
    print(f"edge={edge_path}")
    print(f"bars={bars_path}")
    print(f"local_jump_trades={local_trade_path}")
    print(f"note={note_path}")
    for path in plot_paths:
        print(f"plot={path}")


if __name__ == "__main__":
    main(
        start_text="2026-06-11T00:00:00Z",
        end_text=None,
        max_age_sec=60,
    )

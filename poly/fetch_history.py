from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import requests

MARKET_ID = "573655"
LABEL = "BTC150K"
CONDITION_ID = "0xa0f4c4924ea1a8b410b4ce821c2a9955fad21a1b19bdcfde90816732278b3dd5"
YES_TOKEN_ID = "13915689317269078219168496739008737517740566192006337297676041270492637394586"
NO_TOKEN_ID = "13290642914521189871602119663452054126359842904805799115978921503195267156991"

OUT_DIR = Path(__file__).resolve().parent / "data"


def get_json(url: str, params: dict | None = None):
    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    return response.json()


# data-api trades 是全市场成交记录；CLOB auth trades 是账户自己的成交记录。
def normalize_trades(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["price"] = pd.to_numeric(df["price"])
    df["size"] = pd.to_numeric(df["size"])
    df["notional"] = df["price"] * df["size"]
    df["source"] = "data-api.trades"
    keep = [
        "timestamp",
        "source",
        "conditionId",
        "asset",
        "outcome",
        "side",
        "price",
        "size",
        "notional",
        "transactionHash",
    ]
    return df[keep].sort_values("timestamp")


def trades_to_1m(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    parts = []
    for outcome, group in df.groupby("outcome"):
        bars = (
            group.set_index("timestamp")
            .resample("1min")
            .agg(
                open=("price", "first"),
                high=("price", "max"),
                low=("price", "min"),
                close=("price", "last"),
                volume=("size", "sum"),
                notional=("notional", "sum"),
                trades=("price", "count"),
            )
            .dropna(subset=["open"])
            .reset_index()
        )
        bars.insert(0, "outcome", outcome)
        bars.insert(1, "source", "data-api.trades_1m")
        parts.append(bars)
    return pd.concat(parts, ignore_index=True).sort_values(["timestamp", "outcome"])


# prices-history 是 Polymarket 官方价格序列，适合补充价格曲线。
def normalize_price_history(payload: dict, outcome: str) -> pd.DataFrame:
    df = pd.DataFrame(payload["history"])
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["t"], unit="s", utc=True)
    df["price"] = pd.to_numeric(df["p"])
    df.insert(0, "outcome", outcome)
    df.insert(1, "source", "clob.prices-history")
    return df[["timestamp", "outcome", "source", "price"]].sort_values("timestamp")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    market = get_json(f"https://gamma-api.polymarket.com/markets/{MARKET_ID}")
    (OUT_DIR / f"{LABEL}-market.json").write_text(
        json.dumps(market, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    trades = get_json(
        "https://data-api.polymarket.com/trades",
        {
            "market": CONDITION_ID,
            "limit": 500,
            "takerOnly": "false",
        },
    )
    trade_df = normalize_trades(trades)
    trade_df.to_parquet(OUT_DIR / f"{LABEL}-trades.parquet", index=False)

    bars = trades_to_1m(trade_df)
    bars.to_parquet(OUT_DIR / f"{LABEL}-1m.parquet", index=False)

    history = []
    for outcome, token_id in (("Yes", YES_TOKEN_ID), ("No", NO_TOKEN_ID)):
        payload = get_json(
            "https://clob.polymarket.com/prices-history",
            {
                "market": token_id,
                "interval": "1d",
                "fidelity": 60,
            },
        )
        history.append(normalize_price_history(payload, outcome))
    history_df = pd.concat(history, ignore_index=True)
    history_df.to_parquet(OUT_DIR / f"{LABEL}-price-history-1h.parquet", index=False)

    print(f"market={market['question']}")
    print(f"trades={len(trade_df)} bars_1m={len(bars)} history={len(history_df)}")
    print(f"out_dir={OUT_DIR}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import sys

from utils.config_loader import ensure_dirs
from utils.config_loader import load_settings
from utils.market_data import MarketDataStore


# 命令行参数优先；没有命令行参数时才用 main(...) 传入的 set。
def main(config_name: str | None = None) -> None:
    selected = (sys.argv[1] if len(sys.argv) > 1 else None) or config_name
    settings = load_settings(selected, mode="backtest")
    ensure_dirs(settings)
    store = MarketDataStore(settings)

    for market in store.markets:
        df = store.fetch_ohlcv(market)
        path = store.raw_ohlcv_path(market)
        store.save_ohlcv(df, path)

        print(f"saved={path}")
        print(f"rows={len(df)}")
        print(f"start={df['timestamp'].iloc[0]}")
        print(f"end={df['timestamp'].iloc[-1]}")

    for path in store.fetch_extra_data():
        print(f"saved={path}")


if __name__ == "__main__":
    main()

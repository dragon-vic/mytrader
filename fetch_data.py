from __future__ import annotations

import sys

from utils.config_loader import ensure_dirs
from utils.config_loader import load_settings
from utils.config_loader import market_configs
from utils.market_data import fetch_ohlcv
from utils.market_data import raw_ohlcv_path
from utils.market_data import save_ohlcv


# 命令行参数优先；没有命令行参数时才用 main(...) 传入的 set。
def main(config_name: str | None = None) -> None:
    selected = (sys.argv[1] if len(sys.argv) > 1 else None) or config_name
    settings = load_settings(selected)
    ensure_dirs(settings)

    for market in market_configs(settings):
        df = fetch_ohlcv(settings, market)
        path = raw_ohlcv_path(settings, market)
        save_ohlcv(df, path)

        print(f"saved={path}")
        print(f"rows={len(df)}")
        print(f"start={df['timestamp'].iloc[0]}")
        print(f"end={df['timestamp'].iloc[-1]}")


if __name__ == "__main__":
    main()

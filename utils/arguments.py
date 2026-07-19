from __future__ import annotations

# run.py 的交互模式和命令行模式。
MODE_OPTIONS = [
    ("回测", "backtest"),
    ("实盘", "live"),
    ("模拟盘", "testnet"),
]
RUN_MODES = ("backtest", "testnet", "live")

# 项目默认入口参数。
DEFAULT_CONFIG_NAME = "pre_ipo"

# 外部信号 data client 名称。
EXTERNAL_SIGNAL_CLIENT_NAME = "EXTERNAL_SIGNAL"

# 外部信号 data client 默认值。
EXTERNAL_SIGNAL_DEFAULT_HOST = "127.0.0.1"
EXTERNAL_SIGNAL_DEFAULT_PORT = 9001
EXTERNAL_SIGNAL_DEFAULT_INSTRUMENT = "BTCUSDT-PERP.BINANCE"
EXTERNAL_SIGNAL_DEFAULT_SIDE = "BUY"
EXTERNAL_COMMAND_CLIENT_NAME = "EXTERNAL_COMMAND"
EXTERNAL_COMMAND_DEFAULT_HOST = "127.0.0.1"
EXTERNAL_COMMAND_DEFAULT_PORT = 9002

# 报告文件名。
ORDERS_FILE = "orders.csv"
POSITIONS_FILE = "positions.csv"
SUMMARY_FILE = "summary.json"

# NT orders report 保留列。
ORDER_COLUMNS = [
    "ts_last",
    "strategy_id",
    "account_id",
    "instrument_id",
    "side",
    "quantity",
    "filled_qty",
    "avg_px",
    "commissions",
    "status",
    "position_id",
    "client_order_id",
]

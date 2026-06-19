from __future__ import annotations


# run.py 的交互模式和命令行模式。
MODE_OPTIONS = [
    ("模拟盘", "testnet"),
    ("回测", "backtest"),
    ("实盘", "live"),
]
RUN_MODES = ("backtest", "testnet", "live")

# 项目默认入口参数。
DEFAULT_CONFIG_NAME = "maxfunding"

# NT msgbus topic。
NODE_STOP_TOPIC = "controls.node.stop"
EVENT_ORDER_TOPIC = "events.order.*"
EVENT_POSITION_TOPIC = "events.position.*"
EVENT_ACCOUNT_TOPIC = "events.account.BINANCE-USDT_FUTURES-master"

# Binance 和外部信号 data client 名称。
BINANCE_CLIENT_NAME = "BINANCE"
EXTERNAL_SIGNAL_CLIENT_NAME = "EXTERNAL_SIGNAL"

# 外部信号 data client 默认值。
EXTERNAL_SIGNAL_DEFAULT_HOST = "127.0.0.1"
EXTERNAL_SIGNAL_DEFAULT_PORT = 9001
EXTERNAL_SIGNAL_DEFAULT_INSTRUMENT = "BTCUSDT-PERP.BINANCE"
EXTERNAL_SIGNAL_DEFAULT_SIDE = "BUY"
EXTERNAL_SIGNAL_SEND_INTERVAL_SECONDS = 60

# 外部信息分析接口默认值。
AI_MODEL = "deepseek-v4-pro"
DEEPSEEK_BASE_URL = "https://api.deepseek.com"

# 行情和 funding 默认接口参数。
FUNDING_API_BASE_URL = "https://fapi.binance.com"
TIMEFRAME_UNITS = {"s": "SECOND", "m": "MINUTE", "h": "HOUR", "d": "DAY"}

# 报告文件名。
ORDERS_FILE = "orders.csv"
POSITIONS_FILE = "positions.csv"
SUMMARY_FILE = "summary.md"

REPORT_FILES = {
    "orders": ORDERS_FILE,
}

LIVE_RESULT_FILES = (
    ORDERS_FILE,
    POSITIONS_FILE,
    "orders_aggregate.csv",
    "positions_aggregate.csv",
    "accounts_aggregate.csv",
    "fills.csv",
    "live_report.csv",
    "live_report_aggregate.csv",
    "summary.md",
    SUMMARY_FILE,
)

OBSOLETE_REPORT_FILES = (
    "account_states.csv",
    "position_events.csv",
    "trades.csv",
    "summary.csv",
    "fills_clean.csv",
    "funding_decisions.csv",
)

# NT trader 原始报告保留列。
REPORT_COLUMNS = {
    "orders": [
        "ts_last",
        "instrument_id",
        "side",
        "quantity",
        "filled_qty",
        "avg_px",
        "commissions",
        "status",
        "position_id",
        "client_order_id",
    ],
    "positions": [
        "ts_opened",
        "ts_closed",
        "instrument_id",
        "entry",
        "quantity",
        "peak_qty",
        "avg_px_open",
        "avg_px_close",
        "realized_pnl",
        "realized_return",
        "commissions",
        "duration_ns",
        "position_id",
        "opening_order_id",
        "closing_order_id",
    ],
    "accounts": [
        "ts_event",
        "currency",
        "total",
        "free",
        "locked",
        "account_id",
        "account_type",
        "base_currency",
        "reported",
        "info",
    ],
}

from __future__ import annotations


# run.py 的交互模式和命令行模式。
MODE_OPTIONS = [
    ("模拟盘", "testnet"),
    ("回测", "backtest"),
    ("实盘", "live"),
]
RUN_MODES = ("backtest", "testnet", "live")

# 项目默认入口参数。
DEFAULT_CONFIG_NAME = "funding"
DEFAULT_TRADER_ID = "TRADER-001"
DEFAULT_LIVE_LOG_FILE = "live_raw"
LOGS_DIR = "logs"
LIVE_LOG_MARKER = "TradingNode: RUNNING"

# NT msgbus topic。
NODE_STOP_TOPIC = "controls.node.stop"
EVENT_ORDER_TOPIC = "events.order.*"
EVENT_POSITION_TOPIC = "events.position.*"
EVENT_ACCOUNT_TOPIC = "events.account.*"

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
TIMEFRAME_UNITS = {"m": "MINUTE", "h": "HOUR", "d": "DAY"}

# 报告文件名。
ORDERS_FILE = "orders.csv"
POSITIONS_FILE = "positions.csv"
RESULT_FILE = "result.csv"
ACCOUNT_CHANGES_FILE = "account_changes.csv"
POSITION_EVENTS_FILE = "position_events.csv"
SUMMARY_FILE = "summary_aggregate.md"

REPORT_FILES = {
    "orders": ORDERS_FILE,
    "accounts": RESULT_FILE,
}

LIVE_RESULT_FILES = (
    ORDERS_FILE,
    POSITIONS_FILE,
    RESULT_FILE,
    ACCOUNT_CHANGES_FILE,
    POSITION_EVENTS_FILE,
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
    "trades.csv",
    "summary.csv",
    "fills_clean.csv",
    "funding_decisions.csv",
)

# NT trader 原始报告保留列。
REPORT_COLUMNS = {
    "orders": [
        "ts_init",
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
    "position_events": [
        "ts_event",
        "event_type",
        "instrument_id",
        "event_side",
        "fill_quantity",
        "fill_price",
        "realized_pnl",
        "adjustment_type",
        "quantity_change",
        "pnl_change",
        "reason",
        "account_id",
        "strategy_id",
        "position_id",
    ],
}

ACCOUNT_COLUMNS = [
    "ts_event",
    "currency",
    "total",
    "free",
    "locked",
    "event_type",
    "info_type",
    "info_reason",
    "info",
    "account_id",
    "account_type",
    "base_currency",
    "is_reported",
]

POSITION_COLUMNS = [
    "ts_event",
    "instrument_id",
    "event_side",
    "fill_quantity",
    "fill_price",
    "realized_pnl",
    "adjustment_type",
    "quantity_change",
    "pnl_change",
    "reason",
    "event_type",
    "account_id",
    "strategy_id",
    "position_id",
]

# summary_aggregate.md 字段和中文标签。
EMPTY_SUMMARY = {
    "trades": 0,
    "win_rate": 0.0,
    "realized_pnl": 0.0,
    "estimated_funding_income": 0.0,
    "actual_funding_income": 0.0,
    "net_with_funding": 0.0,
    "avg_trade_net": 0.0,
    "best_trade_net": 0.0,
    "worst_trade_net": 0.0,
    "gross_profit": 0.0,
    "gross_loss": 0.0,
    "profit_factor": "",
    "total_commissions": 0.0,
    "avg_duration_min": 0.0,
}

SUMMARY_LABELS = {
    "trades": "交易次数",
    "win_rate": "胜率",
    "realized_pnl": "已实现盈亏",
    "estimated_funding_income": "预估资金费收入",
    "actual_funding_income": "实际资金费收入",
    "net_with_funding": "含资金费净收益",
    "avg_trade_net": "单笔平均净收益",
    "best_trade_net": "最佳单笔净收益",
    "worst_trade_net": "最差单笔净收益",
    "gross_profit": "盈利交易合计",
    "gross_loss": "亏损交易合计",
    "profit_factor": "盈利因子",
    "total_commissions": "总手续费",
    "avg_duration_min": "平均持仓分钟",
}

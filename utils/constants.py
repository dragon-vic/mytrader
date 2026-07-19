from __future__ import annotations

from pathlib import Path
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parent.parent
STRATEGIES_DIR = PROJECT_ROOT / "strategies"
GLOBAL_CONFIG_PATH = STRATEGIES_DIR / "global.yaml"
LOCAL_TZ = ZoneInfo("Asia/Shanghai")

# run.py 的交互模式和命令行模式。
MODE_OPTIONS = [
    ("回测", "backtest"),
    ("实盘", "live"),
    ("模拟盘", "testnet"),
]
RUN_MODES = ("backtest", "testnet", "live")

# 项目默认入口参数。
DEFAULT_CONFIG_NAME = "pre_ipo"
CONFIG_FILES = {
    "backtest": "backtest_config.yaml",
    "live": "live_config.yaml",
    "testnet": "live_config.yaml",
}

# 外部命令 data client 默认值。
EXTERNAL_COMMAND_CLIENT_NAME = "EXTERNAL_COMMAND"
EXTERNAL_COMMAND_DEFAULT_HOST = "127.0.0.1"
EXTERNAL_COMMAND_DEFAULT_PORT = 9002

# Node 内部停止控制 topic。
NODE_STOP_TOPIC = "controls.node.stop"

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

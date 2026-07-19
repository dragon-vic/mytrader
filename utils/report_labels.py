from __future__ import annotations

import pandas as pd


COLUMN_LABELS = {
    "ts_last": "成交时间",
    "strategy_id": "策略ID",
    "account_id": "账户ID",
    "instrument_id": "标的",
    "side": "方向",
    "quantity": "数量",
    "filled_qty": "已成交数量",
    "avg_px": "平均成交价",
    "commissions": "手续费合计",
    "status": "订单状态",
    "client_order_id": "客户端订单ID",
    "position_id": "仓位ID",
    "open_time": "开仓时间",
    "close_time": "平仓时间",
    "qty": "数量",
    "avg_open": "开仓均价",
    "avg_close": "平仓均价",
    "realized_pnl": "已实现盈亏",
    "realized_return": "已实现收益率",
    "duration_min": "持仓分钟",
    "opening_order_id": "开仓订单ID",
    "closing_order_id": "平仓订单ID",
}


# 输出 CSV 前把内部英文列名改成中文。
def to_chinese_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=COLUMN_LABELS)

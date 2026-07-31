from __future__ import annotations

import json
from typing import Any

from adapters.external_json import ExternalJson
from adapters.external_json import external_json_type
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import CustomData
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.trading.strategy import Strategy

from utils.constants import EXTERNAL_JSON_CLIENT_NAME
from utils.constants import EXTERNAL_JSON_SEND_TOPIC


class AgentTradingConfig(StrategyConfig, frozen=True):
    pass


class AgentTradingStrategy(Strategy):
    # 初始化 Agent 交易策略；具体交易参数在方案确认后加入。
    def __init__(self, config: AgentTradingConfig) -> None:
        super().__init__(config)

    # 订阅外部 Agent 的通用 JSON 数据。
    def on_start(self) -> None:
        self.subscribe_data(
            external_json_type(),
            client_id=ClientId(EXTERNAL_JSON_CLIENT_NAME),
        )
        self.log.info("agent_trading started")

    # 业务 schema 确定前只接收和记录原始 JSON。
    def on_data(self, data) -> None:
        payload = data.data if isinstance(data, CustomData) else data
        if isinstance(payload, ExternalJson):
            message = json.loads(payload.payload)
            if not isinstance(message, dict):
                raise TypeError("agent message must be a JSON object")
            self._handle_agent_message(message)

    def on_stop(self) -> None:
        self.unsubscribe_data(
            external_json_type(),
            client_id=ClientId(EXTERNAL_JSON_CLIENT_NAME),
        )
        self.log.info("agent_trading stopped")

    # 交易 schema 确定后，仓位检查和 NT 下单逻辑统一从这里进入。
    def _handle_agent_message(self, payload: dict[str, Any]) -> None:
        self.log.info(
            f"agent_json_received payload="
            f"{json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}",
        )

    # 将 NT 内部消息发送给外部 Controller。
    def send_json(self, payload: dict[str, Any]) -> None:
        self.msgbus.publish(EXTERNAL_JSON_SEND_TOPIC, payload)

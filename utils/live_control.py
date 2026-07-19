from __future__ import annotations

from nautilus_trader.common.enums import LogColor
from nautilus_trader.live.node import TradingNode

from utils.control_messages import NODE_STOP_TOPIC
from utils.control_messages import NodeStopRequest


class NodeStopController:
    def __init__(self, node: TradingNode) -> None:
        self.node = node
        self.loop = node.get_event_loop()
        self.requested = False
        self.attached = False
        self.handler = self.request

    def attach(self) -> None:
        if self.attached:
            return
        self.node.trader.subscribe(NODE_STOP_TOPIC, self.handler)
        self.attached = True

    def detach(self) -> None:
        if self.attached:
            self.node.trader.unsubscribe(NODE_STOP_TOPIC, self.handler)
            self.attached = False

    # 任意线程都通过 node event loop 发起一次幂等停止。
    def request(self, message: NodeStopRequest) -> None:
        if not isinstance(message, NodeStopRequest):
            raise TypeError(f"{NODE_STOP_TOPIC} requires NodeStopRequest")
        self.loop.call_soon_threadsafe(self._stop, message)

    def _stop(self, message: NodeStopRequest) -> None:
        if self.requested:
            self.node.get_logger().warning(
                f"NODE_STOP_DUPLICATE source={message.source} reason={message.reason}",
            )
            return
        self.requested = True
        self.node.get_logger().info(
            f"NODE_STOP_REQUEST source={message.source} reason={message.reason}",
            color=LogColor.YELLOW,
        )
        self.loop.create_task(self.node.stop_async())

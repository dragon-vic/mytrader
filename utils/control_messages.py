from __future__ import annotations

from dataclasses import dataclass


NODE_STOP_TOPIC = "controls.node.stop"


@dataclass(frozen=True)
class NodeStopRequest:
    source: str
    reason: str

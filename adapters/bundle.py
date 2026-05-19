from __future__ import annotations

from dataclasses import dataclass

from nautilus_trader.config import CacheConfig
from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.config import LiveExecClientConfig
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory


@dataclass(frozen=True)
class ClientBundle:
    name: str
    cache: CacheConfig
    data_config: LiveDataClientConfig
    exec_config: LiveExecClientConfig
    data_factory: type[LiveDataClientFactory]
    exec_factory: type[LiveExecClientFactory]

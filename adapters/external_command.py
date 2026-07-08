import asyncio
import json
from typing import Any

from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.core.data import Data
from nautilus_trader.data.messages import RequestData
from nautilus_trader.data.messages import SubscribeData
from nautilus_trader.data.messages import UnsubscribeData
from nautilus_trader.live.data_client import LiveDataClient
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.model.custom import customdataclass
from nautilus_trader.model.data import CustomData
from nautilus_trader.model.data import DataType
from nautilus_trader.model.identifiers import ClientId

from utils.arguments import EXTERNAL_COMMAND_CLIENT_NAME
from utils.arguments import EXTERNAL_COMMAND_DEFAULT_HOST
from utils.arguments import EXTERNAL_COMMAND_DEFAULT_PORT


@customdataclass
class ExternalCommand(Data):
    command: str = ""
    reason: str = ""
    source: str = ""
    sent_ns: int = 0


# 构建外部命令的 NT data type。
def external_command_type() -> DataType:
    return DataType(ExternalCommand)


class ExternalCommandDataClientConfig(LiveDataClientConfig, frozen=True):
    host: str = EXTERNAL_COMMAND_DEFAULT_HOST
    port: int = EXTERNAL_COMMAND_DEFAULT_PORT


class ExternalCommandDataClient(LiveDataClient):
    def __init__(self, loop, msgbus, cache, clock, config, name=None):
        super().__init__(
            loop=loop,
            client_id=ClientId(name or EXTERNAL_COMMAND_CLIENT_NAME),
            venue=None,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )
        self._config = config
        self._server = None
        self._subscribed = False

    async def _connect(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client,
            self._config.host,
            self._config.port,
        )
        self._log.info(f"Listening external commands on {self._config.host}:{self._config.port}")

    async def _disconnect(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _subscribe(self, command: SubscribeData) -> None:
        self._subscribed = True
        self._log.info(f"Subscribed: {command.data_type}")

    async def _unsubscribe(self, command: UnsubscribeData) -> None:
        self._subscribed = False
        self._log.info(f"Unsubscribed: {command.data_type}")

    async def _request(self, request: RequestData) -> None:
        self._log.error("ExternalCommandDataClient does not support request_data")

    async def _handle_client(self, reader, writer) -> None:
        line = await reader.readline()
        payload = json.loads(line)
        if self._subscribed:
            sent_ns = int(payload["sent_ns"])
            command = ExternalCommand(
                sent_ns,
                sent_ns,
                command=str(payload["command"]),
                reason=str(payload["reason"]),
                source=str(payload["source"]),
                sent_ns=sent_ns,
            )
            self._handle_data(CustomData(data_type=external_command_type(), data=command))
        writer.close()
        await writer.wait_closed()


class ExternalCommandLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop, name, config, msgbus, cache, clock):
        return ExternalCommandDataClient(
            loop=loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
            name=name,
        )


# 构建外部命令 live data client 配置。
def build_data_client(settings: dict[str, Any], cfg: dict[str, Any]):
    return (
        cfg["client_id"],
        ExternalCommandDataClientConfig(
            host=cfg.get("host", EXTERNAL_COMMAND_DEFAULT_HOST),
            port=int(cfg.get("port", EXTERNAL_COMMAND_DEFAULT_PORT)),
        ),
        ExternalCommandLiveDataClientFactory,
    )

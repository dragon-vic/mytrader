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
from nautilus_trader.model.identifiers import InstrumentId

from adapters.common import LiveContext
from utils.arguments import EXTERNAL_SIGNAL_CLIENT_NAME
from utils.arguments import EXTERNAL_SIGNAL_DEFAULT_HOST
from utils.arguments import EXTERNAL_SIGNAL_DEFAULT_INSTRUMENT
from utils.arguments import EXTERNAL_SIGNAL_DEFAULT_PORT
from utils.arguments import EXTERNAL_SIGNAL_DEFAULT_SIDE


@customdataclass
class ExternalSignal(Data):
    instrument_id: InstrumentId = InstrumentId.from_str(EXTERNAL_SIGNAL_DEFAULT_INSTRUMENT)
    side: str = EXTERNAL_SIGNAL_DEFAULT_SIDE
    sent_ns: int = 0


# 构建外部信号的 NT data type。
def external_signal_type() -> DataType:
    return DataType(ExternalSignal)


class ExternalSignalDataClientConfig(LiveDataClientConfig, frozen=True):
    host: str = EXTERNAL_SIGNAL_DEFAULT_HOST
    port: int = EXTERNAL_SIGNAL_DEFAULT_PORT


class ExternalSignalDataClient(LiveDataClient):
    """Receives one JSON signal per TCP connection.

    Required fields: instrument_id, side and sent_ns.
    """

    def __init__(self, loop, msgbus, cache, clock, config, name=None):
        super().__init__(
            loop=loop,
            client_id=ClientId(name or EXTERNAL_SIGNAL_CLIENT_NAME),
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
        self._log.info(f"Listening external signals on {self._config.host}:{self._config.port}")

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
        self._log.error("ExternalSignalDataClient does not support request_data")

    async def _handle_client(self, reader, writer) -> None:
        try:
            payload = json.loads(await reader.readline())
            sent_ns = int(payload["sent_ns"])
            side = str(payload["side"]).upper()
            if side not in {"BUY", "SELL"}:
                raise ValueError(f"side must be BUY or SELL, got {side}")
            signal = ExternalSignal(
                sent_ns,
                sent_ns,
                instrument_id=InstrumentId.from_str(payload["instrument_id"]),
                side=side,
                sent_ns=sent_ns,
            )
            if self._subscribed:
                self._handle_data(CustomData(data_type=external_signal_type(), data=signal))
        except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            self._log.warning(f"Dropped invalid external signal: {exc}")
        finally:
            writer.close()
            await writer.wait_closed()


class ExternalSignalLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop, name, config, msgbus, cache, clock):
        return ExternalSignalDataClient(
            loop=loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
            name=name,
        )


def normalize_client(_cfg: dict[str, Any]) -> None:
    pass


# 构建外部信号 live data client 配置。
def build_data_client(_context: LiveContext, cfg: dict[str, Any]):
    return (
        cfg["client_id"],
        ExternalSignalDataClientConfig(
            host=cfg["host"],
            port=int(cfg["port"]),
        ),
        ExternalSignalLiveDataClientFactory,
    )

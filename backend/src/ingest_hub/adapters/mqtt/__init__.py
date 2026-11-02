"""The MQTT adapter: JSON messages on subscribed topics, routed to tables by topic filter.

Each message whose topic matches a route becomes one point: the payload's numeric values are its
fields, its tags come from the route (optionally from topic segments), and its time is when the
message was received, or a time the payload carries. Messages no route matches, and payloads
that are not JSON objects with a numeric value, are ignored.

The session is persistent (a fixed client identifier and no clean start), so the broker keeps
QoS 1 and 2 messages for the hub across a reconnect.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import Literal, Protocol

import aiomqtt
import httpx2
from jigkit.sinks import Precision
from pydantic import BaseModel, ConfigDict, Field

from ingest_hub.adapters.mqtt.mapping import Route, message_point, route_for
from ingest_hub.collect import SourceContext
from ingest_hub.config import read_secret

logger = logging.getLogger(__name__)


class MqttParams(BaseModel):
    """An MQTT source's ``params`` in the configuration file."""

    model_config = ConfigDict(extra="forbid")

    host: str = Field(min_length=1)
    port: int = Field(1883, ge=1, le=65535)
    username: str | None = None
    password_file: Path | None = None
    #: The client identifier, which names the persistent session; the source's name when unset.
    client_id: str | None = None
    qos: Literal[0, 1, 2] = 1
    keepalive: int = Field(60, ge=5)
    #: The unit point times are written in.
    precision: Literal["s", "ms", "us", "ns"] = "s"
    routes: list[Route] = Field(min_length=1)


class Message(Protocol):
    """What the source needs from a received message."""

    @property
    def topic(self) -> object:
        """The topic; its ``str()`` is the topic's text."""

    @property
    def payload(self) -> object:
        """The payload bytes."""


class Session(Protocol):
    """A connected client: subscribe, then iterate messages."""

    async def subscribe(self, topic: str, qos: int = 0) -> object:
        """Subscribe to a topic filter."""

    @property
    def messages(self) -> AsyncIterator[Message]:
        """Messages as they arrive, until the connection drops."""


Connect = Callable[[], AbstractAsyncContextManager[Session]]


class MqttSource:
    """Subscribes to every route's filter and turns matching messages into points."""

    def __init__(
        self,
        params: MqttParams,
        connect: Connect,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._params = params
        self._connect = connect
        self._clock = clock
        self._precision = Precision(params.precision)

    async def run(self, context: SourceContext) -> None:
        """Connect, subscribe and deliver until the connection drops, which raises.

        Every batch is reported as success, so a broker that keeps sending resets the back-off.
        """
        async with self._connect() as session:
            for route in self._params.routes:
                await session.subscribe(route.topic, qos=self._params.qos)
            logger.info(
                "MQTT %s: subscribed to %d topic filter(s)",
                context.name,
                len(self._params.routes),
            )
            context.succeeded()
            async for message in session.messages:
                topic = str(message.topic)
                route = route_for(self._params.routes, topic)
                if route is None:
                    continue
                payload = message.payload
                if not isinstance(payload, bytes | bytearray):
                    payload = str(payload).encode()
                point = message_point(route, topic, bytes(payload), self._clock(), self._precision)
                if point is None:
                    continue
                await context.emit([point])
                context.succeeded()
        raise ConnectionError("the MQTT connection closed")


def _aiomqtt_connect(params: MqttParams, client_id: str) -> Connect:
    """A connect function backed by aiomqtt, with the password read now."""
    password = read_secret(params.password_file) if params.password_file else None

    def connect() -> AbstractAsyncContextManager[Session]:
        return aiomqtt.Client(
            params.host,
            params.port,
            username=params.username,
            password=password,
            identifier=client_id,
            clean_session=False,
            keepalive=params.keepalive,
        )

    return connect


def build(params: MqttParams, _client: httpx2.AsyncClient, name: str) -> MqttSource:
    """An MQTT source from its validated parameters."""
    return MqttSource(params, _aiomqtt_connect(params, params.client_id or f"ingest-hub-{name}"))


__all__ = ["MqttParams", "MqttSource", "Route", "build"]

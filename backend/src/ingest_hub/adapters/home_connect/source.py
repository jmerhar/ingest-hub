"""The Home Connect source: every appliance's state, followed live on Home Connect's event stream.

When it connects, the source reads the appliances paired with the account and each connected
one's status, settings and programmes, then follows the event stream, writing each change as it
arrives. Every ``sample_minutes`` it writes each appliance's whole state again, from what it
already knows, so a quiet appliance still has recent points and the source's heartbeat keeps
beating. A stream that ends, or whose connection drops once it has delivered something, is
opened again; one that fails before delivering anything is a failure, retried after the source's
back-off, so a connection that keeps failing cannot spend the daily request limit. The full state
is read again on reopening only if the last read is ``resync_minutes`` old. The same holds for
an appliance that reconnects, and for one the stream names that the source does not know yet
(read with every other appliance), so neither a failing source nor an appliance whose connection
keeps dropping can use up Home Connect's daily request limit; a newly paired appliance is read at
once.

A read replaces what is known of an appliance rather than adding to it, since Home Connect stops
sending what no longer applies, such as a finished program's progress, rather than clearing it.
So when an appliance's operation state changes (a program starts, finishes, pauses), its state is
read again, at most every ``REREAD_SECONDS``: a change inside that time is read once it has
passed. The whole-state samples then repeat only what is true, and a field that was only ever an
event, such as a program having finished, is written when it happens.

Each appliance type has its table, ``<table_prefix><type>`` such as ``home_connect_dishwasher``,
tagged ``appliance`` with the appliance's Home Connect ID; fields are named as ``values.py``
describes, plus ``connected``. A field keeps the type it was first written with (kept in the
state directory); a later value of another type is left out and reported, since InfluxDB would
refuse it. Each appliance's progress is the time of the last message received, so a stream that
stops arriving is noticed; its label is the appliance's brand, model and type.

The access token lasts a day and is renewed before it expires. A refresh token Home Connect no
longer accepts, or a missing login, ends the run with the command that fixes it.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx2
from jigkit.sinks import FieldValue, Point, Precision
from pydantic import BaseModel, ConfigDict, Field

from ingest_hub.adapters.home_connect.api import (
    ApiError,
    Appliance,
    Event,
    HomeConnectApi,
    RateLimited,
    StreamBroken,
    Unauthorized,
)
from ingest_hub.adapters.home_connect.auth import (
    URL,
    AuthError,
    DeviceLogin,
    TokenRefused,
    Tokens,
    finish_login,
    load_tokens,
    refresh,
    save_tokens,
    start_login,
)
from ingest_hub.adapters.home_connect.values import field_name, field_value, snake, type_of
from ingest_hub.adapters.inbox import write_json
from ingest_hub.collect import Progress, SignInStep, SourceContext

logger = logging.getLogger(__name__)

#: The type each field was first written with, in the source's state directory.
STATE_FILE = "home-connect.json"
#: The pause before opening a stream again after Home Connect closed it.
RECONNECT_SECONDS = 5.0
#: Events that carry an appliance's values.
VALUE_EVENTS = ("STATUS", "EVENT", "NOTIFY")
#: The field whose change has an appliance's state read again.
OPERATION_STATE = "status_operation_state"
#: The least time between two reads of one appliance's state after its operation state changes.
REREAD_SECONDS = 300.0


class NotSignedIn(AuthError):
    """No login has been completed for the source yet."""


class HomeConnectParams(BaseModel):
    """A Home Connect source's ``params`` in the configuration file."""

    model_config = ConfigDict(extra="forbid")

    #: The client ID of the application registered with "Device Flow".
    client_id_file: Path
    table_prefix: str = Field("home_connect_", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    #: How often every appliance's whole state is written, even when nothing changes.
    sample_minutes: float = Field(15, gt=0)
    #: How old the last full read must be before a reconnect reads every appliance again.
    resync_minutes: float = Field(15, ge=0)
    url: str = URL


@dataclass
class Tracked:
    """What the source knows of one appliance."""

    appliance: Appliance
    values: dict[str, FieldValue] = field(default_factory=dict)
    #: Whether its operation state changed since its state was last read.
    stale: bool = False

    @property
    def table_suffix(self) -> str:
        return snake(self.appliance.type) or "appliance"


class HomeConnectSource:
    """Follows every appliance of one Home Connect account."""

    def __init__(
        self, params: HomeConnectParams, client: httpx2.AsyncClient, name: str, client_id: str
    ) -> None:
        self._params = params
        self._client = client
        self._name = name
        self._client_id = client_id
        self._appliances: dict[str, Tracked] = {}
        self._types: dict[str, str] = {}
        self._refused: set[str] = set()
        self._last_sync = float("-inf")
        self._last_sample = float("-inf")
        #: When each appliance's state was last read.
        self._read_at: dict[str, float] = {}
        self._login: DeviceLogin | None = None
        #: The clock and sleep a sign-in waits with, which runs outside the source's own run.
        self.now = time.time
        self.sleep = asyncio.sleep

    @property
    def fix(self) -> str:
        return f"run `ingest-hub home-connect-auth --source {self._name}`"

    def signed_in(self, state_dir: Path) -> bool:
        """Whether a sign-in is kept in the state directory."""
        try:
            return load_tokens(state_dir) is not None
        except AuthError:
            return False

    async def start_sign_in(self, _state_dir: Path) -> SignInStep:
        """Start a device login; the person confirms its code at the address it gives."""
        self._login = await start_login(self._client, self._client_id, self._params.url)
        return SignInStep(
            url=self._login.verification_url,
            code=self._login.user_code or None,
            instructions=(
                "Open the link, sign in with the Home Connect account your appliances are paired "
                "with, and allow the access it asks for."
            ),
        )

    async def finish_sign_in(self, state_dir: Path, _answer: str | None = None) -> str:
        """Wait until the started login's code is confirmed, and keep the tokens it gives."""
        if self._login is None:
            raise AuthError("no sign-in was started")
        login, self._login = self._login, None
        tokens = await finish_login(
            self._client,
            self._client_id,
            login,
            now=self.now,
            sleep=self.sleep,
            url=self._params.url,
        )
        appliances = await HomeConnectApi(
            self._client, tokens.access_token, self._params.url
        ).appliances()
        save_tokens(state_dir, tokens)
        names = ", ".join(a.label for a in appliances) or "none yet"
        return f"Signed in. Appliances: {names}."

    async def run(self, context: SourceContext) -> None:
        """Read the appliances and follow the event stream for ever; a failure ends the run."""
        tokens = load_tokens(context.state_dir)
        if tokens is None:
            raise NotSignedIn(f"not signed in to Home Connect yet: {self.fix}")
        self._types = _load_types(context.state_dir / STATE_FILE)
        refreshed = False
        while True:
            if not tokens.fresh(context.clock()):
                tokens = await self._refresh(context, tokens)
                refreshed = True
            api = HomeConnectApi(self._client, tokens.access_token, self._params.url)
            try:
                if context.clock() - self._last_sync >= self._params.resync_minutes * 60:
                    await self._sync(context, api)
                await self._follow(context, api, tokens)
            except Unauthorized:
                if refreshed:
                    raise
                tokens = dataclasses.replace(tokens, access_token="")
                continue
            except RateLimited as error:
                context.problem(str(error))
                logger.warning("%s: %s", self._name, error)
                await context.sleep(error.retry_after)
                continue
            finally:
                logger.debug("%s: %d request(s) on this connection", self._name, api.requests)
            refreshed = False
            await context.sleep(RECONNECT_SECONDS)

    async def _refresh(self, context: SourceContext, tokens: Tokens) -> Tokens:
        try:
            tokens = await refresh(
                self._client, self._client_id, tokens, context.clock(), self._params.url
            )
        except TokenRefused as error:
            raise TokenRefused(f"{error}: {self.fix}") from error
        save_tokens(context.state_dir, tokens)
        return tokens

    async def _sync(self, context: SourceContext, api: HomeConnectApi) -> None:
        """Read the appliances and each connected one's state, and write it all."""
        appliances = await api.appliances()
        self._appliances = {
            a.ha_id: Tracked(
                a, self._appliances[a.ha_id].values if a.ha_id in self._appliances else {}
            )
            for a in appliances
        }
        for tracked in self._appliances.values():
            if tracked.appliance.connected:
                await self._read(context, api, tracked)
            else:
                tracked.values["connected"] = False
        self._last_sync = context.clock()
        await self._sample(context)
        names = ", ".join(t.appliance.label for t in self._appliances.values()) or "none"
        logger.info("%s: appliances: %s", self._name, names)

    async def _follow(self, context: SourceContext, api: HomeConnectApi, tokens: Tokens) -> None:
        """Follow the event stream until it ends or the access token is about to expire. A stream
        whose connection drops after it delivered something ends as one Home Connect closes; one
        that fails before delivering anything is a failure."""
        delivered = False
        try:
            async for event in api.events():
                delivered = True
                context.succeeded()
                await self._handle(context, api, event)
                await self._read_stale(context, api)
                if context.clock() - self._last_sample >= self._params.sample_minutes * 60:
                    await self._sample(context)
                if not tokens.fresh(context.clock()):
                    return
        except StreamBroken as error:
            if not delivered:
                raise
            logger.info("%s: %s; opening it again", self._name, error)

    def _due(self, since: float, context: SourceContext) -> bool:
        """Whether a read last made at ``since`` is ``resync_minutes`` old."""
        return context.clock() - since >= self._params.resync_minutes * 60

    async def _read(self, context: SourceContext, api: HomeConnectApi, tracked: Tracked) -> dict:
        """Read a connected appliance's state, replacing what is known of it; its fields."""
        self._read_at[tracked.appliance.ha_id] = context.clock()
        tracked.stale = False
        fields = {"connected": True, **_fields(await api.state(tracked.appliance.ha_id))}
        tracked.values = dict(fields)
        return fields

    async def _read_stale(self, context: SourceContext, api: HomeConnectApi) -> None:
        """Read again, and write, each appliance whose operation state changed, once
        ``REREAD_SECONDS`` have passed since its last read."""
        for ha_id, tracked in self._appliances.items():
            last = self._read_at.get(ha_id, float("-inf"))
            if tracked.stale and context.clock() - last >= REREAD_SECONDS:
                fields = await self._read(context, api, tracked)
                await self._emit(context, [self._point(tracked, context.now_ms(), fields)])

    async def _handle(self, context: SourceContext, api: HomeConnectApi, event: Event) -> None:
        unknown = event.ha_id and event.ha_id not in self._appliances and event.kind != "KEEP-ALIVE"
        if event.kind in ("PAIRED", "DEPAIRED") or (
            unknown and self._due(self._last_sync, context)
        ):
            await self._sync(context, api)
            return
        tracked = self._appliances.get(event.ha_id)
        if tracked is None:
            return
        now_ms = context.now_ms()
        if event.kind in ("CONNECTED", "DISCONNECTED"):
            connected = event.kind == "CONNECTED"
            changes: dict[str, FieldValue] = {"connected": connected}
            if connected and self._due(self._read_at.get(event.ha_id, float("-inf")), context):
                changes = await self._read(context, api, tracked)
            tracked.values["connected"] = connected
            await self._emit(context, [self._point(tracked, now_ms, changes)])
        elif event.kind in VALUE_EVENTS:
            operation = tracked.values.get(OPERATION_STATE)
            by_time: dict[int, list[tuple[str, Any]]] = defaultdict(list)
            for item in event.items:
                stamp = item.get("timestamp")
                when = int(stamp * 1000) if isinstance(stamp, (int, float)) else now_ms
                by_time[when].append((item["key"], item.get("value")))
            points = [
                self._point(tracked, when, self._apply(tracked, items))
                for when, items in sorted(by_time.items())
            ]
            await self._emit(context, points)
            if tracked.values.get(OPERATION_STATE) != operation:
                tracked.stale = True

    def _apply(self, tracked: Tracked, items: list[tuple[str, Any]]) -> dict[str, FieldValue]:
        """Take keys and values into what is known of an appliance; the fields they set."""
        changes = _fields(items)
        tracked.values.update(changes)
        return changes

    def _point(self, tracked: Tracked, when_ms: int, fields: dict[str, FieldValue]) -> Point:
        return Point(
            self._params.table_prefix + tracked.table_suffix,
            (("appliance", tracked.appliance.ha_id),),
            when_ms,
            dict(fields),
            precision=Precision.MILLISECONDS,
        )

    async def _sample(self, context: SourceContext) -> None:
        """Write every appliance's whole state as it is now."""
        now_ms = context.now_ms()
        await self._emit(
            context, [self._point(t, now_ms, t.values) for t in self._appliances.values()]
        )
        self._last_sample = context.clock()

    async def _emit(self, context: SourceContext, points: list[Point]) -> None:
        """Hand points over, each field checked against the type it was first written with, with
        every appliance's progress up to now."""
        checked = [p for p in (self._typed(context, p) for p in points) if p.fields]
        now_ms = context.now_ms()
        progress = [
            Progress(ha_id, now_ms, tracked.appliance.label)
            for ha_id, tracked in self._appliances.items()
        ]
        if not await context.emit(checked, progress):
            context.problem("a batch was refused; its values are lost")

    def _typed(self, context: SourceContext, point: Point) -> Point:
        fields: dict[str, FieldValue] = {}
        learned = False
        for name, value in point.fields.items():
            key = f"{point.table}.{name}"
            kind = type_of(value)
            learned = learned or key not in self._types
            known = self._types.setdefault(key, kind)
            if known == kind:
                fields[name] = value
                continue
            if key not in self._refused:
                self._refused.add(key)
                message = f"{key} arrived as {kind}, but was first written as {known}; left out"
                context.problem(message)
                logger.warning("%s: %s", self._name, message)
        if learned:
            _save_types(context.state_dir / STATE_FILE, self._types)
        return dataclasses.replace(point, fields=fields)


def _fields(items: list[tuple[str, Any]]) -> dict[str, FieldValue]:
    """Home Connect's keys and values as fields, leaving out what has no value."""
    fields: dict[str, FieldValue] = {}
    for key, raw in items:
        value = field_value(key, raw)
        if value is not None:
            fields[field_name(key)] = value
    return fields


def _load_types(path: Path) -> dict[str, str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as error:
        raise ApiError(f"cannot read {path.name}: {error}") from error
    types = data.get("field_types") if isinstance(data, dict) else None
    return {str(k): str(v) for k, v in types.items()} if isinstance(types, dict) else {}


def _save_types(path: Path, types: dict[str, str]) -> None:
    write_json(path, {"field_types": dict(sorted(types.items()))}, indent=1)


__all__ = ["HomeConnectParams", "HomeConnectSource", "NotSignedIn"]

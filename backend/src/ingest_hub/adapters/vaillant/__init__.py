"""Vaillant heating systems through the myVAILLANT cloud, using the myPyllant library.

Two kinds of data, on two schedules:

- **Energy**, every ``energy_minutes``: each device's hourly buckets for every operation mode and
  energy type the device reports, in Wh, into ``<prefix>energy`` with tags ``device`` (the
  device type, lower-cased), ``operation_mode`` and ``energy_type``, at the bucket's start.
  Buckets arrive late and are revised, so each poll re-reads ``rescan_hours`` before the device's
  cursor; the cursor, published as the device's progress, trails the poll by ``lag_hours``.
  A device without a cursor is read from its first data.
- **Live state**, every ``state_minutes``: current power per device (``<prefix>power``) and the
  temperatures, pressures, setpoints and states of the system, its circuits, zones and hot-water
  cylinders (``<prefix>system``, ``…circuit``, ``…zone``, ``…dhw``), stamped when read.

Vaillant enforces request quotas and has frequent incidents. A refused call (HTTP 429, or 403 naming
the quota) or a server error, at login or later, is waited out for as long as Vaillant says or at
most ``pause_minutes``, rather than retried at once; bucket series are requested one at a time,
a moment apart.
"""

from __future__ import annotations

import copy
import datetime as dt
import json
import logging
import os
import re
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import Any, Protocol

import httpx2
from jigkit.sinks import FieldValue, Point, Precision
from pydantic import BaseModel, ConfigDict, Field

from ingest_hub.collect import Progress, SourceContext
from ingest_hub.config import read_secret

logger = logging.getLogger(__name__)

HOUR = dt.timedelta(hours=1)
STATE_FILE = "vaillant.json"
#: A session this close to expiry is refreshed before the next call.
REFRESH_MARGIN = dt.timedelta(minutes=2)
#: HTTP statuses that mean "not now" rather than "never"; a 403 does too when it names the quota.
TRANSIENT = frozenset({429, 500, 502, 503, 504})
#: How Vaillant says when a spent quota comes back: "replenished in 00:00:49".
REPLENISHED = re.compile(r"replenished in (\d+):(\d{2}):(\d{2})")
#: Seconds added to the replenishment time Vaillant names, so the next call is not early.
QUOTA_MARGIN = 10
#: The first energy poll waits this long after the first state poll, so the two bursts of
#: requests do not land together.
ENERGY_DELAY = 60.0
#: Seconds between one bucket series and the next, to stay under the per-minute quota.
SERIES_SPACING = 2.0


class VaillantParams(BaseModel):
    """A Vaillant source's ``params`` in the configuration file."""

    model_config = ConfigDict(extra="forbid")

    username_file: Path
    password_file: Path
    brand: str = "vaillant"
    #: The country the account was registered in, as myPyllant names it.
    country: str = "netherlands"
    table_prefix: str = Field("vaillant_", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    energy_minutes: float = Field(60, ge=15)
    state_minutes: float = Field(5, ge=1)
    lag_hours: float = Field(3, ge=0)
    rescan_hours: float = Field(48, ge=0)
    pause_minutes: float = Field(15, gt=0)


class Api(Protocol):
    """The parts of ``myPyllant.api.MyPyllantAPI`` the source uses."""

    oauth_session_expires: dt.datetime | None

    def get_systems(self, **kwargs: Any) -> AsyncIterator[Any]:
        """Each system the account sees, with its state and devices."""

    def get_data_by_device(
        self, device: Any, data_resolution: Any, data_from: Any, data_to: Any
    ) -> AsyncIterator[Any]:
        """A device's buckets, one item per operation mode and energy type."""

    async def refresh_token(self) -> None:
        """Renew the session."""


Connect = Callable[[], AbstractAsyncContextManager[Api]]


def _slug(value: object) -> str:
    """A Vaillant enum value as a tag value: ``HEATPUMP`` becomes ``heatpump``."""
    return str(value or "unknown").strip().lower()


def _transient(error: BaseException) -> bool:
    """Whether a failed call means "not now": a quota refusal (429, or 403 naming the quota) or
    a server error."""
    status = _http_status(error)
    if status == 403:
        return "quota" in str(error).lower()
    return status in TRANSIENT


def _pause_seconds(error: BaseException, longest: float) -> float:
    """How long to wait after a refusal: the replenishment time Vaillant names plus a margin,
    else ``longest``, and never more than ``longest``."""
    match = REPLENISHED.search(str(error))
    if match is None:
        return longest
    hours, minutes, seconds = (int(part) for part in match.groups())
    return min(longest, hours * 3600 + minutes * 60 + seconds + QUOTA_MARGIN)


def _http_status(error: BaseException) -> int | None:
    """The HTTP status of a failed call, when it was one."""
    status = getattr(error, "status", None)
    return status if isinstance(status, int) else None


def _numbers(
    record: dict[str, Any], *, skip: tuple[str, ...] = ("index",)
) -> dict[str, FieldValue]:
    """A state record's scalar values as fields: numbers as floats, flags and states as such."""
    fields: dict[str, FieldValue] = {}
    for key, value in record.items():
        # Quick-veto start and end times are moments, not measurements.
        if key in skip or value is None or key.endswith("_date_time"):
            continue
        if isinstance(value, bool):
            fields[key] = value
        elif isinstance(value, int | float):
            fields[key] = float(value)
        elif isinstance(value, str) and value and len(value) <= 64:
            fields[key] = value
    return fields


class VaillantSource:
    """Polls one myVAILLANT account's systems."""

    def __init__(self, params: VaillantParams, connect: Connect, resolution: Any) -> None:
        self._params = params
        self._connect = connect
        self._hour = resolution
        self._prefix = params.table_prefix

    async def run(self, context: SourceContext) -> None:
        """Log in, then poll state and energy on their schedules until something fails.

        A quota or server error, at login or later, is waited out and the session kept or opened
        again, so a source that is refused does not spend more of the quota restarting.
        """
        path = context.state_dir / STATE_FILE
        cursors = _load(path)
        context.restore_progress([Progress(d, c) for d, c in cursors.items()])
        schedule = {"state": context.clock(), "energy": context.clock() + ENERGY_DELAY}
        while True:
            try:
                async with self._connect() as api:
                    await self._poll_forever(context, api, cursors, path, schedule)
            except Exception as error:
                if not _transient(error):
                    raise
                await self._pause(context, error)

    async def _poll_forever(
        self,
        context: SourceContext,
        api: Api,
        cursors: dict[str, int],
        path: Path,
        schedule: dict[str, float],
    ) -> None:
        """Poll whatever is due, then wait for the next thing that will be."""
        while True:
            now = context.clock()
            try:
                await self._refresh(api, now)
                if now >= schedule["state"]:
                    await self.poll_state(context, api)
                    schedule["state"] = now + self._params.state_minutes * 60
                if now >= schedule["energy"]:
                    await self.poll_energy(context, api, cursors, path)
                    schedule["energy"] = now + self._params.energy_minutes * 60
            except Exception as error:
                if not _transient(error):
                    raise
                await self._pause(context, error)
                continue
            context.succeeded()
            await context.sleep(max(1.0, min(schedule.values()) - context.clock()))

    async def _pause(self, context: SourceContext, error: BaseException) -> None:
        """Wait out a quota or server error: as long as Vaillant says, or ``pause_minutes``."""
        seconds = _pause_seconds(error, self._params.pause_minutes * 60)
        message = f"Vaillant answered HTTP {_http_status(error)}; pausing {seconds:.0f} s"
        context.problem(message)
        logger.warning(message)
        await context.sleep(seconds)

    async def _refresh(self, api: Api, now: float) -> None:
        """Renew the session when it is about to expire."""
        expires = api.oauth_session_expires
        if expires is None:
            return
        moment = dt.datetime.fromtimestamp(now, dt.UTC)
        if expires.astimezone(dt.UTC) - moment <= REFRESH_MARGIN:
            await api.refresh_token()

    async def poll_state(self, context: SourceContext, api: Api) -> None:
        """Read every system's live state and current power, stamped now."""
        stamp = int(context.clock())
        points: list[Point] = []
        async for system in api.get_systems(include_connection_status=True, include_mpc=True):
            types = {getattr(d, "device_uuid", None): _slug(d.device_type) for d in system.devices}
            for entry in (system.mpc or {}).get("devices", []):
                power = entry.get("current_power")
                device = types.get(entry.get("device_id"))
                if device and isinstance(power, int | float):
                    points.append(
                        self._point(
                            "power", {"device": device}, stamp, {"current_power": float(power)}
                        )
                    )
            state = system.state or {}
            if fields := _numbers(state.get("system") or {}):
                points.append(self._point("system", {}, stamp, fields))
            for group, table in (("circuits", "circuit"), ("zones", "zone"), ("dhw", "dhw")):
                for record in state.get(group) or []:
                    if fields := _numbers(record):
                        tags = {table: str(record.get("index", 0))}
                        points.append(self._point(table, tags, stamp, fields))
        await context.emit(points)

    async def poll_energy(
        self, context: SourceContext, api: Api, cursors: dict[str, int], path: Path
    ) -> None:
        """Read each device's hourly buckets from its cursor (less the rescan) up to now."""
        params = self._params
        now = dt.datetime.fromtimestamp(context.clock(), dt.UTC)
        async for system in api.get_systems():
            for device in system.devices:
                name = _slug(device.device_type)
                cursor = cursors.get(name)
                if cursor is None:
                    first = getattr(device, "first_data", None) or now - dt.timedelta(days=7)
                    start = first.astimezone(dt.UTC)
                else:
                    start = dt.datetime.fromtimestamp(cursor / 1000, dt.UTC) - dt.timedelta(
                        hours=params.rescan_hours
                    )
                start = start.replace(minute=0, second=0, microsecond=0)
                product = str(getattr(device, "product_name", "") or "")
                points = []
                async for series in self._series(context, api, device, start, now):
                    tags = {
                        "device": name,
                        "operation_mode": _slug(series.operation_mode),
                        "energy_type": _slug(series.energy_type or series.value_type),
                    }
                    for bucket in series.data:
                        if bucket.value is None:
                            continue
                        fields: dict[str, FieldValue] = {"wh": float(bucket.value)}
                        if product:
                            fields["product"] = product
                        moment = int(bucket.start_date.astimezone(dt.UTC).timestamp())
                        points.append(self._point("energy", tags, moment, fields))
                complete_dt = now - dt.timedelta(hours=params.lag_hours)
                complete = int(complete_dt.timestamp() * 1000)
                if cursor is not None:
                    complete = max(complete, cursor)
                if await context.emit(points, [Progress(name, complete, product or None)]):
                    cursors[name] = complete
                    _save(path, cursors)
                else:
                    context.problem(f"{name}: a batch was refused; its cursor stays put")

    async def _series(
        self, context: SourceContext, api: Api, device: Any, start: dt.datetime, end: dt.datetime
    ) -> AsyncIterator[Any]:
        """A device's bucket series one request at a time, spaced out to respect the quota."""
        wanted = list(getattr(device, "data", None) or [])
        for n, entry in enumerate(wanted or [None]):
            if n:
                await context.sleep(SERIES_SPACING)
            single = copy.copy(device)
            if entry is not None:
                single.data = [entry]
            async for series in api.get_data_by_device(single, self._hour, start, end):
                yield series

    def _point(
        self, table: str, tags: dict[str, str], stamp: int, fields: dict[str, FieldValue]
    ) -> Point:
        """A point in one of the source's tables, at second precision."""
        return Point(
            self._prefix + table,
            tuple(tags.items()),
            stamp,
            fields,
            precision=Precision.SECONDS,
        )


def _load(path: Path) -> dict[str, int]:
    """The saved cursors, or none when there are none or they cannot be read."""
    try:
        return {str(k): int(v) for k, v in json.loads(path.read_text(encoding="utf-8")).items()}
    except FileNotFoundError:
        return {}
    except (OSError, ValueError, TypeError, AttributeError) as error:
        logger.error("cannot read %s (%s); starting without cursors", path, error)
        return {}


def _save(path: Path, cursors: dict[str, int]) -> None:
    """Write the cursors atomically."""
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(cursors, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def build(params: VaillantParams, _client: httpx2.AsyncClient, _name: str = "") -> VaillantSource:
    """A Vaillant source from its validated parameters, with its credentials read now."""
    from myPyllant.api import MyPyllantAPI
    from myPyllant.enums import DeviceDataBucketResolution

    username = read_secret(params.username_file)
    password = read_secret(params.password_file)

    def connect() -> AbstractAsyncContextManager[Api]:
        return MyPyllantAPI(username, password, params.brand, params.country)

    return VaillantSource(params, connect, DeviceDataBucketResolution.HOUR)


__all__ = ["VaillantParams", "VaillantSource", "build"]

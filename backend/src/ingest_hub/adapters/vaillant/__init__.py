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
  cylinders (``<prefix>system``, ``…circuit``, ``…zone``, ``…dhw``), stamped when read. The
  energy poll reuses the devices this poll found rather than asking for them again.

Vaillant counts requests per clock hour and refuses the rest of the hour once its quota is spent.
A state poll takes about four requests and an energy poll one per series (ten for a heat pump with
a backup heater). The source keeps its own count against ``requests_per_hour``, kept across
restarts: a poll that would not fit in what is left of the hour waits for the next one, and a due
energy poll holds back state polls until it has run, so it is never starved. Each poll logs how
many requests it took.

A refused call (HTTP 429, or 403 naming the quota) or a server error, at login or later, is waited
out for as long as Vaillant says or at most ``pause_minutes``, rather than retried at once; only a
server error is reported as a problem. Bucket series are requested one at a time, a moment apart.
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
#: Requests spent in the current clock hour, so a restart does not start the count again.
BUDGET_FILE = "vaillant-budget.json"
#: Seconds after the top of the hour that a poll held back for the next hour waits, so it does not
#: land before Vaillant's own count starts again.
HOUR_MARGIN = 30.0
#: What a state poll is assumed to cost until one has been counted.
STATE_COST = 4
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
    state_minutes: float = Field(20, ge=1)
    #: Requests the source allows itself per clock hour, a little under Vaillant's quota.
    requests_per_hour: int = Field(18, ge=1)
    lag_hours: float = Field(3, ge=0)
    rescan_hours: float = Field(48, ge=0)
    #: The longest a quota refusal is waited out; Vaillant's quota is counted per hour.
    pause_minutes: float = Field(60, gt=0)


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


class Budget:
    """Requests spent in the current clock hour, against an hourly allowance."""

    def __init__(self, allowance: int, path: Path) -> None:
        self.allowance = allowance
        self._path = path
        self.hour, self.spent = _load_budget(path)

    def _roll(self, now: float) -> None:
        """Start the count again when a new clock hour has begun."""
        hour = int(now // 3600)
        if hour != self.hour:
            self.hour, self.spent = hour, 0

    def fits(self, now: float, cost: int) -> bool:
        """Whether a poll of ``cost`` requests fits in what is left of this hour. An hour with
        nothing spent takes any poll, so one larger than the allowance still runs."""
        self._roll(now)
        return self.spent == 0 or self.spent + cost <= self.allowance

    def used(self, now: float) -> int:
        """Requests counted in this hour so far."""
        self._roll(now)
        return self.spent

    def charge(self, now: float, requests: int) -> None:
        """Count requests made in this hour."""
        self._roll(now)
        self.spent += requests
        self._save()

    def exhaust(self, now: float) -> None:
        """Treat this hour as spent, after Vaillant refused a call for the quota."""
        self._roll(now)
        self.spent = max(self.spent, self.allowance)
        self._save()

    @staticmethod
    def next_hour(now: float) -> float:
        """When the next clock hour's count starts, with a margin."""
        return (int(now // 3600) + 1) * 3600 + HOUR_MARGIN

    def _save(self) -> None:
        """Keep the count across restarts."""
        _write_json(self._path, {"hour": self.hour, "spent": self.spent})


def _load_budget(path: Path) -> tuple[int | None, int]:
    """The saved hour and its count, or none when there is none or it cannot be read."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return int(data["hour"]), int(data["spent"])
    except FileNotFoundError:
        return None, 0
    except (OSError, ValueError, TypeError, KeyError) as error:
        logger.error("cannot read %s (%s); counting requests from zero", path, error)
        return None, 0


class VaillantSource:
    """Polls one myVAILLANT account's systems."""

    def __init__(self, params: VaillantParams, connect: Connect, resolution: Any) -> None:
        self._params = params
        self._connect = connect
        self._hour = resolution
        self._prefix = params.table_prefix
        #: The systems the latest state poll read, whose devices the energy poll reuses.
        self._systems: list[Any] = []
        #: What each kind of poll cost when last counted.
        self._costs: dict[str, int] = {}
        #: The current session's request count when the budget was last charged.
        self._seen = 0

    async def run(self, context: SourceContext) -> None:
        """Log in, then poll state and energy on their schedules until something fails.

        A quota or server error, at login or later, is waited out and the session kept or opened
        again, so a source that is refused does not spend more of the quota restarting.
        """
        path = context.state_dir / STATE_FILE
        cursors = _load(path)
        context.restore_progress([Progress(d, c) for d, c in cursors.items()])
        budget = Budget(self._params.requests_per_hour, context.state_dir / BUDGET_FILE)
        schedule = {"state": context.clock(), "energy": context.clock() + ENERGY_DELAY}
        while True:
            try:
                self._seen = 0
                async with self._connect() as api:
                    await self._poll_forever(context, api, cursors, path, schedule, budget)
            except Exception as error:
                if not _transient(error):
                    raise
                await self._pause(context, error, budget)

    async def _poll_forever(
        self,
        context: SourceContext,
        api: Api,
        cursors: dict[str, int],
        path: Path,
        schedule: dict[str, float],
        budget: Budget,
    ) -> None:
        """Poll whatever is due and fits in this hour's budget, then wait for the next thing that
        will be. Energy goes first; when it does not fit, state waits with it."""
        minutes = {"state": self._params.state_minutes, "energy": self._params.energy_minutes}
        while True:
            now = context.clock()
            try:
                # Charges the login, and any session refresh, as well as the polls.
                self._charge(api, budget, now, None)
                await self._refresh(api, now)
                for kind in ("energy", "state"):
                    if now < schedule[kind]:
                        continue
                    cost = self._estimate(kind)
                    if not budget.fits(now, cost):
                        later = budget.next_hour(now)
                        held = [kind, "state"] if kind == "energy" else [kind]
                        for name in held:
                            schedule[name] = max(schedule[name], later)
                        logger.info(
                            "Vaillant %s poll held until %s: %d of %d requests used this hour",
                            kind,
                            dt.datetime.fromtimestamp(later, dt.UTC).strftime("%H:%M:%S UTC"),
                            budget.spent,
                            budget.allowance,
                        )
                        if kind == "energy":
                            break
                        continue
                    if kind == "state":
                        await self.poll_state(context, api)
                    else:
                        await self.poll_energy(context, api, cursors, path)
                    self._charge(api, budget, context.clock(), (kind, cost))
                    schedule[kind] = now + minutes[kind] * 60
            except Exception as error:
                if not _transient(error):
                    raise
                await self._pause(context, error, budget)
                continue
            context.succeeded()
            await context.sleep(max(1.0, min(schedule.values()) - context.clock()))

    def _estimate(self, kind: str) -> int:
        """What a poll is expected to cost: what it cost when last counted, else a guess from
        the devices known (one request per energy series, plus finding the devices)."""
        if kind in self._costs:
            return self._costs[kind]
        if kind == "state":
            return STATE_COST
        series = sum(max(1, len(getattr(d, "data", None) or [])) for d in self._devices())
        return series + (0 if self._systems else STATE_COST)

    def _devices(self) -> list[Any]:
        """Every device of the systems the latest state poll found."""
        return [device for system in self._systems for device in system.devices]

    def _charge(self, api: Api, budget: Budget, now: float, poll: tuple[str, int] | None) -> None:
        """Charge the budget with the requests made since it was last charged.

        With a client that counts its requests, that is the count since then, which for a poll
        is also remembered as its cost. With one that does not, a poll is charged its estimate.
        """
        count = _requests(api)
        if count is not None:
            spent, self._seen = count - self._seen, count
            if poll is not None:
                self._costs[poll[0]] = spent
        else:
            spent = poll[1] if poll is not None else 0
        if spent:
            budget.charge(now, spent)

    async def _pause(self, context: SourceContext, error: BaseException, budget: Budget) -> None:
        """Wait out a quota or server error: as long as Vaillant says, or ``pause_minutes``.

        A quota refusal is logged, with the requests counted this hour and Vaillant's own words,
        but not reported as a problem, since the wait ends with the hour; a refusal that keeps
        coming still shows, as writes and progress that stop. A server error is reported.
        """
        status = _http_status(error)
        seconds = _pause_seconds(error, self._params.pause_minutes * 60)
        message = f"Vaillant answered HTTP {status}; pausing {seconds:.0f} s"
        if status in {403, 429}:
            counted = budget.used(context.clock())
            message += f" after {counted} request(s) counted this hour: {str(error)[:300]}"
            budget.exhaust(context.clock())
        else:
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
        before = _requests(api)
        systems = [s async for s in api.get_systems(include_mpc=True)]
        self._systems = systems
        for system in systems:
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
        _log_requests("state", api, before)
        await context.emit(points)

    async def poll_energy(
        self, context: SourceContext, api: Api, cursors: dict[str, int], path: Path
    ) -> None:
        """Read each device's hourly buckets from its cursor (less the rescan) up to now."""
        params = self._params
        now = dt.datetime.fromtimestamp(context.clock(), dt.UTC)
        before = _requests(api)
        systems = self._systems or [s async for s in api.get_systems()]
        for system in systems:
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
        _log_requests("energy", api, before)

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


def _requests(api: Api) -> int | None:
    """How many requests the API client has made, when it counts them (myPyllant's does)."""
    count = getattr(getattr(api, "aiohttp_session", None), "request_count", None)
    return count if isinstance(count, int) else None


def _log_requests(poll: str, api: Api, before: int | None) -> None:
    """Log how many requests a poll took, which is what Vaillant's quota counts."""
    after = _requests(api)
    if before is not None and after is not None:
        logger.info("Vaillant %s poll: %d request(s)", poll, after - before)


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
    _write_json(path, cursors)


def _write_json(path: Path, data: dict[str, Any]) -> None:
    """Write a small JSON document atomically."""
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
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

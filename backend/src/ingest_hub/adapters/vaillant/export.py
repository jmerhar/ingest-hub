"""Vaillant's EU Data Act data exports, imported from a folder they are dropped into.

The export a myVAILLANT user downloads from Vaillant's EU Data Act portal is one JSON array of
messages, each ``{"topic", "function", "timestamp", "message_value"}`` with the message itself
as a JSON string. It carries far more detail than the myVAILLANT app's API, and five kinds of
message are imported, each at the message's timestamp:

- the controller's state (``system-control.state``, every few minutes): the same values the
  ``vaillant`` source polls, into its tables and columns: ``<prefix>system``, and
  ``<prefix>zone``, ``<prefix>circuit`` and ``<prefix>dhw`` tagged ``zone``, ``circuit`` and
  ``dhw`` with their index. Names are the export's in snake case, as the app's API gives them,
  except the hot-water temperature (``currentCircuitTemperature``), written as
  ``current_dhw_temperature`` as the API names it; quick-veto times are left out.

The other four are tagged ``device``:

- live power (``gateway.hem.message.received`` / ``status_message_mpc``, every minute or two
  while it changes, hourly otherwise):
  ``<prefix>power``, field ``current_power`` in W, the table and field the ``vaillant`` source
  writes its own readings to;
- interpreted eBUS readings (``ebus.responses.interpreted``, every few minutes): ``<prefix>ebus``,
  one field per data point, its name in snake case (``CompressorFrequencySignal`` becomes
  ``compressor_frequency_signal``, a list entry ``Value0`` of ``FaultList`` becomes
  ``fault_list_value0``); numbers as floats, ``on``/``true`` and ``off``/``false`` as booleans,
  other values as strings, and ``NO_VALUE`` left out;
- operating statistics (``end-user-system-realtime-stats.inbound``, hourly): ``<prefix>stats``,
  tag ``operation_mode``, fields ``operating_minutes`` and ``on_off_cycles``, both running totals;
- energy counters (``end-user-system-emf.inbound``, hourly): ``<prefix>energy_total``, tags
  ``operation_mode`` and ``energy_type``, field ``wh``, the running total as Vaillant reports it.

A device is named by its type in the export's topology (``HEAT_PUMP`` becomes ``heat_pump``), or
``ebus_<address>`` when no topology has listed it; ``devices`` renames either, so the names can
match another source's. Vaillant sends the topology when the gateway connects, so most exports do
not carry it: the names it gives are kept in the source's state directory and used for every
later export. Every scan imports each ``*.json`` file in ``inbox`` and moves it to
``imported/`` beside it once its points are safe, or to ``failed/`` when it cannot be read.
Importing the same messages again writes the same points, so overlapping exports are harmless.
No progress is published: exports arrive whenever someone drops one.

With ``fetch`` set, the source also fetches exports from the portal itself (see ``portal.py``),
with a portal account of its own. The portal gives the previous day (by UTC), so the source
fetches while the newest day it has imported completely (see :func:`complete_day`), from a fetch
or a dropped file, is older than yesterday: from ``not_before`` (noon, in ``timezone``) each day,
at most every ``retry_minutes``. It saves the export in the inbox as
``euda-data-<UTC time>.json``, readable by its owner only, where the scan imports it. A failed
fetch is reported and keeps the round from counting as a success, so it raises the source's
alerts while it lasts.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx2
from jigkit.sinks import FieldValue, Point, Precision
from pydantic import BaseModel, ConfigDict, Field, field_validator

from ingest_hub.adapters.inbox import FAILED, IMPORTED, move_aside, wait_or_wake, write_json
from ingest_hub.adapters.vaillant.portal import PORTAL_URL, Portal, PortalError
from ingest_hub.collect import SourceContext
from ingest_hub.config import read_secret

logger = logging.getLogger(__name__)

#: Points per batch handed to delivery.
BATCH = 5000
TOPOLOGY = "hvac-topology"
#: The device names learnt from topologies, in the source's state directory.
NAMES_FILE = "vaillant-export-devices.json"
#: The newest day any imported export held completely, in the source's state directory.
FETCH_FILE = "vaillant-export-fetch.json"
#: The topics whose messages from a day's last hour mark the day as complete in an export.
COMPLETE_TOPICS = frozenset({"ebus.responses.interpreted", "system-control.state"})
#: eBUS values that mean "no reading", left out rather than stored.
NO_VALUE = frozenset({"", "NO_VALUE"})
TRUE = frozenset({"on", "true"})
FALSE = frozenset({"off", "false"})
NUMBER = re.compile(r"^-?\d+(\.\d+)?$")
#: The controller state's sections: (key in ``data``, table, index tag); the system has no index.
STATE_SECTIONS = (
    ("zoneSettings", "zone", "zone"),
    ("circuitSettings", "circuit", "circuit"),
    ("domesticHotWaterSettings", "dhw", "dhw"),
)
#: Controller state names the export spells differently from the app's API, by table.
STATE_RENAMES = {"dhw": {"current_circuit_temperature": "current_dhw_temperature"}}
#: A word boundary inside a camelCase name: ``flowTemperature``, ``BuildPumpPWM``.
CAMEL = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


class ExportError(Exception):
    """A file is not an export that can be read; the message says why."""


class FetchParams(BaseModel):
    """Fetching exports from the portal: a portal account and how often to try."""

    model_config = ConfigDict(extra="forbid")

    username_file: Path
    password_file: Path
    #: The account's myVAILLANT realm, by country.
    realm: str = "vaillant-netherlands-b2c"
    url: str = PORTAL_URL
    #: The least time between two fetches while yesterday is missing.
    retry_minutes: float = Field(60, gt=0)
    #: The time of day, in ``timezone``, before which no fetch is tried: the portal adds a
    #: day's messages through the morning after it.
    not_before: dt.time = dt.time(12, 0)
    timezone: str = "UTC"

    @field_validator("timezone")
    @classmethod
    def _known_zone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ValueError(f"unknown time zone {value!r}") from error
        return value


class VaillantExportParams(BaseModel):
    """A Vaillant export source's ``params`` in the configuration file."""

    model_config = ConfigDict(extra="forbid")

    #: The folder exports are dropped into; created when missing.
    inbox: Path
    table_prefix: str = Field("vaillant_", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    scan_minutes: float = Field(10, gt=0)
    #: Device names to write instead of the ones the export gives, by the export's name.
    devices: dict[str, str] = {}
    #: Fetch exports from the portal as well as importing dropped ones.
    fetch: FetchParams | None = None


def _snake(name: str) -> str:
    """A camelCase name in snake case: ``FanUnitAirInletTemperature`` →
    ``fan_unit_air_inlet_temperature``."""
    return CAMEL.sub("_", name).lower()


def _value(raw: object) -> FieldValue | None:
    """An eBUS value as a field value, or None when it is no reading."""
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int | float):
        return float(raw)
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if text in NO_VALUE:
        return None
    if NUMBER.match(text):
        return float(text)
    if text.lower() in TRUE:
        return True
    if text.lower() in FALSE:
        return False
    return text if len(text) <= 64 else None


def _ms(timestamp: str) -> int:
    """An export timestamp (ISO 8601 with an offset) in Unix milliseconds."""
    moment = dt.datetime.fromisoformat(timestamp)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=dt.UTC)
    return int(moment.timestamp() * 1000)


def _state_fields(state: object, renames: dict[str, str]) -> dict[str, FieldValue]:
    """A controller state section's values as fields. Each value is ``{"value", "metadata",
    "lastUpdated"}``; one without a value is left out, as are quick-veto times."""
    fields: dict[str, FieldValue] = {}
    if not isinstance(state, dict):
        return fields
    for key, entry in state.items():
        raw = entry.get("value") if isinstance(entry, dict) else entry
        name = _snake(key)
        if name.endswith(("_date_time", "_timestamp")) or isinstance(raw, dict | list):
            continue
        value = _value(raw)
        if value is not None:
            fields[renames.get(name, name)] = value
    return fields


def read_entries(path: Path) -> list[Any]:
    """The entries of an export file, their messages still unparsed; raises
    :class:`ExportError` when the file is not a JSON array."""
    try:
        entries = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as error:
        raise ExportError(f"{path.name}: {error}") from error
    if not isinstance(entries, list):
        raise ExportError(f"{path.name}: not a JSON array of messages")
    return entries


def parse_entry(entry: Any) -> tuple[str, str | None, int, dict[str, Any]] | None:
    """One entry as (topic, function, ms, message), or None when it cannot be read."""
    try:
        raw = entry["message_value"]
        message = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(message, dict):
            return None
        return str(entry["topic"]), entry.get("function"), _ms(entry["timestamp"]), message
    except KeyError, TypeError, ValueError:
        return None


class Points:
    """Turns an export's messages into points."""

    def __init__(
        self, params: VaillantExportParams, names: dict[tuple[int, int], str] | None = None
    ) -> None:
        self._prefix = params.table_prefix
        self._renames = params.devices
        #: Device names by (bus coupler address, eBUS address), from topologies; a topology in
        #: the export adds to them in place.
        self._names = {} if names is None else names
        self.counts: Counter[str] = Counter()

    def learn_topology(self, message: dict[str, Any]) -> None:
        """Name devices after their type in a topology message."""
        for device in message.get("devices") or []:
            try:
                address = (int(device.get("busCouplerAddress") or 0), int(device["eBusAddress"]))
            except KeyError, TypeError, ValueError:
                continue
            if kind := device.get("deviceType"):
                self._names[address] = str(kind).strip().lower()

    def _device(self, coupler: object, address: object) -> str:
        """A device's name from its addresses."""
        try:
            key = (int(coupler or 0), int(address))  # type: ignore[call-overload]
        except TypeError, ValueError:
            name = "unknown"
        else:
            name = self._names.get(key, f"ebus_{key[1]}")
        return self._renames.get(name, name)

    def _point(
        self, table: str, tags: dict[str, str], ms: int, fields: dict[str, FieldValue]
    ) -> Point:
        self.counts[table] += 1
        return Point(
            self._prefix + table,
            tuple(tags.items()),
            ms,
            fields,
            precision=Precision.MILLISECONDS,
        )

    def build(self, topic: str, function: str | None, ms: int, message: dict) -> Iterable[Point]:
        """The points one message carries; none for kinds that are not imported."""
        if topic == "gateway.hem.message.received" and function == "status_message_mpc":
            yield from self._power(ms, message)
        elif topic == "ebus.responses.interpreted":
            yield from self._ebus(ms, message)
        elif topic == "end-user-system-realtime-stats.inbound":
            yield from self._stats(ms, message)
        elif topic == "end-user-system-emf.inbound":
            yield from self._energy(ms, message)
        elif topic == "system-control.state":
            yield from self._state(ms, message)

    def _state(self, ms: int, message: dict) -> Iterable[Point]:
        data = message.get("data") or {}
        system = (data.get("systemParameters") or {}).get("state")
        if fields := _state_fields(system, {}):
            yield self._point("system", {}, ms, fields)
        for key, table, tag in STATE_SECTIONS:
            section = data.get(key)
            if not isinstance(section, dict):
                continue
            for index, settings in section.items():
                state = (settings or {}).get("state") if isinstance(settings, dict) else None
                if fields := _state_fields(state, STATE_RENAMES.get(table, {})):
                    yield self._point(table, {tag: str(index)}, ms, fields)

    def _power(self, ms: int, message: dict) -> Iterable[Point]:
        for status in message.get("mpc_device_status") or []:
            power = status.get("current_power")
            if isinstance(power, int | float) and not isinstance(power, bool):
                device = self._device(status.get("bus_coupler_address"), status.get("ebus_address"))
                yield self._point("power", {"device": device}, ms, {"current_power": float(power)})

    def _ebus(self, ms: int, message: dict) -> Iterable[Point]:
        device_info = (message.get("eventPayload") or {}).get("device") or {}
        fields: dict[str, FieldValue] = {}
        for point in message.get("dataPoint") or []:
            name = point.get("name")
            if not name or point.get("disabled"):
                continue
            for entry in point.get("valueUnitMap") or []:
                part = entry.get("name")
                field = _snake(name if not part or part == name else f"{name}_{part}")
                value = _value(entry.get("value"))
                if value is not None:
                    fields[field] = value
        if fields:
            device = self._device(
                device_info.get("busCouplerAddress"), device_info.get("eBusAddress")
            )
            yield self._point("ebus", {"device": device}, ms, fields)

    def _stats(self, ms: int, message: dict) -> Iterable[Point]:
        payload = message.get("eventPayload") or {}
        device_info = payload.get("device") or {}
        stats = payload.get("payload") or {}
        fields: dict[str, FieldValue] = {}
        for key, field in (
            ("operatingTime", "operating_minutes"),
            ("onOffCycles", "on_off_cycles"),
        ):
            value = stats.get(key)
            if isinstance(value, int | float) and not isinstance(value, bool):
                fields[field] = float(value)
        if fields:
            tags = {
                "device": self._device(
                    device_info.get("busCouplerAddress"), device_info.get("eBusAddress")
                ),
                "operation_mode": str(stats.get("operationMode") or "unknown").lower(),
            }
            yield self._point("stats", tags, ms, fields)

    def _energy(self, ms: int, message: dict) -> Iterable[Point]:
        payload = message.get("eventPayload") or {}
        value = payload.get("energyValue")
        if isinstance(value, int | float) and not isinstance(value, bool):
            tags = {
                "device": self._device(
                    payload.get("busCouplerAddress"), payload.get("ebusAddress")
                ),
                "operation_mode": str(payload.get("operationMode") or "unknown").lower(),
                "energy_type": str(payload.get("energyType") or "unknown").lower(),
            }
            yield self._point("energy_total", tags, ms, {"wh": float(value)})


@dataclass
class Export:
    """What an export file carries: its points, how many went to each table, and the newest day
    it holds completely (see :func:`complete_day`)."""

    points: list[Point]
    counts: Counter[str]
    complete_day: dt.date | None


def export_points(
    path: Path, params: VaillantExportParams, names: dict[tuple[int, int], str] | None = None
) -> tuple[list[Point], Counter[str]]:
    """Every point an export file carries, and how many went to each table."""
    export = read_export(path, params, names)
    return export.points, export.counts


def read_export(
    path: Path, params: VaillantExportParams, names: dict[tuple[int, int], str] | None = None
) -> Export:
    """Everything an export file carries.

    ``names`` are device names already learnt, by address; any the export's topology gives are
    added to it in place.
    """
    entries = read_entries(path)
    builder = Points(params, names)
    # Devices are named from the topology before any message is turned into points, wherever in
    # the file it comes. Messages are parsed one at a time, so an export of many days needs
    # little more memory than its text.
    for entry in entries:
        topology = isinstance(entry, dict) and entry.get("topic") == TOPOLOGY
        if topology and (parsed := parse_entry(entry)):
            builder.learn_topology(parsed[3])
    points: list[Point] = []
    marks: list[tuple[str, int]] = []
    skipped = 0
    for entry in entries:
        parsed = parse_entry(entry)
        if parsed is None:
            skipped += 1
        else:
            points.extend(builder.build(*parsed))
            if parsed[0] in COMPLETE_TOPICS:
                marks.append((parsed[0], parsed[2]))
    if skipped:
        logger.warning("%s: skipped %d unreadable message(s)", path.name, skipped)
    return Export(points, builder.counts, complete_day(marks))


class VaillantExportSource:
    """Imports the exports dropped into a folder, or handed to it through an upload."""

    file_suffixes = (".json",)

    def __init__(self, params: VaillantExportParams, portal: Portal | None = None) -> None:
        self._params = params
        self.inbox = params.inbox
        self._woken = asyncio.Event()
        self._portal = portal
        #: The newest day an imported export held; read from the state directory when needed.
        self._newest: dt.date | None = None
        self._newest_loaded = False
        #: When a fetch may next be tried.
        self._next_try = float("-inf")

    def wake(self) -> None:
        """Scan the inbox now rather than at the next scan, for a file just saved there."""
        self._woken.set()

    async def run(self, context: SourceContext) -> None:
        """Scan the inbox every ``scan_minutes`` and whenever woken, fetching after the scan when
        yesterday is still missing, for ever."""
        self.inbox.mkdir(parents=True, exist_ok=True)
        while True:
            self._woken.clear()
            # A dropped file of yesterday is imported first, so it saves a fetch.
            await self.scan(context)
            fetched = await self.fetch(context)
            if fetched is not None:
                await self.scan(context)
            if fetched is not False:
                context.succeeded()
            wait = self._params.scan_minutes * 60
            if self._wants_fetch(context):
                wait = min(wait, max(1.0, self._next_try - context.clock()))
            await wait_or_wake(context, self._woken, wait)

    def _newest_day(self, context: SourceContext) -> dt.date | None:
        """The newest day an imported export held, read once from the state directory, or
        from the newest imported file when the state has none."""
        if not self._newest_loaded:
            self._newest_loaded = True
            self._newest = _load_newest(context.state_dir / FETCH_FILE)
            if self._newest is None:
                self._newest = _newest_imported(self.inbox / IMPORTED)
                if self._newest is not None:
                    _save_newest(context.state_dir / FETCH_FILE, self._newest)
        return self._newest

    def _note_day(self, context: SourceContext, day: dt.date | None) -> None:
        """Remember an imported export's newest day, when it is newer than any before."""
        newest = self._newest_day(context)
        if day is not None and (newest is None or day > newest):
            self._newest = day
            _save_newest(context.state_dir / FETCH_FILE, day)

    def _wants_fetch(self, context: SourceContext) -> bool:
        """Whether the portal should be asked: fetching is set up and yesterday is missing."""
        if self._portal is None or self._params.fetch is None:
            return False
        yesterday = dt.datetime.fromtimestamp(context.clock(), dt.UTC).date() - dt.timedelta(days=1)
        newest = self._newest_day(context)
        return newest is None or newest < yesterday

    async def fetch(self, context: SourceContext) -> bool | None:
        """Fetch an export from the portal when yesterday is missing and a try is due: True when
        one was saved in the inbox, False when the fetch failed, None when none was due."""
        if not self._wants_fetch(context) or context.clock() < self._next_try:
            return None
        assert self._portal is not None and self._params.fetch is not None
        now = context.clock()
        zone = ZoneInfo(self._params.fetch.timezone)
        local = dt.datetime.fromtimestamp(now, zone)
        if local.time() < self._params.fetch.not_before:
            self._next_try = dt.datetime.combine(
                local.date(), self._params.fetch.not_before, zone
            ).timestamp()
            return None
        self._next_try = now + self._params.fetch.retry_minutes * 60
        try:
            export = await self._portal.fetch()
        except (PortalError, httpx2.HTTPError) as error:
            message = f"fetching from the portal failed: {str(error) or type(error).__name__}"
            context.problem(message)
            logger.warning("%s", message)
            return False
        stamp = dt.datetime.fromtimestamp(now, dt.UTC).strftime("%Y-%m-%d_%H-%M-%S")
        _write_private(self.inbox / f"euda-data-{stamp}.json", export)
        logger.info("fetched an export from the portal: %d bytes", len(export))
        return True

    async def scan(self, context: SourceContext) -> None:
        """Import every export waiting in the inbox, oldest name first."""
        inbox = self._params.inbox
        names_path = context.state_dir / NAMES_FILE
        for path in sorted(inbox.glob("*.json")):
            names = _load_names(names_path)
            known = dict(names)
            try:
                export = read_export(path, self._params, names)
                points, counts = export.points, export.counts
            except ExportError as error:
                context.problem(f"cannot read export {error}; moved to {FAILED}/")
                logger.error("cannot read export %s; moving it to %s/", error, FAILED)
                move_aside(path, FAILED)
                continue
            for start in range(0, len(points), BATCH):
                if not await context.emit(points[start : start + BATCH]):
                    context.problem(
                        f"{path.name}: a batch was refused; the file stays in the inbox"
                    )
                    return
            if names != known:
                _save_names(names_path, names)
            move_aside(path, IMPORTED)
            self._note_day(context, export.complete_day)
            summary = ", ".join(f"{n} {table}" for table, n in sorted(counts.items())) or "nothing"
            logger.info("imported %s: %s", path.name, summary)


def _load_names(path: Path) -> dict[tuple[int, int], str]:
    """The device names learnt from earlier topologies, or none when they cannot be read."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        names = {}
        for key, name in data.items():
            coupler, address = key.split(":")
            names[(int(coupler), int(address))] = str(name)
        return names
    except FileNotFoundError:
        return {}
    except (OSError, ValueError, AttributeError) as error:
        logger.error("cannot read %s (%s); naming devices by address", path, error)
        return {}


def _save_names(path: Path, names: dict[tuple[int, int], str]) -> None:
    """Keep the device names for later exports, written atomically."""
    data = {f"{coupler}:{address}": name for (coupler, address), name in sorted(names.items())}
    write_json(path, data, indent=2)


def _write_private(path: Path, data: bytes) -> None:
    """Write a file readable by its owner only, appearing whole under its name."""
    temporary = path.with_name(f".{path.name}.part")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)
    os.replace(temporary, path)


def _load_newest(path: Path) -> dt.date | None:
    """The newest imported day kept in the state directory, or None."""
    try:
        return dt.date.fromisoformat(json.loads(path.read_text(encoding="utf-8"))["newest_day"])
    except FileNotFoundError:
        return None
    except (OSError, ValueError, KeyError, TypeError) as error:
        logger.error("cannot read %s (%s); looking at the imported files", path, error)
        return None


def _newest_imported(folder: Path) -> dt.date | None:
    """The newest complete day in the most recently imported file that has one, or None."""
    files = (
        sorted(folder.glob("*.json"), key=lambda f: f.stat().st_mtime) if folder.is_dir() else []
    )
    for path in reversed(files):
        try:
            day = complete_day(
                (parsed[0], parsed[2])
                for entry in read_entries(path)
                if (parsed := parse_entry(entry))
            )
        except ExportError:
            continue
        if day:
            return day
    return None


def complete_day(entries: Iterable[tuple[str, int]]) -> dt.date | None:
    """The newest day an export holds completely, from its messages as (topic, milliseconds).

    eBUS readings and the controller's state arrive every few minutes for as long as the system
    is online, and the portal adds a day's messages in stages, so a day counts as complete when
    the export holds both from the day's last hour (UTC). None when no day does."""
    last_hour: dict[dt.date, set[str]] = {}
    for topic, ms in entries:
        if topic in COMPLETE_TOPICS:
            moment = dt.datetime.fromtimestamp(ms / 1000, dt.UTC)
            if moment.hour == 23:
                last_hour.setdefault(moment.date(), set()).add(topic)
    days = [day for day, topics in last_hour.items() if topics >= COMPLETE_TOPICS]
    return max(days) if days else None


def _save_newest(path: Path, day: dt.date) -> None:
    write_json(path, {"newest_day": day.isoformat()})


def build(
    params: VaillantExportParams, _client: httpx2.AsyncClient, _name: str = ""
) -> VaillantExportSource:
    """A Vaillant export source from its validated parameters, with the portal account's
    credentials read now when it fetches."""
    portal = None
    if params.fetch is not None:
        portal = Portal(
            read_secret(params.fetch.username_file),
            read_secret(params.fetch.password_file),
            realm=params.fetch.realm,
            url=params.fetch.url,
        )
    return VaillantExportSource(params, portal)


__all__ = ["VaillantExportParams", "VaillantExportSource", "build", "export_points"]

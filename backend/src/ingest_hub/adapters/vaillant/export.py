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
``ebus_<address>`` when the export does not list it; ``devices`` renames either, so the names can
match another source's. Every scan imports each ``*.json`` file in ``inbox`` and moves it to
``imported/`` beside it once its points are safe, or to ``failed/`` when it cannot be read.
Importing the same messages again writes the same points, so overlapping exports are harmless.
No progress is published: exports arrive whenever someone drops one.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import logging
import os
import re
from collections import Counter
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import httpx2
from jigkit.sinks import FieldValue, Point, Precision
from pydantic import BaseModel, ConfigDict, Field

from ingest_hub.collect import SourceContext

logger = logging.getLogger(__name__)

#: Points per batch handed to delivery.
BATCH = 5000
IMPORTED = "imported"
FAILED = "failed"
TOPOLOGY = "hvac-topology"
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


class VaillantExportParams(BaseModel):
    """A Vaillant export source's ``params`` in the configuration file."""

    model_config = ConfigDict(extra="forbid")

    #: The folder exports are dropped into; created when missing.
    inbox: Path
    table_prefix: str = Field("vaillant_", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    scan_minutes: float = Field(10, gt=0)
    #: Device names to write instead of the ones the export gives, by the export's name.
    devices: dict[str, str] = {}


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

    def __init__(self, params: VaillantExportParams) -> None:
        self._prefix = params.table_prefix
        self._renames = params.devices
        #: Device names by (bus coupler address, eBUS address), from the export's topology.
        self._names: dict[tuple[int, int], str] = {}
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


def export_points(path: Path, params: VaillantExportParams) -> tuple[list[Point], Counter[str]]:
    """Every point an export file carries, and how many went to each table."""
    entries = read_entries(path)
    builder = Points(params)
    # Devices are named from the topology before any message is turned into points, wherever in
    # the file it comes. Messages are parsed one at a time, so an export of many days needs
    # little more memory than its text.
    for entry in entries:
        topology = isinstance(entry, dict) and entry.get("topic") == TOPOLOGY
        if topology and (parsed := parse_entry(entry)):
            builder.learn_topology(parsed[3])
    points: list[Point] = []
    skipped = 0
    for entry in entries:
        parsed = parse_entry(entry)
        if parsed is None:
            skipped += 1
        else:
            points.extend(builder.build(*parsed))
    if skipped:
        logger.warning("%s: skipped %d unreadable message(s)", path.name, skipped)
    return points, builder.counts


class VaillantExportSource:
    """Imports the exports dropped into a folder, or handed to it through an upload."""

    file_suffixes = (".json",)

    def __init__(self, params: VaillantExportParams) -> None:
        self._params = params
        self.inbox = params.inbox
        self._woken = asyncio.Event()

    def wake(self) -> None:
        """Scan the inbox now rather than at the next scan, for a file just saved there."""
        self._woken.set()

    async def run(self, context: SourceContext) -> None:
        """Scan the inbox every ``scan_minutes``, and whenever woken, for ever."""
        self.inbox.mkdir(parents=True, exist_ok=True)
        while True:
            self._woken.clear()
            await self.scan(context)
            context.succeeded()
            await self._wait(context, self._params.scan_minutes * 60)

    async def _wait(self, context: SourceContext, seconds: float) -> None:
        """Wait for the next scan, or until woken."""
        timer = asyncio.ensure_future(context.sleep(seconds))
        woken = asyncio.ensure_future(self._woken.wait())
        done, pending = await asyncio.wait({timer, woken}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for task in done:
            task.result()

    async def scan(self, context: SourceContext) -> None:
        """Import every export waiting in the inbox, oldest name first."""
        inbox = self._params.inbox
        for path in sorted(inbox.glob("*.json")):
            try:
                points, counts = export_points(path, self._params)
            except ExportError as error:
                context.problem(f"cannot read export {error}; moved to {FAILED}/")
                logger.error("cannot read export %s; moving it to %s/", error, FAILED)
                _move(path, FAILED)
                continue
            for start in range(0, len(points), BATCH):
                if not await context.emit(points[start : start + BATCH]):
                    context.problem(
                        f"{path.name}: a batch was refused; the file stays in the inbox"
                    )
                    return
            _move(path, IMPORTED)
            summary = ", ".join(f"{n} {table}" for table, n in sorted(counts.items())) or "nothing"
            logger.info("imported %s: %s", path.name, summary)


def _move(path: Path, folder: str) -> None:
    """Move a file into a folder beside it, replacing an earlier file of the same name."""
    target = path.parent / folder
    target.mkdir(exist_ok=True)
    os.replace(path, target / path.name)


def build(
    params: VaillantExportParams, _client: httpx2.AsyncClient, _name: str = ""
) -> VaillantExportSource:
    """A Vaillant export source from its validated parameters."""
    return VaillantExportSource(params)


__all__ = ["VaillantExportParams", "VaillantExportSource", "build", "export_points"]

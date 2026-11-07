"""Modbus TCP devices, read through a register map.

Every ``interval_seconds`` the source reads the registers its map lists, grouped into as few
requests as the map allows, and writes one point per table, ``<prefix><table>``, stamped with the
start of the round; a table the map gives ``every_seconds`` is read only that often. A repeated
block (one slot per inverter port, say) gives one point per slot, tagged with the slot's tag
fields. ``tags`` adds tags to every point.

It only ever reads (function codes 3 and 4). When a request spanning several registers is
refused with "illegal address", those registers are read one at a time from then on, and a
register the device refuses on its own is left out after one warning, so a map that lists a
register a model lacks costs one warning, not the round. A connection that fails is reported
and opened again at the next round.

Maps come with the adapter (``alphaess``, ``hoymiles-dtu``) or from a file named by its path; see
:mod:`ingest_hub.adapters.modbus.registers` for the format.
"""

from __future__ import annotations

import asyncio
import logging
import struct
from collections.abc import Callable
from typing import Any

import httpx2
from jigkit.sinks import FieldValue, Point, Precision
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ingest_hub.adapters.modbus.client import ILLEGAL_ADDRESS, ModbusClient, ModbusError
from ingest_hub.adapters.modbus.registers import (
    MapError,
    Register,
    RegisterMap,
    Repeat,
    load_map,
    plan,
)
from ingest_hub.collect import SourceContext

logger = logging.getLogger(__name__)

#: Failures that mean the connection, not a register, is the problem.
CONNECTION_ERRORS = (OSError, TimeoutError, asyncio.IncompleteReadError, ConnectionError)


class ModbusParams(BaseModel):
    """A Modbus source's ``params`` in the configuration file."""

    model_config = ConfigDict(extra="forbid")

    host: str
    port: int = Field(502, ge=1, le=65535)
    #: The Modbus unit (slave) id.
    unit: int = Field(1, ge=0, le=255)
    #: A bundled map's name, or the path of a map file.
    map: str
    #: Overrides the map's own prefix for table names.
    table_prefix: str | None = Field(None, pattern=r"^([A-Za-z_][A-Za-z0-9_]*)?$")
    interval_seconds: float = Field(10, gt=0)
    timeout_seconds: float = Field(5, gt=0)
    tags: dict[str, str] = {}

    @model_validator(mode="after")
    def _map_loads(self) -> ModbusParams:
        try:
            load_map(self.map)
        except MapError as error:
            raise ValueError(str(error)) from error
        return self


Client = Callable[[], Any]


class ModbusSource:
    """Polls one Modbus device."""

    def __init__(self, params: ModbusParams, register_map: RegisterMap, client: Client) -> None:
        self._params = params
        self._map = register_map
        self._client = client
        self._prefix = (
            params.table_prefix if params.table_prefix is not None else register_map.table_prefix
        )
        self._tags = tuple(sorted(params.tags.items()))
        #: When each table read less often than every round is next due.
        self._due: dict[str, float] = {}
        #: Registers read one at a time, after a request spanning them was refused.
        self._singles: set[tuple[str, str]] = set()
        #: Registers the device refuses, left out of every round.
        self._absent: set[tuple[str, str]] = set()

    async def run(self, context: SourceContext) -> None:
        """Read a round every ``interval_seconds`` for ever."""
        client = self._client()
        try:
            while True:
                started = context.clock()
                try:
                    points = await self.poll(context, client, started)
                except CONNECTION_ERRORS as error:
                    message = f"cannot read {self._params.host}: {error or type(error).__name__}"
                    context.problem(message)
                    logger.warning(message)
                else:
                    if await context.emit(points):
                        context.succeeded()
                    else:
                        context.problem("a batch was refused")
                elapsed = context.clock() - started
                await context.sleep(max(0.0, self._params.interval_seconds - elapsed))
        finally:
            await client.close()

    async def poll(self, context: SourceContext, client: Any, now: float) -> list[Point]:
        """One round: every due register and repeated block, as points stamped ``now``."""
        due = self._due_tables(now)
        values: dict[str, dict[str, FieldValue]] = {}
        wanted = [
            r
            for r in self._map.registers
            if r.table in due and (r.table, r.name) not in self._absent
        ]
        grouped = [r for r in wanted if (r.table, r.name) not in self._singles]
        singles = [r for r in wanted if (r.table, r.name) in self._singles]
        reads = plan(grouped, self._map.function, self._map.max_read, self._map.max_gap)
        reads += plan(singles, self._map.function, 1, -1) if singles else []
        for read in reads:
            await self._read(
                context, client, read.function, read.address, read.count, read.registers, values
            )
        stamp = int(now)
        points = [
            self._point(table, self._tags, stamp, fields)
            for table, fields in values.items()
            if fields
        ]
        for repeat in self._map.repeats:
            if repeat.table in due:
                points += await self._repeat(context, client, repeat, stamp)
        for table in due:
            if every := self._map.every(table):
                self._due[table] = now + every
        return points

    def _due_tables(self, now: float) -> set[str]:
        """The tables to read this round."""
        tables = {r.table for r in self._map.registers} | {r.table for r in self._map.repeats}
        return {t for t in tables if self._map.every(t) is None or now >= self._due.get(t, 0.0)}

    async def _read(
        self,
        context: SourceContext,
        client: Any,
        function: int,
        address: int,
        count: int,
        registers: tuple[Register, ...],
        values: dict[str, dict[str, FieldValue]],
    ) -> None:
        """Read one span and decode its registers into ``values``."""
        try:
            words = await client.read(function, address, count)
        except ModbusError as error:
            if error.code != ILLEGAL_ADDRESS:
                context.problem(str(error))
                logger.warning("%s", error)
            elif len(registers) > 1:
                # Some device leaves a gap in the span unreadable: read these one by one.
                self._singles.update((r.table, r.name) for r in registers)
                for r in registers:
                    await self._read(context, client, function, r.address, r.size, (r,), values)
            else:
                (r,) = registers
                self._absent.add((r.table, r.name))
                logger.warning(
                    "%s.%s (register %d) is not available; leaving it out",
                    r.table,
                    r.name,
                    r.address,
                )
            return
        for r in registers:
            offset = r.address - address
            value = r.decode(words[offset : offset + r.size])
            if value is not None:
                values.setdefault(r.table, {})[r.name] = value

    async def _repeat(
        self, context: SourceContext, client: Any, repeat: Repeat, stamp: int
    ) -> list[Point]:
        """A point per filled slot of a repeated block, up to the first empty one."""
        points = []
        function = repeat.function or self._map.function
        for slot in range(repeat.max_count):
            address = repeat.start + slot * repeat.stride
            try:
                words = await client.read(function, address, repeat.length)
            except ModbusError as error:
                if error.code != ILLEGAL_ADDRESS:
                    context.problem(str(error))
                    logger.warning("%s", error)
                break
            data = struct.pack(f">{len(words)}H", *words)
            if any(f.is_zero(data) for f in repeat.fields if f.name in repeat.stop_when_zero):
                break
            tags = dict(self._tags)
            fields: dict[str, FieldValue] = {}
            for f in repeat.fields:
                value = f.decode(data)
                if value is None:
                    continue
                if f.tag:
                    tags[f.name] = str(value)
                else:
                    fields[f.name] = value
            if fields:
                points.append(self._point(repeat.table, tuple(sorted(tags.items())), stamp, fields))
        return points

    def _point(
        self,
        table: str,
        tags: tuple[tuple[str, str], ...],
        stamp: int,
        fields: dict[str, FieldValue],
    ) -> Point:
        return Point(self._prefix + table, tags, stamp, fields, precision=Precision.SECONDS)


def build(params: ModbusParams, _client: httpx2.AsyncClient, _name: str = "") -> ModbusSource:
    """A Modbus source from its validated parameters."""

    def connect() -> ModbusClient:
        return ModbusClient(params.host, params.port, params.unit, params.timeout_seconds)

    return ModbusSource(params, load_map(params.map), connect)


__all__ = ["ModbusParams", "ModbusSource", "build"]

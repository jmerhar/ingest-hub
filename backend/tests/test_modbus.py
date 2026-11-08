"""The Modbus source, against a stand-in Modbus TCP server with synthetic registers."""

from __future__ import annotations

import asyncio
import contextlib
import struct
from pathlib import Path

import pytest
from pydantic import ValidationError

from ingest_hub.adapters import ADAPTERS
from ingest_hub.adapters.modbus import ModbusParams, ModbusSource, build
from ingest_hub.adapters.modbus.client import ModbusClient, ModbusError
from ingest_hub.adapters.modbus.registers import (
    MapError,
    Register,
    RegisterMap,
    bundled_maps,
    load_map,
    plan,
)
from ingest_hub.collect import SourceContext


class Device:
    """A stand-in Modbus TCP device: registers by address, refusing those in ``illegal``."""

    def __init__(self, registers: dict[int, int], unit: int = 1) -> None:
        self.registers = registers
        self.unit = unit
        self.illegal: set[int] = set()
        self.busy = False
        self.requests: list[tuple[int, int, int]] = []
        self.reply: bytes | None = None  # sent instead of a proper answer, once
        self.server: asyncio.Server | None = None
        self.port = 0

    async def __aenter__(self) -> Device:
        self.server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *_exc) -> None:
        assert self.server is not None
        self.server.close()
        await self.server.wait_closed()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                head = await reader.readexactly(7)
                tid, _proto, length, unit = struct.unpack(">HHHB", head)
                body = await reader.readexactly(length - 1)
                function, address, count = struct.unpack(">BHH", body)
                self.requests.append((function, address, count))
                if self.reply is not None:
                    writer.write(self.reply)
                    self.reply = None
                elif self.busy:
                    writer.write(struct.pack(">HHHBBB", tid, 0, 3, unit, function | 0x80, 6))
                elif any(a in self.illegal for a in range(address, address + count)):
                    writer.write(struct.pack(">HHHBBB", tid, 0, 3, unit, function | 0x80, 2))
                else:
                    words = [self.registers.get(a, 0) for a in range(address, address + count)]
                    payload = struct.pack(f">BB{count}H", function, 2 * count, *words)
                    writer.write(struct.pack(">HHHB", tid, 0, len(payload) + 1, unit) + payload)
                await writer.drain()
        except asyncio.IncompleteReadError, ConnectionResetError:
            pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()


def u32(value: int) -> tuple[int, int]:
    return (value >> 16) & 0xFFFF, value & 0xFFFF


MAP = RegisterMap.model_validate(
    {
        "table_prefix": "dev_",
        "max_read": 10,
        "max_gap": 2,
        "tables": {"info": {"every_seconds": 3600}},
        "registers": [
            {"table": "meter", "name": "voltage", "address": 0, "scale": 0.1, "unit": "V"},
            {"table": "meter", "name": "power", "address": 1, "type": "s32"},
            {"table": "meter", "name": "energy", "address": 5, "type": "u32", "scale": 0.01},
            {"table": "meter", "name": "status", "address": 20},
            {"table": "info", "name": "serial", "address": 30, "type": "str", "length": 2},
            {"table": "info", "name": "empty", "address": 32, "type": "str", "length": 1},
            {"table": "info", "name": "id", "address": 33, "type": "hex", "length": 1},
            {
                "table": "inputs",
                "name": "temperature",
                "address": 40,
                "type": "s16",
                "scale": 0.5,
                "offset": 10,
                "function": 4,
            },
        ],
        "repeats": [
            {
                "table": "port",
                "start": 100,
                "stride": 10,
                "length": 4,
                "max_count": 3,
                "stop_when_zero": ["serial"],
                "fields": [
                    {"name": "serial", "byte": 0, "type": "hex", "length": 2, "tag": True},
                    {"name": "port", "byte": 2, "type": "u8", "tag": True},
                    {"name": "flag", "byte": 3, "type": "u8"},
                    {"name": "power", "byte": 4, "type": "u16", "scale": 0.1},
                    {"name": "label", "byte": 6, "type": "str", "length": 2},
                ],
            }
        ],
    }
)


def registers() -> dict[int, int]:
    regs = {0: 2305, 1: 0xFFFF, 2: 0xFFFE, 5: 0, 6: 12345, 20: 7, 40: 0xFFFC}
    regs.update({30: 0x4142, 31: 0x4300, 32: 0, 33: 0xBEEF})
    # Two filled port slots, then an empty one.
    regs.update({100: 0x1164, 101: 0x0103, 102: 815, 103: 0x4F4B})
    regs.update({110: 0x1164, 111: 0x0200, 112: 0, 113: 0})
    return regs


class Harness:
    def __init__(self, tmp_path: Path, register_map: RegisterMap = MAP, **params) -> None:
        self.batches: list[list] = []
        self.problems: list[str] = []
        self.sleeps: list[float] = []
        self.successes = 0
        self.accept = True
        self.now = 1_000_000.0
        self.stop_after = 1
        self.register_map = register_map
        self.params = params

        async def emit(points):
            self.batches.append(points)
            return self.accept

        async def sleep(seconds):
            self.sleeps.append(seconds)
            self.now += seconds
            if len(self.sleeps) >= self.stop_after:
                raise Stop

        def succeeded():
            self.successes += 1

        self.context = SourceContext(
            "dev",
            tmp_path,
            emit,
            on_success=succeeded,
            on_problem=self.problems.append,
            clock=lambda: self.now,
            sleep=sleep,
        )

    def source(self, device: Device) -> ModbusSource:
        values = {"host": "127.0.0.1", "port": device.port, "map": "alphaess", **self.params}
        params = ModbusParams.model_validate(values)
        return ModbusSource(
            params,
            self.register_map,
            lambda: ModbusClient("127.0.0.1", device.port, device.unit, 2.0),
        )


class Stop(Exception):
    pass


def by_table(points):
    return {(p.table, p.tags): p.fields for p in points}


async def test_a_round_reads_every_table_in_few_requests_and_decodes_values(tmp_path):
    async with Device(registers()) as device:
        harness = Harness(tmp_path, tags={"site": "home"})
        with pytest.raises(Stop):
            await harness.source(device).run(harness.context)
    (batch,) = harness.batches
    assert all(p.timestamp == 1_000_000 for p in batch)
    site = (("site", "home"),)
    assert by_table(batch) == {
        ("dev_meter", site): {"voltage": 230.5, "power": -2, "energy": 123.45, "status": 7},
        ("dev_info", site): {"serial": "ABC", "id": "beef"},
        ("dev_inputs", site): {"temperature": -12.0},
        ("dev_port", (("port", "1"), ("serial", "1164"), ("site", "home"))): {
            "flag": 3,
            "power": 81.5,
            "label": "OK",
        },
        ("dev_port", (("port", "2"), ("serial", "1164"), ("site", "home"))): {
            "flag": 0,
            "power": 0.0,
        },
    }
    # Registers 0-6 together (gap of 2), 20 alone, 30-33 together, input register 40, then the
    # repeated slots up to the empty third one.
    assert device.requests == [
        (3, 0, 7),
        (3, 20, 1),
        (3, 30, 4),
        (4, 40, 1),
        (3, 100, 4),
        (3, 110, 4),
        (3, 120, 4),
    ]
    assert harness.successes == 1 and harness.sleeps == [10.0]


async def test_slow_tables_are_read_only_as_often_as_the_map_says(tmp_path):
    async with Device(registers()) as device:
        harness = Harness(tmp_path, interval_seconds=1800)
        harness.stop_after = 3
        with pytest.raises(Stop):
            await harness.source(device).run(harness.context)
    tables = [{p.table for p in batch} for batch in harness.batches]
    assert ["dev_info" in t for t in tables] == [True, False, True]


async def test_a_refused_span_is_read_register_by_register_and_absent_ones_left_out(
    tmp_path, caplog
):
    async with Device(registers()) as device:
        device.illegal = {3, 4}  # the gap inside the 0-6 span, but no register lives there
        device.illegal.add(20)  # a register the device does not have
        harness = Harness(tmp_path)
        harness.stop_after = 2
        source = harness.source(device)
        with pytest.raises(Stop):
            await source.run(harness.context)
    meter = [by_table(b)[("dev_meter", ())] for b in harness.batches]
    assert meter == [{"voltage": 230.5, "power": -2, "energy": 123.45}] * 2
    assert "meter.status (register 20) is not available" in caplog.text
    first, second = device.requests[:9], device.requests[9:]
    assert (3, 0, 7) in first and (3, 0, 1) in first and (3, 1, 2) in first
    # The second round reads them one by one straight away and no longer asks for register 20.
    assert (3, 0, 7) not in second and (3, 20, 1) not in second and (3, 0, 1) in second


async def test_other_modbus_exceptions_are_reported_and_the_round_goes_on(tmp_path):
    async with Device(registers()) as device:
        device.busy = True
        harness = Harness(tmp_path)
        with pytest.raises(Stop):
            await harness.source(device).run(harness.context)
    # Nothing could be read: an empty round writes nothing but still counts as done.
    assert harness.batches == [] and harness.successes == 1
    assert any("Modbus exception 6" in p for p in harness.problems)


async def test_an_unreachable_device_is_reported_and_tried_again(tmp_path):
    async with Device(registers()) as device:
        port = device.port
    harness = Harness(tmp_path)
    harness.stop_after = 2
    source = ModbusSource(
        ModbusParams(host="127.0.0.1", port=port, map="alphaess"),
        MAP,
        lambda: ModbusClient("127.0.0.1", port, 1, 1.0),
    )
    with pytest.raises(Stop):
        await source.run(harness.context)
    assert len(harness.problems) == 2 and "cannot read 127.0.0.1" in harness.problems[0]
    assert harness.batches == [] and harness.successes == 0


async def test_a_refused_batch_is_reported(tmp_path):
    async with Device(registers()) as device:
        harness = Harness(tmp_path)
        harness.accept = False
        with pytest.raises(Stop):
            await harness.source(device).run(harness.context)
    assert harness.problems == ["a batch was refused"] and harness.successes == 0


async def test_the_table_prefix_can_be_overridden(tmp_path):
    async with Device(registers()) as device:
        harness = Harness(tmp_path, table_prefix="")
        with pytest.raises(Stop):
            await harness.source(device).run(harness.context)
    assert {p.table for p in harness.batches[0]} == {"meter", "info", "inputs", "port"}


async def test_a_repeated_block_ends_at_a_refused_slot(tmp_path):
    async with Device(registers()) as device:
        device.illegal = {110}
        harness = Harness(tmp_path)
        with pytest.raises(Stop):
            await harness.source(device).run(harness.context)
        device.busy = True
        source = harness.source(device)
        client = ModbusClient("127.0.0.1", device.port, 1, 2.0)
        try:
            points = await source._repeat(harness.context, client, MAP.repeats[0], 0)
        finally:
            await client.close()
    ports = [p for p in harness.batches[0] if p.table == "dev_port"]
    assert len(ports) == 1
    assert points == [] and any("exception 6" in p for p in harness.problems)


# ------------------------------------------------------------------ the client


async def test_the_client_only_reads(tmp_path):
    client = ModbusClient("127.0.0.1", 1, 1, 1.0)
    with pytest.raises(ValueError, match="not a read"):
        await client.read(6, 0, 1)
    with pytest.raises(ValueError, match="cannot read"):
        await client.read(3, 0, 126)


@pytest.mark.parametrize(
    "reply",
    [
        struct.pack(">HHHBBB", 999, 0, 3, 1, 3, 2) + b"\x00\x01",  # someone else's transaction
        struct.pack(">HHHBBB", 1, 0, 3, 1, 4, 2) + b"\x00\x01",  # the wrong function
        struct.pack(">HHHBBB", 1, 0, 3, 1, 3, 4) + b"\x00\x01",  # the wrong length
    ],
)
async def test_a_malformed_reply_closes_the_connection(reply):
    async with Device(registers()) as device:
        device.reply = reply
        client = ModbusClient("127.0.0.1", device.port, 1, 2.0)
        with pytest.raises((ConnectionError, asyncio.IncompleteReadError)):
            await client.read(3, 0, 1)
        assert client._streams is None
        # The next read opens a new connection and works.
        assert await client.read(3, 0, 1) == [2305]
        await client.close()
        await client.close()


async def test_an_exception_reply_keeps_the_connection():
    async with Device(registers()) as device:
        device.illegal = {5}
        client = ModbusClient("127.0.0.1", device.port, 1, 2.0)
        with pytest.raises(ModbusError) as error:
            await client.read(3, 5, 1)
        assert error.value.code == 2 and client._streams is not None
        assert await client.read(4, 0, 1) == [2305]
        await client.close()


async def test_a_silent_device_times_out():
    async def never(reader, writer):
        try:
            with contextlib.suppress(Exception):
                await reader.read()
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    server = await asyncio.start_server(never, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = ModbusClient("127.0.0.1", port, 1, 0.2)
    try:
        with pytest.raises(TimeoutError):
            await client.read(3, 0, 1)
        assert client._streams is None
    finally:
        server.close()
        await server.wait_closed()


# ------------------------------------------------------------------ maps


def test_reads_are_planned_within_their_limits():
    regs = [
        Register(table="t", name=f"r{a}", address=a, type="u32" if a == 9 else "u16")
        for a in (0, 1, 4, 9, 30)
    ]
    reads = plan(regs, 3, max_read=10, max_gap=3)
    assert [(r.address, r.count) for r in reads] == [(0, 5), (9, 2), (30, 1)]
    assert [(r.address, r.count) for r in plan(regs, 3, 1, -1)] == [
        (0, 1),
        (1, 1),
        (4, 1),
        (9, 2),
        (30, 1),
    ]


def test_the_bundled_maps_load():
    assert bundled_maps() == ["alphaess", "hoymiles-dtu"]
    alpha = load_map("alphaess")
    assert alpha.table_prefix == "alphaess_" and len(alpha.registers) > 150
    soc = next(r for r in alpha.registers if r.name == "soc")
    assert (soc.table, soc.address, soc.scale, soc.unit) == ("battery", 0x102, 0.1, "%")
    hoymiles = load_map("hoymiles-dtu")
    assert hoymiles.repeats[0].start == 0x1000 and hoymiles.repeats[0].stride == 40


def test_a_map_can_come_from_a_file(tmp_path):
    path = tmp_path / "m.toml"
    path.write_text('table_prefix = "x_"\n[[registers]]\ntable = "a"\nname = "b"\naddress = 1\n')
    assert load_map(str(path)).registers[0].name == "b"


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("not toml [", "register map"),
        (
            '[[registers]]\ntable = "a"\nname = "b"\naddress = 1\n'
            '[[registers]]\ntable = "a"\nname = "b"\naddress = 2\n',
            "defined twice",
        ),
        (
            'max_read = 2\n[[registers]]\ntable = "a"\nname = "b"\naddress = 1\ntype = "str"\n'
            "length = 3\n",
            "longer than max_read",
        ),
        (
            '[[repeats]]\ntable = "a"\nstart = 0\nstride = 4\nlength = 1\nstop_when_zero = ["x"]\n'
            '[[repeats.fields]]\nname = "y"\nbyte = 0\n',
            "not a field",
        ),
        (
            '[[repeats]]\ntable = "a"\nstart = 0\nstride = 4\nlength = 1\n'
            '[[repeats.fields]]\nname = "y"\nbyte = 1\ntype = "u16"\n',
            "ends beyond",
        ),
    ],
)
def test_maps_that_make_no_sense_are_refused(tmp_path, text, message):
    path = tmp_path / "bad.toml"
    path.write_text(text)
    with pytest.raises(MapError, match=message):
        load_map(str(path))


def test_a_missing_map_is_refused_with_its_name():
    with pytest.raises(MapError, match="no register map 'nonesuch'"):
        load_map("nonesuch")
    with pytest.raises(ValidationError, match="no register map"):
        ModbusParams(host="h", map="nonesuch")


def test_the_adapter_is_registered_and_builds_a_client():
    adapter = ADAPTERS["modbus"]
    params = adapter.params.model_validate({"host": "10.0.0.2", "map": "hoymiles-dtu", "unit": 3})
    source = adapter.build(params, None, "solar")
    client = source._client()
    assert (client._host, client._port, client._unit) == ("10.0.0.2", 502, 3)
    assert isinstance(build(params, None), ModbusSource)


async def test_a_full_repeated_block_stops_at_its_count_and_slots_without_fields_are_skipped(
    tmp_path,
):
    slots = RegisterMap.model_validate(
        {
            "tables": {"port": {"every_seconds": 3600}},
            "repeats": [
                {
                    "table": "port",
                    "start": 100,
                    "stride": 10,
                    "length": 4,
                    "max_count": 2,
                    "fields": [
                        {"name": "serial", "byte": 0, "type": "hex", "length": 2, "tag": True},
                        {"name": "label", "byte": 6, "type": "str", "length": 2},
                    ],
                }
            ],
        }
    )
    async with Device(registers()) as device:
        harness = Harness(tmp_path, slots, interval_seconds=60)
        harness.stop_after = 2
        with pytest.raises(Stop):
            await harness.source(device).run(harness.context)
    # Two slots read, the second with no label and so no point; nothing in the second round,
    # which comes before the table is due again.
    assert [len(b) for b in harness.batches] == [1]
    assert device.requests == [(3, 100, 4), (3, 110, 4)]


def test_as_float_keeps_an_unscaled_number_a_float():
    plain = Register(table="t", name="a", address=0)
    forced = Register(table="t", name="b", address=0, as_float=True)
    assert plain.decode([33]) == 33 and isinstance(plain.decode([33]), int)
    assert forced.decode([33]) == 33.0 and isinstance(forced.decode([33]), float)
    temp = next(r for r in load_map("alphaess").registers if r.name == "inverter_temp")
    assert temp.decode([330]) == 33.0


def test_skip_zero_leaves_out_a_zero_from_a_running_total():
    total = Register(table="t", name="total", address=0, type="u32", scale=0.1, skip_zero=True)
    assert total.decode([0, 0]) is None
    assert total.decode([0, 1012]) == 101.2
    grid = {r.name: r for r in load_map("alphaess").registers if r.table == "grid"}
    assert grid["total_energy_consumed_from_grid"].skip_zero
    assert grid["total_active_power"].decode([0, 0]) == 0
    (port,) = load_map("hoymiles-dtu").repeats
    production = next(f for f in port.fields if f.name == "total_production")
    assert production.decode(bytes(40)) is None
    assert production.decode(bytes(20) + (1184).to_bytes(4, "big") + bytes(16)) == 1184

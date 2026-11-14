"""Importing Vaillant's EU Data Act exports from a drop folder, with synthetic exports."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from jigkit.sinks import Point, Precision
from pydantic import ValidationError

from ingest_hub.adapters import ADAPTERS
from ingest_hub.adapters.vaillant import export
from ingest_hub.adapters.vaillant.export import (
    VaillantExportParams,
    VaillantExportSource,
    _snake,
    _value,
    export_points,
)
from ingest_hub.collect import SourceContext

T0 = "2026-11-03T17:00:06.839+00:00"
MS0 = 1793725206839


def entry(topic: str, message: Any, function: str | None = None, timestamp: str = T0) -> dict:
    return {
        "topic": topic,
        "function": function,
        "timestamp": timestamp,
        "message_value": json.dumps(message) if not isinstance(message, str) else message,
    }


TOPOLOGY = entry(
    "hvac-topology",
    {
        "devices": [
            {"deviceType": "HEAT_PUMP", "eBusAddress": 8, "busCouplerAddress": 0},
            {"deviceType": "HP_ACCESSORY", "eBusAddress": 118, "busCouplerAddress": 0},
            {"deviceType": "GATEWAY"},  # no address: not usable
            {"eBusAddress": 21},  # no type
        ]
    },
)
POWER = entry(
    "gateway.hem.message.received",
    {
        "mpc_device_status": [
            {"ebus_address": 8, "bus_coupler_address": 0, "current_power": 980},
            {"ebus_address": 118, "bus_coupler_address": 0, "current_power": 5},
            {"ebus_address": 9, "current_power": 1.5},
            {"ebus_address": 8, "current_power": None},
            {"ebus_address": 8, "current_power": True},
        ]
    },
    "status_message_mpc",
)
EBUS = entry(
    "ebus.responses.interpreted",
    {
        "eventPayload": {"device": {"eBusAddress": 8, "busCouplerAddress": 0}},
        "dataPoint": [
            {
                "name": "CompressorFrequencySignal",
                "valueUnitMap": [{"value": "35.5", "unit": "rps"}],
            },
            {"name": "3WV", "valueUnitMap": [{"name": "3WV", "value": "DHW"}]},
            {"name": "dhwActive", "valueUnitMap": [{"name": "dhwActive", "value": "on"}]},
            {"name": "boilerError", "valueUnitMap": [{"name": "boilerError", "value": "false"}]},
            {
                "name": "dhwTemperature",
                "valueUnitMap": [{"name": "dhwTemperature", "value": "NO_VALUE"}],
            },
            {
                "name": "FaultList",
                "valueUnitMap": [
                    {"name": "Value0", "value": "0"},
                    {"name": "Value1", "value": "-3"},
                ],
            },
            {"name": "ignored", "disabled": True, "valueUnitMap": [{"value": "1"}]},
            {"valueUnitMap": [{"value": "1"}]},
        ],
    },
)
EMPTY_EBUS = entry(
    "ebus.responses.interpreted",
    {"eventPayload": {"device": {"eBusAddress": 8}}, "dataPoint": []},
)
STATS = entry(
    "end-user-system-realtime-stats.inbound",
    {
        "eventPayload": {
            "device": {"eBusAddress": 8, "busCouplerAddress": 0},
            "payload": {
                "operationMode": "DOMESTIC_HOT_WATER",
                "operatingTime": 181,
                "onOffCycles": 3,
            },
        }
    },
)
STATS_WITHOUT_CYCLES = entry(
    "end-user-system-realtime-stats.inbound",
    {
        "eventPayload": {
            "device": {"eBusAddress": 118},
            "payload": {"operationMode": "HEATING", "operatingTime": 0, "onOffCycles": None},
        }
    },
)
EMPTY_STATS = entry(
    "end-user-system-realtime-stats.inbound",
    {"eventPayload": {"device": {}, "payload": {"operatingTime": None}}},
)
ENERGY = entry(
    "end-user-system-emf.inbound",
    {
        "eventPayload": {
            "ebusAddress": 8,
            "busCouplerAddress": 0,
            "operationMode": "HEATING",
            "energyType": "CONSUMED_ELECTRICAL_ENERGY",
            "energyValue": 2627.75,
        }
    },
)
ENERGY_WITHOUT_VALUE = entry(
    "end-user-system-emf.inbound", {"eventPayload": {"ebusAddress": 8, "energyValue": None}}
)
OTHER = entry("system-control.state", {"data": {}})


def write_export(path: Path, entries: list[dict]) -> Path:
    path.write_text(json.dumps(entries))
    return path


def params(tmp_path: Path, **overrides) -> VaillantExportParams:
    return VaillantExportParams.model_validate({"inbox": tmp_path / "inbox", **overrides})


def rows(points: list[Point]) -> list[tuple[str, dict, int, dict]]:
    return [(p.table, dict(p.tags), p.timestamp, p.fields) for p in points]


def test_each_kind_of_message_becomes_points_named_after_the_topology(tmp_path):
    # The topology comes last, yet names every device.
    path = write_export(
        tmp_path / "x.json",
        [
            POWER,
            EBUS,
            EMPTY_EBUS,
            STATS,
            STATS_WITHOUT_CYCLES,
            EMPTY_STATS,
            ENERGY,
            ENERGY_WITHOUT_VALUE,
            OTHER,
            TOPOLOGY,
        ],
    )
    points, counts = export_points(path, params(tmp_path))
    assert all(p.precision is Precision.MILLISECONDS for p in points)
    assert rows(points) == [
        ("vaillant_power", {"device": "heat_pump"}, MS0, {"current_power": 980.0}),
        ("vaillant_power", {"device": "hp_accessory"}, MS0, {"current_power": 5.0}),
        ("vaillant_power", {"device": "ebus_9"}, MS0, {"current_power": 1.5}),
        (
            "vaillant_ebus",
            {"device": "heat_pump"},
            MS0,
            {
                "compressor_frequency_signal": 35.5,
                "3wv": "DHW",
                "dhw_active": True,
                "boiler_error": False,
                "fault_list_value0": 0.0,
                "fault_list_value1": -3.0,
            },
        ),
        (
            "vaillant_stats",
            {"device": "heat_pump", "operation_mode": "domestic_hot_water"},
            MS0,
            {"operating_minutes": 181.0, "on_off_cycles": 3.0},
        ),
        (
            "vaillant_stats",
            {"device": "hp_accessory", "operation_mode": "heating"},
            MS0,
            {"operating_minutes": 0.0},
        ),
        (
            "vaillant_energy_total",
            {
                "device": "heat_pump",
                "operation_mode": "heating",
                "energy_type": "consumed_electrical_energy",
            },
            MS0,
            {"wh": 2627.75},
        ),
    ]
    assert counts == {"power": 3, "ebus": 1, "stats": 2, "energy_total": 1}


def test_devices_can_be_renamed_and_tables_prefixed(tmp_path):
    path = write_export(tmp_path / "x.json", [TOPOLOGY, POWER])
    settings = params(
        tmp_path, table_prefix="hp_", devices={"heat_pump": "heatpump", "ebus_9": "meter"}
    )
    points, _ = export_points(path, settings)
    assert [(p.table, dict(p.tags)["device"]) for p in points] == [
        ("hp_power", "heatpump"),
        ("hp_power", "hp_accessory"),
        ("hp_power", "meter"),
    ]


def test_a_device_without_a_usable_address_is_unknown(tmp_path):
    message = {"mpc_device_status": [{"ebus_address": "eight", "current_power": 1}]}
    path = write_export(
        tmp_path / "x.json", [entry("gateway.hem.message.received", message, "status_message_mpc")]
    )
    points, _ = export_points(path, params(tmp_path))
    assert dict(points[0].tags) == {"device": "unknown"}


def test_unreadable_messages_are_skipped_and_counted(tmp_path, caplog):
    path = write_export(
        tmp_path / "x.json",
        [
            POWER,
            entry("t", "{not json"),
            entry("t", "[1]"),
            {"topic": "t"},
            entry("t", {}, timestamp="yesterday"),
            # A timestamp without an offset is taken as UTC.
            entry(
                "end-user-system-emf.inbound",
                ENERGY["message_value"],
                timestamp="2026-11-03T17:00:06.839",
            ),
        ],
    )
    points, _ = export_points(path, params(tmp_path))
    assert len(points) == 4 and points[-1].timestamp == MS0
    assert "skipped 4 unreadable message(s)" in caplog.text


@pytest.mark.parametrize(
    ("raw", "value"),
    [
        (True, True),
        (3, 3.0),
        ("12.5", 12.5),
        ("-1", -1.0),
        ("ON", True),
        ("Off", False),
        (" closed ", "closed"),
        ("", None),
        ("NO_VALUE", None),
        ("x" * 65, None),
        (None, None),
        ([1], None),
    ],
)
def test_ebus_values(raw, value):
    assert _value(raw) == value


@pytest.mark.parametrize(
    ("name", "snake"),
    [
        ("FanUnitAirInletTemperature", "fan_unit_air_inlet_temperature"),
        ("BuildPumpPWM", "build_pump_pwm"),
        ("3WV", "3wv"),
        ("modulationValue1", "modulation_value1"),
        ("FaultList_Value0", "fault_list_value0"),
    ],
)
def test_names_in_snake_case(name, snake):
    assert _snake(name) == snake


class Stop(Exception):
    """Ends a source's run loop in a test."""


class Harness:
    def __init__(self, tmp_path: Path, **overrides) -> None:
        self.params = params(tmp_path, **overrides)
        self.inbox = self.params.inbox
        self.batches: list[list[Point]] = []
        self.accept = True
        self.problems: list[str] = []
        self.successes = 0
        self.sleeps: list[float] = []

        async def emit(points: list[Point]) -> bool:
            self.batches.append(points)
            return self.accept

        async def sleep(seconds: float) -> None:
            self.sleeps.append(seconds)
            raise Stop

        def succeeded() -> None:
            self.successes += 1

        self.context = SourceContext(
            "heating_export",
            tmp_path,
            emit,
            on_success=succeeded,
            on_problem=self.problems.append,
            sleep=sleep,
        )
        self.source = VaillantExportSource(self.params)


async def test_a_scan_imports_each_export_and_moves_it_aside(tmp_path, caplog):
    harness = Harness(tmp_path)
    caplog.set_level("INFO")
    with pytest.raises(Stop):
        await harness.source.run(harness.context)  # creates the inbox; nothing in it yet
    assert harness.inbox.is_dir() and harness.batches == [] and harness.successes == 1
    assert harness.sleeps == [600.0]

    write_export(harness.inbox / "b.json", [TOPOLOGY, POWER, ENERGY])
    write_export(harness.inbox / "a.json", [OTHER])
    (harness.inbox / "notes.txt").write_text("not an export")
    await harness.source.scan(harness.context)
    # a.json has nothing to import, so only b.json hands over a batch.
    assert [len(b) for b in harness.batches] == [4]
    assert sorted(p.name for p in (harness.inbox / "imported").iterdir()) == ["a.json", "b.json"]
    assert sorted(p.name for p in harness.inbox.iterdir()) == ["imported", "notes.txt"]
    assert "imported a.json: nothing" in caplog.text
    assert "imported b.json: 1 energy_total, 3 power" in caplog.text


async def test_large_exports_are_handed_over_in_batches(tmp_path, monkeypatch):
    monkeypatch.setattr(export, "BATCH", 2)
    harness = Harness(tmp_path)
    harness.inbox.mkdir()
    write_export(harness.inbox / "x.json", [TOPOLOGY, POWER, ENERGY])
    await harness.source.scan(harness.context)
    assert [len(b) for b in harness.batches] == [2, 2]


async def test_an_export_dropped_again_replaces_the_imported_copy(tmp_path):
    harness = Harness(tmp_path)
    (harness.inbox / "imported").mkdir(parents=True)
    (harness.inbox / "imported" / "x.json").write_text("old")
    write_export(harness.inbox / "x.json", [POWER])
    await harness.source.scan(harness.context)
    assert json.loads((harness.inbox / "imported" / "x.json").read_text()) == [POWER]


@pytest.mark.parametrize("content", ["{not json", '{"topic": "x"}', b"\xff\xfe"])
async def test_a_file_that_is_not_an_export_is_moved_to_failed(tmp_path, content):
    harness = Harness(tmp_path)
    harness.inbox.mkdir()
    bad = harness.inbox / "bad.json"
    if isinstance(content, bytes):
        bad.write_bytes(content)
    else:
        bad.write_text(content)
    write_export(harness.inbox / "good.json", [POWER])
    await harness.source.scan(harness.context)
    assert (harness.inbox / "failed" / "bad.json").exists()
    assert (harness.inbox / "imported" / "good.json").exists()
    assert any("cannot read export bad.json" in p for p in harness.problems)


async def test_a_refused_batch_leaves_the_export_in_the_inbox(tmp_path):
    harness = Harness(tmp_path)
    harness.inbox.mkdir()
    harness.accept = False
    write_export(harness.inbox / "a.json", [POWER])
    write_export(harness.inbox / "b.json", [POWER])
    await harness.source.scan(harness.context)
    assert len(harness.batches) == 1  # b.json waits for the next scan too
    assert sorted(p.name for p in harness.inbox.iterdir()) == ["a.json", "b.json"]
    assert any("stays in the inbox" in p for p in harness.problems)


def test_the_adapter_is_registered_and_validates_its_params(tmp_path):
    adapter = ADAPTERS["vaillant_export"]
    built = adapter.build(adapter.params.model_validate({"inbox": str(tmp_path)}), None, "x")
    assert isinstance(built, VaillantExportSource)
    assert "EU Data Act export (`.json`)" in built.upload_help
    assert "fetches" not in built.upload_help
    fetching = VaillantExportSource(params(tmp_path), portal=object())  # type: ignore[arg-type]
    assert "also fetches the previous day's export itself" in fetching.upload_help
    with pytest.raises(ValidationError, match="inbox"):
        VaillantExportParams.model_validate({})
    with pytest.raises(ValidationError, match="scan_minutes"):
        params(tmp_path, scan_minutes=0)


def stated(value: Any) -> dict:
    return {"value": value, "metadata": None, "lastUpdated": 1793725206000}


STATE = entry(
    "system-control.state",
    {
        "data": {
            "systemParameters": {
                "state": {
                    "outdoorTemperature": stated(7.03125),
                    "outdoorTemperatureAverage24h": stated(8.5),
                    "systemWaterPressure": stated(2),
                    "energyManagerState": stated("STANDBY"),
                    "systemOff": stated(False),
                    "currentFlowTemperature": stated(None),
                }
            },
            "zoneSettings": {
                "0": {
                    "state": {
                        "currentRoomTemperature": stated(22.1625),
                        "currentRoomHumidity": stated(51.0),
                        "quickVetoEndDateTime": stated(0),
                        "quickVetoStartTimestamp": stated(0),
                        "nested": stated({"a": 1}),
                    },
                    "configuration": {"ignored": stated(1)},
                },
                "1": {"state": {}},
                "2": None,
            },
            "circuitSettings": {"0": {"state": {"circuitState": stated("STANDBY")}}},
            "domesticHotWaterSettings": {
                "255": {
                    "state": {
                        "currentCircuitTemperature": stated(54.5),
                        "currentSpecialFunction": stated("REGULAR"),
                    }
                }
            },
            "ventilationSettings": {},
        }
    },
)


def test_the_controller_state_goes_into_the_live_sources_tables(tmp_path):
    path = write_export(
        tmp_path / "x.json",
        [
            STATE,
            entry("system-control.state", {"data": {"zoneSettings": [], "systemParameters": {}}}),
            entry("system-control.state", {"data": {"circuitSettings": {"0": {"state": [1]}}}}),
        ],
    )
    points, counts = export_points(path, params(tmp_path))
    assert rows(points) == [
        (
            "vaillant_system",
            {},
            MS0,
            {
                "outdoor_temperature": 7.03125,
                "outdoor_temperature_average24h": 8.5,
                "system_water_pressure": 2.0,
                "energy_manager_state": "STANDBY",
                "system_off": False,
            },
        ),
        (
            "vaillant_zone",
            {"zone": "0"},
            MS0,
            {"current_room_temperature": 22.1625, "current_room_humidity": 51.0},
        ),
        ("vaillant_circuit", {"circuit": "0"}, MS0, {"circuit_state": "STANDBY"}),
        (
            "vaillant_dhw",
            {"dhw": "255"},
            MS0,
            {"current_dhw_temperature": 54.5, "current_special_function": "REGULAR"},
        ),
    ]
    assert counts == {"system": 1, "zone": 1, "circuit": 1, "dhw": 1}


async def test_device_names_from_one_export_name_the_next(tmp_path):
    harness = Harness(tmp_path, devices={"heat_pump": "heatpump"})
    harness.inbox.mkdir()
    write_export(harness.inbox / "a.json", [TOPOLOGY, POWER])
    await harness.source.scan(harness.context)
    saved = json.loads((tmp_path / export.NAMES_FILE).read_text())
    assert saved == {"0:118": "hp_accessory", "0:8": "heat_pump"}
    # The next export has no topology, as most do, yet its devices keep their names.
    write_export(harness.inbox / "b.json", [POWER])
    await harness.source.scan(harness.context)
    devices = [dict(p.tags)["device"] for p in harness.batches[-1]]
    assert devices == ["heatpump", "hp_accessory", "ebus_9"]


async def test_unreadable_device_names_fall_back_to_addresses(tmp_path, caplog):
    harness = Harness(tmp_path)
    harness.inbox.mkdir()
    (tmp_path / export.NAMES_FILE).write_text("{not json")
    write_export(harness.inbox / "a.json", [POWER])
    await harness.source.scan(harness.context)
    assert "naming devices by address" in caplog.text
    assert dict(harness.batches[0][0].tags) == {"device": "ebus_8"}
    assert not (tmp_path / export.NAMES_FILE).read_text().startswith("{\n")  # left alone

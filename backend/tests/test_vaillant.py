"""The Vaillant source, against a stand-in for myPyllant's API with synthetic devices."""

from __future__ import annotations

import contextlib
import datetime as dt
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from jigkit.sinks import Point, Precision
from pydantic import ValidationError

from ingest_hub.adapters import vaillant
from ingest_hub.adapters.vaillant import STATE_FILE, VaillantParams, VaillantSource, _load
from ingest_hub.collect import PROGRESS_TABLE, SourceContext

UTC = dt.UTC
NOW = dt.datetime(2026, 11, 4, 10, 20, tzinfo=UTC)
FIRST = dt.datetime(2026, 11, 3, 17, 54, 12, tzinfo=UTC)


@dataclass
class Device:
    device_uuid: str
    device_type: str
    product_name: str | None = None
    first_data: dt.datetime | None = FIRST


@dataclass
class Bucket:
    start_date: dt.datetime
    value: float | None


@dataclass
class Series:
    operation_mode: str
    energy_type: str | None
    data: list[Bucket]
    value_type: str | None = None


@dataclass
class System:
    devices: list[Device]
    mpc: dict | None = None
    state: dict | None = None


PUMP = Device("uuid-pump", "HEATPUMP", "Heat pump X")
HEATER = Device("uuid-heater", "ELECTRIC_AUXILIARY_HEATER", None)


def hour(h: int, day: int = 3) -> dt.datetime:
    return dt.datetime(2026, 11, day, h, tzinfo=UTC)


@dataclass
class FakeApi:
    systems: list[System]
    series: dict[str, list[Series]] = field(default_factory=dict)
    oauth_session_expires: dt.datetime | None = None
    calls: list[tuple[str, dt.datetime, dt.datetime]] = field(default_factory=list)
    refreshed: int = 0
    fail_with: list[BaseException] = field(default_factory=list)
    system_calls: int = 0

    async def get_systems(self, **kwargs: Any):
        self.system_calls += 1
        if self.fail_with:
            raise self.fail_with.pop(0)
        for system in self.systems:
            yield system

    async def get_data_by_device(self, device, data_resolution, data_from, data_to):
        assert data_resolution == "HOUR"
        self.calls.append((device.device_type, data_from, data_to))
        for series in self.series.get(device.device_type, []):
            yield series

    async def refresh_token(self) -> None:
        self.refreshed += 1


class Harness:
    def __init__(self, tmp_path: Path, api: FakeApi, now: dt.datetime = NOW) -> None:
        self.now = now.timestamp()
        self.api = api
        self.batches: list[list[Point]] = []
        self.accept = True
        self.problems: list[str] = []
        self.progress: list[tuple[str, int, str | None]] = []
        self.sleeps: list[float] = []
        self.stop_after = 99

        async def emit(points: list[Point]) -> bool:
            self.batches.append(points)
            return self.accept

        async def sleep(seconds: float) -> None:
            self.sleeps.append(seconds)
            self.now += seconds
            if len(self.sleeps) >= self.stop_after:
                raise SystemExit

        self.context = SourceContext(
            "heating",
            tmp_path,
            emit,
            on_success=lambda: None,
            on_problem=self.problems.append,
            on_progress=lambda p: self.progress.append((p.stream, p.complete_until_ms, p.label)),
            clock=lambda: self.now,
            sleep=sleep,
        )

        @contextlib.asynccontextmanager
        async def connect():
            yield api

        self.source = VaillantSource(params(), connect, "HOUR")

    @property
    def path(self) -> Path:
        return self.context.state_dir / STATE_FILE


def params(**overrides) -> VaillantParams:
    values = {"username_file": "/u", "password_file": "/p", **overrides}
    return VaillantParams.model_validate(values)


def energy_api() -> FakeApi:
    return FakeApi(
        [System([PUMP, HEATER])],
        {
            "HEATPUMP": [
                Series(
                    "HEATING",
                    "CONSUMED_ELECTRICAL_ENERGY",
                    [Bucket(hour(17), None), Bucket(hour(18), 0.4), Bucket(hour(19), 3.9)],
                ),
                Series(
                    "DOMESTIC_HOT_WATER",
                    None,
                    [Bucket(hour(18), 45.7)],
                    value_type="HEAT_GENERATED",
                ),
            ],
            "ELECTRIC_AUXILIARY_HEATER": [
                Series("HEATING", "CONSUMED_ELECTRICAL_ENERGY", [Bucket(hour(20), 12.0)]),
            ],
        },
    )


async def test_first_energy_poll_reads_from_first_data_and_publishes_progress(tmp_path):
    harness = Harness(tmp_path, energy_api())
    cursors: dict[str, int] = {}
    await harness.source.poll_energy(harness.context, harness.api, cursors, harness.path)

    # Read from the device's first data, floored to the hour, up to now.
    assert harness.api.calls[0] == ("HEATPUMP", hour(17), NOW)
    pump_batch, heater_batch = harness.batches
    data = [p for p in pump_batch if p.table == "vaillant_energy"]
    assert [(dict(p.tags), p.timestamp, p.fields) for p in data] == [
        (
            {
                "device": "heatpump",
                "operation_mode": "heating",
                "energy_type": "consumed_electrical_energy",
            },
            int(hour(18).timestamp()),
            {"wh": 0.4, "product": "Heat pump X"},
        ),
        (
            {
                "device": "heatpump",
                "operation_mode": "heating",
                "energy_type": "consumed_electrical_energy",
            },
            int(hour(19).timestamp()),
            {"wh": 3.9, "product": "Heat pump X"},
        ),
        (
            {
                "device": "heatpump",
                "operation_mode": "domestic_hot_water",
                "energy_type": "heat_generated",
            },
            int(hour(18).timestamp()),
            {"wh": 45.7, "product": "Heat pump X"},
        ),
    ]
    assert all(p.precision is Precision.SECONDS for p in data)
    heater = [p for p in heater_batch if p.table == "vaillant_energy"]
    assert heater[0].fields == {"wh": 12.0}  # no product name, no product field

    complete = int((NOW - dt.timedelta(hours=3)).timestamp() * 1000)
    progress = [p for p in pump_batch if p.table == PROGRESS_TABLE]
    assert dict(progress[0].tags) == {"source": "heating", "stream": "heatpump"}
    assert progress[0].fields == {"complete_until": complete}
    assert harness.progress == [
        ("heatpump", complete, "Heat pump X"),
        ("electric_auxiliary_heater", complete, None),
    ]
    assert cursors == {"heatpump": complete, "electric_auxiliary_heater": complete}
    assert json.loads(harness.path.read_text()) == cursors


async def test_later_polls_rescan_before_the_cursor_and_never_go_backwards(tmp_path):
    harness = Harness(tmp_path, energy_api())
    ahead = int((NOW - dt.timedelta(minutes=30)).timestamp() * 1000)
    cursors = {"heatpump": ahead, "electric_auxiliary_heater": ahead}
    await harness.source.poll_energy(harness.context, harness.api, cursors, harness.path)
    expected_start = (NOW - dt.timedelta(minutes=30, hours=48)).replace(minute=0, second=0)
    assert harness.api.calls[0][1] == expected_start
    assert cursors["heatpump"] == ahead


async def test_a_device_without_first_data_reaches_back_a_week(tmp_path):
    api = FakeApi([System([Device("u", "HEATPUMP", first_data=None)])])
    harness = Harness(tmp_path, api)
    await harness.source.poll_energy(harness.context, api, {}, harness.path)
    assert api.calls[0][1] == (NOW - dt.timedelta(days=7)).replace(minute=0)


async def test_a_refused_batch_leaves_the_cursor(tmp_path):
    harness = Harness(tmp_path, energy_api())
    harness.accept = False
    cursors: dict[str, int] = {}
    await harness.source.poll_energy(harness.context, harness.api, cursors, harness.path)
    assert cursors == {} and not harness.path.exists()
    assert any("refused" in p for p in harness.problems)


async def test_state_poll_writes_power_and_state_tables(tmp_path):
    state = {
        "system": {
            "outdoor_temperature": 9.34,
            "system_off": False,
            "energy_manager_state": "IDLE",
            "system_water_pressure": 2,
        },
        "circuits": [
            {"index": 1},
            {"index": 0, "current_circuit_flow_temperature": 30.5, "circuit_state": "STANDBY"},
        ],
        "zones": [
            {
                "index": 0,
                "current_room_temperature": 22.54,
                "current_room_humidity": None,
                "quick_veto_end_date_time": "2026-11-04T12:00:00Z",
                "schedule": [1, 2],
            }
        ],
        "dhw": [
            {"index": 255, "current_dhw_temperature": 52, "current_special_function": "x" * 80}
        ],
        "ventilation": [{}],
        "dhw_extra": [],
    }
    mpc = {
        "devices": [
            {"device_id": "uuid-pump", "current_power": 6},
            {"device_id": "uuid-gone", "current_power": 3},
            {"device_id": "uuid-heater", "current_power": None},
        ]
    }
    api = FakeApi([System([PUMP, HEATER], mpc=mpc, state=state)])
    harness = Harness(tmp_path, api)
    await harness.source.poll_state(harness.context, api)
    (batch,) = harness.batches
    rows = {(p.table, p.tags): p.fields for p in batch}
    assert all(p.timestamp == int(NOW.timestamp()) for p in batch)
    assert rows == {
        ("vaillant_power", (("device", "heatpump"),)): {"current_power": 6.0},
        ("vaillant_system", ()): {
            "outdoor_temperature": 9.34,
            "system_off": False,
            "energy_manager_state": "IDLE",
            "system_water_pressure": 2.0,
        },
        ("vaillant_circuit", (("circuit", "0"),)): {
            "current_circuit_flow_temperature": 30.5,
            "circuit_state": "STANDBY",
        },
        ("vaillant_zone", (("zone", "0"),)): {"current_room_temperature": 22.54},
        ("vaillant_dhw", (("dhw", "255"),)): {"current_dhw_temperature": 52.0},
    }


async def test_state_without_mpc_or_state_writes_nothing(tmp_path):
    api = FakeApi([System([PUMP])])
    harness = Harness(tmp_path, api)
    await harness.source.poll_state(harness.context, api)
    assert harness.batches == []  # an empty emit writes nothing


class Refused(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status


async def test_run_polls_on_schedule_refreshes_and_restores_cursors(tmp_path):
    api = energy_api()
    api.systems[0].state = {"system": {"outdoor_temperature": 8.0}}
    api.oauth_session_expires = NOW + dt.timedelta(minutes=1)
    harness = Harness(tmp_path, api)
    harness.path.write_text(json.dumps({"heatpump": 123}))
    harness.stop_after = 2
    with pytest.raises(SystemExit):
        await harness.source.run(harness.context)
    # Restored first, then the first poll's progress.
    assert harness.progress[0] == ("heatpump", 123, None)
    assert api.refreshed >= 1
    # State at once, energy a minute later, then state again twenty minutes after the first.
    assert harness.sleeps == [60.0, 1140.0]
    # The energy poll reused the state poll's systems rather than asking for them again.
    assert api.system_calls == 1
    energy_reads = {c[0] for c in api.calls}
    assert energy_reads == {"HEATPUMP", "ELECTRIC_AUXILIARY_HEATER"}
    assert len(api.calls) == 2


async def test_a_session_far_from_expiry_is_not_refreshed(tmp_path):
    api = energy_api()
    api.oauth_session_expires = NOW + dt.timedelta(hours=1)
    harness = Harness(tmp_path, api)
    harness.stop_after = 1
    with pytest.raises(SystemExit):
        await harness.source.run(harness.context)
    assert api.refreshed == 0


async def test_a_quota_refusal_at_login_is_waited_out_for_as_long_as_vaillant_says(tmp_path):
    api = energy_api()
    attempts = []
    quota = Refused(403)
    quota.args = (
        '403, message=\'Quota Exceeded, response was: { "message": "Out of call volume '
        "quota. Quota will be replenished in 00:00:49.\" }'",
    )

    @contextlib.asynccontextmanager
    async def connect():
        attempts.append(1)
        if len(attempts) == 1:
            raise quota
        yield api

    harness = Harness(tmp_path, api)
    harness.source = VaillantSource(params(), connect, "HOUR")
    harness.stop_after = 2
    with pytest.raises(SystemExit):
        await harness.source.run(harness.context)
    assert harness.sleeps[0] == 49 + 10
    assert len(attempts) == 2 and any("pausing 59 s" in p for p in harness.problems)


async def test_a_403_that_is_not_about_the_quota_ends_the_run(tmp_path):
    api = energy_api()
    api.fail_with = [Refused(403)]
    harness = Harness(tmp_path, api)
    with pytest.raises(Refused):
        await harness.source.run(harness.context)


def test_a_long_replenishment_is_capped():
    error = Exception("Quota will be replenished in 02:00:00")
    assert vaillant._pause_seconds(error, 900.0) == 900.0


async def test_series_are_requested_one_at_a_time_a_moment_apart(tmp_path):
    @dataclass
    class WithSeries(Device):
        data: list = field(default_factory=list)

    device = WithSeries("u", "HEATPUMP", data=["first", "second"])
    seen = []

    class Recording(FakeApi):
        async def get_data_by_device(self, device, data_resolution, data_from, data_to):
            seen.append(list(device.data))
            for item in ():
                yield item

    api = Recording([System([device])])
    harness = Harness(tmp_path, api)
    await harness.source.poll_energy(harness.context, api, {}, harness.path)
    assert seen == [["first"], ["second"]]
    assert harness.sleeps == [vaillant.SERIES_SPACING]
    assert device.data == ["first", "second"]  # the device itself is left as it was


async def test_quota_and_server_errors_are_waited_out(tmp_path):
    api = energy_api()
    api.fail_with = [Refused(429)]
    harness = Harness(tmp_path, api)
    harness.stop_after = 2
    with pytest.raises(SystemExit):
        await harness.source.run(harness.context)
    assert harness.sleeps[0] == 60 * 60
    assert any("HTTP 429" in p for p in harness.problems)


async def test_other_errors_end_the_run(tmp_path):
    api = energy_api()
    api.fail_with = [Refused(401)]
    harness = Harness(tmp_path, api)
    with pytest.raises(Refused):
        await harness.source.run(harness.context)
    api.fail_with = [RuntimeError("boom")]
    with pytest.raises(RuntimeError):
        await harness.source.run(harness.context)


def test_saved_cursors_survive_a_bad_file(tmp_path, caplog):
    path = tmp_path / STATE_FILE
    assert _load(path) == {}
    path.write_text("{not json")
    assert _load(path) == {}
    assert "starting without cursors" in caplog.text


async def test_build_reads_the_credentials_and_connects_with_mypyllant(tmp_path):
    (tmp_path / "u").write_text("user@example.org\n")
    (tmp_path / "p").write_text("synthetic\n")
    source = vaillant.build(
        params(username_file=tmp_path / "u", password_file=tmp_path / "p"), None
    )
    api = source._connect()
    try:
        assert api.username == "user@example.org" and api.password == "synthetic"
        assert api.brand == "vaillant" and api.country == "netherlands"
    finally:
        await api.aiohttp_session.close()


def test_params_are_validated():
    with pytest.raises(ValidationError, match="energy_minutes"):
        params(energy_minutes=1)
    with pytest.raises(ValidationError, match="table_prefix"):
        params(table_prefix="1x")


async def test_energy_can_be_due_while_state_is_not(tmp_path):
    api = energy_api()
    harness = Harness(tmp_path, api)

    @contextlib.asynccontextmanager
    async def connect():
        yield api

    harness.source = VaillantSource(params(state_minutes=30, energy_minutes=15), connect, "HOUR")
    harness.stop_after = 3
    with pytest.raises(SystemExit):
        await harness.source.run(harness.context)
    # Energy at 1 and 16 minutes, state only at 0.
    assert harness.sleeps == [60.0, 900.0, 840.0]
    assert len(api.calls) == 4
    states = [b for b in harness.batches if b and b[0].table == "vaillant_system"]
    assert len(states) <= 1


async def test_each_poll_logs_how_many_requests_it_took(tmp_path, caplog):
    class Session:
        request_count = 0

    class Counting(FakeApi):
        async def get_systems(self, **kwargs):
            self.aiohttp_session.request_count += 4
            async for system in super().get_systems(**kwargs):
                yield system

        async def get_data_by_device(self, *args):
            self.aiohttp_session.request_count += 1
            async for series in super().get_data_by_device(*args):
                yield series

    api = Counting([System([PUMP, HEATER])])
    api.aiohttp_session = Session()
    harness = Harness(tmp_path, api)
    caplog.set_level("INFO")
    await harness.source.poll_state(harness.context, api)
    await harness.source.poll_energy(harness.context, api, {}, harness.path)
    assert "Vaillant state poll: 4 request(s)" in caplog.text
    assert "Vaillant energy poll: 2 request(s)" in caplog.text

"""The Tuya source: fetch windows, cursors, progress, failures and its parameters."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from jigkit.sinks import Point
from pydantic import ValidationError

from ingest_hub.adapters.tuya.client import Device, Report, TuyaError
from ingest_hub.adapters.tuya.source import (
    DAY_MS,
    HOUR_MS,
    MINUTE_MS,
    STATE_FILE,
    TuyaParams,
    TuyaSource,
    _State,
)
from ingest_hub.collect import PROGRESS_TABLE, SourceContext

NOW_MS = 1_800_000_000_000
SPEC = [
    {"code": "add_ele", "type": "Integer", "values": '{"scale":3}'},
    {"code": "cur_voltage", "type": "Integer", "values": '{"scale":1}'},
]


class FakeClient:
    """Serves devices, specifications and canned reports, recording each report query."""

    def __init__(self, devices: list[Device], reports: dict[str, list[Report]] | None = None):
        self.devices_list = devices
        self.reports_by_device = reports or {}
        self.queries: list[tuple[str, list[str], int, int]] = []
        self.failing: set[str] = set()
        self.discoveries = 0

    async def devices(self) -> list[Device]:
        self.discoveries += 1
        return self.devices_list

    async def specification(self, device_id: str) -> list[dict]:
        if device_id == "spec-fails":
            raise TuyaError("spec unavailable", 1106)
        return [] if device_id == "no-spec" else SPEC

    async def reports(self, device_id, codes, start_ms, end_ms):
        self.queries.append((device_id, codes, start_ms, end_ms))
        if device_id in self.failing:
            raise TuyaError("boom", 1106)
        return [r for r in self.reports_by_device.get(device_id, []) if start_ms <= r.event_time]


class Harness:
    """A real SourceContext whose emit records batches and answers as told."""

    def __init__(self, tmp_path: Path, now_ms: int = NOW_MS) -> None:
        self.now_ms = now_ms
        self.batches: list[list[Point]] = []
        self.accept = True
        self.problems: list[str] = []
        self.successes = 0

        async def emit(points: list[Point]) -> bool:
            self.batches.append(points)
            return self.accept

        def on_success() -> None:
            self.successes += 1

        self.context = SourceContext(
            "plugs",
            tmp_path,
            emit,
            on_success=on_success,
            on_problem=self.problems.append,
            clock=lambda: self.now_ms / 1000,
        )

    @property
    def state_path(self) -> Path:
        return self.context.state_dir / STATE_FILE

    def progress(self) -> list[tuple[str, int]]:
        return [
            (dict(p.tags)["stream"], p.fields["complete_until"])
            for batch in self.batches
            for p in batch
            if p.table == PROGRESS_TABLE
        ]


def params(**overrides) -> TuyaParams:
    values = {"access_id_file": "/run/id", "access_secret_file": "/run/secret", **overrides}
    return TuyaParams.model_validate(values)


PLUG = Device("plug-1", "Kitchen", "cz", True)
OFFLINE = Device("plug-2", "Shed", "cz", False)


async def poll(source: TuyaSource, harness: Harness, state: _State) -> None:
    await source.poll(harness.context, state, harness.state_path)


async def test_first_poll_fetches_the_retention_window_and_publishes_progress(tmp_path):
    reports = {
        "plug-1": [
            Report("add_ele", "70", NOW_MS - DAY_MS),
            Report("cur_voltage", "2300", NOW_MS - 1000),
        ]
    }
    client = FakeClient([PLUG], reports)
    harness = Harness(tmp_path)
    state = _State()
    await poll(TuyaSource(params(), client), harness, state)

    ((device, codes, start, end),) = client.queries
    assert device == "plug-1" and codes == ["add_ele", "cur_voltage"]
    assert start == NOW_MS - 7 * DAY_MS and end == NOW_MS
    (batch,) = harness.batches
    data = [p for p in batch if p.table == "tuya_cz"]
    assert [p.fields for p in data] == [
        {"add_ele": 70, "name": "Kitchen"},
        {"cur_voltage": 230.0, "name": "Kitchen"},
    ]
    # Progress travels in the same batch as the points it covers, a lag behind the poll.
    assert harness.progress() == [("plug-1", NOW_MS - HOUR_MS)]
    assert state.cursors == {"plug-1": NOW_MS - HOUR_MS}
    saved = json.loads(harness.state_path.read_text())
    assert saved["cursors"] == {"plug-1": NOW_MS - HOUR_MS}


async def test_later_polls_start_an_overlap_before_the_cursor(tmp_path):
    client = FakeClient([PLUG])
    harness = Harness(tmp_path)
    cursor = NOW_MS - 2 * HOUR_MS
    state = _State({"plug-1": cursor}, last_rescan_ms=NOW_MS)
    await poll(TuyaSource(params(), client), harness, state)
    assert client.queries[0][2] == cursor - 120 * MINUTE_MS
    assert state.cursors["plug-1"] == NOW_MS - HOUR_MS


async def test_a_device_that_reported_nothing_still_advances(tmp_path):
    client = FakeClient([OFFLINE])
    harness = Harness(tmp_path)
    state = _State({"plug-2": NOW_MS - 3 * HOUR_MS}, last_rescan_ms=NOW_MS)
    await poll(TuyaSource(params(), client), harness, state)
    (batch,) = harness.batches
    assert [p.table for p in batch] == [PROGRESS_TABLE]
    assert harness.progress() == [("plug-2", NOW_MS - HOUR_MS)]


async def test_progress_never_moves_backwards(tmp_path):
    # A cursor ahead of now minus the lag, as after the clock stepped back.
    cursor = NOW_MS - 10 * MINUTE_MS
    harness = Harness(tmp_path)
    state = _State({"plug-1": cursor}, last_rescan_ms=NOW_MS)
    await poll(TuyaSource(params(), FakeClient([PLUG])), harness, state)
    assert harness.progress() == [("plug-1", cursor)]
    assert state.cursors["plug-1"] == cursor


async def test_the_daily_rescan_reaches_further_back_then_waits_a_day(tmp_path):
    client = FakeClient([PLUG])
    harness = Harness(tmp_path)
    cursor = NOW_MS - HOUR_MS
    state = _State({"plug-1": cursor}, last_rescan_ms=NOW_MS - 25 * HOUR_MS)
    source = TuyaSource(params(), client)
    await poll(source, harness, state)
    assert client.queries[-1][2] == cursor - 48 * HOUR_MS
    assert state.last_rescan_ms == NOW_MS
    assert json.loads(harness.state_path.read_text())["last_rescan_ms"] == NOW_MS

    # The rescan moved the cursor to an hour before the poll; the next poll, a quarter of an
    # hour later and within the day, goes back only the ordinary overlap from there.
    assert state.cursors["plug-1"] == NOW_MS - HOUR_MS
    harness.now_ms += 15 * MINUTE_MS
    await poll(source, harness, state)
    assert client.queries[-1][2] == NOW_MS - HOUR_MS - 120 * MINUTE_MS


async def test_the_window_never_reaches_past_retention(tmp_path):
    client = FakeClient([PLUG])
    state = _State({"plug-1": NOW_MS - 30 * DAY_MS}, last_rescan_ms=NOW_MS)
    await poll(TuyaSource(params(), client), Harness(tmp_path), state)
    assert client.queries[0][2] == NOW_MS - 7 * DAY_MS


async def test_a_refused_batch_leaves_the_cursor_where_it_was(tmp_path):
    harness = Harness(tmp_path)
    harness.accept = False
    cursor = NOW_MS - 2 * HOUR_MS
    state = _State({"plug-1": cursor}, last_rescan_ms=NOW_MS)
    await poll(TuyaSource(params(), FakeClient([PLUG])), harness, state)
    assert state.cursors["plug-1"] == cursor
    assert not harness.state_path.exists()
    assert any("refused" in p for p in harness.problems)


async def test_one_failing_device_does_not_stop_the_others(tmp_path):
    client = FakeClient([PLUG, OFFLINE])
    client.failing = {"plug-1"}
    harness = Harness(tmp_path)
    state = _State(last_rescan_ms=0)
    await poll(TuyaSource(params(), client), harness, state)
    assert set(state.cursors) == {"plug-2"}
    assert any("plug-1" in p for p in harness.problems)
    # A rescan with a failed device is not recorded as done.
    assert state.last_rescan_ms == 0


async def test_a_poll_where_every_device_fails_raises(tmp_path):
    client = FakeClient([PLUG, Device("no-spec", "x", "cz", True)])
    client.failing = {"plug-1"}
    with pytest.raises(TuyaError, match="every device failed"):
        await poll(TuyaSource(params(), client), Harness(tmp_path), _State())


async def test_devices_can_be_limited_and_are_rediscovered_periodically(tmp_path):
    client = FakeClient([PLUG, OFFLINE])
    harness = Harness(tmp_path)
    source = TuyaSource(params(devices=["plug-2"], discover_hours=6), client)
    state = _State()
    await poll(source, harness, state)
    assert [q[0] for q in client.queries] == ["plug-2"]
    harness.now_ms += 5 * HOUR_MS
    await poll(source, harness, state)
    assert client.discoveries == 1
    harness.now_ms += HOUR_MS
    await poll(source, harness, state)
    assert client.discoveries == 2


async def test_a_custom_table_prefix_and_unscaled_codes(tmp_path):
    reports = {"plug-1": [Report("cur_voltage", "2300", NOW_MS - 1)]}
    harness = Harness(tmp_path)
    source = TuyaSource(
        params(table_prefix="plug_", unscaled_codes=["cur_voltage"]), FakeClient([PLUG], reports)
    )
    await poll(source, harness, _State())
    point = harness.batches[0][0]
    assert point.table == "plug_cz" and point.fields["cur_voltage"] == 2300


async def test_run_polls_reports_success_and_sleeps(tmp_path):
    harness = Harness(tmp_path)
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        raise SystemExit  # stop after the first round

    harness.context.sleep = sleep
    with pytest.raises(SystemExit):
        await TuyaSource(params(poll_minutes=10), FakeClient([PLUG])).run(harness.context)
    assert harness.successes == 1 and sleeps == [600]
    assert harness.state_path.exists()


def test_state_survives_a_round_trip_and_a_bad_file(tmp_path, caplog):
    path = tmp_path / STATE_FILE
    _State({"a": 1}, 5).save(path)
    assert _State.load(path) == _State({"a": 1}, 5)
    path.write_text("{not json")
    assert _State.load(path) == _State()
    assert "starting without cursors" in caplog.text
    assert _State.load(tmp_path / "missing.json") == _State()


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"region": "mars"}, "unknown region"),
        ({"table_prefix": "1x"}, "start with a letter"),
        ({"table_prefix": "a-b"}, "start with a letter"),
        ({"overlap_minutes": 30, "lag_minutes": 60}, "at least lag_minutes"),
        ({"surprise": 1}, "Extra inputs"),
    ],
)
def test_params_are_validated(overrides, message):
    with pytest.raises(ValidationError, match=message):
        params(**overrides)


def test_params_url_follows_region_unless_overridden():
    assert params(region="us").url == "https://openapi.tuyaus.com"
    assert params(base_url="http://stub").url == "http://stub"


async def test_colliding_reports_are_reported_as_a_problem(tmp_path):
    reports = {"plug-1": [Report("add_ele", "5", NOW_MS - 1), Report("add_ele", "6", NOW_MS - 1)]}
    harness = Harness(tmp_path)
    await poll(TuyaSource(params(), FakeClient([PLUG], reports)), harness, _State())
    assert any("1 colliding report(s)" in p for p in harness.problems)


async def test_a_device_whose_specification_fails_does_not_stop_discovery(tmp_path):
    client = FakeClient([Device("spec-fails", "x", "cz", True), PLUG])
    harness = Harness(tmp_path)
    state = _State()
    await poll(TuyaSource(params(), client), harness, state)
    assert set(state.cursors) == {"plug-1"}
    assert any("spec-fails" in p and "no status codes" in p for p in harness.problems)

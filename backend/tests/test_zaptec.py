"""The Zaptec source, against a stand-in bridge and constants endpoint with synthetic data."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import httpx2
import pytest
from jigkit.sinks import Point
from jigkit.testing.http import Received, StubServer, serving
from pydantic import ValidationError

from ingest_hub.adapters import ADAPTERS
from ingest_hub.adapters import zaptec as zaptec_module
from ingest_hub.adapters.zaptec import NAMES_FILE, STATE_FILE, ZaptecParams, ZaptecSource, snake
from ingest_hub.collect import SourceContext

NOW = dt.datetime(2026, 11, 7, 12, 0, tzinfo=dt.UTC).timestamp()
CHARGER = "charger-uuid"
CONSTANTS = {
    "Observations": {
        "TotalChargePower": 513,
        "ChargerOperationMode": 710,
        "TemperatureInternal5": 201,
        "Capabilities": 100,
        "IsOnline": -2,
    }
}
STATE = {
    "charger_id": CHARGER,
    "age_seconds": 12.8,
    "observations": {
        "513": "1234.5",
        "710": "3",
        "201": "24.266",
        "100": '{"DeviceType":"Go"}',
        "-2": "1",
        "-1": "",
        "999": "a-firmware-name",
        "554": "x" * 300,
        "160": "NaN",
    },
}


def session(sid: str, end: str, **extra) -> dict:
    return {
        "id": sid,
        "chargerId": CHARGER,
        "startDateTime": "2026-11-06T18:00:00Z",
        "endDateTime": end,
        "energy": 12.5,
        "aborted": False,
        "voided": False,
        "offline": False,
        "stoppedByRfid": True,
        "signed": True,
        "reliableClock": True,
        "authorizedUser": {"email": "someone@example.org", "fullName": "Some One"},
        "energyDetails": [
            {"timestamp": "2026-11-06T18:00:00Z", "energy": 0.0},
            {"timestamp": "2026-11-06T19:00:00Z", "energy": 7.2},
            {"timestamp": None, "energy": 1.0},
        ],
        **extra,
    }


class Bridge:
    """Answers like the bridge (state and paged sessions) and like Zaptec's constants."""

    def __init__(self) -> None:
        self.state: tuple[int, str] = (200, json.dumps(STATE))
        self.constants: tuple[int, str] = (200, json.dumps(CONSTANTS))
        self.pages: list[dict] = [{"sessions": [], "cursor": None, "hasMore": False}]
        self.sessions_status = 200
        self.requests: list[Received] = []

    def __call__(self, request: Received) -> tuple[int, str]:
        self.requests.append(request)
        if request.path == "/api/constants":
            return self.constants
        if request.path == "/state.json":
            return self.state
        if request.path == "/sessions":
            if self.sessions_status != 200:
                return self.sessions_status, "Zaptec unavailable"
            index = int(request.query.get("cursor", "0"))
            return 200, json.dumps(self.pages[index])
        return 404, "not found"


class Harness:
    def __init__(self, tmp_path: Path) -> None:
        self.batches: list[list[Point]] = []
        self.problems: list[str] = []
        self.sleeps: list[float] = []
        self.successes = 0
        self.accept = True
        self.now = NOW
        self.stop_after = 1

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
            "charger",
            tmp_path,
            emit,
            on_success=succeeded,
            on_problem=self.problems.append,
            clock=lambda: self.now,
            sleep=sleep,
        )


class Stop(Exception):
    pass


async def run(tmp_path, bridge: Bridge, harness: Harness | None = None, **params) -> Harness:
    harness = harness or Harness(tmp_path)
    async with serving(StubServer(respond=bridge)) as server:
        values = {
            "bridge_url": server.url + "/",
            "constants_url": server.url + "/api/constants",
            **params,
        }
        async with httpx2.AsyncClient() as client:
            source = ZaptecSource(ZaptecParams.model_validate(values), client)
            with pytest.raises(Stop):
                await source.run(harness.context)
    return harness


async def test_state_fields_are_named_after_zaptec_and_never_change_type(tmp_path):
    bridge = Bridge()
    harness = await run(tmp_path, bridge)
    (state,) = [p for b in harness.batches for p in b if p.table == "zaptec_state"]
    assert state.tags == (("charger", CHARGER),) and state.timestamp == int(NOW)
    assert state.fields == {
        "total_charge_power": 1234.5,
        "charger_operation_mode": 3.0,
        "temperature_internal5": 24.266,
        "capabilities_text": '{"DeviceType":"Go"}',
        "is_online": 1.0,
        "observation_999_text": "a-firmware-name",
        "observation_160_text": "NaN",
    }
    assert json.loads((tmp_path / NAMES_FILE).read_text())["513"] == "total_charge_power"
    assert harness.successes == 1 and harness.problems == []


async def test_names_kept_from_before_are_used_when_zaptec_cannot_be_reached(tmp_path, caplog):
    (tmp_path / NAMES_FILE).write_text(json.dumps({"513": "total_charge_power"}))
    bridge = Bridge()
    bridge.constants = (502, "bad gateway")
    harness = await run(tmp_path, bridge)
    state = harness.batches[0][0]
    assert state.fields["total_charge_power"] == 1234.5 and "observation_710" in state.fields
    assert "cannot fetch Zaptec's observation names" in caplog.text


async def test_without_any_names_observations_are_named_by_id(tmp_path):
    (tmp_path / NAMES_FILE).write_text("{broken")
    bridge = Bridge()
    bridge.constants = (200, json.dumps({"NoObservations": {}}))
    harness = await run(tmp_path, bridge)
    fields = harness.batches[0][0].fields
    assert fields["observation_513"] == 1234.5 and fields["observation_m2"] == 1.0


async def test_an_offline_charger_is_reported_and_the_sessions_still_read(tmp_path):
    bridge = Bridge()
    bridge.state = (503, "charger offline")
    harness = await run(tmp_path, bridge)
    assert any("poll_state" in p and "503" in p for p in harness.problems)
    assert harness.successes == 0
    assert any(r.path == "/sessions" for r in bridge.requests)


async def test_sessions_are_read_page_by_page_from_history_then_from_the_latest_end(tmp_path):
    bridge = Bridge()
    bridge.pages = [
        {"sessions": [session("s1", "2026-11-06T20:00:00Z")], "cursor": "1", "hasMore": True},
        {
            "sessions": [
                session("s2", "2026-11-07T08:30:00+00:00", energyDetails=None),
                {"id": "open", "endDateTime": None},
            ],
            "cursor": None,
            "hasMore": False,
        },
    ]
    harness = await run(tmp_path, bridge, history_days=30)
    first = [r for r in bridge.requests if r.path == "/sessions"]
    assert first[0].query["from"] == "2026-10-08T12:00:00Z"
    assert first[0].query["to"] == "2026-11-07T12:00:00Z" and first[0].query["pageSize"] == "200"
    assert first[1].query["cursor"] == "1"
    sessions = [p for b in harness.batches for p in b if p.table == "zaptec_session"]
    assert [(p.timestamp, p.fields["id"]) for p in sessions] == [
        (int(dt.datetime(2026, 11, 6, 20, tzinfo=dt.UTC).timestamp()), "s1"),
        (int(dt.datetime(2026, 11, 7, 8, 30, tzinfo=dt.UTC).timestamp()), "s2"),
    ]
    assert sessions[0].fields == {
        "id": "s1",
        "energy": 12.5,
        "start_time": "2026-11-06T18:00:00+00:00",
        "duration_seconds": 7200.0,
        "aborted": False,
        "voided": False,
        "offline": False,
        "stopped_by_rfid": True,
        "signed": True,
        "reliable_clock": True,
    }
    assert "someone@example.org" not in repr(harness.batches)
    energy = [p for b in harness.batches for p in b if p.table == "zaptec_session_energy"]
    assert [(dict(p.tags)["session"], p.fields["energy"]) for p in energy] == [
        ("s1", 0.0),
        ("s1", 7.2),
    ]
    saved = json.loads((tmp_path / STATE_FILE).read_text())
    assert saved == {"sessions_until": "2026-11-07T08:30:00+00:00"}

    # The next read starts a day before the latest end.
    bridge.requests.clear()
    bridge.pages = [{"sessions": [], "cursor": None, "hasMore": False}]
    await run(tmp_path, bridge)
    later = [r for r in bridge.requests if r.path == "/sessions"]
    assert later[0].query["from"] == "2026-11-06T08:30:00Z"


async def test_a_session_without_an_id_or_figures_gives_what_it_has(tmp_path):
    bridge = Bridge()
    bridge.pages = [
        {
            "sessions": [
                {"endDateTime": "2026-11-07T01:00:00Z"},
                {"id": "bare", "endDateTime": "2026-11-07T02:00:00Z"},
            ],
            "cursor": None,
            "hasMore": False,
        }
    ]
    harness = await run(tmp_path, bridge)
    (bare,) = [p for b in harness.batches for p in b if p.table == "zaptec_session"]
    assert bare.fields == {"id": "bare"} and bare.tags == (("charger", "unknown"),)


async def test_an_unreadable_saved_position_reads_from_history(tmp_path, caplog):
    (tmp_path / STATE_FILE).write_text("{oops")
    bridge = Bridge()
    await run(tmp_path, bridge, history_days=1)
    first = next(r for r in bridge.requests if r.path == "/sessions")
    assert first.query["from"] == "2026-11-06T12:00:00Z"
    assert "reading sessions from the start" in caplog.text


async def test_a_sessions_failure_is_reported_and_retried_on_schedule(tmp_path):
    bridge = Bridge()
    bridge.sessions_status = 503
    harness = Harness(tmp_path)
    harness.stop_after = 2
    harness = await run(tmp_path, bridge, harness, state_seconds=60, sessions_minutes=1)
    assert sum("poll_sessions" in p for p in harness.problems) == 2
    assert not (tmp_path / STATE_FILE).exists()


async def test_refused_batches_are_reported(tmp_path):
    bridge = Bridge()
    bridge.pages = [
        {"sessions": [session("s1", "2026-11-06T20:00:00Z")], "cursor": None, "hasMore": False}
    ]
    harness = Harness(tmp_path)
    harness.accept = False
    await run(tmp_path, bridge, harness)
    assert any("state batch was refused" in p for p in harness.problems)
    assert any("session batch was refused" in p for p in harness.problems)
    assert not (tmp_path / STATE_FILE).exists()


async def test_the_schedule_reads_state_more_often_than_sessions(tmp_path):
    bridge = Bridge()
    harness = Harness(tmp_path)
    harness.stop_after = 3
    harness = await run(tmp_path, bridge, harness, state_seconds=60, sessions_minutes=2)
    paths = [r.path for r in bridge.requests if r.path != "/api/constants"]
    assert paths == ["/state.json", "/sessions", "/state.json", "/state.json", "/sessions"]
    assert harness.sleeps == [60.0, 60.0, 60.0]
    # Names are fetched once a day, not every round.
    assert sum(r.path == "/api/constants" for r in bridge.requests) == 1


async def test_an_empty_state_writes_nothing(tmp_path):
    bridge = Bridge()
    bridge.state = (200, json.dumps({"charger_id": CHARGER, "observations": {"1": " "}}))
    harness = await run(tmp_path, bridge)
    assert [p.table for b in harness.batches for p in b] == []


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("TotalChargePower", "total_charge_power"),
        ("IsOnline", "is_online"),
        ("TemperatureInternal5", "temperature_internal5"),
        ("OCPPVersion", "ocpp_version"),
    ],
)
def test_names_in_snake_case(name, expected):
    assert snake(name) == expected


def test_params_and_registration():
    adapter = ADAPTERS["zaptec"]
    params = adapter.params.model_validate({"bridge_url": "http://bridge:8080"})
    assert params.state_seconds == 60 and params.constants_url == zaptec_module.CONSTANTS_URL
    assert isinstance(adapter.build(params, None, "charger"), ZaptecSource)
    with pytest.raises(ValidationError, match="bridge_url"):
        ZaptecParams.model_validate({})


async def test_sessions_can_fall_due_without_the_state(tmp_path):
    bridge = Bridge()
    harness = Harness(tmp_path)
    harness.stop_after = 2
    await run(tmp_path, bridge, harness, state_seconds=600, sessions_minutes=1)
    paths = [r.path for r in bridge.requests if r.path != "/api/constants"]
    assert paths == ["/state.json", "/sessions", "/sessions"]


async def test_a_failed_daily_refresh_keeps_the_names_it_has(tmp_path):
    bridge = Bridge()
    harness = Harness(tmp_path)
    async with serving(StubServer(respond=bridge)) as server:
        params = ZaptecParams(bridge_url=server.url, constants_url=server.url + "/api/constants")
        async with httpx2.AsyncClient() as client:
            source = ZaptecSource(params, client)
            await source.poll_state(harness.context)
            bridge.constants = (500, "down")
            harness.now += zaptec_module.NAMES_REFRESH
            (tmp_path / NAMES_FILE).unlink()  # only the names already in memory remain
            await source.poll_state(harness.context)
    first, second = (b[0] for b in harness.batches)
    assert second.fields == first.fields
    assert sum(r.path == "/api/constants" for r in bridge.requests) == 2


async def test_a_timeout_is_reported_by_its_type(tmp_path):
    class TimingOut:
        async def get(self, *args, **kwargs):
            raise httpx2.ReadTimeout("")

    harness = Harness(tmp_path)
    source = ZaptecSource(ZaptecParams(bridge_url="http://bridge"), TimingOut())
    with pytest.raises(Stop):
        await source.run(harness.context)
    assert harness.problems == ["poll_state: ReadTimeout", "poll_sessions: ReadTimeout"]

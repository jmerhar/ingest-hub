"""Polling the Google Health API, and consenting to it, against a stand-in Google."""

from __future__ import annotations

import datetime as dt
import json
import urllib.parse
from collections import Counter
from pathlib import Path

import httpx2
import pytest
from jigkit.sinks import Point
from jigkit.testing.http import StubServer, serving
from pydantic import ValidationError

from ingest_hub import cli
from ingest_hub.adapters import ADAPTERS
from ingest_hub.adapters.google import health, oauth
from ingest_hub.adapters.google.health import GoogleHealthParams, GoogleHealthSource
from ingest_hub.collect import SourceContext
from ingest_hub.config import ConfigError
from ingest_hub.settings import Settings

from .google_api_fake import CLIENT_ID, CLIENT_SECRET, CODE, REFRESH, SCOPES, FakeGoogle

#: Noon on 12 Nov 2026, UTC.
NOW = dt.datetime(2026, 11, 12, 12, tzinfo=dt.UTC).timestamp()


def utc(*parts: int) -> int:
    return int(dt.datetime(*parts, tzinfo=dt.UTC).timestamp() * 1000)


def secrets(tmp_path: Path) -> tuple[Path, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "client_id").write_text(CLIENT_ID)
    (tmp_path / "client_secret").write_text(CLIENT_SECRET + "\n")
    return tmp_path / "client_id", tmp_path / "client_secret"


class Stop(Exception):
    """Ends a source's run loop in a test."""


class Harness:
    def __init__(self, tmp_path: Path, server: StubServer, **overrides) -> None:
        client_id, client_secret = secrets(tmp_path / "secrets")
        self.params = GoogleHealthParams.model_validate(
            {
                "person": "alex",
                "client_id_file": str(client_id),
                "client_secret_file": str(client_secret),
                "timezone": "Europe/Amsterdam",
                "history_days": 3,
                "url": server.url + "/v4",
                "token_url": server.url + "/token",
                **overrides,
            }
        )
        self.state = tmp_path / "state"
        oauth.save_token(self.state, REFRESH, SCOPES.split(), NOW)
        self.now = NOW
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
            if seconds >= 60:
                raise Stop

        def succeeded() -> None:
            self.successes += 1

        self.context = SourceContext(
            "google_alex",
            self.state,
            emit,
            on_success=succeeded,
            on_problem=self.problems.append,
            clock=lambda: self.now,
            sleep=sleep,
        )
        self.http = httpx2.AsyncClient()
        self.source = health.build(self.params, self.http, "google_alex")

    @property
    def points(self) -> list[Point]:
        return [p for batch in self.batches for p in batch]

    def take(self) -> list[Point]:
        points = self.points
        self.batches.clear()
        return points


def table(points: list[Point], name: str) -> list[Point]:
    return [p for p in points if p.table == "google_" + name]


def tags(point: Point) -> dict[str, str]:
    found = dict(point.tags)
    assert found.pop("person") == "alex"
    return found


async def test_a_poll_writes_every_data_type_like_the_export(tmp_path, caplog):
    caplog.set_level("INFO")
    async with serving(StubServer(respond=FakeGoogle())) as server:
        harness = Harness(tmp_path, server)
        await harness.source.poll(harness.context)
        points = harness.points
    assert Counter(p.table for p in points) == {
        "google_sleep": 2,
        "google_sleep_stage": 2,
        "google_daily_heart_rate_variability": 1,
        "google_daily_resting_heart_rate": 1,
        "google_daily_vo2_max": 1,
        "google_oxygen_saturation": 1,
        "google_respiratory_rate_sleep_summary": 1,
        "google_weight": 1,
        "google_steps": 3,
    }

    (hrv,) = table(points, "daily_heart_rate_variability")
    assert tags(hrv) == {"date": "2026-11-11", "night": "2026-11-10"}
    assert hrv.timestamp == utc(2026, 11, 10, 11)  # noon of the night, in Amsterdam
    assert hrv.fields == {  # 64-bit integers come as text; the 0 is Google's "none"
        "average_heart_rate_variability_milliseconds": 35.5,
        "non_rem_heart_rate_beats_per_minute": 58.0,
        "entropy": 2.2,
    }
    (rhr,) = table(points, "daily_resting_heart_rate")
    assert tags(rhr) == {"date": "2026-11-11"} and rhr.timestamp == utc(2026, 11, 10, 23)
    (vo2,) = table(points, "daily_vo2_max")
    assert vo2.fields == {"vo2_max": 48.5, "estimated": True, "cardio_fitness_level": "GOOD"}

    (spo2,) = table(points, "oxygen_saturation")  # the 0 and the unreadable time are left out
    assert tags(spo2) == {"night": "2026-11-10"} and spo2.timestamp == utc(2026, 11, 11, 1, 8)
    assert spo2.fields == {"oxygen_saturation_percentage": 97.3}
    (summary,) = table(points, "respiratory_rate_sleep_summary")
    assert tags(summary) == {"night": "2026-11-10"}
    assert summary.fields == {
        "deep_sleep_stats_breaths_per_minute": 13.0,
        "deep_sleep_stats_standard_deviation_breaths_per_minute": 1.2,
        "deep_sleep_stats_signal_to_noise": 4.7,
        "full_sleep_stats_breaths_per_minute": 13.5,
        "full_sleep_stats_standard_deviation_breaths_per_minute": 1.0,
        "full_sleep_stats_signal_to_noise": 5.0,
    }
    (weight,) = table(points, "weight")
    assert tags(weight) == {} and weight.fields == {"weight_grams": 72500.0}
    steps = [(tags(p)["source"], p.timestamp, p.fields["steps"]) for p in table(points, "steps")]
    assert steps == [
        ("Pixel Watch 2", utc(2026, 11, 11, 8, 2), 6.0),
        ("MobileTrack", utc(2026, 11, 11, 8, 2), 9.0),
        ("unknown", utc(2026, 11, 11, 8, 3), 0.0),
    ]

    main, nap = table(points, "sleep")
    assert (main.timestamp, tags(main), main.fields) == (
        utc(2026, 11, 10, 22, 30),
        {"night": "2026-11-10"},
        {
            "main": True,
            "minutes_in_sleep_period": 450.0,
            "minutes_asleep": 420.0,
            "type": "STAGES",
            "data_source": "DERIVED",
        },
    )
    # A classic nap of the same morning is not the main sleep even with stages elsewhere.
    assert tags(nap) == {"night": "2026-11-10"} and nap.fields["main"] is False
    stages = [(p.timestamp, tags(p)["stage"], p.fields) for p in table(points, "sleep_stage")]
    assert stages == [
        (utc(2026, 11, 10, 22, 30), "light", {"seconds": 810.0}),
        (utc(2026, 11, 10, 22, 43, 30), "deep", {"seconds": 2790.0}),
    ]
    assert "1 sleep(s) still being processed" in caplog.text
    assert "google_alex: " in caplog.text and "request(s)" in caplog.text


async def test_each_poll_reads_again_behind_its_cursor(tmp_path):
    fake = FakeGoogle()
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server, rescan_hours=24)
        await harness.source.poll(harness.context)
        first = list(fake.filters)
        fake.filters.clear()
        harness.now += 3600
        await harness.source.poll(harness.context)
    # The first poll reads history_days (3) back, in chunks of a week at most.
    assert (
        "sleep",
        'sleep.interval.end_time >= "2026-11-09T12:00:00Z" AND '
        'sleep.interval.end_time < "2026-11-12T12:00:00Z"',
    ) in first
    assert (
        "daily-resting-heart-rate",
        'daily_resting_heart_rate.date >= "2026-11-09" AND '
        'daily_resting_heart_rate.date < "2026-11-13"',
    ) in first
    # The next reads from its cursor less rescan_hours, a day.
    assert (
        "sleep",
        'sleep.interval.end_time >= "2026-11-11T12:00:00Z" AND '
        'sleep.interval.end_time < "2026-11-12T13:00:00Z"',
    ) in fake.filters
    assert (
        "daily-resting-heart-rate",
        'daily_resting_heart_rate.date >= "2026-11-11" AND '
        'daily_resting_heart_rate.date < "2026-11-13"',
    ) in fake.filters
    state = json.loads((harness.state / health.STATE_FILE).read_text())
    assert state["cursors"]["daily-resting-heart-rate"] == "2026-11-12"
    assert state["cursors"]["sleep"] == str(utc(2026, 11, 12, 13))
    assert sorted(state["sleeps"]) == ["100", "101"]


async def test_heart_rate_is_written_per_minute_when_asked_for(tmp_path):
    async with serving(StubServer(respond=FakeGoogle())) as server:
        harness = Harness(tmp_path, server, heart_rate_minutes=True)
        await harness.source.poll(harness.context)
    minutes = [(p.timestamp, tags(p), p.fields) for p in table(harness.points, "heart_rate_minute")]
    assert minutes == [
        (
            utc(2026, 11, 11, 1),
            {"source": "Pixel Watch 2"},
            {"mean": 52.0, "min": 50.0, "max": 54.0, "count": 2.0},
        ),
        (
            utc(2026, 11, 11, 1, 1),
            {"source": "Pixel Watch 2"},
            {"mean": 60.0, "min": 60.0, "max": 60.0, "count": 1.0},
        ),
    ]


def session(sleep_id: str, start: str, end: str, minutes: int) -> dict:
    return {
        "name": f"users/1/dataTypes/sleep/dataPoints/{sleep_id}",
        "dataSource": {},
        "sleep": {
            "interval": {"startTime": start, "endTime": end},
            "type": "STAGES",
            "metadata": {"processed": True},
            "summary": {"minutesInSleepPeriod": str(minutes)},
        },
    }


async def test_a_longer_session_found_later_takes_over_as_main(tmp_path):
    fake = FakeGoogle()
    fake.data["sleep"] = [session("200", "2026-11-11T22:00:00Z", "2026-11-12T05:00:00Z", 420)]
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server, rescan_hours=0)
        await harness.source.poll(harness.context)
        assert table(harness.take(), "sleep")[0].fields["main"] is True
        # A longer sleep of the same night ends after the first poll; the first is not read
        # again, so only its ``main`` is written anew.
        fake.data["sleep"].append(
            session("201", "2026-11-12T10:30:00Z", "2026-11-12T19:30:00Z", 540)
        )
        harness.now += 8 * 3600
        await harness.source.poll(harness.context)
    updates = [(p.timestamp, p.fields) for p in table(harness.points, "sleep")]
    assert updates == [
        (
            utc(2026, 11, 12, 10, 30),
            {"main": True, "minutes_in_sleep_period": 540.0, "type": "STAGES"},
        ),
        (utc(2026, 11, 11, 22), {"main": False}),
    ]


async def test_overlapping_sessions_and_unchanged_nights(tmp_path):
    fake = FakeGoogle()
    untyped = session("302", "2026-11-12T09:00:00Z", "2026-11-12T09:30:00Z", 30)
    untyped["sleep"].pop("type")
    fake.data["sleep"] = [
        session("300", "2026-11-11T22:30:00Z", "2026-11-12T05:00:00Z", 390),
        session("301", "2026-11-11T23:00:00Z", "2026-11-12T04:00:00Z", 300),  # inside it
        untyped,
    ]
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server, rescan_hours=0)
        await harness.source.poll(harness.context)
        first = table(harness.take(), "sleep")
        assert [(p.timestamp, p.fields["main"]) for p in first] == [
            (utc(2026, 11, 11, 22, 30), True),
            (utc(2026, 11, 12, 9), False),
        ]
        assert "type" not in first[1].fields
        # A nap of the same night found later changes no session's place.
        fake.data["sleep"].append(
            session("303", "2026-11-12T10:30:00Z", "2026-11-12T12:30:00Z", 120)
        )
        harness.now += 3600
        await harness.source.poll(harness.context)
    assert [(p.timestamp, p.fields["main"]) for p in table(harness.points, "sleep")] == [
        (utc(2026, 11, 12, 10, 30), False)
    ]


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        (
            "no token",
            "not signed in to Google yet: run `ingest-hub google-auth --source google_alex`",
        ),
        ("refused", "Google refused the sign-in (withdrawn, expired or unused): run `ingest-hub"),
        ("api error", "/users/me/dataTypes/sleep/dataPoints: HTTP 403 MISSING_OAUTH_SCOPE"),
        ("refused batch", "a batch was refused"),
    ],
)
async def test_a_failed_poll_is_reported_and_the_source_carries_on(tmp_path, change, problem):
    fake = FakeGoogle()
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        if change == "no token":
            (harness.state / oauth.TOKEN_FILE).unlink()
        elif change == "refused":
            fake.refresh_ok = False
        elif change == "api error":
            body = {"error": {"code": 403, "details": [{"reason": "MISSING_OAUTH_SCOPE"}]}}
            fake.failures = [(403, json.dumps(body))]
        else:
            harness.accept = False
        with pytest.raises(Stop):
            await harness.source.run(harness.context)
    assert harness.successes == 0 and harness.sleeps[-1] == 3600
    assert len(harness.problems) == 1 and harness.problems[0].startswith(problem)
    if change == "refused batch":
        cursors = json.loads((harness.state / health.STATE_FILE).read_text())["cursors"]
        assert "sleep" not in cursors  # the refused points are read again next time


async def test_a_good_poll_is_a_success(tmp_path):
    async with serving(StubServer(respond=FakeGoogle())) as server:
        harness = Harness(tmp_path, server, poll_minutes=30)
        with pytest.raises(Stop):
            await harness.source.run(harness.context)
    assert harness.successes == 1 and harness.problems == [] and harness.sleeps == [1800]


async def test_an_unreadable_state_starts_from_history_days_again(tmp_path, caplog):
    fake = FakeGoogle()
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        (harness.state / health.STATE_FILE).write_text('{"cursors": []}')
        await harness.source.poll(harness.context)
    assert "reading from history_days back" in caplog.text
    assert table(harness.points, "sleep")


def test_data_points_that_cannot_be_read_are_skipped(tmp_path):
    params = GoogleHealthParams(person="alex", client_id_file=tmp_path, client_secret_file=tmp_path)
    points = health.Points(params, health.State())
    sleeps = [
        {
            "name": "users/1/dataTypes/sleep/dataPoints/9",
            "sleep": {"metadata": {"processed": True}},
        },
        {
            "name": "",
            "sleep": {
                "metadata": {"processed": True},
                "interval": {
                    "startTime": "2026-11-10T22:00:00Z",
                    "endTime": "2026-11-11T06:00:00Z",
                },
            },
        },
    ]
    assert points.build(health.DATA_TYPES[0], sleeps) == []
    daily = next(t for t in health.DATA_TYPES if t.name == "daily-resting-heart-rate")
    assert (
        points.build(
            daily, [{"dailyRestingHeartRate": {"date": {"year": 2026}, "beatsPerMinute": "60"}}]
        )
        == []
    )
    assert (
        points.build(daily, [{"dailyRestingHeartRate": {"date": {"year": 2026}}}, {"other": {}}])
        == []
    )
    assert (
        points.heart_rate(
            [
                {
                    "heartRate": {
                        "sampleTime": {"physicalTime": "2026-11-11T01:00:00Z"},
                        "beatsPerMinute": "0",
                    }
                }
            ]
        )
        == []
    )
    stage = {
        "startTime": "2026-11-10T22:10:00Z",
        "endTime": "2026-11-10T22:00:00Z",
        "type": "LIGHT",
    }
    assert (
        points._stages(
            "2026-11-10", [stage, {**stage, "endTime": "2026-11-10T22:20:00Z", "type": " "}]
        )
        == []
    )
    assert points.skipped == 5


@pytest.mark.parametrize(
    ("raw", "positive", "value"),
    [
        ("58", True, 58.0),
        (0, True, None),
        ("0", False, 0.0),
        (True, True, True),
        ("GOOD", True, "GOOD"),
        ("", True, None),
        ([1], True, None),
        (float("nan"), False, None),
        ("x" * 65, True, None),
    ],
)
def test_json_values_become_fields(raw, positive, value):
    assert health._value(raw, positive=positive) == value


def test_the_adapter_is_registered_and_reads_its_credentials(tmp_path):
    client_id, client_secret = secrets(tmp_path)
    adapter = ADAPTERS["google_health"]
    valid = {
        "person": "alex",
        "client_id_file": str(client_id),
        "client_secret_file": str(client_secret),
    }
    built = adapter.build(adapter.params.model_validate(valid), None, "google_alex")
    assert isinstance(built, GoogleHealthSource)
    assert health.credentials_of(adapter.params.model_validate(valid)) == oauth.Credentials(
        CLIENT_ID, CLIENT_SECRET
    )
    with pytest.raises(ValidationError, match="timezone"):
        GoogleHealthParams.model_validate({**valid, "timezone": "Mars/Olympus"})
    with pytest.raises(ValidationError, match="person"):
        GoogleHealthParams.model_validate({**valid, "person": ""})


def write_auth_config(tmp_path: Path, server: StubServer) -> Settings:
    client_id, client_secret = secrets(tmp_path / "secrets")
    (tmp_path / "secrets" / "influx").write_text("token")
    config = tmp_path / "config" / "sources.toml"
    config.parent.mkdir(parents=True)
    config.write_text(
        f"""
[destinations.health]
kind = "influx3"
url = "http://127.0.0.1:9"
database = "health"
token_file = "{tmp_path / "secrets" / "influx"}"

[sources.google_alex]
kind = "google_health"
destination = "health"

[sources.google_alex.params]
person = "alex"
client_id_file = "{client_id}"
client_secret_file = "{client_secret}"
url = "{server.url}/v4"
token_url = "{server.url}/token"

[sources.cpap]
kind = "resmed_sd"
destination = "health"

[sources.cpap.params]
person = "alex"
inbox = "{tmp_path / "inbox"}"
"""
    )
    return Settings(data_dir=tmp_path, config_file=config)


async def test_google_auth_starts_a_consent_and_finishes_it_with_the_address(tmp_path, capsys):
    fake = FakeGoogle()
    async with serving(StubServer(respond=fake)) as server:
        settings = write_auth_config(tmp_path, server)
        assert await cli._google_auth(settings, "google_alex", 8765, None) == 0
        shown = capsys.readouterr().out
        url = next(
            line for line in shown.splitlines() if line.startswith("https://accounts.google.com")
        )
        state = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)["state"][0]
        assert "ingest-hub google-auth --source google_alex --address" in shown
        address = f"http://127.0.0.1:8765/?state={state}&code={CODE}&scope=x"
        assert await cli._google_auth(settings, "google_alex", 8765, address) == 0
        assert "Signed in: the source reads alex's data" in capsys.readouterr().out
        assert oauth.load_token(tmp_path / "state" / "google_alex") == REFRESH

        # A consent that grants less says what is not read.
        fake.granted = SCOPES.split()[0]
        await cli._google_auth(settings, "google_alex", 8765, None)
        state = json.loads((tmp_path / "state" / "google_alex" / oauth.CONSENT_FILE).read_text())[
            "state"
        ]
        await cli._google_auth(
            settings, "google_alex", 8765, f"http://127.0.0.1:8765/?state={state}&code={CODE}"
        )
        assert (
            "Not granted, so not read: health_metrics_and_measurements.readonly"
            in capsys.readouterr().out
        )


async def test_google_auth_refuses_what_it_cannot_do(tmp_path):
    fake = FakeGoogle(linked=False)
    async with serving(StubServer(respond=fake)) as server:
        settings = write_auth_config(tmp_path, server)
        with pytest.raises(ConfigError, match="no source named 'other'"):
            await cli._google_auth(settings, "other", 8765, None)
        with pytest.raises(ConfigError, match="not a Google Health source"):
            await cli._google_auth(settings, "cpap", 8765, None)
        with pytest.raises(oauth.AuthError, match="no consent was started"):
            await cli._google_auth(settings, "google_alex", 8765, "http://127.0.0.1:8765/?code=x")
        await cli._google_auth(settings, "google_alex", 8765, None)
        state = json.loads((tmp_path / "state" / "google_alex" / oauth.CONSENT_FILE).read_text())[
            "state"
        ]
        with pytest.raises(health.ApiError, match="ACCOUNT_NOT_LINKED"):
            await cli._google_auth(
                settings, "google_alex", 8765, f"http://127.0.0.1:8765/?state={state}&code={CODE}"
            )
        assert oauth.load_token(tmp_path / "state" / "google_alex") is None


def test_the_command_reports_a_failed_consent_and_exits_2(tmp_path, monkeypatch, capsys):
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "sources.toml").write_text("[destinations]\n[sources]\n")
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.delenv("CONFIG_FILE", raising=False)
    assert cli.main(["google-auth", "--source", "nothing"]) == 2
    assert "no source named 'nothing'" in capsys.readouterr().err


async def test_a_respiratory_summary_takes_the_night_of_the_sleep_it_ends(tmp_path):
    fake = FakeGoogle()
    # A lie-in until 13:30 local: by its own time the summary would be the next night's.
    fake.data["sleep"] = [session("400", "2026-11-11T01:00:00Z", "2026-11-11T12:30:00Z", 690)]
    summary = fake.data["respiratory-rate-sleep-summary"][0]["respiratoryRateSleepSummary"]
    summary["sampleTime"] = {"physicalTime": "2026-11-11T12:30:00Z", "utcOffset": "3600s"}
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        await harness.source.poll(harness.context)
    (point,) = table(harness.points, "respiratory_rate_sleep_summary")
    assert tags(point) == {"night": "2026-11-10"}

"""Importing Google Takeout exports, with synthetic exports (see ``google_export.py``)."""

from __future__ import annotations

import datetime as dt
import json
import zipfile
from collections import Counter
from pathlib import Path

import pytest
from jigkit.sinks import Point
from pydantic import ValidationError

from ingest_hub.adapters import ADAPTERS
from ingest_hub.adapters.google import takeout
from ingest_hub.adapters.google.takeout import (
    STATE_FILE,
    TakeoutParams,
    TakeoutSource,
    file_kind,
    parse_offset,
    parse_time,
    snake,
    value,
)
from ingest_hub.collect import AcceptsFiles, SourceContext

from .google_export import (
    ACTIVITY,
    SLEEP,
    SLEEPS,
    SLEEPS_HEADER,
    T,
    csv,
    export_files,
    write_split,
    write_zip,
)


def utc(*parts: int) -> int:
    """Unix milliseconds of a UTC time."""
    return int(dt.datetime(*parts, tzinfo=dt.UTC).timestamp() * 1000)


class Stop(Exception):
    """Ends a source's run loop in a test."""


class Harness:
    def __init__(self, tmp_path: Path, **overrides) -> None:
        params = {
            "inbox": str(tmp_path / "inbox"),
            "person": "alex",
            "timezone": "Europe/Amsterdam",
            **overrides,
        }
        self.params = TakeoutParams.model_validate(params)
        self.inbox = self.params.inbox
        self.inbox.mkdir(parents=True, exist_ok=True)
        self.state = tmp_path / "state"
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
            "health",
            self.state,
            emit,
            on_success=succeeded,
            on_problem=self.problems.append,
            sleep=sleep,
        )
        self.source = TakeoutSource(self.params)

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
    """A point's tags but ``person``, which every point has."""
    found = dict(point.tags)
    assert found.pop("person") == "alex"
    return found


EXPECTED = {
    "google_daily_resting_heart_rate": 2,
    "google_daily_heart_rate_variability": 1,
    "google_daily_sleep_temperature_derivations": 1,
    "google_daily_readiness": 1,
    "google_cardio_acute_chronic_workload_ratio": 1,
    "google_respiratory_rate_sleep_summary": 1,
    "google_oxygen_saturation": 1,
    "google_steps": 4,
    "google_weight": 1,
    "google_sleep": 4,
    "google_sleep_stage": 5,
    "google_sleep_score": 3,
    "google_stress_score": 2,
}


async def test_an_export_split_over_two_zips_is_imported_as_one(tmp_path, caplog):
    harness = Harness(tmp_path)
    caplog.set_level("INFO")
    write_split(harness.inbox, export_files())
    assert await harness.source.scan(harness.context)
    points = harness.points
    assert Counter(p.table for p in points) == EXPECTED
    assert {dict(p.tags)["person"] for p in points} == {"alex"}
    assert harness.problems == []
    assert sorted(p.name for p in (harness.inbox / "imported").iterdir()) == [
        "takeout-1-001.zip",
        "takeout-1-002.zip",
    ]
    assert "imported takeout-1-001.zip, takeout-1-002.zip: 13 file(s), 0 unchanged" in caplog.text
    assert "left out 1 sleep session(s) overlapping a longer one" in caplog.text
    assert "skipped 2 row(s) that could not be read" in caplog.text


async def test_daily_values_are_keyed_by_date_and_the_night_before(tmp_path):
    harness = Harness(tmp_path)
    write_zip(harness.inbox / "t.zip", export_files())
    await harness.source.scan(harness.context)
    points = harness.points

    rhr = table(points, "daily_resting_heart_rate")[0]
    assert tags(rhr) == {"date": "2026-11-11"}
    assert rhr.timestamp == utc(2026, 11, 10, 23)  # midnight in Amsterdam
    assert rhr.fields == {"beats_per_minute": 58.252}

    (hrv,) = table(points, "daily_heart_rate_variability")
    # Computed on waking on 11 Nov from the night of 10 Nov, stamped at that night's noon.
    assert tags(hrv) == {"date": "2026-11-11", "night": "2026-11-10"}
    assert hrv.timestamp == utc(2026, 11, 10, 11)
    assert hrv.fields == {  # the 0.0 deep-sleep RMSSD is the export's "none"
        "average_heart_rate_variability_milliseconds": 35.579,
        "non_rem_heart_rate_beats_per_minute": 57.988,
        "entropy": 2.247,
    }
    (temperature,) = table(points, "daily_sleep_temperature_derivations")
    assert temperature.fields == {"nightly_temperature_celsius": 32.63077481840193}
    (readiness,) = table(points, "daily_readiness")
    assert tags(readiness)["night"] == "2026-11-10"
    assert readiness.fields == {
        "score": 53.0,
        "type": "MEDIUM",
        "readiness_level": "TYPE_UNSPECIFIED",
        "sleep_readiness": "VERY_HIGH",
        "heart_rate_variability_readiness": "MEDIUM",
        "resting_heart_rate_readiness": "LOW",
    }
    (ratio,) = table(points, "cardio_acute_chronic_workload_ratio")
    assert tags(ratio) == {"date": "2026-11-11"}
    assert ratio.fields == {"ratio": 1.0465, "label": "OPTIMAL_TRAINING"}

    ready, plain = table(points, "stress_score")
    assert tags(ready) == {"date": "2026-11-11", "night": "2026-11-10"}
    assert ready.fields == {
        "stress_score": 63.0,
        "sleep_points": 25.0,
        "max_sleep_points": 30.0,
        "responsiveness_points": 19.0,
        "max_responsiveness_points": 30.0,
        "exertion_points": 19.0,
        "max_exertion_points": 40.0,
        "status": "READY",
    }
    # Without Premium only the overall score; rows without a score or whose calculation failed
    # are left out.
    assert plain.fields == {"stress_score": 77.0, "status": "READY_NOT_PREMIUM"}


async def test_steps_are_tagged_with_their_source_and_sleep_samples_with_the_night(tmp_path):
    harness = Harness(tmp_path)
    write_zip(harness.inbox / "t.zip", export_files())
    await harness.source.scan(harness.context)
    points = harness.points

    steps = [(p.timestamp, tags(p), p.fields) for p in table(points, "steps")]
    assert steps == [
        (utc(2026, 11, 11, 8, 2), {"source": "Pixel Watch 2"}, {"steps": 6.0}),
        (utc(2026, 11, 11, 8, 2), {"source": "MobileTrack"}, {"steps": 9.0}),
        (utc(2026, 11, 11, 8, 3) + 250, {"source": "Phone Health Connect"}, {"steps": 0.0}),
        (utc(2026, 11, 11, 8, 4) + 250, {"source": "Pixel Watch 2"}, {"steps": 4.0}),
    ]
    (spo2,) = table(points, "oxygen_saturation")  # the 0.0 reading is left out
    assert tags(spo2) == {"night": "2026-11-10"}  # one device records it: no source
    assert spo2.fields == {"oxygen_saturation_percentage": 97.3}
    (summary,) = table(points, "respiratory_rate_sleep_summary")
    assert tags(summary)["night"] == "2026-11-10"
    assert summary.fields == {
        "deep_sleep_stats_breaths_per_minute": 13.0,
        "deep_sleep_stats_standard_deviation_breaths_per_minute": 1.2,
        "deep_sleep_stats_signal_to_noise": 4.763,
        "light_sleep_stats_breaths_per_minute": 12.4,
        "light_sleep_stats_standard_deviation_breaths_per_minute": 1.0,
        "light_sleep_stats_signal_to_noise": 6.48,
        "rem_sleep_stats_signal_to_noise": 1.784,
        "full_sleep_stats_breaths_per_minute": 13.0,
        "full_sleep_stats_standard_deviation_breaths_per_minute": 1.2,
        "full_sleep_stats_signal_to_noise": 4.763,
    }
    (weight,) = table(points, "weight")
    assert tags(weight) == {}
    assert weight.fields == {"weight_grams": 72500.0}
    assert table(points, "heart_rate_minute") == []  # only with heart_rate_minutes


async def test_sleeps_have_a_main_session_a_nap_and_no_duplicate_recordings(tmp_path):
    harness = Harness(tmp_path)
    write_split(harness.inbox, export_files())
    await harness.source.scan(harness.context)
    points = harness.points

    sleeps = [(p.timestamp, tags(p)["night"], p.fields["main"]) for p in table(points, "sleep")]
    assert sleeps == [
        (utc(2026, 11, 10, 22, 30), "2026-11-10", True),
        (utc(2026, 11, 11, 9), "2026-11-10", False),  # the nap, the morning after
        (utc(2026, 7, 13, 21), "2026-07-13", True),  # 23:00 at +02:00
        # Given as 02:25 at +00:00; its stages' +02:00 makes it 00:25 UTC, 02:25 local.
        (utc(2025, 6, 23, 0, 25), "2025-06-22", True),
    ]
    assert table(points, "sleep")[0].fields == {
        "main": True,
        "minutes_in_sleep_period": 450.0,
        "minutes_after_wake_up": 0.0,
        "minutes_to_fall_asleep": 0.0,
        "minutes_asleep": 420.0,
        "minutes_awake": 30.0,
        "minutes_longest_awakening": 12.0,
        "minutes_to_persistent_sleep": 0.0,
        "type": "STAGES",
        "data_source": "DERIVED",
    }
    stages = [(p.timestamp, tags(p), p.fields) for p in table(points, "sleep_stage")]
    assert stages == [
        (utc(2026, 11, 10, 22, 30), {"night": "2026-11-10", "stage": "light"}, {"seconds": 810.0}),
        (
            utc(2026, 11, 10, 22, 43, 30),
            {"night": "2026-11-10", "stage": "deep"},
            {"seconds": 2790.0},
        ),
        # A stage of a sleep not in the export takes the night of its own start.
        (utc(2026, 11, 12, 23, 10), {"night": "2026-11-12", "stage": "awake"}, {"seconds": 120.0}),
        (utc(2025, 6, 23, 0, 25), {"night": "2025-06-22", "stage": "light"}, {"seconds": 900.0}),
        (utc(2025, 6, 23, 0, 40), {"night": "2025-06-22", "stage": "deep"}, {"seconds": 1800.0}),
    ]
    first, summer, corrected = table(points, "sleep_score")
    assert corrected.timestamp == utc(2025, 6, 23, 7) and tags(corrected)["night"] == "2025-06-22"
    assert tags(first) == {"night": "2026-11-10"}
    assert first.fields["overall_score"] == 87.5
    assert "duration_score" not in first.fields  # -1: Premium only
    assert tags(summer) == {"night": "2026-07-13"} and summer.fields["duration_score"] == 77.5


async def test_importing_the_same_export_again_writes_nothing(tmp_path):
    harness = Harness(tmp_path)
    write_split(harness.inbox, export_files())
    await harness.source.scan(harness.context)
    harness.take()
    write_split(harness.inbox, export_files(), stem="takeout-2")
    assert await harness.source.scan(harness.context)
    assert harness.points == []
    assert len(list((harness.inbox / "imported").iterdir())) == 4


async def test_a_later_export_imports_only_the_files_that_grew(tmp_path):
    harness = Harness(tmp_path)
    write_zip(harness.inbox / "a.zip", export_files())
    await harness.source.scan(harness.context)
    harness.take()
    files = export_files()
    name = f"{ACTIVITY}Steps/steps_2026-11-01.csv"
    files[name] += b"2026-11-11T08:05:00Z,12,Pixel Watch 2\n"
    write_zip(harness.inbox / "b.zip", files)
    await harness.source.scan(harness.context)
    assert Counter(p.table for p in harness.points) == {"google_steps": 5}


async def test_a_longer_session_in_a_later_file_takes_over_as_main(tmp_path):
    harness = Harness(tmp_path)
    write_zip(harness.inbox / "a.zip", export_files())
    await harness.source.scan(harness.context)
    harness.take()
    # A later file adds a longer sleep on the night of 13 Jul that does not overlap the first.
    later = csv(
        SLEEPS_HEADER,
        f"300,STAGES,600,0,0,590,10,2,0,+02:00,2026-07-14T05:00:00Z,+02:00,2026-07-14T09:59:00Z,"
        f"DERIVED,sleep_session_v2,{T},{T}",
    )
    files = export_files() | {f"{SLEEP}UserSleeps_2026-07-14.csv": later}
    write_zip(harness.inbox / "b.zip", files)
    await harness.source.scan(harness.context)
    updates = [(p.timestamp, p.fields) for p in table(harness.points, "sleep")]
    assert updates == [
        (utc(2026, 7, 14, 5), updates[0][1]),
        (utc(2026, 7, 13, 21), {"main": False}),
    ]
    assert updates[0][1]["main"] is True
    state = json.loads((harness.state / STATE_FILE).read_text())
    assert state["sleeps"]["200"][4] is False and state["sleeps"]["300"][4] is True


async def test_heart_rate_is_written_per_minute_when_asked_for(tmp_path):
    harness = Harness(tmp_path, heart_rate_minutes=True)
    write_zip(harness.inbox / "t.zip", export_files())
    await harness.source.scan(harness.context)
    minutes = [(p.timestamp, tags(p), p.fields) for p in table(harness.points, "heart_rate_minute")]
    assert minutes == [
        (
            utc(2026, 11, 11, 1, 0),
            {"source": "Pixel Watch 2"},
            {"mean": 52.0, "min": 50.0, "max": 54.0, "count": 2.0},
        ),
        (
            utc(2026, 11, 11, 1, 1),
            {"source": "Pixel Watch 2"},
            {"mean": 60.0, "min": 60.0, "max": 60.0, "count": 1.0},
        ),
    ]


async def test_large_files_are_handed_over_in_batches(tmp_path, monkeypatch):
    monkeypatch.setattr(takeout, "BATCH", 2)
    harness = Harness(tmp_path)
    write_zip(harness.inbox / "t.zip", export_files())
    await harness.source.scan(harness.context)
    assert max(len(b) for b in harness.batches) == 2
    assert len(harness.points) == sum(EXPECTED.values())


@pytest.mark.parametrize(
    ("content", "reason"),
    [
        (b"not a zip", "not a zip"),
        (None, "no Google Health folder in it"),
    ],
)
async def test_a_file_that_is_not_an_export_is_moved_to_failed(tmp_path, content, reason):
    harness = Harness(tmp_path)
    path = harness.inbox / "bad.zip"
    if content is None:
        write_zip(path, {"Takeout/Photos/one.jpg": b"jpeg"})
    else:
        path.write_bytes(content)
    write_zip(harness.inbox / "good.zip", export_files())
    assert await harness.source.scan(harness.context)
    assert (harness.inbox / "failed" / "bad.zip").exists()
    assert (harness.inbox / "imported" / "good.zip").exists()
    (problem,) = harness.problems
    assert problem.startswith(f"cannot read bad.zip: {reason}") and problem.endswith("to failed/")


async def test_a_file_that_cannot_be_read_is_reported_and_the_rest_imported(tmp_path):
    harness = Harness(tmp_path)
    files = export_files()
    files[f"{ACTIVITY}Weight/weight.csv"] = b"timestamp,weight grams\n\xff\xfe\n"
    files[f"{ACTIVITY}Daily Readiness/daily_readiness.csv"] = b'timestamp,score\n"2026-11-11,53\n'
    write_zip(harness.inbox / "t.zip", files)
    assert await harness.source.scan(harness.context)
    assert table(harness.points, "weight") == [] and table(harness.points, "daily_readiness") == []
    assert len(harness.points) == sum(EXPECTED.values()) - 2
    assert any("Weight/weight.csv" in p and "codec" in p for p in harness.problems)
    assert any("daily_readiness.csv: not CSV" in p for p in harness.problems)


async def test_a_damaged_file_in_a_zip_is_reported(tmp_path):
    harness = Harness(tmp_path)
    path = write_zip(harness.inbox / "t.zip", export_files())
    name = f"{SLEEP}UserSleeps_2026-02-24.csv"
    with zipfile.ZipFile(path) as archive:
        member = archive.getinfo(name)
    data = path.read_bytes()
    at = member.header_offset + 30 + len(member.filename) + 10
    path.write_bytes(data[:at] + bytes([data[at] ^ 0xFF]) + data[at + 1 :])
    assert await harness.source.scan(harness.context)
    assert any("UserSleeps_2026-02-24.csv" in p for p in harness.problems)


async def test_a_refused_batch_leaves_the_export_to_be_imported_again(tmp_path):
    harness = Harness(tmp_path)
    harness.accept = False
    write_split(harness.inbox, export_files())
    assert not await harness.source.scan(harness.context)
    assert sorted(p.name for p in harness.inbox.iterdir()) == [
        "takeout-1-001.zip",
        "takeout-1-002.zip",
    ]
    assert any("stay in the inbox" in p for p in harness.problems)
    assert not (harness.state / STATE_FILE).exists()
    harness.accept = True
    harness.take()
    assert await harness.source.scan(harness.context)
    assert len(harness.points) == sum(EXPECTED.values())


async def test_an_unreadable_state_imports_everything_again(tmp_path, caplog):
    harness = Harness(tmp_path)
    harness.state.mkdir()
    (harness.state / STATE_FILE).write_text(json.dumps({"files": {}, "sleeps": {"1": [1]}}))
    write_zip(harness.inbox / "t.zip", export_files())
    await harness.source.scan(harness.context)
    assert len(harness.points) == sum(EXPECTED.values())
    assert "importing every file again" in caplog.text


async def test_an_empty_inbox_is_a_successful_round(tmp_path):
    harness = Harness(tmp_path, scan_minutes=5)
    with pytest.raises(Stop):
        await harness.source.run(harness.context)
    assert harness.successes == 1 and harness.sleeps == [300.0]
    harness.source.wake()
    assert harness.source._woken.is_set()


async def test_a_refused_round_is_not_a_success(tmp_path):
    harness = Harness(tmp_path)
    harness.accept = False
    write_zip(harness.inbox / "t.zip", export_files())
    with pytest.raises(Stop):
        await harness.source.run(harness.context)
    assert harness.successes == 0


async def test_a_session_without_an_id_is_skipped(tmp_path):
    harness = Harness(tmp_path)
    rows = SLEEPS.decode().splitlines()
    nameless = rows[1].replace("100,STAGES", ",STAGES", 1)
    files = export_files() | {f"{SLEEP}UserSleeps_2026-02-24.csv": csv(rows[0], nameless)}
    write_zip(harness.inbox / "t.zip", files)
    await harness.source.scan(harness.context)
    assert table(harness.points, "sleep") == []


def test_the_adapter_is_registered_and_validates_its_params(tmp_path):
    adapter = ADAPTERS["google_takeout"]
    valid = {"inbox": str(tmp_path), "person": "alex"}
    built = adapter.build(adapter.params.model_validate(valid), None, "x")
    assert isinstance(built, TakeoutSource) and isinstance(built, AcceptsFiles)
    assert built.file_suffixes == (".zip",) and built.upload_max_bytes == 2 * 1024**3
    with pytest.raises(ValidationError, match="inbox"):
        TakeoutParams.model_validate({})
    with pytest.raises(ValidationError, match="timezone"):
        TakeoutParams.model_validate({**valid, "timezone": "Mars/Olympus"})
    with pytest.raises(ValidationError, match="person"):
        TakeoutParams.model_validate({"inbox": str(tmp_path)})


@pytest.mark.parametrize(
    ("header", "name"),
    [
        ("Daily Resting Heart Rate", "daily_resting_heart_rate"),
        (
            "average heart rate variability milliseconds",
            "average_heart_rate_variability_milliseconds",
        ),
        ("deep sleep stats - milli breaths per minute", "deep_sleep_stats_breaths_per_minute"),
        ("relative nightly stddev 30d celsius", "relative_nightly_stddev_30d_celsius"),
        ("Demographic Vo2max", "demographic_vo2max"),
    ],
)
def test_headers_become_snake_case(header, name):
    assert snake(header) == name


@pytest.mark.parametrize(
    ("raw", "positive", "text", "parsed"),
    [
        ("12.5", False, False, 12.5),
        ("-1", False, False, -1.0),
        ("-1.0", True, False, None),
        ("0.0", True, False, None),
        ("0", False, False, 0.0),
        ("1e3", True, False, 1000.0),
        ("NaN", False, False, None),
        ("", False, False, None),
        (None, False, False, None),
        ("N/A", False, False, None),
        ("1e999", False, False, None),
        (" MEDIUM ", False, True, "MEDIUM"),
        ("53", False, True, "53"),
        ("", False, True, None),
        ("x" * 65, False, True, None),
    ],
)
def test_cells_become_field_values(raw, positive, text, parsed):
    assert value(raw, positive=positive, text=text) == parsed


@pytest.mark.parametrize(
    ("text", "moment"),
    [
        ("2026-11-11T08:02:00Z", dt.datetime(2026, 11, 11, 8, 2, tzinfo=dt.UTC)),
        ("2026-11-11T08:02:00.25Z", dt.datetime(2026, 11, 11, 8, 2, 0, 250000, tzinfo=dt.UTC)),
        ("2026-07-14T06:30:00+02:00", dt.datetime(2026, 7, 14, 4, 30, tzinfo=dt.UTC)),
    ],
)
def test_timestamps_are_read_in_utc(text, moment):
    assert parse_time(text) == moment


@pytest.mark.parametrize("text", ["2026-11-11T08:02:00", "yesterday"])
def test_a_timestamp_without_a_zone_is_refused(text):
    with pytest.raises(ValueError):
        parse_time(text)


def test_offsets():
    assert parse_offset("+02:00") == dt.timedelta(hours=2)
    assert parse_offset("-03:30") == -dt.timedelta(hours=3, minutes=30)
    with pytest.raises(ValueError):
        parse_offset("CET")


@pytest.mark.parametrize(
    ("path", "heart_rate", "kind"),
    [
        ("Health Fitness Data_GoogleData/UserSleeps_2026-02-24.csv", False, "sleeps"),
        ("Health Fitness Data_GoogleData/UserSleepStages_2026-02-24.csv", False, "stages"),
        ("Health Fitness Data_GoogleData/UserSleepScores_2026-02-24.csv", False, "scores"),
        ("Health Fitness Data_GoogleData/UserSleeps README.txt", False, None),
        ("Stress Score/Stress Score.csv", False, "stress"),
        ("Physical Activity_GoogleData/Heart Rate/heart_rate_2026-11-11.csv", False, None),
        ("Physical Activity_GoogleData/Heart Rate/heart_rate_2026-11-11.csv", True, "heart_rate"),
        ("Physical Activity_GoogleData/Steps/steps_2026-11-01.csv", False, "Steps"),
        ("Physical Activity_GoogleData/Steps/steps_readme.txt", False, None),
        ("Physical Activity_GoogleData/Calories/calories_2026-11-01.csv", False, None),
        ("User Profile_GoogleData/user_profile.csv", False, None),
    ],
)
def test_files_are_known_by_their_path_in_the_export(path, heart_rate, kind):
    assert file_kind(path, heart_rate) == kind


def builder(tmp_path: Path) -> takeout.Points:
    return takeout.Points(TakeoutParams(inbox=tmp_path, person="alex"), takeout.State())


def sleep_row(sleep_id: str, start: str, end: str, minutes: str, **overrides: str) -> dict:
    row = dict.fromkeys(SLEEPS_HEADER.split(","), "")
    row |= {
        "sleep_id": sleep_id,
        "sleep_start": start,
        "sleep_end": end,
        "start_utc_offset": "+00:00",
        "minutes_in_sleep_period": minutes,
    }
    return row | overrides


def test_rows_that_cannot_be_read_are_skipped(tmp_path):
    points = builder(tmp_path)
    stage = {
        "sleep_id": "1",
        "sleep_stage_type": "LIGHT",
        "start_utc_offset": "+00:00",
        "sleep_stage_start": "2026-11-11T01:00:00Z",
        "sleep_stage_end": "2026-11-11T01:10:00Z",
    }
    stages = [
        stage | {"sleep_stage_start": "bad"},
        stage | {"sleep_stage_type": " "},
        stage | {"sleep_stage_end": "2026-11-11T00:00:00Z"},  # ends before it starts
    ]
    assert list(points.stages(stages)) == []
    score = {"sleep_id": "1", "score_time": "2026-11-11T07:00:00Z", "score_utc_offset": "+00:00"}
    scores = [score | {"score_time": "bad"}, score | {"overall_score": "-1"}]
    assert list(points.scores(scores)) == []
    stress = {"STATUS": "READY", "CALCULATION_FAILED": "false", "DATE": "2026-11-11T00:00:00"}
    assert list(points.stress([stress | {"DATE": "bad"}, stress | {"STRESS_SCORE": ""}])) == []
    assert points.skipped == 5


def test_a_session_without_its_optional_columns_still_counts(tmp_path):
    points = builder(tmp_path)
    (first,) = points.sleeps(
        [sleep_row("1", "2026-11-10T23:00:00Z", "2026-11-11T07:00:00Z", "480")]
    )
    assert first.fields == {"main": True, "minutes_in_sleep_period": 480.0}
    # A nap in a later file leaves the night's main sleep as it was: nothing more is written.
    (nap,) = points.sleeps([sleep_row("2", "2026-11-11T09:00:00Z", "2026-11-11T09:30:00Z", "30")])
    assert nap.fields["main"] is False


async def test_paths_climbing_out_of_the_export_are_ignored(tmp_path):
    harness = Harness(tmp_path)
    files = export_files() | {f"{ACTIVITY}../Steps/steps_2026-11-01.csv": b"timestamp,steps\n"}
    write_zip(harness.inbox / "t.zip", files)
    await harness.source.scan(harness.context)
    assert len(harness.points) == sum(EXPECTED.values())


def test_a_respiratory_summary_takes_the_night_of_the_sleep_that_ended_with_it(tmp_path):
    points = builder(tmp_path)
    # A long lie-in ending at 12:30: by its own time the summary would belong to the next night.
    list(points.sleeps([sleep_row("1", "2026-11-11T01:00:00Z", "2026-11-11T12:30:00Z", "690")]))
    row = {"timestamp": "2026-11-11T12:30:00Z", "full sleep stats - milli breaths per minute": "13"}
    (summary,) = points.family("Respiratory Rate Sleep Summary", [row])
    assert tags(summary)["night"] == "2026-11-10"


async def test_without_api_data_only_what_the_api_lacks_is_imported(tmp_path, caplog):
    harness = Harness(tmp_path, api_data=False)
    caplog.set_level("INFO")
    write_split(harness.inbox, export_files())
    assert await harness.source.scan(harness.context)
    points = harness.take()
    assert Counter(p.table for p in points) == {
        "google_daily_readiness": 1,
        "google_cardio_acute_chronic_workload_ratio": 1,
        "google_stress_score": 2,
        "google_sleep_score": 3,
    }
    # The sessions are still read, for their scores' nights and corrected times.
    nights = {tags(p)["night"]: p.timestamp for p in table(points, "sleep_score")}
    assert nights == {
        "2026-11-10": utc(2026, 11, 11, 6),
        "2026-07-13": utc(2026, 7, 14, 4, 30),
        "2025-06-22": utc(2025, 6, 23, 7),
    }
    assert "corrected the times of 2 sleep(s) and score(s)" in caplog.text

    # With api_data set again, the sessions are written, though their file has not changed.
    harness.source = TakeoutSource(harness.params.model_copy(update={"api_data": True}))
    write_split(harness.inbox, export_files(), stem="takeout-2")
    await harness.source.scan(harness.context)
    assert Counter(p.table for p in harness.points)["google_sleep"] == 4


async def test_stages_whose_offsets_cannot_be_read_correct_nothing(tmp_path, caplog):
    harness = Harness(tmp_path)
    files = export_files()
    stages = f"{SLEEP}UserSleepStages_2026-02-24.csv"
    files[stages] += b",,,,,,,,,,\n"  # a row without an id or offset
    files[f"{SLEEP}UserSleepStages_2027-02-24.csv"] = b'sleep_id,start_utc_offset\n"300,+02:00\n'
    write_zip(harness.inbox / "t.zip", files)
    await harness.source.scan(harness.context)
    assert "cannot read the stages' offsets" in caplog.text
    corrected = [p for p in table(harness.points, "sleep") if tags(p)["night"] == "2025-06-22"]
    assert corrected[0].timestamp == utc(2025, 6, 23, 0, 25)  # the readable file still gives it

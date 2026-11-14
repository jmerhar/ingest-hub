"""Importing ResMed SD cards, with synthetic cards (see ``resmed_card.py``)."""

from __future__ import annotations

import datetime as dt
import json
import zipfile
from collections import Counter
from pathlib import Path

import pytest
from jigkit.sinks import Point
from pydantic import ValidationError

from ingest_hub.adapters import ADAPTERS, resmed
from ingest_hub.adapters.inbox import wait_or_wake
from ingest_hub.adapters.night import night_of
from ingest_hub.adapters.resmed import (
    STATE_FILE,
    ResmedParams,
    ResmedSource,
    _zip_root,
    card_kind,
    card_offset,
    field_name,
    parse_zone,
)
from ingest_hub.adapters.resmed.edf import read_edf
from ingest_hub.collect import AcceptsFiles, SourceContext

from .resmed_card import (
    SERIAL,
    STR_SIGNALS,
    card_files,
    edf,
    pld,
    sa2,
    str_day,
    write_card,
    write_zip,
)


def utc(*parts: int) -> int:
    """Unix milliseconds of a UTC time."""
    return int(dt.datetime(*parts, tzinfo=dt.UTC).timestamp() * 1000)


class Stop(Exception):
    """Ends a source's run loop in a test."""


class Harness:
    def __init__(self, tmp_path: Path, **overrides) -> None:
        params = {"inbox": str(tmp_path / "inbox"), "person": "alex", **overrides}
        self.params = ResmedParams.model_validate(params)
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
            "cpap",
            self.state,
            emit,
            on_success=succeeded,
            on_problem=self.problems.append,
            sleep=sleep,
        )
        self.source = ResmedSource(self.params)

    @property
    def points(self) -> list[Point]:
        return [p for batch in self.batches for p in batch]

    def take(self) -> list[Point]:
        points = self.points
        self.batches.clear()
        return points


def table(points: list[Point], name: str) -> list[Point]:
    return [p for p in points if p.table == name]


def tags(point: Point) -> dict[str, str]:
    return dict(point.tags)


async def test_a_card_becomes_nights_mask_periods_events_and_detail(tmp_path, caplog):
    harness = Harness(tmp_path)
    caplog.set_level("INFO")
    write_zip(harness.inbox / "card.zip", card_files())
    assert await harness.source.scan(harness.context)
    points = harness.points
    assert Counter(p.table for p in points) == {
        "cpap_night": 3,
        "cpap_mask": 3,
        "cpap_event": 7,
        "cpap_detail": 120,
    }
    assert {(tags(p)["device"], tags(p)["person"]) for p in points} == {(SERIAL, "alex")}

    first, rest, second = table(points, "cpap_night")
    # The card's +01:00: STR's days start at noon on the device's clock.
    assert first.timestamp == utc(2026, 10, 10, 11)
    assert tags(first) == {"device": SERIAL, "night": "2026-10-10", "person": "alex"}
    assert first.fields == {
        "mask_events": 1.0,
        "duration": 2.0,
        "mode": 1.0,
        "s_a_min_press": 8.0,
        "s_epr_level": 2.0,
        "heated_tube": 3.0,
        "blow_press_95": 11.78,
        "leak_95": 0.78,
        "ahi": 2.4,
    }
    # A day without therapy keeps its settings only; a day the device was off has nothing at all.
    assert tags(rest)["night"] == "2026-10-11"
    assert rest.fields == {
        "mode": 1.0,
        "s_a_min_press": 8.0,
        "s_epr_level": 2.0,
        "heated_tube": 3.0,
    }
    assert tags(second)["night"] == "2026-10-13"
    assert second.fields["ahi"] == 0.0 and second.fields["leak_95"] == 1.2

    masks = [
        (p.timestamp, p.fields["minutes"], tags(p)["night"]) for p in table(points, "cpap_mask")
    ]
    assert masks == [
        (utc(2026, 10, 10, 22), 2.0, "2026-10-10"),
        (utc(2026, 10, 13, 21, 45), 2.0, "2026-10-13"),
        (utc(2026, 10, 14, 0, 30), 1.0, "2026-10-13"),
    ]

    events = [(p.timestamp, tags(p)["type"], p.fields) for p in table(points, "cpap_event")]
    assert events[:5] == [
        (utc(2026, 10, 10, 22, 0, 30), "obstructive_apnea", {"duration": 12.0}),
        (utc(2026, 10, 10, 22, 0, 45), "hypopnea", {"duration": 0.0}),
        (utc(2026, 10, 10, 22, 1, 1), "central_apnea", {"duration": 15.0}),
        (utc(2026, 10, 10, 22, 1, 10), "arousal", {"duration": 0.0}),
        (utc(2026, 10, 10, 22, 1, 30), "apnea", {"duration": 10.0}),
    ]
    # Both of 13 Oct's sessions, the second after midnight, belong to its night.
    assert [(tags(p)["night"], p.timestamp) for p in table(points, "cpap_event")[5:]] == [
        ("2026-10-13", utc(2026, 10, 13, 21, 45, 5)),
        ("2026-10-13", utc(2026, 10, 14, 0, 30, 5)),
    ]

    detail = table(points, "cpap_detail")
    assert detail[0].timestamp == utc(2026, 10, 10, 22, 0, 10)
    assert detail[0].fields == {"mask_press": 8.0, "leak": 0.0}  # its flow limitation is missing
    assert detail[1].timestamp == utc(2026, 10, 10, 22, 0, 12)
    assert detail[1].fields == {"mask_press": 8.0, "leak": 0.02, "flow_lim": 0.1}
    assert detail[59].timestamp == utc(2026, 10, 10, 22, 2, 8)
    assert Counter(tags(p)["night"] for p in detail) == {"2026-10-10": 60, "2026-10-13": 60}

    assert (harness.inbox / "imported" / "card.zip").exists()
    assert "imported card.zip: 9 file(s), 0 unchanged, 3 night(s)" in caplog.text
    assert harness.problems == []


async def test_importing_the_same_card_again_writes_nothing(tmp_path, caplog):
    harness = Harness(tmp_path)
    write_zip(harness.inbox / "a.zip", card_files())
    await harness.source.scan(harness.context)
    assert len(harness.take()) == 133
    write_zip(harness.inbox / "b.zip", card_files())
    caplog.set_level("INFO")
    assert await harness.source.scan(harness.context)
    assert harness.points == []
    assert (harness.inbox / "imported" / "b.zip").exists()
    assert "b.zip" not in caplog.text  # nothing imported, nothing logged


async def test_the_next_copy_of_a_card_adds_only_what_changed(tmp_path):
    harness = Harness(tmp_path)
    write_zip(harness.inbox / "a.zip", card_files())
    await harness.source.scan(harness.context)
    harness.take()
    write_zip(harness.inbox / "b.zip", card_files(extra_night=True))
    await harness.source.scan(harness.context)
    points = harness.points
    # STR changed, so its days are written again, over the old ones; the new session is new.
    assert Counter(p.table for p in points) == {"cpap_night": 4, "cpap_mask": 4, "cpap_detail": 30}
    assert {tags(p)["night"] for p in table(points, "cpap_detail")} == {"2026-10-14"}


async def test_a_card_folder_is_read_in_place_and_left_as_it_was(tmp_path):
    folder = write_card(tmp_path / "card", card_files())
    before = sorted(
        (p.relative_to(folder), p.read_bytes()) for p in folder.rglob("*") if p.is_file()
    )
    harness = Harness(tmp_path, card=str(folder))
    assert await harness.source.scan(harness.context)
    assert len(harness.take()) == 133
    await harness.source.scan(harness.context)
    assert harness.points == []
    after = sorted(
        (p.relative_to(folder), p.read_bytes()) for p in folder.rglob("*") if p.is_file()
    )
    assert after == before
    state = json.loads((harness.state / STATE_FILE).read_text())["files"]
    assert "STR.edf" in state and not any("BRP" in name for name in state)

    write_card(folder, card_files(extra_night=True))
    await harness.source.scan(harness.context)
    assert len(harness.points) == 38


async def test_a_card_folder_that_is_not_there_is_skipped(tmp_path):
    harness = Harness(tmp_path, card=str(tmp_path / "unmounted"))
    assert await harness.source.scan(harness.context)
    assert harness.points == [] and harness.problems == []


async def test_a_card_zipped_inside_a_folder_is_found(tmp_path):
    harness = Harness(tmp_path)
    files = card_files() | {"../STR.edf": b"outside", "SD card.txt": b"beside"}
    write_zip(harness.inbox / "card.zip", files, folder="SD card/")
    with zipfile.ZipFile(harness.inbox / "card.zip", "a") as archive:
        # As long as the card's folder name, so it is told apart by its folder, not its length.
        archive.writestr("SD-card/STR.edf", b"not this card's")
    await harness.source.scan(harness.context)
    assert len(harness.points) == 133


@pytest.mark.parametrize(
    ("zone", "night_utc"),
    [("UTC", utc(2026, 10, 10, 12)), ("Europe/Amsterdam", utc(2026, 10, 10, 10))],
)
async def test_timezone_overrides_the_cards_setting(tmp_path, zone, night_utc):
    harness = Harness(tmp_path, timezone=zone)
    write_zip(harness.inbox / "card.zip", card_files())
    await harness.source.scan(harness.context)
    assert table(harness.points, "cpap_night")[0].timestamp == night_utc


async def test_a_card_without_its_settings_is_read_as_utc(tmp_path, caplog):
    harness = Harness(tmp_path)
    files = card_files()
    files["SETTINGS/CurrentSettings.json"] = b"{}"
    write_zip(harness.inbox / "card.zip", files)
    await harness.source.scan(harness.context)
    assert table(harness.points, "cpap_night")[0].timestamp == utc(2026, 10, 10, 12)
    assert "no TimeZoneOffset setting on the card; reading times as UTC" in caplog.text


async def test_a_copy_without_settings_uses_the_last_cards_offset(tmp_path, caplog):
    harness = Harness(tmp_path)
    write_zip(harness.inbox / "a.zip", card_files())
    await harness.source.scan(harness.context)
    harness.take()
    state = json.loads((harness.state / STATE_FILE).read_text())
    assert state["offset"] == "+01:00"
    caplog.set_level("INFO")
    files = card_files(extra_night=True)
    del files["SETTINGS/CurrentSettings.json"]
    write_zip(harness.inbox / "b.zip", files)
    await harness.source.scan(harness.context)
    detail = table(harness.points, "cpap_detail")
    assert detail[0].timestamp == utc(2026, 10, 14, 21)
    assert "b.zip: no TimeZoneOffset setting; using +01:00" in caplog.text


async def test_an_offset_that_is_not_one_is_not_kept(tmp_path):
    harness = Harness(tmp_path)
    harness.state.mkdir()
    (harness.state / STATE_FILE).write_text(json.dumps({"files": {}, "offset": "CET"}))
    files = card_files()
    del files["SETTINGS/CurrentSettings.json"]
    write_zip(harness.inbox / "card.zip", files)
    await harness.source.scan(harness.context)
    assert table(harness.points, "cpap_night")[0].timestamp == utc(2026, 10, 10, 12)


@pytest.mark.parametrize(
    ("name", "content", "reason"),
    [
        ("broken.zip", b"not a zip", "not a zip"),
        ("other.zip", None, "no STR.edf or DATALOG folder"),
    ],
)
async def test_a_file_that_is_not_a_card_is_moved_to_failed(tmp_path, name, content, reason):
    harness = Harness(tmp_path)
    path = harness.inbox / name
    if content is None:
        write_zip(path, {"photos/one.jpg": b"jpeg"})
    else:
        path.write_bytes(content)
    write_zip(harness.inbox / "zcard.zip", card_files())
    assert await harness.source.scan(harness.context)
    assert (harness.inbox / "failed" / name).exists()
    assert (harness.inbox / "imported" / "zcard.zip").exists()
    assert any(f"cannot read card {name}: {reason}" in p for p in harness.problems)


async def test_a_file_that_cannot_be_read_is_reported_and_the_rest_imported(tmp_path, monkeypatch):
    harness = Harness(tmp_path)
    files = card_files()
    files["DATALOG/20261010/20261010_230000_CSL.edf"] = b"garbage"
    files["DATALOG/20261013/20261013_224500_PLD.edf"] = pld(dt.datetime(2026, 10, 13, 22, 45), 40)
    monkeypatch.setattr(resmed, "MEMBER_MAX_BYTES", 5000)
    write_zip(harness.inbox / "card.zip", files)
    assert await harness.source.scan(harness.context)
    assert len(harness.points) == 133 - 30
    assert sorted(harness.problems) == [
        "card.zip: cannot read DATALOG/20261010/20261010_230000_CSL.edf: not an EDF header "
        "(invalid literal for int() with base 10: '')",
        "card.zip: cannot read DATALOG/20261013/20261013_224500_PLD.edf: larger than 5000 bytes",
    ]
    # Neither is tried again until it changes.
    write_zip(harness.inbox / "again.zip", files)
    harness.take()
    harness.problems.clear()
    await harness.source.scan(harness.context)
    assert harness.points == [] and harness.problems == []


async def test_a_damaged_file_in_a_zip_is_reported(tmp_path):
    harness = Harness(tmp_path)
    files = card_files()
    path = write_zip(harness.inbox / "card.zip", files)
    data = path.read_bytes()
    # Change a byte of STR.edf's stored (deflated) content, so its CRC-32 no longer matches.
    with zipfile.ZipFile(path) as archive:
        member = archive.getinfo("STR.edf")
    at = member.header_offset + 30 + len(member.filename) + 20
    path.write_bytes(data[:at] + bytes([data[at] ^ 0xFF]) + data[at + 1 :])
    assert await harness.source.scan(harness.context)
    assert table(harness.points, "cpap_night") == []
    assert [p for p in harness.problems if "STR.edf: cannot be read from the zip" in p]


async def test_a_refused_batch_leaves_the_card_to_be_imported_again(tmp_path):
    harness = Harness(tmp_path)
    harness.accept = False
    write_zip(harness.inbox / "card.zip", card_files())
    assert not await harness.source.scan(harness.context)
    assert len(harness.batches) == 1
    assert (harness.inbox / "card.zip").exists()
    assert any("stays in the inbox" in p for p in harness.problems)
    assert json.loads((harness.state / STATE_FILE).read_text())["files"] == {}

    harness.accept = True
    harness.take()
    assert await harness.source.scan(harness.context)
    assert len(harness.points) == 133


async def test_a_refused_batch_from_the_card_folder_is_read_again(tmp_path):
    folder = write_card(tmp_path / "card", card_files())
    harness = Harness(tmp_path, card=str(folder))
    accepted = 0

    async def emit(points: list[Point]) -> bool:
        nonlocal accepted
        harness.batches.append(points)
        accepted += 1
        return accepted <= 2  # STR, then the first EVE

    harness.context._emit = emit
    assert not await harness.source.scan(harness.context)
    assert any("read again next scan" in p for p in harness.problems)
    saved = json.loads((harness.state / STATE_FILE).read_text())["files"]
    assert sorted(saved) == [
        "DATALOG/20261010/20261010_230000_CSL.edf",
        "DATALOG/20261010/20261010_230000_EVE.edf",
        "STR.edf",
    ]


async def test_large_files_are_handed_over_in_batches(tmp_path, monkeypatch):
    monkeypatch.setattr(resmed, "BATCH", 50)
    harness = Harness(tmp_path)
    write_zip(harness.inbox / "card.zip", card_files())
    await harness.source.scan(harness.context)
    assert max(len(b) for b in harness.batches) == 50
    assert len(harness.points) == 133


async def test_an_oximeter_s_readings_are_written(tmp_path):
    harness = Harness(tmp_path)
    files = card_files()
    files["DATALOG/20261010/20261010_230010_SA2.edf"] = sa2(
        dt.datetime(2026, 10, 10, 23, 0, 10), 1, pulse=58
    )
    write_zip(harness.inbox / "card.zip", files)
    await harness.source.scan(harness.context)
    oximetry = table(harness.points, "cpap_oximetry")
    assert len(oximetry) == 60
    assert oximetry[1].timestamp == utc(2026, 10, 10, 22, 0, 11)
    assert oximetry[1].fields == {"pulse": 58.0, "spo2": 96.0}


async def test_an_unreadable_state_imports_everything_again(tmp_path, caplog):
    harness = Harness(tmp_path)
    harness.state.mkdir()
    (harness.state / STATE_FILE).write_text("{not json")
    write_zip(harness.inbox / "card.zip", card_files())
    await harness.source.scan(harness.context)
    assert len(harness.points) == 133
    assert "importing every file again" in caplog.text


async def test_the_source_scans_every_scan_minutes_and_when_woken(tmp_path):
    harness = Harness(tmp_path, scan_minutes=5)
    with pytest.raises(Stop):
        await harness.source.run(harness.context)
    assert harness.successes == 1 and harness.sleeps == [300.0]

    async def wait_forever(_seconds: float) -> None:
        await __import__("asyncio").Event().wait()

    harness.context.sleep = wait_forever
    harness.source.wake()
    await wait_or_wake(harness.context, harness.source._woken, 300)  # returns at once: woken


async def test_a_refused_round_is_not_a_success(tmp_path):
    harness = Harness(tmp_path)
    harness.accept = False
    write_zip(harness.inbox / "card.zip", card_files())
    with pytest.raises(Stop):
        await harness.source.run(harness.context)
    assert harness.successes == 0


def test_mask_periods_that_start_together_are_one_and_backwards_ones_are_left_out():
    days = [
        str_day(20736, [(700, 650), (800, 800), (800, 800), (800, 814), (820, 820)], duration=1)
    ]
    summary = read_edf(edf(dt.datetime(2026, 10, 10, 12), 86400, STR_SIGNALS, days))
    points = list(resmed.Points("cpap_", dt.UTC, "alex").build("STR", summary))
    assert [(p.timestamp, p.fields) for p in table(points, "cpap_mask")] == [
        (utc(2026, 10, 11, 1, 20), {"minutes": 14.0}),  # three periods starting together
        (utc(2026, 10, 11, 1, 40), {"minutes": 0.0}),
    ]


def test_the_adapter_is_registered_and_validates_its_params(tmp_path):
    adapter = ADAPTERS["resmed_sd"]
    valid = {"inbox": str(tmp_path), "person": "alex"}
    built = adapter.build(adapter.params.model_validate(valid), None, "x")
    assert isinstance(built, ResmedSource) and isinstance(built, AcceptsFiles)
    assert built.file_suffixes == (".zip",)
    assert "`zip -r card.zip . -x '*_BRP.edf'`" in built.upload_help
    with pytest.raises(ValidationError, match="inbox"):
        ResmedParams.model_validate({})
    assert ResmedParams.model_validate({**valid, "timezone": None}).timezone is None
    with pytest.raises(ValidationError, match="person"):
        ResmedParams.model_validate({"inbox": str(tmp_path)})
    with pytest.raises(ValidationError, match="person"):
        ResmedParams.model_validate({**valid, "person": "two words"})
    for zone in ("Mars/Olympus", "+24:00", "1:00"):
        with pytest.raises(ValidationError, match="timezone"):
            ResmedParams.model_validate({**valid, "timezone": zone})


@pytest.mark.parametrize(
    ("text", "offset"),
    [("+01:00", 60), ("-03:30", -210), ("UTC", 0), ("utc", 0), ("Europe/Amsterdam", 60)],
)
def test_zones_are_names_or_fixed_offsets(text, offset):
    winter = dt.datetime(2026, 1, 15, 12)
    assert parse_zone(text).utcoffset(winter) == dt.timedelta(minutes=offset)


@pytest.mark.parametrize(
    ("label", "name"),
    [
        ("S.A.MinPress", "s_a_min_press"),
        ("S.EPR.EPRType", "s_epr_epr_type"),
        ("S.ABFilter", "s_ab_filter"),
        ("Leak.95", "leak_95"),
        ("BlowPress.5", "blow_press_5"),
        ("HTubeTemp.50", "h_tube_temp_50"),
        ("TgtIPAP.Max", "tgt_ipap_max"),
        ("SpO2.50", "spo2_50"),
        ("SpO2Thresh", "spo2_thresh"),
        ("AHI", "ahi"),
        ("MaskPress.2s", "mask_press"),
        ("Flow.40ms", "flow"),
        ("SpO2.1s", "spo2"),
        ("Central Apnea", "central_apnea"),
        ("Apnea (unclassified)", "apnea_unclassified"),
    ],
)
def test_labels_become_snake_case_field_names(label, name):
    assert field_name(label) == name


@pytest.mark.parametrize(
    ("local", "night"),
    [
        (dt.datetime(2026, 10, 12, 23, 23), "2026-10-12"),
        (dt.datetime(2026, 10, 13, 9, 34), "2026-10-12"),
        (dt.datetime(2026, 10, 13, 11, 59, 59), "2026-10-12"),
        (dt.datetime(2026, 10, 13, 12), "2026-10-13"),
    ],
)
def test_a_session_belongs_to_the_night_of_the_noon_before_it(local, night):
    assert night_of(local) == night


@pytest.mark.parametrize(
    ("path", "kind"),
    [
        ("STR.edf", "STR"),
        ("str.EDF", "STR"),
        ("SETTINGS/CurrentSettings.json", "SETTINGS"),
        ("DATALOG/20261012/20261012_232338_PLD.edf", "PLD"),
        ("datalog/20261012/20261012_232328_eve.edf", "EVE"),
        ("DATALOG/20261012/20261012_232328_CSL.edf", "CSL"),
        ("DATALOG/20261012/20261012_232338_SA2.edf", "SA2"),
        ("DATALOG/20261012/20261012_232338_BRP.edf", None),
        ("DATALOG/20261012_232338_PLD.edf", None),
        ("Identification.json", None),
        ("SETTINGS/CurrentSettings.crc", None),
        ("other/STR.edf", None),
    ],
)
def test_files_are_known_by_their_path_on_the_card(path, kind):
    assert card_kind(path) == kind


@pytest.mark.parametrize(
    ("names", "root"),
    [
        (["STR.edf", "DATALOG/x/y.edf"], ""),
        (["card/DATALOG/x/y.edf", "card/STR.edf"], "card/"),
        (["a/b/DATALOG/x/y.edf"], "a/b/"),
        (["DATALOG"], None),  # a file named DATALOG, not the folder
        (["STR.edf/x"], None),
        (["../STR.edf"], None),
        (["notes.txt"], None),
        (["__MACOSX/DATALOG/x/._y.edf", "card/DATALOG/x/y.edf"], "card/"),
    ],
)
def test_a_zip_s_card_root_is_where_its_str_or_datalog_is(names, root):
    assert _zip_root(names) == root


@pytest.mark.parametrize(
    ("raw", "offset"),
    [
        (b'{"a": [{"TimeZoneOffset": "-05:00"}]}', "-05:00"),
        (b'{"TimeZoneOffset": "CET"}', None),
        (b"[]", None),
        (b"\xff", None),
        (b"{", None),
    ],
)
def test_the_card_s_offset_is_read_from_its_settings(raw, offset):
    assert card_offset(raw) == offset


def anchor(device: str, actual: str) -> resmed.ClockAnchor:
    return resmed.ClockAnchor(
        device=dt.datetime.fromisoformat(device), actual=dt.datetime.fromisoformat(actual)
    )


@pytest.mark.parametrize(
    ("anchors", "device", "seconds"),
    [
        ([], "2026-10-10T23:00:00", 0),
        ([("2026-11-14T02:17:14", "2026-11-14T02:09:00")], "2025-11-19T12:00:00", -494),
        # Right at set-up, 8 min 14 s fast a year later: in a line between, and beyond them.
        (
            [
                ("2025-11-14T02:17:14", "2025-11-14T02:17:14"),
                ("2026-11-14T02:17:14", "2026-11-14T02:09:00"),
            ],
            "2026-05-15T14:17:14",
            -247,
        ),
        (
            [
                ("2025-11-14T02:17:14", "2025-11-14T02:17:14"),
                ("2026-11-14T02:17:14", "2026-11-14T02:09:00"),
            ],
            "2027-11-14T02:17:14",
            -988,
        ),
        (
            [
                ("2025-11-14T02:17:14", "2025-11-14T02:17:14"),
                ("2026-11-14T02:17:14", "2026-11-14T02:09:00"),
            ],
            "2024-11-14T02:17:14",
            494,
        ),
        # With three, each stretch between two has its own rate.
        (
            [
                ("2026-01-01T00:00:00", "2026-01-01T00:00:00"),
                ("2026-01-11T00:00:00", "2026-01-11T00:00:10"),
                ("2026-01-21T00:00:00", "2026-01-21T00:00:50"),
            ],
            "2026-01-16T00:00:00",
            30,
        ),
    ],
)
def test_the_clock_correction_runs_in_a_line_through_the_anchors(anchors, device, seconds):
    result = resmed.clock_correction(
        [anchor(*a) for a in anchors], dt.datetime.fromisoformat(device)
    )
    assert round(result.total_seconds()) == seconds


def test_anchors_are_local_times_at_distinct_device_times(tmp_path):
    valid = {"inbox": str(tmp_path), "person": "alex"}
    one = {"device": "2026-11-14T02:17:14", "actual": "2026-11-14T02:09:00"}
    assert ResmedParams.model_validate({**valid, "clock": [one]}).clock[0].actual.minute == 9
    with pytest.raises(ValidationError, match="without a zone"):
        ResmedParams.model_validate({**valid, "clock": [{**one, "actual": "2026-11-14T02:09:00Z"}]})
    with pytest.raises(ValidationError, match="same device time"):
        ResmedParams.model_validate(
            {**valid, "clock": [one, {**one, "actual": "2026-11-14T02:10:00"}]}
        )


async def test_the_clock_correction_moves_moments_but_not_the_nights(tmp_path):
    fast = [{"device": "2026-10-12T00:00:00", "actual": "2026-10-11T23:51:46"}]  # 8 min 14 s
    harness = Harness(tmp_path, clock=fast)
    write_zip(harness.inbox / "card.zip", card_files())
    await harness.source.scan(harness.context)
    points = harness.points
    shift = 494_000
    assert table(points, "cpap_night")[0].timestamp == utc(2026, 10, 10, 11)  # the device's noon
    assert table(points, "cpap_mask")[0].timestamp == utc(2026, 10, 10, 22) - shift
    assert table(points, "cpap_event")[0].timestamp == utc(2026, 10, 10, 22, 0, 30) - shift
    assert table(points, "cpap_detail")[0].timestamp == utc(2026, 10, 10, 22, 0, 10) - shift
    assert {tags(p)["night"] for p in table(points, "cpap_detail")} == {"2026-10-10", "2026-10-13"}


async def test_a_night_keeps_the_correction_it_was_first_imported_with(tmp_path):
    first = [{"device": "2026-10-12T00:00:00", "actual": "2026-10-11T23:51:46"}]
    harness = Harness(tmp_path, clock=first)
    write_zip(harness.inbox / "a.zip", card_files())
    await harness.source.scan(harness.context)
    harness.take()
    later = [{"device": "2026-10-12T00:00:00", "actual": "2026-10-11T23:55:00"}]  # 5 min fast
    harness.source = ResmedSource(harness.params.model_copy(update={"clock": [anchor(**later[0])]}))
    write_zip(harness.inbox / "b.zip", card_files(extra_night=True))
    await harness.source.scan(harness.context)
    masks = {tags(p)["night"]: p.timestamp for p in table(harness.points, "cpap_mask")}
    # STR changed, so every day's mask periods are written again, on the times they had.
    assert masks["2026-10-10"] == utc(2026, 10, 10, 22) - 494_000
    assert masks["2026-10-14"] == utc(2026, 10, 14, 21) - 300_000
    detail = table(harness.points, "cpap_detail")
    assert detail[0].timestamp == utc(2026, 10, 14, 21) - 300_000
    state = json.loads((harness.state / STATE_FILE).read_text())
    assert state["corrections"]["2026-10-10"] == -494_000

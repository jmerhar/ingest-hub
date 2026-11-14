"""Google Health data from Google Takeout exports, imported from the zips dropped into a folder.

A Takeout of Google Health (Fitbit and Pixel Watch data) is one zip, or several (``-1-001.zip``,
``-1-002.zip``, …) that each hold different files of the same export; the files are CSV under
``Takeout/Google Health/``. Every export repeats the whole history.

Every point is tagged ``person``, whose data it is, from the configuration. What is imported,
each table named ``<prefix>`` + its folder in snake case, which is also the
Google Health API's name for the data type (``Daily Resting Heart Rate`` →
``google_daily_resting_heart_rate``), with a field per column, its header in snake case
(``average heart rate variability milliseconds`` → ``average_heart_rate_variability_milliseconds``):

- daily values, at a civil date the export gives as midnight UTC or a bare date, tagged ``date``.
  Those Google computes from the night's sleep on waking (heart rate variability, oxygen
  saturation, respiratory rate, sleep temperature, readiness, stress) carry the date of the
  morning, so they are also tagged ``night``, the date before (see ``night.py``), and stamped at
  noon of the night in ``timezone``; the others (resting heart rate, VO2 max, the cardio
  workload ratio) at midnight of their date;
- timestamped samples: steps, tagged ``source`` (the export's ``data source``, such as ``Pixel
  Watch 2``; the watch and the phone both count steps, which must never be added together),
  weight, body fat, and the sleep-time series, tagged ``night``: oxygen saturation every minute,
  heart rate variability every five minutes, and each sleep's respiratory rate summary;
- sleep sessions (``sleep``, at each session's start, tagged ``night`` from its local start
  time), their stages (``sleep_stage``, tagged ``night`` and ``stage``, field ``seconds``) and
  scores (``sleep_score``, tagged ``night``); ``main`` marks each night's main sleep. Sessions
  that overlap are the same sleep recorded twice (the export keeps the recordings of two staging
  algorithms side by side); one is kept, the others are left out with their stages and scores
  (see ``common.py``). Where a session's and its score's times are local time labelled
  ``+00:00`` while its stages carry the true offset, the stages' offset corrects them;
- the stress score (``stress_score``): the overall score of every row that has one, the Premium
  breakdown only where the export gives it (``STATUS`` ``READY``);
- with ``heart_rate_minutes``, heart rate per minute (``heart_rate_minute``, tagged ``source``):
  ``mean``, ``min``, ``max`` and ``count`` of the second-level readings.

Numbers are floats; the few columns that hold text (readiness levels, the workload label) are
kept as text. Empty values, ``NaN`` and text in a number's column are left out, and so are values
of zero or less in the measurements where the export uses them for "none" (-1 in sleep scores and
respiratory rates, 0.0 in heart rate variability and oxygen saturation), so a missing value never
becomes a zero.

Samples without an offset are put in a night in ``timezone``; sleep sessions carry their own
offset, so a night away keeps its local times. A sleep's stages and scores take its night by
``sleep_id``, and its respiratory summary by its end time, from the sessions imported so far,
which the source keeps in its state directory with the fingerprint (size and CRC-32) of every
file imported. A file whose fingerprint is unchanged is skipped; a file that grew, as most do from
one export to the next, is imported again, writing the same points over the old ones. The sleep
files are imported before the rest, whichever zip of an export holds them.

With ``api_data`` false, only the data the Google Health API does not offer is imported (sleep
scores, readiness, stress, the cardio workload ratio, the demographic VO2 max), for a
``google_health`` source to read the rest; the sleep sessions are still read, for the scores'
nights and times, but not written.
"""

from __future__ import annotations

import asyncio
import csv
import datetime as dt
import io
import json
import logging
import math
import re
import zipfile
import zlib
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx2
from jigkit.sinks import FieldValue, Point, Precision
from pydantic import BaseModel, ConfigDict, Field, field_validator

from ingest_hub.adapters.google.common import (
    Sleep,
    parse_offset,
    parse_time,
    resolve_sleeps,
    to_ms,
)
from ingest_hub.adapters.inbox import FAILED, IMPORTED, move_aside, wait_or_wake, write_json
from ingest_hub.adapters.night import night_of
from ingest_hub.collect import SourceContext

logger = logging.getLogger(__name__)

#: Points per batch handed to delivery.
BATCH = 5000
#: The largest export zip the source takes as an upload: Google's default split size.
UPLOAD_MAX_BYTES = 2 * 1024**3
#: The fingerprints of the files imported so far and the sleeps seen, in the state directory.
STATE_FILE = "google-takeout.json"
#: The folder of an export every imported path starts from.
ROOT = "Google Health/"
ACTIVITY = "Physical Activity_GoogleData/"
SLEEP_FOLDER = "Health Fitness Data_GoogleData/"
SLEEPS = re.compile(rf"^{SLEEP_FOLDER}UserSleeps_[\d-]+\.csv$")
STAGES = re.compile(rf"^{SLEEP_FOLDER}UserSleepStages_[\d-]+\.csv$")
SCORES = re.compile(rf"^{SLEEP_FOLDER}UserSleepScores_[\d-]+\.csv$")
STRESS = "Stress Score/Stress Score.csv"
HEART_RATE = re.compile(rf"^{ACTIVITY}Heart Rate/heart_rate_[\d-]+\.csv$")
NUMBER = re.compile(r"^-?\d+(\.\d+)?([eE][-+]?\d+)?$")
#: The longest text kept as a field.
TEXT_MAX = 64
#: The ``Physical Activity`` columns that hold text; every other column's values are numbers, and
#: anything else in one is left out, since a field keeps one type.
TEXT_COLUMNS = frozenset(
    {
        "type",
        "readiness level",
        "sleep readiness",
        "heart rate variability readiness",
        "resting heart rate readiness",
        "label",
    }
)
#: Stress rows with a score; ``READY`` also has the breakdown.
STRESS_SCORED = frozenset({"READY", "READY_NOT_PREMIUM"})
STRESS_COLUMNS = (
    "STRESS_SCORE",
    "SLEEP_POINTS",
    "MAX_SLEEP_POINTS",
    "RESPONSIVENESS_POINTS",
    "MAX_RESPONSIVENESS_POINTS",
    "EXERTION_POINTS",
    "MAX_EXERTION_POINTS",
)
#: Sleep session columns written as fields; the rest are identifiers and times.
SLEEP_COLUMNS = (
    "minutes_in_sleep_period",
    "minutes_after_wake_up",
    "minutes_to_fall_asleep",
    "minutes_asleep",
    "minutes_awake",
    "minutes_longest_awakening",
    "minutes_to_persistent_sleep",
)
SCORE_COLUMNS = (
    "overall_score",
    "duration_score",
    "composition_score",
    "revitalization_score",
    "sleep_time_minutes",
    "deep_sleep_minutes",
    "rem_sleep_percent",
    "resting_heart_rate",
    "sleep_goal_minutes",
    "waso_count_long_wakes",
    "waso_count_all_wake_time",
    "restlessness_normalized",
    "hr_below_resting_hr",
)


@dataclass(frozen=True)
class Family:
    """One kind of ``Physical Activity`` file.

    ``kind`` is ``night`` (a daily value computed from the night before), ``day`` (a daily value)
    or ``sample`` (a timestamped sample); ``night`` on a sample tags it with the night it was
    measured in. ``positive`` leaves out numbers of zero or less, which the file uses for "none".
    ``source`` tags a sample with its source, for the data several devices record at once.
    ``api`` marks the data the Google Health API offers as well.
    """

    kind: str
    positive: bool = True
    night: bool = False
    source: bool = False
    api: bool = True


FAMILIES = {
    "Daily Resting Heart Rate": Family("day"),
    "Daily Heart Rate Variability": Family("night"),
    "Daily Oxygen Saturation": Family("night"),
    "Daily Respiratory Rate": Family("night"),
    "Daily Sleep Temperature Derivations": Family("night"),
    "Daily Readiness": Family("night", positive=False, api=False),
    "Demographic Vo2max": Family("day", api=False),
    "Cardio Acute Chronic Workload Ratio": Family("day", positive=False, api=False),
    "Respiratory Rate Sleep Summary": Family("sample", night=True),
    "Heart Rate Variability": Family("sample", night=True),
    "Oxygen Saturation": Family("sample", night=True),
    "Steps": Family("sample", positive=False, source=True),
    "Weight": Family("sample"),
    "Body Fat": Family("sample"),
}


class ExportError(Exception):
    """A file is not a Takeout export that can be read; the message says why."""


class TakeoutParams(BaseModel):
    """A Google Takeout source's ``params`` in the configuration file."""

    model_config = ConfigDict(extra="forbid")

    #: Whose data it is, written as the ``person`` tag of every point.
    person: str = Field(pattern=r"^[A-Za-z0-9_.-]+$")
    #: The folder export zips are dropped into; created when missing.
    inbox: Path
    table_prefix: str = Field("google_", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    scan_minutes: float = Field(10, gt=0)
    #: The zone civil dates and samples without an offset are read in.
    timezone: str = "UTC"
    #: Also import heart rate, as each minute's mean, minimum and maximum.
    heart_rate_minutes: bool = False
    #: Also import what the Google Health API offers; false when a ``google_health`` source
    #: reads it.
    api_data: bool = True

    @field_validator("timezone")
    @classmethod
    def _known_zone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ValueError(f"unknown time zone {value!r}") from error
        return value


def snake(header: str) -> str:
    """A column header or folder name in snake case: ``deep sleep stats - milli breaths per
    minute`` → ``deep_sleep_stats_breaths_per_minute``. The respiratory summary's columns say
    "milli breaths" but hold breaths per minute, so the "milli" is dropped."""
    name = header.lower().replace("milli breaths", "breaths")
    return re.sub(r"[^a-z0-9]+", "_", name).strip("_")


def value(raw: str | None, *, positive: bool, text: bool = False) -> FieldValue | None:
    """A cell as a field value: with ``text`` the text, up to :data:`TEXT_MAX` characters;
    otherwise a float, or None when it is empty, ``NaN``, not a number, or with ``positive`` zero
    or less."""
    cell = (raw or "").strip()
    if text:
        return cell if 0 < len(cell) <= TEXT_MAX else None
    if not NUMBER.match(cell):
        return None
    number = float(cell)
    if not math.isfinite(number) or (positive and number <= 0):
        return None
    return number


@dataclass
class State:
    """The fingerprints of the files imported so far, by path, and the sleeps seen, by id."""

    files: dict[str, str] = field(default_factory=dict)
    sleeps: dict[str, Sleep] = field(default_factory=dict)

    def night_by_end(self) -> dict[int, str]:
        """The night of each sleep, by its end in Unix milliseconds."""
        return {sleep.end_ms: sleep.night for sleep in self.sleeps.values()}


class Points:
    """Turns an export's files into points."""

    def __init__(
        self, params: TakeoutParams, state: State, offsets: dict[str, str] | None = None
    ) -> None:
        self._prefix = params.table_prefix
        self._person = params.person
        self._write_sessions = params.api_data
        #: Each session's UTC offset as its stages give it, by ``sleep_id``.
        self._offsets = offsets or {}
        #: Sessions and scores whose times were corrected from their stages' offset.
        self.corrected = 0
        self._zone = ZoneInfo(params.timezone)
        self._state = state
        self.counts: Counter[str] = Counter()
        self.skipped = 0
        #: Sessions left out as recordings of a sleep already counted.
        self.duplicates = 0

    def _point(
        self, table: str, tags: dict[str, str], ms: int, fields: dict[str, FieldValue]
    ) -> Point:
        self.counts[table] += 1
        return Point(
            self._prefix + table,
            tuple(sorted({**tags, "person": self._person}.items())),
            ms,
            fields,
            precision=Precision.MILLISECONDS,
        )

    def _civil(self, date: dt.date, hour: int) -> int:
        """A civil date at an hour, in ``timezone``, in Unix milliseconds."""
        return to_ms(dt.datetime.combine(date, dt.time(hour), self._zone))

    def _sample_night(self, moment: dt.datetime) -> str:
        return night_of(moment.astimezone(self._zone).replace(tzinfo=None))

    def family(self, name: str, rows: Iterable[dict[str, str]]) -> Iterator[Point]:
        """The points of one ``Physical Activity`` file of the family ``name``."""
        spec = FAMILIES[name]
        table = snake(name)
        by_end = self._state.night_by_end() if name == "Respiratory Rate Sleep Summary" else {}
        for row in rows:
            fields = {
                snake(column): parsed
                for column, raw in row.items()
                if column not in ("timestamp", "data source")
                and column is not None
                and (parsed := value(raw, positive=spec.positive, text=column in TEXT_COLUMNS))
                is not None
            }
            try:
                stamp = row["timestamp"].strip()
                if spec.kind == "sample":
                    moment = parse_time(stamp)
                else:
                    date = dt.date.fromisoformat(stamp[:10])
            except AttributeError, KeyError, ValueError:
                self.skipped += 1
                continue
            if not fields:
                continue
            if spec.kind == "night":
                tags = {
                    "date": date.isoformat(),
                    "night": (date - dt.timedelta(days=1)).isoformat(),
                }
                yield self._point(table, tags, self._civil(date - dt.timedelta(days=1), 12), fields)
            elif spec.kind == "day":
                yield self._point(table, {"date": date.isoformat()}, self._civil(date, 0), fields)
            else:
                ms = to_ms(moment)
                tags = (
                    {"source": (row.get("data source") or "unknown").strip()} if spec.source else {}
                )
                if spec.night:
                    tags["night"] = by_end.get(ms) or self._sample_night(moment)
                yield self._point(table, tags, ms, fields)

    def sleeps(self, rows: Iterable[dict[str, str]]) -> Iterator[Point]:
        """The points of a ``UserSleeps`` file, and updates of ``main`` for sessions in other files
        whose night it changes."""
        sessions: list[tuple[str, dict[str, str], Sleep]] = []
        for row in rows:
            try:
                start = parse_time(row["sleep_start"])
                end = parse_time(row["sleep_end"])
                sleep_id = row["sleep_id"].strip()
                offset = self._offset(sleep_id, row["start_utc_offset"])
                if offset is not None:
                    start, end = start - offset, end - offset
                else:
                    offset = parse_offset(row["start_utc_offset"])
                local = (start + offset).replace(tzinfo=None)
                minutes = float(row.get("minutes_in_sleep_period") or 0)
            except AttributeError, KeyError, ValueError:
                self.skipped += 1
                continue
            if not sleep_id:
                self.skipped += 1
                continue
            sleep = Sleep(
                night_of(local),
                to_ms(start),
                to_ms(end),
                minutes,
                version=(row.get("algorithm_version") or "").strip(),
                staged=(row.get("sleep_type") or "").strip() == "STAGES",
            )
            sessions.append((sleep_id, row, sleep))
        known = self._state.sleeps
        for sleep_id, _, sleep in sessions:
            known[sleep_id] = sleep
        nights = {sleep.night for _, _, sleep in sessions}
        main = resolve_sleeps(known, nights)
        if not self._write_sessions:
            return
        in_file = set()
        for sleep_id, row, sleep in sessions:
            in_file.add(sleep_id)
            if sleep.duplicate:
                self.duplicates += 1
                continue
            sleep.main = sleep_id == main.get(sleep.night)
            fields: dict[str, FieldValue] = {"main": sleep.main}
            for column in SLEEP_COLUMNS:
                if (parsed := value(row.get(column), positive=False)) is not None:
                    fields[column] = parsed
            for column, name in (("sleep_type", "type"), ("data_source", "data_source")):
                if text := (row.get(column) or "").strip():
                    fields[name] = text
            yield self._point("sleep", {"night": sleep.night}, sleep.start_ms, fields)
        # A session in another file loses its place as the night's main sleep to a longer one.
        for sleep_id, sleep in known.items():
            if sleep_id in in_file or sleep.night not in nights or sleep.duplicate:
                continue
            is_main = sleep_id == main.get(sleep.night)
            if is_main != sleep.main:
                sleep.main = is_main
                yield self._point(
                    "sleep", {"night": sleep.night}, sleep.start_ms, {"main": is_main}
                )

    def _offset(self, sleep_id: str, given: str) -> dt.timedelta | None:
        """The offset to correct a session's or score's times by, or None when they are right.

        Some exports give a session's and its score's times in local time, labelled UTC with an
        offset of ``+00:00``, while its stages carry the true offset; the true UTC time is then
        the given one less the stages' offset."""
        stage = self._offsets.get(sleep_id)
        if given.strip() != "+00:00" or stage in (None, "+00:00"):
            return None
        offset = parse_offset(stage)
        self.corrected += 1
        return offset

    def _kept(self, row: dict[str, str]) -> Sleep | None | bool:
        """The sleep a stage or score row belongs to: the session, None when it is not known,
        or False when it is a duplicate, whose rows are left out."""
        sleep = self._state.sleeps.get((row.get("sleep_id") or "").strip())
        if sleep is not None and sleep.duplicate:
            return False
        return sleep

    def stages(self, rows: Iterable[dict[str, str]]) -> Iterator[Point]:
        """The points of a ``UserSleepStages`` file."""
        for row in rows:
            try:
                start = parse_time(row["sleep_stage_start"])
                end = parse_time(row["sleep_stage_end"])
                local = (start + parse_offset(row["start_utc_offset"])).replace(tzinfo=None)
                stage = row["sleep_stage_type"].strip().lower()
            except AttributeError, KeyError, ValueError:
                self.skipped += 1
                continue
            sleep = self._kept(row)
            if sleep is False:
                continue
            night = sleep.night if sleep else night_of(local)
            seconds = (end - start).total_seconds()
            if not stage or seconds < 0:
                self.skipped += 1
                continue
            yield self._point(
                "sleep_stage", {"night": night, "stage": stage}, to_ms(start), {"seconds": seconds}
            )

    def scores(self, rows: Iterable[dict[str, str]]) -> Iterator[Point]:
        """The points of a ``UserSleepScores`` file. -1 is a score the export does not have."""
        for row in rows:
            try:
                moment = parse_time(row["score_time"])
                offset = self._offset((row.get("sleep_id") or "").strip(), row["score_utc_offset"])
                if offset is not None:
                    moment -= offset
                else:
                    offset = parse_offset(row["score_utc_offset"])
                local = (moment + offset).replace(tzinfo=None)
            except AttributeError, KeyError, ValueError:
                self.skipped += 1
                continue
            sleep = self._kept(row)
            if sleep is False:
                continue
            night = sleep.night if sleep else night_of(local)
            fields = {
                column: parsed
                for column in SCORE_COLUMNS
                if (parsed := value(row.get(column), positive=False)) is not None
                and not (isinstance(parsed, float) and parsed < 0)
            }
            if fields:
                yield self._point("sleep_score", {"night": night}, to_ms(moment), fields)

    def stress(self, rows: Iterable[dict[str, str]]) -> Iterator[Point]:
        """The points of the stress score file: rows with a score, the breakdown when given."""
        for row in rows:
            status = (row.get("STATUS") or "").strip()
            if status not in STRESS_SCORED or (row.get("CALCULATION_FAILED") or "") == "true":
                continue
            try:
                date = dt.date.fromisoformat((row.get("DATE") or "")[:10])
            except ValueError:
                self.skipped += 1
                continue
            columns = STRESS_COLUMNS if status == "READY" else STRESS_COLUMNS[:1]
            fields: dict[str, FieldValue] = {
                column.lower(): parsed
                for column in columns
                if (parsed := value(row.get(column), positive=False)) is not None
            }
            if "stress_score" not in fields:
                continue
            fields["status"] = status
            night = date - dt.timedelta(days=1)
            tags = {"date": date.isoformat(), "night": night.isoformat()}
            yield self._point("stress_score", tags, self._civil(night, 12), fields)

    def heart_rate(self, rows: Iterable[dict[str, str]]) -> Iterator[Point]:
        """Each minute's heart rate, by source, from a file of second-level readings."""
        minutes: dict[tuple[int, str], list[float]] = {}
        for row in rows:
            try:
                moment = parse_time(row["timestamp"])
                bpm = float(row["beats per minute"])
            except AttributeError, KeyError, ValueError, TypeError:
                self.skipped += 1
                continue
            if not math.isfinite(bpm) or bpm <= 0:
                continue
            minute = to_ms(moment) // 60_000 * 60_000
            source = (row.get("data source") or "unknown").strip()
            minutes.setdefault((minute, source), []).append(bpm)
        for (minute, source), readings in sorted(minutes.items()):
            fields: dict[str, FieldValue] = {
                "mean": round(sum(readings) / len(readings), 3),
                "min": min(readings),
                "max": max(readings),
                "count": float(len(readings)),
            }
            yield self._point("heart_rate_minute", {"source": source}, minute, fields)


def file_kind(relative: str, heart_rate: bool) -> str | None:
    """What an export file is, by its path from ``Google Health/``: ``sleeps``, ``stages``,
    ``scores``, ``stress``, ``heart_rate``, a ``Physical Activity`` family's name, or None when
    it is not imported."""
    if SLEEPS.match(relative):
        return "sleeps"
    if STAGES.match(relative):
        return "stages"
    if SCORES.match(relative):
        return "scores"
    if relative == STRESS:
        return "stress"
    if HEART_RATE.match(relative):
        return "heart_rate" if heart_rate else None
    parts = relative.split("/")
    if len(parts) == 3 and parts[0] + "/" == ACTIVITY and parts[1] in FAMILIES:
        return parts[1] if parts[2].endswith(".csv") else None
    return None


class Export:
    """The imported files of one or more zips of exports, by their path from ``Google Health/``;
    where two zips hold the same path, the later one by name. Files are read from the zips one
    at a time and never extracted."""

    def __init__(self, paths: list[Path], heart_rate: bool) -> None:
        self._zips: list[zipfile.ZipFile] = []
        self.members: dict[str, tuple[zipfile.ZipFile, zipfile.ZipInfo]] = {}
        self.failed: list[tuple[Path, str]] = []
        self.paths: list[Path] = []
        for path in sorted(paths):
            try:
                archive = zipfile.ZipFile(path)
            except (OSError, zipfile.BadZipFile) as error:
                self.failed.append((path, f"not a zip ({error})"))
                continue
            found = 0
            for member in archive.infolist():
                at = member.filename.find(ROOT)
                if member.is_dir() or at < 0:
                    continue
                relative = member.filename[at + len(ROOT) :]
                if ".." in relative.split("/"):
                    continue
                found += 1
                if file_kind(relative, heart_rate):
                    self.members[relative] = (archive, member)
            if not found:
                archive.close()
                self.failed.append((path, f"no {ROOT.rstrip('/')} folder in it"))
                continue
            self._zips.append(archive)
            self.paths.append(path)

    def files(self) -> list[str]:
        """The imported paths: the sleep sessions first, so the files that join to them find
        them."""
        return sorted(self.members, key=lambda r: (not SLEEPS.match(r), r))

    def stage_offsets(self) -> dict[str, str]:
        """Each session's UTC offset as its stages give it, the most common among them, by
        ``sleep_id``; sessions whose stages cannot be read are left out."""
        counts: dict[str, Counter[str]] = {}
        for relative in self.members:
            if not STAGES.match(relative):
                continue
            try:
                for row in self.rows(relative):
                    sleep_id = (row.get("sleep_id") or "").strip()
                    if sleep_id and (offset := (row.get("start_utc_offset") or "").strip()):
                        counts.setdefault(sleep_id, Counter())[offset] += 1
            except ExportError as error:
                logger.warning("cannot read the stages' offsets: %s", error)
        return {sleep_id: c.most_common(1)[0][0] for sleep_id, c in counts.items()}

    def fingerprint(self, relative: str) -> str:
        member = self.members[relative][1]
        return f"{member.file_size}:{member.CRC:08x}"

    def rows(self, relative: str) -> Iterator[dict[str, str]]:
        """A file's rows, by column; raises :class:`ExportError` when it cannot be read."""
        archive, member = self.members[relative]
        try:
            with archive.open(member) as raw:
                text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
                yield from csv.DictReader(text, strict=True)
        except (OSError, zipfile.BadZipFile, EOFError, zlib.error, UnicodeDecodeError) as error:
            raise ExportError(f"{relative}: {error}") from error
        except csv.Error as error:
            raise ExportError(f"{relative}: not CSV ({error})") from error

    def close(self) -> None:
        for archive in self._zips:
            archive.close()


def _batches(points: Iterable[Point]) -> Iterator[list[Point]]:
    batch: list[Point] = []
    for point in points:
        batch.append(point)
        if len(batch) >= BATCH:
            yield batch
            batch = []
    if batch:
        yield batch


class TakeoutSource:
    """Imports Google Takeout exports dropped into a folder, or handed to it through an upload."""

    file_suffixes = (".zip",)
    upload_max_bytes = UPLOAD_MAX_BYTES

    def __init__(self, params: TakeoutParams) -> None:
        self._params = params
        self.inbox = params.inbox
        self._woken = asyncio.Event()

    def wake(self) -> None:
        """Scan now rather than at the next scan, for a file just saved in the inbox."""
        self._woken.set()

    async def run(self, context: SourceContext) -> None:
        """Import the zips in the inbox every ``scan_minutes`` and whenever woken, for ever."""
        self.inbox.mkdir(parents=True, exist_ok=True)
        while True:
            self._woken.clear()
            if await self.scan(context):
                context.succeeded()
            await wait_or_wake(context, self._woken, self._params.scan_minutes * 60)

    async def scan(self, context: SourceContext) -> bool:
        """Import every zip waiting in the inbox as one export; False when a batch was refused,
        leaving them in the inbox."""
        zips = sorted(self.inbox.glob("*.zip"))
        if not zips:
            return True
        export = await asyncio.to_thread(Export, zips, self._params.heart_rate_minutes)
        try:
            for path, reason in export.failed:
                context.problem(f"cannot read {path.name}: {reason}; moved to {FAILED}/")
                logger.error("cannot read %s: %s; moving it to %s/", path.name, reason, FAILED)
                move_aside(path, FAILED)
            imported = await self.import_export(context, export)
        finally:
            export.close()
        if not imported:
            names = ", ".join(p.name for p in export.paths)
            context.problem(f"a batch was refused; {names} stay in the inbox")
            return False
        for path in export.paths:
            move_aside(path, IMPORTED)
        return True

    async def import_export(self, context: SourceContext, export: Export) -> bool:
        """Import every file of an export not imported before, as its fingerprint says; False
        when a batch was refused."""
        state_path = context.state_dir / STATE_FILE
        state = _load_state(state_path)
        offsets = await asyncio.to_thread(export.stage_offsets)
        builder = Points(self._params, state, offsets)
        changed = skipped = 0
        try:
            for relative in export.files():
                kind = file_kind(relative, self._params.heart_rate_minutes)
                if not self._wanted(kind):
                    continue
                fingerprint = export.fingerprint(relative)
                if kind == "sleeps" and not self._params.api_data:
                    # Read for their nights only; written once api_data is set again.
                    fingerprint += ":nights"
                if state.files.get(relative) == fingerprint:
                    skipped += 1
                    continue
                points = self._points(builder, relative, export)
                try:
                    for batch in _batches(points):
                        if not await context.emit(batch):
                            return False
                except ExportError as error:
                    context.problem(f"cannot read {error}")
                    logger.error("cannot read %s", error)
                state.files[relative] = fingerprint
                changed += 1
        finally:
            if changed:
                _save_state(state_path, state)
        if builder.skipped:
            logger.warning("skipped %d row(s) that could not be read", builder.skipped)
        if builder.duplicates:
            logger.info("left out %d sleep session(s) overlapping a longer one", builder.duplicates)
        if builder.corrected:
            logger.info("corrected the times of %d sleep(s) and score(s)", builder.corrected)
        if changed:
            summary = ", ".join(f"{n} {t}" for t, n in sorted(builder.counts.items())) or "nothing"
            names = ", ".join(p.name for p in export.paths)
            logger.info(
                "imported %s: %d file(s), %d unchanged: %s", names, changed, skipped, summary
            )
        return True

    def _wanted(self, kind: str | None) -> bool:
        """Whether files of a kind are imported: all, or with ``api_data`` false, those the
        Google Health API does not offer, and the sleep sessions for their scores' nights."""
        if self._params.api_data or kind in ("sleeps", "scores", "stress"):
            return True
        return kind in FAMILIES and not FAMILIES[kind].api

    def _points(self, builder: Points, relative: str, export: Export) -> Iterator[Point]:
        kind = file_kind(relative, self._params.heart_rate_minutes)
        rows = export.rows(relative)
        if kind == "sleeps":
            return builder.sleeps(rows)
        if kind == "stages":
            return builder.stages(rows)
        if kind == "scores":
            return builder.scores(rows)
        if kind == "stress":
            return builder.stress(rows)
        if kind == "heart_rate":
            return builder.heart_rate(rows)
        assert kind is not None
        return builder.family(kind, rows)


def _load_state(path: Path) -> State:
    """The state kept so far, or an empty one when it cannot be read."""
    try:
        data: Any = json.loads(path.read_text(encoding="utf-8"))
        files = {str(k): str(v) for k, v in data["files"].items()}
        sleeps = {
            str(k): Sleep(
                str(v[0]),
                int(v[1]),
                int(v[2]),
                float(v[3]),
                bool(v[4]),
                bool(v[5]),
                str(v[6]),
                bool(v[7]),
            )
            for k, v in data["sleeps"].items()
        }
        return State(files, sleeps)
    except FileNotFoundError:
        return State()
    except (OSError, ValueError, KeyError, TypeError, AttributeError, IndexError) as error:
        logger.error("cannot read %s (%s); importing every file again", path, error)
        return State()


def _save_state(path: Path, state: State) -> None:
    """Keep the state, written atomically."""
    sleeps = {
        k: [s.night, s.start_ms, s.end_ms, s.minutes, s.main, s.duplicate, s.version, s.staged]
        for k, s in sorted(state.sleeps.items())
    }
    write_json(path, {"files": dict(sorted(state.files.items())), "sleeps": sleeps})


def build(params: TakeoutParams, _client: httpx2.AsyncClient, _name: str = "") -> TakeoutSource:
    """A Google Takeout source from its validated parameters."""
    return TakeoutSource(params)


__all__ = ["TakeoutParams", "TakeoutSource", "build", "snake", "value"]

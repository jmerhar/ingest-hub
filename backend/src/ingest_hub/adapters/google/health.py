"""Google Health data from the Google Health API, polled as one person who has consented.

The person consents once with ``ingest-hub google-auth --source NAME`` (see ``oauth.py``), which
keeps a refresh token in the source's state directory. Every ``poll_minutes`` the source reads
each data type below from its cursor minus ``rescan_hours`` up to now (the first time, from
``history_days`` back), since watches sync late and Google revises what it has, and writes the
same tables, fields and tags as the ``google_takeout`` source, so a poll and an export of the
same data land on the same series. Every point is tagged ``person``.

Each data type is written to the table of its name in snake case, its values under Google's names
in snake case, as the export names them where the two differ:

- daily values, tagged ``date``: ``daily-resting-heart-rate`` and ``daily-vo2-max``, and, tagged
  ``night`` too (the date before), ``daily-heart-rate-variability``, ``daily-oxygen-saturation``,
  ``daily-respiratory-rate`` and ``daily-sleep-temperature-derivations``;
- samples: ``steps``, tagged ``source`` (the device that recorded them, else the platform), since
  the watch and the phone both count them; ``weight``, ``body-fat``, and, tagged ``night``,
  ``oxygen-saturation``, ``heart-rate-variability`` and ``respiratory-rate-sleep-summary``;
- ``sleep`` sessions into ``sleep`` and their stages into ``sleep_stage``, tagged ``night``;
- with ``heart_rate_minutes``, ``heart-rate`` as each minute's ``mean``, ``min``, ``max`` and
  ``count`` in ``heart_rate_minute``, tagged ``source``.

Daily values are stamped like the export's: at noon of their night, or at midnight of their date,
in ``timezone``. Samples carry their own UTC offset, which gives their night. A sleep session
still being processed (``metadata.processed`` false) is left for a later poll, so its stages are
written once they are final; ``main`` and the leaving out of a session that overlaps a longer one
follow the export's rules (see ``common.py``). Values of zero or less in measurements, which Google
uses for "none", are left out. Sleep score, readiness, stress and the cardio workload ratio are not
in the API; the export carries them.

A refused sign-in (withdrawn, unused for six months, or from an OAuth app still in "Testing") or a
missing one is reported as such on every poll, with the command that fixes it, until the consent
is given again.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import math
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx2
from jigkit.sinks import FieldValue, Point, Precision
from pydantic import BaseModel, ConfigDict, Field, field_validator

from ingest_hub.adapters.google.api import API_URL, ApiError, HealthApi
from ingest_hub.adapters.google.common import (
    Sleep,
    parse_seconds,
    parse_time,
    resolve_sleeps,
    to_ms,
)
from ingest_hub.adapters.google.oauth import (
    TOKEN_URL,
    AuthError,
    Credentials,
    TokenRefused,
    load_token,
)
from ingest_hub.adapters.inbox import write_json
from ingest_hub.adapters.night import night_of
from ingest_hub.collect import SourceContext
from ingest_hub.config import read_secret

logger = logging.getLogger(__name__)

#: Points per batch handed to delivery.
BATCH = 5000
#: The cursors and the sleeps seen, in the source's state directory.
STATE_FILE = "google-health.json"
#: The days one request covers; heart rate, a reading a second, a day at a time.
CHUNK_DAYS = 7
HEART_RATE_CHUNK_DAYS = 1
SLEEP_SUMMARY_FIELDS = {
    "minutesInSleepPeriod": "minutes_in_sleep_period",
    "minutesAfterWakeUp": "minutes_after_wake_up",
    "minutesToFallAsleep": "minutes_to_fall_asleep",
    "minutesAsleep": "minutes_asleep",
    "minutesAwake": "minutes_awake",
}


@dataclass(frozen=True)
class DataType:
    """How one Google Health data type is read and written.

    ``record`` is the API's record type: ``daily`` (a civil date), ``sample`` (a moment),
    ``interval`` (a start and end) or ``session``. ``fields`` maps a value's path in the data
    point, dot-separated, to its field. ``night`` tags a daily value with the night before its
    date, or a sample with the night it was taken in; ``positive`` leaves out numbers of zero or
    less, which Google uses for "none"; ``source`` tags a sample with the device that recorded it,
    for the data several devices record at once.
    """

    name: str
    record: str
    fields: dict[str, str]
    night: bool = False
    positive: bool = True
    source: bool = False

    @property
    def key(self) -> str:
        """The data type's key in a data point: ``daily-vo2-max`` → ``dailyVo2Max``."""
        first, *rest = self.name.split("-")
        return first + "".join(part.capitalize() for part in rest)

    @property
    def table(self) -> str:
        return self.name.replace("-", "_")


def _stats(stage: str) -> dict[str, str]:
    """A respiratory rate summary's statistics for one stage, named as the export names them."""
    prefix = f"{stage}_sleep_stats_"
    return {
        f"{stage}SleepStats.breathsPerMinute": prefix + "breaths_per_minute",
        f"{stage}SleepStats.standardDeviation": prefix + "standard_deviation_breaths_per_minute",
        f"{stage}SleepStats.signalToNoise": prefix + "signal_to_noise",
    }


#: Sleep first, so the respiratory rate summaries of the same poll find their session's night.
DATA_TYPES = (
    DataType("sleep", "session", {}),
    DataType(
        "daily-heart-rate-variability",
        "daily",
        {
            "averageHeartRateVariabilityMilliseconds": (
                "average_heart_rate_variability_milliseconds"
            ),
            "nonRemHeartRateBeatsPerMinute": "non_rem_heart_rate_beats_per_minute",
            "entropy": "entropy",
            "deepSleepRootMeanSquareOfSuccessiveDifferencesMilliseconds": (
                "deep_sleep_root_mean_square_of_successive_differences_milliseconds"
            ),
        },
        night=True,
    ),
    DataType(
        "daily-oxygen-saturation",
        "daily",
        {
            "averagePercentage": "average_percentage",
            "lowerBoundPercentage": "lower_bound_percentage",
            "upperBoundPercentage": "upper_bound_percentage",
            "standardDeviationPercentage": "standard_deviation_percentage",
        },
        night=True,
    ),
    DataType(
        "daily-respiratory-rate", "daily", {"breathsPerMinute": "breaths_per_minute"}, night=True
    ),
    DataType(
        "daily-sleep-temperature-derivations",
        "daily",
        {
            "nightlyTemperatureCelsius": "nightly_temperature_celsius",
            "baselineTemperatureCelsius": "baseline_temperature_celsius",
            "relativeNightlyStddev30dCelsius": "relative_nightly_stddev_30d_celsius",
        },
        night=True,
    ),
    DataType("daily-resting-heart-rate", "daily", {"beatsPerMinute": "beats_per_minute"}),
    DataType(
        "daily-vo2-max",
        "daily",
        {
            "vo2Max": "vo2_max",
            "vo2MaxCovariance": "vo2_max_covariance",
            "estimated": "estimated",
            "cardioFitnessLevel": "cardio_fitness_level",
        },
    ),
    DataType(
        "oxygen-saturation", "sample", {"percentage": "oxygen_saturation_percentage"}, night=True
    ),
    DataType(
        "heart-rate-variability",
        "sample",
        {
            "rootMeanSquareOfSuccessiveDifferencesMilliseconds": (
                "root_mean_square_of_successive_differences_milliseconds"
            ),
            "standardDeviationMilliseconds": "standard_deviation_milliseconds",
        },
        night=True,
    ),
    DataType(
        "respiratory-rate-sleep-summary",
        "sample",
        {**_stats("deep"), **_stats("light"), **_stats("rem"), **_stats("full")},
        night=True,
    ),
    DataType("weight", "sample", {"weightGrams": "weight_grams"}),
    DataType("body-fat", "sample", {"percentage": "body_fat_percentage"}),
    DataType("steps", "interval", {"count": "steps"}, positive=False, source=True),
)
HEART_RATE = DataType("heart-rate", "sample", {"beatsPerMinute": "beats_per_minute"})


class NotSignedIn(AuthError):
    """No consent has been given for the source yet."""


class GoogleHealthParams(BaseModel):
    """A Google Health API source's ``params`` in the configuration file."""

    model_config = ConfigDict(extra="forbid")

    #: Whose data it is, written as the ``person`` tag of every point.
    person: str = Field(pattern=r"^[A-Za-z0-9_.-]+$")
    #: The OAuth client (a "Desktop app" client of the Google Cloud project).
    client_id_file: Path
    client_secret_file: Path
    table_prefix: str = Field("google_", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    #: The zone civil dates are placed in.
    timezone: str = "UTC"
    poll_minutes: float = Field(60, gt=0)
    #: How far back the first poll reads.
    history_days: int = Field(30, ge=1)
    #: How far behind its cursor each poll reads again.
    rescan_hours: float = Field(72, ge=0)
    #: Also read heart rate, written as each minute's mean, minimum and maximum.
    heart_rate_minutes: bool = False
    url: str = API_URL
    token_url: str = TOKEN_URL

    @field_validator("timezone")
    @classmethod
    def _known_zone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ValueError(f"unknown time zone {value!r}") from error
        return value


def credentials_of(params: GoogleHealthParams) -> Credentials:
    """The OAuth client's credentials, read from their files."""
    return Credentials(read_secret(params.client_id_file), read_secret(params.client_secret_file))


def _get(data: Any, path: str) -> Any:
    """A value at a dot-separated path, or None."""
    for part in path.split("."):
        if not isinstance(data, dict):
            return None
        data = data.get(part)
    return data


def _value(raw: Any, *, positive: bool) -> FieldValue | None:
    """A JSON value as a field: numbers (the API gives 64-bit integers as text) as floats,
    booleans and text as they are; None for anything else, and with ``positive`` for numbers of
    zero or less."""
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int | float) or (isinstance(raw, str) and _is_number(raw)):
        number = float(raw)
        if not math.isfinite(number) or (positive and number <= 0):
            return None
        return number
    if isinstance(raw, str) and 0 < len(raw) <= 64:
        return raw
    return None


def _is_number(text: str) -> bool:
    try:
        float(text)
    except ValueError:
        return False
    return True


def source_of(point: dict[str, Any]) -> str:
    """The ``source`` tag of a data point: the device that recorded it, else the platform."""
    data_source = point.get("dataSource") or {}
    device = data_source.get("device") or {}
    name = str(device.get("displayName") or "").strip()
    return name or str(data_source.get("platform") or "unknown")


def _date(raw: Any) -> dt.date:
    """A ``google.type.Date``; raises ValueError when it is not one."""
    try:
        return dt.date(int(raw["year"]), int(raw["month"]), int(raw["day"]))
    except (KeyError, TypeError) as error:
        raise ValueError(f"not a date: {raw!r}") from error


@dataclass
class State:
    """Each data type's cursor (a date for daily types, Unix milliseconds for the rest) and the
    sleep sessions seen, by their ID."""

    cursors: dict[str, str] = field(default_factory=dict)
    sleeps: dict[str, Sleep] = field(default_factory=dict)


class Points:
    """Turns data points into points."""

    def __init__(self, params: GoogleHealthParams, state: State) -> None:
        self._prefix = params.table_prefix
        self._person = params.person
        self._zone = ZoneInfo(params.timezone)
        self._state = state
        self.counts: Counter[str] = Counter()
        self.skipped = 0
        self.duplicates = 0
        self.unprocessed = 0

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
        return to_ms(dt.datetime.combine(date, dt.time(hour), self._zone))

    def _fields(self, kind: DataType, data: dict[str, Any]) -> dict[str, FieldValue]:
        return {
            name: value
            for path, name in kind.fields.items()
            if (value := _value(_get(data, path), positive=kind.positive)) is not None
        }

    def build(self, kind: DataType, points: Iterable[dict[str, Any]]) -> list[Point]:
        """The points a page of data points of one type carries."""
        if kind.record == "session":
            return self._sleeps(points)
        out: list[Point] = []
        by_end = self._state_by_end() if kind.name == "respiratory-rate-sleep-summary" else {}
        for point in points:
            data = point.get(kind.key)
            if not isinstance(data, dict) or not (fields := self._fields(kind, data)):
                continue
            try:
                if kind.record == "daily":
                    date = _date(data.get("date"))
                    if kind.night:
                        night = date - dt.timedelta(days=1)
                        tags = {"date": date.isoformat(), "night": night.isoformat()}
                        out.append(self._point(kind.table, tags, self._civil(night, 12), fields))
                    else:
                        tags = {"date": date.isoformat()}
                        out.append(self._point(kind.table, tags, self._civil(date, 0), fields))
                    continue
                when = data.get("sampleTime") if kind.record == "sample" else data.get("interval")
                moment = parse_time(
                    str(_get(when, "physicalTime" if kind.record == "sample" else "startTime"))
                )
                offset_key = "utcOffset" if kind.record == "sample" else "startUtcOffset"
                offset = parse_seconds(str(_get(when, offset_key) or "0s"))
            except ValueError:
                self.skipped += 1
                continue
            tags = {"source": source_of(point)} if kind.source else {}
            if kind.night:
                local = (moment + offset).replace(tzinfo=None)
                tags["night"] = by_end.get(to_ms(moment)) or night_of(local)
            out.append(self._point(kind.table, tags, to_ms(moment), fields))
        return out

    def _state_by_end(self) -> dict[int, str]:
        return {sleep.end_ms: sleep.night for sleep in self._state.sleeps.values()}

    def heart_rate(self, points: Iterable[dict[str, Any]]) -> list[Point]:
        """Each minute's heart rate, by source, from second-level readings."""
        minutes: dict[tuple[int, str], list[float]] = {}
        for point in points:
            data = point.get(HEART_RATE.key) or {}
            try:
                moment = parse_time(str(_get(data, "sampleTime.physicalTime")))
                bpm = float(data["beatsPerMinute"])
            except ValueError, KeyError, TypeError:
                self.skipped += 1
                continue
            if bpm > 0:
                minute = to_ms(moment) // 60_000 * 60_000
                minutes.setdefault((minute, source_of(point)), []).append(bpm)
        return [
            self._point(
                "heart_rate_minute",
                {"source": source},
                minute,
                {
                    "mean": round(sum(readings) / len(readings), 3),
                    "min": min(readings),
                    "max": max(readings),
                    "count": float(len(readings)),
                },
            )
            for (minute, source), readings in sorted(minutes.items())
        ]

    def _sleeps(self, points: Iterable[dict[str, Any]]) -> list[Point]:
        """Sleep sessions and their stages, with ``main`` updates for the nights they touch."""
        sessions: list[tuple[str, dict[str, Any], Sleep]] = []
        for point in points:
            data = point.get("sleep") or {}
            if not (data.get("metadata") or {}).get("processed"):
                self.unprocessed += 1
                continue
            try:
                interval = data["interval"]
                start = parse_time(str(interval["startTime"]))
                end = parse_time(str(interval["endTime"]))
                offset = parse_seconds(str(interval.get("startUtcOffset") or "0s"))
                sleep_id = str(point.get("name") or "").rsplit("/", 1)[-1]
                minutes = float((data.get("summary") or {}).get("minutesInSleepPeriod") or 0)
            except KeyError, TypeError, ValueError:
                self.skipped += 1
                continue
            if not sleep_id:
                self.skipped += 1
                continue
            night = night_of((start + offset).replace(tzinfo=None))
            data = {**data, "recordingMethod": _get(point, "dataSource.recordingMethod")}
            staged = str(data.get("type") or "") == "STAGES"
            sleep = Sleep(night, to_ms(start), to_ms(end), minutes, staged=staged)
            sessions.append((sleep_id, data, sleep))
        known = self._state.sleeps
        for sleep_id, _, sleep in sessions:
            known[sleep_id] = sleep
        nights = {sleep.night for _, _, sleep in sessions}
        main = resolve_sleeps(known, nights)
        out: list[Point] = []
        seen = set()
        for sleep_id, data, sleep in sessions:
            seen.add(sleep_id)
            if sleep.duplicate:
                self.duplicates += 1
                continue
            sleep.main = sleep_id == main.get(sleep.night)
            fields: dict[str, FieldValue] = {"main": sleep.main}
            summary = data.get("summary") or {}
            for key, name in SLEEP_SUMMARY_FIELDS.items():
                if (value := _value(summary.get(key), positive=False)) is not None:
                    fields[name] = value
            if kind := str(data.get("type") or "").strip():
                fields["type"] = kind
            if method := str(data.get("recordingMethod") or "").strip():
                fields["data_source"] = method
            out.append(self._point("sleep", {"night": sleep.night}, sleep.start_ms, fields))
            out.extend(self._stages(sleep.night, data.get("stages") or []))
        for sleep_id, sleep in known.items():
            if sleep_id in seen or sleep.night not in nights or sleep.duplicate:
                continue
            is_main = sleep_id == main.get(sleep.night)
            if is_main != sleep.main:
                sleep.main = is_main
                out.append(
                    self._point("sleep", {"night": sleep.night}, sleep.start_ms, {"main": is_main})
                )
        return out

    def _stages(self, night: str, stages: list[Any]) -> list[Point]:
        out: list[Point] = []
        for stage in stages:
            try:
                start = parse_time(str(stage["startTime"]))
                end = parse_time(str(stage["endTime"]))
                kind = str(stage["type"]).strip().lower()
            except KeyError, TypeError, ValueError:
                self.skipped += 1
                continue
            seconds = (end - start).total_seconds()
            if not kind or seconds < 0:
                self.skipped += 1
                continue
            out.append(
                self._point(
                    "sleep_stage",
                    {"night": night, "stage": kind},
                    to_ms(start),
                    {"seconds": seconds},
                )
            )
        return out


def _batches(points: list[Point]) -> Iterable[list[Point]]:
    for start in range(0, len(points), BATCH):
        yield points[start : start + BATCH]


class GoogleHealthSource:
    """Polls one person's Google Health data."""

    def __init__(
        self,
        params: GoogleHealthParams,
        client: httpx2.AsyncClient,
        name: str,
        credentials: Credentials,
    ) -> None:
        self._params = params
        self._client = client
        self._name = name
        self._credentials = credentials
        self._zone = ZoneInfo(params.timezone)
        self._types = DATA_TYPES + ((HEART_RATE,) if params.heart_rate_minutes else ())

    async def run(self, context: SourceContext) -> None:
        """Poll every ``poll_minutes`` for ever, reporting a failed poll rather than raising."""
        fix = f"run `ingest-hub google-auth --source {self._name}`"
        while True:
            try:
                await self.poll(context)
            except NotSignedIn:
                context.problem(f"not signed in to Google yet: {fix}")
                logger.warning("%s: not signed in to Google yet: %s", self._name, fix)
            except TokenRefused as error:
                context.problem(f"{error}: {fix}")
                logger.error("%s: %s: %s", self._name, error, fix)
            except (ApiError, AuthError, httpx2.HTTPError, ValueError) as error:
                message = str(error) or type(error).__name__
                context.problem(message)
                logger.warning("%s: %s", self._name, message)
            else:
                context.succeeded()
            await context.sleep(self._params.poll_minutes * 60)

    async def poll(self, context: SourceContext) -> None:
        """Read every data type from its cursor minus ``rescan_hours`` up to now."""
        token = load_token(context.state_dir)
        if token is None:
            raise NotSignedIn("no consent")
        api = HealthApi(
            self._client,
            self._credentials,
            token,
            url=self._params.url,
            token_url=self._params.token_url,
            clock=context.clock,
            sleep=context.sleep,
        )
        path = context.state_dir / STATE_FILE
        state = _load_state(path)
        builder = Points(self._params, state)
        now = dt.datetime.fromtimestamp(context.clock(), dt.UTC)
        try:
            for kind in self._types:
                if kind.record == "daily":
                    await self._daily(context, api, builder, state, kind, now)
                else:
                    await self._timed(context, api, builder, state, kind, now)
        finally:
            _save_state(path, state)
        summary = ", ".join(f"{n} {t}" for t, n in sorted(builder.counts.items())) or "nothing"
        logger.info("%s: %d request(s): %s", self._name, api.requests, summary)
        if builder.unprocessed:
            logger.info("%s: %d sleep(s) still being processed", self._name, builder.unprocessed)

    async def _emit(self, context: SourceContext, points: list[Point]) -> None:
        for batch in _batches(points):
            if not await context.emit(batch):
                raise ApiError("a batch was refused")

    async def _daily(
        self,
        context: SourceContext,
        api: HealthApi,
        builder: Points,
        state: State,
        kind: DataType,
        now: dt.datetime,
    ) -> None:
        today = now.astimezone(self._zone).date()
        rescan = dt.timedelta(days=math.ceil(self._params.rescan_hours / 24))
        cursor = state.cursors.get(kind.name)
        start = (
            dt.date.fromisoformat(cursor) - rescan
            if cursor
            else today - dt.timedelta(days=self._params.history_days)
        )
        end = today + dt.timedelta(days=1)
        date = f"{kind.table}.date"
        filter_ = f'{date} >= "{start.isoformat()}" AND {date} < "{end.isoformat()}"'
        points = [p async for p in api.data_points(kind.name, filter_)]
        await self._emit(context, builder.build(kind, points))
        state.cursors[kind.name] = today.isoformat()

    async def _timed(
        self,
        context: SourceContext,
        api: HealthApi,
        builder: Points,
        state: State,
        kind: DataType,
        now: dt.datetime,
    ) -> None:
        cursor = state.cursors.get(kind.name)
        start = (
            dt.datetime.fromtimestamp(int(cursor) / 1000, dt.UTC)
            - dt.timedelta(hours=self._params.rescan_hours)
            if cursor
            else now - dt.timedelta(days=self._params.history_days)
        )
        step = dt.timedelta(days=HEART_RATE_CHUNK_DAYS if kind is HEART_RATE else CHUNK_DAYS)
        field_name = {
            "sample": f"{kind.table}.sample_time.physical_time",
            "interval": f"{kind.table}.interval.start_time",
            "session": f"{kind.table}.interval.end_time",
        }[kind.record]
        while start < now:
            end = min(start + step, now)
            filter_ = f'{field_name} >= "{_rfc3339(start)}" AND {field_name} < "{_rfc3339(end)}"'
            points = [p async for p in api.data_points(kind.name, filter_)]
            built = (
                builder.heart_rate(points) if kind is HEART_RATE else builder.build(kind, points)
            )
            await self._emit(context, built)
            state.cursors[kind.name] = str(to_ms(end))
            start = end


def _rfc3339(moment: dt.datetime) -> str:
    return moment.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_state(path: Path) -> State:
    """The state kept so far, or an empty one when it cannot be read."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        cursors = {str(k): str(v) for k, v in data["cursors"].items()}
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
        return State(cursors, sleeps)
    except FileNotFoundError:
        return State()
    except (OSError, ValueError, KeyError, TypeError, AttributeError, IndexError) as error:
        logger.error("cannot read %s (%s); reading from history_days back", path, error)
        return State()


def _save_state(path: Path, state: State) -> None:
    sleeps = {
        k: [s.night, s.start_ms, s.end_ms, s.minutes, s.main, s.duplicate, s.version, s.staged]
        for k, s in sorted(state.sleeps.items())
    }
    write_json(path, {"cursors": dict(sorted(state.cursors.items())), "sleeps": sleeps})


def build(
    params: GoogleHealthParams, client: httpx2.AsyncClient, name: str = ""
) -> GoogleHealthSource:
    """A Google Health API source from its validated parameters, with its OAuth client's
    credentials read now."""
    return GoogleHealthSource(params, client, name, credentials_of(params))


__all__ = ["DATA_TYPES", "GoogleHealthParams", "GoogleHealthSource", "build"]

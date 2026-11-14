"""ResMed CPAP data, imported from a copy of the device's SD card.

An AirSense 10 or 11 writes everything it records to its SD card, as EDF files (see ``edf.py``):

- ``STR.edf``: one record per day, from noon to noon, with the day's summary: mask-on and
  mask-off times, therapy minutes, the indices (AHI and its parts), percentiles of pressure, leak
  and breathing, and the day's settings;
- ``DATALOG/<YYYYMMDD>/``: each therapy session's files, named by the session's start:
  ``_EVE.edf`` and ``_CSL.edf`` hold annotations (apneas, hypopneas, arousals, Cheyne-Stokes
  respiration), ``_PLD.edf`` pressure, leak and breathing every two seconds, ``_SA2.edf`` pulse
  and SpO2 every second from an oximeter, and ``_BRP.edf`` the flow and pressure waveforms at
  25 Hz, which are not imported.

What is written, all at millisecond precision and tagged ``person`` (whose CPAP it is, from
the configuration), ``device`` (the serial number the files carry) and ``night``:

- ``<prefix>night``: one point per STR day, at its start (noon), every value of the day as a
  field: its signal's label in snake case (``Leak.95`` → ``leak_95``, ``S.EPR.Level`` →
  ``s_epr_level``), in the file's units. A day without therapy keeps only the settings
  (``s_*``, ``mode``, ``humidifier``, ``heated_tube``);
- ``<prefix>mask``: one point per mask-on period of a day, at its start, field ``minutes``;
  periods that start in the same minute are one point, their minutes added;
- ``<prefix>event``: one point per annotation, at its time, tag ``type`` (the annotation in snake
  case: ``obstructive_apnea``, ``central_apnea``, ``hypopnea``, ``apnea``, ``arousal``), field
  ``duration`` in seconds, 0 when the device gives none. ``Recording starts`` is left out;
- ``<prefix>detail``: the PLD signals every two seconds, named like the night's without their
  rate (``MaskPress.2s`` → ``mask_press``);
- ``<prefix>oximetry``: SA2's ``pulse`` and ``spo2``, only when an oximeter recorded them.

A digital value of -1, or one outside its signal's range, is a missing value and left out, so
neither missing values nor days without therapy become zeros.

``night`` is ResMed's own day, ``YYYY-MM-DD``: a session belongs to the date of the noon before
it started, so a night that starts in the evening and ends the next morning is one night, as on
the device and in its folders.

The device's clock has no zone in the files. Times are read in ``timezone``, an IANA zone or a
fixed offset such as ``+01:00``; unset, the card's own ``TimeZoneOffset`` setting is used, or
for a copy without it the last card's, or UTC when no card has had one. That setting is a fixed
offset, which an AirSense 11 keeps all year without summer time, so it reads the files' times
as the device meant them. A point's time depends on the zone, so a different zone for a night
already imported writes a second copy of it beside the first.

The device's clock drifts, and it has no setting to correct it. ``clock`` lists moments it was
compared with the true time, such as a session started at a noted time, whose file names give the
device's side; the difference is interpolated between them and extended in a line beyond them.
Mask periods, events and the series are corrected; a night's summary keeps the device's noon, and
``night`` the device's own day. Each night keeps the correction it was first imported with, kept
in the state directory, so importing it again writes the same points; an anchor added later
corrects only the nights imported after it.

A card arrives either as a zip in ``inbox`` (uploaded on the status page, or dropped there),
moved to ``imported/`` beside it once its points are safe or to ``failed/`` when it is not a card
that can be read, or as a folder holding a copy of the card, ``card``, which is read in place and
never changed. A full card is too large to upload, so leave the BRP files out of the zip
(``zip -r card.zip . -x '*_BRP.edf'``); the source skips them in any case.

A card keeps every night it has, so each copy repeats the nights before it. The source keeps, in
its state directory, a fingerprint (size and CRC-32) of every file it has imported and skips a
file whose fingerprint is unchanged; a file that changed, such as STR.edf after another night, is
imported again, writing the same points over the old ones. Delete ``resmed-imported.json`` from
the state directory to import everything again.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
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

from ingest_hub.adapters.inbox import FAILED, IMPORTED, move_aside, wait_or_wake, write_json
from ingest_hub.adapters.night import night_of
from ingest_hub.adapters.resmed.edf import Edf, EdfError, read_edf
from ingest_hub.collect import SourceContext

logger = logging.getLogger(__name__)

#: Points per batch handed to delivery.
BATCH = 5000
#: The fingerprints of the files imported so far, in the source's state directory.
STATE_FILE = "resmed-imported.json"
#: The largest card file read from a zip; nothing the source imports comes near it.
MEMBER_MAX_BYTES = 64 * 1024 * 1024
#: The session files imported, by the kind their name ends in.
SESSION_FILE = re.compile(r"^\d{8}_\d{6}_(EVE|CSL|PLD|SA2)\.edf$", re.IGNORECASE)
#: A signal's rate at the end of its label: ``MaskPress.2s``, ``Flow.40ms``.
RATE = re.compile(r"\.\d+m?s$")
#: A word boundary inside a CamelCase label: ``MaskPress``, ``TgtIPAP``, ``Spo2Thresh``.
CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
#: A fixed offset from UTC: ``+01:00``.
OFFSET = re.compile(r"^([+-])(\d{2}):(\d{2})$")
SERIAL = re.compile(r"\bSRN=(\S+)")
#: STR signals that are not read: the day's date (its record says which day it is) and checksum.
STR_IGNORED = frozenset({"Date", "Crc16"})
#: STR signals that are not fields of a day: the mask times, written as ``mask`` points.
STR_SKIPPED = STR_IGNORED | {"MaskOn", "MaskOff"}
#: STR signals kept for a day without therapy, besides those starting ``S.``.
STR_SETTINGS = frozenset({"Mode", "Humidifier", "HeatedTube"})
#: Session signals that are not measurements.
SESSION_SKIPPED = frozenset({"Crc16", "EDF Annotations"})
#: Annotations that are not events.
NOT_EVENTS = frozenset({"recording starts"})
#: The table each kind of session file is written to.
SESSION_TABLES = {"EVE": "event", "CSL": "event", "PLD": "detail", "SA2": "oximetry"}


class CardError(Exception):
    """A file is not a card copy that can be read; the message says why."""


def parse_zone(text: str) -> dt.tzinfo:
    """A zone from an IANA name, ``UTC`` or a fixed offset such as ``+01:00``; raises ValueError
    when it is none of these."""
    if match := OFFSET.match(text):
        sign = -1 if match.group(1) == "-" else 1
        offset = dt.timedelta(hours=int(match.group(2)), minutes=int(match.group(3)))
        if offset >= dt.timedelta(hours=24):
            raise ValueError(f"offset {text!r} is out of range")
        return dt.timezone(sign * offset)
    if text.upper() == "UTC":
        return dt.UTC
    try:
        return ZoneInfo(text)
    except (ZoneInfoNotFoundError, ValueError) as error:
        raise ValueError(f"unknown time zone {text!r}") from error


class ClockAnchor(BaseModel):
    """A moment the device's clock was compared with the true local time."""

    model_config = ConfigDict(extra="forbid")

    #: What the device's clock said, as the card's file names give it.
    device: dt.datetime
    #: The true local time at that moment.
    actual: dt.datetime

    @field_validator("device", "actual")
    @classmethod
    def _local(cls, value: dt.datetime) -> dt.datetime:
        if value.tzinfo is not None:
            raise ValueError("a local time, without a zone")
        return value


def clock_correction(anchors: list[ClockAnchor], device: dt.datetime) -> dt.timedelta:
    """What to add to a moment on the device's clock to make it true: the anchors' differences,
    interpolated between them and extended in a line beyond them; one anchor's difference
    everywhere; nothing without anchors."""
    points = sorted(((a.device, a.actual - a.device) for a in anchors), key=lambda p: p[0])
    if not points:
        return dt.timedelta(0)
    if len(points) == 1:
        return points[0][1]
    index = 1
    while index < len(points) - 1 and device > points[index][0]:
        index += 1
    (t0, c0), (t1, c1) = points[index - 1], points[index]
    fraction = (device - t0) / (t1 - t0)
    return c0 + (c1 - c0) * fraction


class ResmedParams(BaseModel):
    """A ResMed SD card source's ``params`` in the configuration file."""

    model_config = ConfigDict(extra="forbid")

    #: Whose CPAP it is, written as the ``person`` tag of every point.
    person: str = Field(pattern=r"^[A-Za-z0-9_.-]+$")
    #: The folder zips of the card are dropped into; created when missing.
    inbox: Path
    #: A folder holding a copy of the card, read in place.
    card: Path | None = None
    table_prefix: str = Field("cpap_", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    scan_minutes: float = Field(10, gt=0)
    #: The zone the device's clock is read in; unset, the card's ``TimeZoneOffset``.
    timezone: str | None = None
    #: Moments the device's clock was checked against the true time, to correct its drift.
    clock: list[ClockAnchor] = []

    @field_validator("clock")
    @classmethod
    def _distinct(cls, value: list[ClockAnchor]) -> list[ClockAnchor]:
        if len({anchor.device for anchor in value}) != len(value):
            raise ValueError("two anchors at the same device time")
        return value

    @field_validator("timezone")
    @classmethod
    def _known_zone(cls, value: str | None) -> str | None:
        if value is not None:
            parse_zone(value)
        return value


def field_name(label: str) -> str:
    """A signal's label as a field name: its rate dropped, in snake case. ``S.EPR.EPRType`` →
    ``s_epr_epr_type``, ``MaskPress.2s`` → ``mask_press``, ``SpO2Thresh`` → ``spo2_thresh``."""
    name = RATE.sub("", label).replace("SpO2", "Spo2")
    name = CAMEL.sub("_", re.sub(r"[^A-Za-z0-9]+", "_", name))
    return re.sub(r"_+", "_", name).strip("_").lower()


def card_kind(relative: str) -> str | None:
    """What a file on the card is, by its path from the card's root: ``STR``, ``SETTINGS``, a
    session file's kind (``EVE``, ``CSL``, ``PLD``, ``SA2``), or None when it is not imported."""
    parts = relative.split("/")
    if len(parts) == 1 and parts[0].lower() == "str.edf":
        return "STR"
    if len(parts) == 2 and [p.lower() for p in parts] == ["settings", "currentsettings.json"]:
        return "SETTINGS"
    if len(parts) == 3 and parts[0].lower() == "datalog" and (m := SESSION_FILE.match(parts[2])):
        return m.group(1).upper()
    return None


def _order(relative: str) -> tuple[int, str]:
    """Settings first, then the day summaries, then the sessions in time order."""
    kind = card_kind(relative)
    return (0 if kind == "SETTINGS" else 1 if kind == "STR" else 2), relative.lower()


class Card:
    """The files of one copy of a card that the source imports, by their path from its root."""

    label: str

    def files(self) -> list[str]:
        """The paths of the files the source imports, settings and summaries first."""
        raise NotImplementedError

    def fingerprint(self, relative: str) -> str:
        """A file's size and CRC-32, which change whenever its content does."""
        raise NotImplementedError

    def read(self, relative: str) -> bytes:
        """A file's content."""
        raise NotImplementedError


class FolderCard(Card):
    """A card copied into a folder, read in place."""

    def __init__(self, root: Path) -> None:
        self.label = str(root)
        self._paths: dict[str, Path] = {}
        for path in root.rglob("*"):
            relative = path.relative_to(root).as_posix()
            if card_kind(relative) and path.is_file():
                self._paths[relative] = path

    def files(self) -> list[str]:
        return sorted(self._paths, key=_order)

    def fingerprint(self, relative: str) -> str:
        data = self.read(relative)
        return f"{len(data)}:{zlib.crc32(data):08x}"

    def read(self, relative: str) -> bytes:
        return self._paths[relative].read_bytes()


class ZipCard(Card):
    """A card in a zip, with its files at the top or under one folder. Files are read from the
    zip one at a time and never extracted."""

    def __init__(self, path: Path) -> None:
        self.label = path.name
        try:
            self._zip = zipfile.ZipFile(path)
        except (OSError, zipfile.BadZipFile) as error:
            raise CardError(f"{path.name}: not a zip ({error})") from error
        members = [m for m in self._zip.infolist() if not m.is_dir()]
        root = _zip_root([m.filename for m in members])
        if root is None:
            self._zip.close()
            raise CardError(f"{path.name}: no STR.edf or DATALOG folder in it")
        self._members: dict[str, zipfile.ZipInfo] = {}
        for member in members:
            if member.filename.startswith(root):
                relative = member.filename[len(root) :]
                if card_kind(relative):
                    self._members[relative] = member

    def files(self) -> list[str]:
        return sorted(self._members, key=_order)

    def fingerprint(self, relative: str) -> str:
        member = self._members[relative]
        return f"{member.file_size}:{member.CRC:08x}"

    def read(self, relative: str) -> bytes:
        member = self._members[relative]
        try:
            with self._zip.open(member) as handle:
                data = handle.read(MEMBER_MAX_BYTES + 1)
        except (OSError, zipfile.BadZipFile, EOFError, zlib.error) as error:
            raise CardError(f"{relative}: cannot be read from the zip ({error})") from error
        if len(data) > MEMBER_MAX_BYTES:
            raise CardError(f"{relative}: larger than {MEMBER_MAX_BYTES} bytes")
        return data

    def close(self) -> None:
        self._zip.close()


def _zip_root(names: list[str]) -> str | None:
    """The prefix of a zip's names up to the card's root, from its STR.edf or else its
    DATALOG folder, or None when it has neither. The ``__MACOSX`` folder macOS adds, which
    repeats the zip's folders, is left out."""
    for wanted in ("str.edf", "datalog"):
        for name in names:
            parts = name.split("/")
            if parts[0] == "__MACOSX":
                continue
            for index, part in enumerate(parts):
                if part.lower() == wanted and (wanted == "datalog") == (index < len(parts) - 1):
                    if ".." in parts[:index]:
                        break
                    return "".join(f"{p}/" for p in parts[:index])
    return None


def card_offset(raw: bytes) -> str | None:
    """The ``TimeZoneOffset`` setting from a card's ``CurrentSettings.json``, or None."""
    try:
        settings = json.loads(raw)
    except ValueError, UnicodeDecodeError:
        return None
    stack: list[Any] = [settings]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            offset = item.get("TimeZoneOffset")
            if isinstance(offset, str) and OFFSET.match(offset):
                return offset
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)
    return None


class Points:
    """Turns a card's files into points."""

    def __init__(
        self,
        prefix: str,
        zone: dt.tzinfo,
        person: str,
        anchors: list[ClockAnchor] | None = None,
        corrections: dict[str, int] | None = None,
    ) -> None:
        self._prefix = prefix
        self._zone = zone
        self._person = person
        self._anchors = anchors or []
        #: The correction of each night's times in milliseconds, kept once it is first used, so
        #: a night imported again lands on the same times; adds to it in place.
        self._corrections = {} if corrections is None else corrections
        self.counts: Counter[str] = Counter()
        self.nights: set[str] = set()

    def _ms(self, local: dt.datetime) -> int:
        return int(local.replace(tzinfo=self._zone).timestamp() * 1000)

    def _true(self, night: str, device: dt.datetime) -> dt.datetime:
        """A moment of a night on the device's clock, corrected for the clock's drift."""
        if night not in self._corrections:
            correction = clock_correction(self._anchors, device)
            self._corrections[night] = round(correction.total_seconds() * 1000)
        return device + dt.timedelta(milliseconds=self._corrections[night])

    def _point(
        self, table: str, tags: dict[str, str], local: dt.datetime, fields: dict[str, FieldValue]
    ) -> Point:
        self.counts[table] += 1
        return Point(
            self._prefix + table,
            tuple(sorted({**tags, "person": self._person}.items())),
            self._ms(local),
            fields,
            precision=Precision.MILLISECONDS,
        )

    def build(self, kind: str, edf: Edf) -> Iterator[Point]:
        """The points one file carries."""
        if kind == "STR":
            yield from self._days(edf)
        elif SESSION_TABLES[kind] == "event":
            yield from self._events(edf)
        else:
            yield from self._series(SESSION_TABLES[kind], edf)

    def _days(self, edf: Edf) -> Iterator[Point]:
        device = _serial(edf)
        values = {
            signal.label: edf.samples(position)
            for position, signal in enumerate(edf.signals)
            if signal.label not in STR_IGNORED
        }
        for record in range(edf.records):
            local = edf.start + dt.timedelta(seconds=record * edf.record_seconds)
            tags = {"device": device, "night": night_of(local)}
            day = {label: samples[record] for label, samples in values.items()}
            duration = (day.get("Duration") or [None])[0]
            therapy = duration is not None and duration > 0
            fields: dict[str, FieldValue] = {}
            for label, samples in day.items():
                if label in STR_SKIPPED or samples[0] is None:
                    continue
                if therapy or label.startswith("S.") or label in STR_SETTINGS:
                    fields[field_name(label)] = samples[0]
            if fields:
                self.nights.add(tags["night"])
                yield self._point("night", tags, local, fields)
            # A mask taken off and put back within a minute is a period of no minutes, starting
            # in the same minute as the next one; periods that start together are one point.
            periods: dict[float, float] = {}
            for on, off in zip(day.get("MaskOn", []), day.get("MaskOff", []), strict=False):
                if on is not None and off is not None and off >= on:
                    periods[on] = periods.get(on, 0.0) + off - on
            for on, minutes in periods.items():
                start = self._true(tags["night"], local + dt.timedelta(minutes=on))
                yield self._point("mask", tags, start, {"minutes": minutes})

    def _events(self, edf: Edf) -> Iterator[Point]:
        tags = {"device": _serial(edf), "night": night_of(edf.start)}
        for annotation in edf.annotations():
            if annotation.text.lower() in NOT_EVENTS:
                continue
            moment = self._true(tags["night"], edf.start + dt.timedelta(seconds=annotation.onset))
            self.nights.add(tags["night"])
            yield self._point(
                "event",
                {**tags, "type": field_name(annotation.text)},
                moment,
                {"duration": float(annotation.duration or 0.0)},
            )

    def _series(self, table: str, edf: Edf) -> Iterator[Point]:
        tags = {"device": _serial(edf), "night": night_of(edf.start)}
        by_time: dict[float, dict[str, FieldValue]] = {}
        for position, signal in enumerate(edf.signals):
            if signal.label in SESSION_SKIPPED or signal.samples <= 0:
                continue
            name = field_name(signal.label)
            step = edf.record_seconds / signal.samples
            for record, samples in enumerate(edf.samples(position)):
                base = record * edf.record_seconds
                for index, value in enumerate(samples):
                    if value is not None:
                        by_time.setdefault(base + index * step, {})[name] = value
        if by_time:
            self.nights.add(tags["night"])
        for seconds in sorted(by_time):
            moment = self._true(tags["night"], edf.start + dt.timedelta(seconds=seconds))
            yield self._point(table, tags, moment, by_time[seconds])


def _serial(edf: Edf) -> str:
    """The device's serial number from a file's recording field, or ``unknown``."""
    match = SERIAL.search(edf.recording)
    return match.group(1) if match else "unknown"


def _batches(points: Iterable[Point]) -> Iterator[list[Point]]:
    batch: list[Point] = []
    for point in points:
        batch.append(point)
        if len(batch) >= BATCH:
            yield batch
            batch = []
    if batch:
        yield batch


class ResmedSource:
    """Imports copies of a ResMed SD card: zips in an inbox, and a folder read in place."""

    file_suffixes = (".zip",)

    def __init__(self, params: ResmedParams) -> None:
        self._params = params
        self.inbox = params.inbox
        self._woken = asyncio.Event()

    def wake(self) -> None:
        """Scan now rather than at the next scan, for a file just saved in the inbox."""
        self._woken.set()

    async def run(self, context: SourceContext) -> None:
        """Import what the inbox and the card folder hold every ``scan_minutes`` and whenever
        woken, for ever."""
        self.inbox.mkdir(parents=True, exist_ok=True)
        while True:
            self._woken.clear()
            if await self.scan(context):
                context.succeeded()
            await wait_or_wake(context, self._woken, self._params.scan_minutes * 60)

    async def scan(self, context: SourceContext) -> bool:
        """Import every zip waiting in the inbox, oldest name first, then the card folder;
        False when a batch was refused."""
        for path in sorted(self.inbox.glob("*.zip")):
            try:
                card = ZipCard(path)
            except CardError as error:
                context.problem(f"cannot read card {error}; moved to {FAILED}/")
                logger.error("cannot read card %s; moving it to %s/", error, FAILED)
                move_aside(path, FAILED)
                continue
            try:
                imported = await self.import_card(context, card)
            finally:
                card.close()
            if not imported:
                context.problem(f"{path.name}: a batch was refused; the file stays in the inbox")
                return False
            move_aside(path, IMPORTED)
        folder = self._params.card
        if folder is not None and folder.is_dir():
            card = await asyncio.to_thread(FolderCard, folder)
            if not await self.import_card(context, card):
                context.problem(f"{folder}: a batch was refused; it is read again next scan")
                return False
        return True

    def _zone(self, card: Card, files: list[str], state: State) -> dt.tzinfo:
        """The zone the card's times are read in: ``timezone``, else the card's own setting,
        else the setting of the last card that had one, else UTC."""
        if self._params.timezone is not None:
            return parse_zone(self._params.timezone)
        for relative in files:
            if card_kind(relative) == "SETTINGS" and (offset := card_offset(card.read(relative))):
                state.offset = offset
                return parse_zone(offset)
        if state.offset is not None:
            logger.info("%s: no TimeZoneOffset setting; using %s", card.label, state.offset)
            return parse_zone(state.offset)
        logger.warning(
            "%s: no TimeZoneOffset setting on the card; reading times as UTC", card.label
        )
        return dt.UTC

    async def import_card(self, context: SourceContext, card: Card) -> bool:
        """Import every file of a card not imported before, as its fingerprint says; False when
        a batch was refused, leaving the files from that one on to be imported again."""
        state_path = context.state_dir / STATE_FILE
        state = _load_state(state_path)
        known_offset = state.offset
        done = state.files
        files = card.files()
        fingerprints = await asyncio.to_thread(lambda: {f: card.fingerprint(f) for f in files})
        builder = Points(
            self._params.table_prefix,
            self._zone(card, files, state),
            self._params.person,
            self._params.clock,
            state.corrections,
        )
        changed = 0
        skipped = 0
        try:
            for relative in files:
                kind = card_kind(relative)
                if kind == "SETTINGS" or done.get(relative) == fingerprints[relative]:
                    skipped += kind != "SETTINGS"
                    continue
                try:
                    edf = read_edf(card.read(relative), relative)
                except (EdfError, CardError) as error:
                    context.problem(f"{card.label}: cannot read {error}")
                    logger.error("%s: cannot read %s", card.label, error)
                else:
                    for batch in _batches(builder.build(kind, edf)):  # type: ignore[arg-type]
                        if not await context.emit(batch):
                            return False
                done[relative] = fingerprints[relative]
                changed += 1
        finally:
            if changed or state.offset != known_offset:
                _save_state(state_path, state)
        if changed or builder.counts:
            summary = ", ".join(f"{n} {t}" for t, n in sorted(builder.counts.items())) or "nothing"
            logger.info(
                "imported %s: %d file(s), %d unchanged, %d night(s): %s",
                card.label,
                changed,
                skipped,
                len(builder.nights),
                summary,
            )
        return True


@dataclass
class State:
    """What the source keeps between imports: the fingerprints of the files imported so far, by
    path, the last ``TimeZoneOffset`` a card gave, for a copy without its settings, and the clock
    correction each night was imported with."""

    files: dict[str, str] = field(default_factory=dict)
    offset: str | None = None
    #: Each night's clock correction in milliseconds, as first applied.
    corrections: dict[str, int] = field(default_factory=dict)


def _load_state(path: Path) -> State:
    """The state kept so far, or an empty one when it cannot be read."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        files = {str(k): str(v) for k, v in data["files"].items()}
        offset = data.get("offset")
        corrections = {str(k): int(v) for k, v in data.get("corrections", {}).items()}
        offset = offset if isinstance(offset, str) and OFFSET.match(offset) else None
        return State(files, offset, corrections)
    except FileNotFoundError:
        return State()
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        logger.error("cannot read %s (%s); importing every file again", path, error)
        return State()


def _save_state(path: Path, state: State) -> None:
    """Keep the state, written atomically."""
    data = {
        "files": dict(sorted(state.files.items())),
        "offset": state.offset,
        "corrections": dict(sorted(state.corrections.items())),
    }
    write_json(path, data)


def build(params: ResmedParams, _client: httpx2.AsyncClient, _name: str = "") -> ResmedSource:
    """A ResMed SD card source from its validated parameters."""
    return ResmedSource(params)


__all__ = ["ResmedParams", "ResmedSource", "build", "field_name", "parse_zone"]

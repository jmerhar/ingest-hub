"""Reading EDF and EDF+ files, as ResMed devices write them, from their bytes.

An EDF file is a 256-byte header, 256 bytes more per signal, then data records of a fixed length:
each record holds every signal's samples for the record's duration, as little-endian 16-bit
integers. A sample's physical value is its digital value mapped linearly from the signal's
digital range onto its physical range.

EDF+ adds annotations: a signal labelled ``EDF Annotations`` whose bytes are time-stamped
annotation lists (TALs), each ``+onset[\\x15duration]\\x14text\\x14[text\\x14...]\\x00``, with the
onset in seconds from the file's start. The first TAL of each record only keeps time and has no
text. Only that signal is read as text; every other signal (ResMed adds a ``Crc16``) is numbers,
so splitting a whole record on NUL bytes would read numbers as annotations.

Only what the files need is checked: a file whose header or record layout does not add up raises
:class:`EdfError`, and a truncated last record is left out.
"""

from __future__ import annotations

import datetime as dt
import re
import struct
from dataclasses import dataclass

ANNOTATIONS = "EDF Annotations"
HEADER = 256
#: The month names an EDF+ ``Startdate`` uses.
MONTHS = ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")
#: The four-digit start date EDF+ puts in the recording field: ``Startdate 12-OCT-2026``.
STARTDATE = re.compile(r"^Startdate (\d{2})-([A-Z]{3})-(\d{4})\b")
#: A digital value that marks a sample as missing, whatever the signal's range.
MISSING = -1


class EdfError(Exception):
    """A file is not EDF that can be read; the message says why."""


@dataclass(frozen=True)
class Signal:
    """One signal's header: its label, unit, ranges and samples per record."""

    label: str
    unit: str
    physical_min: float
    physical_max: float
    digital_min: int
    digital_max: int
    samples: int

    def value(self, digital: int) -> float | None:
        """A digital sample's physical value, or None when it is missing: ``-1``, or outside the
        signal's digital range."""
        if digital == MISSING or not self.digital_min <= digital <= self.digital_max:
            return None
        span = self.digital_max - self.digital_min
        scale = (self.physical_max - self.physical_min) / span if span else 0.0
        return round(self.physical_min + (digital - self.digital_min) * scale, 6)


@dataclass(frozen=True)
class Annotation:
    """One annotation: seconds from the file's start, its duration in seconds when given, and
    its text."""

    onset: float
    duration: float | None
    text: str


@dataclass
class Edf:
    """A parsed EDF file: its start (the device's clock, no zone), the recording field, the
    record duration in seconds, its signals and its data records' bytes."""

    start: dt.datetime
    recording: str
    record_seconds: float
    signals: list[Signal]
    data: bytes
    records: int

    def _offsets(self) -> list[int]:
        offsets, at = [], 0
        for signal in self.signals:
            offsets.append(at)
            at += signal.samples * 2
        return offsets

    @property
    def record_bytes(self) -> int:
        return sum(s.samples for s in self.signals) * 2

    def index(self, label: str) -> int | None:
        """The position of the first signal with this label, or None."""
        for position, signal in enumerate(self.signals):
            if signal.label == label:
                return position
        return None

    def samples(self, position: int) -> list[list[float | None]]:
        """One signal's physical samples, record by record, missing ones as None."""
        signal = self.signals[position]
        offset = self._offsets()[position]
        layout = struct.Struct(f"<{signal.samples}h")
        size = self.record_bytes
        return [
            [signal.value(v) for v in layout.unpack_from(self.data, record * size + offset)]
            for record in range(self.records)
        ]

    def annotations(self) -> list[Annotation]:
        """Every annotation with text in the file's ``EDF Annotations`` signals, in order."""
        found: list[Annotation] = []
        offsets = self._offsets()
        size = self.record_bytes
        for position, signal in enumerate(self.signals):
            if signal.label != ANNOTATIONS:
                continue
            for record in range(self.records):
                start = record * size + offsets[position]
                found.extend(parse_tals(self.data[start : start + signal.samples * 2]))
        return found


def parse_tals(raw: bytes) -> list[Annotation]:
    """The annotations with text in one record's annotation bytes. A TAL whose onset cannot be
    read is left out."""
    found: list[Annotation] = []
    for tal in raw.split(b"\x00"):
        parts = tal.split(b"\x14")
        if len(parts) < 2:
            continue
        timing = parts[0].split(b"\x15")
        try:
            onset = float(timing[0])
            duration = float(timing[1]) if len(timing) > 1 and timing[1] else None
        except ValueError:
            continue
        for text in parts[1:]:
            if label := text.decode("utf-8", "replace").strip():
                found.append(Annotation(onset, duration, label))
    return found


def _start(date: str, time: str, recording: str) -> dt.datetime:
    """The start from the header's ``dd.mm.yy`` and ``hh.mm.ss``, with the four-digit year from
    an EDF+ ``Startdate`` when the recording field has one; otherwise years 85-99 are 19xx."""
    day, month, year = (int(part) for part in date.split("."))
    hour, minute, second = (int(part) for part in time.split("."))
    if (match := STARTDATE.match(recording)) and match.group(2) in MONTHS:
        year = int(match.group(3))
    else:
        year += 1900 if year >= 85 else 2000
    return dt.datetime(year, month, day, hour, minute, second)


def read_edf(raw: bytes, name: str = "") -> Edf:
    """Parse an EDF or EDF+ file from its bytes; raises :class:`EdfError` when it cannot be."""

    def text(start: int, length: int) -> str:
        return raw[start : start + length].decode("latin-1").strip()

    try:
        count = int(text(252, 4))
        header_bytes = int(text(184, 8))
        records = int(text(236, 8))
        record_seconds = float(text(244, 8))
        recording = text(88, 80)
        start = _start(text(168, 8), text(176, 8), recording)
        if count <= 0 or header_bytes != HEADER * (count + 1) or len(raw) < header_bytes:
            raise ValueError("the header's size does not match its signal count")
        fields: list[list[str]] = []
        at = HEADER
        for width in (16, 80, 8, 8, 8, 8, 8, 80, 8, 32):
            fields.append([text(at + i * width, width) for i in range(count)])
            at += width * count
        signals = [
            Signal(
                label=fields[0][i],
                unit=fields[2][i],
                physical_min=float(fields[3][i]),
                physical_max=float(fields[4][i]),
                digital_min=int(fields[5][i]),
                digital_max=int(fields[6][i]),
                samples=int(fields[8][i]),
            )
            for i in range(count)
        ]
    except (ValueError, UnicodeDecodeError) as error:
        raise EdfError(f"{name}: not an EDF header ({error})") from error
    edf = Edf(start, recording, record_seconds, signals, raw[header_bytes:], 0)
    if edf.record_bytes <= 0:
        raise EdfError(f"{name}: its records hold no samples")
    whole = len(edf.data) // edf.record_bytes
    # A file still being written, or cut short, says -1 or more records than it holds.
    edf.records = whole if records < 0 else min(records, whole)
    return edf


__all__ = ["Annotation", "Edf", "EdfError", "Signal", "parse_tals", "read_edf"]

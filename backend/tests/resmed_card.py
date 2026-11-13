"""Synthetic ResMed SD cards for the tests: EDF files built from scratch, in the layout an
AirSense 11 writes, with made-up values and serial number."""

from __future__ import annotations

import datetime as dt
import struct
import zipfile
from dataclasses import dataclass
from pathlib import Path

SERIAL = "10000000001"
MONTHS = ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")


@dataclass(frozen=True)
class Sig:
    """A signal's header."""

    label: str
    pmin: float
    pmax: float
    dmin: int
    dmax: int
    samples: int = 1
    unit: str = ""


ANNOTATIONS = Sig("EDF Annotations", -32768, 32767, -32768, 32767, 31)
CRC = Sig("Crc16", -32768, 32767, -32768, 32767, 1)
#: Crc16 values whose bytes read ``+0`` and ``+1``, as a TAL's onset does.
JUNK_CRC = (struct.unpack("<h", b"+0")[0], struct.unpack("<h", b"+1")[0])


def edf(
    start: dt.datetime,
    record_seconds: float,
    signals: list[Sig],
    records: list[list[list[int] | bytes]],
    *,
    plus: bool = False,
    serial: str = SERIAL,
    startdate: bool = True,
    nrec: int | None = None,
) -> bytes:
    """An EDF file: each record lists every signal's digital samples, or the TAL bytes of an
    annotation signal, padded to its size."""

    def pad(text: object, width: int) -> bytes:
        return str(text).encode("latin-1").ljust(width)[:width]

    recording = f"Startdate {start.day:02d}-{MONTHS[start.month - 1]}-{start.year} X X X "
    recording = (recording if startdate else "X X X X ") + f"SRN={serial} MID=46 VID=3"
    header = b"".join(
        [
            pad(0, 8),
            pad("X X X X", 80),
            pad(recording, 80),
            pad(start.strftime("%d.%m.%y"), 8),
            pad(start.strftime("%H.%M.%S"), 8),
            pad(256 * (len(signals) + 1), 8),
            pad("EDF+D" if plus else "EDF", 44),
            pad(len(records) if nrec is None else nrec, 8),
            pad(f"{record_seconds:.2f}", 8),
            pad(len(signals), 4),
        ]
    )
    for width, get in (
        (16, lambda s: s.label),
        (80, lambda s: ""),
        (8, lambda s: s.unit),
        (8, lambda s: s.pmin),
        (8, lambda s: s.pmax),
        (8, lambda s: s.dmin),
        (8, lambda s: s.dmax),
        (80, lambda s: ""),
        (8, lambda s: s.samples),
        (32, lambda s: ""),
    ):
        header += b"".join(pad(get(s), width) for s in signals)
    data = b""
    for record in records:
        for signal, values in zip(signals, record, strict=True):
            if isinstance(values, bytes):
                data += values.ljust(signal.samples * 2, b"\x00")
            else:
                assert len(values) == signal.samples, signal.label
                data += struct.pack(f"<{signal.samples}h", *values)
    return header + data


def tal(onset: float, text: str = "", duration: float | None = None) -> bytes:
    """One time-stamped annotation list."""
    timing = f"+{onset:g}" + (f"\x15{duration:g}" if duration is not None else "")
    return f"{timing}\x14{text}\x14\x00".encode()


def annotations(start: dt.datetime, events: list[tuple[float, float | None, str]]) -> bytes:
    """An EVE or CSL file: one record per event after a first holding ``Recording starts``,
    each record opening with its time-keeping TAL, as the device writes them."""
    records: list[list[list[int] | bytes]] = [
        [tal(0) + tal(0, "Recording starts", 0), [JUNK_CRC[0]]]
    ]
    for onset, duration, text in events:
        records.append([tal(onset) + tal(onset, text, duration), [JUNK_CRC[1]]])
    return edf(start, 0, [ANNOTATIONS, CRC], records, plus=True)


#: STR's signals: a selection of the real file's, with its ranges.
STR_SIGNALS = [
    Sig("Date", 0, 24836, 0, 24836),
    Sig("MaskOn", 0, 1440, 0, 1440, 20, "MINUTES"),
    Sig("MaskOff", 0, 1440, 0, 1440, 20, "MINUTES"),
    Sig("MaskEvents", 0, 255, 0, 255),
    Sig("Duration", 0, 1440, 0, 1440, unit="min."),
    Sig("Mode", 0, 16, 0, 16),
    Sig("S.A.MinPress", 4, 20, 200, 1000, unit="cmH2O"),
    Sig("S.EPR.Level", 1, 3, 50, 150, unit="cmH2O"),
    Sig("HeatedTube", 0, 16, 0, 16),
    Sig("BlowPress.95", -10, 45, -500, 2250, unit="cmH2O"),
    Sig("Leak.95", 0, 2, 0, 100, unit="L/s"),
    Sig("SpO2.50", 0, 100, 0, 100, unit="%"),
    Sig("AHI", 0, 240, 0, 2400),
    CRC,
]


def str_day(
    date: int,
    mask: list[tuple[int, int]],
    *,
    duration: int,
    settings: bool = True,
    leak: int = 39,
    blow: int = 589,
    ahi: int = 24,
) -> list[list[int] | bytes]:
    """One STR record: -1 for everything not given, as the device writes a day without it."""
    ons = [on for on, _ in mask] + [-1] * (20 - len(mask))
    offs = [off for _, off in mask] + [-1] * (20 - len(mask))
    measured = duration > 0
    return [
        [date],
        ons,
        offs,
        [len(mask) if measured else -1],
        [duration],
        [1 if settings else -1],
        [400 if settings else -1],
        [100 if settings else -1],
        [3 if settings else -1],
        [blow if measured else -1],
        [leak if measured else -1],
        [-1],
        [ahi if measured else -1],
        [JUNK_CRC[0]],
    ]


PLD_SIGNALS = [
    Sig("MaskPress.2s", 0, 40, 0, 2000, 30, "cmH2O"),
    Sig("Leak.2s", 0, 2, 0, 100, 30, "L/s"),
    Sig("FlowLim.2s", 0, 1, 0, 100, 30),
    CRC,
]
SA2_SIGNALS = [
    Sig("Pulse.1s", 0, 300, 0, 300, 60, "bpm"),
    Sig("SpO2.1s", 0, 100, 0, 100, 60, "%"),
    CRC,
]


def pld(start: dt.datetime, minutes: int) -> bytes:
    """A PLD file: mask pressure 8.00 cmH2O, leak rising by 0.02 L/s a sample, and the first
    flow-limitation sample missing."""
    records: list[list[list[int] | bytes]] = []
    for minute in range(minutes):
        leak = [(minute * 30 + i) % 101 for i in range(30)]
        flow = [-1 if minute == 0 and i == 0 else 10 for i in range(30)]
        records.append([[400] * 30, leak, flow, [JUNK_CRC[0]]])
    return edf(start, 60, PLD_SIGNALS, records)


def sa2(start: dt.datetime, minutes: int, *, pulse: int = -1) -> bytes:
    """An SA2 file, every sample -1 (no oximeter) unless ``pulse`` is given."""
    spo2 = -1 if pulse < 0 else 96
    records = [[[pulse] * 60, [spo2] * 60, [0]] for _ in range(minutes)]
    return edf(start, 60, SA2_SIGNALS, records)  # type: ignore[arg-type]


SETTINGS = (
    '{"FlowGenerator": {"SettingProfiles": {"FeatureProfiles": '
    '{"TimeZoneFeature": {"TimeZoneOffset": "+01:00"}}}}}'
)


def card_files(*, extra_night: bool = False) -> dict[str, bytes]:
    """A card's files by path: STR with four days from 10 Oct 2026, then

    - 10 Oct: one session from 23:00 with events, a CSL, two minutes of PLD, an empty SA2 and a
      BRP that is not EDF at all;
    - 11 Oct: no therapy, but settings, a count of no mask events and a pressure;
    - 12 Oct: the device off, every value -1;
    - 13 Oct: two sessions, 22:45 and 01:30 the next morning.

    With ``extra_night``, STR has a fifth day and a session on 14 Oct follows: the next copy of
    the same card.
    """
    noon = dt.datetime(2026, 10, 10, 12)
    # The device can record a few values on a day without therapy, such as a pressure from a
    # mask fitting.
    no_therapy = str_day(20737, [], duration=-1)
    no_therapy[3], no_therapy[9] = [0], [501]
    days = [
        str_day(20736, [(660, 662)], duration=2),
        no_therapy,
        str_day(20738, [], duration=-1, settings=False),
        str_day(20739, [(645, 647), (810, 811)], duration=3, leak=60, ahi=0),
    ]
    if extra_night:
        days.append(str_day(20740, [(600, 601)], duration=1))
    files = {
        "Identification.json": b'{"FlowGenerator": {}}',
        "SETTINGS/CurrentSettings.json": SETTINGS.encode(),
        "STR.edf": edf(noon, 86400, STR_SIGNALS, days),
    }
    first = dt.datetime(2026, 10, 10, 23, 0, 0)
    files |= {
        "DATALOG/20261010/20261010_230000_EVE.edf": annotations(
            first,
            [
                (30, 12, "Obstructive Apnea"),
                (45, 0, "Hypopnea"),
                (61, 15, "Central Apnea"),
                (70, None, "Arousal"),
                (90, 10, "Apnea"),
            ],
        ),
        "DATALOG/20261010/20261010_230000_CSL.edf": annotations(first, []),
        "DATALOG/20261010/20261010_230010_PLD.edf": pld(first + dt.timedelta(seconds=10), 2),
        "DATALOG/20261010/20261010_230010_SA2.edf": sa2(first + dt.timedelta(seconds=10), 2),
        "DATALOG/20261010/20261010_230010_BRP.edf": b"not read",
    }
    for start in (dt.datetime(2026, 10, 13, 22, 45), dt.datetime(2026, 10, 14, 1, 30)):
        stamp = start.strftime("%Y%m%d_%H%M%S")
        files[f"DATALOG/20261013/{stamp}_EVE.edf"] = annotations(start, [(5, 11, "Central Apnea")])
        files[f"DATALOG/20261013/{stamp}_PLD.edf"] = pld(start, 1)
    if extra_night:
        start = dt.datetime(2026, 10, 14, 22, 0)
        files["DATALOG/20261014/20261014_220000_PLD.edf"] = pld(start, 1)
    return files


def write_card(root: Path, files: dict[str, bytes]) -> Path:
    """Write a card's files into a folder."""
    for name, data in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    return root


def write_zip(path: Path, files: dict[str, bytes], folder: str = "") -> Path:
    """Zip a card's files, under ``folder`` when given."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(folder + name, data)
    return path

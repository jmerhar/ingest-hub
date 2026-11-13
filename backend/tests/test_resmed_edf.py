"""Reading EDF and EDF+ files, with synthetic files in the layout a ResMed device writes."""

from __future__ import annotations

import datetime as dt
import struct

import pytest

from ingest_hub.adapters.resmed.edf import Annotation, EdfError, Signal, parse_tals, read_edf

from .resmed_card import (
    ANNOTATIONS,
    CRC,
    PLD_SIGNALS,
    STR_SIGNALS,
    Sig,
    annotations,
    edf,
    pld,
    str_day,
    tal,
)

START = dt.datetime(2026, 10, 10, 23, 0, 0)


def test_the_header_gives_the_start_signals_and_records():
    parsed = read_edf(pld(START, 3), "x_PLD.edf")
    assert parsed.start == START
    assert parsed.record_seconds == 60
    assert parsed.records == 3
    assert [s.label for s in parsed.signals] == [s.label for s in PLD_SIGNALS]
    assert parsed.signals[0] == Signal("MaskPress.2s", "cmH2O", 0, 40, 0, 2000, 30)
    assert "SRN=10000000001" in parsed.recording
    assert parsed.index("Leak.2s") == 1 and parsed.index("Nothing") is None


def test_samples_are_scaled_from_the_digital_range_onto_the_physical_one():
    parsed = read_edf(pld(START, 2))
    assert parsed.samples(0)[0][:2] == [8.0, 8.0]  # 400 of 0..2000 onto 0..40 cmH2O
    leak = parsed.samples(1)
    assert leak[0][:3] == [0.0, 0.02, 0.04] and leak[1][0] == 0.6  # 30 of 0..100 onto 0..2 L/s
    assert parsed.samples(2)[0][:2] == [None, 0.1]  # -1 is missing


@pytest.mark.parametrize(
    ("digital", "value"),
    [(-500, -10.0), (2250, 45.0), (0, 0.0), (589, 11.78), (-1, None), (-501, None), (2251, None)],
)
def test_minus_one_and_values_outside_the_range_are_missing(digital, value):
    assert Signal("BlowPress.95", "cmH2O", -10, 45, -500, 2250, 1).value(digital) == value


def test_a_signal_with_an_empty_digital_range_is_its_physical_minimum():
    assert Signal("Flat", "", 5, 5, 3, 3, 1).value(3) == 5.0


def test_a_truncated_record_and_an_unfinished_count_are_left_out():
    whole = pld(START, 3)
    assert read_edf(whole[: len(whole) - 10]).records == 2
    # A file still being written gives -1 records; the ones it holds are read.
    unfinished = whole[:236] + b"-1".ljust(8) + whole[244:]
    assert read_edf(unfinished).records == 3


def test_the_year_comes_from_the_edf_plus_startdate_or_the_two_digit_rule():
    plain = edf(dt.datetime(1999, 5, 1, 12), 60, [CRC], [[[0]]], startdate=False)
    assert read_edf(plain).start == dt.datetime(1999, 5, 1, 12)
    recent = edf(dt.datetime(2026, 5, 1, 12), 60, [CRC], [[[0]]], startdate=False)
    assert read_edf(recent).start == dt.datetime(2026, 5, 1, 12)


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"0" * 300,
        pld(START, 1)[:300],  # cut inside the signal headers
        pld(START, 1).replace(b"1280    ", b"2000    ", 1),  # header size disagrees
        edf(START, 60, [Sig("Empty", 0, 1, 0, 1, 0)], [[[]]]),  # records of no samples
    ],
)
def test_a_file_that_is_not_edf_is_refused(raw):
    with pytest.raises(EdfError, match="bad.edf"):
        read_edf(raw, "bad.edf")


def test_annotations_come_only_from_the_annotation_signal():
    events = [(30, 12, "Obstructive Apnea"), (45, 0, "Hypopnea"), (70, None, "Arousal")]
    parsed = read_edf(annotations(START, events))
    assert parsed.annotations() == [
        Annotation(0, 0, "Recording starts"),
        Annotation(30, 12, "Obstructive Apnea"),
        Annotation(45, 0, "Hypopnea"),
        Annotation(70, None, "Arousal"),
    ]


def test_numbers_that_look_like_a_tal_are_not_annotations():
    # Another signal's samples whose bytes happen to spell a TAL, "+9 Junk".
    junk = list(struct.unpack("<4h", b"+9\x14Jk\x14\x00\x00"))
    numbers = Sig("Numbers", -32768, 32767, -32768, 32767, 4)
    raw = edf(START, 0, [ANNOTATIONS, numbers], [[tal(0) + tal(1, "Hypopnea", 0), junk]], plus=True)
    assert read_edf(raw).annotations() == [Annotation(1, 0, "Hypopnea")]


def test_a_tal_can_hold_several_annotations_and_unreadable_ones_are_skipped():
    raw = (
        tal(0)
        + b"+12.5\x152\x14Central Apnea\x14Arousal\x14\x00"
        + b"+x\x14Broken\x14\x00"
        + b"no separator\x00"
        + tal(13, "  ")
    )
    assert parse_tals(raw) == [
        Annotation(12.5, 2, "Central Apnea"),
        Annotation(12.5, 2, "Arousal"),
    ]


def test_a_summary_file_reads_its_daily_records():
    raw = edf(START, 86400, STR_SIGNALS, [str_day(20736, [(660, 662)], duration=2)])
    parsed = read_edf(raw)
    assert parsed.records == 1
    assert parsed.samples(parsed.index("MaskOn"))[0][:2] == [660.0, None]
    assert parsed.samples(parsed.index("S.EPR.Level"))[0] == [2.0]
    assert parsed.annotations() == []

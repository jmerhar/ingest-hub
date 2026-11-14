"""The rules the Google Health sources share."""

from __future__ import annotations

import datetime as dt

import pytest

from ingest_hub.adapters.google.common import Sleep, parse_seconds, resolve_sleeps

HOUR = 3_600_000


def session(start_h: float, hours: float, **extra) -> Sleep:
    return Sleep(
        "2026-11-10", int(start_h * HOUR), int((start_h + hours) * HOUR), hours * 60, **extra
    )


def test_of_overlapping_sessions_the_newest_algorithms_is_kept_then_the_longest():
    sleeps = {
        "old": session(0, 8, version="sleep_session_v1"),
        "new": session(0.5, 7, version="sleep_session_v2"),
        "fragment": session(2, 1, version="sleep_session_v2"),
        "nap": session(10, 1, version="sleep_session_v1"),
    }
    assert resolve_sleeps(sleeps, {"2026-11-10"}) == {"2026-11-10": "new"}
    assert [k for k, s in sleeps.items() if s.duplicate] == ["old", "fragment"]


def test_the_main_sleep_is_the_longest_with_stages_or_else_the_longest():
    sleeps = {"classic": session(0, 9, staged=False), "staged": session(10, 2)}
    assert resolve_sleeps(sleeps, {"2026-11-10"}) == {"2026-11-10": "staged"}
    sleeps = {"short": session(0, 1, staged=False), "long": session(10, 2, staged=False)}
    assert resolve_sleeps(sleeps, {"2026-11-10"}) == {"2026-11-10": "long"}
    assert resolve_sleeps(sleeps, {"2026-11-11"}) == {}


@pytest.mark.parametrize(
    ("text", "seconds"), [("3600s", 3600), ("-7200s", -7200), ("0s", 0), ("3.5s", 3.5)]
)
def test_offsets_as_the_api_gives_them(text, seconds):
    assert parse_seconds(text) == dt.timedelta(seconds=seconds)


@pytest.mark.parametrize("text", ["+01:00", "3600", "s"])
def test_an_offset_that_is_not_a_duration_is_refused(text):
    with pytest.raises(ValueError):
        parse_seconds(text)

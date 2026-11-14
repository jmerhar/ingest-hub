"""What the Google Health sources share: reading the times Google gives, and sorting a night's
sleep sessions into its main sleep, naps and duplicates."""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass


def parse_time(text: str) -> dt.datetime:
    """A timestamp Google gives, in UTC: RFC 3339 with ``Z`` or an offset, with or without
    fractions of a second. Raises ValueError when it is none of these."""
    moment = dt.datetime.fromisoformat(text.strip())
    if moment.tzinfo is None:
        raise ValueError(f"no zone in {text!r}")
    return moment.astimezone(dt.UTC)


def parse_offset(text: str) -> dt.timedelta:
    """A UTC offset such as ``+01:00``; raises ValueError when it is not one."""
    match = re.fullmatch(r"([+-])(\d{2}):(\d{2})", text.strip())
    if not match:
        raise ValueError(f"not an offset: {text!r}")
    sign = -1 if match.group(1) == "-" else 1
    return sign * dt.timedelta(hours=int(match.group(2)), minutes=int(match.group(3)))


def parse_seconds(text: str) -> dt.timedelta:
    """A UTC offset as the Google Health API gives it, a duration in seconds such as ``3600s``;
    raises ValueError when it is not one."""
    match = re.fullmatch(r"(-?\d+(\.\d+)?)s", text.strip())
    if not match:
        raise ValueError(f"not a duration: {text!r}")
    return dt.timedelta(seconds=float(match.group(1)))


def to_ms(moment: dt.datetime) -> int:
    """An aware moment in Unix milliseconds."""
    return int(moment.timestamp() * 1000)


@dataclass
class Sleep:
    """What is kept of a sleep session to join its stages, scores and summary to it."""

    night: str
    start_ms: int
    end_ms: int
    minutes: float
    #: Whether the session was last written as its night's main sleep.
    main: bool = False
    #: Whether it overlaps a session of its night that is kept instead: the same sleep recorded
    #: twice.
    duplicate: bool = False
    #: The algorithm that recorded it, such as ``sleep_session_v2``; empty when unknown.
    version: str = ""
    #: Whether it has sleep stages (``STAGES``) rather than only asleep and awake (``CLASSIC``).
    staged: bool = True


def resolve_sleeps(sleeps: dict[str, Sleep], nights: set[str]) -> dict[str, str]:
    """Mark the sessions of these nights that overlap one kept instead as duplicates, and return
    the id of each night's main sleep.

    Of sessions that overlap, the one kept is the newest algorithm's, then the longest: the
    session the Google Health API returns where an export holds the same sleep from two versions
    of the algorithm. The main sleep is, as the API defines it, the longest kept session with
    stages, or the longest of all when none has stages; the earliest when two tie.
    """
    by_night: dict[str, list[str]] = {}
    for sleep_id, sleep in sleeps.items():
        if sleep.night in nights:
            by_night.setdefault(sleep.night, []).append(sleep_id)
    main: dict[str, str] = {}
    for night, ids in by_night.items():
        kept: list[tuple[str, Sleep]] = []
        for sleep_id in sorted(ids, key=lambda i: _keep_rank(i, sleeps[i])):
            sleep = sleeps[sleep_id]
            sleep.duplicate = any(
                sleep.start_ms < other.end_ms and sleep.end_ms > other.start_ms for _, other in kept
            )
            if not sleep.duplicate:
                kept.append((sleep_id, sleep))
        # A night's first session is always kept.
        main[night] = min(kept, key=lambda k: _main_rank(*k))[0]
    return main


def _keep_rank(sleep_id: str, sleep: Sleep) -> tuple:
    """Which of overlapping sessions is kept, lowest first: newest algorithm, then longest."""
    return (_negate(sleep.version), -sleep.minutes, sleep.start_ms, sleep_id)


def _main_rank(sleep_id: str, sleep: Sleep) -> tuple:
    """Which kept session is the main sleep, lowest first: staged, then longest."""
    return (not sleep.staged, -sleep.minutes, sleep.start_ms, sleep_id)


def _negate(text: str) -> tuple[int, ...]:
    """A key that sorts text in descending order."""
    return tuple(-ord(c) for c in text) + (0,)


__all__ = ["Sleep", "parse_offset", "parse_seconds", "parse_time", "resolve_sleeps", "to_ms"]

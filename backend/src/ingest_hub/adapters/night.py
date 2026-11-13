"""The night a moment of sleep data belongs to, the key every sleep source tags its points with.

A night runs from noon to noon, as on ResMed's devices: a session belongs to the date of the noon
before it started, so a night that starts in the evening and ends the next morning is one night,
and so is a nap the afternoon before. The moment is local time, in whatever zone the source
knows it in.
"""

from __future__ import annotations

import datetime as dt

#: When a night starts.
DAY_START = dt.timedelta(hours=12)


def night_of(local: dt.datetime) -> str:
    """The night a local moment belongs to, ``YYYY-MM-DD``: the date of the noon before it."""
    return (local - DAY_START).date().isoformat()


__all__ = ["DAY_START", "night_of"]

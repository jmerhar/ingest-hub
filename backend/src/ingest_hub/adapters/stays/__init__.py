"""Stays from a JSON feed of bookings, such as a holiday rental's, so other data can be split by
who was staying.

The feed answers ``GET {url}`` (with ``{token_param}={token}`` when ``token_file`` is set, and
``{since_param}=YYYY-MM-DD`` when ``since_param`` is) with::

    {"from": "2026-10-19",
     "stays": [{"id": "26b21016bfab7bf2", "check_in": "2026-11-16", "check_out": "2026-11-22",
                "nights": 6, "source": "airbnb", "status": "booked"}, ...]}

listing every stay with a night on or after ``from``: its dates in ``timezone``, ``check_out``
the day it ends (so not a night), the channel it came through, and a status such as ``booked``
or ``manual`` (the owner's own block). An ``id`` stays the same when a stay's dates change, and a
stay missing from a read has been cancelled.

Every ``poll_minutes`` the source reads the feed, asking ``window_days`` back (the first time,
``history_days``), and writes each stay to ``table``, tagged ``stay`` with its id, at its check-in
moment: its date at ``check_in_time``. The fields are ``start`` and ``end`` (the check-in and
check-out moments, at ``check_in_time`` and ``check_out_time``, in Unix milliseconds; a
``check_out_time`` of ``"24:00"`` is the end of the check-out day), ``nights``,
``source``, ``status``, and the dates as ``check_in`` and ``check_out``. A stay seen before that
is missing from a read covering it is written again with status ``cancelled``; one whose check-in
moves is written at its new moment, and its old point again with status ``moved``. So each stay
has one point whose status is current, and the stays the source has seen are kept in its state
directory. The feed's progress, stream ``stays``, is the time of the last good read.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx2
from jigkit.sinks import FieldValue, Point, Precision
from pydantic import BaseModel, ConfigDict, Field, field_validator

from ingest_hub.adapters.inbox import write_json
from ingest_hub.collect import Job, Progress, Schedule, SourceContext
from ingest_hub.config import read_secret

logger = logging.getLogger(__name__)

#: The stays seen so far, in the source's state directory.
STATE_FILE = "stays.json"
TIMEOUT = 30.0
#: Statuses the source writes itself, for stays no longer in the feed as they were.
CANCELLED, MOVED = "cancelled", "moved"


class StaysError(Exception):
    """The feed could not be read; the message says why."""


class StaysParams(BaseModel):
    """A stays source's ``params`` in the configuration file."""

    model_config = ConfigDict(extra="forbid")

    #: The feed's address, without the token.
    url: str
    #: A file holding the token the feed expects, sent as ``token_param``; none when unset.
    token_file: Path | None = None
    token_param: str = "token"
    #: The query parameter asking for stays from a date; when empty, the feed's own window.
    since_param: str = "since"
    table: str = Field("stays", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    #: The time zone the stays' dates are in.
    timezone: str = "UTC"
    check_in_time: dt.time = dt.time(16, 0)
    #: ``"24:00"`` ends a stay at the end of its check-out day.
    check_out_time: dt.time | Literal["24:00"] = dt.time(11, 0)
    poll_minutes: float = Field(60, gt=0)
    #: How far back the first read asks, and later reads.
    history_days: int = Field(365, ge=0)
    window_days: int = Field(30, ge=0)

    @field_validator("timezone")
    @classmethod
    def _known_zone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ValueError(f"unknown time zone {value!r}") from error
        return value


class StaysSource:
    """Reads the feed every ``poll_minutes``, writing what changed."""

    def __init__(self, params: StaysParams, client: httpx2.AsyncClient, token: str | None) -> None:
        self._params = params
        self._client = client
        self._token = token
        self._zone = ZoneInfo(params.timezone)

    async def run(self, context: SourceContext) -> None:
        """Poll for ever, reporting a failed read rather than raising."""
        poll = Job("poll", self._params.poll_minutes * 60, lambda: self.poll(context))
        await Schedule(context, [poll]).run()

    async def poll(self, context: SourceContext) -> None:
        """Read the feed, and write each stay that is new or changed, and those gone from it."""
        path = context.state_dir / STATE_FILE
        known = _load(path)
        today = dt.datetime.fromtimestamp(context.clock(), self._zone).date()
        days = self._params.window_days if known is not None else self._params.history_days
        known = known or {}
        body = await self._read(today - dt.timedelta(days=days))
        covered = _date(body.get("from"), "from")
        points: list[Point] = []
        seen: set[str] = set()
        for entry in body.get("stays") or []:
            try:
                stay = self._stay(entry)
            except (KeyError, TypeError, ValueError) as error:
                context.problem(f"a stay the feed lists cannot be read: {error}")
                # Still listed, so not cancelled: it keeps what was last read of it.
                if isinstance(entry, dict) and "id" in entry:
                    seen.add(str(entry["id"]))
                continue
            seen.add(stay["id"])
            before = known.get(stay["id"])
            if before == stay:
                continue
            if before is not None and before["time"] != stay["time"]:
                points.append(self._point({**before, "status": MOVED}))
            points.append(self._point(stay))
            known[stay["id"]] = stay
        for stay_id, stay in known.items():
            gone = stay_id not in seen and stay["status"] not in (CANCELLED, MOVED)
            if gone and dt.date.fromisoformat(stay["check_out"]) > covered:
                known[stay_id] = {**stay, "status": CANCELLED}
                points.append(self._point(known[stay_id]))
        if not await context.emit(points, [Progress("stays", context.now_ms())]):
            raise StaysError("a batch was refused")
        write_json(path, {"stays": known}, indent=1)
        logger.info("%d stay(s) in the feed, %d point(s) written", len(seen), len(points))

    async def _read(self, since: dt.date) -> dict[str, Any]:
        query: dict[str, str] = {}
        if self._token is not None:
            query[self._params.token_param] = self._token
        if self._params.since_param:
            query[self._params.since_param] = since.isoformat()
        try:
            response = await self._client.get(self._params.url, params=query, timeout=TIMEOUT)
        except httpx2.HTTPError as error:
            raise StaysError(
                f"cannot reach the feed: {str(error) or type(error).__name__}"
            ) from error
        try:
            body = response.json()
        except ValueError:
            body = None
        if response.status_code != 200:
            reason = body.get("error") if isinstance(body, dict) else None
            raise StaysError(
                f"the feed answered HTTP {response.status_code} {reason or ''}".rstrip()
            )
        if not isinstance(body, dict) or not isinstance(body.get("stays"), list):
            raise StaysError("the feed's answer has no list of stays")
        return body

    def _stay(self, entry: dict[str, Any]) -> dict[str, Any]:
        """A stay as kept and written, from the feed's entry."""
        check_in = _date(entry["check_in"], "check_in")
        check_out = _date(entry["check_out"], "check_out")
        if check_out <= check_in:
            raise ValueError(f"check_out {check_out} is not after check_in {check_in}")
        start = self._moment(check_in, self._params.check_in_time)
        return {
            "id": str(entry["id"]),
            "time": start,
            "end": self._moment(check_out, self._params.check_out_time),
            "check_in": check_in.isoformat(),
            "check_out": check_out.isoformat(),
            "nights": int(entry.get("nights") or (check_out - check_in).days),
            "source": str(entry.get("source") or ""),
            "status": str(entry.get("status") or ""),
        }

    def _moment(self, day: dt.date, at: dt.time | str) -> int:
        if not isinstance(at, dt.time):
            day, at = day + dt.timedelta(days=1), dt.time(0)
        return int(dt.datetime.combine(day, at, self._zone).timestamp() * 1000)

    def _point(self, stay: dict[str, Any]) -> Point:
        fields: dict[str, FieldValue] = {
            "start": stay["time"],
            "end": stay["end"],
            "nights": stay["nights"],
            "check_in": stay["check_in"],
            "check_out": stay["check_out"],
            "source": stay["source"],
            "status": stay["status"],
        }
        return Point(
            self._params.table,
            (("stay", stay["id"]),),
            stay["time"],
            fields,
            precision=Precision.MILLISECONDS,
        )


def _date(value: Any, name: str) -> dt.date:
    try:
        return dt.date.fromisoformat(str(value))
    except ValueError as error:
        raise ValueError(f"{name} {value!r} is not a date") from error


def _load(path: Path) -> dict[str, dict[str, Any]] | None:
    """The stays seen before, or None before the first read."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as error:
        raise StaysError(f"cannot read {path.name}: {error}") from error
    stays = data.get("stays") if isinstance(data, dict) else None
    return stays if isinstance(stays, dict) else {}


def build(params: StaysParams, client: httpx2.AsyncClient, _name: str = "") -> StaysSource:
    """A stays source from its validated parameters, with its token read now."""
    token = read_secret(params.token_file) if params.token_file else None
    return StaysSource(params, client, token)


__all__ = ["StaysError", "StaysParams", "StaysSource", "build"]

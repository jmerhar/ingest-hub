"""A Zaptec charger, read through a bridge that holds the owner's Zaptec login.

The source never logs in to Zaptec. Zaptec limits token requests per address, so a second login
from the same host can lock out whatever already controls the charger; instead the source reads
an HTTP bridge that keeps one Zaptec session and serves:

- ``GET {bridge_url}/state.json``: ``{"charger_id", "age_seconds", "observations": {"<StateId>":
  "<value>"}}``, the charger's observations as Zaptec's ``/api/chargers/{id}/state`` gives them,
  answered with 503 while Zaptec is unreachable or the charger is offline;
- ``GET {bridge_url}/sessions?from=<ISO>&to=<ISO>&pageSize=<n>[&cursor=...]``: ``{"sessions":
  [...], "cursor", "hasMore"}``, the charger's completed sessions as Zaptec's
  ``/api/sessions/archived`` gives them, those that ended between ``from`` and ``to``.

Three tables, tagged ``charger``:

- ``<prefix>state``, every ``state_seconds``: one field per observation, named after Zaptec's own
  name for it in snake case (``TotalChargePower`` becomes ``total_charge_power``), from Zaptec's
  public ``/api/constants``. A value that is a number is stored as a float under that name; any
  other value up to 256 characters as a string under ``<name>_text``, so a field never changes
  type. Empty values are left out.
- ``<prefix>session``, one point per completed session at its end: ``id``, ``energy`` (kWh),
  ``start_time``, ``duration_seconds`` and the flags ``aborted``, ``voided``, ``offline``,
  ``stopped_by_rfid``, ``signed`` and ``reliable_clock``. Who authorised the session is not stored.
- ``<prefix>session_energy``: each session's energy readings at their times, tagged ``session``
  as well, field ``energy`` (kWh).

Sessions are read every ``sessions_minutes`` from a day before the latest session end seen (Zaptec
can list a session late), and the first time from ``history_days`` back. Writing a session again
writes the same points.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

import httpx2
from jigkit.sinks import FieldValue, Point, Precision
from pydantic import BaseModel, ConfigDict, Field

from ingest_hub.collect import SourceContext

logger = logging.getLogger(__name__)

CONSTANTS_URL = "https://api.zaptec.com/api/constants"
STATE_FILE = "zaptec.json"
NAMES_FILE = "zaptec-observations.json"
#: How often Zaptec's observation names are fetched again.
NAMES_REFRESH = 24 * 3600.0
#: How far before the latest session end seen each read starts.
SESSION_OVERLAP = dt.timedelta(days=1)
#: The longest text value stored; longer ones (signed meter readings, for one) are left out.
MAX_TEXT = 256
PAGE_SIZE = 200
TIMEOUT = 30.0
CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
FLAGS = {
    "aborted": "aborted",
    "voided": "voided",
    "offline": "offline",
    "stoppedByRfid": "stopped_by_rfid",
    "signed": "signed",
    "reliableClock": "reliable_clock",
}


class ZaptecError(Exception):
    """The bridge could not be read; the message says why."""


class ZaptecParams(BaseModel):
    """A Zaptec source's ``params`` in the configuration file."""

    model_config = ConfigDict(extra="forbid")

    bridge_url: str
    constants_url: str = CONSTANTS_URL
    table_prefix: str = Field("zaptec_", pattern=r"^([A-Za-z_][A-Za-z0-9_]*)?$")
    state_seconds: float = Field(60, gt=0)
    sessions_minutes: float = Field(20, gt=0)
    history_days: float = Field(365, gt=0)


def snake(name: str) -> str:
    """A Zaptec name in snake case: ``TotalChargePower`` becomes ``total_charge_power``."""
    return CAMEL.sub("_", name).lower()


def _number(value: str) -> float | None:
    """A value as a number, or None when it is not one."""
    try:
        number = float(value)
    except ValueError:
        return None
    return number if number == number and abs(number) != float("inf") else None


def _moment(text: str) -> dt.datetime:
    """A Zaptec date-time as an aware UTC datetime."""
    moment = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    return moment if moment.tzinfo else moment.replace(tzinfo=dt.UTC)


class ZaptecSource:
    """Reads one charger's state and sessions through the bridge."""

    def __init__(self, params: ZaptecParams, client: httpx2.AsyncClient) -> None:
        self._params = params
        self._client = client
        self._base = params.bridge_url.rstrip("/")
        self._prefix = params.table_prefix
        #: Observation names by StateId, and when they were last fetched.
        self._names: dict[str, str] = {}
        self._names_at = float("-inf")

    async def run(self, context: SourceContext) -> None:
        """Read the state every ``state_seconds`` and sessions every ``sessions_minutes``."""
        schedule = {"state": context.clock(), "sessions": context.clock()}
        while True:
            now = context.clock()
            ok = True
            if now >= schedule["state"]:
                schedule["state"] = now + self._params.state_seconds
                ok = await self._attempt(context, self.poll_state) and ok
            if now >= schedule["sessions"]:
                schedule["sessions"] = now + self._params.sessions_minutes * 60
                ok = await self._attempt(context, self.poll_sessions) and ok
            if ok:
                context.succeeded()
            await context.sleep(max(1.0, min(schedule.values()) - context.clock()))

    async def _attempt(self, context: SourceContext, poll: Any) -> bool:
        """Run one poll, reporting a failure rather than raising it."""
        try:
            await poll(context)
        except (ZaptecError, httpx2.HTTPError, ValueError, KeyError, TypeError) as error:
            message = f"{poll.__name__}: {error}"
            context.problem(message)
            logger.warning("%s", message)
            return False
        return True

    async def _get(self, url: str, **query: str) -> Any:
        """GET a URL as JSON, raising ZaptecError for any answer but 200."""
        response = await self._client.get(url, params=query or None, timeout=TIMEOUT)
        if response.status_code != 200:
            raise ZaptecError(f"{url} answered {response.status_code}: {response.text[:200]}")
        return response.json()

    async def _observation_names(self, context: SourceContext) -> dict[str, str]:
        """Observation names by StateId: fetched daily, kept in the state directory for when
        Zaptec cannot be reached."""
        path = context.state_dir / NAMES_FILE
        if context.clock() - self._names_at >= NAMES_REFRESH:
            try:
                constants = await self._get(self._params.constants_url)
                observations = constants["Observations"]
                self._names = {str(int(i)): snake(name) for name, i in observations.items()}
                _write_json(path, self._names)
            except (ZaptecError, httpx2.HTTPError, ValueError, KeyError, TypeError) as error:
                logger.warning("cannot fetch Zaptec's observation names (%s)", error)
                if not self._names:
                    self._names = _read_names(path)
            self._names_at = context.clock()
        return self._names

    async def poll_state(self, context: SourceContext) -> None:
        """Read the charger's observations and write them as one point."""
        names = await self._observation_names(context)
        document = await self._get(f"{self._base}/state.json")
        charger = str(document["charger_id"])
        fields: dict[str, FieldValue] = {}
        for state_id, raw in document["observations"].items():
            value = str(raw).strip()
            if not value:
                continue
            name = names.get(str(state_id)) or f"observation_{str(state_id).replace('-', 'm')}"
            number = _number(value)
            if number is not None:
                fields[name] = number
            elif len(value) <= MAX_TEXT:
                fields[f"{name}_text"] = value
        if fields:
            stamp = int(context.clock())
            point = Point(
                self._prefix + "state",
                (("charger", charger),),
                stamp,
                fields,
                precision=Precision.SECONDS,
            )
            if not await context.emit([point]):
                raise ZaptecError("a state batch was refused")

    async def poll_sessions(self, context: SourceContext) -> None:
        """Read the sessions that ended since a day before the latest one seen."""
        path = context.state_dir / STATE_FILE
        now = dt.datetime.fromtimestamp(context.clock(), dt.UTC)
        until = _read_until(path)
        start = (
            until - SESSION_OVERLAP if until else now - dt.timedelta(days=self._params.history_days)
        )
        query = {
            "from": start.isoformat().replace("+00:00", "Z"),
            "to": now.isoformat().replace("+00:00", "Z"),
            "pageSize": str(PAGE_SIZE),
        }
        latest = until
        while True:
            page = await self._get(f"{self._base}/sessions", **query)
            points: list[Point] = []
            for session in page["sessions"]:
                points += self._session_points(session)
                if session.get("endDateTime"):
                    end = _moment(session["endDateTime"])
                    latest = max(latest, end) if latest else end
            if points and not await context.emit(points):
                raise ZaptecError("a session batch was refused")
            if not page.get("hasMore") or not page.get("cursor"):
                break
            query["cursor"] = str(page["cursor"])
        if latest and latest != until:
            _write_json(path, {"sessions_until": latest.isoformat()})

    def _session_points(self, session: dict[str, Any]) -> list[Point]:
        """A completed session as its points; one without an end or an id gives none."""
        if not session.get("endDateTime") or not session.get("id"):
            return []
        charger = str(session.get("chargerId") or "unknown")
        end = _moment(session["endDateTime"])
        fields: dict[str, FieldValue] = {"id": str(session["id"])}
        if isinstance(session.get("energy"), int | float):
            fields["energy"] = float(session["energy"])
        if session.get("startDateTime"):
            begin = _moment(session["startDateTime"])
            fields["start_time"] = begin.isoformat()
            fields["duration_seconds"] = (end - begin).total_seconds()
        for key, name in FLAGS.items():
            if isinstance(session.get(key), bool):
                fields[name] = session[key]
        tags = (("charger", charger),)
        points = [
            Point(
                self._prefix + "session",
                tags,
                int(end.timestamp()),
                fields,
                precision=Precision.SECONDS,
            )
        ]
        for detail in session.get("energyDetails") or []:
            if isinstance(detail.get("energy"), int | float) and detail.get("timestamp"):
                points.append(
                    Point(
                        self._prefix + "session_energy",
                        (("charger", charger), ("session", str(session["id"]))),
                        int(_moment(detail["timestamp"]).timestamp()),
                        {"energy": float(detail["energy"])},
                        precision=Precision.SECONDS,
                    )
                )
        return points


def _read_until(path: Path) -> dt.datetime | None:
    """The latest session end seen, or None before the first read or when it cannot be read."""
    try:
        return _moment(json.loads(path.read_text(encoding="utf-8"))["sessions_until"])
    except FileNotFoundError:
        return None
    except (OSError, ValueError, KeyError, TypeError) as error:
        logger.error("cannot read %s (%s); reading sessions from the start", path, error)
        return None


def _read_names(path: Path) -> dict[str, str]:
    """Observation names kept from an earlier fetch, or none."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return {str(k): str(v) for k, v in data.items()}
    except OSError, ValueError, AttributeError:
        return {}


def _write_json(path: Path, data: Any) -> None:
    """Write a small JSON document atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def build(params: ZaptecParams, client: httpx2.AsyncClient, _name: str = "") -> ZaptecSource:
    """A Zaptec source from its validated parameters, using the hub's shared HTTP client."""
    return ZaptecSource(params, client)


__all__ = ["ZaptecParams", "ZaptecSource", "build", "snake"]

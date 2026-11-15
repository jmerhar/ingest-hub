"""Home Connect's REST API, read-only: the appliances, their state, and their event stream.

Home Connect allows 1000 requests per day for a user and application. Opening the event stream
(``GET /api/homeappliances/events``, server-sent events for every appliance) counts as one, and
what arrives on it costs nothing, so a source reads each appliance's state once when it connects
and follows the stream from then on. The stream sends a ``KEEP-ALIVE`` every 55 seconds, so a
read that waits much longer than that means the connection is dead.

A request answered 401 raises :class:`Unauthorized` (the access token has expired), 429
:class:`RateLimited` with the seconds Home Connect asks to wait, and any other failure
:class:`ApiError`. A part of an appliance's state it cannot give, such as the active programme
while none runs or anything while the appliance is offline, is left out.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import httpx2

from ingest_hub.adapters.home_connect.auth import URL

TIMEOUT = 30.0
#: The longest the event stream may stay silent: two keep-alives and a margin.
STREAM_READ_TIMEOUT = 130.0
ACCEPT = "application/vnd.bsh.sdk.v1+json"
ACTIVE_PROGRAM = "BSH.Common.Root.ActiveProgram"
SELECTED_PROGRAM = "BSH.Common.Root.SelectedProgram"


class ApiError(Exception):
    """A request to Home Connect failed; the message says why."""


class Unauthorized(ApiError):
    """Home Connect refused the access token."""


class RateLimited(ApiError):
    """Home Connect asks to wait ``retry_after`` seconds before the next request."""

    def __init__(self, retry_after: float) -> None:
        super().__init__(
            f"Home Connect's request limit is reached; retrying in {retry_after:.0f} s"
        )
        self.retry_after = retry_after


@dataclass(frozen=True)
class Appliance:
    """A home appliance paired with the account."""

    ha_id: str
    type: str
    brand: str
    model: str
    name: str
    connected: bool

    @property
    def label(self) -> str:
        """The brand, model and type, such as ``Brand DW1 (Dishwasher)``, for status pages."""
        maker = " ".join(part for part in (self.brand, self.model) if part) or self.name
        return f"{maker} ({self.type})" if self.type else maker


@dataclass(frozen=True)
class Event:
    """One server-sent event: its kind (``STATUS``, ``EVENT``, ``NOTIFY``, ``KEEP-ALIVE``,
    ``CONNECTED``, ``DISCONNECTED``, ``PAIRED``, ``DEPAIRED``, ...), the appliance it concerns,
    and its items, each with a ``key``, a ``value`` and a ``timestamp`` in Unix seconds."""

    kind: str
    ha_id: str = ""
    items: list[dict[str, Any]] = field(default_factory=list)


def _error_key(response: httpx2.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return ""
    error = body.get("error") if isinstance(body, dict) else None
    return str(error.get("key") or "") if isinstance(error, dict) else ""


def _raise_for(response: httpx2.Response) -> None:
    if response.status_code == 401:
        raise Unauthorized("Home Connect refused the access token")
    if response.status_code == 429:
        try:
            wait = float(response.headers.get("retry-after") or 60)
        except ValueError:
            wait = 60.0
        raise RateLimited(max(wait, 1.0))
    key = _error_key(response)
    raise ApiError(f"Home Connect answered HTTP {response.status_code} {key}".rstrip())


class HomeConnectApi:
    """Requests with one access token, counted in ``requests``."""

    def __init__(self, client: httpx2.AsyncClient, access_token: str, url: str = URL) -> None:
        self._client = client
        self._url = url
        self._headers = {"Authorization": f"Bearer {access_token}", "Accept": ACCEPT}
        self.requests = 0

    async def _get(self, path: str) -> dict[str, Any] | None:
        """The ``data`` of a GET, or None for a 404 or 409 (nothing to give, or offline)."""
        self.requests += 1
        try:
            response = await self._client.get(
                f"{self._url}/api{path}", headers=self._headers, timeout=TIMEOUT
            )
        except httpx2.HTTPError as error:
            raise ApiError(
                f"cannot reach Home Connect: {str(error) or type(error).__name__}"
            ) from error
        if response.status_code in (404, 409):
            return None
        if response.status_code != 200:
            _raise_for(response)
        try:
            body = response.json()
        except ValueError as error:
            raise ApiError("Home Connect answered with something other than JSON") from error
        data = body.get("data") if isinstance(body, dict) else None
        return data if isinstance(data, dict) else {}

    async def appliances(self) -> list[Appliance]:
        """Every appliance paired with the account."""
        data = await self._get("/homeappliances") or {}
        return [
            Appliance(
                ha_id=str(item["haId"]),
                type=str(item.get("type") or ""),
                brand=str(item.get("brand") or ""),
                model=str(item.get("vib") or ""),
                name=str(item.get("name") or ""),
                connected=bool(item.get("connected")),
            )
            for item in data.get("homeappliances") or []
            if isinstance(item, dict) and item.get("haId")
        ]

    async def state(self, ha_id: str) -> list[tuple[str, Any]]:
        """An appliance's status, settings and programmes, as keys and values. The selected
        programme's options come before the active one's, which share their keys and win."""
        base = f"/homeappliances/{ha_id}"
        items: list[tuple[str, Any]] = []
        for part, kind in (("/status", "status"), ("/settings", "settings")):
            data = await self._get(base + part) or {}
            items += [(i["key"], i.get("value")) for i in data.get(kind) or [] if "key" in i]
        for part, root in (
            ("/programs/selected", SELECTED_PROGRAM),
            ("/programs/active", ACTIVE_PROGRAM),
        ):
            data = await self._get(base + part)
            items.append((root, data.get("key") if data else None))
            items += [
                (i["key"], i.get("value")) for i in (data or {}).get("options") or [] if "key" in i
            ]
        return items

    async def events(self) -> AsyncIterator[Event]:
        """The event stream, for every appliance, until Home Connect closes it."""
        self.requests += 1
        headers = {**self._headers, "Accept": "text/event-stream"}
        timeout = httpx2.Timeout(TIMEOUT, read=STREAM_READ_TIMEOUT)
        try:
            async with self._client.stream(
                "GET", f"{self._url}/api/homeappliances/events", headers=headers, timeout=timeout
            ) as response:
                if response.status_code != 200:
                    await response.aread()
                    _raise_for(response)
                async for event in parse_events(response.aiter_lines()):
                    yield event
        except httpx2.HTTPError as error:
            raise ApiError(
                f"the event stream broke: {str(error) or type(error).__name__}"
            ) from error


async def parse_events(lines: AsyncIterator[str]) -> AsyncIterator[Event]:
    """Server-sent events from their lines: ``event:``, ``data:`` and ``id:`` fields, each event
    ended by a blank line. Data that is not a JSON object leaves the event without items."""
    kind, data, ha_id = "", [], ""
    async for raw in lines:
        line = raw.rstrip("\r\n")
        if line:
            name, _, value = line.partition(":")
            value = value.removeprefix(" ")
            if name == "event":
                kind = value
            elif name == "data":
                data.append(value)
            elif name == "id":
                ha_id = value
            continue
        if kind or data:
            yield _event(kind or "message", "\n".join(data), ha_id)
        kind, data, ha_id = "", [], ""
    if kind or data:
        yield _event(kind or "message", "\n".join(data), ha_id)


def _event(kind: str, data: str, ha_id: str) -> Event:
    try:
        body = json.loads(data) if data else {}
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    items = [i for i in body.get("items") or [] if isinstance(i, dict) and "key" in i]
    return Event(kind, str(body.get("haId") or ha_id), items)


__all__ = [
    "ACTIVE_PROGRAM",
    "SELECTED_PROGRAM",
    "ApiError",
    "Appliance",
    "Event",
    "HomeConnectApi",
    "RateLimited",
    "Unauthorized",
    "parse_events",
]

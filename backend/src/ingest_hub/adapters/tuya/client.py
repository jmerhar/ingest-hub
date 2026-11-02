"""A read-only client for the Tuya cloud OpenAPI.

Every request is signed with the cloud project's Access ID and Secret: an uppercase hex
HMAC-SHA256 of ``client_id + access_token + t + stringToSign``, where ``stringToSign`` is
``"GET\\n" + sha256("") + "\\n\\n" + path`` and the path's query parameters are sorted by name
(unsorted parameters are refused as "sign invalid"). The token call itself is signed without a
token.

Responses carry device details such as local keys and IP addresses, so they are never logged;
errors name only Tuya's code and message.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx2

logger = logging.getLogger(__name__)

#: The OpenAPI host of each data centre.
HOSTS = {
    "eu": "https://openapi.tuyaeu.com",
    "weu": "https://openapi-weaz.tuyaeu.com",
    "us": "https://openapi.tuyaus.com",
    "cn": "https://openapi.tuyacn.com",
    "in": "https://openapi.tuyain.com",
}

#: "The log query is too frequent".
RATE_LIMITED = 40000309
#: The token is invalid or has expired; fetching a new one fixes it.
TOKEN_CODES = frozenset({1010, 1011})
#: Tuya refuses device pages of more than this many, and report-log queries of more codes.
MAX_PAGE = 20
MAX_CODES = 20
#: Report logs per page; Tuya's maximum.
LOG_PAGE = 100
TIMEOUT_SECONDS = 30.0
#: A token is replaced this long before Tuya says it expires.
TOKEN_MARGIN_SECONDS = 60.0
#: Attempts at a rate-limited call, with a pause that doubles from the first.
RATE_LIMIT_ATTEMPTS = 6
RATE_LIMIT_FIRST_PAUSE = 2.0
_EMPTY_BODY_HASH = hashlib.sha256(b"").hexdigest()


class TuyaError(Exception):
    """A call that failed, with Tuya's error code when it gave one."""

    def __init__(self, message: str, code: int | None = None) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class Device:
    """A device the cloud project can see. Names come from the app and need not be unique."""

    id: str
    name: str
    category: str
    online: bool


@dataclass(frozen=True)
class Report:
    """One entry of a device's report log: a status code's value at a moment."""

    code: str
    value: str
    event_time: int


def canonical(path: str) -> str:
    """A path with its query parameters sorted by name, the form Tuya signs."""
    route, _, query = path.partition("?")
    if not query:
        return route
    pairs = sorted(query.split("&"), key=lambda pair: pair.partition("=")[0])
    return f"{route}?{'&'.join(pairs)}"


def sign(access_id: str, secret: str, token: str, timestamp: str, path: str) -> str:
    """The ``sign`` header for a GET of ``path``, which must already be canonical."""
    to_sign = f"GET\n{_EMPTY_BODY_HASH}\n\n{path}"
    message = (access_id + token + timestamp + to_sign).encode()
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest().upper()


class TuyaClient:
    """Calls the OpenAPI of one cloud project, keeping its access token fresh."""

    def __init__(
        self,
        access_id: str,
        secret: str,
        base_url: str,
        client: httpx2.AsyncClient,
        *,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._access_id = access_id
        self._secret = secret
        self._base_url = base_url.rstrip("/")
        self._client = client
        self._clock = clock
        self._sleep = sleep
        self._token = ""
        self._token_expires = 0.0

    async def get(self, path: str) -> Any:
        """Call an endpoint and return its ``result``.

        A rate-limited call is retried after growing pauses, and a call refused for its token is
        retried once with a new one. Anything else Tuya refuses raises :class:`TuyaError`.
        """
        pause = RATE_LIMIT_FIRST_PAUSE
        refreshed = False
        attempt = 1
        while True:
            try:
                token = await self._valid_token()
                return await self._call(path, token)
            except TuyaError as error:
                if error.code in TOKEN_CODES and not refreshed:
                    refreshed = True
                    self._token = ""
                    continue
                if error.code != RATE_LIMITED or attempt >= RATE_LIMIT_ATTEMPTS:
                    raise
                logger.info("Tuya rate limit; retrying in %.0f s", pause)
                await self._sleep(pause)
                pause *= 2
                attempt += 1

    async def devices(self) -> list[Device]:
        """Every device the project can see."""
        devices: list[Device] = []
        last_id = ""
        while True:
            path = f"/v2.0/cloud/thing/device?page_size={MAX_PAGE}"
            if last_id:
                path += f"&last_id={quote(last_id, safe='')}"
            page = await self.get(path) or []
            for item in page:
                name = str(item.get("customName") or item.get("name") or "").strip()
                devices.append(
                    Device(
                        id=str(item["id"]),
                        name=name,
                        category=str(item.get("category") or "unknown"),
                        online=bool(item.get("isOnline")),
                    )
                )
                last_id = str(item["id"])
            if len(page) < MAX_PAGE:
                return devices

    async def specification(self, device_id: str) -> list[dict[str, Any]]:
        """The status codes a device reports, each with its type and value range."""
        result = await self.get(f"/v1.0/devices/{quote(device_id, safe='')}/specifications")
        return list((result or {}).get("status") or [])

    async def reports(
        self, device_id: str, codes: list[str], start_ms: int, end_ms: int
    ) -> list[Report]:
        """A device's report log for ``codes`` between two times (inclusive), every page."""
        reports: list[Report] = []
        for first in range(0, len(codes), MAX_CODES):
            chunk = ",".join(codes[first : first + MAX_CODES])
            key = ""
            while True:
                path = (
                    f"/v2.0/cloud/thing/{quote(device_id, safe='')}/report-logs"
                    f"?codes={quote(chunk, safe=',')}&start_time={start_ms}"
                    f"&end_time={end_ms}&size={LOG_PAGE}"
                )
                if key:
                    path += f"&last_row_key={quote(key, safe='')}"
                page = await self.get(path) or {}
                reports.extend(
                    Report(str(log["code"]), str(log["value"]), int(log["event_time"]))
                    for log in page.get("logs") or []
                )
                key = str(page.get("last_row_key") or "")
                if not page.get("has_more") or not key:
                    break
        return reports

    async def _valid_token(self) -> str:
        """The access token, fetched when there is none or it is about to expire."""
        if self._token and self._clock() < self._token_expires:
            return self._token
        result = await self._call("/v1.0/token?grant_type=1", "")
        self._token = str(result["access_token"])
        lifetime = float(result.get("expire_time") or 0)
        self._token_expires = self._clock() + max(0.0, lifetime - TOKEN_MARGIN_SECONDS)
        return self._token

    async def _call(self, path: str, token: str) -> Any:
        """One signed GET, returning ``result`` or raising :class:`TuyaError`."""
        path = canonical(path)
        timestamp = str(int(self._clock() * 1000))
        headers = {
            "client_id": self._access_id,
            "sign": sign(self._access_id, self._secret, token, timestamp, path),
            "t": timestamp,
            "sign_method": "HMAC-SHA256",
        }
        if token:
            headers["access_token"] = token
        route = path.partition("?")[0]
        try:
            response = await self._client.get(
                self._base_url + path, headers=headers, timeout=TIMEOUT_SECONDS
            )
        except httpx2.RequestError as error:
            raise TuyaError(f"cannot reach Tuya for {route}: {type(error).__name__}") from error
        try:
            body = response.json()
        except ValueError:
            body = None
        if not isinstance(body, dict) or not body.get("success"):
            code = body.get("code") if isinstance(body, dict) else None
            message = body.get("msg") if isinstance(body, dict) else None
            raise TuyaError(
                f"Tuya refused {route} (HTTP {response.status_code}, code {code}): "
                f"{message or 'no usable response'}",
                code if isinstance(code, int) else None,
            )
        return body.get("result")

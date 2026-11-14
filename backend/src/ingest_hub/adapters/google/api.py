"""A client for the Google Health API (``health.googleapis.com/v4``), as one person.

Requests carry an access token from :func:`oauth.refresh`, renewed shortly before it expires and
once more when the API answers 401. A list request follows ``nextPageToken`` until the last page.
Google limits each person to 300 requests a minute and answers 429 beyond it; that, and a server
error, is waited out and retried a few times, for as long as ``Retry-After`` says when it says.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import httpx2

from ingest_hub.adapters.google.oauth import TOKEN_URL, Credentials, refresh

logger = logging.getLogger(__name__)

API_URL = "https://health.googleapis.com/v4"
TIMEOUT = 60.0
#: Tries of one request before its failure is reported.
TRIES = 4
#: The first wait after a refused or failed request, doubled each time, in seconds.
BACKOFF = 5.0
#: The longest wait a ``Retry-After`` is followed for, in seconds.
MAX_WAIT = 120.0
#: Renew an access token this many seconds before Google says it expires.
EARLY = 120.0
#: The page size asked for; the API caps sessions (sleep, exercise) at 25 a page.
PAGE_SIZE = 10000
SESSION_PAGE_SIZE = 25
SESSION_TYPES = frozenset({"sleep", "exercise"})


class ApiError(Exception):
    """A request failed; the message says which and why."""


class HealthApi:
    """One person's Google Health data."""

    def __init__(
        self,
        client: httpx2.AsyncClient,
        credentials: Credentials,
        refresh_token: str,
        *,
        url: str = API_URL,
        token_url: str = TOKEN_URL,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._client = client
        self._credentials = credentials
        self._refresh_token = refresh_token
        self._url = url.rstrip("/")
        self._token_url = token_url
        self._clock = clock
        self._sleep = sleep
        self._access: str | None = None
        self._expires = 0.0
        #: Requests made, for the logs.
        self.requests = 0

    async def _token(self) -> str:
        if self._access is None or self._clock() >= self._expires - EARLY:
            self._access, lasts = await refresh(
                self._client, self._credentials, self._refresh_token, self._token_url
            )
            self._expires = self._clock() + lasts
        return self._access

    async def get(self, path: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        """A GET, its answer as JSON; :class:`ApiError` when it fails for good."""
        renewed = False
        wait = BACKOFF
        for attempt in range(1, TRIES + 1):
            token = await self._token()
            self.requests += 1
            try:
                response = await self._client.get(
                    self._url + path,
                    params=params,
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                    timeout=TIMEOUT,
                )
            except httpx2.HTTPError as error:
                problem = str(error) or type(error).__name__
            else:
                if response.status_code == 200:
                    return response.json()
                if response.status_code == 401 and not renewed:
                    renewed = True
                    self._access = None
                    continue
                problem = f"HTTP {response.status_code} {_reason(response)}".rstrip()
                if response.status_code != 429 and response.status_code < 500:
                    raise ApiError(f"{path}: {problem}")
                wait = _retry_after(response) or wait
            if attempt < TRIES:
                logger.info("%s: %s; trying again in %.0f s", path, problem, wait)
                await self._sleep(min(wait, MAX_WAIT))
                wait *= 2
        raise ApiError(f"{path}: {problem}")

    async def identity(self) -> dict[str, Any]:
        """The person's identity: their Health user ID, once their account is linked."""
        return await self.get("/users/me/identity")

    async def data_points(self, data_type: str, filter_: str) -> AsyncIterator[dict[str, Any]]:
        """Every data point of a type (kebab case) that a filter selects, page by page."""
        size = SESSION_PAGE_SIZE if data_type in SESSION_TYPES else PAGE_SIZE
        params = {"filter": filter_, "pageSize": str(size)}
        while True:
            body = await self.get(f"/users/me/dataTypes/{data_type}/dataPoints", params)
            for point in body.get("dataPoints") or []:
                if isinstance(point, dict):
                    yield point
            token = body.get("nextPageToken")
            if not token:
                return
            params = {**params, "pageToken": str(token)}


def _reason(response: httpx2.Response) -> str:
    """The reason and message of an error answer, as short as Google gives them."""
    try:
        error = response.json().get("error") or {}
    except ValueError, AttributeError:
        return ""
    reasons = [
        str(detail.get("reason"))
        for detail in error.get("details") or []
        if isinstance(detail, dict) and detail.get("reason")
    ]
    message = str(error.get("message") or error.get("status") or "")
    return " ".join([*reasons, message]).strip()


def _retry_after(response: httpx2.Response) -> float | None:
    """The wait a ``Retry-After`` header asks for, in seconds, when it gives one."""
    try:
        return max(0.0, float(response.headers.get("retry-after", "")))
    except ValueError:
        return None


__all__ = ["API_URL", "ApiError", "HealthApi"]

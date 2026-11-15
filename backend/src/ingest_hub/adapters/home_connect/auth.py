"""Signing in to Home Connect: the one-time device login, and the tokens it leaves.

The login is OAuth 2.0's device flow, for an application registered in the Home Connect Developer
Program with "Device Flow": :func:`start_login` asks for a code, which the person confirms in a
browser while :func:`finish_login` waits for the answer. This works wherever the command runs, a
container on a server included. It asks for read-only access (:data:`SCOPES`): identifying the
appliances, monitoring them and reading their settings, never starting or changing anything.

The tokens are kept in the source's state directory, readable by their owner only
(:func:`save_tokens`). An access token lasts a day; :func:`refresh` trades the refresh token for a
new one, keeping a new refresh token if Home Connect gives one. A refresh token Home Connect no
longer accepts, because access was withdrawn or it went unused for too long, raises
:class:`TokenRefused`, so the source can report that the login has to be repeated.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

import httpx2

from ingest_hub.adapters.inbox import write_json

URL = "https://api.home-connect.com"
#: Read-only: identify the appliances, monitor their state, read their settings.
SCOPES = "IdentifyAppliance Monitor Settings"
#: The tokens, in the source's state directory.
TOKEN_FILE = "home-connect-token.json"
TIMEOUT = 30.0
#: How long before an access token expires it is replaced.
REFRESH_MARGIN_SECONDS = 600.0


class AuthError(Exception):
    """A login or token request failed; the message says why."""


class TokenRefused(AuthError):
    """Home Connect no longer accepts the saved refresh token: the login has to be repeated."""


@dataclass(frozen=True)
class Tokens:
    """A refresh token, and the access token it gave with when that expires (Unix seconds)."""

    refresh_token: str
    access_token: str = ""
    expires_at: float = 0.0

    def fresh(self, now: float) -> bool:
        """Whether the access token is good for at least :data:`REFRESH_MARGIN_SECONDS` more."""
        return bool(self.access_token) and self.expires_at - now > REFRESH_MARGIN_SECONDS


@dataclass(frozen=True)
class DeviceLogin:
    """A started login: the code to confirm, where, and how to poll for its answer."""

    device_code: str
    user_code: str
    verification_url: str
    #: Seconds between polls, and until the code expires.
    interval: float
    expires_in: float


async def _post(client: httpx2.AsyncClient, url: str, form: dict[str, str]) -> tuple[int, dict]:
    """A form posted to one of Home Connect's OAuth endpoints: its status and JSON answer, empty
    when the answer is not JSON."""
    try:
        response = await client.post(url, data=form, timeout=TIMEOUT)
    except httpx2.HTTPError as error:
        raise AuthError(
            f"cannot reach Home Connect: {str(error) or type(error).__name__}"
        ) from error
    try:
        body = response.json()
    except ValueError:
        body = {}
    return response.status_code, body if isinstance(body, dict) else {}


def _reason(status: int, body: dict) -> str:
    description = body.get("error_description") or ""
    return f"HTTP {status} {body.get('error') or ''} {description}".rstrip()


async def start_login(client: httpx2.AsyncClient, client_id: str, url: str = URL) -> DeviceLogin:
    """A new device login for the application ``client_id``."""
    status, body = await _post(
        client,
        f"{url}/security/oauth/device_authorization",
        {"client_id": client_id, "scope": SCOPES},
    )
    if status != 200 or not body.get("device_code"):
        raise AuthError(f"Home Connect refused to start a login: {_reason(status, body)}")
    return DeviceLogin(
        device_code=str(body["device_code"]),
        user_code=str(body.get("user_code") or ""),
        verification_url=str(
            body.get("verification_uri_complete") or body.get("verification_uri") or ""
        ),
        interval=_number(body.get("interval"), 5.0),
        expires_in=_number(body.get("expires_in"), 600.0),
    )


def _number(value: object, default: float) -> float:
    """A number from an answer, or ``default`` when it has none."""
    return float(value) if isinstance(value, (int, float)) else default


async def finish_login(
    client: httpx2.AsyncClient,
    client_id: str,
    login: DeviceLogin,
    *,
    now: Callable[[], float],
    sleep: Callable[[float], Awaitable[None]],
    url: str = URL,
) -> Tokens:
    """Wait until the person confirms the login's code, and return the tokens it gives; raises
    :class:`AuthError` when it is refused or the code expires first."""
    deadline = now() + login.expires_in
    interval = login.interval
    while now() < deadline:
        await sleep(interval)
        status, body = await _post(
            client,
            f"{url}/security/oauth/token",
            {
                "grant_type": "device_code",
                "device_code": login.device_code,
                "client_id": client_id,
            },
        )
        if status == 200 and body.get("refresh_token"):
            return _tokens(body, now(), "")
        error = body.get("error")
        if error == "slow_down":
            interval += 5
        elif error != "authorization_pending" and status != 200:
            raise AuthError(f"the login was not completed: {_reason(status, body)}")
    raise AuthError("the code expired before it was confirmed; start again")


async def refresh(
    client: httpx2.AsyncClient, client_id: str, tokens: Tokens, now: float, url: str = URL
) -> Tokens:
    """A new access token for ``tokens``' refresh token."""
    status, body = await _post(
        client,
        f"{url}/security/oauth/token",
        {
            "grant_type": "refresh_token",
            "refresh_token": tokens.refresh_token,
            "client_id": client_id,
        },
    )
    if status == 200 and body.get("access_token"):
        return _tokens(body, now, tokens.refresh_token)
    if body.get("error") in ("invalid_grant", "unauthorized_client"):
        raise TokenRefused(f"Home Connect refused the login ({body['error']})")
    raise AuthError(f"Home Connect's token endpoint answered {_reason(status, body)}")


def _tokens(body: dict, now: float, refresh_token: str) -> Tokens:
    return Tokens(
        refresh_token=str(body.get("refresh_token") or refresh_token),
        access_token=str(body.get("access_token") or ""),
        expires_at=now + float(body.get("expires_in") or 86400),
    )


def save_tokens(state_dir: Path, tokens: Tokens) -> Path:
    """Keep the tokens in a state directory, readable by their owner only, written whole."""
    path = state_dir / TOKEN_FILE
    write_json(
        path,
        {
            "refresh_token": tokens.refresh_token,
            "access_token": tokens.access_token,
            "expires_at": tokens.expires_at,
        },
        private=True,
    )
    return path


def load_tokens(state_dir: Path) -> Tokens | None:
    """The tokens kept in a state directory, or None when there are none."""
    try:
        data = json.loads((state_dir / TOKEN_FILE).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as error:
        raise AuthError(f"cannot read {TOKEN_FILE}: {error}") from error
    if not isinstance(data, dict) or not data.get("refresh_token"):
        return None
    return Tokens(
        str(data["refresh_token"]),
        str(data.get("access_token") or ""),
        float(data.get("expires_at") or 0.0),
    )


__all__ = [
    "SCOPES",
    "AuthError",
    "DeviceLogin",
    "TokenRefused",
    "Tokens",
    "finish_login",
    "load_tokens",
    "refresh",
    "save_tokens",
    "start_login",
]

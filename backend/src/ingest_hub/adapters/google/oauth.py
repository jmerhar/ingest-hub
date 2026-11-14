"""Signing in to Google for the Google Health API: the one-time consent and its refresh token.

The consent is Google's OAuth 2.0 flow for an installed application (a "Desktop app" client) with
PKCE: the person opens :func:`authorization_url` in a browser, consents, and Google redirects the
browser to ``http://127.0.0.1:<port>/`` with a code. Nothing listens there, so the browser shows
an error page; its address holds the code, and the person pastes it back
(:func:`code_from_redirect`). This works wherever the command runs, a container on a server
included, without a port reaching it. The consent is started and finished by two runs of the
command, so neither has to wait for input; the started one is kept in between
(:func:`save_consent`). :func:`exchange` trades the code for a refresh token, kept
in the source's state directory, readable by its owner only (:func:`save_token`), and
:func:`refresh` trades that for an access token, which lasts an hour.

Google ends a refresh token when the person withdraws access, when it is unused for six months,
and, while the OAuth app's publishing status is "Testing", after seven days; :class:`TokenRefused`
says so, so the source can report it rather than retrying in silence.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import os
import secrets
import urllib.parse
from dataclasses import dataclass
from pathlib import Path

import httpx2

AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
SCOPE_PREFIX = "https://www.googleapis.com/auth/googlehealth."
#: The read-only scopes the source asks for, and nothing else.
SCOPES = (
    SCOPE_PREFIX + "sleep.readonly",
    SCOPE_PREFIX + "health_metrics_and_measurements.readonly",
    SCOPE_PREFIX + "activity_and_fitness.readonly",
)
#: The refresh token, in the source's state directory.
TOKEN_FILE = "google-token.json"
#: A consent started and not yet finished, in the source's state directory.
CONSENT_FILE = "google-consent.json"
TIMEOUT = 30.0


class AuthError(Exception):
    """The consent or a token request failed; the message says why."""


class TokenRefused(AuthError):
    """Google no longer accepts the saved refresh token: the consent has to be given again."""


@dataclass(frozen=True)
class Credentials:
    """The OAuth client the consent is given to."""

    client_id: str
    client_secret: str


@dataclass(frozen=True)
class Consent:
    """What one consent needs to keep between showing the address and reading the answer."""

    url: str
    state: str
    verifier: str
    redirect_uri: str


def start_consent(credentials: Credentials, port: int) -> Consent:
    """A new consent: the address to open, with a fresh state and PKCE verifier."""
    verifier = secrets.token_urlsafe(64)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=")
    state = secrets.token_urlsafe(16)
    redirect_uri = f"http://127.0.0.1:{port}/"
    query = {
        "client_id": credentials.client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
        "code_challenge": challenge.decode(),
        "code_challenge_method": "S256",
    }
    return Consent(f"{AUTH_URL}?{urllib.parse.urlencode(query)}", state, verifier, redirect_uri)


def code_from_redirect(pasted: str, state: str) -> str:
    """The code in the address the browser was sent to after consenting; raises
    :class:`AuthError` when it is refused, from another consent, or has none."""
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(pasted.strip()).query)
    if error := query.get("error"):
        raise AuthError(f"Google answered {error[0]}: the consent was not given")
    if query.get("state") != [state]:
        raise AuthError("that address is not from this consent; start again")
    if not (code := query.get("code")):
        raise AuthError("that address holds no code; paste the whole address")
    return code[0]


async def _token_request(client: httpx2.AsyncClient, form: dict[str, str], url: str) -> dict:
    """A request to the token endpoint, its answer as JSON; :class:`TokenRefused` when Google
    refuses the grant, :class:`AuthError` for any other failure."""
    try:
        response = await client.post(url, data=form, timeout=TIMEOUT)
    except httpx2.HTTPError as error:
        raise AuthError(f"cannot reach Google: {str(error) or type(error).__name__}") from error
    try:
        body = response.json()
    except ValueError:
        body = {}
    if response.status_code == 200 and isinstance(body, dict):
        return body
    reason = body.get("error") if isinstance(body, dict) else None
    if isinstance(reason, dict):
        reason = reason.get("status")
    if reason == "invalid_grant":
        raise TokenRefused("Google refused the sign-in (withdrawn, expired or unused)")
    raise AuthError(
        f"Google's token endpoint answered HTTP {response.status_code} {reason or ''}".rstrip()
    )


async def exchange(
    client: httpx2.AsyncClient,
    credentials: Credentials,
    consent: Consent,
    code: str,
    token_url: str = TOKEN_URL,
) -> tuple[str, list[str]]:
    """The refresh token for a consent's code, and the scopes the person granted."""
    body = await _token_request(
        client,
        {
            "code": code,
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
            "redirect_uri": consent.redirect_uri,
            "grant_type": "authorization_code",
            "code_verifier": consent.verifier,
        },
        token_url,
    )
    if not (refresh_token := body.get("refresh_token")):
        raise AuthError("Google gave no refresh token; start again")
    return str(refresh_token), str(body.get("scope") or "").split()


async def refresh(
    client: httpx2.AsyncClient,
    credentials: Credentials,
    refresh_token: str,
    token_url: str = TOKEN_URL,
) -> tuple[str, float]:
    """A new access token and how many seconds it lasts."""
    body = await _token_request(
        client,
        {
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        token_url,
    )
    if not (access := body.get("access_token")):
        raise AuthError("Google gave no access token")
    return str(access), float(body.get("expires_in") or 3600)


def missing_scopes(granted: list[str]) -> list[str]:
    """The scopes the source asks for that a consent did not grant, by their short names."""
    return [scope.removeprefix(SCOPE_PREFIX) for scope in SCOPES if scope not in granted]


def _write_private(path: Path, data: dict) -> None:
    """Write JSON readable by its owner only, appearing whole under its name."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.part")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(data, handle)
    os.replace(temporary, path)


def save_consent(state_dir: Path, consent: Consent) -> None:
    """Keep a started consent until its answer is pasted, replacing any earlier one."""
    data = {
        "state": consent.state,
        "verifier": consent.verifier,
        "redirect_uri": consent.redirect_uri,
    }
    _write_private(state_dir / CONSENT_FILE, {**data, "url": consent.url})


def load_consent(state_dir: Path) -> Consent:
    """The consent started last; raises :class:`AuthError` when there is none."""
    try:
        data = json.loads((state_dir / CONSENT_FILE).read_text(encoding="utf-8"))
        return Consent(data["url"], data["state"], data["verifier"], data["redirect_uri"])
    except FileNotFoundError:
        raise AuthError("no consent was started; run this without --address first") from None
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise AuthError(f"cannot read {CONSENT_FILE}: {error}") from error


def save_token(state_dir: Path, refresh_token: str, scopes: list[str], when: float) -> Path:
    """Keep a refresh token in a state directory, readable by its owner only, written whole."""
    path = state_dir / TOKEN_FILE
    data = {
        "refresh_token": refresh_token,
        "scopes": scopes,
        "granted_at": dt.datetime.fromtimestamp(when, dt.UTC).isoformat(),
    }
    _write_private(path, data)
    (state_dir / CONSENT_FILE).unlink(missing_ok=True)
    return path


def load_token(state_dir: Path) -> str | None:
    """The refresh token kept in a state directory, or None when there is none."""
    try:
        data = json.loads((state_dir / TOKEN_FILE).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as error:
        raise AuthError(f"cannot read {TOKEN_FILE}: {error}") from error
    token = data.get("refresh_token") if isinstance(data, dict) else None
    return str(token) if token else None


__all__ = [
    "SCOPES",
    "AuthError",
    "Consent",
    "Credentials",
    "TokenRefused",
    "code_from_redirect",
    "exchange",
    "load_consent",
    "load_token",
    "missing_scopes",
    "refresh",
    "save_consent",
    "save_token",
    "start_consent",
]

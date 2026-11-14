"""Consenting to the Google Health API and keeping its tokens, against a stand-in Google."""

from __future__ import annotations

import base64
import hashlib
import json
import urllib.parse

import httpx2
import pytest
from jigkit.testing.http import StubServer, serving

from ingest_hub.adapters.google import oauth
from ingest_hub.adapters.google.oauth import (
    SCOPES,
    AuthError,
    Credentials,
    TokenRefused,
    code_from_redirect,
    exchange,
    load_consent,
    load_token,
    missing_scopes,
    refresh,
    save_consent,
    save_token,
    start_consent,
)

from .google_api_fake import CLIENT_ID, CLIENT_SECRET, CODE, REFRESH, FakeGoogle

CREDENTIALS = Credentials(CLIENT_ID, CLIENT_SECRET)


def test_a_consent_asks_for_offline_read_only_access_with_pkce():
    consent = start_consent(CREDENTIALS, 8765)
    url = urllib.parse.urlsplit(consent.url)
    assert f"{url.scheme}://{url.netloc}{url.path}" == oauth.AUTH_URL
    query = {k: v[0] for k, v in urllib.parse.parse_qs(url.query).items()}
    challenge = base64.urlsafe_b64encode(hashlib.sha256(consent.verifier.encode()).digest())
    assert query == {
        "client_id": CLIENT_ID,
        "redirect_uri": "http://127.0.0.1:8765/",
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "state": consent.state,
        "code_challenge": challenge.rstrip(b"=").decode(),
        "code_challenge_method": "S256",
    }
    assert all(scope.endswith(".readonly") for scope in SCOPES)
    assert start_consent(CREDENTIALS, 8765).state != consent.state


def test_the_code_is_read_from_the_pasted_address():
    address = f"http://127.0.0.1:8765/?state=s1&iss=https://accounts.google.com&code={CODE}&scope=x"
    assert code_from_redirect(f"  {address}\n", "s1") == CODE
    with pytest.raises(AuthError, match="access_denied"):
        code_from_redirect("http://127.0.0.1:8765/?error=access_denied&state=s1", "s1")
    with pytest.raises(AuthError, match="not from this consent"):
        code_from_redirect(address, "s2")
    with pytest.raises(AuthError, match="holds no code"):
        code_from_redirect("http://127.0.0.1:8765/?state=s1", "s1")


async def test_a_code_is_exchanged_for_a_refresh_token_and_that_for_access_tokens():
    fake = FakeGoogle()
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as client:
        url = server.url + "/token"
        consent = start_consent(CREDENTIALS, 8765)
        token, scopes = await exchange(client, CREDENTIALS, consent, CODE, url)
        assert token == REFRESH and scopes == fake.granted.split()
        assert await refresh(client, CREDENTIALS, REFRESH, url) == ("access-1", 3600.0)
        with pytest.raises(TokenRefused, match="refused the sign-in"):
            await exchange(client, CREDENTIALS, consent, "used-before", url)
        fake.refresh_ok = False
        with pytest.raises(TokenRefused):
            await refresh(client, CREDENTIALS, REFRESH, url)
        with pytest.raises(AuthError, match="HTTP 401 invalid_client"):
            await refresh(client, Credentials(CLIENT_ID, "wrong"), REFRESH, url)
        with pytest.raises(AuthError, match="HTTP 401 UNAUTHENTICATED"):
            await refresh(client, CREDENTIALS, REFRESH, server.url + "/elsewhere")


@pytest.mark.parametrize(
    ("answer", "message"),
    [
        ({"scope": "x"}, "no refresh token"),
        ({"refresh_token": "r"}, None),
    ],
)
async def test_an_exchange_without_a_refresh_token_fails(answer, message):
    stub = StubServer(status=200, reply=json.dumps(answer))
    async with serving(stub) as server, httpx2.AsyncClient() as client:
        consent = start_consent(CREDENTIALS, 1)
        if message:
            with pytest.raises(AuthError, match=message):
                await exchange(client, CREDENTIALS, consent, CODE, server.url)
        else:
            assert await exchange(client, CREDENTIALS, consent, CODE, server.url) == ("r", [])


async def test_a_token_answer_without_an_access_token_or_json_fails():
    async with (
        serving(StubServer(status=200, reply="{}")) as server,
        httpx2.AsyncClient() as client,
    ):
        with pytest.raises(AuthError, match="no access token"):
            await refresh(client, CREDENTIALS, REFRESH, server.url)
    async with (
        serving(StubServer(status=500, reply="<html>")) as server,
        httpx2.AsyncClient() as client,
    ):
        with pytest.raises(AuthError, match="HTTP 500"):
            await refresh(client, CREDENTIALS, REFRESH, server.url)


async def test_an_unreachable_token_endpoint_fails():
    async with httpx2.AsyncClient() as client:
        with pytest.raises(AuthError, match="cannot reach Google"):
            await refresh(client, CREDENTIALS, REFRESH, "http://127.0.0.1:9/token")


def test_scopes_not_granted_are_named():
    assert missing_scopes(list(SCOPES)) == []
    assert missing_scopes([SCOPES[0]]) == [
        "health_metrics_and_measurements.readonly",
        "activity_and_fitness.readonly",
    ]


def test_the_token_and_a_started_consent_are_kept_privately(tmp_path):
    state = tmp_path / "state"
    assert load_token(state) is None
    with pytest.raises(AuthError, match="no consent was started"):
        load_consent(state)
    consent = start_consent(CREDENTIALS, 8765)
    save_consent(state, consent)
    assert load_consent(state) == consent
    assert (state / oauth.CONSENT_FILE).stat().st_mode & 0o777 == 0o600
    path = save_token(state, REFRESH, list(SCOPES), 1794000000.0)
    assert path.stat().st_mode & 0o777 == 0o600
    assert load_token(state) == REFRESH
    assert json.loads(path.read_text())["granted_at"] == "2026-11-06T21:20:00+00:00"
    assert not (state / oauth.CONSENT_FILE).exists()  # finished
    assert not list(state.glob(".*.part"))


def test_files_that_cannot_be_read_are_reported(tmp_path):
    (tmp_path / oauth.TOKEN_FILE).write_text("{not json")
    with pytest.raises(AuthError, match="cannot read google-token.json"):
        load_token(tmp_path)
    (tmp_path / oauth.TOKEN_FILE).write_text("[]")
    assert load_token(tmp_path) is None
    (tmp_path / oauth.CONSENT_FILE).write_text('{"state": "s"}')
    with pytest.raises(AuthError, match="cannot read google-consent.json"):
        load_consent(tmp_path)

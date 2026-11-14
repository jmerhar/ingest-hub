"""The Google Health API client, against a stand-in Google."""

from __future__ import annotations

import json

import httpx2
import pytest
from jigkit.testing.http import StubServer, serving

from ingest_hub.adapters.google import api as api_module
from ingest_hub.adapters.google.api import ApiError, HealthApi, _reason, _retry_after
from ingest_hub.adapters.google.oauth import Credentials, TokenRefused

from .google_api_fake import CLIENT_ID, CLIENT_SECRET, REFRESH, FakeGoogle

CREDENTIALS = Credentials(CLIENT_ID, CLIENT_SECRET)
EVERYTHING = 'steps.interval.start_time >= "2000-01-01T00:00:00Z" AND x < "2100-01-01T00:00:00Z"'


class Clock:
    def __init__(self) -> None:
        self.now = 1_800_000_000.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)


def client_for(server: StubServer, http: httpx2.AsyncClient, clock: Clock) -> HealthApi:
    return HealthApi(
        http,
        CREDENTIALS,
        REFRESH,
        url=server.url + "/v4",
        token_url=server.url + "/token",
        clock=clock,
        sleep=clock.sleep,
    )


async def test_pages_are_followed_and_the_access_token_reused_until_it_expires():
    fake = FakeGoogle(page=2)
    clock = Clock()
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as http:
        api = client_for(server, http, clock)
        steps = [p async for p in api.data_points("steps", EVERYTHING)]
        assert len(steps) == 3 and api.requests == 2 and fake.tokens_issued == 1
        page_sizes = [r.query["pageSize"] for r in server.requests if "pageSize" in r.query]
        assert page_sizes == ["10000", "10000"]
        clock.now += 3600 - 60  # within two minutes of expiring
        await api.identity()
        assert fake.tokens_issued == 2
        sleep_filter = EVERYTHING.replace("steps.interval.start_time", "sleep.interval.end_time")
        [p async for p in api.data_points("sleep", sleep_filter)]
        assert server.requests[-1].query["pageSize"] == "25"


async def test_an_expired_access_token_is_renewed_once():
    fake = FakeGoogle()
    clock = Clock()
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as http:
        api = client_for(server, http, clock)
        await api.identity()
        fake.tokens_issued += 1  # Google forgets the token it gave
        assert (await api.identity()) == {"healthUserId": "1"}
        fake.tokens_issued += 5
        fake.failures = [(401, "{}")] * 3
        with pytest.raises(ApiError, match="HTTP 401"):
            await api.identity()


async def test_refusals_and_server_errors_are_waited_out_and_retried():
    fake = FakeGoogle(failures=[(429, "{}"), (503, "{}")])
    clock = Clock()
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as http:
        api = client_for(server, http, clock)
        assert await api.identity() == {"healthUserId": "1"}
        assert clock.slept == [api_module.BACKOFF, api_module.BACKOFF * 2]
        fake.failures = [(500, "{}")] * api_module.TRIES
        with pytest.raises(ApiError, match="identity: HTTP 500"):
            await api.identity()


async def test_a_request_google_refuses_for_good_fails_at_once_with_its_reason():
    fake = FakeGoogle(linked=False)
    clock = Clock()
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as http:
        api = client_for(server, http, clock)
        with pytest.raises(ApiError, match="HTTP 400 ACCOUNT_NOT_LINKED The account is not"):
            await api.identity()
        assert clock.slept == []
        fake.refresh_ok = False
        api = client_for(server, http, clock)
        with pytest.raises(TokenRefused):
            await api.identity()


async def test_an_unreachable_api_is_retried_then_reported():
    fake = FakeGoogle()
    clock = Clock()
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as http:
        api = HealthApi(
            http,
            CREDENTIALS,
            REFRESH,
            url="http://127.0.0.1:9/v4",
            token_url=server.url + "/token",
            clock=clock,
            sleep=clock.sleep,
        )
        with pytest.raises(ApiError, match="identity: "):
            await api.identity()
        assert len(clock.slept) == api_module.TRIES - 1


def test_retry_after_and_reasons_are_read_from_the_answer():
    assert _retry_after(httpx2.Response(429, headers={"retry-after": "30"})) == 30.0
    assert _retry_after(httpx2.Response(429, headers={"retry-after": "soon"})) is None
    assert _retry_after(httpx2.Response(429)) is None
    assert _reason(httpx2.Response(500, text="<html>")) == ""
    assert _reason(httpx2.Response(403, text=json.dumps({"error": {"status": "DENIED"}}))) == (
        "DENIED"
    )


async def test_a_retry_after_header_sets_the_wait(monkeypatch):
    calls = 0

    def respond(request):
        nonlocal calls
        if request.path == "/token":
            return 200, json.dumps({"access_token": "a", "expires_in": 3600})
        calls += 1
        return (200, "{}") if calls > 1 else (429, "{}")

    clock = Clock()
    async with serving(StubServer(respond=respond)) as server, httpx2.AsyncClient() as http:
        api = client_for(server, http, clock)
        monkeypatch.setattr(api_module, "_retry_after", lambda response: 1000.0)
        await api.identity()
        assert clock.slept == [api_module.MAX_WAIT]


async def test_entries_of_a_page_that_are_not_data_points_are_skipped():
    def respond(request):
        if request.path == "/token":
            return 200, json.dumps({"access_token": "a", "expires_in": 3600})
        return 200, json.dumps({"dataPoints": [1, {"steps": {}}, None]})

    clock = Clock()
    async with serving(StubServer(respond=respond)) as server, httpx2.AsyncClient() as http:
        api = client_for(server, http, clock)
        assert [p async for p in api.data_points("steps", EVERYTHING)] == [{"steps": {}}]

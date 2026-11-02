"""The Tuya client: signing, the token, paging, rate limits and errors, against a real server."""

from __future__ import annotations

import json

import httpx2
import pytest
from jigkit.testing.http import Received, StubServer, serving

from ingest_hub.adapters.tuya.client import (
    RATE_LIMITED,
    TuyaClient,
    TuyaError,
    canonical,
    sign,
)


def test_canonical_sorts_query_parameters_by_name():
    path = "/v2.0/cloud/thing/dev1/report-logs?start_time=1&codes=a,b&size=100&end_time=2"
    assert canonical(path) == (
        "/v2.0/cloud/thing/dev1/report-logs?codes=a,b&end_time=2&size=100&start_time=1"
    )
    assert canonical("/v1.0/devices/x/status") == "/v1.0/devices/x/status"


def test_sign_matches_the_reference_implementation():
    # Computed with the PHP client that has been calling Tuya in production.
    path = "/v2.0/cloud/thing/dev1/report-logs?codes=a,b&end_time=2&size=100&start_time=1"
    assert sign("client-id", "secret-xyz", "token-abc", "1700000000000", path) == (
        "D8A42A4E3A2B41D8FD031DF9C4EFA3CB14ACF22EAB43694E7001636F1AAB2F5B"
    )


def ok(result: object) -> tuple[int, str]:
    """A successful Tuya answer."""
    return 200, json.dumps({"success": True, "result": result})


def failed(code: int, msg: str = "nope") -> tuple[int, str]:
    """A refusal, which Tuya sends with HTTP 200."""
    return 200, json.dumps({"success": False, "code": code, "msg": msg})


TOKEN = {"access_token": "tok-1", "expire_time": 7200}


class Clock:
    """A settable clock."""

    def __init__(self, now: float = 1_700_000_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


async def no_sleep(_seconds: float) -> None:
    """Skip a pause."""


def router(routes: dict[str, object]):
    """Answer by path: a fixed answer, or a list of answers taken in turn."""

    def respond(request: Received) -> tuple[int, str]:
        answer = routes.get(request.path)
        if answer is None:
            return 404, "not found"
        if isinstance(answer, list):
            return answer.pop(0)
        return answer  # type: ignore[return-value]

    return respond


async def test_requests_are_signed_with_the_token(monkeypatch):
    stub = StubServer(respond=router({"/v1.0/token": ok(TOKEN), "/v1.0/x": ok({"a": 1})}))
    async with serving(stub), httpx2.AsyncClient() as http:
        client = TuyaClient("id", "secret", stub.url, http, clock=Clock())
        assert await client.get("/v1.0/x?b=2&a=1") == {"a": 1}

    token_call, call = stub.requests
    assert "access_token" not in token_call.headers
    assert token_call.query == {"grant_type": "1"}
    assert call.headers["access_token"] == "tok-1"
    assert call.headers["client_id"] == "id"
    assert call.headers["t"] == "1700000000000"
    assert call.headers["sign_method"] == "HMAC-SHA256"
    expected = sign("id", "secret", "tok-1", "1700000000000", "/v1.0/x?a=1&b=2")
    assert call.headers["sign"] == expected


async def test_the_token_is_reused_until_it_nears_expiry():
    tokens = [ok(TOKEN), ok({"access_token": "tok-2", "expire_time": 7200})]
    stub = StubServer(respond=router({"/v1.0/token": tokens, "/v1.0/x": ok(1)}))
    clock = Clock()
    async with serving(stub), httpx2.AsyncClient() as http:
        client = TuyaClient("id", "secret", stub.url, http, clock=clock)
        await client.get("/v1.0/x")
        await client.get("/v1.0/x")
        clock.now += 7200 - 30
        await client.get("/v1.0/x")
    paths = [r.path for r in stub.requests]
    assert paths == ["/v1.0/token", "/v1.0/x", "/v1.0/x", "/v1.0/token", "/v1.0/x"]
    assert stub.requests[-1].headers["access_token"] == "tok-2"


async def test_a_refused_token_is_replaced_once():
    tokens = [ok(TOKEN), ok({"access_token": "tok-2", "expire_time": 7200})]
    answers = [failed(1010, "token invalid"), ok("fine")]
    stub = StubServer(respond=router({"/v1.0/token": tokens, "/v1.0/x": answers}))
    async with serving(stub), httpx2.AsyncClient() as http:
        client = TuyaClient("id", "secret", stub.url, http, clock=Clock())
        assert await client.get("/v1.0/x") == "fine"
    assert stub.requests[-1].headers["access_token"] == "tok-2"


async def test_a_token_refused_twice_raises():
    stub = StubServer(
        respond=router({"/v1.0/token": ok(TOKEN), "/v1.0/x": failed(1011, "token expired")})
    )
    async with serving(stub), httpx2.AsyncClient() as http:
        client = TuyaClient("id", "secret", stub.url, http, clock=Clock())
        with pytest.raises(TuyaError) as raised:
            await client.get("/v1.0/x")
    assert raised.value.code == 1011


async def test_rate_limited_calls_are_retried_after_growing_pauses():
    pauses: list[float] = []

    async def sleep(seconds: float) -> None:
        pauses.append(seconds)

    answers = [failed(RATE_LIMITED), failed(RATE_LIMITED), ok("done")]
    stub = StubServer(respond=router({"/v1.0/token": ok(TOKEN), "/v1.0/x": answers}))
    async with serving(stub), httpx2.AsyncClient() as http:
        client = TuyaClient("id", "secret", stub.url, http, clock=Clock(), sleep=sleep)
        assert await client.get("/v1.0/x") == "done"
    assert pauses == [2.0, 4.0]


async def test_rate_limiting_gives_up_eventually():
    stub = StubServer(
        respond=router({"/v1.0/token": ok(TOKEN), "/v1.0/x": failed(RATE_LIMITED, "too often")})
    )
    async with serving(stub), httpx2.AsyncClient() as http:
        client = TuyaClient("id", "secret", stub.url, http, clock=Clock(), sleep=no_sleep)
        with pytest.raises(TuyaError, match="too often"):
            await client.get("/v1.0/x")
    assert len([r for r in stub.requests if r.path == "/v1.0/x"]) == 6


@pytest.mark.parametrize(
    ("answer", "message"),
    [
        (failed(1106, "permission deny"), "code 1106"),
        ((500, "<html>oops</html>"), "HTTP 500"),
        ((200, json.dumps([1, 2])), "no usable response"),
    ],
)
async def test_refusals_raise_with_the_code_and_route(answer, message):
    stub = StubServer(respond=router({"/v1.0/token": ok(TOKEN), "/v1.0/devices/d1/x": answer}))
    async with serving(stub), httpx2.AsyncClient() as http:
        client = TuyaClient("id", "secret", stub.url, http, clock=Clock())
        with pytest.raises(TuyaError, match=message) as raised:
            await client.get("/v1.0/devices/d1/x?q=1")
    assert "/v1.0/devices/d1/x" in str(raised.value)
    assert "q=1" not in str(raised.value)


async def test_an_unreachable_server_raises():
    async with httpx2.AsyncClient() as http:
        client = TuyaClient("id", "secret", "http://127.0.0.1:9", http, clock=Clock())
        with pytest.raises(TuyaError, match="cannot reach Tuya"):
            await client.get("/v1.0/x")


async def test_devices_are_paged_by_the_last_id():
    first = [
        {"id": f"d{n}", "name": f"Plug {n}", "category": "cz", "isOnline": True} for n in range(20)
    ]
    second = [
        {"id": "d20", "name": "dev", "customName": " Custom ", "category": "cs"},
        {"id": "d21", "name": "", "isOnline": False},
    ]
    stub = StubServer(
        respond=router(
            {"/v1.0/token": ok(TOKEN), "/v2.0/cloud/thing/device": [ok(first), ok(second)]}
        )
    )
    async with serving(stub), httpx2.AsyncClient() as http:
        devices = await TuyaClient("id", "secret", stub.url, http, clock=Clock()).devices()

    assert len(devices) == 22
    assert devices[0].name == "Plug 0" and devices[0].online
    assert devices[20].name == "Custom" and devices[20].category == "cs"
    assert devices[21].category == "unknown" and not devices[21].online
    pages = [r.query for r in stub.requests if r.path == "/v2.0/cloud/thing/device"]
    assert pages == [{"page_size": "20"}, {"page_size": "20", "last_id": "d19"}]


async def test_specification_returns_the_status_codes():
    spec = {"category": "cz", "status": [{"code": "switch_1", "type": "Boolean"}]}
    stub = StubServer(
        respond=router({"/v1.0/token": ok(TOKEN), "/v1.0/devices/d1/specifications": ok(spec)})
    )
    async with serving(stub), httpx2.AsyncClient() as http:
        client = TuyaClient("id", "secret", stub.url, http, clock=Clock())
        assert await client.specification("d1") == spec["status"]


async def test_reports_follow_pages_and_split_codes_into_twenties():
    codes = [f"c{n:02d}" for n in range(25)]
    path = "/v2.0/cloud/thing/d1/report-logs"
    pages = [
        ok(
            {
                "logs": [{"code": "c00", "value": "1", "event_time": 10}],
                "has_more": True,
                "last_row_key": "k/1",
            }
        ),
        ok(
            {
                "logs": [{"code": "c01", "value": "2", "event_time": 20}],
                "has_more": False,
                "last_row_key": "k2",
            }
        ),
        ok({"logs": [{"code": "c24", "value": "3", "event_time": 30}], "has_more": True}),
    ]
    stub = StubServer(respond=router({"/v1.0/token": ok(TOKEN), path: pages}))
    async with serving(stub), httpx2.AsyncClient() as http:
        client = TuyaClient("id", "secret", stub.url, http, clock=Clock())
        reports = await client.reports("d1", codes, 5, 50)

    assert [(r.code, r.value, r.event_time) for r in reports] == [
        ("c00", "1", 10),
        ("c01", "2", 20),
        ("c24", "3", 30),
    ]
    queries = [r.query for r in stub.requests if r.path == path]
    assert queries[0]["codes"] == ",".join(codes[:20])
    assert queries[0]["start_time"] == "5" and queries[0]["end_time"] == "50"
    assert queries[0]["size"] == "100" and "last_row_key" not in queries[0]
    assert queries[1]["last_row_key"] == "k/1"
    assert queries[2]["codes"] == ",".join(codes[20:])

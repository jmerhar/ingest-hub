"""Signing sources in through the web interface: a device login that finishes by itself, and a
consent finished with a pasted answer, each followed by the source starting again at once."""

from __future__ import annotations

import asyncio
import json
import urllib.parse

import httpx2
import pytest
from jigkit.testing.http import StubServer, serving

from ingest_hub import web
from ingest_hub.adapters.google import oauth
from ingest_hub.adapters.home_connect import auth
from ingest_hub.collect import runtime as runtime_module

from .google_api_fake import CODE, FakeGoogle
from .test_google_health import write_auth_config
from .test_home_connect import FakeHomeConnect
from .test_home_connect import write_config as write_home_connect_config


@pytest.fixture(autouse=True)
def long_backoff(monkeypatch):
    """A failed source waits an hour, so only a restart can start it again within a test."""
    monkeypatch.setattr(runtime_module, "FIRST_BACKOFF_SECONDS", 3600.0)


async def status_of(client: httpx2.AsyncClient, name: str) -> dict:
    doc = (await client.get("/api/status")).json()
    return next(s for s in doc["sources"] if s["name"] == name)


async def until(client: httpx2.AsyncClient, name: str, condition) -> dict:
    async with asyncio.timeout(10):
        while not condition(source := await status_of(client, name)):
            await asyncio.sleep(0.02)
    return source


def post_json(client: httpx2.AsyncClient, path: str, body: object = None, **kwargs):
    return client.post(
        path, content=json.dumps(body or {}), headers={"Content-Type": "application/json"}, **kwargs
    )


async def test_a_device_login_started_on_the_page_finishes_by_itself_and_starts_the_source(
    tmp_path,
):
    fake = FakeHomeConnect()
    fake.token_answers = [
        (400, {"error": "authorization_pending"}),
        (200, {"access_token": "a", "refresh_token": "r", "expires_in": 86400}),
    ]
    async with serving(StubServer(respond=fake)) as server:
        settings = write_home_connect_config(tmp_path, server)
        app = web.build_app(settings, only="appliances")
        transport = httpx2.ASGITransport(app=app)
        async with (
            httpx2.AsyncClient(transport=transport, base_url="http://hub") as client,
            app.router.lifespan_context(app),
        ):
            source = await until(client, "appliances", lambda s: s["state"] == "backing-off")
            assert source["sign_in"] == {"signed_in": False, "current": None}
            assert "not signed in to Home Connect yet" in source["last_error"]

            assert (await client.post("/api/sources/appliances/sign-in")).status_code == 415
            assert (await post_json(client, "/api/sources/nowhere/sign-in")).status_code == 404
            nowhere = await post_json(
                client, "/api/sources/nowhere/sign-in/answer", {"answer": "x"}
            )
            assert nowhere.status_code == 404
            started = await post_json(client, "/api/sources/appliances/sign-in")
            assert started.status_code == 201
            assert started.json() == {
                "state": "waiting",
                "at": started.json()["at"],
                "url": "https://example.test/verify?user_code=ABCD-1234",
                "code": "ABCD-1234",
                "instructions": started.json()["instructions"],
                "answer": None,
                "message": None,
            }
            assert "dev-code" not in started.text
            refused = await post_json(
                client, "/api/sources/appliances/sign-in/answer", {"answer": "x"}
            )
            assert refused.status_code == 409

            source = await until(
                client, "appliances", lambda s: s["sign_in"]["current"]["state"] == "done"
            )
            assert source["sign_in"]["signed_in"] is True
            assert source["sign_in"]["current"]["message"] == (
                "Signed in. Appliances: Brand DW1 (Dishwasher)."
            )
            tokens = auth.load_tokens(tmp_path / "state" / "appliances")
            assert tokens is not None and tokens.refresh_token == "r"
            # Restarted at once, despite its hour-long back-off, it reads the appliances.
            source = await until(client, "appliances", lambda s: s["points_emitted"] > 0)
            assert source["state"] == "running" and source["restarts"] == 1


async def test_a_failed_device_login_shows_why_and_a_new_one_replaces_a_waiting_one(tmp_path):
    fake = FakeHomeConnect()
    fake.token_answers = [(400, {"error": "authorization_pending"})] * 200 + [
        (400, {"error": "access_denied"})
    ]
    async with serving(StubServer(respond=fake)) as server:
        settings = write_home_connect_config(tmp_path, server)
        app = web.build_app(settings, only="appliances")
        transport = httpx2.ASGITransport(app=app)
        async with (
            httpx2.AsyncClient(transport=transport, base_url="http://hub") as client,
            app.router.lifespan_context(app),
        ):
            first = await post_json(client, "/api/sources/appliances/sign-in")
            second = await post_json(client, "/api/sources/appliances/sign-in")
            assert first.status_code == second.status_code == 201
            source = await until(
                client, "appliances", lambda s: s["sign_in"]["current"]["state"] == "failed"
            )
            assert "access_denied" in source["sign_in"]["current"]["message"]
            assert source["sign_in"]["signed_in"] is False

            fake.answers["/security/oauth/device_authorization"] = (500, "{}")
            failed = await post_json(client, "/api/sources/appliances/sign-in")
            assert failed.status_code == 502 and "refused to start" in failed.json()["error"]


async def test_a_consent_started_on_the_page_finishes_with_the_pasted_address(tmp_path):
    fake = FakeGoogle()
    async with serving(StubServer(respond=fake)) as server:
        settings = write_auth_config(tmp_path, server)
        app = web.build_app(settings, only="google_alex")
        transport = httpx2.ASGITransport(app=app)
        async with (
            httpx2.AsyncClient(transport=transport, base_url="http://hub") as client,
            app.router.lifespan_context(app),
        ):
            source = await status_of(client, "google_alex")
            assert source["sign_in"]["signed_in"] is False
            answer = "/api/sources/google_alex/sign-in/answer"
            assert (await post_json(client, answer, {"answer": "x"})).status_code == 409

            started = (await post_json(client, "/api/sources/google_alex/sign-in")).json()
            assert started["answer"] == "the address the browser ended up on"
            assert started["url"].startswith("https://accounts.google.com/")
            assert "alex's" in started["instructions"]
            state = urllib.parse.parse_qs(urllib.parse.urlsplit(started["url"]).query)["state"][0]

            assert (await client.post(answer, content="{}")).status_code == 415
            not_json = await client.post(
                answer, content="nonsense", headers={"Content-Type": "application/json"}
            )
            assert not_json.status_code == 400
            assert not_json.json() == {"error": "paste the address the browser ended up on"}
            assert (await post_json(client, answer, {"answer": "  "})).status_code == 400
            wrong = await post_json(
                client, answer, {"answer": "http://127.0.0.1:8765/?state=x&code=y"}
            )
            assert wrong.status_code == 400 and "not from this consent" in wrong.json()["error"]
            # A wrong answer leaves the sign-in waiting for a right one.
            right = f"http://127.0.0.1:8765/?state={state}&code={CODE}"
            done = await post_json(client, answer, {"answer": right})
            assert done.status_code == 200
            assert done.json()["state"] == "done"
            assert done.json()["message"] == "Signed in: the source reads alex's data."
            assert oauth.load_token(tmp_path / "state" / "google_alex") is not None
            assert (await post_json(client, answer, {"answer": right})).status_code == 409
            source = await status_of(client, "google_alex")
            assert source["sign_in"]["signed_in"] is True


def test_sources_that_need_no_sign_in_have_none_in_their_status():
    class Plain:
        async def run(self, context) -> None:
            return None

    class Runtime:
        def source(self, _name):
            return Plain()

    class Assembly:
        runtime = Runtime()

    assert web._sign_in(Assembly(), "plain", None) is None  # type: ignore[arg-type]

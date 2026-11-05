"""The web interface: health, status JSON and the page, with the sources running in its lifespan."""

from __future__ import annotations

import asyncio
import json
import os
import time

import httpx2
import pytest
from jigkit.testing.http import StubServer, serving

from ingest_hub import cli, healthcheck, web
from ingest_hub.settings import Settings

from .conftest import tuya_cloud, write_config
from .test_cli import DEVICES, influx


@pytest.fixture
def environment(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    for name in ("CONFIG_FILE", "WEB_PORT", web.ONLY_ENV):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path / "data"


class Clock:
    def __init__(self) -> None:
        self.offset = 0.0

    def __call__(self) -> float:
        return time.time() + self.offset


async def test_status_health_and_page_while_collecting(environment, secrets):
    reports = {"dev-a": [{"code": "add_ele", "value": "7", "event_time": int(time.time() * 1000)}]}
    async with (
        serving(StubServer(respond=influx)) as db,
        serving(StubServer(respond=tuya_cloud(DEVICES, reports))) as tuya,
    ):
        write_config(environment / "config" / "sources.toml", db.url, tuya.url, secrets)
        clock = Clock()
        app = web.build_app(Settings(revision="abc1234def"), clock=clock)
        transport = httpx2.ASGITransport(app=app)
        async with httpx2.AsyncClient(transport=transport, base_url="http://hub") as client:
            before = await client.get("/api/status")
            assert before.status_code == 503 and before.json() == {"status": "starting"}
            assert (await client.get("/health")).status_code == 503
            async with app.router.lifespan_context(app):
                async with asyncio.timeout(10):
                    while True:
                        doc = (await client.get("/api/status")).json()
                        if doc["sources"][0]["batches_written"]:
                            break
                        await asyncio.sleep(0.02)

                health = await client.get("/health")
                assert health.status_code == 200
                assert health.json() == {
                    "version": doc["version"],
                    "revision": "abc1234def",
                    "status": "ok",
                }
                assert doc["revision"] == "abc1234def" and doc["alerts"] is False
                assert doc["destinations"] == {"home": {"kind": "influx3", "database": "example"}}
                (source,) = doc["sources"]
                assert source["name"] == "plugs" and source["progress"].keys() == {"dev-a"}
                # The device's name labels its stream on the page.
                assert source["labels"] == {"dev-a": "Plug A"}
                raw = (await client.get("/api/status")).text
                for secret in ("synthetic-token", "synthetic-secret", "synthetic-id", db.url):
                    assert secret not in raw

                page = await client.get("/")
                assert page.status_code == 200 and "<title>ingest-hub</title>" in page.text
                assert "connect-src 'self'" in page.headers["content-security-policy"]
                assert page.headers["x-content-type-options"] == "nosniff"
                assert page.headers["cache-control"] == "no-cache"
                assert '<link rel="icon" href="favicon.svg"' in page.text

                icon = await client.get("/favicon.svg")
                assert icon.status_code == 200 and icon.text.startswith("<svg")
                assert icon.headers["content-type"] == "image/svg+xml"
                redirect = await client.get("/favicon.ico")
                assert redirect.status_code == 301
                assert redirect.headers["location"] == "/favicon.svg"

                clock.offset = web.STALE_SECONDS + 60
                stale = await client.get("/health")
                assert stale.status_code == 503 and "not refreshed" in stale.json()["reason"]
            # After shutdown the runtime has stopped.
            gone = await client.get("/health")
            assert gone.status_code == 503 and gone.json()["reason"] == "not running"


def test_create_app_takes_only_from_the_environment(environment, secrets, monkeypatch):
    write_config(environment / "config" / "sources.toml", "http://db", "http://t", secrets)
    captured = {}

    def fake_build(settings, *, only=None, **_):
        captured["only"] = only

    monkeypatch.setattr(web, "build_app", fake_build)
    monkeypatch.setenv(web.ONLY_ENV, "plugs")
    web.create_app()
    assert captured == {"only": "plugs"}
    monkeypatch.setenv(web.ONLY_ENV, "")
    web.create_app()
    assert captured == {"only": None}


def test_run_serves_the_web_app_when_a_port_is_set(environment, secrets, monkeypatch):
    write_config(environment / "config" / "sources.toml", "http://db", "http://t", secrets)
    calls = []
    monkeypatch.setattr(cli, "serve", lambda factory, settings, port: calls.append((factory, port)))
    monkeypatch.setenv("WEB_PORT", "8123")
    assert cli.main(["run", "--only", "plugs"]) == 0
    assert calls == [("ingest_hub.web:create_app", 8123)]
    assert os.environ[web.ONLY_ENV] == "plugs"


def test_run_with_a_port_but_no_configuration_fails_before_serving(environment, monkeypatch):
    monkeypatch.setattr(cli, "serve", lambda *a, **k: pytest.fail("served without a config"))
    monkeypatch.setenv("WEB_PORT", "8123")
    assert cli.main(["run"]) == 2


def test_the_probe_asks_health_when_the_web_interface_is_on(monkeypatch):
    calls = []
    monkeypatch.setattr(healthcheck, "probe", lambda url, expect: calls.append((url, expect)) or 0)
    monkeypatch.setenv("WEB_PORT", "8000")
    assert healthcheck.main() == 0
    assert calls == [("http://127.0.0.1:8000/health", {"status": "ok"})]


EXPORT = [
    {
        "topic": "gateway.hem.message.received",
        "function": "status_message_mpc",
        "timestamp": "2026-11-03T17:00:06.839+00:00",
        "message_value": '{"mpc_device_status": [{"ebus_address": 8, "current_power": 5}]}',
    }
]


async def test_a_file_handed_to_a_source_is_saved_in_its_inbox_and_imported_at_once(
    environment, secrets, monkeypatch
):
    inbox = environment / "inbox"
    extra = f"""
[sources.exports]
kind = "vaillant_export"
destination = "home"

[sources.exports.params]
inbox = "{inbox}"
"""
    async with serving(StubServer(respond=influx)) as db:
        write_config(environment / "config" / "sources.toml", db.url, db.url, secrets, extra)
        app = web.build_app(Settings(), only="exports")
        transport = httpx2.ASGITransport(app=app)
        async with (
            httpx2.AsyncClient(transport=transport, base_url="http://hub") as client,
            app.router.lifespan_context(app),
        ):
            (source,) = (await client.get("/api/status")).json()["sources"]
            assert source["accepts_files"] == [".json"]

            def post(name, body=b"[]", headers=None):
                return client.post(
                    f"/api/sources/{name}/files",
                    content=body,
                    headers={"X-Filename": "export.json"} if headers is None else headers,
                )

            assert (await post("plugs")).status_code == 404  # not running
            assert (await post("nowhere")).status_code == 404
            assert (await post("exports", headers={})).status_code == 400
            txt = await post("exports", headers={"X-Filename": "notes.txt"})
            assert txt.status_code == 415 and ".json" in txt.json()["error"]
            assert (await post("exports", body=b"")).status_code == 400
            monkeypatch.setattr(web, "UPLOAD_MAX_BYTES", 10)
            assert (await post("exports", body=b"[" + b" " * 20 + b"]")).status_code == 413
            monkeypatch.undo()

            body = json.dumps(EXPORT).encode()
            saved = await post("exports", body, {"X-Filename": "../my export.json"})
            assert saved.status_code == 201
            name = saved.json()["saved"]
            assert name.endswith("Z-my-export.json") and saved.json()["bytes"] == len(body)
            # Woken by the upload, the source imports it long before its next scan.
            async with asyncio.timeout(10):
                while not (inbox / "imported" / name).exists():
                    await asyncio.sleep(0.02)
            doc = (await client.get("/api/status")).json()
            assert doc["sources"][0]["points_emitted"] == 1


def test_an_upload_never_replaces_a_file_of_the_same_name(tmp_path):
    assert web._write(tmp_path, "a.json", b"1") == "a.json"
    assert web._write(tmp_path, "a.json", b"2") == "a-2.json"
    assert web._write(tmp_path, "a.json", b"3") == "a-3.json"
    assert (tmp_path / "a.json").read_bytes() == b"1"
    assert not list(tmp_path.glob(".*.part"))


@pytest.mark.parametrize(
    ("filename", "saved"),
    [
        ("euda-data-2026-11-05.json", "20261105T090000Z-euda-data-2026-11-05.json"),
        ("C:\\Users\\x\\Export.JSON", "20261105T090000Z-Export.JSON"),
        ("../../etc/passwd.json", "20261105T090000Z-passwd.json"),
        ("my export (1).json", "20261105T090000Z-my-export-1-.json"),
        ("export.txt", None),
        ("...", None),
    ],
)
def test_saved_names(filename, saved):
    moment = 1793869200.0  # 2026-11-05 09:00:00 UTC
    assert web.saved_name(filename, (".json",), moment) == saved

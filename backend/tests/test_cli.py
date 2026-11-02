"""The command line, end to end against stand-ins for Tuya and InfluxDB."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import time

import pytest
from jigkit.testing.http import Received, StubServer, serving

from ingest_hub import cli
from ingest_hub.healthcheck import check
from ingest_hub.settings import Settings

from .conftest import tuya_cloud, write_config

DEVICES = [{"id": "dev-a", "name": "Plug A", "category": "cz", "isOnline": True}]


def influx(request: Received) -> tuple[int, str]:
    """InfluxDB 3's answers: an empty write is refused as empty, anything else stored."""
    if request.path == "/api/v3/write_lp":
        if not request.body:
            return 400, "empty write"
        return 204, ""
    return 404, ""


@pytest.fixture
def environment(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("CONFIG_FILE", raising=False)
    monkeypatch.chdir(tmp_path)  # no stray .env is read
    return tmp_path / "data"


async def test_run_collects_until_sigterm(environment, secrets):
    # The source fetches the last week by wall-clock time, so the report is an hour old.
    an_hour_ago = int(time.time() * 1000) - 3_600_000
    reports = {"dev-a": [{"code": "add_ele", "value": "70", "event_time": an_hour_ago}]}
    async with (
        serving(StubServer(respond=influx)) as db,
        serving(StubServer(respond=tuya_cloud(DEVICES, reports))) as tuya,
    ):
        write_config(environment / "config" / "sources.toml", db.url, tuya.url, secrets)
        task = asyncio.create_task(cli._run(Settings(), None))
        async with asyncio.timeout(10):
            while not any(r.path == "/api/v3/write_lp" and r.body for r in db.requests):
                await asyncio.sleep(0.01)
        os.kill(os.getpid(), signal.SIGTERM)
        assert await asyncio.wait_for(task, 10) == 0

    (write,) = [r for r in db.requests if r.body]
    assert write.query == {"db": "example", "precision": "millisecond"}
    assert write.headers["authorization"] == "Bearer synthetic-token"
    lines = write.body.splitlines()
    assert lines[0].startswith("tuya_cz,device_id=dev-a,category=cz add_ele=70i,name=")
    assert lines[1].startswith("ingest_progress,source=plugs,stream=dev-a complete_until=")
    status = json.loads((environment / "status.json").read_text())
    assert status["sources"][0]["state"] == "stopped"
    assert check(environment) is None
    cursors = json.loads((environment / "state" / "plugs" / "tuya.json").read_text())["cursors"]
    assert set(cursors) == {"dev-a"}


async def test_check_reports_each_destination(environment, secrets, capsys):
    async with serving(StubServer(respond=influx)) as db:
        write_config(environment / "config" / "sources.toml", db.url, "http://tuya", secrets)
        assert await cli._check(Settings(), None) == 0
    out = capsys.readouterr().out
    assert "destination home: ok" in out
    assert "source plugs (tuya) -> home: configured" in out


async def test_check_fails_when_a_destination_refuses(environment, secrets, capsys):
    async with serving(StubServer(status=403)) as db:
        write_config(environment / "config" / "sources.toml", db.url, "http://tuya", secrets)
        assert await cli._check(Settings(), None) == 1
    assert "may not write" in capsys.readouterr().out


async def test_import_tuya_dump(environment, secrets, tmp_path, capsys):
    dump = tmp_path / "dump"
    dump.mkdir()
    (dump / "a.json").write_text(
        json.dumps(
            {
                "name": "Plug A",
                "device": {"id": "dev-a", "category": "cz", "product": "x"},
                "from_ms": 0,
                "to_ms": 1,
                "reports": [{"event_time": 1000, "code": "cur_voltage", "value": "2301"}],
            }
        )
    )
    async with (
        serving(StubServer(respond=influx)) as db,
        serving(StubServer(respond=tuya_cloud(DEVICES, {}))) as tuya,
    ):
        write_config(environment / "config" / "sources.toml", db.url, tuya.url, secrets)
        assert await cli._import_tuya_dump(Settings(), dump, "plugs") == 0
    (write,) = [r for r in db.requests if r.body]
    assert write.body == 'tuya_cz,device_id=dev-a,category=cz cur_voltage=230.1,name="Plug A" 1000'
    assert "imported 1 point(s) for 1 device(s)" in capsys.readouterr().out


def test_main_reports_configuration_errors(environment, capsys):
    assert cli.main(["check"]) == 2
    assert "no configuration file" in capsys.readouterr().err


def test_main_import_with_a_non_tuya_or_unknown_source(environment, secrets, capsys, tmp_path):
    config = write_config(environment / "config" / "sources.toml", "http://db", "http://t", secrets)
    assert cli.main(["import-tuya-dump", str(tmp_path), "--source", "nope"]) == 2
    assert "no source named 'nope'" in capsys.readouterr().err
    config.write_text(config.read_text().replace('kind = "tuya"', 'kind = "other"'))
    assert cli.main(["import-tuya-dump", str(tmp_path), "--source", "plugs"]) == 2
    assert "not a Tuya source" in capsys.readouterr().err


def test_main_run_and_check_dispatch(environment, monkeypatch):
    calls = []

    async def fake(settings, only):
        calls.append(only)
        return 0

    monkeypatch.setattr(cli, "_run", fake)
    monkeypatch.setattr(cli, "_check", fake)
    assert cli.main(["run", "--only", "plugs"]) == 0
    assert cli.main(["check"]) == 0
    assert calls == ["plugs", None]
    assert (environment / "logs" / "ingest-hub.log").exists()


def test_main_reports_invalid_settings(environment, monkeypatch, capsys):
    monkeypatch.setenv("TZ", "Mars/Olympus")
    assert cli.main(["check"]) == 2
    assert "invalid settings" in capsys.readouterr().err

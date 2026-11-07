"""The AlphaESS cloud source, against a stand-in Open API with synthetic systems."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path

import httpx2
import pytest
from jigkit.sinks import Point
from jigkit.testing.http import Received, StubServer, serving
from pydantic import ValidationError

from ingest_hub.adapters import ADAPTERS
from ingest_hub.adapters.alphaess_cloud import STATE_FILE, AlphaCloudParams, AlphaCloudSource
from ingest_hub.collect import SourceContext

ZONE = "Europe/Amsterdam"
# 7 Nov 2026, 12:00 in Amsterdam.
NOW = dt.datetime(2026, 11, 7, 11, 0, tzinfo=dt.UTC).timestamp()
APP_ID, SECRET = "synthetic-app", "synthetic-secret"


def day_energy(day: str, **values) -> dict:
    zero = {
        "eCharge": 0.0,
        "epv": 0.0,
        "eOutput": 0.0,
        "eInput": 0.0,
        "eGridCharge": 0.0,
        "eDischarge": 0.0,
        "eChargingPile": 0.0,
    }
    return {"sysSn": "SN1", "theDate": day, **zero, **values}


class Api:
    """Answers like AlphaESS's Open API, checking each request's signature."""

    def __init__(self) -> None:
        self.energy: dict[str, dict] = {}
        self.power: dict[str, list] = {}
        self.systems = [{"sysSn": "SN1"}, {"popv": 5}]
        self.answer: tuple[int, str] | None = None
        self.requests: list[Received] = []

    def __call__(self, request: Received) -> tuple[int, str]:
        self.requests.append(request)
        if self.answer:
            return self.answer
        stamp = request.headers["timestamp"]
        expected = hashlib.sha512(f"{APP_ID}{SECRET}{stamp}".encode()).hexdigest()
        if request.headers.get("appid") != APP_ID or request.headers.get("sign") != expected:
            return 200, json.dumps({"code": 6007, "msg": "sign verification error"})
        name = request.path.rsplit("/", 1)[-1]
        if name == "getEssList":
            data = self.systems
        elif name == "getOneDateEnergyBySn":
            data = self.energy.get(
                request.query["queryDate"], day_energy(request.query["queryDate"])
            )
        else:
            data = self.power.get(request.query["queryDate"], [])
        return 200, json.dumps({"code": 200, "msg": "Success", "data": data})


class Harness:
    def __init__(self, tmp_path: Path) -> None:
        self.batches: list[list[Point]] = []
        self.problems: list[str] = []
        self.successes = 0
        self.sleeps: list[float] = []
        self.accept = True
        self.now = NOW
        self.stop_at = 10_000

        async def emit(points):
            self.batches.append(points)
            return self.accept

        async def sleep(seconds):
            self.sleeps.append(seconds)
            if seconds > 60 and len([s for s in self.sleeps if s > 60]) >= self.stop_at:
                raise Stop

        def succeeded():
            self.successes += 1

        self.context = SourceContext(
            "cloud",
            tmp_path,
            emit,
            on_success=succeeded,
            on_problem=self.problems.append,
            clock=lambda: self.now,
            sleep=sleep,
        )


class Stop(Exception):
    pass


async def poll(tmp_path, api: Api, harness: Harness | None = None, **params) -> Harness:
    harness = harness or Harness(tmp_path)
    async with serving(StubServer(respond=api)) as server:
        values = {
            "app_id_file": "/x",
            "app_secret_file": "/y",
            "timezone": ZONE,
            "base_url": server.url + "/api/",
            "history_days": 2,
            **params,
        }
        async with httpx2.AsyncClient() as client:
            source = AlphaCloudSource(
                AlphaCloudParams.model_validate(values), client, APP_ID, SECRET
            )
            await source.poll(harness.context)
    return harness


def rows(harness: Harness, table: str):
    return [p for b in harness.batches for p in b if p.table == table]


async def test_history_is_read_day_by_day_and_empty_days_are_skipped(tmp_path):
    api = Api()
    api.energy["2026-11-06"] = day_energy("2026-11-06", eInput=37.22, eDischarge=3.6, eCharge=0.9)
    api.power["2026-11-06"] = [
        {
            "sysSn": "SN1",
            "uploadTime": "2026-11-06 23:55:19",
            "ppv": 0.0,
            "load": 474.0,
            "cbat": 10.4,
            "feedIn": 0.0,
            "gridCharge": 417.0,
            "pbat": 0.0,
            "pchargingPile": 0,
        },
        {"sysSn": "SN1", "uploadTime": None, "ppv": 1.0},
        {"sysSn": "SN1", "uploadTime": "2026-11-06 23:50:19", "ppv": None},
    ]
    harness = await poll(tmp_path, api)
    days = [r.query.get("queryDate") for r in api.requests if "Energy" in r.path]
    assert days == ["2026-11-05", "2026-11-06", "2026-11-07"]
    (power,) = rows(harness, "alphaess_cloud_power")
    # 23:55:19 in Amsterdam (UTC+1) is 22:55:19 UTC.
    assert power.timestamp == int(dt.datetime(2026, 11, 6, 22, 55, 19, tzinfo=dt.UTC).timestamp())
    assert power.tags == (("sn", "SN1"),)
    assert power.fields == {
        "pv_power": 0.0,
        "load": 474.0,
        "soc": 10.4,
        "grid_import": 417.0,
        "feed_in": 0.0,
        "battery_power": 0.0,
        "charging_pile": 0.0,
    }
    (energy,) = rows(harness, "alphaess_cloud_energy")
    assert energy.timestamp == int(dt.datetime(2026, 11, 5, 23, 0, tzinfo=dt.UTC).timestamp())
    assert energy.fields["grid_import"] == 37.22 and energy.fields["pv"] == 0.0
    # 5 Nov is two days old and complete; 6 Nov and today are read again next time.
    assert json.loads((tmp_path / STATE_FILE).read_text()) == {"SN1": "2026-11-05"}
    assert all(s == 2.0 for s in harness.sleeps)


async def test_later_polls_start_after_the_last_complete_day(tmp_path):
    (tmp_path / STATE_FILE).write_text(json.dumps({"SN1": "2026-11-05"}))
    api = Api()
    await poll(tmp_path, api, systems=["SN1"])
    days = [r.query.get("queryDate") for r in api.requests if "Energy" in r.path]
    assert days == ["2026-11-06", "2026-11-07"]
    assert not any(r.path.endswith("getEssList") for r in api.requests)


async def test_an_unreadable_state_reads_the_history_again(tmp_path, caplog):
    (tmp_path / STATE_FILE).write_text("{oops")
    api = Api()
    await poll(tmp_path, api, history_days=0)
    days = [r.query.get("queryDate") for r in api.requests if "Energy" in r.path]
    assert days == ["2026-11-07"]
    assert "reading the history again" in caplog.text


async def test_an_api_error_is_reported_by_run_and_the_poll_tried_again(tmp_path):
    api = Api()
    api.answer = (200, json.dumps({"code": 6053, "msg": "request too frequent"}))
    harness = Harness(tmp_path)
    harness.stop_at = 2
    async with serving(StubServer(respond=api)) as server:
        params = AlphaCloudParams(
            app_id_file="/x", app_secret_file="/y", timezone=ZONE, base_url=server.url + "/api"
        )
        async with httpx2.AsyncClient() as client:
            source = AlphaCloudSource(params, client, APP_ID, SECRET)
            with pytest.raises(Stop):
                await source.run(harness.context)
    assert harness.problems == ["getEssList: 6053 request too frequent"] * 2
    assert harness.successes == 0 and [s for s in harness.sleeps if s > 60] == [900.0, 900.0]


async def test_a_good_poll_counts_as_success(tmp_path):
    api = Api()
    harness = Harness(tmp_path)
    harness.stop_at = 1
    async with serving(StubServer(respond=api)) as server:
        params = AlphaCloudParams(
            app_id_file="/x",
            app_secret_file="/y",
            timezone=ZONE,
            base_url=server.url + "/api",
            history_days=0,
        )
        async with httpx2.AsyncClient() as client:
            with pytest.raises(Stop):
                await AlphaCloudSource(params, client, APP_ID, SECRET).run(harness.context)
    assert harness.successes == 1 and harness.problems == []


async def test_a_wrong_secret_or_http_failure_is_an_error(tmp_path):
    api = Api()
    harness = Harness(tmp_path)
    async with serving(StubServer(respond=api)) as server:
        params = AlphaCloudParams(
            app_id_file="/x", app_secret_file="/y", base_url=server.url + "/api"
        )
        async with httpx2.AsyncClient() as client:
            source = AlphaCloudSource(params, client, APP_ID, "wrong")
            with pytest.raises(Exception, match="6007 sign verification error"):
                await source.poll(harness.context)
            api.answer = (502, "bad gateway")
            with pytest.raises(Exception, match="HTTP 502"):
                await AlphaCloudSource(params, client, APP_ID, SECRET).poll(harness.context)


async def test_a_refused_batch_is_an_error_and_keeps_the_day(tmp_path):
    api = Api()
    api.energy["2026-11-05"] = day_energy("2026-11-05", epv=1.5)
    harness = Harness(tmp_path)
    harness.accept = False
    with pytest.raises(Exception, match="a batch was refused"):
        await poll(tmp_path, api, harness)
    assert not (tmp_path / STATE_FILE).exists()


def test_params_and_registration(tmp_path):
    (tmp_path / "id").write_text("an-id\n")
    (tmp_path / "secret").write_text("a-secret\n")
    adapter = ADAPTERS["alphaess_cloud"]
    params = adapter.params.model_validate(
        {"app_id_file": str(tmp_path / "id"), "app_secret_file": str(tmp_path / "secret")}
    )
    source = adapter.build(params, None, "cloud")
    assert isinstance(source, AlphaCloudSource) and source._secret == "a-secret"
    with pytest.raises(ValidationError):
        AlphaCloudParams(app_id_file="/x", app_secret_file="/y", timezone="Mars/Olympus")

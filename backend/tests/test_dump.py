"""Importing a saved Tuya report-log dump."""

from __future__ import annotations

import json

import httpx2
import pytest
from jigkit.sinks import Outcome, Point
from jigkit.testing.http import StubServer, serving

from ingest_hub.adapters.tuya import dump as dump_module
from ingest_hub.adapters.tuya.client import TuyaClient
from ingest_hub.adapters.tuya.dump import DumpError, import_dump, read_dump
from ingest_hub.adapters.tuya.source import TuyaParams

from .conftest import tuya_cloud


class Sink:
    """Keeps every batch, or refuses them."""

    def __init__(self, outcome: Outcome = Outcome.OK) -> None:
        self.outcome = outcome
        self.batches: list[list[Point]] = []
        self.last_error = None if outcome is Outcome.OK else "HTTP 400: bad"

    async def write(self, points: list[Point]) -> Outcome:
        self.batches.append(points)
        return self.outcome


def device_file(directory, name, device_id, reports, category="cz"):
    body = {
        "name": name,
        "device": {"id": device_id, "category": category, "product": "p"},
        "from_ms": 0,
        "to_ms": 1,
        "reports": reports,
    }
    (directory / f"{name}.json").write_text(json.dumps(body))


def params(**overrides) -> TuyaParams:
    return TuyaParams.model_validate(
        {"access_id_file": "/x", "access_secret_file": "/y", **overrides}
    )


async def run_import(directory, sink, **overrides):
    async with (
        serving(StubServer(respond=tuya_cloud([], {}))) as tuya,
        httpx2.AsyncClient() as http,
    ):
        client = TuyaClient("id", "secret", tuya.url, http)
        return await import_dump(directory, params(**overrides), client, sink)


async def test_every_device_is_typed_and_written_in_batches(tmp_path, monkeypatch):
    monkeypatch.setattr(dump_module, "BATCH", 2)
    reports = [{"event_time": t, "code": "add_ele", "value": "5"} for t in (1, 2, 3)]
    device_file(tmp_path, "a", "dev-a", reports)
    device_file(tmp_path, "b", "dev-b", [])
    sink = Sink()
    written = await run_import(tmp_path, sink)
    assert written == {"dev-a": 3, "dev-b": 0}
    assert [len(b) for b in sink.batches] == [2, 1]
    assert sink.batches[0][0].fields == {"add_ele": 5, "name": "a"}


async def test_devices_outside_the_sources_list_are_skipped(tmp_path):
    device_file(tmp_path, "a", "dev-a", [{"event_time": 1, "code": "add_ele", "value": "5"}])
    device_file(tmp_path, "b", "dev-b", [{"event_time": 1, "code": "add_ele", "value": "5"}])
    assert await run_import(tmp_path, Sink(), devices=["dev-b"]) == {"dev-b": 1}


async def test_a_failed_write_stops_the_import_naming_the_file(tmp_path):
    device_file(tmp_path, "a", "dev-a", [{"event_time": 1, "code": "add_ele", "value": "5"}])
    with pytest.raises(DumpError, match=r"a\.json: the write failed \(reject\): HTTP 400"):
        await run_import(tmp_path, Sink(Outcome.REJECT))


def test_an_empty_or_malformed_dump(tmp_path):
    with pytest.raises(DumpError, match="no .json files"):
        read_dump(tmp_path)
    (tmp_path / "x.json").write_text(json.dumps({"reports": []}))
    with pytest.raises(DumpError, match="x.json is not a device dump"):
        read_dump(tmp_path)


def test_a_missing_category_and_name_have_defaults(tmp_path):
    (tmp_path / "x.json").write_text(json.dumps({"device": {"id": "d"}}))
    (dump,) = read_dump(tmp_path)
    assert dump.category == "unknown" and dump.name == "" and dump.reports == []

"""Comparing a shadow table with the table it is meant to reproduce."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from jigkit.influx import Influx3Sink
from jigkit.testing.http import Received, StubServer, serving

from ingest_hub import cli
from ingest_hub.compare import compare
from ingest_hub.settings import Settings

from .conftest import write_config

NOW = datetime(2026, 11, 2, 12, 0, 30, tzinfo=UTC)
FLOAT = "Float64"
TAG = "Dictionary(Int32, Utf8)"


def database(schemas: dict, first: str | None, minutes: dict, status: int = 200):
    """A stand-in InfluxDB 3 answering the queries the comparison makes."""

    def respond(request: Received) -> tuple[int, str]:
        if status != 200:
            return status, "boom"
        body = json.loads(request.body)
        sql = body["q"]
        if "information_schema" in sql:
            table = body["params"]["table"]
            rows = [{"column_name": k, "data_type": v} for k, v in schemas.get(table, {}).items()]
        elif "min(time)" in sql:
            rows = [{"first": first}] if first else [{}]
        else:
            table = sql.split('FROM "')[1].split('"')[0]
            rows = minutes.get(table, [])
        return 200, json.dumps(rows)

    return respond


SCHEMA = {"device": TAG, "time": "Timestamp(ns)", "power": FLOAT, "id": FLOAT}


def minute(at: str, n: int, power: float, id_: float = 0.0) -> dict:
    return {"minute": f"2026-11-02T{at}:00", "n": n, "power": power, "id": id_}


async def run(schemas, first, minutes, **kwargs):
    async with serving(StubServer(respond=database(schemas, first, minutes))) as db:
        sink = Influx3Sink(db.url, "example")
        try:
            return await compare(sink, "old_t", "new_t", hours=24, now=NOW, **kwargs), db
        finally:
            await sink.aclose()


async def test_matching_tables_match():
    rows = [minute("11:57", 12, 100.0), minute("11:58", 12, 101.0)]
    shadow = [minute("11:57", 12, 100.4), minute("11:58", 11, 101.0)]
    result, db = await run(
        {"old_t": SCHEMA, "new_t": SCHEMA},
        "2026-11-02T11:56:41.5",
        {"old_t": rows, "new_t": shadow},
    )
    assert result.schema_matches and result.ok(0.01)
    assert (result.rows_old, result.rows_new, result.minutes_both) == (24, 23, 2)
    # The partial first minute and the one in progress are left out.
    assert result.start == datetime(2026, 11, 2, 11, 57, tzinfo=UTC)
    assert result.end == datetime(2026, 11, 2, 11, 59, tzinfo=UTC)
    queries = [json.loads(r.body)["q"] for r in db.requests if "date_bin" in r.body]
    assert "TIMESTAMP '2026-11-02T11:57:00Z'" in queries[0]
    assert 'avg("power") AS "power"' in queries[0]
    power = next(c for c in result.columns if c.name == "power")
    assert power.max_relative == pytest.approx(0.4 / 100.4)
    report = result.report(0.01)
    assert "schema: identical" in report and "verdict: match" in report


async def test_schema_differences_and_gaps_are_reported():
    old = dict(SCHEMA)
    new = {**SCHEMA, "id": "Int64", "extra": FLOAT}
    del new["power"]
    result, _ = await run(
        {"old_t": old, "new_t": new},
        "2026-11-02T10:00:00",
        {
            "old_t": [minute("11:00", 5, 1.0), minute("11:01", 5, 1.0)],
            "new_t": [minute("11:01", 5, 1.0), minute("11:02", 5, 9.0)],
        },
    )
    assert result.only_old == {"power": FLOAT}
    assert result.only_new == {"extra": FLOAT}
    assert result.type_changes == {"id": (FLOAT, "Int64")}
    assert len(result.minutes_only_old) == 1 and len(result.minutes_only_new) == 1
    assert not result.ok(0.01)
    report = result.report(0.01)
    assert "only in old_t: power Float64" in report
    assert "id is Float64 in old_t but Int64 in new_t" in report
    assert "minutes only in new_t: 1 (11:02)" in report
    assert "verdict: DIFFERENT" in report


async def test_a_column_over_tolerance_and_many_missing_minutes():
    old = [minute(f"11:{m:02d}", 1, 10.0) for m in range(30)]
    new = [minute("11:00", 1, 0.0)]
    result, _ = await run(
        {"old_t": SCHEMA, "new_t": SCHEMA}, "2026-11-02T10:00:00", {"old_t": old, "new_t": new}
    )
    report = result.report(0.01)
    assert "<-- over tolerance" in report
    assert "and 19 more" in report
    zero = await run(
        {"old_t": SCHEMA, "new_t": SCHEMA},
        "2026-11-02T10:00:00",
        {"old_t": [minute("11:00", 1, 0.0)], "new_t": [minute("11:00", 1, 0.0, None)]},
    )
    assert zero[0].ok(0.0)


async def test_a_shadow_without_rows_or_without_whole_minutes():
    empty, _ = await run({"old_t": SCHEMA}, None, {})
    assert not empty.ok(0.01) and "has no rows yet" in empty.report(0.01)
    no_rows, _ = await run({"old_t": SCHEMA, "new_t": SCHEMA}, None, {})
    assert no_rows.start is None
    fresh, _ = await run({"old_t": SCHEMA, "new_t": SCHEMA}, "2026-11-02T11:59:10", {})
    assert fresh.start is None and not fresh.ok(0.01)


async def test_compare_rejects_names_that_are_not_identifiers():
    async with serving(
        StubServer(respond=database({"old_t": SCHEMA, "new t": SCHEMA}, "2026-11-02T10:00:00", {}))
    ) as db:
        sink = Influx3Sink(db.url, "example")
        with pytest.raises(ValueError, match="plain identifier"):
            await compare(sink, "old_t", "new t", hours=1, now=NOW)
        await sink.aclose()


@pytest.fixture
def environment(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("CONFIG_FILE", raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path / "data"


async def test_the_command_prints_the_report_and_exits_by_verdict(environment, secrets, capsys):
    rows = [minute("11:57", 1, 1.0)]
    stub = StubServer(
        respond=database(
            {"old_t": SCHEMA, "new_t": SCHEMA},
            "2026-11-02T00:00:00",
            {"old_t": rows, "new_t": rows},
        )
    )
    async with serving(stub) as db:
        write_config(environment / "config" / "sources.toml", db.url, "http://t", secrets)
        code = await cli._compare(Settings(), "home", "old_t", "new_t", 24 * 365, 0.01)
    out = capsys.readouterr().out
    assert "old_t (old) against new_t (new)" in out
    assert code == 0 and "verdict: match" in out


async def test_the_command_refuses_unknown_or_non_influx3_destinations(environment, secrets):
    path = write_config(environment / "config" / "sources.toml", "http://db", "http://t", secrets)
    with pytest.raises(cli.ConfigError, match="no destination named 'nope'"):
        await cli._compare(Settings(), "nope", "a", "b", 1, 0.01)
    path.write_text(path.read_text().replace('kind = "influx3"', 'kind = "influx2"'))
    with pytest.raises(cli.ConfigError, match="not InfluxDB 3"):
        await cli._compare(Settings(), "home", "a", "b", 1, 0.01)


async def test_the_command_reports_a_failing_database(environment, secrets):
    async with serving(StubServer(respond=database({}, None, {}, status=401))) as db:
        write_config(environment / "config" / "sources.toml", db.url, "http://t", secrets)
        with pytest.raises(cli.ConfigError, match="cannot query 'home'"):
            await cli._compare(Settings(), "home", "a", "b", 1, 0.01)


def test_main_dispatches_compare(environment, monkeypatch):
    calls = []

    async def fake(settings, destination, old, new, hours, tolerance):
        calls.append((destination, old, new, hours, tolerance))
        return 1

    monkeypatch.setattr(cli, "_compare", fake)
    assert cli.main(["compare", "--destination", "d", "a", "b", "--hours", "2"]) == 1
    assert calls == [("d", "a", "b", 2.0, 0.01)]

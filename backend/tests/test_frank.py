"""The Frank Energie market-price source, against a stand-in GraphQL server."""

from __future__ import annotations

import json
from datetime import date

import httpx2
import pytest
from jigkit.sinks import Point, Precision
from jigkit.testing.http import Received, StubServer, serving
from pydantic import ValidationError

from ingest_hub.adapters import frank
from ingest_hub.adapters.frank import FrankError, FrankParams, FrankSource
from ingest_hub.collect import SourceContext

# 2026-11-02 12:00 UTC
NOW = 1793620800.0

ROW = {
    "from": "2026-11-01T23:00:00.000Z",
    "till": "2026-11-01T23:15:00.000Z",
    "resolution": "PT15M",
    "marketPrice": 0.25096,
    "marketPriceTax": 0.0527,
    "sourcingMarkupPrice": 0.01815,
    "energyTaxPrice": 0.11085,
    "allInPrice": 0.43266,
}


def graphql(days: dict[str, object]):
    """Answers like Frank: rows for known days, its 'not found' error otherwise."""

    def respond(request: Received) -> tuple[int, str]:
        variables = json.loads(request.body)["variables"]
        answer = days.get(variables["date"])
        if answer is None:
            message = f"No marketprices found for segment ELECTRICITY for {variables['date']}"
            return 200, json.dumps({"data": None, "errors": [{"message": message}]})
        if isinstance(answer, tuple):
            return answer
        return 200, json.dumps({"data": {"marketPrices": {"electricityPrices": answer}}})

    return respond


def context(tmp_path, batches):
    async def emit(points: list[Point]) -> bool:
        batches.append(points)
        return True

    return SourceContext(
        "prices", tmp_path, emit, on_success=lambda: None, on_problem=print, clock=lambda: NOW
    )


async def poll(tmp_path, days, **params):
    batches: list[list[Point]] = []
    stub = StubServer(respond=graphql(days))
    async with serving(stub), httpx2.AsyncClient() as client:
        source = FrankSource(FrankParams(url=stub.url, **params), client)
        await source.poll(context(tmp_path, batches))
    return batches, stub


async def test_today_is_written_and_an_unpublished_tomorrow_is_skipped(tmp_path, caplog):
    caplog.set_level("INFO")
    batches, stub = await poll(tmp_path, {"2026-11-02": [ROW]})
    asked = [json.loads(r.body)["variables"] for r in stub.requests]
    assert asked == [
        {"date": "2026-11-02", "resolution": "PT15M"},
        {"date": "2026-11-03", "resolution": "PT15M"},
    ]
    assert "query MarketPrices" in json.loads(stub.requests[0].body)["query"]
    ((point,),) = batches
    assert point.table == "electricity_price"
    assert point.tags == (("market", "power"), ("resolution", "PT15M"))
    assert point.timestamp == 1793574000 and point.precision is Precision.SECONDS
    # The arithmetic the module documents, in its order of operations.
    assert point.fields == {
        "vat_rate": 0.21,
        "market_price_excl_vat": 0.25096,
        "market_price_incl_vat": 0.25096 + 0.0527,
        "surcharge_excl_vat": 0.01815 / 1.21,
        "surcharge_incl_vat": 0.01815,
        "energy_tax_excl_vat": 0.11085 / 1.21,
        "energy_tax_incl_vat": 0.11085,
        "total_excl_vat": 0.25096 + 0.01815 / 1.21 + 0.11085 / 1.21,
        "total_incl_vat": 0.43266,
    }
    assert all(isinstance(v, float) for v in point.fields.values())
    assert "no prices for 2026-11-03 yet" in caplog.text


async def test_whole_numbers_are_written_as_floats_and_a_missing_resolution_is_filled(tmp_path):
    row = {**ROW, "marketPrice": 0, "resolution": None}
    batches, _ = await poll(tmp_path, {"2026-11-02": [row], "2026-11-03": []})
    (point,) = batches[0]
    assert point.fields["market_price_excl_vat"] == 0.0
    assert isinstance(point.fields["market_price_excl_vat"], float)
    assert dict(point.tags)["resolution"] == "PT15M"
    assert len(batches) == 1  # an empty list for tomorrow writes nothing


async def test_days_before_the_switch_are_asked_for_hourly(tmp_path):
    _, stub = await poll(tmp_path, {}, quarter_hours_from=date(2026, 11, 3))
    resolutions = [json.loads(r.body)["variables"]["resolution"] for r in stub.requests]
    assert resolutions == ["PT60M", "PT15M"]


@pytest.mark.parametrize(
    ("answer", "message"),
    [
        ((200, json.dumps({"errors": [{"message": "Cannot query field"}]})), "Cannot query field"),
        ((502, "<html>bad gateway</html>"), "HTTP 502 without JSON"),
        ((200, "[1]"), "HTTP 200 without JSON"),
    ],
)
async def test_other_failures_raise(tmp_path, answer, message):
    with pytest.raises(FrankError, match=message):
        await poll(tmp_path, {"2026-11-02": answer})


async def test_an_unreachable_api_raises(tmp_path):
    async with httpx2.AsyncClient() as client:
        source = FrankSource(FrankParams(url="http://127.0.0.1:9"), client)
        with pytest.raises(FrankError, match="cannot reach Frank"):
            await source.prices(date(2026, 11, 2))


async def test_run_polls_and_sleeps_for_the_interval(tmp_path):
    stub = StubServer(respond=graphql({}))
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        raise SystemExit

    async with serving(stub), httpx2.AsyncClient() as client:
        ctx = context(tmp_path, [])
        ctx.sleep = sleep
        with pytest.raises(SystemExit):
            await frank.build(FrankParams(url=stub.url, poll_hours=2), client).run(ctx)
    assert sleeps == [7200]


def test_params_are_validated():
    with pytest.raises(ValidationError, match="table"):
        FrankParams(table="1x")
    with pytest.raises(ValidationError, match="vat_rate"):
        FrankParams(vat_rate=1.5)

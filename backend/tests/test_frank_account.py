"""A Frank customer's account data, against a stand-in GraphQL server with a synthetic account."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx2
import pytest
from jigkit.sinks import Point
from jigkit.testing.http import Received, StubServer, serving
from pydantic import ValidationError

from ingest_hub.adapters import frank
from ingest_hub.adapters.frank import FrankParams, FrankSource
from ingest_hub.adapters.frank.account import (
    GIVE_UP_DAYS,
    STATE_FILE,
    FrankAccount,
    FrankAccountError,
)
from ingest_hub.collect import SourceContext

ZONE = dt.timezone(dt.timedelta(hours=1))  # Amsterdam in November
# 8 Nov 2026, 13:00 in Amsterdam.
NOW = dt.datetime(2026, 11, 8, 12, 0, tzinfo=dt.UTC).timestamp()
EMAIL, PASSWORD = "someone@example.org", "synthetic-password"


def hours(day: str, usage: float, count: int | None = None) -> list[dict]:
    """A day's hourly items from Amsterdam midnight: all of its hours (23 to 25), or ``count``."""
    amsterdam = ZoneInfo("Europe/Amsterdam")
    start = dt.datetime.fromisoformat(day).replace(tzinfo=amsterdam).astimezone(dt.UTC)
    end = (dt.datetime.fromisoformat(day) + dt.timedelta(days=1)).replace(tzinfo=amsterdam)
    total = round((end.astimezone(dt.UTC) - start).total_seconds() / 3600)
    return [
        {
            "from": (start + dt.timedelta(hours=h)).isoformat().replace("+00:00", "Z"),
            "till": "",
            "usage": usage,
            "costs": round(usage * 0.3, 4),
        }
        for h in range(total if count is None else count)
    ]


class Frank:
    """Answers like Frank's GraphQL API for one synthetic customer."""

    def __init__(self) -> None:
        self.token = "token-1"
        self.logins = 0
        self.expire_once = False
        self.refuse_login = False
        self.sites = [
            {
                "reference": "SITE1",
                "deliveryStartDate": "2025-01-01",
                "lastMeterReadingDate": "2026-11-07",
            }
        ]
        self.usage: dict[str, dict] = {}
        self.battery_status = {"2026-11-07": "COMPLETE_FINAL", "2026-11-08": "ACTIVE"}
        self.heating_sessions: dict[str, dict] = {}
        self.batteries = [{"id": "bat1", "createdAt": "2026-11-07T13:11:34.503Z"}]
        self.heaters = [{"id": "hvac1", "createdAt": "2026-11-08T08:44:20.543Z"}]
        self.requests: list[dict] = []
        self.unpublished: set[str] = set()

    def __call__(self, request: Received) -> tuple[int, str]:
        body = json.loads(request.body)
        self.requests.append(body)
        query, v = body["query"], body["variables"]
        if "mutation Login" in query:
            self.logins += 1
            if self.refuse_login or v != {"email": EMAIL, "password": PASSWORD}:
                return 200, json.dumps(
                    {"data": None, "errors": [{"message": "user-error:password-invalid"}]}
                )
            self.token = f"token-{self.logins}"
            return 200, json.dumps({"data": {"login": {"authToken": self.token}}})
        if request.headers.get("authorization") != f"Bearer {self.token}" or self.expire_once:
            self.expire_once = False
            return 200, json.dumps(
                {"data": None, "errors": [{"message": "user-error:auth-not-authorised"}]}
            )
        data = self.answer(query, v)
        if "__errors__" in data:
            return 200, json.dumps({"data": None, "errors": data["__errors__"]})
        return 200, json.dumps({"data": data})

    def answer(self, query: str, v: dict) -> dict:
        if "userSites" in query:
            return {"userSites": self.sites}
        if "marketPrices(" in query and "customerMarketPrices" not in query:
            if v["date"] in self.unpublished:
                raise_not_published = {"message": f"No marketprices found for {v['date']}"}
                return {"__errors__": [raise_not_published]}
            step = 15 if v["resolution"] == "PT15M" else 60
            return {
                "marketPrices": {
                    "electricityPrices": [
                        {
                            "from": f"{v['date']}T10:{m:02d}:00.000Z",
                            "resolution": v["resolution"],
                            "marketPrice": round(0.20 + m / 1000, 5),
                            "marketPriceTax": 0.04,
                        }
                        for m in range(0, 60, step)
                    ]
                    # A period in an hour the contract has no price for is left out.
                    + [
                        {
                            "from": f"{v['date']}T11:00:00.000Z",
                            "marketPrice": 1,
                            "marketPriceTax": 0,
                        }
                    ]
                }
            }
        if "customerMarketPrices" in query:
            row = {
                "from": f"{v['date']}T10:00:00.000Z",
                "till": "",
                "resolution": "PT60M",
                "marketPrice": 0.24,
                "marketPriceTax": 0.05,
                "consumptionSourcingMarkupPrice": 0.025,
                "energyTax": 0.11,
                "allInPrice": 0.43,
                "perUnit": "KWH",
            }
            gas = {**row, "perUnit": "M3", "allInPrice": 1.8}
            return {"customerMarketPrices": {"electricityPrices": [row], "gasPrices": [gas]}}
        if "periodUsageAndCosts" in query:
            return {"periodUsageAndCosts": self.usage.get(v["date"], {})}
        if "monthSummary" in query:
            return {
                "monthSummary": {
                    "actualCostsUntilLastMeterReadingDate": 137.53,
                    "expectedCostsUntilLastMeterReadingDate": 123.29,
                    "expectedCosts": 528.38,
                    "lastMeterReadingDate": "2026-11-07",
                    "meterReadingDayCompleteness": 1,
                }
            }
        if "smartBatteries" in query:
            return {"smartBatteries": self.batteries}
        if "smartBatterySessions" in query:
            day = v["s"]
            sessions = [
                {
                    "date": day,
                    "result": 0.55,
                    "cumulativeResult": 0.55,
                    "status": self.battery_status.get(day),
                }
            ]
            return {
                "smartBatterySessions": {
                    "periodEpexResult": 1.5,
                    "periodFrankSlim": 0.2,
                    "periodImbalanceResult": 0,
                    "periodTradingResult": 0,
                    "periodTotalResult": 0,
                    "sessions": sessions if day in self.battery_status else [],
                }
            }
        if "smartBattery(" in query:
            return {
                "smartBattery": {
                    "settings": {
                        "batteryMode": "IMBALANCE_TRADING",
                        "imbalanceTradingStrategy": "STANDARD",
                        "tradingAlgorithm": "T3",
                    }
                },
                "smartBatterySummary": {
                    "lastKnownStateOfCharge": 36,
                    "lastKnownStatus": "DISCHARGE_INTRADAY",
                    "totalResult": 1.7,
                },
            }
        if "smartHvacs" in query:
            return {"smartHvacs": self.heaters}
        if "smartHvacSessions" in query:
            session = self.heating_sessions.get(v["s"])
            return {
                "smartHvacSessions": {
                    "periodResult": 0,
                    "periodSavings": 0,
                    "periodBonus": 0,
                    "sessions": [session] if session else [],
                }
            }
        if "smartHvacSummary" in query:
            return {
                "smartHvacSummary": {
                    "totalResult": 0,
                    "totalSavings": 0,
                    "totalBonus": 0,
                    "operationalStatus": "LEARNING_PHASE",
                    "steeringStatus": "INACTIVE",
                },
                "smartHvacSettings": {"mode": "EPEX"},
            }
        raise AssertionError(f"unexpected query {query}")


class Harness:
    def __init__(self, tmp_path: Path) -> None:
        self.batches: list[list[Point]] = []
        self.problems: list[str] = []
        self.accept = True
        self.now = NOW

        async def emit(points):
            self.batches.append(points)
            return self.accept

        async def sleep(_seconds):
            return None

        self.progress = []
        self.context = SourceContext(
            "frank",
            tmp_path,
            emit,
            on_success=lambda: None,
            on_problem=self.problems.append,
            on_progress=self.progress.append,
            clock=lambda: self.now,
            sleep=sleep,
        )

    def rows(self, table: str) -> list[Point]:
        return [p for b in self.batches for p in b if p.table == table]


async def poll(tmp_path, api: Frank, harness: Harness | None = None, **options) -> Harness:
    harness = harness or Harness(tmp_path)
    async with serving(StubServer(respond=api)) as server, httpx2.AsyncClient() as client:
        values = {
            "prefix": "frank_",
            "timezone": "Europe/Amsterdam",
            "history_days": 2,
            "vat_rate": 0.21,
            **options,
        }
        account = FrankAccount(client, server.url, EMAIL, PASSWORD, **values)
        await account.poll(harness.context)
    return harness


def stamp(day: str) -> int:
    return int(dt.datetime.fromisoformat(day).replace(tzinfo=ZONE).timestamp())


async def test_a_first_poll_reads_everything_the_login_gives(tmp_path):
    api = Frank()
    api.usage = {
        "2026-11-06": {
            "electricity": {"items": hours("2026-11-06", 1.0)},
            "gas": {"items": hours("2026-11-06", 0.1)},
            "feedIn": {"items": []},
        },
        "2026-11-07": {
            "electricity": {"items": hours("2026-11-07", 2.0)},
            "gas": {"items": hours("2026-11-07", 0.0)},
        },
    }
    harness = await poll(tmp_path, api)
    assert api.logins == 1

    prices = harness.rows("frank_price")
    # 6 Nov (history_days back) to tomorrow, electricity and gas each.
    assert sorted(
        {dt.datetime.fromtimestamp(p.timestamp, dt.UTC).date().isoformat() for p in prices}
    ) == ["2026-11-06", "2026-11-07", "2026-11-08", "2026-11-09"]
    electricity = [p for p in prices if dict(p.tags)["carrier"] == "electricity"]
    # Each day's four quarter hours of the 10:00 hour, priced with the contract's hour.
    assert len(electricity) == 4 * 4
    second = electricity[1]
    assert second.tags == (("carrier", "electricity"), ("resolution", "PT15M"), ("unit", "KWH"))
    assert second.timestamp == int(dt.datetime(2026, 11, 6, 10, 15, tzinfo=dt.UTC).timestamp())
    assert second.fields == {
        "vat_rate": 0.21,
        "market_price_excl_vat": 0.215,
        "market_price_incl_vat": 0.215 + 0.04,
        "surcharge_incl_vat": 0.025,
        "energy_tax_incl_vat": 0.11,
        "total_incl_vat": 0.215 + 0.04 + 0.025 + 0.11,
    }
    gas = [p for p in prices if dict(p.tags)["carrier"] == "gas"]
    assert len(gas) == 4 and gas[0].fields["total_incl_vat"] == 1.8
    assert dict(gas[0].tags) == {"carrier": "gas", "resolution": "PT60M", "unit": "M3"}

    usage = harness.rows("frank_usage")
    assert len(usage) == 4 * 24  # two days, electricity and gas; an empty feed-in writes nothing
    first = usage[0]
    assert first.tags == (("carrier", "electricity"),) and first.timestamp == stamp("2026-11-06")
    assert first.fields == {"usage": 1.0, "costs": 0.3}
    # Usage runs to the last meter reading (7 Nov), and both days are complete.
    assert [p.complete_until_ms for p in harness.progress] == [
        stamp("2026-11-07") * 1000,
        stamp("2026-11-08") * 1000,
    ]

    (month,) = harness.rows("frank_month")
    assert month.tags == (("site", "SITE1"),) and month.fields["expected_month_costs"] == 528.38
    assert month.fields["completeness"] == 1.0 and month.fields["last_reading"] == "2026-11-07"

    (battery,) = harness.rows("frank_battery")
    assert battery.tags == (("battery", "bat1"),)
    assert battery.fields == {
        "soc": 36.0,
        "total_result": 1.7,
        "status": "DISCHARGE_INTRADAY",
        "mode": "IMBALANCE_TRADING",
        "strategy": "STANDARD",
        "algorithm": "T3",
    }
    days = harness.rows("frank_battery_day")
    assert [d.timestamp for d in days] == [stamp("2026-11-07"), stamp("2026-11-08")]
    assert days[0].fields == {
        "epex_result": 1.5,
        "frank_slim": 0.2,
        "imbalance_result": 0.0,
        "trading_result": 0.0,
        "total_result": 0.0,
        "result": 0.55,
        "cumulative_result": 0.55,
        "status": "COMPLETE_FINAL",
    }

    (heating,) = harness.rows("frank_heating")
    assert (
        heating.fields["operational_status"] == "LEARNING_PHASE"
        and heating.fields["mode"] == "EPEX"
    )
    (heating_day,) = harness.rows("frank_heating_day")
    # No session yet: only the period's figures.
    assert heating_day.fields == {"period_result": 0.0, "period_savings": 0.0, "period_bonus": 0.0}

    state = json.loads((tmp_path / STATE_FILE).read_text())
    # Prices are final up to yesterday; the battery's 7 Nov is final, 8 Nov still active; the
    # heater's only day is today.
    assert state == {
        "prices": "2026-11-07",
        "usage": "2026-11-07",
        "usage_read": "2026-11-07",
        "battery:bat1": "2026-11-07",
    }


async def test_the_next_poll_starts_after_each_last_complete_day(tmp_path):
    api = Frank()
    (tmp_path / STATE_FILE).write_text(
        json.dumps(
            {
                "prices": "2026-11-07",
                "usage": "2026-11-07",
                "battery:bat1": "2026-11-07",
                "heating:hvac1": "2026-11-07",
            }
        )
    )
    await poll(tmp_path, api)
    asked = [
        (b["query"].split("(")[1].split(":")[0] if "$" in b["query"] else "", b["variables"])
        for b in api.requests
    ]
    price_days = [
        v["date"]
        for q, v in asked
        if "customerMarketPrices" in str(api.requests[asked.index((q, v))]["query"])
    ]
    assert price_days == ["2026-11-08", "2026-11-09"]
    assert not any("periodUsageAndCosts" in b["query"] for b in api.requests)
    battery_days = [
        b["variables"]["s"] for b in api.requests if "smartBatterySessions" in b["query"]
    ]
    assert battery_days == ["2026-11-08"]


async def test_an_incomplete_usage_day_holds_the_cursor_until_it_fills_in(tmp_path):
    api = Frank()
    api.usage = {
        "2026-11-06": {"electricity": {"items": hours("2026-11-06", 1.0, count=20)}},
        "2026-11-07": {"electricity": {"items": hours("2026-11-07", 1.0)}},
    }
    harness = await poll(tmp_path, api)
    assert "usage" not in json.loads((tmp_path / STATE_FILE).read_text())
    assert harness.progress == []
    # Frank fills the day in; the next poll reads it again and moves on past both days.
    api.usage["2026-11-06"] = {"electricity": {"items": hours("2026-11-06", 1.0)}}
    harness = await poll(tmp_path, api)
    assert json.loads((tmp_path / STATE_FILE).read_text())["usage"] == "2026-11-07"
    assert [p.complete_until_ms for p in harness.progress] == [
        stamp("2026-11-07") * 1000,
        stamp("2026-11-08") * 1000,
    ]


async def test_a_day_that_never_fills_in_is_taken_as_it_is_after_a_while(tmp_path):
    api = Frank()
    old = (dt.date(2026, 11, 8) - dt.timedelta(days=GIVE_UP_DAYS + 1)).isoformat()
    (tmp_path / STATE_FILE).write_text(
        json.dumps({"usage": (dt.date.fromisoformat(old) - dt.timedelta(days=1)).isoformat()})
    )
    api.usage = {old: {"electricity": {"items": hours(old, 1.0, count=3)}}}
    api.sites[0]["lastMeterReadingDate"] = old
    harness = await poll(tmp_path, api)
    assert harness.problems == [f"usage for {old} is still incomplete; taking it as it is"]
    assert json.loads((tmp_path / STATE_FILE).read_text())["usage"] == old


async def test_old_battery_days_settle_whatever_their_status(tmp_path):
    api = Frank()
    api.batteries = [{"id": "bat1", "createdAt": "2026-11-03T10:00:00Z"}]
    api.battery_status = {
        "2026-11-03": "ACTIVE",
        "2026-11-04": "ACTIVE",
        "2026-11-05": "ACTIVE",
        "2026-11-06": "ACTIVE",
        "2026-11-07": "COMPLETE_FINAL",
        "2026-11-08": "ACTIVE",
    }
    api.heaters = []
    await poll(tmp_path, api, history_days=0)
    # 3–5 Nov are three or more days old; 6 Nov is neither old nor final, so 7 Nov waits too.
    assert json.loads((tmp_path / STATE_FILE).read_text())["battery:bat1"] == "2026-11-05"


async def test_heating_sessions_are_written_when_frank_reports_them(tmp_path):
    api = Frank()
    api.batteries = []
    api.heating_sessions = {
        "2026-11-08": {
            "date": "2026-11-08",
            "result": 0.4,
            "savings": 0.3,
            "bonus": 0.1,
            "cumulativeResult": 0.4,
            "status": "COMPLETE_FINAL",
        }
    }
    harness = await poll(tmp_path, api, history_days=0)
    (day,) = harness.rows("frank_heating_day")
    assert day.tags == (("device", "hvac1"),)
    assert day.fields["savings"] == 0.3 and day.fields["status"] == "COMPLETE_FINAL"
    assert json.loads((tmp_path / STATE_FILE).read_text())["heating:hvac1"] == "2026-11-08"


async def test_a_refused_token_means_logging_in_again_once(tmp_path):
    api = Frank()
    api.batteries, api.heaters = [], []
    api.expire_once = True
    await poll(tmp_path, api, history_days=0)
    assert api.logins == 2


async def test_a_second_refusal_or_another_error_is_raised(tmp_path):
    api = Frank()
    async with serving(StubServer(respond=api)) as server, httpx2.AsyncClient() as client:
        account = FrankAccount(
            client,
            server.url,
            EMAIL,
            PASSWORD,
            prefix="frank_",
            timezone="Europe/Amsterdam",
            history_days=0,
            vat_rate=0.21,
        )
        harness = Harness(tmp_path)
        api.token = "never-matches"
        api.logins = 0

        def fixed_login(request):  # every login answers with a token Frank then refuses
            body = json.loads(request.body)
            if "mutation Login" in body["query"]:
                api.logins += 1
                return 200, json.dumps({"data": {"login": {"authToken": "stale"}}})
            return api(request)

        server.respond = fixed_login
        with pytest.raises(FrankAccountError, match="auth-not-authorised"):
            await account.query(harness.context, "query { userSites { reference } }")
        assert api.logins == 2


async def test_a_refused_login_is_an_error(tmp_path):
    api = Frank()
    api.refuse_login = True
    with pytest.raises(FrankAccountError, match="login refused: user-error:password-invalid"):
        await poll(tmp_path, api)


@pytest.mark.parametrize(
    ("sites", "site", "message"),
    [([], None, "no sites"), ([{"reference": "OTHER"}], "SITE1", "site 'SITE1' is not among")],
)
async def test_a_missing_site_is_an_error(tmp_path, sites, site, message):
    api = Frank()
    api.sites = sites
    with pytest.raises(FrankAccountError, match=message):
        await poll(tmp_path, api, site=site)


async def test_a_chosen_site_is_used(tmp_path):
    api = Frank()
    api.sites = [
        {"reference": "OTHER"},
        {"reference": "SITE1", "lastMeterReadingDate": "2026-11-07"},
    ]
    api.batteries, api.heaters = [], []
    harness = await poll(tmp_path, api, site="SITE1", history_days=0)
    assert harness.rows("frank_month")[0].tags == (("site", "SITE1"),)


async def test_a_refused_batch_is_an_error(tmp_path):
    api = Frank()
    harness = Harness(tmp_path)
    harness.accept = False
    with pytest.raises(FrankAccountError, match="a batch was refused"):
        await poll(tmp_path, api, harness)


@pytest.mark.parametrize(
    ("answer", "message"),
    [((502, "<html>"), "HTTP 502 without JSON"), ((200, "[1]"), "HTTP 200 without JSON")],
)
async def test_an_answer_without_json_is_an_error(tmp_path, answer, message):
    async with (
        serving(StubServer(respond=lambda _r: answer)) as server,
        httpx2.AsyncClient() as client,
    ):
        account = FrankAccount(
            client,
            server.url,
            EMAIL,
            PASSWORD,
            prefix="frank_",
            timezone="UTC",
            history_days=0,
            vat_rate=0.21,
        )
        with pytest.raises(FrankAccountError, match=message):
            await account.poll(Harness(tmp_path).context)


async def test_an_unreachable_api_is_an_error(tmp_path):
    async with httpx2.AsyncClient() as client:
        account = FrankAccount(
            client,
            "http://127.0.0.1:9",
            EMAIL,
            PASSWORD,
            prefix="frank_",
            timezone="UTC",
            history_days=0,
            vat_rate=0.21,
        )
        with pytest.raises(FrankAccountError, match="cannot reach Frank"):
            await account.poll(Harness(tmp_path).context)


async def test_an_unreadable_state_reads_the_history_again(tmp_path, caplog):
    (tmp_path / STATE_FILE).write_text("{oops")
    api = Frank()
    api.batteries, api.heaters = [], []
    await poll(tmp_path, api, history_days=0)
    assert "reading the history again" in caplog.text
    (tmp_path / STATE_FILE).write_text("[1]")
    await poll(tmp_path, api, history_days=0)


async def test_missing_figures_are_left_out(tmp_path):
    api = Frank()
    api.sites = [{"reference": "SITE1", "lastMeterReadingDate": "not-a-date"}]
    api.batteries = [{"id": "bat1"}]  # no creation date: from today
    api.heaters = []
    api.battery_status = {}
    harness = await poll(tmp_path, api, history_days=0)
    (day,) = harness.rows("frank_battery_day")
    assert "status" not in day.fields and "result" not in day.fields


# ------------------------------------------------------------------ the source's schedule
async def test_the_source_reports_a_failing_account_and_keeps_the_prices(tmp_path, monkeypatch):
    def respond(request: Received) -> tuple[int, str]:
        body = json.loads(request.body)
        if "marketPrices" in body["query"]:
            return 200, json.dumps({"data": {"marketPrices": {"electricityPrices": []}}})
        return 200, json.dumps(
            {"data": None, "errors": [{"message": "user-error:password-invalid"}]}
        )

    harness = Harness(tmp_path)
    successes = []
    harness.context._on_success = lambda: successes.append(harness.now)
    sleeps: list[float] = []

    async def sleep(seconds):
        if seconds < 1:  # the spacing between requests
            return
        harness.now += seconds
        sleeps.append(seconds)
        if len(sleeps) >= 3:
            raise SystemExit

    harness.context.sleep = sleep
    (tmp_path / "user").write_text(EMAIL)
    (tmp_path / "pass").write_text(PASSWORD)
    async with serving(StubServer(respond=respond)) as server, httpx2.AsyncClient() as client:
        params = FrankParams(
            url=server.url,
            username_file=tmp_path / "user",
            password_file=tmp_path / "pass",
            poll_hours=1,
            account_minutes=30,
        )
        source = frank.build(params, client)
        assert isinstance(source, FrankSource)
        with pytest.raises(SystemExit):
            await source.run(harness.context)
    # The first round fails on the account; at 30 minutes the account is due alone, and fails
    # again; at an hour both are due.
    assert sum("account: login refused" in p for p in harness.problems) == 3
    assert successes == []
    assert sleeps[:2] == [1800.0, 1800.0]


def test_the_login_comes_in_pairs_and_the_zone_must_exist(tmp_path):
    with pytest.raises(ValidationError, match="go together"):
        FrankParams(username_file=tmp_path / "user")
    with pytest.raises(ValidationError, match="unknown time zone"):
        FrankParams(timezone="Mars/Olympus")


async def test_rows_without_a_unit_an_empty_month_and_other_days_sessions(tmp_path):
    api = Frank()
    api.batteries, api.heaters = [], []
    plain = api.answer

    def answer(query, v):
        data = plain(query, v)
        if "customerMarketPrices" in query:
            prices = data["customerMarketPrices"]
            for row in prices["electricityPrices"] + prices["gasPrices"]:
                row.pop("perUnit")
        if "monthSummary" in query:
            data = {"monthSummary": None}
        return data

    api.answer = answer
    harness = await poll(tmp_path, api, history_days=0)
    assert harness.rows("frank_price")
    assert all("unit" not in dict(p.tags) for p in harness.rows("frank_price"))
    assert harness.rows("frank_month") == []
    # A period whose sessions are for other days gives no session figures.
    api.heaters = [{"id": "hvac1", "createdAt": "2026-11-08T08:00:00Z"}]
    api.heating_sessions = {"2026-11-08": {"date": "2026-11-07", "result": 9.0}}
    harness = await poll(tmp_path, api, history_days=0, timezone="Europe/Amsterdam")
    (day,) = harness.rows("frank_heating_day")
    assert "result" not in day.fields
    assert FrankParams(timezone="Europe/Amsterdam").timezone == "Europe/Amsterdam"


async def test_a_long_history_is_read_a_share_per_round(tmp_path, monkeypatch):
    monkeypatch.setattr("ingest_hub.adapters.frank.account.ROUND_DAYS", 2)
    api = Frank()
    api.batteries, api.heaters = [], []
    async with serving(StubServer(respond=api)) as server, httpx2.AsyncClient() as client:
        account = FrankAccount(
            client,
            server.url,
            EMAIL,
            PASSWORD,
            prefix="frank_",
            timezone="Europe/Amsterdam",
            history_days=5,
            vat_rate=0.21,
        )
        harness = Harness(tmp_path)
        assert await account.poll(harness.context) is True
        state = json.loads((tmp_path / STATE_FILE).read_text())
        # Two days of each from 3 Nov; usage days without hours stay unread as complete.
        assert state["prices"] == "2026-11-04"
        rounds = 1
        while await account.poll(harness.context):
            rounds += 1
        state = json.loads((tmp_path / STATE_FILE).read_text())
        assert state["prices"] == "2026-11-07" and rounds >= 2
        # Today and tomorrow are read in the last round even when the share is spent.
        days = [
            b["variables"]["date"] for b in api.requests if "customerMarketPrices" in b["query"]
        ]
        assert days[-2:] == ["2026-11-08", "2026-11-09"]


async def test_the_source_comes_back_soon_while_history_remains(tmp_path, monkeypatch):
    async def more(self, context):
        return True

    monkeypatch.setattr(FrankAccount, "poll", more)

    def respond(request: Received) -> tuple[int, str]:
        return 200, json.dumps({"data": {"marketPrices": {"electricityPrices": []}}})

    harness = Harness(tmp_path)
    sleeps: list[float] = []

    async def sleep(seconds):
        harness.now += seconds
        sleeps.append(seconds)
        raise SystemExit

    harness.context.sleep = sleep
    (tmp_path / "user").write_text(EMAIL)
    (tmp_path / "pass").write_text(PASSWORD)
    async with serving(StubServer(respond=respond)) as server, httpx2.AsyncClient() as client:
        params = FrankParams(
            url=server.url, username_file=tmp_path / "user", password_file=tmp_path / "pass"
        )
        with pytest.raises(SystemExit):
            await frank.build(params, client).run(harness.context)
    assert sleeps == [frank.BACKFILL_PAUSE]


async def test_recent_unfilled_days_do_not_hold_up_a_long_backfill(tmp_path, monkeypatch):
    monkeypatch.setattr("ingest_hub.adapters.frank.account.ROUND_DAYS", 3)
    api = Frank()
    api.batteries, api.heaters = [], []
    today = dt.date(2026, 11, 8)
    for back in range(5, 41):  # 40 days back to 5 days back have every hour
        day = (today - dt.timedelta(days=back)).isoformat()
        api.usage[day] = {"electricity": {"items": hours(day, 1.0)}}
    # 4 days back has some hours; the last three days have none yet: an unfilled tail longer
    # than a round's share. The oldest days, beyond
    # GIVE_UP_DAYS, have none either and are taken silently.
    partial = (today - dt.timedelta(days=4)).isoformat()
    api.usage[partial] = {"electricity": {"items": hours(partial, 1.0, count=5)}}
    async with serving(StubServer(respond=api)) as server, httpx2.AsyncClient() as client:
        account = FrankAccount(
            client,
            server.url,
            EMAIL,
            PASSWORD,
            prefix="frank_",
            timezone="Europe/Amsterdam",
            history_days=45,
            vat_rate=0.21,
        )
        harness = Harness(tmp_path)
        rounds = 1
        while await account.poll(harness.context):
            rounds += 1
            assert rounds < 30, "the backfill does not finish"
    state = json.loads((tmp_path / STATE_FILE).read_text())
    assert state["usage"] == (today - dt.timedelta(days=5)).isoformat()
    assert state["usage_read"] == "2026-11-07"
    assert harness.problems == []
    # Once the partial day fills in, the next round moves the cursor past it.
    api.usage[partial] = {"electricity": {"items": hours(partial, 1.0)}}
    async with serving(StubServer(respond=api)) as server, httpx2.AsyncClient() as client:
        account = FrankAccount(
            client,
            server.url,
            EMAIL,
            PASSWORD,
            prefix="frank_",
            timezone="Europe/Amsterdam",
            history_days=45,
            vat_rate=0.21,
        )
        assert await account.poll(Harness(tmp_path).context) is False
    assert json.loads((tmp_path / STATE_FILE).read_text())["usage"] == partial


async def test_the_source_waits_account_minutes_once_history_is_in(tmp_path, monkeypatch):
    async def done(self, context):
        return False

    monkeypatch.setattr(FrankAccount, "poll", done)

    def respond(request: Received) -> tuple[int, str]:
        return 200, json.dumps({"data": {"marketPrices": {"electricityPrices": []}}})

    harness = Harness(tmp_path)
    sleeps: list[float] = []

    async def sleep(seconds):
        sleeps.append(seconds)
        raise SystemExit

    harness.context.sleep = sleep
    (tmp_path / "user").write_text(EMAIL)
    (tmp_path / "pass").write_text(PASSWORD)
    async with serving(StubServer(respond=respond)) as server, httpx2.AsyncClient() as client:
        params = FrankParams(
            url=server.url, username_file=tmp_path / "user", password_file=tmp_path / "pass"
        )
        with pytest.raises(SystemExit):
            await frank.build(params, client).run(harness.context)
    assert sleeps == [3600.0]


async def test_hourly_market_periods_before_the_switch_and_unpublished_days(tmp_path):
    api = Frank()
    api.batteries, api.heaters = [], []
    api.unpublished = {"2026-11-09"}
    harness = await poll(tmp_path, api, history_days=0, quarter_hours_from=dt.date(2026, 11, 9))
    electricity = [
        p for p in harness.rows("frank_price") if dict(p.tags)["carrier"] == "electricity"
    ]
    # 8 Nov is before the switch, so one hourly period; tomorrow is not published yet.
    assert [dict(p.tags)["resolution"] for p in electricity] == ["PT60M"]


async def test_other_price_errors_are_raised(tmp_path):
    api = Frank()
    plain = api.answer

    def answer(query, v):
        if "marketPrices(" in query and "customerMarketPrices" not in query:
            return {"__errors__": [{"message": "something else"}]}
        return plain(query, v)

    api.answer = answer
    with pytest.raises(FrankAccountError, match="something else"):
        await poll(tmp_path, api, history_days=0)

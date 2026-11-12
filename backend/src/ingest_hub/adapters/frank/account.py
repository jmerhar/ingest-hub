"""What a Frank Energie customer's login adds to the public prices.

Every ``account_minutes`` the source logs in (once, and again when Frank refuses the token) and
writes, with ``table_prefix`` (``frank_``) before each table:

- ``price``: the contract's prices, tagged ``carrier`` (``electricity``, ``gas``), ``resolution``
  and ``unit``: ``market_price_excl_vat``, ``market_price_incl_vat``, ``surcharge_incl_vat``,
  ``energy_tax_incl_vat`` and ``total_incl_vat``. Frank gives a contract's prices per hour, with
  the market price averaged over the hour, while it bills electricity per market period; so
  electricity is written per public market period (quarter hours from ``quarter_hours_from``),
  its market price from the public prices and its surcharge and energy tax from the contract's
  hour, and ``total_incl_vat`` is their sum. The contract's surcharge can differ from the public
  one. Gas is written as the contract gives it.
- ``usage``: what Frank bills per hour, tagged ``carrier`` (``electricity``, ``gas``, ``feed_in``):
  ``usage`` (kWh or m³) and ``costs`` (EUR). Frank has a day's figures a day or two late and can
  report a day before every hour is in, so a day counts as complete only once each carrier that
  has hours has all of them, and is read again until it is; one still incomplete after
  ``GIVE_UP_DAYS`` is taken as it is (reported when it has some hours, silently when it has none,
  as before the meter's first readings). The stream ``usage`` in ``ingest_progress`` says how far
  usage is complete.
- ``battery``: each smart-trading battery's state, tagged ``battery``: ``soc``, ``total_result``
  (EUR), ``status``, ``mode``, ``strategy`` and ``algorithm``.
- ``battery_day``: each day's trading, tagged ``battery``, at the day's start: ``result`` and
  ``cumulative_result`` (EUR), ``status``, and the day's ``epex_result``, ``frank_slim``,
  ``imbalance_result``, ``trading_result`` and ``total_result``.
- ``heating``: each smart-heating device's state, tagged ``device``: ``total_result``,
  ``total_savings``, ``total_bonus`` (EUR), ``operational_status``, ``steering_status`` and
  ``mode``.
- ``heating_day``: each day's smart heating, tagged ``device``: ``result``, ``savings``,
  ``bonus`` and ``cumulative_result`` (EUR) and ``status`` when Frank reports a session, and the
  day's ``period_result``, ``period_savings`` and ``period_bonus``.
- ``month``: the month so far, tagged ``site``: ``actual_costs`` and ``expected_costs`` (until the
  last meter reading), ``expected_month_costs``, ``completeness`` and ``last_reading``.

Days are the site's, in ``timezone``. The first time, prices and usage are read from
``history_days`` back (not before the site's delivery start), battery and heating days from the
device's creation, at most ``ROUND_DAYS`` of prices and of usage per round; while history
remains, the source runs the next round a minute later. After that each kind of data is read from
its last complete day; a day is read again until it is complete, so writing a day again writes
the same points.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx2
from jigkit.sinks import Point, Precision

from ingest_hub.collect import Progress, SourceContext

logger = logging.getLogger(__name__)

STATE_FILE = "frank-account.json"
#: Seconds between one request and the next.
SPACING = 0.4
TIMEOUT = 30.0
#: Days after which a day Frank still reports incomplete is taken as it is.
GIVE_UP_DAYS = 7
#: Days after which a battery or heating day counts as final whatever its status says.
SETTLED_DAYS = 3
#: Days of history each round reads at most per kind, so a long backfill is spread over rounds.
ROUND_DAYS = 31

LOGIN = (
    "mutation Login($email: String!, $password: String!) { "
    "login(email: $email, password: $password) { authToken } }"
)
SITES = "query { userSites { reference deliveryStartDate lastMeterReadingDate } }"
PRICE_FIELDS = (
    "from till resolution marketPrice marketPriceTax consumptionSourcingMarkupPrice energyTax "
    "allInPrice perUnit"
)
PRICES = (
    "query($date: String!, $site: String!) { "
    "customerMarketPrices(date: $date, siteReference: $site)"
    f" {{ electricityPrices {{ {PRICE_FIELDS} }} gasPrices {{ {PRICE_FIELDS} }} }} }}"
)
MARKET = (
    "query($date: String!, $resolution: PriceResolution!) { marketPrices(date: $date, "
    "resolution: $resolution) { electricityPrices { from resolution marketPrice "
    "marketPriceTax } } }"
)
#: The message Frank answers with for a day it has no prices for (yet).
NOT_PUBLISHED = "No marketprices found"
CARRIER_ITEMS = "usageTotal costsTotal items { from till usage costs }"
USAGE = (
    "query($date: String!, $site: String!) { periodUsageAndCosts(date: $date, siteReference: $site)"
    f" {{ electricity {{ {CARRIER_ITEMS} }} gas {{ {CARRIER_ITEMS} }}"
    f" feedIn {{ {CARRIER_ITEMS} }} }} }}"
)
MONTH = (
    "query($site: String!) { monthSummary(siteReference: $site) { "
    "actualCostsUntilLastMeterReadingDate expectedCostsUntilLastMeterReadingDate expectedCosts "
    "lastMeterReadingDate meterReadingDayCompleteness } }"
)
BATTERIES = "query { smartBatteries { id createdAt } }"
BATTERY = (
    "query($d: String!) { smartBattery(deviceId: $d) { settings { batteryMode "
    "imbalanceTradingStrategy tradingAlgorithm } } smartBatterySummary(deviceId: $d) { "
    "lastKnownStateOfCharge lastKnownStatus totalResult } }"
)
BATTERY_DAY = (
    "query($s: String!, $e: String!, $d: String!) { smartBatterySessions(startDate: $s, "
    "endDate: $e, deviceId: $d) { periodEpexResult periodFrankSlim periodImbalanceResult "
    "periodTradingResult periodTotalResult sessions { date result cumulativeResult status } } }"
)
HEATERS = "query { smartHvacs { id createdAt } }"
HEATER = (
    "query($d: String!) { smartHvacSummary(deviceId: $d) { totalResult totalSavings totalBonus "
    "operationalStatus steeringStatus } smartHvacSettings(deviceId: $d) { mode } }"
)
HEATING_DAY = (
    "query($s: String!, $e: String!, $d: String!) { smartHvacSessions(startDate: $s, endDate: $e, "
    "deviceId: $d) { periodResult periodSavings periodBonus sessions { date result savings bonus "
    "cumulativeResult status } } }"
)
#: The usage query's carriers, by the names they are stored under.
CARRIERS = {"electricity": "electricity", "gas": "gas", "feedIn": "feed_in"}


class FrankAccountError(Exception):
    """Frank refused a request or the login; the message says why."""


class FrankAccount:
    """One customer's account data, read with their login."""

    def __init__(
        self,
        client: httpx2.AsyncClient,
        url: str,
        email: str,
        password: str,
        *,
        prefix: str,
        timezone: str,
        history_days: int,
        vat_rate: float,
        site: str | None = None,
        quarter_hours_from: dt.date = dt.date(2025, 10, 1),
    ) -> None:
        self._client = client
        self._url = url
        self._email = email
        self._password = password
        self._prefix = prefix
        self._zone = ZoneInfo(timezone)
        self._history_days = history_days
        self._vat_rate = vat_rate
        self._site = site
        self._quarter_hours_from = quarter_hours_from
        self._token: str | None = None

    # ------------------------------------------------------------------ requests
    async def _post(self, context: SourceContext, query: str, variables: dict, token: str | None):
        await context.sleep(SPACING)
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        try:
            response = await self._client.post(
                self._url,
                json={"query": query, "variables": variables},
                headers=headers,
                timeout=TIMEOUT,
            )
        except httpx2.RequestError as error:
            raise FrankAccountError(f"cannot reach Frank: {type(error).__name__}") from error
        try:
            answer = response.json()
        except ValueError:
            answer = None
        if not isinstance(answer, dict):
            raise FrankAccountError(f"Frank answered HTTP {response.status_code} without JSON")
        return answer

    async def _login(self, context: SourceContext) -> str:
        answer = await self._post(
            context, LOGIN, {"email": self._email, "password": self._password}, None
        )
        token = ((answer.get("data") or {}).get("login") or {}).get("authToken")
        if not token:
            raise FrankAccountError(f"login refused: {_messages(answer) or 'no token'}")
        return str(token)

    async def query(self, context: SourceContext, query: str, **variables: str) -> dict[str, Any]:
        """A query's data, logging in first, and once more when Frank refuses the token."""
        for attempt in (1, 2):
            if self._token is None:
                self._token = await self._login(context)
            answer = await self._post(context, query, variables, self._token)
            messages = _messages(answer)
            if not messages:
                return answer.get("data") or {}
            if attempt == 1 and _is_auth(messages):
                self._token = None
                continue
            raise FrankAccountError(messages)
        raise AssertionError("unreachable")  # pragma: no cover

    # ------------------------------------------------------------------ the poll
    async def poll(self, context: SourceContext) -> bool:
        """Read everything the login gives, writing each kind as it comes; True when history
        remains for another round."""
        path = context.state_dir / STATE_FILE
        state = _load(path)
        today = dt.datetime.fromtimestamp(context.clock(), self._zone).date()
        site = await self._site_info(context)
        start = max(
            today - dt.timedelta(days=self._history_days),
            _date(site.get("deliveryStartDate")) or dt.date.min,
        )
        more = await self._prices(context, state, path, site["reference"], start, today)
        more = await self._usage(context, state, path, site, start, today) or more
        await self._month(context, site["reference"])
        for battery in (await self.query(context, BATTERIES)).get("smartBatteries") or []:
            await self._battery(context, state, path, battery, today)
        for device in (await self.query(context, HEATERS)).get("smartHvacs") or []:
            await self._heater(context, state, path, device, today)
        return more

    async def _site_info(self, context: SourceContext) -> dict[str, Any]:
        sites = (await self.query(context, SITES)).get("userSites") or []
        for site in sites:
            if self._site in (None, site.get("reference")):
                return site
        raise FrankAccountError(
            f"site {self._site!r} is not among the account's" if self._site else "no sites"
        )

    async def _emit(self, context: SourceContext, points: list[Point], progress=()) -> None:
        if (points or progress) and not await context.emit(points, progress):
            raise FrankAccountError("a batch was refused")

    def _point(self, table: str, tags: dict[str, str], stamp: int, fields: dict) -> Point:
        clean = {k: v for k, v in fields.items() if v is not None}
        return Point(
            self._prefix + table,
            tuple(sorted(tags.items())),
            stamp,
            clean,
            precision=Precision.SECONDS,
        )

    def _midnight(self, day: dt.date) -> int:
        return int(dt.datetime.combine(day, dt.time(), self._zone).timestamp())

    # ------------------------------------------------------------------ prices
    async def _prices(self, context, state, path, site: str, start: dt.date, today: dt.date):
        """The contract's prices from the day after the last final one up to tomorrow; True when
        the round's share of history ran out first."""
        day = _next(state.get("prices")) or start
        read = 0
        while day <= today + dt.timedelta(days=1):
            if read >= ROUND_DAYS and day < today:
                return True
            read += 1
            contract = await self._published(
                context, PRICES, "customerMarketPrices", date=day.isoformat(), site=site
            )
            resolution = "PT15M" if day >= self._quarter_hours_from else "PT60M"
            market = await self._published(
                context, MARKET, "marketPrices", date=day.isoformat(), resolution=resolution
            )
            hours = {_stamp(r["from"]): r for r in contract.get("electricityPrices") or []}
            points = []
            for row in market.get("electricityPrices") or []:
                start = _stamp(row["from"])
                hour = hours.get(start - start % 3600)
                if hour is not None:
                    points.append(self._electricity_point(row, hour))
            points += [self._price_point("gas", row) for row in contract.get("gasPrices") or []]
            await self._emit(context, points)
            if day < today:
                state["prices"] = day.isoformat()
                _save(path, state)
            day += dt.timedelta(days=1)
        return False

    async def _published(self, context, query: str, key: str, **variables: str) -> dict[str, Any]:
        """A price query's answer, or nothing for a day Frank has not published yet."""
        try:
            return (await self.query(context, query, **variables)).get(key) or {}
        except FrankAccountError as error:
            if NOT_PUBLISHED in str(error):
                return {}
            raise

    def _electricity_point(self, market: dict[str, Any], hour: dict[str, Any]) -> Point:
        """A market period's electricity price with the contract's surcharge and energy tax."""
        spot, tax = float(market["marketPrice"]), float(market["marketPriceTax"])
        surcharge = float(hour["consumptionSourcingMarkupPrice"])
        energy_tax = float(hour["energyTax"])
        tags = {"carrier": "electricity", "resolution": str(market.get("resolution") or "PT60M")}
        if hour.get("perUnit"):
            tags["unit"] = str(hour["perUnit"])
        return self._point(
            "price",
            tags,
            _stamp(market["from"]),
            {
                "vat_rate": float(self._vat_rate),
                "market_price_excl_vat": spot,
                "market_price_incl_vat": spot + tax,
                "surcharge_incl_vat": surcharge,
                "energy_tax_incl_vat": energy_tax,
                "total_incl_vat": spot + tax + surcharge + energy_tax,
            },
        )

    def _price_point(self, carrier: str, row: dict[str, Any]) -> Point:
        spot, tax = float(row["marketPrice"]), float(row["marketPriceTax"])
        tags = {"carrier": carrier, "resolution": str(row.get("resolution") or "PT60M")}
        if row.get("perUnit"):
            tags["unit"] = str(row["perUnit"])
        return self._point(
            "price",
            tags,
            _stamp(row["from"]),
            {
                "vat_rate": float(self._vat_rate),
                "market_price_excl_vat": spot,
                "market_price_incl_vat": spot + tax,
                "surcharge_incl_vat": float(row["consumptionSourcingMarkupPrice"]),
                "energy_tax_incl_vat": float(row["energyTax"]),
                "total_incl_vat": float(row["allInPrice"]),
            },
        )

    # ------------------------------------------------------------------ usage
    async def _usage(self, context, state, path, site, start: dt.date, today: dt.date):
        """Billed usage per hour, up to the site's last meter reading; True when the round's
        share of history ran out first."""
        last = min(_date(site.get("lastMeterReadingDate")) or today, today - dt.timedelta(days=1))
        complete = _date(state.get("usage"))
        # The furthest day read so far: days up to it are only incomplete ones read again, and
        # only days after it count against the round's share of history.
        frontier = _date(state.get("usage_read")) or complete
        day = _next(state.get("usage")) or start
        prefix_open = True
        read = 0
        while day <= last:
            fresh = frontier is None or day > frontier
            if fresh and read >= ROUND_DAYS:
                return True
            read += fresh
            data = await self.query(context, USAGE, date=day.isoformat(), site=site["reference"])
            usage = data.get("periodUsageAndCosts") or {}
            points, whole = self._usage_points(usage, day)
            stale = (today - day).days > GIVE_UP_DAYS
            if not whole and stale and points:
                context.problem(f"usage for {day} is still incomplete; taking it as it is")
            if prefix_open and (whole or stale):
                complete = day
            else:
                prefix_open = False
            progress = []
            if complete is not None and complete.isoformat() != state.get("usage"):
                state["usage"] = complete.isoformat()
                until = self._midnight(complete + dt.timedelta(days=1)) * 1000
                progress = [Progress("usage", until)]
            if fresh:
                frontier = day
                state["usage_read"] = day.isoformat()
            await self._emit(context, points, progress)
            _save(path, state)
            day += dt.timedelta(days=1)
        return False

    def _usage_points(self, usage: dict[str, Any], day: dt.date) -> tuple[list[Point], bool]:
        """A day's hourly points, and whether every carrier with hours has all of them."""
        hours = round((self._midnight(day + dt.timedelta(days=1)) - self._midnight(day)) / 3600)
        points, whole, any_hours = [], True, False
        for key, carrier in CARRIERS.items():
            items = [i for i in (usage.get(key) or {}).get("items") or [] if i.get("from")]
            if not items:
                continue
            any_hours = True
            whole = whole and len({i["from"] for i in items}) >= hours
            for item in items:
                points.append(
                    self._point(
                        "usage",
                        {"carrier": carrier},
                        _stamp(item["from"]),
                        {"usage": _num(item.get("usage")), "costs": _num(item.get("costs"))},
                    )
                )
        return points, whole and any_hours

    # ------------------------------------------------------------------ month
    async def _month(self, context: SourceContext, site: str) -> None:
        summary = (await self.query(context, MONTH, site=site)).get("monthSummary") or {}
        if summary:
            point = self._point(
                "month",
                {"site": site},
                int(context.clock()),
                {
                    "actual_costs": _num(summary.get("actualCostsUntilLastMeterReadingDate")),
                    "expected_costs": _num(summary.get("expectedCostsUntilLastMeterReadingDate")),
                    "expected_month_costs": _num(summary.get("expectedCosts")),
                    "completeness": _num(summary.get("meterReadingDayCompleteness")),
                    "last_reading": _text(summary.get("lastMeterReadingDate")),
                },
            )
            await self._emit(context, [point])

    # ------------------------------------------------------------------ battery
    async def _battery(self, context, state, path, battery: dict[str, Any], today: dt.date):
        device = str(battery["id"])
        data = await self.query(context, BATTERY, d=device)
        settings = (data.get("smartBattery") or {}).get("settings") or {}
        summary = data.get("smartBatterySummary") or {}
        await self._emit(
            context,
            [
                self._point(
                    "battery",
                    {"battery": device},
                    int(context.clock()),
                    {
                        "soc": _num(summary.get("lastKnownStateOfCharge")),
                        "total_result": _num(summary.get("totalResult")),
                        "status": _text(summary.get("lastKnownStatus")),
                        "mode": _text(settings.get("batteryMode")),
                        "strategy": _text(settings.get("imbalanceTradingStrategy")),
                        "algorithm": _text(settings.get("tradingAlgorithm")),
                    },
                )
            ],
        )

        async def read(day: dt.date) -> tuple[list[Point], bool]:
            data = await self.query(
                context, BATTERY_DAY, s=day.isoformat(), e=day.isoformat(), d=device
            )
            period = data.get("smartBatterySessions") or {}
            session = _session(period, day)
            fields = {
                "epex_result": _num(period.get("periodEpexResult")),
                "frank_slim": _num(period.get("periodFrankSlim")),
                "imbalance_result": _num(period.get("periodImbalanceResult")),
                "trading_result": _num(period.get("periodTradingResult")),
                "total_result": _num(period.get("periodTotalResult")),
                "result": _num(session.get("result")),
                "cumulative_result": _num(session.get("cumulativeResult")),
                "status": _text(session.get("status")),
            }
            point = self._point("battery_day", {"battery": device}, self._midnight(day), fields)
            return [point], session.get("status") == "COMPLETE_FINAL"

        await self._days(context, state, path, f"battery:{device}", battery, today, read)

    # ------------------------------------------------------------------ heating
    async def _heater(self, context, state, path, heater: dict[str, Any], today: dt.date):
        device = str(heater["id"])
        data = await self.query(context, HEATER, d=device)
        summary = data.get("smartHvacSummary") or {}
        settings = data.get("smartHvacSettings") or {}
        await self._emit(
            context,
            [
                self._point(
                    "heating",
                    {"device": device},
                    int(context.clock()),
                    {
                        "total_result": _num(summary.get("totalResult")),
                        "total_savings": _num(summary.get("totalSavings")),
                        "total_bonus": _num(summary.get("totalBonus")),
                        "operational_status": _text(summary.get("operationalStatus")),
                        "steering_status": _text(summary.get("steeringStatus")),
                        "mode": _text(settings.get("mode")),
                    },
                )
            ],
        )

        async def read(day: dt.date) -> tuple[list[Point], bool]:
            data = await self.query(
                context, HEATING_DAY, s=day.isoformat(), e=day.isoformat(), d=device
            )
            period = data.get("smartHvacSessions") or {}
            session = _session(period, day)
            fields = {
                "period_result": _num(period.get("periodResult")),
                "period_savings": _num(period.get("periodSavings")),
                "period_bonus": _num(period.get("periodBonus")),
                "result": _num(session.get("result")),
                "savings": _num(session.get("savings")),
                "bonus": _num(session.get("bonus")),
                "cumulative_result": _num(session.get("cumulativeResult")),
                "status": _text(session.get("status")),
            }
            point = self._point("heating_day", {"device": device}, self._midnight(day), fields)
            return [point], session.get("status") == "COMPLETE_FINAL"

        await self._days(context, state, path, f"heating:{device}", heater, today, read)

    async def _days(self, context, state, path, key: str, device: dict, today: dt.date, read):
        """Read a device's days from the day after its last final one (or its creation) to
        today; a day is final when Frank says so or once it is a few days old."""
        created = _date(str(device.get("createdAt") or "")[:10]) or today
        day = _next(state.get(key)) or created
        prefix_open = True
        while day <= today:
            points, final = await read(day)
            await self._emit(context, points)
            if prefix_open and (final or (today - day).days >= SETTLED_DAYS):
                state[key] = day.isoformat()
                _save(path, state)
            else:
                prefix_open = False
            day += dt.timedelta(days=1)


def _messages(answer: dict[str, Any]) -> str:
    return "; ".join(str(e.get("message", e)) for e in answer.get("errors") or [])


def _is_auth(messages: str) -> bool:
    lowered = messages.lower()
    return any(word in lowered for word in ("auth", "token", "expired", "jwt"))


def _session(period: dict[str, Any], day: dt.date) -> dict[str, Any]:
    """The period's session for ``day``, or an empty one."""
    for session in period.get("sessions") or []:
        if session.get("date") == day.isoformat():
            return session
    return {}


def _num(value: Any) -> float | None:
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None


def _text(value: Any) -> str | None:
    return str(value) if value not in (None, "") else None


def _stamp(text: str) -> int:
    return int(dt.datetime.fromisoformat(str(text).replace("Z", "+00:00")).timestamp())


def _date(text: Any) -> dt.date | None:
    try:
        return dt.date.fromisoformat(str(text)[:10]) if text else None
    except ValueError:
        return None


def _next(text: Any) -> dt.date | None:
    day = _date(text)
    return day + dt.timedelta(days=1) if day else None


def _load(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as error:
        logger.error("cannot read %s (%s); reading the history again", path, error)
        return {}
    return data if isinstance(data, dict) else {}


def _save(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)

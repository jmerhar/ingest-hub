"""Frank Energie: its public electricity market prices and, with a customer's login, their
account's data (see ``account.py``).

Every ``poll_hours`` the source asks the public GraphQL ``marketPrices`` query for today and
tomorrow (by UTC date) and writes one point per price period, tagged by market and resolution, at
the period's start. With ``username_file`` and ``password_file`` it also reads the account every
``account_minutes``. A failed round is reported, and the source carries on with the next one.

Each price poll asks for today and tomorrow (by UTC date) and writes one point per price period,
tagged by market and resolution, at the period's start. Frank publishes tomorrow's prices in the
afternoon, so until then tomorrow's query finds nothing; that is expected, not an error.

Frank reports the spot price and its VAT separately and the surcharge and energy tax including
VAT. The excluded-VAT parts are derived with one fixed rate, in this order, so the stored values
are exactly what the same arithmetic gives anywhere:

- ``market_price_incl_vat`` = spot + spot VAT
- ``surcharge_excl_vat`` = surcharge incl. VAT / (1 + rate)
- ``energy_tax_excl_vat`` = energy tax incl. VAT / (1 + rate)
- ``total_excl_vat`` = spot + surcharge excl. VAT + energy tax excl. VAT
- ``total_incl_vat`` = Frank's all-in price
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx2
from jigkit.sinks import Point, Precision
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ingest_hub.adapters.frank.account import FrankAccount, FrankAccountError
from ingest_hub.collect import SourceContext
from ingest_hub.config import read_secret

logger = logging.getLogger(__name__)

URL = "https://graphql.frankenergie.nl/"
QUERY = (
    "query MarketPrices($date:String!,$resolution:PriceResolution!){"
    "marketPrices(date:$date,resolution:$resolution){electricityPrices{"
    "from till resolution marketPrice marketPriceTax sourcingMarkupPrice energyTaxPrice "
    "allInPrice}}}"
)
TIMEOUT_SECONDS = 30.0
#: Seconds between account rounds while history is still being read.
BACKFILL_PAUSE = 60.0
#: The message Frank answers with for a day it has no prices for (yet).
NOT_PUBLISHED = "No marketprices found"


class FrankError(Exception):
    """Frank's API could not be read."""


class FrankParams(BaseModel):
    """A Frank source's ``params`` in the configuration file."""

    model_config = ConfigDict(extra="forbid")

    url: str = URL
    table: str = Field("electricity_price", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    #: The ``market`` tag's value.
    market: str = "power"
    vat_rate: float = Field(0.21, ge=0, lt=1)
    poll_hours: float = Field(6, gt=0)
    #: Days from this date on are asked for at 15-minute resolution, earlier ones hourly.
    quarter_hours_from: date = date(2025, 10, 1)
    #: A customer's login, which adds their account's data; both or neither.
    username_file: Path | None = None
    password_file: Path | None = None
    #: The account's site to read; the first one when not set.
    site: str | None = None
    account_minutes: float = Field(60, gt=0)
    #: Put before each account table's name.
    table_prefix: str = Field("frank_", pattern=r"^([A-Za-z_][A-Za-z0-9_]*)?$")
    #: The site's time zone, which days are counted in.
    timezone: str = "Europe/Amsterdam"
    #: How far back account prices and usage are read the first time.
    history_days: int = Field(60, ge=0)

    @field_validator("timezone")
    @classmethod
    def _known_zone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ValueError(f"unknown time zone {value!r}") from error
        return value

    @model_validator(mode="after")
    def _both_or_neither(self) -> FrankParams:
        if (self.username_file is None) != (self.password_file is None):
            raise ValueError("username_file and password_file go together")
        return self


class FrankSource:
    """Polls Frank's market prices for today and tomorrow."""

    def __init__(
        self,
        params: FrankParams,
        client: httpx2.AsyncClient,
        account: FrankAccount | None = None,
    ) -> None:
        self._params = params
        self._client = client
        self._account = account
        #: When each kind of round is next due.
        self._due: dict[str, float] = {}

    async def run(self, context: SourceContext) -> None:
        """Poll the prices every ``poll_hours`` and the account every ``account_minutes``, for
        ever, reporting a failed round rather than raising it."""
        due = self._due = {"prices": context.clock(), "account": context.clock()}
        if self._account is None:
            due.pop("account")
        every = {
            "prices": self._params.poll_hours * 3600,
            "account": self._params.account_minutes * 60,
        }
        while True:
            ok = True
            for name in list(due):
                if context.clock() >= due[name]:
                    due[name] = context.clock() + every[name]
                    ok = await self._attempt(context, name) and ok
            if ok:
                context.succeeded()
            await context.sleep(max(1.0, min(due.values()) - context.clock()))

    async def _attempt(self, context: SourceContext, name: str) -> bool:
        """One round of prices or account data; a failure is reported, not raised."""
        try:
            if name == "prices":
                await self.poll(context)
            else:
                assert self._account is not None
                if await self._account.poll(context):
                    # History remains: the next round soon, rather than in account_minutes.
                    self._due["account"] = context.clock() + BACKFILL_PAUSE
        except (
            FrankError,
            FrankAccountError,
            httpx2.HTTPError,
            ValueError,
            KeyError,
            TypeError,
        ) as error:
            message = f"{name}: {str(error) or type(error).__name__}"
            context.problem(message)
            logger.warning("Frank %s", message)
            return False
        return True

    async def poll(self, context: SourceContext) -> None:
        """Fetch and deliver today's and tomorrow's prices."""
        today = datetime.fromtimestamp(context.clock(), UTC).date()
        for day in (today, today + timedelta(days=1)):
            points = await self.prices(day)
            if not points:
                logger.info("Frank: no prices for %s yet", day.isoformat())
                continue
            await context.emit(points)

    def resolution(self, day: date) -> str:
        """The resolution a day is asked for at."""
        return "PT15M" if day >= self._params.quarter_hours_from else "PT60M"

    async def prices(self, day: date) -> list[Point]:
        """The points for one day, or none when Frank has not published it."""
        resolution = self.resolution(day)
        body = {"query": QUERY, "variables": {"date": day.isoformat(), "resolution": resolution}}
        try:
            response = await self._client.post(self._params.url, json=body, timeout=TIMEOUT_SECONDS)
        except httpx2.RequestError as error:
            raise FrankError(f"cannot reach Frank: {type(error).__name__}") from error
        try:
            answer = response.json()
        except ValueError:
            answer = None
        if not isinstance(answer, dict):
            raise FrankError(f"Frank answered HTTP {response.status_code} without JSON")
        errors = answer.get("errors") or []
        if errors:
            messages = "; ".join(str(e.get("message", e)) for e in errors)
            if all(NOT_PUBLISHED in str(e.get("message", "")) for e in errors):
                return []
            raise FrankError(f"Frank refused the query for {day.isoformat()}: {messages}")
        rows = ((answer.get("data") or {}).get("marketPrices") or {}).get("electricityPrices")
        return [self.point(row, resolution) for row in rows or []]

    def point(self, row: dict[str, Any], resolution: str) -> Point:
        """One price period as a point."""
        rate = self._params.vat_rate
        spot = float(row["marketPrice"])
        spot_tax = float(row["marketPriceTax"])
        surcharge = float(row["sourcingMarkupPrice"])
        energy_tax = float(row["energyTaxPrice"])
        surcharge_excl = surcharge / (1 + rate)
        energy_tax_excl = energy_tax / (1 + rate)
        start = datetime.fromisoformat(str(row["from"]).replace("Z", "+00:00"))
        return Point(
            self._params.table,
            (("market", self._params.market), ("resolution", row.get("resolution") or resolution)),
            int(start.timestamp()),
            {
                "vat_rate": float(rate),
                "market_price_excl_vat": spot,
                "market_price_incl_vat": spot + spot_tax,
                "surcharge_excl_vat": surcharge_excl,
                "surcharge_incl_vat": surcharge,
                "energy_tax_excl_vat": energy_tax_excl,
                "energy_tax_incl_vat": energy_tax,
                "total_excl_vat": spot + surcharge_excl + energy_tax_excl,
                "total_incl_vat": float(row["allInPrice"]),
            },
            precision=Precision.SECONDS,
        )


def build(params: FrankParams, client: httpx2.AsyncClient, _name: str = "") -> FrankSource:
    """A Frank source from its validated parameters, with the login read now when it has one."""
    account = None
    if params.username_file is not None and params.password_file is not None:
        account = FrankAccount(
            client,
            params.url,
            read_secret(params.username_file),
            read_secret(params.password_file),
            prefix=params.table_prefix,
            timezone=params.timezone,
            history_days=params.history_days,
            vat_rate=params.vat_rate,
            site=params.site,
        )
    return FrankSource(params, client, account)


__all__ = ["FrankError", "FrankParams", "FrankSource", "build"]

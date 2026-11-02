"""Frank Energie's public electricity market prices, from its GraphQL ``marketPrices`` query.

Each poll asks for today and tomorrow (by UTC date) and writes one point per price period,
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
from typing import Any

import httpx2
from jigkit.sinks import Point, Precision
from pydantic import BaseModel, ConfigDict, Field

from ingest_hub.collect import SourceContext

logger = logging.getLogger(__name__)

URL = "https://graphql.frankenergie.nl/"
QUERY = (
    "query MarketPrices($date:String!,$resolution:PriceResolution!){"
    "marketPrices(date:$date,resolution:$resolution){electricityPrices{"
    "from till resolution marketPrice marketPriceTax sourcingMarkupPrice energyTaxPrice "
    "allInPrice}}}"
)
TIMEOUT_SECONDS = 30.0
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


class FrankSource:
    """Polls Frank's market prices for today and tomorrow."""

    def __init__(self, params: FrankParams, client: httpx2.AsyncClient) -> None:
        self._params = params
        self._client = client

    async def run(self, context: SourceContext) -> None:
        """Poll for ever, one poll every ``poll_hours``."""
        while True:
            await self.poll(context)
            context.succeeded()
            await context.sleep(self._params.poll_hours * 3600)

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
    """A Frank source from its validated parameters."""
    return FrankSource(params, client)


__all__ = ["FrankError", "FrankParams", "FrankSource", "build"]

"""AlphaESS systems through AlphaESS's Open API (open.alphaess.com).

A developer account's app ID and secret sign every request; the account sees the systems bound to
it. Every ``poll_minutes`` the source reads, for each system:

- ``<prefix>power``: the five-minute readings of each day, tagged ``sn``: ``pv_power`` (the
  system's own PV inputs), ``load``, ``soc``, ``grid_import``, ``feed_in``, ``battery_power``
  (negative while charging) and ``charging_pile``, in W and %.
- ``<prefix>energy``: each day's totals in kWh, at the start of the day, tagged ``sn``: ``pv``,
  ``charge``, ``discharge``, ``grid_import``, ``grid_export``, ``grid_charge`` and
  ``charging_pile``.

The API gives times and days in the system's own time zone, ``timezone``. The first time, the
source reads back ``history_days``, skipping days with no data; after that, from the last day it
completed. Today and yesterday are read again on every poll, since a day's last readings can
arrive after midnight, and a day counts as complete once it is two days old; writing a day again
writes the same points. Requests are spaced a moment apart.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx2
from jigkit.sinks import FieldValue, Point, Precision
from pydantic import BaseModel, ConfigDict, Field, field_validator

from ingest_hub.collect import SourceContext
from ingest_hub.config import read_secret

logger = logging.getLogger(__name__)

BASE_URL = "https://openapi.alphaess.com/api"
STATE_FILE = "alphaess-cloud.json"
#: Seconds between one request and the next.
SPACING = 2.0
TIMEOUT = 30.0
POWER_FIELDS = {
    "ppv": "pv_power",
    "load": "load",
    "cbat": "soc",
    "gridCharge": "grid_import",
    "feedIn": "feed_in",
    "pbat": "battery_power",
    "pchargingPile": "charging_pile",
}
ENERGY_FIELDS = {
    "epv": "pv",
    "eCharge": "charge",
    "eDischarge": "discharge",
    "eInput": "grid_import",
    "eOutput": "grid_export",
    "eGridCharge": "grid_charge",
    "eChargingPile": "charging_pile",
}


class AlphaCloudError(Exception):
    """The API refused a request or answered with something unreadable."""


class AlphaCloudParams(BaseModel):
    """An AlphaESS cloud source's ``params`` in the configuration file."""

    model_config = ConfigDict(extra="forbid")

    app_id_file: Path
    app_secret_file: Path
    #: Systems to read; empty reads every system the account sees.
    systems: list[str] = []
    #: The systems' time zone, in which the API gives times and days.
    timezone: str = "UTC"
    base_url: str = BASE_URL
    table_prefix: str = Field("alphaess_cloud_", pattern=r"^([A-Za-z_][A-Za-z0-9_]*)?$")
    poll_minutes: float = Field(15, gt=0)
    history_days: int = Field(60, ge=0)

    @field_validator("timezone")
    @classmethod
    def _known_zone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ValueError(f"unknown time zone {value!r}") from error
        return value


class AlphaCloudSource:
    """Reads one developer account's systems."""

    def __init__(
        self, params: AlphaCloudParams, client: httpx2.AsyncClient, app_id: str, secret: str
    ) -> None:
        self._params = params
        self._client = client
        self._app_id = app_id
        self._secret = secret
        self._zone = ZoneInfo(params.timezone)
        self._base = params.base_url.rstrip("/")
        self._prefix = params.table_prefix

    async def run(self, context: SourceContext) -> None:
        """Poll every ``poll_minutes`` for ever, reporting a failed poll rather than raising."""
        while True:
            try:
                await self.poll(context)
            except (AlphaCloudError, httpx2.HTTPError, ValueError, KeyError, TypeError) as error:
                context.problem(str(error))
                logger.warning("AlphaESS cloud: %s", error)
            else:
                context.succeeded()
            await context.sleep(self._params.poll_minutes * 60)

    async def poll(self, context: SourceContext) -> None:
        """Read every day from each system's last completed one up to today."""
        path = context.state_dir / STATE_FILE
        done = _load(path)
        today = dt.datetime.fromtimestamp(context.clock(), self._zone).date()
        systems = self._params.systems or await self._systems(context)
        for sn in systems:
            last = done.get(sn)
            first = (
                dt.date.fromisoformat(last) + dt.timedelta(days=1)
                if last
                else today - dt.timedelta(days=self._params.history_days)
            )
            day = first
            while day <= today:
                await self._day(context, sn, day)
                if day < today - dt.timedelta(days=1):
                    done[sn] = day.isoformat()
                    _save(path, done)
                day += dt.timedelta(days=1)

    async def _systems(self, context: SourceContext) -> list[str]:
        """The serial numbers of the systems the account sees."""
        data = await self._call(context, "getEssList")
        return [str(s["sysSn"]) for s in data or [] if s.get("sysSn")]

    async def _day(self, context: SourceContext, sn: str, day: dt.date) -> None:
        """Write one day's energy totals and five-minute readings; a day with neither is
        skipped."""
        energy = await self._call(
            context, "getOneDateEnergyBySn", sysSn=sn, queryDate=day.isoformat()
        )
        power = await self._call(context, "getOneDayPowerBySn", sysSn=sn, queryDate=day.isoformat())
        points = [p for p in (self._power_point(sn, row) for row in power or []) if p]
        totals = _fields(energy or {}, ENERGY_FIELDS)
        if any(totals.values()):
            midnight = dt.datetime.combine(day, dt.time(), self._zone)
            points.append(self._point("energy", sn, int(midnight.timestamp()), totals))
        if points and not await context.emit(points):
            raise AlphaCloudError(f"{sn} {day}: a batch was refused")

    def _power_point(self, sn: str, row: dict[str, Any]) -> Point | None:
        """One five-minute reading, stamped by its upload time in the system's zone."""
        stamp = row.get("uploadTime")
        fields = _fields(row, POWER_FIELDS)
        if not stamp or not fields:
            return None
        moment = dt.datetime.fromisoformat(str(stamp)).replace(tzinfo=self._zone)
        return self._point("power", sn, int(moment.timestamp()), fields)

    def _point(self, table: str, sn: str, stamp: int, fields: dict[str, FieldValue]) -> Point:
        return Point(
            self._prefix + table, (("sn", sn),), stamp, fields, precision=Precision.SECONDS
        )

    async def _call(self, context: SourceContext, path: str, **query: str) -> Any:
        """A signed GET; the answer's ``data``, or AlphaCloudError when it is not a success."""
        await context.sleep(SPACING)
        stamp = str(int(context.clock()))
        sign = hashlib.sha512(f"{self._app_id}{self._secret}{stamp}".encode()).hexdigest()
        response = await self._client.get(
            f"{self._base}/{path}",
            params=query or None,
            headers={"appId": self._app_id, "timeStamp": stamp, "sign": sign},
            timeout=TIMEOUT,
        )
        if response.status_code != 200:
            raise AlphaCloudError(f"{path} answered HTTP {response.status_code}")
        body = response.json()
        if body.get("code") != 200:
            raise AlphaCloudError(f"{path}: {body.get('code')} {body.get('msg')}")
        return body.get("data")


def _fields(row: dict[str, Any], names: dict[str, str]) -> dict[str, FieldValue]:
    """The numeric values among ``names``, as floats under their stored names."""
    return {
        stored: float(row[key])
        for key, stored in names.items()
        if isinstance(row.get(key), int | float) and not isinstance(row.get(key), bool)
    }


def _load(path: Path) -> dict[str, str]:
    """The last completed day per system, or none when there is none or it cannot be read."""
    try:
        return {str(k): str(v) for k, v in json.loads(path.read_text(encoding="utf-8")).items()}
    except FileNotFoundError:
        return {}
    except (OSError, ValueError, AttributeError) as error:
        logger.error("cannot read %s (%s); reading the history again", path, error)
        return {}


def _save(path: Path, done: dict[str, str]) -> None:
    """Keep the last completed day per system, written atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(done, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def build(
    params: AlphaCloudParams, client: httpx2.AsyncClient, _name: str = ""
) -> AlphaCloudSource:
    """An AlphaESS cloud source from its validated parameters, with its credentials read now."""
    return AlphaCloudSource(
        params, client, read_secret(params.app_id_file), read_secret(params.app_secret_file)
    )


__all__ = ["AlphaCloudParams", "AlphaCloudSource", "build"]

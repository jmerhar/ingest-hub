"""The Tuya source: every device's report log, polled into one table per device category.

Tuya keeps about a week of report logs, so the source polls continuously and keeps a cursor per
device in its state directory. The cursor is also the device's published progress
(``complete_until``): every report with an earlier event time has been fetched and delivered.

Each poll fetches a device's log from ``cursor - overlap`` to now, and once a day from
``cursor - rescan`` instead, so reports Tuya surfaces late are still stored; re-fetched reports
overwrite themselves. The cursor then moves to ``poll start - lag``, never backwards, and only
once the batch is safe (written or spooled). A device that reported nothing still advances: an
empty, fully fetched window means there was nothing to report.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ingest_hub.adapters.tuya.client import HOSTS, Device, TuyaClient, TuyaError
from ingest_hub.adapters.tuya.values import CodeSpec, build_points, parse_specification
from ingest_hub.collect import Progress, SourceContext

logger = logging.getLogger(__name__)

MINUTE_MS = 60_000
HOUR_MS = 60 * MINUTE_MS
DAY_MS = 24 * HOUR_MS
STATE_FILE = "tuya.json"


class TuyaParams(BaseModel):
    """A Tuya source's ``params`` in the configuration file."""

    model_config = ConfigDict(extra="forbid")

    #: The cloud project's data centre.
    region: str = "eu"
    #: Overrides the region's host; for tests and proxies.
    base_url: str | None = None
    access_id_file: Path
    access_secret_file: Path
    #: Tables are named this prefix plus the device category, such as ``tuya_cz`` for plugs.
    table_prefix: str = "tuya_"
    #: Only these device IDs; all devices when unset.
    devices: list[str] | None = None
    #: Integer codes stored as Tuya reports them, without the specification's scale.
    unscaled_codes: list[str] = Field(default_factory=lambda: ["add_ele"])
    poll_minutes: float = Field(15, ge=1)
    #: How far behind the poll's start a device's progress stays.
    lag_minutes: float = Field(60, ge=0)
    #: How far before the cursor each poll starts fetching again.
    overlap_minutes: float = Field(120, ge=0)
    #: How far before the cursor the daily rescan starts, and how often it runs.
    rescan_hours: float = Field(48, ge=0)
    rescan_every_hours: float = Field(24, gt=0)
    #: How far back a device without a cursor is fetched; Tuya keeps about a week.
    retention_days: float = Field(7, gt=0)
    #: How often the device list and specifications are fetched again.
    discover_hours: float = Field(6, gt=0)

    @field_validator("region")
    @classmethod
    def _known_region(cls, value: str) -> str:
        """Reject a data centre the client does not know."""
        if value not in HOSTS:
            raise ValueError(f"unknown region {value!r}; one of {', '.join(sorted(HOSTS))}")
        return value

    @field_validator("table_prefix")
    @classmethod
    def _table_prefix(cls, value: str) -> str:
        """The prefix must start a valid table name."""
        if not value or not value[0].isalpha() or not value.replace("_", "").isalnum():
            raise ValueError("must start with a letter and hold only letters, digits and '_'")
        return value

    @model_validator(mode="after")
    def _overlap_covers_lag(self) -> TuyaParams:
        """Each poll must re-fetch at least the lag, or reports inside it could be missed."""
        if self.overlap_minutes < self.lag_minutes:
            raise ValueError("overlap_minutes must be at least lag_minutes")
        return self

    @property
    def url(self) -> str:
        """The OpenAPI host to call."""
        return self.base_url or HOSTS[self.region]


@dataclass
class _Known:
    """A device and what its specification says."""

    device: Device
    specs: dict[str, CodeSpec]


@dataclass
class _State:
    """What survives a restart: each device's cursor and when the last rescan ran."""

    cursors: dict[str, int] = field(default_factory=dict)
    last_rescan_ms: int = 0

    @classmethod
    def load(cls, path: Path) -> _State:
        """The saved state, or a fresh one when there is none or it cannot be read."""
        try:
            data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
            return cls(
                {str(k): int(v) for k, v in dict(data.get("cursors") or {}).items()},
                int(data.get("last_rescan_ms") or 0),
            )
        except FileNotFoundError:
            return cls()
        except (OSError, ValueError, TypeError, AttributeError) as error:
            logger.error("cannot read %s (%s); starting without cursors", path, error)
            return cls()

    def save(self, path: Path) -> None:
        """Write the state atomically."""
        temporary = path.with_name(f".{path.name}.tmp")
        body = {"cursors": self.cursors, "last_rescan_ms": self.last_rescan_ms}
        temporary.write_text(json.dumps(body, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(temporary, path)


class TuyaSource:
    """Polls a Tuya cloud project's devices."""

    def __init__(self, params: TuyaParams, client: TuyaClient) -> None:
        self._params = params
        self._client = client
        self._unscaled = frozenset(params.unscaled_codes)
        self._known: list[_Known] = []
        self._discovered_ms: int | None = None

    async def run(self, context: SourceContext) -> None:
        """Poll for ever, one poll every ``poll_minutes``."""
        path = context.state_dir / STATE_FILE
        state = _State.load(path)
        context.restore_progress([Progress(d, c) for d, c in state.cursors.items()])
        while True:
            await self.poll(context, state, path)
            context.succeeded()
            await context.sleep(self._params.poll_minutes * 60)

    async def poll(self, context: SourceContext, state: _State, path: Path) -> None:
        """Fetch every device's new reports once.

        A device that fails is reported and skipped, so one broken device does not hold back the
        rest; the poll fails only when every device does.
        """
        now = context.now_ms()
        if self._discovered_ms is None or now - self._discovered_ms >= (
            self._params.discover_hours * HOUR_MS
        ):
            await self.discover()
            self._discovered_ms = now
        rescan = now - state.last_rescan_ms >= self._params.rescan_every_hours * HOUR_MS
        failures = []
        for known in self._known:
            try:
                await self._poll_device(context, state, path, known, rescan)
            except TuyaError as error:
                failures.append(known.device.id)
                context.problem(f"device {known.device.id}: {error}")
                logger.error("Tuya device %s: %s", known.device.id, error)
        if self._known and len(failures) == len(self._known):
            raise TuyaError(f"every device failed ({len(failures)})")
        if rescan and not failures:
            state.last_rescan_ms = now
            state.save(path)

    async def discover(self) -> None:
        """Fetch the device list and each device's specification.

        A device whose specification cannot be fetched is kept without one, so it fails on its
        own in each poll (and is retried at the next discovery) instead of failing every device.
        """
        wanted = set(self._params.devices) if self._params.devices else None
        devices = [d for d in await self._client.devices() if wanted is None or d.id in wanted]
        known = []
        for device in devices:
            try:
                specs = parse_specification(await self._client.specification(device.id))
            except TuyaError as error:
                logger.error("Tuya device %s: no specification: %s", device.id, error)
                specs = {}
            known.append(_Known(device, specs))
        self._known = known
        logger.info("Tuya: %d device(s)", len(known))

    async def _poll_device(
        self,
        context: SourceContext,
        state: _State,
        path: Path,
        known: _Known,
        rescan: bool,
    ) -> None:
        """Fetch one device's window, deliver it with its progress, and move its cursor."""
        device, params = known.device, self._params
        if not known.specs:
            raise TuyaError("its specification lists no status codes")
        start_poll = context.now_ms()
        oldest = start_poll - int(params.retention_days * DAY_MS)
        cursor = state.cursors.get(device.id)
        if cursor is None:
            start = oldest
        else:
            reach = params.rescan_hours * HOUR_MS if rescan else params.overlap_minutes * MINUTE_MS
            start = max(oldest, cursor - int(reach))
        reports = await self._client.reports(device.id, sorted(known.specs), start, start_poll)
        built = build_points(
            reports,
            table=params.table_prefix + device.category,
            device_id=device.id,
            category=device.category,
            name=device.name,
            specs=known.specs,
            unscaled=self._unscaled,
        )
        if built.collisions:
            context.problem(f"device {device.id}: {built.collisions} colliding report(s)")
        complete = start_poll - int(params.lag_minutes * MINUTE_MS)
        if cursor is not None:
            complete = max(complete, cursor)
        if await context.emit(built.points, [Progress(device.id, complete, device.name or None)]):
            state.cursors[device.id] = complete
            state.save(path)
        else:
            context.problem(f"device {device.id}: a batch was refused; its cursor stays put")

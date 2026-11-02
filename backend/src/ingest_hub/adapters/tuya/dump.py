"""Importing a saved report-log dump, for days Tuya no longer returns.

A dump is a directory of JSON files, one per device::

    {"name": ..., "device": {"id": ..., "category": ..., "product": ...},
     "from_ms": ..., "to_ms": ..., "reports": [{"event_time": ..., "code": ..., "value": ...}]}

Values are typed from each device's current specification, through the same code as live
polling, so imported and polled points are indistinguishable. Writing them again overwrites them,
so an interrupted import is simply run again. No progress is written: progress describes what
the live source has fetched, and only it advances it.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from jigkit.sinks import Outcome, Point, Sink

from ingest_hub.adapters.tuya.client import Report, TuyaClient
from ingest_hub.adapters.tuya.source import TuyaParams
from ingest_hub.adapters.tuya.values import build_points, parse_specification

logger = logging.getLogger(__name__)

#: Points per write.
BATCH = 5000


class DumpError(Exception):
    """The dump cannot be imported; the message says why."""


@dataclass(frozen=True)
class DumpFile:
    """One device's saved reports."""

    path: Path
    device_id: str
    category: str
    name: str
    reports: list[Report]


def read_dump(directory: Path) -> list[DumpFile]:
    """Every device file in a dump directory, in name order."""
    files = sorted(directory.glob("*.json"))
    if not files:
        raise DumpError(f"no .json files in {directory}")
    dumps = []
    for path in files:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            device = data["device"]
            reports = [
                Report(str(r["code"]), str(r["value"]), int(r["event_time"]))
                for r in data.get("reports") or []
            ]
            dumps.append(
                DumpFile(
                    path,
                    str(device["id"]),
                    str(device.get("category") or "unknown"),
                    str(data.get("name") or ""),
                    reports,
                )
            )
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise DumpError(f"{path.name} is not a device dump: {error}") from error
    return dumps


def _batches(points: list[Point]) -> Iterator[list[Point]]:
    """Points in slices of :data:`BATCH`."""
    for first in range(0, len(points), BATCH):
        yield points[first : first + BATCH]


async def import_dump(
    directory: Path, params: TuyaParams, client: TuyaClient, sink: Sink
) -> dict[str, int]:
    """Write every report in the dump; returns the points written per device.

    Stops at the first write that fails, saying which file it was in.
    """
    written: dict[str, int] = {}
    unscaled = frozenset(params.unscaled_codes)
    for dump in read_dump(directory):
        if params.devices and dump.device_id not in params.devices:
            logger.info("%s: device not in this source's list; skipped", dump.path.name)
            continue
        specs = parse_specification(await client.specification(dump.device_id))
        built = build_points(
            dump.reports,
            table=params.table_prefix + dump.category,
            device_id=dump.device_id,
            category=dump.category,
            name=dump.name,
            specs=specs,
            unscaled=unscaled,
        )
        for batch in _batches(built.points):
            outcome = await sink.write(batch)
            if outcome is not Outcome.OK:
                raise DumpError(
                    f"{dump.path.name}: the write failed ({outcome.value}): {sink.last_error}"
                )
        written[dump.device_id] = len(built.points)
        logger.info(
            "%s: %d point(s) from %d report(s)",
            dump.path.name,
            len(built.points),
            len(dump.reports),
        )
    return written

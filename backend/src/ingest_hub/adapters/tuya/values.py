"""Turning report-log entries, whose values are all strings, into typed points.

Each device's specification says what every status code holds:

- ``Integer`` with a ``scale``: the reported integer divided by 10^scale, always a float, so two
  devices of one category that scale a code differently still write one column type. Codes
  listed as unscaled keep the integer Tuya reports (``add_ele`` by default: energy in Wh since
  the previous report, which consumers sum themselves).
- ``Boolean``: a bool. ``Bitmap``: the integer. ``Enum``, ``String`` and anything else: the text.

A value that does not parse as its type, or a code the specification does not describe, is kept
as text under ``<code>_raw`` rather than dropped. It never takes the typed column's name, because
InfluxDB fixes a column's type on its first write and refuses a batch that disagrees.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from jigkit.sinks import FieldValue, Point, Precision

from ingest_hub.adapters.tuya.client import Report

logger = logging.getLogger(__name__)

RAW_SUFFIX = "_raw"
#: The field holding the device's name when the point was written; informational only.
NAME_FIELD = "name"


@dataclass(frozen=True)
class CodeSpec:
    """What one status code holds."""

    type: str
    scale: int = 0


def parse_specification(status: Iterable[Mapping[str, Any]]) -> dict[str, CodeSpec]:
    """The specification's status codes, by code."""
    specs: dict[str, CodeSpec] = {}
    for entry in status:
        code = str(entry.get("code") or "")
        if not code:
            continue
        values = entry.get("values") or "{}"
        try:
            ranges = json.loads(values) if isinstance(values, str) else dict(values)
        except ValueError, TypeError:
            ranges = {}
        try:
            scale = int(ranges.get("scale") or 0)
        except TypeError, ValueError:
            scale = 0
        specs[code] = CodeSpec(str(entry.get("type") or ""), scale)
    return specs


def field_for(
    code: str, raw: str, spec: CodeSpec | None, unscaled: frozenset[str]
) -> tuple[str, FieldValue]:
    """The field name and typed value one report becomes."""
    fallback = (code + RAW_SUFFIX, raw)
    if spec is None:
        return fallback
    kind = spec.type.lower()
    try:
        if kind == "integer":
            number = int(raw.strip())
            if code in unscaled:
                return code, number
            return code, number / 10**spec.scale
        if kind == "boolean":
            text = raw.strip().lower()
            if text not in ("true", "false"):
                return fallback
            return code, text == "true"
        if kind == "bitmap":
            return code, int(raw.strip())
    except ValueError:
        return fallback
    return code, raw


@dataclass
class Built:
    """The points one device's reports became, and how many collided."""

    points: list[Point]
    #: Reports dropped because another report of the same code and millisecond had a different
    #: value: one field per code can hold only one value per moment.
    collisions: int = 0


def build_points(
    reports: Iterable[Report],
    *,
    table: str,
    device_id: str,
    category: str,
    name: str,
    specs: Mapping[str, CodeSpec],
    unscaled: frozenset[str],
) -> Built:
    """One point per moment at which the device reported, tagged by device and category.

    Reports of several codes at the same millisecond share a point. Re-fetching the same reports
    gives the same points, which overwrite themselves in InfluxDB.
    """
    by_time: dict[int, dict[str, FieldValue]] = {}
    collisions = 0
    for report in reports:
        field, value = field_for(report.code, report.value, specs.get(report.code), unscaled)
        fields = by_time.setdefault(report.event_time, {})
        if field in fields and fields[field] != value:
            collisions += 1
        fields[field] = value
    if collisions:
        logger.warning(
            "device %s: %d report(s) shared a code and millisecond with a different value; "
            "only the last of each was kept",
            device_id,
            collisions,
        )
    tags = (("device_id", device_id), ("category", category))
    points = [
        Point(
            table,
            tags,
            moment,
            {**fields, NAME_FIELD: name} if name else fields,
            precision=Precision.MILLISECONDS,
        )
        for moment, fields in sorted(by_time.items())
    ]
    return Built(points, collisions)

"""Comparing two tables in one InfluxDB 3 database, to prove a new writer matches an old one.

Run a new source into a shadow table next to the writer it replaces, then compare:

- the schemas: every column and its type, which must be identical, since dashboards and queries
  depend on them;
- each minute of the window both tables cover: how many rows each has, and how far apart the
  means of every shared numeric column are.

Two independent writers stamp the same reading a moment apart, so a row can land in the next
second or minute on one side; the comparison allows for that rather than matching rows one to one.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from jigkit.influx import Influx3Sink
from jigkit.influx.sink import IDENTIFIER, parse_time, sql_timestamp

NUMERIC = ("Float64", "Int64", "UInt64")


@dataclass
class ColumnDiff:
    """How one numeric column compares across the minutes both tables have."""

    name: str
    minutes: int = 0
    #: The largest difference between the two tables' per-minute means, relative to the larger.
    max_relative: float = 0.0
    #: The mean of those per-minute relative differences.
    mean_relative: float = 0.0


@dataclass
class Comparison:
    """The result: schema differences, row counts and per-column agreement."""

    old: str
    new: str
    start: datetime | None = None
    end: datetime | None = None
    only_old: dict[str, str] = field(default_factory=dict)
    only_new: dict[str, str] = field(default_factory=dict)
    type_changes: dict[str, tuple[str, str]] = field(default_factory=dict)
    rows_old: int = 0
    rows_new: int = 0
    minutes_both: int = 0
    minutes_only_old: list[datetime] = field(default_factory=list)
    minutes_only_new: list[datetime] = field(default_factory=list)
    columns: list[ColumnDiff] = field(default_factory=list)

    @property
    def schema_matches(self) -> bool:
        """Whether both tables have the same columns with the same types."""
        return not (self.only_old or self.only_new or self.type_changes)

    def ok(self, tolerance: float) -> bool:
        """Whether the schemas match, no minute is missing on either side, and every column's
        worst per-minute mean is within ``tolerance`` (relative)."""
        return (
            self.schema_matches
            and self.minutes_both > 0
            and not self.minutes_only_old
            and not self.minutes_only_new
            and all(c.max_relative <= tolerance for c in self.columns)
        )

    def report(self, tolerance: float) -> str:
        """A human-readable summary."""
        lines = [f"{self.old} (old) against {self.new} (new)"]
        if self.schema_matches:
            lines.append("schema: identical")
        for name, kind in sorted(self.only_old.items()):
            lines.append(f"schema: only in {self.old}: {name} {kind}")
        for name, kind in sorted(self.only_new.items()):
            lines.append(f"schema: only in {self.new}: {name} {kind}")
        for name, (was, now) in sorted(self.type_changes.items()):
            lines.append(f"schema: {name} is {was} in {self.old} but {now} in {self.new}")
        if self.start is None or self.end is None:
            lines.append(f"window: {self.new} has no rows yet")
            return "\n".join(lines)
        lines.append(
            f"window: {self.start:%Y-%m-%d %H:%M} to {self.end:%Y-%m-%d %H:%M} UTC, "
            f"{self.minutes_both} minute(s) in both"
        )
        lines.append(f"rows: {self.rows_old} old, {self.rows_new} new")
        for label, missing in (
            (f"only in {self.old}", self.minutes_only_old),
            (f"only in {self.new}", self.minutes_only_new),
        ):
            if missing:
                shown = ", ".join(f"{m:%H:%M}" for m in missing[:10])
                more = f" and {len(missing) - 10} more" if len(missing) > 10 else ""
                lines.append(f"minutes {label}: {len(missing)} ({shown}{more})")
        for column in self.columns:
            flag = "" if column.max_relative <= tolerance else "  <-- over tolerance"
            lines.append(
                f"{column.name}: worst minute {column.max_relative:.4%}, "
                f"average {column.mean_relative:.4%}{flag}"
            )
        lines.append("verdict: " + ("match" if self.ok(tolerance) else "DIFFERENT"))
        return "\n".join(lines)


def _quoted(name: str) -> str:
    """A column or table name for SQL, refused unless it is a plain identifier."""
    if not IDENTIFIER.fullmatch(name):
        raise ValueError(f"not a plain identifier: {name!r}")
    return f'"{name}"'


async def _schema(sink: Influx3Sink, table: str) -> dict[str, str]:
    """A table's columns and their types."""
    rows = await sink.rows(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'iox' AND table_name = $table",
        {"table": table},
    )
    return {str(r["column_name"]): str(r["data_type"]) for r in rows}


async def _minutes(
    sink: Influx3Sink, table: str, columns: list[str], start: datetime, end: datetime
) -> dict[datetime, dict[str, Any]]:
    """Each minute's row count and column means for a table, in a window."""
    means = "".join(f", avg({_quoted(c)}) AS {_quoted(c)}" for c in columns)
    rows = await sink.query(
        f"SELECT date_bin(INTERVAL '1 minute', time) AS minute, count(*) AS n{means} "
        f"FROM {_quoted(table)} WHERE time >= {sql_timestamp(start)} "
        f"AND time < {sql_timestamp(end)} GROUP BY minute"
    )
    return {parse_time(str(r["minute"])): r for r in rows}


async def compare(
    sink: Influx3Sink,
    old: str,
    new: str,
    *,
    hours: float,
    now: datetime | None = None,
) -> Comparison:
    """Compare table ``new`` against ``old`` over the last ``hours`` both cover."""
    result = Comparison(old, new)
    old_schema, new_schema = await _schema(sink, old), await _schema(sink, new)
    result.only_old = {k: v for k, v in old_schema.items() if k not in new_schema}
    result.only_new = {k: v for k, v in new_schema.items() if k not in old_schema}
    result.type_changes = {
        k: (old_schema[k], new_schema[k])
        for k in old_schema.keys() & new_schema.keys()
        if old_schema[k] != new_schema[k]
    }
    if not new_schema:
        return result

    first = await sink.rows(f"SELECT min(time) AS first FROM {_quoted(new)}")
    if not first or first[0].get("first") is None:
        return result
    now = now or datetime.now(UTC)
    # Whole minutes only: the one the shadow started in, and the one in progress, are partial.
    start = max(parse_time(str(first[0]["first"])), now - timedelta(hours=hours))
    start = start.replace(second=0, microsecond=0) + timedelta(minutes=1)
    end = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
    if end <= start:
        return result
    result.start, result.end = start, end

    numeric = sorted(
        k
        for k in old_schema.keys() & new_schema.keys()
        if old_schema[k] in NUMERIC and new_schema[k] in NUMERIC
    )
    old_minutes = await _minutes(sink, old, numeric, start, end)
    new_minutes = await _minutes(sink, new, numeric, start, end)
    result.rows_old = sum(int(r["n"]) for r in old_minutes.values())
    result.rows_new = sum(int(r["n"]) for r in new_minutes.values())
    both = sorted(old_minutes.keys() & new_minutes.keys())
    result.minutes_both = len(both)
    result.minutes_only_old = sorted(old_minutes.keys() - new_minutes.keys())
    result.minutes_only_new = sorted(new_minutes.keys() - old_minutes.keys())

    for column in numeric:
        diffs = []
        for minute in both:
            a, b = old_minutes[minute].get(column), new_minutes[minute].get(column)
            if a is None or b is None:
                continue
            a, b = float(a), float(b)
            scale = max(abs(a), abs(b))
            diffs.append(0.0 if scale == 0 else abs(a - b) / scale)
        if diffs:
            result.columns.append(
                ColumnDiff(column, len(diffs), max(diffs), sum(diffs) / len(diffs))
            )
    return result

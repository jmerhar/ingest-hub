"""Turning MQTT messages with JSON object payloads into points, by route."""

from __future__ import annotations

import json
import math
import re
from typing import Any, Literal

from jigkit.sinks import FieldValue, Point, Precision
from pydantic import BaseModel, ConfigDict, Field, field_validator

#: A ``{N}`` placeholder in a tag value: the topic's N-th segment, counting from 0.
SEGMENT = re.compile(r"\{(\d+)\}")
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class Route(BaseModel):
    """Messages on one topic filter, and the table they become points in."""

    model_config = ConfigDict(extra="forbid")

    #: An MQTT topic filter; ``+`` and ``#`` wildcards allowed.
    topic: str = Field(min_length=1)
    table: str
    #: Tag values, where ``{N}`` stands for the topic's N-th segment (from 0).
    tags: dict[str, str] = Field(default_factory=dict)
    #: ``float`` writes every number as a float, so a value that happens to be whole never
    #: fixes a column as integer; ``keep`` writes integers as integers.
    numbers: Literal["float", "keep"] = "float"
    #: Only these top-level keys become fields; every numeric key when unset.
    fields: list[str] | None = None
    #: A top-level key holding the measurement time in Unix seconds; the time the message was
    #: received when unset.
    timestamp_field: str | None = None

    @field_validator("table")
    @classmethod
    def _table(cls, value: str) -> str:
        """A table name must be a plain identifier."""
        if not IDENTIFIER.fullmatch(value):
            raise ValueError("must be letters, digits and '_', not starting with a digit")
        return value

    @field_validator("topic")
    @classmethod
    def _topic(cls, value: str) -> str:
        """``#`` may only end a filter, and wildcards must fill a whole segment."""
        segments = value.split("/")
        for n, segment in enumerate(segments):
            if "#" in segment and (segment != "#" or n != len(segments) - 1):
                raise ValueError("'#' must be a whole, final segment")
            if "+" in segment and segment != "+":
                raise ValueError("'+' must be a whole segment")
        return value


def topic_matches(topic_filter: str, topic: str) -> bool:
    """Whether a topic matches an MQTT filter."""
    pattern, levels = topic_filter.split("/"), topic.split("/")
    for n, part in enumerate(pattern):
        if part == "#":
            return True
        if n >= len(levels) or (part != "+" and part != levels[n]):
            return False
    return len(pattern) == len(levels)


def route_for(routes: list[Route], topic: str) -> Route | None:
    """The first route whose filter matches the topic."""
    return next((r for r in routes if topic_matches(r.topic, topic)), None)


def _tag_value(template: str, segments: list[str]) -> str:
    """A tag value with its ``{N}`` placeholders filled from the topic; out of range is empty."""
    return SEGMENT.sub(
        lambda m: segments[int(m.group(1))] if int(m.group(1)) < len(segments) else "", template
    )


def _number(value: Any, mode: str) -> FieldValue | None:
    """A JSON value as a field, or None when it is not a finite number (bools are not)."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return float(value) if mode == "float" else value


def message_point(
    route: Route, topic: str, payload: bytes, received: float, precision: Precision
) -> Point | None:
    """The point one message becomes, or None when it carries nothing to write.

    The payload must be a JSON object. Its numeric top-level values become fields; anything
    else (text, booleans, nulls, lists, nested objects) is left out.
    """
    try:
        data = json.loads(payload)
    except ValueError:
        return None
    if not isinstance(data, dict):
        return None
    keys = route.fields if route.fields is not None else list(data)
    fields = {
        key: number
        for key in keys
        if key != route.timestamp_field
        and (number := _number(data.get(key), route.numbers)) is not None
    }
    if not fields:
        return None
    moment = received
    if route.timestamp_field is not None:
        stamp = _number(data.get(route.timestamp_field), "float")
        if stamp is None:
            return None
        moment = float(stamp)
    segments = topic.split("/")
    tags = tuple((k, _tag_value(v, segments)) for k, v in route.tags.items())
    return Point(
        route.table,
        tags,
        int(moment * precision.per_second),
        fields,
        precision=precision,
    )

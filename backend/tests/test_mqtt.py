"""The MQTT adapter: routing, turning payloads into points, and the source's session handling."""

from __future__ import annotations

import contextlib
import json
from pathlib import Path

import pytest
from jigkit.sinks import Point, Precision
from pydantic import ValidationError

from ingest_hub.adapters import mqtt
from ingest_hub.adapters.mqtt import MqttParams, MqttSource
from ingest_hub.adapters.mqtt.mapping import Route, message_point, route_for, topic_matches
from ingest_hub.collect import SourceContext

# A three-phase meter's status message, in the shape such devices publish (synthetic values).
EM = {
    "id": 0,
    "a_current": 0.091,
    "a_voltage": 235.6,
    "a_act_power": 5,
    "n_current": None,
    "total_act_power": 166.207,
    "user_calibrated_phase": [],
    "calibrated": True,
    "label": "x",
}


@pytest.mark.parametrize(
    ("topic_filter", "topic", "expected"),
    [
        ("meter/status/em:0", "meter/status/em:0", True),
        ("meter/status/em:0", "meter/status/emdata:0", False),
        ("+/status/em:0", "meter-1/status/em:0", True),
        ("meter/#", "meter/status/em:0", True),
        ("meter/#", "other/status", False),
        ("meter/+", "meter/status/em:0", False),
        ("meter/status/em:0/x", "meter/status/em:0", False),
    ],
)
def test_topic_matches(topic_filter, topic, expected):
    assert topic_matches(topic_filter, topic) is expected


def route(**overrides) -> Route:
    values = {"topic": "+/status/em:0", "table": "meter_em", "tags": {"device": "{0}"}}
    return Route.model_validate({**values, **overrides})


def test_numeric_values_become_float_fields_tagged_from_the_topic():
    point = message_point(
        route(), "meter-1/status/em:0", json.dumps(EM).encode(), 1700000000.7, Precision.SECONDS
    )
    assert point == Point(
        "meter_em",
        (("device", "meter-1"),),
        1700000000,
        {
            "id": 0.0,
            "a_current": 0.091,
            "a_voltage": 235.6,
            "a_act_power": 5.0,
            "total_act_power": 166.207,
        },
        precision=Precision.SECONDS,
    )
    assert all(isinstance(v, float) for v in point.fields.values())


def test_numbers_can_keep_their_type_and_fields_can_be_chosen():
    point = message_point(
        route(numbers="keep", fields=["id", "a_act_power", "missing", "label"]),
        "m/status/em:0",
        json.dumps(EM).encode(),
        5.0,
        Precision.MILLISECONDS,
    )
    assert point.fields == {"id": 0, "a_act_power": 5}
    assert point.timestamp == 5000 and point.precision is Precision.MILLISECONDS


def test_a_timestamp_field_sets_the_time_and_is_not_a_field():
    payload = json.dumps({"ts": 1700000123.25, "power": 2}).encode()
    point = message_point(
        route(timestamp_field="ts"), "m/status/em:0", payload, 9.0, Precision.MILLISECONDS
    )
    assert point.timestamp == 1700000123250 and point.fields == {"power": 2.0}
    missing = json.dumps({"power": 2}).encode()
    assert (
        message_point(route(timestamp_field="ts"), "m/s", missing, 9.0, Precision.SECONDS) is None
    )


@pytest.mark.parametrize(
    "payload", [b"true", b"not json", b"[1, 2]", b'{"text": "x", "flag": false}', b'{"x": NaN}']
)
def test_payloads_without_numbers_give_nothing(payload):
    assert message_point(route(), "m/status/em:0", payload, 1.0, Precision.SECONDS) is None


def test_a_tag_naming_a_missing_segment_is_empty():
    point = message_point(
        route(tags={"device": "{0}", "far": "{9}", "fixed": "home"}),
        "m/status/em:0",
        b'{"p": 1}',
        1.0,
        Precision.SECONDS,
    )
    assert point.tags == (("device", "m"), ("far", ""), ("fixed", "home"))


def test_route_for_picks_the_first_match():
    routes = [route(topic="a/#", table="first"), route(topic="a/b", table="second")]
    assert route_for(routes, "a/b").table == "first"
    assert route_for(routes, "c") is None


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"table": "1bad"}, "letters, digits"),
        ({"topic": "a/#/b"}, "whole, final"),
        ({"topic": "a/b#"}, "whole, final"),
        ({"topic": "a/b+"}, "whole segment"),
        ({"surprise": 1}, "Extra inputs"),
    ],
)
def test_routes_are_validated(overrides, message):
    with pytest.raises(ValidationError, match=message):
        route(**overrides)


class FakeMessage:
    def __init__(self, topic: str, payload: object) -> None:
        self.topic = topic
        self.payload = payload


class FakeSession:
    """Records subscriptions and yields queued messages, then ends like a dropped connection."""

    def __init__(self, messages: list[FakeMessage]) -> None:
        self.queued = messages
        self.subscriptions: list[tuple[str, int]] = []

    async def subscribe(self, topic: str, qos: int = 0) -> None:
        self.subscriptions.append((topic, qos))

    @property
    def messages(self):
        async def iterate():
            for message in self.queued:
                yield message

        return iterate()


def params(**overrides) -> MqttParams:
    values = {
        "host": "broker",
        "routes": [
            {"topic": "+/status/em:0", "table": "meter_em", "tags": {"device": "{0}"}},
            {"topic": "+/status/emdata:0", "table": "meter_emdata", "tags": {"device": "{0}"}},
        ],
    }
    return MqttParams.model_validate({**values, **overrides})


async def test_the_source_subscribes_routes_messages_and_raises_when_the_connection_ends(tmp_path):
    session = FakeSession(
        [
            FakeMessage("m1/online", b"true"),
            FakeMessage("m1/status/em:0", json.dumps(EM).encode()),
            FakeMessage("m1/status/emdata:0", '{"total_act": 12.5}'),  # text payloads too
            FakeMessage("m1/status/em:0", b"[]"),
        ]
    )

    @contextlib.asynccontextmanager
    async def connect():
        yield session

    batches: list[list[Point]] = []
    successes = []

    async def emit(points: list[Point]) -> bool:
        batches.append(points)
        return True

    context = SourceContext(
        "meter", tmp_path, emit, on_success=lambda: successes.append(1), on_problem=print
    )
    source = MqttSource(params(qos=2), connect, clock=lambda: 1700000000.0)
    with pytest.raises(ConnectionError, match="closed"):
        await source.run(context)

    assert session.subscriptions == [("+/status/em:0", 2), ("+/status/emdata:0", 2)]
    assert [b[0].table for b in batches] == ["meter_em", "meter_emdata"]
    assert batches[1][0].fields == {"total_act": 12.5}
    assert len(successes) == 3  # subscribed, then each delivered message


def test_params_are_validated():
    with pytest.raises(ValidationError, match="routes"):
        MqttParams.model_validate({"host": "b", "routes": []})
    with pytest.raises(ValidationError, match="qos"):
        params(qos=3)


async def test_build_names_the_session_after_the_source_and_reads_the_password(tmp_path: Path):
    (tmp_path / "pw").write_text("synthetic\n")
    source = mqtt.build(params(username="hub", password_file=tmp_path / "pw"), None, "meter")
    client = source._connect()
    assert client._client._client_id == b"ingest-hub-meter"
    # A persistent session, so the broker keeps QoS 1 and 2 messages across a reconnect.
    assert client._client._clean_session is False
    assert client._client._username == b"hub" and client._client._password == b"synthetic"
    named = mqtt.build(params(client_id="custom"), None, "meter")._connect()
    assert named._client._client_id == b"custom" and named._client._password is None

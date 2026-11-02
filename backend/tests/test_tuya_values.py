"""Typing Tuya's string values from the device specification, and grouping them into points."""

from __future__ import annotations

import logging

import pytest
from jigkit.sinks import Precision

from ingest_hub.adapters.tuya.client import Report
from ingest_hub.adapters.tuya.values import (
    CodeSpec,
    build_points,
    field_for,
    parse_specification,
)

# The shape a plug's specification has, with ranges as JSON text.
PLUG_SPEC = [
    {"code": "switch_1", "type": "Boolean", "values": "{}"},
    {"code": "add_ele", "type": "Integer", "values": '{"min":0,"max":50000,"scale":3,"step":100}'},
    {"code": "cur_power", "type": "Integer", "values": '{"unit":"W","scale":1}'},
    {"code": "cur_current", "type": "Integer", "values": '{"unit":"mA","scale":0}'},
    {"code": "fault", "type": "Bitmap", "values": '{"label":["ov_cr"]}'},
    {"code": "relay_status", "type": "Enum", "values": '{"range":["power_off","last"]}'},
    {"code": "cycle_time", "type": "String", "values": "{}"},
    {"code": "odd", "type": "Integer", "values": "not json"},
    {"code": "odd_scale", "type": "Integer", "values": {"scale": "x"}},
    {"code": "", "type": "Integer"},
]
UNSCALED = frozenset({"add_ele"})


def test_parse_specification_reads_types_and_scales():
    specs = parse_specification(PLUG_SPEC)
    assert specs["add_ele"] == CodeSpec("Integer", 3)
    assert specs["cur_power"] == CodeSpec("Integer", 1)
    assert specs["switch_1"] == CodeSpec("Boolean", 0)
    assert specs["odd"] == CodeSpec("Integer", 0)
    assert specs["odd_scale"] == CodeSpec("Integer", 0)
    assert "" not in specs


@pytest.mark.parametrize(
    ("code", "raw", "expected"),
    [
        # Energy is stored as reported: Wh since the previous report, an integer.
        ("add_ele", "72", ("add_ele", 72)),
        ("cur_power", "1561", ("cur_power", 156.1)),
        # Always a float, so devices scaling a code differently share one column type.
        ("cur_current", "148", ("cur_current", 148.0)),
        ("switch_1", "true", ("switch_1", True)),
        ("switch_1", "False", ("switch_1", False)),
        ("switch_1", "maybe", ("switch_1_raw", "maybe")),
        ("fault", "3", ("fault", 3)),
        ("fault", "x", ("fault_raw", "x")),
        ("relay_status", "last", ("relay_status", "last")),
        ("cycle_time", "", ("cycle_time", "")),
        ("cur_power", "n/a", ("cur_power_raw", "n/a")),
        ("unknown_code", "7", ("unknown_code_raw", "7")),
    ],
)
def test_field_for(code, raw, expected):
    specs = parse_specification(PLUG_SPEC)
    result = field_for(code, raw, specs.get(code), UNSCALED)
    assert result == expected
    assert type(result[1]) is type(expected[1])


def test_build_points_groups_reports_by_moment_with_tags_and_name():
    reports = [
        Report("cur_power", "200", 2000),
        Report("add_ele", "5", 1000),
        Report("cur_current", "10", 1000),
    ]
    built = build_points(
        reports,
        table="tuya_cz",
        device_id="dev-1",
        category="cz",
        name="Kitchen",
        specs=parse_specification(PLUG_SPEC),
        unscaled=UNSCALED,
    )
    assert built.collisions == 0
    first, second = built.points
    assert first.table == "tuya_cz"
    assert first.tags == (("device_id", "dev-1"), ("category", "cz"))
    assert first.timestamp == 1000 and first.precision is Precision.MILLISECONDS
    assert first.fields == {"add_ele": 5, "cur_current": 10.0, "name": "Kitchen"}
    assert second.fields == {"cur_power": 20.0, "name": "Kitchen"}


def test_build_points_without_a_name_writes_no_name_field():
    built = build_points(
        [Report("fault", "0", 5)],
        table="t",
        device_id="d",
        category="cz",
        name="",
        specs=parse_specification(PLUG_SPEC),
        unscaled=UNSCALED,
    )
    assert built.points[0].fields == {"fault": 0}


def test_colliding_reports_are_counted_and_logged(caplog):
    reports = [
        Report("add_ele", "5", 1000),
        Report("add_ele", "5", 1000),  # the same report twice is not a collision
        Report("add_ele", "7", 1000),
    ]
    with caplog.at_level(logging.WARNING):
        built = build_points(
            reports,
            table="t",
            device_id="dev-9",
            category="cz",
            name="",
            specs=parse_specification(PLUG_SPEC),
            unscaled=UNSCALED,
        )
    assert built.collisions == 1
    assert built.points[0].fields == {"add_ele": 7}
    assert "dev-9" in caplog.text

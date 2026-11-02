"""Shared fixtures: a stand-in Tuya cloud, and configuration files that point at stand-ins."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from jigkit.testing.http import Received

#: A synthetic plug's specification, in the shape Tuya returns.
PLUG_SPEC = {
    "category": "cz",
    "status": [
        {"code": "switch_1", "type": "Boolean", "values": "{}"},
        {"code": "add_ele", "type": "Integer", "values": '{"min":0,"scale":3}'},
        {"code": "cur_voltage", "type": "Integer", "values": '{"unit":"V","scale":1}'},
    ],
}


def tuya_cloud(devices: list[dict], reports: dict[str, list[dict]]):
    """A ``respond`` for StubServer that answers like the Tuya OpenAPI."""

    def respond(request: Received) -> tuple[int, str]:
        path = request.path
        if path == "/v1.0/token":
            result: object = {"access_token": "tok", "expire_time": 7200}
        elif path == "/v2.0/cloud/thing/device":
            result = devices
        elif path.endswith("/specifications"):
            result = PLUG_SPEC
        elif path.endswith("/report-logs"):
            device = path.split("/")[4]
            start, end = int(request.query["start_time"]), int(request.query["end_time"])
            logs = [r for r in reports.get(device, []) if start <= r["event_time"] <= end]
            result = {"logs": logs, "has_more": False}
        else:
            return 404, "{}"
        return 200, json.dumps({"success": True, "result": result})

    return respond


@pytest.fixture
def secrets(tmp_path: Path) -> Path:
    """A directory of secret files with synthetic values."""
    directory = tmp_path / "secrets"
    directory.mkdir()
    for name, value in {
        "tuya_id": "synthetic-id",
        "tuya_secret": "synthetic-secret",
        "influx_token": "synthetic-token",
    }.items():
        (directory / name).write_text(value + "\n")
    return directory


def write_config(
    path: Path, influx_url: str, tuya_url: str, secrets: Path, extra: str = ""
) -> Path:
    """A configuration with one InfluxDB 3 destination and one Tuya source."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"""
[destinations.home]
kind = "influx3"
url = "{influx_url}"
database = "example"
token_file = "{secrets / "influx_token"}"

[sources.plugs]
kind = "tuya"
destination = "home"

[sources.plugs.params]
base_url = "{tuya_url}"
access_id_file = "{secrets / "tuya_id"}"
access_secret_file = "{secrets / "tuya_secret"}"
{extra}
"""
    )
    return path

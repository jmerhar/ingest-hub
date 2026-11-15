"""Home Connect keys and values as fields.

Home Connect names everything an appliance reports with a dotted key whose second-to-last part
says what it is: ``BSH.Common.Status.DoorState``, ``BSH.Common.Setting.PowerState``,
``BSH.Common.Option.RemainingProgramTime``, ``Dishcare.Dishwasher.Event.RinseAidLack``. A key
becomes the field ``<kind>_<name>`` in snake case: ``status_door_state``, ``setting_power_state``,
``option_remaining_program_time``, ``event_rinse_aid_lack``. The two ``Root`` keys, the active and
the selected programme, become ``active_program`` and ``selected_program``. A key of fewer than
three parts is the whole key in snake case.

Values keep their type: a number is a float, a boolean a boolean, and text a string. An
enumeration's value is stored without its type's prefix (``BSH.Common.EnumType.DoorState.Open``
is ``Open``), and a programme by its last part (``Dishcare.Dishwasher.Program.Eco50`` is
``Eco50``). No programme is an empty string; any other empty value, and lists or objects, are
left out.
"""

from __future__ import annotations

import re
from typing import Any

from jigkit.sinks import FieldValue

ROOT_FIELDS = {"ActiveProgram": "active_program", "SelectedProgram": "selected_program"}
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
_ENUM = re.compile(r"^[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)*\.EnumType\.[A-Za-z0-9]+\.(.+)$")
_PROGRAM = re.compile(r"^[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)*\.Program\.(.+)$")


def snake(name: str) -> str:
    """``RinseAidLack`` → ``rinse_aid_lack``; any other character becomes an underscore."""
    spaced = _CAMEL.sub("_", name)
    return re.sub(r"[^a-z0-9]+", "_", spaced.lower()).strip("_")


def field_name(key: str) -> str:
    """The field a key is written to."""
    parts = key.split(".")
    if len(parts) < 3:
        return snake(key.replace(".", "_"))
    kind, name = parts[-2], parts[-1]
    if kind == "Root" and name in ROOT_FIELDS:
        return ROOT_FIELDS[name]
    return f"{snake(kind)}_{snake(name)}"


def field_value(key: str, value: Any) -> FieldValue | None:
    """The value a key's field is given, or None when it is left out."""
    if field_name(key) in ROOT_FIELDS.values():
        if value is None:
            return ""
        text = str(value)
        match = _PROGRAM.match(text)
        return match.group(1) if match else text
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        match = _ENUM.match(value)
        return match.group(1) if match else value
    return None


def type_of(value: FieldValue) -> str:
    """A field value's type, as InfluxDB keeps it for a field."""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, float):
        return "float"
    if isinstance(value, int):
        return "integer"
    return "string"


__all__ = ["field_name", "field_value", "snake", "type_of"]

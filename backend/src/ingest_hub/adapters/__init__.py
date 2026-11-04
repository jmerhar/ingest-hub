"""The source adapters built into the image, by the ``kind`` a configuration names them with."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx2
from pydantic import BaseModel

from ingest_hub.adapters import frank, mqtt, tuya, vaillant
from ingest_hub.adapters.vaillant import export as vaillant_export
from ingest_hub.collect import Source


@dataclass(frozen=True)
class Adapter:
    """How to validate a kind of source's parameters and build one."""

    params: type[BaseModel]
    #: Builds the source from its validated parameters, the shared HTTP client and its name.
    build: Callable[[Any, httpx2.AsyncClient, str], Source]


ADAPTERS: dict[str, Adapter] = {
    "frank": Adapter(frank.FrankParams, frank.build),
    "mqtt": Adapter(mqtt.MqttParams, mqtt.build),
    "tuya": Adapter(tuya.TuyaParams, tuya.build),
    "vaillant": Adapter(vaillant.VaillantParams, vaillant.build),
    "vaillant_export": Adapter(vaillant_export.VaillantExportParams, vaillant_export.build),
}

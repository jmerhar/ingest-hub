"""The source adapters built into the image, by the ``kind`` a configuration names them with."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx2
from pydantic import BaseModel

from ingest_hub.adapters import (
    alphaess_cloud,
    frank,
    modbus,
    mqtt,
    resmed,
    tuya,
    vaillant,
    zaptec,
)
from ingest_hub.adapters.google import takeout as google_takeout
from ingest_hub.adapters.vaillant import export as vaillant_export
from ingest_hub.collect import Source


@dataclass(frozen=True)
class Adapter:
    """How to validate a kind of source's parameters and build one."""

    params: type[BaseModel]
    #: Builds the source from its validated parameters, the shared HTTP client and its name.
    build: Callable[[Any, httpx2.AsyncClient, str], Source]


ADAPTERS: dict[str, Adapter] = {
    "alphaess_cloud": Adapter(alphaess_cloud.AlphaCloudParams, alphaess_cloud.build),
    "frank": Adapter(frank.FrankParams, frank.build),
    "google_takeout": Adapter(google_takeout.TakeoutParams, google_takeout.build),
    "modbus": Adapter(modbus.ModbusParams, modbus.build),
    "mqtt": Adapter(mqtt.MqttParams, mqtt.build),
    "resmed_sd": Adapter(resmed.ResmedParams, resmed.build),
    "tuya": Adapter(tuya.TuyaParams, tuya.build),
    "vaillant": Adapter(vaillant.VaillantParams, vaillant.build),
    "vaillant_export": Adapter(vaillant_export.VaillantExportParams, vaillant_export.build),
    "zaptec": Adapter(zaptec.ZaptecParams, zaptec.build),
}

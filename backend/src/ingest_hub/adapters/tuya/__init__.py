"""The Tuya cloud adapter: smart plugs and other devices, from their report logs."""

from __future__ import annotations

import httpx2

from ingest_hub.adapters.tuya.client import TuyaClient
from ingest_hub.adapters.tuya.source import TuyaParams, TuyaSource
from ingest_hub.config import read_secret


def make_client(params: TuyaParams, client: httpx2.AsyncClient) -> TuyaClient:
    """A client for the cloud project the parameters name, with its secrets read."""
    return TuyaClient(
        read_secret(params.access_id_file),
        read_secret(params.access_secret_file),
        params.url,
        client,
    )


def build(params: TuyaParams, client: httpx2.AsyncClient) -> TuyaSource:
    """A Tuya source from its validated parameters."""
    return TuyaSource(params, make_client(params, client))


__all__ = ["TuyaParams", "TuyaSource", "build", "make_client"]

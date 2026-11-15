"""The Home Connect adapter: appliances of Bosch, Siemens, Neff, Gaggenau and the other brands on
BSH's Home Connect platform, followed live through its official API."""

from __future__ import annotations

import httpx2

from ingest_hub.adapters.home_connect.source import HomeConnectParams, HomeConnectSource
from ingest_hub.config import read_secret


def build(
    params: HomeConnectParams, client: httpx2.AsyncClient, name: str = ""
) -> HomeConnectSource:
    """A Home Connect source from its validated parameters, with its client ID read now."""
    return HomeConnectSource(params, client, name, read_secret(params.client_id_file))


__all__ = ["HomeConnectParams", "HomeConnectSource", "build"]

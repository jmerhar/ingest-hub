"""Turning a configuration into running parts: sinks per destination, sources, the runtime."""

from __future__ import annotations

from dataclasses import dataclass

import httpx2
from jigkit.influx import Influx2Sink, Influx3Sink
from jigkit.sinks import Sink
from pydantic import BaseModel, ValidationError

from ingest_hub.adapters import ADAPTERS
from ingest_hub.collect import SourceSpec
from ingest_hub.config import ConfigError, DestinationConfig, HubConfig, describe, read_secret


def make_sink(destination: DestinationConfig) -> Sink:
    """The sink for one destination, with its token read."""
    token = read_secret(destination.token_file) if destination.token_file else ""
    if destination.kind == "influx3":
        return Influx3Sink(destination.url, destination.database, token)
    return Influx2Sink(destination.url, destination.database, token, org=destination.org)


def source_params(config: HubConfig, name: str) -> BaseModel:
    """A source's parameters, validated by its adapter."""
    source = config.sources[name]
    adapter = ADAPTERS.get(source.kind)
    if adapter is None:
        known = ", ".join(sorted(ADAPTERS))
        raise ConfigError(f"source {name!r}: unknown kind {source.kind!r}; one of {known}")
    try:
        return adapter.params.model_validate(source.params)
    except ValidationError as error:
        raise ConfigError(f"source {name!r} params: {describe(error)}") from None


@dataclass
class Plan:
    """What a run starts: the sources and the sinks they share."""

    specs: list[SourceSpec]
    sinks: dict[str, Sink]


def plan(config: HubConfig, client: httpx2.AsyncClient, only: str | None = None) -> Plan:
    """Build every enabled source, or only the one named, with its destination's sink.

    ``--only`` runs a source even when it is disabled, since naming it is explicit.
    """
    if only is not None and only not in config.sources:
        raise ConfigError(f"no source named {only!r}")
    if only is not None:
        names = [only]
    else:
        names = [name for name, source in config.sources.items() if source.enabled]
    if not names:
        raise ConfigError("no enabled sources to run")
    sinks: dict[str, Sink] = {}
    specs = []
    for name in names:
        source = config.sources[name]
        params = source_params(config, name)
        if source.destination not in sinks:
            sinks[source.destination] = make_sink(config.destinations[source.destination])
        heartbeat = read_secret(source.heartbeat_url_file) if source.heartbeat_url_file else None
        specs.append(
            SourceSpec(
                name=name,
                kind=source.kind,
                source=ADAPTERS[source.kind].build(params, client, name),
                destination=source.destination,
                sink=sinks[source.destination],
                heartbeat_url=heartbeat,
            )
        )
    return Plan(specs, sinks)

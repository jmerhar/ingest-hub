"""The configuration file: where points go (destinations) and where they come from (sources).

A TOML file, one table per destination and per source::

    [destinations.home]
    kind = "influx3"
    url = "http://influxdb:8181"
    database = "example"
    token_file = "/run/secrets/influxdb_token"

    [sources.plugs]
    kind = "tuya"
    destination = "home"
    heartbeat_url_file = "/run/secrets/plugs_heartbeat_url"

    [sources.plugs.params]
    access_id_file = "/run/secrets/tuya_access_id"
    access_secret_file = "/run/secrets/tuya_access_secret"

Secrets never appear in the file itself: every credential is named by the path of a file that
holds it (``*_file``), such as a mounted Docker secret. Each adapter validates its own
``params``.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

#: A source or destination name: it becomes a tag value, a directory name and a CLI argument.
NAME = re.compile(r"[a-z][a-z0-9_-]{0,62}")


class ConfigError(Exception):
    """The configuration cannot be used; the message says why, without any secret."""


class DestinationConfig(BaseModel):
    """One database points are written to."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["influx3", "influx2"]
    url: str = Field(min_length=1)
    database: str = Field(min_length=1)
    token_file: Path | None = None
    #: The organisation an InfluxDB 2.x bucket belongs to.
    org: str = ""


class SourceConfig(BaseModel):
    """One source, the adapter that reads it, and its destination."""

    model_config = ConfigDict(extra="forbid")

    kind: str = Field(min_length=1)
    destination: str
    enabled: bool = True
    #: A file holding the push-monitor URL called after this source's points are written.
    heartbeat_url_file: Path | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class HubConfig(BaseModel):
    """The whole file."""

    model_config = ConfigDict(extra="forbid")

    destinations: dict[str, DestinationConfig] = Field(default_factory=dict)
    sources: dict[str, SourceConfig] = Field(default_factory=dict)

    @field_validator("destinations", "sources")
    @classmethod
    def _names(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Every name must be usable as a tag value, a directory and an argument."""
        for name in value:
            if not NAME.fullmatch(name):
                raise ValueError(
                    f"{name!r} is not a valid name: lowercase letters, digits, '-' and '_', "
                    "starting with a letter"
                )
        return value

    @model_validator(mode="after")
    def _destinations_exist(self) -> HubConfig:
        """Every source must name a destination the file defines."""
        for name, source in self.sources.items():
            if source.destination not in self.destinations:
                raise ValueError(
                    f"source {name!r} names destination {source.destination!r}, "
                    "which is not defined"
                )
        return self


def load_config(path: Path) -> HubConfig:
    """Read and validate the configuration file."""
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ConfigError(f"no configuration file at {path}") from None
    except OSError as error:
        raise ConfigError(f"cannot read {path}: {error.strerror}") from None
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(f"{path} is not valid TOML: {error}") from None
    try:
        return HubConfig.model_validate(data)
    except ValidationError as error:
        raise ConfigError(f"{path}: {describe(error)}") from None


def describe(error: ValidationError) -> str:
    """A validation error as one line per problem, naming where each is, never its value."""
    return "; ".join(
        f"{'.'.join(str(part) for part in item['loc']) or '(top)'}: {item['msg']}"
        for item in error.errors()
    )


def read_secret(path: Path) -> str:
    """The stripped contents of a secret file, which must exist and hold something."""
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError as error:
        raise ConfigError(f"cannot read secret file {path}: {error.strerror}") from None
    if not value:
        raise ConfigError(f"secret file {path} is empty")
    return value

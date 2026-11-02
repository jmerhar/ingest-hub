"""Settings from the environment; everything about sources lives in the configuration file."""

from __future__ import annotations

from pathlib import Path

from jigkit.settings import AppSettings
from pydantic import Field

#: The configuration file inside the data directory, used when CONFIG_FILE is unset.
DEFAULT_CONFIG = Path("config") / "sources.toml"


class Settings(AppSettings):
    """The process's settings."""

    #: The TOML file of destinations and sources.
    config_file: Path | None = None
    #: The most each source's spool keeps on disk before dropping its oldest batches.
    spool_max_bytes: int = Field(256 * 1024 * 1024, ge=1024 * 1024)

    @property
    def resolved_config_file(self) -> Path:
        """The configuration file, defaulting to one inside the data directory."""
        return self.config_file or self.data_dir / DEFAULT_CONFIG

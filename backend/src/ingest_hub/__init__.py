"""ingest-hub: a config-driven service that collects data from outside sources into databases."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ingest-hub")
except PackageNotFoundError:  # pragma: no cover - only when run from a bare source tree
    __version__ = "0.0.0"

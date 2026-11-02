"""The ``ingest-hub`` command.

    ingest-hub run [--only SOURCE]           collect from every enabled source, or just one
    ingest-hub check [--only SOURCE]         validate the configuration and test each destination
    ingest-hub import-tuya-dump DIR --source SOURCE
                                             write a saved Tuya report-log dump
    ingest-hub compare --destination NAME OLD NEW [--hours H] [--tolerance T]
                                             compare a shadow table with the one it replaces

Settings come from the environment (DATA_DIR, CONFIG_FILE, LOG_LEVEL, SPOOL_MAX_BYTES); sources
and destinations from the configuration file.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import sys
from collections.abc import Coroutine, Sequence
from pathlib import Path
from typing import Any

import httpx2
from jigkit.influx import Influx3Sink, QueryError
from jigkit.logging import configure_logging
from jigkit.notify import AppriseSender
from pydantic import ValidationError

from ingest_hub import __version__
from ingest_hub.adapters import tuya
from ingest_hub.adapters.tuya.dump import DumpError, import_dump
from ingest_hub.alerts import AlertMonitor, read_urls
from ingest_hub.collect import Runtime
from ingest_hub.compare import compare
from ingest_hub.config import ConfigError, load_config
from ingest_hub.hub import make_sink, plan, source_params
from ingest_hub.settings import Settings

logger = logging.getLogger("ingest_hub")

LOG_FILE = Path("logs") / "ingest-hub.log"


async def _run(settings: Settings, only: str | None) -> int:
    """Run the runtime until SIGTERM or SIGINT."""
    config = load_config(settings.resolved_config_file)
    async with httpx2.AsyncClient() as client:
        built = plan(config, client, only)
        runtime = Runtime(
            built.specs,
            data_dir=settings.data_dir,
            spool_max_bytes=settings.spool_max_bytes,
            client=client,
        )
        names = ", ".join(spec.name for spec in built.specs)
        logger.info("ingest-hub %s collecting from %s", __version__, names)
        work = [runtime.run()]
        if config.alerts is not None:
            monitor = AlertMonitor(
                config.alerts, read_urls(config.alerts.urls_file), AppriseSender(), runtime.snapshot
            )
            work.append(monitor.run())
        await _until_signalled(_together(work))
    logger.info("stopped")
    return 0


async def _together(work: list[Coroutine[Any, Any, None]]) -> None:
    """Run coroutines side by side; cancelling this cancels them all."""
    await asyncio.gather(*work)


async def _until_signalled(work: Coroutine[Any, Any, None]) -> None:
    """Await ``work``, cancelling it cleanly on SIGTERM or SIGINT."""
    task = asyncio.ensure_future(work)
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, task.cancel)
    try:
        with contextlib.suppress(asyncio.CancelledError):
            await task
    finally:
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(signum)


async def _check(settings: Settings, only: str | None) -> int:
    """Validate the configuration and secrets, and ask each destination whether it would write."""
    config = load_config(settings.resolved_config_file)
    async with httpx2.AsyncClient() as client:
        built = plan(config, client, only)
        failed = False
        for name, sink in built.sinks.items():
            reason = await sink.check()
            print(f"destination {name}: {reason or 'ok'}")
            failed = failed or reason is not None
            await sink.aclose()
        for spec in built.specs:
            print(f"source {spec.name} ({spec.kind}) -> {spec.destination}: configured")
        if config.alerts is not None:
            print(f"alerts: {len(read_urls(config.alerts.urls_file))} destination URL(s)")
    return 1 if failed else 0


async def _import_tuya_dump(settings: Settings, directory: Path, source: str) -> int:
    """Write a Tuya dump to the destination of the named Tuya source."""
    config = load_config(settings.resolved_config_file)
    if source not in config.sources:
        raise ConfigError(f"no source named {source!r}")
    if config.sources[source].kind != "tuya":
        raise ConfigError(f"source {source!r} is not a Tuya source")
    params = source_params(config, source)
    assert isinstance(params, tuya.TuyaParams)
    sink = make_sink(config.destinations[config.sources[source].destination])
    async with httpx2.AsyncClient() as client:
        try:
            written = await import_dump(directory, params, tuya.make_client(params, client), sink)
        finally:
            await sink.aclose()
    print(f"imported {sum(written.values())} point(s) for {len(written)} device(s)")
    return 0


async def _compare(
    settings: Settings, destination: str, old: str, new: str, hours: float, tolerance: float
) -> int:
    """Compare table ``new`` with ``old`` in an InfluxDB 3 destination; 0 when they match."""
    config = load_config(settings.resolved_config_file)
    if destination not in config.destinations:
        raise ConfigError(f"no destination named {destination!r}")
    sink = make_sink(config.destinations[destination])
    if not isinstance(sink, Influx3Sink):
        await sink.aclose()
        raise ConfigError(f"destination {destination!r} is not InfluxDB 3")
    try:
        result = await compare(sink, old, new, hours=hours)
    except QueryError as error:
        raise ConfigError(f"cannot query {destination!r}: {error}") from None
    finally:
        await sink.aclose()
    print(result.report(tolerance))
    return 0 if result.ok(tolerance) else 1


def main(argv: Sequence[str] | None = None) -> int:
    """Parse the command line and run the command; configuration errors exit 2."""
    parser = argparse.ArgumentParser(prog="ingest-hub", description=__doc__.split("\n", 1)[0])
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "check"):
        command = commands.add_parser(name)
        command.add_argument("--only", metavar="SOURCE", help="just this source")
    dump = commands.add_parser("import-tuya-dump")
    dump.add_argument("directory", type=Path)
    dump.add_argument("--source", required=True, help="the Tuya source whose settings to use")
    comparing = commands.add_parser("compare")
    comparing.add_argument("--destination", required=True)
    comparing.add_argument("old", help="the table the old writer fills")
    comparing.add_argument("new", help="the shadow table the new source fills")
    comparing.add_argument("--hours", type=float, default=24.0, help="look back at most this far")
    comparing.add_argument(
        "--tolerance", type=float, default=0.01, help="largest relative difference of a mean"
    )
    args = parser.parse_args(argv)

    try:
        settings = Settings()
    except ValidationError as error:
        print(f"ingest-hub: invalid settings: {error}", file=sys.stderr)
        return 2
    log_file = settings.data_dir / LOG_FILE if args.command == "run" else None
    configure_logging(settings.log_level, log_file=log_file)
    try:
        if args.command == "run":
            return asyncio.run(_run(settings, args.only))
        if args.command == "check":
            return asyncio.run(_check(settings, args.only))
        if args.command == "compare":
            return asyncio.run(
                _compare(settings, args.destination, args.old, args.new, args.hours, args.tolerance)
            )
        return asyncio.run(_import_tuya_dump(settings, args.directory, args.source))
    except (ConfigError, DumpError) as error:
        print(f"ingest-hub: {error}", file=sys.stderr)
        return 2

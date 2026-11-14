"""The ``ingest-hub`` command.

    ingest-hub run [--only SOURCE]           collect from every enabled source, or just one
    ingest-hub check [--only SOURCE]         validate the configuration and test each destination
    ingest-hub import-tuya-dump DIR --source SOURCE
                                             write a saved Tuya report-log dump
    ingest-hub compare --destination NAME OLD NEW [--hours H] [--tolerance T]
                                             compare a shadow table with the one it replaces
    ingest-hub google-auth --source SOURCE [--port P] [--address URL]
                                             consent to a Google Health source reading your data

Settings come from the environment (DATA_DIR, CONFIG_FILE, LOG_LEVEL, SPOOL_MAX_BYTES); sources
and destinations from the configuration file.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
import sys
import time
from collections.abc import Coroutine, Sequence
from pathlib import Path
from typing import Any

import httpx2
from jigkit.influx import Influx3Sink, QueryError
from jigkit.logging import configure_logging
from jigkit.web import serve
from jigkit.web.server import PORT
from pydantic import ValidationError

from ingest_hub import __version__
from ingest_hub.adapters import tuya
from ingest_hub.adapters.google import health as google_health
from ingest_hub.adapters.google.api import ApiError, HealthApi
from ingest_hub.adapters.google.oauth import (
    AuthError,
    code_from_redirect,
    exchange,
    load_consent,
    missing_scopes,
    save_consent,
    save_token,
    start_consent,
)
from ingest_hub.adapters.tuya.dump import DumpError, import_dump
from ingest_hub.alerts import read_urls
from ingest_hub.compare import compare
from ingest_hub.config import ConfigError, load_config
from ingest_hub.hub import assemble, make_sink, plan, source_params
from ingest_hub.settings import Settings
from ingest_hub.web import ONLY_ENV

logger = logging.getLogger("ingest_hub")

LOG_FILE = Path("logs") / "ingest-hub.log"


async def _run(settings: Settings, only: str | None) -> int:
    """Run the runtime until SIGTERM or SIGINT."""
    config = load_config(settings.resolved_config_file)
    async with httpx2.AsyncClient() as client:
        assembly = assemble(
            config,
            client,
            data_dir=settings.data_dir,
            spool_max_bytes=settings.spool_max_bytes,
            only=only,
        )
        await _until_signalled(assembly.run())
    logger.info("stopped")
    return 0


def _serve(settings: Settings, only: str | None) -> int:
    """Run the sources inside the web application, served until SIGTERM or SIGINT."""
    load_config(settings.resolved_config_file)  # fail with a clear message before serving
    os.environ[ONLY_ENV] = only or ""
    serve("ingest_hub.web:create_app", settings, port=settings.web_port or PORT)
    return 0


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


async def _google_auth(settings: Settings, source: str, port: int, address: str | None) -> int:
    """Consent to a Google Health source in two runs: without ``address``, start a consent and show
    the address to open; with the address the browser was sent to, finish it and keep the refresh
    token in the source's state directory."""
    config = load_config(settings.resolved_config_file)
    if source not in config.sources:
        raise ConfigError(f"no source named {source!r}")
    if config.sources[source].kind != "google_health":
        raise ConfigError(f"source {source!r} is not a Google Health source")
    params = source_params(config, source)
    assert isinstance(params, google_health.GoogleHealthParams)
    credentials = google_health.credentials_of(params)
    state_dir = settings.data_dir / "state" / source
    if address is None:
        consent = start_consent(credentials, port)
        save_consent(state_dir, consent)
        print(
            "1. Open this address in a browser, signed in to the Google account whose data is "
            f"{params.person}'s:\n\n{consent.url}\n\n"
            "2. Allow every access it asks for.\n"
            "3. The browser then fails to open a page on 127.0.0.1. Copy that page's whole address "
            "from the address bar and run:\n\n"
            f"   ingest-hub google-auth --source {source} --address '<the address>'"
        )
        return 0
    consent = load_consent(state_dir)
    code = code_from_redirect(address, consent.state)
    async with httpx2.AsyncClient() as client:
        refresh_token, scopes = await exchange(client, credentials, consent, code, params.token_url)
        api = HealthApi(
            client, credentials, refresh_token, url=params.url, token_url=params.token_url
        )
        await api.identity()
    path = save_token(state_dir, refresh_token, scopes, time.time())
    print(f"Signed in: the source reads {params.person}'s data from its next poll ({path}).")
    if missing := missing_scopes(scopes):
        print(f"Not granted, so not read: {', '.join(missing)}. Consent again to grant them.")
    return 0


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
    auth = commands.add_parser("google-auth")
    auth.add_argument("--source", required=True, help="the Google Health source to consent to")
    auth.add_argument(
        "--port", type=int, default=8765, help="the 127.0.0.1 port Google redirects the browser to"
    )
    auth.add_argument("--address", help="the address the browser was sent to after consenting")
    args = parser.parse_args(argv)

    try:
        settings = Settings()
    except ValidationError as error:
        print(f"ingest-hub: invalid settings: {error}", file=sys.stderr)
        return 2
    log_file = settings.data_dir / LOG_FILE if args.command == "run" else None
    configure_logging(settings.log_level, log_file=log_file)
    try:
        if args.command == "run" and settings.web_port:
            return _serve(settings, args.only)
        if args.command == "run":
            return asyncio.run(_run(settings, args.only))
        if args.command == "check":
            return asyncio.run(_check(settings, args.only))
        if args.command == "compare":
            return asyncio.run(
                _compare(settings, args.destination, args.old, args.new, args.hours, args.tolerance)
            )
        if args.command == "google-auth":
            return asyncio.run(_google_auth(settings, args.source, args.port, args.address))
        return asyncio.run(_import_tuya_dump(settings, args.directory, args.source))
    except (ConfigError, DumpError, AuthError, ApiError) as error:
        print(f"ingest-hub: {error}", file=sys.stderr)
        return 2

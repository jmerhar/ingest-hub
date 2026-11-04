"""Alerts: watching every source's status and notifying people when one needs attention.

Checked once a minute, per source:

- **failing**: the source's latest news is an error and it has not completed a round of work for
  ``failing_minutes`` (a source that polls rarely is not failing between polls);
- **rejected**: its destination refused a batch for good since the last check;
- **backlog**: its spool's oldest batch has waited longer than ``backlog_minutes``, so its
  destination has been unreachable or refusing writes for that long;
- **stalled**: one of its streams' progress is more than ``progress_hours`` behind now (or that
  source's entry in ``progress_hours_by_source``), so a consumer waiting for that stream to be
  complete is held up.

A problem is announced when it starts, repeated every ``repeat_hours`` while it lasts, and
followed by a recovery message when it clears. Rejections are events rather than states, so each
check that finds new ones announces them, at most once per ``repeat_hours``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)

CHECK_INTERVAL_SECONDS = 60.0
TITLE = "ingest-hub"


class AlertsConfig(BaseModel):
    """The ``[alerts]`` section of the configuration file."""

    model_config = ConfigDict(extra="forbid")

    #: A file holding Apprise URLs, one per line; blank lines and ``#`` comments are ignored.
    urls_file: Path
    failing_minutes: float = Field(30, gt=0)
    backlog_minutes: float = Field(15, gt=0)
    progress_hours: float = Field(3, gt=0)
    #: ``progress_hours`` for particular sources, by name, such as one whose progress trails by
    #: design.
    progress_hours_by_source: dict[str, float] = Field(default_factory=dict)
    repeat_hours: float = Field(6, gt=0)


class Sender(Protocol):
    """Delivers a message (``jigkit.notify.AppriseSender`` in practice)."""

    async def send(self, urls: list[str], title: str, body: str) -> bool:
        """Return whether at least one destination accepted it."""


def read_urls(path: Path) -> list[str]:
    """The Apprise URLs in a file, which must name at least one."""
    from ingest_hub.config import ConfigError, read_secret

    urls = [
        line.strip()
        for line in read_secret(path).splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    if not urls:
        raise ConfigError(f"{path} names no Apprise URL")
    return urls


@dataclass
class _Open:
    """A problem that has been announced and not yet cleared."""

    detail: str
    announced_at: float


def _problems(
    source: dict[str, Any], config: AlertsConfig, now: float, started: float
) -> dict[str, str]:
    """The problems a source's status snapshot shows, by kind."""
    found: dict[str, str] = {}
    last = source.get("last_success")
    error_at = source.get("last_error_at")
    erring = error_at is not None and (last is None or error_at > last)
    quiet_for = now - (last if last is not None else started)
    if erring and quiet_for > config.failing_minutes * 60:
        since = "since it started" if last is None else f"for {quiet_for / 60:.0f} min"
        found["failing"] = (
            f"has not completed a round of work {since}: {source.get('last_error') or '?'}"
        )
    spool = source.get("spool") or {}
    oldest = spool.get("oldest_seconds")
    if oldest is not None and oldest > config.backlog_minutes * 60:
        found["backlog"] = (
            f"{spool.get('files', 0)} batch(es) waiting for its destination "
            f"for {oldest / 60:.0f} min"
        )
    limit = config.progress_hours_by_source.get(str(source.get("name")), config.progress_hours)
    behind = {
        stream: (now * 1000 - until) / 3_600_000
        for stream, until in (source.get("progress") or {}).items()
        if now * 1000 - until > limit * 3_600_000
    }
    if behind:
        worst = max(behind.values())
        streams = ", ".join(sorted(behind)[:5]) + (" …" if len(behind) > 5 else "")
        found["stalled"] = (
            f"{len(behind)} stream(s) not complete for the last {worst:.1f} h or more: {streams}"
        )
    return found


class AlertMonitor:
    """Turns status snapshots into notifications."""

    def __init__(
        self,
        config: AlertsConfig,
        urls: list[str],
        sender: Sender,
        snapshot: Callable[[], dict[str, Any]],
        *,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._config = config
        self._urls = urls
        self._sender = sender
        self._snapshot = snapshot
        self._clock = clock
        self._sleep = sleep
        self._open: dict[tuple[str, str], _Open] = {}
        self._rejected: dict[str, int] = {}
        self._rejected_announced: dict[str, float] = {}

    async def run(self) -> None:
        """Check for ever, once a minute. Cancel the task to stop it."""
        while True:
            try:
                await self.check()
            except Exception:
                logger.exception("checking for alerts failed")
            await self._sleep(CHECK_INTERVAL_SECONDS)

    async def check(self) -> None:
        """Compare the current status with what has been announced, and notify the changes."""
        now = self._clock()
        repeat = self._config.repeat_hours * 3600
        snapshot = self._snapshot()
        started = float(snapshot.get("started_at") or now)
        for source in snapshot.get("sources", []):
            name = str(source.get("name"))
            current = _problems(source, self._config, now, started)
            for kind, detail in current.items():
                key = (name, kind)
                known = self._open.get(key)
                due = known is None or now - known.announced_at >= repeat
                if due and await self._send(f"{name}: {kind}", f"{name} {detail}"):
                    self._open[key] = _Open(detail, now)
            for key in [k for k in self._open if k[0] == name and k[1] not in current]:
                await self._send(f"{name}: {key[1]} cleared", f"{name} is fine again ({key[1]}).")
                del self._open[key]
            await self._check_rejections(name, int(source.get("batches_rejected") or 0), now)

    async def _check_rejections(self, name: str, total: int, now: float) -> None:
        """Announce batches the destination refused since the last check."""
        seen = self._rejected.get(name)
        self._rejected[name] = total
        if seen is None or total <= seen:
            return
        last = self._rejected_announced.get(name)
        if last is not None and now - last < self._config.repeat_hours * 3600:
            return
        new = total - seen
        body = (
            f"{name}: its destination refused {new} batch(es) for good; they are kept in the "
            "spool's rejected/ folder and will not be retried"
        )
        if await self._send(f"{name}: rejected", body):
            self._rejected_announced[name] = now

    async def _send(self, subject: str, body: str) -> bool:
        """Send one alert, logging it either way."""
        logger.warning("alert: %s", body)
        try:
            sent = await self._sender.send(self._urls, f"{TITLE}: {subject}", body)
        except Exception:
            logger.exception("sending an alert failed")
            return False
        if not sent:
            logger.error("no alert destination accepted: %s", subject)
        return sent

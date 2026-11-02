"""Running sources side by side: supervision, delivery, heartbeats and the status file."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx2
from jigkit.delivery import Delivery
from jigkit.heartbeat import Heartbeat
from jigkit.sinks import Point, Sink
from jigkit.spool import Spool

from ingest_hub.collect.source import Source, SourceContext
from ingest_hub.collect.status import SourceStatus

logger = logging.getLogger(__name__)

#: The first pause before restarting a failed source, and the longest.
FIRST_BACKOFF_SECONDS = 5.0
MAX_BACKOFF_SECONDS = 900.0
#: A pause varies by up to this fraction either way, so sources that failed together do not all
#: retry in the same instant.
JITTER = 0.2
#: Heartbeat calls per monitor, at most one per this many seconds.
HEARTBEAT_INTERVAL_SECONDS = 60.0
#: How often the status file is rewritten.
STATUS_INTERVAL_SECONDS = 30.0
STATUS_FILE = "status.json"


@dataclass
class SourceSpec:
    """One source to run, and where its points go."""

    name: str
    kind: str
    source: Source
    destination: str
    sink: Sink
    heartbeat_url: str | None = None


class _Slot:
    """Everything that belongs to one running source."""

    def __init__(self, spec: SourceSpec, status: SourceStatus, spool: Spool) -> None:
        self.spec = spec
        self.status = status
        self.spool = spool
        self.heartbeat: Heartbeat | None = None
        self.delivery: Delivery | None = None
        # One batch at a time, so the delivery's counters before and after a submit describe that
        # submit alone (see Runtime._emit).
        self.lock = asyncio.Lock()
        self.backoff = FIRST_BACKOFF_SECONDS


class Runtime:
    """Runs every source in its own task until cancelled."""

    def __init__(
        self,
        specs: Sequence[SourceSpec],
        *,
        data_dir: Path,
        spool_max_bytes: int,
        client: httpx2.AsyncClient,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        status_interval: float = STATUS_INTERVAL_SECONDS,
    ) -> None:
        names = [spec.name for spec in specs]
        if len(set(names)) != len(names):
            raise ValueError("source names must be unique")
        self._data_dir = data_dir
        self._client = client
        self._clock = clock
        self._sleep = sleep
        self._status_interval = status_interval
        self._slots = [
            _Slot(
                spec,
                SourceStatus(spec.name, spec.kind, spec.destination, clock=clock),
                Spool(data_dir / "spool" / spec.name, spool_max_bytes),
            )
            for spec in specs
        ]
        self.started_at = clock()

    @property
    def statuses(self) -> list[SourceStatus]:
        """Each source's status, in the order the sources were given."""
        return [slot.status for slot in self._slots]

    def snapshot(self) -> dict[str, Any]:
        """The whole runtime's status as plain data."""
        return {
            "updated_at": self._clock(),
            "started_at": self.started_at,
            "sources": [slot.status.snapshot(slot.spool) for slot in self._slots],
        }

    async def run(self) -> None:
        """Run every source, its delivery and its heartbeat until cancelled, then close sinks."""
        tasks: list[asyncio.Task[None]] = []
        try:
            for slot in self._slots:
                tasks.extend(self._start(slot))
            tasks.append(asyncio.create_task(self._write_status_forever(), name="status"))
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for slot in self._slots:
                slot.status.state = "stopped"
            await self._close_sinks()
            self.write_status()

    def _start(self, slot: _Slot) -> list[asyncio.Task[None]]:
        """Wire up one source's delivery and heartbeat, and start its tasks."""
        spec, status = slot.spec, slot.status
        tasks = []
        if spec.heartbeat_url:
            slot.heartbeat = Heartbeat(
                self._client,
                spec.heartbeat_url,
                interval_seconds=HEARTBEAT_INTERVAL_SECONDS,
                sleep=self._sleep,
                on_result=status.record_heartbeat,
            )
            tasks.append(asyncio.create_task(slot.heartbeat.run(), name=f"{spec.name}:heartbeat"))
        heartbeat = slot.heartbeat
        slot.delivery = Delivery(
            spec.sink,
            slot.spool,
            events=status,
            sleep=self._sleep,
            on_written=lambda _points: heartbeat.beat() if heartbeat else None,
        )
        tasks.append(asyncio.create_task(slot.delivery.run(), name=f"{spec.name}:delivery"))
        tasks.append(asyncio.create_task(self._supervise(slot), name=f"{spec.name}:source"))
        return tasks

    async def _supervise(self, slot: _Slot) -> None:
        """Run a source, restarting it after a growing pause whenever it fails."""
        spec, status = slot.spec, slot.status
        state_dir = self._data_dir / "state" / spec.name
        state_dir.mkdir(parents=True, exist_ok=True)

        def succeeded() -> None:
            status.record_success()
            slot.backoff = FIRST_BACKOFF_SECONDS

        def problem(message: str) -> None:
            status.record_error(message)

        async def emit(points: list[Point]) -> bool:
            return await self._emit(slot, points)

        context = SourceContext(
            spec.name,
            state_dir,
            emit,
            on_success=succeeded,
            on_problem=problem,
            clock=self._clock,
            sleep=self._sleep,
        )
        while True:
            status.state = "running"
            try:
                await spec.source.run(context)
                message = "the source stopped on its own"
            except asyncio.CancelledError:
                raise
            except Exception as error:
                message = f"{type(error).__name__}: {error}"
            pause = slot.backoff * random.uniform(1 - JITTER, 1 + JITTER)
            logger.error("source %s failed (%s); restarting in %.0f s", spec.name, message, pause)
            status.record_error(message)
            status.restarts += 1
            status.state = "backing-off"
            slot.backoff = min(slot.backoff * 2, MAX_BACKOFF_SECONDS)
            await self._sleep(pause)

    async def _emit(self, slot: _Slot, points: list[Point]) -> bool:
        """Submit one batch, returning whether it was written or spooled.

        Batches of one source are submitted one at a time. The delivery writes a batch straight
        away only while its spool is empty, and its replay loop writes only while the spool holds
        something, so during this submit no other write of this source can change the counters:
        a rise in written or spooled batches is this batch's.
        """
        assert slot.delivery is not None
        status = slot.status
        async with slot.lock:
            written, spooled = status.batches_written, status.batches_spooled
            await slot.delivery.submit(points)
            status.points_emitted += len(points)
            return status.batches_written > written or status.batches_spooled > spooled

    async def _write_status_forever(self) -> None:
        """Rewrite the status file at a fixed interval; the health probe checks its age."""
        while True:
            try:
                self.write_status()
            except OSError:
                logger.exception("could not write the status file")
            await self._sleep(self._status_interval)

    def write_status(self) -> None:
        """Write the status file atomically."""
        path = self._data_dir / STATUS_FILE
        temporary = path.with_name(f".{path.name}.tmp")
        temporary.write_text(json.dumps(self.snapshot(), indent=2), encoding="utf-8")
        os.replace(temporary, path)

    async def _close_sinks(self) -> None:
        """Flush and close each distinct sink once."""
        seen: set[int] = set()
        for slot in self._slots:
            sink = slot.spec.sink
            if id(sink) in seen:
                continue
            seen.add(id(sink))
            with contextlib.suppress(Exception):
                await sink.flush()
            with contextlib.suppress(Exception):
                await sink.aclose()

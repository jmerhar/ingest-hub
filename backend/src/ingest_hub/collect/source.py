"""What a source is, and what the runtime gives it to work with."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from jigkit.sinks import Point, Precision

#: The table progress is written to, in the destination of the source that reports it.
PROGRESS_TABLE = "ingest_progress"


@dataclass(frozen=True)
class Progress:
    """How far one stream of a source is complete.

    ``stream`` is whatever the source keeps a position for: a device, a market, a topic.
    ``complete_until_ms`` (Unix milliseconds) promises that every point of that stream measured
    before it has been fetched and handed over with this batch or an earlier one, so a consumer
    can tell a stream that had nothing to report from one that has not been fetched yet.
    """

    stream: str
    complete_until_ms: int
    #: A human-readable name for the stream, for status pages; never written to the table.
    label: str | None = None


class Source(Protocol):
    """Something that produces points for as long as it runs."""

    async def run(self, context: SourceContext) -> None:
        """Produce points through ``context`` until cancelled.

        Raising ends this run; the runtime logs the error and starts the source again after its
        back-off. Returning is treated the same way, since a source is meant to run for ever.
        """


Emit = Callable[[list[Point]], Awaitable[bool]]


class SourceContext:
    """What a running source uses: emitting points, reporting health, keeping state, waiting."""

    def __init__(
        self,
        name: str,
        state_dir: Path,
        emit: Emit,
        *,
        on_success: Callable[[], None],
        on_problem: Callable[[str], None],
        on_progress: Callable[[Progress], None] = lambda _: None,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.name = name
        #: A directory for this source's own state, such as cursors, kept across restarts.
        self.state_dir = state_dir
        self._emit = emit
        self._on_success = on_success
        self._on_problem = on_problem
        self._on_progress = on_progress
        self.clock = clock
        self.sleep = sleep

    def now_ms(self) -> int:
        """The current time in Unix milliseconds."""
        return int(self.clock() * 1000)

    async def emit(self, points: Sequence[Point], progress: Sequence[Progress] = ()) -> bool:
        """Hand points, and the progress they complete, to delivery as one batch.

        Returns whether the batch is safe: written, or spooled to be written later. False means
        the destination refused it for good or it could not be kept, so a source must not move
        its own cursor past those points.
        """
        now = self.now_ms()
        batch = list(points) + [
            Point(
                PROGRESS_TABLE,
                (("source", self.name), ("stream", p.stream)),
                now,
                {"complete_until": int(p.complete_until_ms)},
                precision=Precision.MILLISECONDS,
            )
            for p in progress
        ]
        if not batch:
            return True
        safe = await self._emit(batch)
        if safe:
            for p in progress:
                self._on_progress(p)
        return safe

    def restore_progress(self, progress: Sequence[Progress]) -> None:
        """Report progress delivered before a restart, such as cursors loaded from state, so a
        stream that stops advancing from now on is noticed even before its next batch."""
        for p in progress:
            self._on_progress(p)

    def succeeded(self) -> None:
        """Report a successful round of work, which also resets the source's back-off."""
        self._on_success()

    def problem(self, message: str) -> None:
        """Report a failure the source worked around, such as one device out of many."""
        self._on_problem(message)

"""What a source is, and what the runtime gives it to work with."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

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


@runtime_checkable
class AcceptsFiles(Protocol):
    """A source that imports files dropped into a folder, which can also be handed to it, such as
    through an upload. It may also set ``upload_max_bytes``, the largest file it is handed, and
    ``upload_help``, a sentence or two on what to hand it and how, shown with the upload button;
    text in backticks is shown as code."""

    #: The folder the source imports from.
    inbox: Path
    #: The name endings (such as ``.json``) of the files it imports.
    file_suffixes: tuple[str, ...]

    def wake(self) -> None:
        """Look at the inbox now, rather than at the next scan."""


@dataclass(frozen=True)
class SignInStep:
    """What a person does to sign a source in: open ``url`` in a browser and follow
    ``instructions``. ``code`` is a code that page shows or asks for, if any. With ``answer`` set,
    the person pastes something back, which ``answer`` describes, such as the address the browser
    ends up on; without, the source notices the sign-in by itself."""

    url: str
    instructions: str
    code: str | None = None
    answer: str | None = None


@runtime_checkable
class SignsIn(Protocol):
    """A source that needs a person to sign in through a browser, once, before it can run, such as
    to an OAuth service. It keeps what the sign-in gives in its state directory."""

    def signed_in(self, state_dir: Path) -> bool:
        """Whether a sign-in is kept, whether or not the service still accepts it."""

    async def start_sign_in(self, state_dir: Path) -> SignInStep:
        """Start a sign-in, replacing any started before, and say what the person does."""

    async def finish_sign_in(self, state_dir: Path, answer: str | None) -> str:
        """Finish the sign-in started last and keep it, returning a sentence on what it gave. For
        a step without ``answer``, this waits until the person has signed in; for one with, it
        takes the person's answer. Raises with a message the person can act on when it fails."""


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

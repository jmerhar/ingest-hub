"""Periodic jobs inside a source: what is due runs, a failure is reported and tried again.

A source that does its work on a timer lists its jobs and hands them to a :class:`Schedule`::

    schedule = Schedule(context, [
        Job("prices", every=6 * 3600, run=self.poll_prices),
        Job("account", every=15 * 60, run=self.poll_account),
    ])
    await schedule.run()

Every job is due at the start. A round runs each due job in the order given, then sleeps until
the next one is due. A job is next due ``every`` seconds after it started, so the interval holds
however long a round takes, and a job that overran runs again at once. A job can move its own
next run with :meth:`Schedule.due_in`, such as sooner while it has a backlog.

A job that raises is reported through the context (prefixed with its name when the schedule has
more than one) and logged, and runs again when next due: a failed round is not a reason to stop
the others or to restart the source. A job that reports its own problems and returns False
counts as failed without a further message. A round in which every job that ran succeeded is
reported as a success.

With ``wake``, setting the event runs the jobs marked ``on_wake`` at once, such as an import
after an upload, instead of at their next turn.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

from ingest_hub.collect.source import SourceContext

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Job:
    """Work a source does every ``every`` seconds. ``run`` returns False when it reported its own
    problem; anything else is a success."""

    name: str
    every: float
    run: Callable[[], Awaitable[bool | None]]
    #: Whether setting the schedule's ``wake`` event runs it at once.
    on_wake: bool = False


class Schedule:
    """Runs a source's jobs when they are due, for ever."""

    def __init__(
        self, context: SourceContext, jobs: Sequence[Job], *, wake: asyncio.Event | None = None
    ) -> None:
        names = [job.name for job in jobs]
        if not names or len(set(names)) != len(names):
            raise ValueError("a schedule needs jobs with distinct names")
        self._context = context
        self._jobs = list(jobs)
        self._wake = wake
        now = context.clock()
        self._due = dict.fromkeys(names, now)

    def due_in(self, name: str, seconds: float) -> None:
        """Run the job ``name`` next ``seconds`` from now, instead of when it would be due."""
        self._due[name] = self._context.clock() + seconds

    async def run(self) -> None:
        """Run whatever is due, then wait for the next job or a wake-up, for ever."""
        while True:
            await self.round()
            delay = max(0.0, min(self._due.values()) - self._context.clock())
            if self._wake is None:
                await self._context.sleep(delay)
            else:
                await wait_or_wake(self._context, self._wake, delay)

    async def round(self) -> None:
        """Run every job that is due, or woken, once."""
        if self._wake is not None and self._wake.is_set():
            # Cleared before the jobs run, so a wake-up while they do runs them again.
            self._wake.clear()
            for job in self._jobs:
                if job.on_wake:
                    self._due[job.name] = min(self._due[job.name], self._context.clock())
        ok, ran = True, False
        for job in self._jobs:
            started = self._context.clock()
            if started < self._due[job.name]:
                continue
            ran = True
            self._due[job.name] = started + job.every
            ok = await self._attempt(job) and ok
        if ran and ok:
            self._context.succeeded()

    async def _attempt(self, job: Job) -> bool:
        """Run a job, reporting a failure rather than raising it."""
        try:
            return await job.run() is not False
        except Exception as error:
            # A timeout's message is empty, so its type names it.
            message = str(error) or type(error).__name__
            if len(self._jobs) > 1:
                message = f"{job.name}: {message}"
            self._context.problem(message)
            logger.warning("%s: %s", self._context.name, message)
            return False


async def wait_or_wake(context: SourceContext, woken: asyncio.Event, seconds: float) -> None:
    """Wait ``seconds``, or until ``woken`` is set, such as by an upload."""
    timer = asyncio.ensure_future(context.sleep(seconds))
    wake = asyncio.ensure_future(woken.wait())
    done, pending = await asyncio.wait({timer, wake}, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    for task in done:
        task.result()


__all__ = ["Job", "Schedule", "wait_or_wake"]

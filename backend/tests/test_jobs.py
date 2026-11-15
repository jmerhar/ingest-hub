"""A source's periodic jobs: what is due runs, failures are reported, wake-ups run some early."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import pytest

from ingest_hub.collect import Job, Schedule, SourceContext, wait_or_wake


class Stop(Exception):
    """Ends a schedule's run in a test."""


class Harness:
    """A context whose clock moves only when the source sleeps, or when a job says so."""

    def __init__(self, tmp_path: Path, rounds: int = 3) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []
        self.problems: list[str] = []
        self.successes = 0
        self.ran: list[tuple[str, float]] = []
        self.rounds = rounds

        async def emit(_points) -> bool:
            return True

        async def sleep(seconds: float) -> None:
            self.sleeps.append(seconds)
            if len(self.sleeps) >= self.rounds:
                raise Stop
            # Waits a moment first, as a real sleep does, so a wake-up can cut it short.
            await asyncio.sleep(0.01)
            self.now += seconds

        def succeeded() -> None:
            self.successes += 1

        self.context = SourceContext(
            "src",
            tmp_path,
            emit,
            on_success=succeeded,
            on_problem=self.problems.append,
            clock=lambda: self.now,
            sleep=sleep,
        )

    def job(self, name: str, every: float, *, takes: float = 0.0, result=None, error=None):
        """A job that records when it ran, takes ``takes`` seconds, then fails or returns."""

        async def run():
            self.ran.append((name, self.now))
            self.now += takes
            if error is not None:
                raise error
            return result

        return run


async def run(schedule: Schedule) -> None:
    with pytest.raises(Stop):
        await schedule.run()


def test_a_schedule_needs_jobs_with_distinct_names(tmp_path):
    harness = Harness(tmp_path)
    with pytest.raises(ValueError, match="distinct names"):
        Schedule(harness.context, [])
    job = Job("a", 1, harness.job("a", 1))
    with pytest.raises(ValueError, match="distinct names"):
        Schedule(harness.context, [job, job])


async def test_every_job_runs_at_the_start_then_when_due(tmp_path):
    harness = Harness(tmp_path, rounds=4)
    await run(
        Schedule(
            harness.context,
            [Job("fast", 10, harness.job("fast", 10)), Job("slow", 25, harness.job("slow", 25))],
        )
    )
    assert harness.ran == [
        ("fast", 1000),
        ("slow", 1000),
        ("fast", 1010),
        ("fast", 1020),
        ("slow", 1025),
    ]
    assert harness.sleeps == [10, 10, 5, 5]
    assert harness.successes == 4 and harness.problems == []


async def test_the_interval_runs_from_the_start_and_an_overrun_runs_again_at_once(tmp_path):
    harness = Harness(tmp_path, rounds=3)
    takes = iter([4.0, 12.0, 1.0])

    async def work():
        harness.ran.append(("work", harness.now))
        harness.now += next(takes)

    await run(Schedule(harness.context, [Job("work", 10, work)]))
    # The overrun round started at 1022, so the next is due at 1032, 9 s after it ended.
    assert harness.sleeps == [6.0, 0.0, 9.0]
    assert harness.ran == [("work", 1000), ("work", 1010), ("work", 1022)]


async def test_a_failure_is_reported_and_the_other_jobs_still_run(tmp_path, caplog):
    caplog.set_level(logging.WARNING)
    harness = Harness(tmp_path, rounds=1)
    await run(
        Schedule(
            harness.context,
            [
                Job("broken", 10, harness.job("broken", 10, error=TimeoutError())),
                Job("fine", 10, harness.job("fine", 10)),
            ],
        )
    )
    assert [name for name, _ in harness.ran] == ["broken", "fine"]
    # A timeout's message is empty, so its type names it; with two jobs, the job is named.
    assert harness.problems == ["broken: TimeoutError"]
    assert harness.successes == 0
    assert "src: broken: TimeoutError" in caplog.text


async def test_a_single_job_s_failure_is_not_prefixed_and_a_later_round_succeeds(tmp_path):
    harness = Harness(tmp_path, rounds=2)
    outcomes = iter([ValueError("bad answer"), None])

    async def poll():
        if (outcome := next(outcomes)) is not None:
            raise outcome

    await run(Schedule(harness.context, [Job("poll", 60, poll)]))
    assert harness.problems == ["bad answer"]
    assert harness.successes == 1


async def test_a_job_that_reports_its_own_problem_returns_false(tmp_path):
    harness = Harness(tmp_path, rounds=2)
    await run(Schedule(harness.context, [Job("j", 5, harness.job("j", 5, result=False))]))
    assert harness.successes == 0 and harness.problems == []


async def test_a_job_can_move_its_next_run(tmp_path):
    harness = Harness(tmp_path, rounds=2)
    schedule: Schedule

    async def backlog():
        harness.ran.append(("backlog", harness.now))
        schedule.due_in("backlog", 3)

    schedule = Schedule(harness.context, [Job("backlog", 900, backlog)])
    await run(schedule)
    assert harness.sleeps == [3, 3]


async def test_a_wake_up_runs_the_jobs_marked_for_it_at_once(tmp_path):
    harness = Harness(tmp_path, rounds=2)
    woken = asyncio.Event()

    async def scan():
        harness.ran.append(("scan", harness.now))
        if len(harness.ran) == 1:
            # Woken while it runs, as by an upload: it runs again without waiting its turn.
            woken.set()

    jobs = [Job("scan", 600, scan, on_wake=True), Job("other", 600, harness.job("other", 600))]
    await run(Schedule(harness.context, jobs, wake=woken))
    assert harness.ran == [("scan", 1000), ("other", 1000), ("scan", 1000)]
    assert harness.sleeps == [600.0, 600.0]


async def test_wait_or_wake_returns_when_woken_or_when_the_time_is_up(tmp_path):
    harness = Harness(tmp_path, rounds=10)
    woken = asyncio.Event()
    woken.set()
    await wait_or_wake(harness.context, woken, 300)
    woken.clear()
    await wait_or_wake(harness.context, woken, 5)
    assert harness.now == 1005

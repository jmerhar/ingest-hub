"""The runtime: delivery outcomes, heartbeats, restarts, the status file and shutdown."""

from __future__ import annotations

import asyncio
import json
import logging

import httpx2
import pytest
from jigkit.sinks import Outcome, Point, Precision
from jigkit.testing.http import StubServer, serving

from ingest_hub.collect import PROGRESS_TABLE, Progress, Runtime, SourceContext, SourceSpec
from ingest_hub.collect import runtime as runtime_module


class FakeSink:
    """Answers writes with a settable outcome and keeps what it accepted."""

    def __init__(self, outcome: Outcome = Outcome.OK) -> None:
        self.outcome = outcome
        self.accepted: list[list[Point]] = []
        self.attempts = 0
        self.last_error: str | None = None
        self.closed = 0
        self.flushed = 0

    @property
    def configured(self) -> bool:
        return True

    async def write(self, points: list[Point]) -> Outcome:
        self.attempts += 1
        if self.outcome is Outcome.OK:
            self.accepted.append(points)
        return self.outcome

    async def check(self) -> str | None:
        return None

    async def flush(self) -> None:
        self.flushed += 1

    async def aclose(self) -> None:
        self.closed += 1


class Recorder:
    """A sleep that records each pause and yields briefly instead of waiting."""

    def __init__(self) -> None:
        self.pauses: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.pauses.append(seconds)
        await asyncio.sleep(0.001)


def point(value: float = 1.0) -> Point:
    return Point("t", (("k", "v"),), 1000, {"f": value}, precision=Precision.SECONDS)


class OneBatch:
    """Emits one batch with progress, keeps the answer, then idles."""

    def __init__(self) -> None:
        self.result: bool | None = None
        self.done = asyncio.Event()

    async def run(self, context: SourceContext) -> None:
        self.result = await context.emit([point()], [Progress("s1", 123)])
        context.succeeded()
        self.done.set()
        await asyncio.Event().wait()


async def wait_for(condition, timeout: float = 5.0) -> None:
    """Poll until ``condition()`` holds."""
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.005)


async def run_until(runtime: Runtime, condition) -> None:
    """Run the runtime until ``condition()`` holds, then stop it."""
    task = asyncio.create_task(runtime.run())
    try:
        await wait_for(condition)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


def make_runtime(tmp_path, specs, client, sleep=None) -> Runtime:
    return Runtime(
        specs,
        data_dir=tmp_path,
        spool_max_bytes=10_000_000,
        client=client,
        sleep=sleep or Recorder(),
        status_interval=0.01,
    )


async def test_a_written_batch_carries_progress_beats_and_shows_in_status(tmp_path):
    sink = FakeSink()
    source = OneBatch()
    async with serving(StubServer(status=200)) as monitor, httpx2.AsyncClient() as client:
        spec = SourceSpec("plugs", "tuya", source, "home", sink, f"{monitor.url}/push/TOKEN")
        runtime = make_runtime(tmp_path, [spec], client)
        # The heartbeat counts once its answer is back, a moment after the monitor sees it.
        await run_until(
            runtime, lambda: source.done.is_set() and runtime.statuses[0].heartbeat_ok is not None
        )

    assert source.result is True
    (batch,) = sink.accepted
    data, progress = batch
    assert data == point()
    assert progress.table == PROGRESS_TABLE
    assert progress.tags == (("source", "plugs"), ("stream", "s1"))
    assert progress.fields == {"complete_until": 123}
    assert isinstance(progress.fields["complete_until"], int)
    assert progress.precision is Precision.MILLISECONDS
    assert monitor.requests[0].path == "/push/TOKEN"

    status = json.loads((tmp_path / "status.json").read_text())
    (entry,) = status["sources"]
    assert entry["name"] == "plugs" and entry["state"] == "stopped"
    assert entry["batches_written"] == 1 and entry["points_emitted"] == 2
    assert entry["heartbeat_ok"] is True and entry["last_success"] is not None
    assert entry["spool"]["files"] == 0
    assert sink.closed == 1 and sink.flushed == 1


async def test_a_batch_the_sink_cannot_take_yet_is_spooled_then_replayed(tmp_path):
    sink = FakeSink(Outcome.RETRY)
    source = OneBatch()
    async with httpx2.AsyncClient() as client:
        runtime = make_runtime(tmp_path, [SourceSpec("p", "k", source, "d", sink)], client)
        task = asyncio.create_task(runtime.run())
        await wait_for(source.done.is_set)
        assert source.result is True
        assert runtime.statuses[0].batches_spooled == 1
        assert len(list((tmp_path / "spool" / "p" / "pending").iterdir())) == 1
        sink.outcome = Outcome.OK
        await wait_for(lambda: sink.accepted)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert runtime.statuses[0].batches_written == 1


async def test_a_batch_the_sink_refuses_reports_false(tmp_path):
    sink = FakeSink(Outcome.REJECT)
    source = OneBatch()
    async with httpx2.AsyncClient() as client:
        runtime = make_runtime(tmp_path, [SourceSpec("p", "k", source, "d", sink)], client)
        await run_until(runtime, source.done.is_set)
    assert source.result is False
    assert runtime.statuses[0].batches_rejected == 1


async def test_an_empty_emit_is_safe_and_writes_nothing(tmp_path):
    sink = FakeSink()
    results = []
    done = asyncio.Event()

    class Empty:
        async def run(self, context: SourceContext) -> None:
            results.append(await context.emit([]))
            done.set()
            await asyncio.Event().wait()

    async with httpx2.AsyncClient() as client:
        runtime = make_runtime(tmp_path, [SourceSpec("p", "k", Empty(), "d", sink)], client)
        await run_until(runtime, done.is_set)
    assert results == [True] and sink.attempts == 0


async def test_a_failing_source_restarts_after_growing_pauses_reset_by_success(
    tmp_path, monkeypatch, caplog
):
    monkeypatch.setattr(runtime_module.random, "uniform", lambda _a, _b: 1.0)
    sleep = Recorder()
    runs = []

    class Flaky:
        async def run(self, context: SourceContext) -> None:
            runs.append(len(runs))
            if len(runs) in (1, 2):
                raise RuntimeError(f"failure {len(runs)}")
            if len(runs) == 3:
                context.succeeded()
                context.problem("a device was out")
                return  # returning counts as stopping
            await asyncio.Event().wait()

    async with httpx2.AsyncClient() as client:
        runtime = make_runtime(
            tmp_path, [SourceSpec("p", "k", Flaky(), "d", FakeSink())], client, sleep
        )
        with caplog.at_level(logging.ERROR):
            await run_until(runtime, lambda: len(runs) >= 4)

    backoffs = [p for p in sleep.pauses if p >= runtime_module.FIRST_BACKOFF_SECONDS]
    # 5 s, then 10 s, then -- after a successful round -- 5 s again.
    assert backoffs[:3] == [5.0, 10.0, 5.0]
    status = runtime.statuses[0]
    assert status.restarts == 3
    assert status.last_error == "the source stopped on its own"
    assert "failure 1" in caplog.text


async def test_the_pause_never_exceeds_the_maximum(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_module.random, "uniform", lambda _a, _b: 1.0)
    sleep = Recorder()

    class Broken:
        async def run(self, context: SourceContext) -> None:
            raise RuntimeError("down")

    async with httpx2.AsyncClient() as client:
        runtime = make_runtime(
            tmp_path, [SourceSpec("p", "k", Broken(), "d", FakeSink())], client, sleep
        )
        await run_until(runtime, lambda: runtime.statuses[0].restarts >= 10)
    backoffs = [p for p in sleep.pauses if p >= runtime_module.FIRST_BACKOFF_SECONDS]
    assert max(backoffs) == runtime_module.MAX_BACKOFF_SECONDS


async def test_a_shared_sink_is_closed_once(tmp_path):
    sink = FakeSink()
    first, second = OneBatch(), OneBatch()
    async with httpx2.AsyncClient() as client:
        runtime = make_runtime(
            tmp_path,
            [SourceSpec("a", "k", first, "d", sink), SourceSpec("b", "k", second, "d", sink)],
            client,
        )
        await run_until(runtime, lambda: first.done.is_set() and second.done.is_set())
    assert sink.closed == 1
    assert [s.name for s in runtime.statuses] == ["a", "b"]


async def test_a_failure_to_close_a_sink_does_not_stop_shutdown(tmp_path):
    class Stubborn(FakeSink):
        async def aclose(self) -> None:
            raise OSError("already gone")

    source = OneBatch()
    async with httpx2.AsyncClient() as client:
        runtime = make_runtime(tmp_path, [SourceSpec("p", "k", source, "d", Stubborn())], client)
        await run_until(runtime, source.done.is_set)
    assert runtime.statuses[0].state == "stopped"


async def test_an_unwritable_status_file_is_logged_not_fatal(tmp_path, caplog):
    source = OneBatch()
    async with httpx2.AsyncClient() as client:
        runtime = make_runtime(tmp_path, [SourceSpec("p", "k", source, "d", FakeSink())], client)
        (tmp_path / "status.json").mkdir()  # os.replace cannot put a file over a directory
        with caplog.at_level(logging.ERROR):
            task = asyncio.create_task(runtime.run())
            await wait_for(lambda: "could not write the status file" in caplog.text)
            (tmp_path / "status.json").rmdir()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    assert source.done.is_set()


def test_source_names_must_be_unique(tmp_path):
    spec = SourceSpec("p", "k", OneBatch(), "d", FakeSink())
    with pytest.raises(ValueError, match="unique"):
        Runtime([spec, spec], data_dir=tmp_path, spool_max_bytes=1 << 20, client=None)  # type: ignore[arg-type]


async def test_progress_is_recorded_only_for_safe_batches_and_can_be_restored(tmp_path):
    sink = FakeSink()
    done = asyncio.Event()

    class Reporting:
        async def run(self, context: SourceContext) -> None:
            context.restore_progress([Progress("old", 5)])
            await context.emit([point()], [Progress("s1", 100)])
            sink.outcome = Outcome.REJECT
            await context.emit([point()], [Progress("s1", 200), Progress("s2", 300)])
            done.set()
            await asyncio.Event().wait()

    async with httpx2.AsyncClient() as client:
        runtime = make_runtime(tmp_path, [SourceSpec("p", "k", Reporting(), "d", sink)], client)
        await run_until(runtime, done.is_set)
    assert runtime.snapshot()["sources"][0]["progress"] == {"old": 5, "s1": 100}

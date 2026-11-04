"""Alerts: which status snapshots raise them, repeating, clearing, rejections and delivery."""

from __future__ import annotations

import logging

import pytest

from ingest_hub.alerts import AlertMonitor, AlertsConfig, read_urls
from ingest_hub.config import ConfigError

NOW = 1_800_000_000.0


class Sender:
    """Records messages; answers as told."""

    def __init__(self) -> None:
        self.sent: list[tuple[list[str], str, str]] = []
        self.accept = True
        self.raise_error = False

    async def send(self, urls: list[str], title: str, body: str) -> bool:
        if self.raise_error:
            raise RuntimeError("transport down")
        self.sent.append((urls, title, body))
        return self.accept


class Clock:
    def __init__(self) -> None:
        self.now = NOW

    def __call__(self) -> float:
        return self.now


def healthy(**overrides) -> dict:
    source = {
        "name": "plugs",
        "last_success": NOW - 60,
        "last_error": None,
        "last_error_at": None,
        "restarts": 0,
        "batches_rejected": 0,
        "progress": {"dev-a": int((NOW - 1800) * 1000)},
        "spool": {"files": 0, "oldest_seconds": None},
    }
    return {**source, **overrides}


class Harness:
    def __init__(self, **config) -> None:
        self.sources = [healthy()]
        self.clock = Clock()
        self.sender = Sender()
        settings = AlertsConfig.model_validate({"urls_file": "/x", **config})
        self.monitor = AlertMonitor(
            settings,
            ["json://example"],
            self.sender,
            lambda: {"started_at": NOW - 7200, "sources": self.sources},
            clock=self.clock,
        )

    def titles(self) -> list[str]:
        return [title for _, title, _ in self.sender.sent]


async def test_a_healthy_source_raises_nothing():
    harness = Harness()
    await harness.monitor.check()
    assert harness.sender.sent == []


async def test_a_failing_source_is_announced_repeated_and_cleared():
    harness = Harness(failing_minutes=30, repeat_hours=6)
    harness.sources = [
        healthy(last_success=NOW - 3600, last_error="TuyaError: boom", last_error_at=NOW - 10)
    ]
    await harness.monitor.check()
    assert harness.titles() == ["ingest-hub: plugs: failing"]
    urls, _, body = harness.sender.sent[0]
    assert urls == ["json://example"]
    assert "for 60 min: TuyaError: boom" in body
    harness.clock.now += 3600
    await harness.monitor.check()
    assert len(harness.sender.sent) == 1  # not repeated within six hours
    harness.clock.now += 6 * 3600
    await harness.monitor.check()
    assert harness.titles().count("ingest-hub: plugs: failing") == 2
    fresh = {"dev-a": int(harness.clock.now * 1000)}
    harness.sources = [healthy(last_success=harness.clock.now, progress=fresh)]
    await harness.monitor.check()
    assert "ingest-hub: plugs: failing cleared" in harness.titles()
    assert "ingest-hub: plugs: stalled cleared" in harness.titles()


async def test_a_rare_poller_between_polls_is_not_failing():
    harness = Harness()
    # Last success five hours ago, after its last error: a source that polls every six hours.
    harness.sources = [
        healthy(last_success=NOW - 5 * 3600, last_error="x", last_error_at=NOW - 6 * 3600)
    ]
    await harness.monitor.check()
    assert harness.sender.sent == []


async def test_a_source_that_never_succeeded_counts_from_the_start():
    harness = Harness()
    harness.sources = [healthy(last_success=None, last_error="refused", last_error_at=NOW - 5)]
    await harness.monitor.check()
    assert "since it started" in harness.sender.sent[0][2]


async def test_a_backlog_and_stalled_progress_are_announced():
    harness = Harness(backlog_minutes=15, progress_hours=3)
    far = int((NOW - 5 * 3600) * 1000)
    harness.sources = [
        healthy(
            spool={"files": 4, "oldest_seconds": 1200},
            progress={f"dev-{n}": far for n in range(7)} | {"fresh": int(NOW * 1000)},
        )
    ]
    await harness.monitor.check()
    bodies = {title: body for _, title, body in harness.sender.sent}
    assert (
        "4 batch(es) waiting for its destination for 20 min" in bodies["ingest-hub: plugs: backlog"]
    )
    stalled = bodies["ingest-hub: plugs: stalled"]
    assert "7 stream(s) not complete for the last 5.0 h" in stalled
    assert stalled.endswith("dev-0, dev-1, dev-2, dev-3, dev-4 …")


async def test_rejections_are_announced_once_per_repeat_window():
    harness = Harness(repeat_hours=6)
    await harness.monitor.check()  # the first look only records the count
    harness.sources = [healthy(batches_rejected=2)]
    await harness.monitor.check()
    assert harness.titles() == ["ingest-hub: plugs: rejected"]
    assert "refused 2 batch(es)" in harness.sender.sent[0][2]
    harness.sources = [healthy(batches_rejected=3)]
    harness.clock.now += 60
    await harness.monitor.check()
    assert len(harness.sender.sent) == 1
    harness.sources = [healthy(batches_rejected=5)]
    harness.clock.now += 6 * 3600
    await harness.monitor.check()
    assert "refused 2 batch(es)" in harness.sender.sent[-1][2]


async def test_a_rejection_alert_nobody_accepted_is_sent_at_the_next_rejection():
    harness = Harness()
    await harness.monitor.check()
    harness.sender.accept = False
    harness.sources = [healthy(batches_rejected=1)]
    await harness.monitor.check()
    harness.sender.accept = True
    harness.sources = [healthy(batches_rejected=2)]
    harness.clock.now += 60
    await harness.monitor.check()
    assert harness.titles().count("ingest-hub: plugs: rejected") == 2


async def test_an_alert_nobody_accepted_is_tried_again(caplog):
    harness = Harness()
    harness.sender.accept = False
    harness.sources = [healthy(spool={"files": 1, "oldest_seconds": 3600})]
    with caplog.at_level(logging.ERROR):
        await harness.monitor.check()
        await harness.monitor.check()
    assert len(harness.sender.sent) == 2
    assert "no alert destination accepted" in caplog.text
    harness.sender.raise_error = True
    with caplog.at_level(logging.ERROR):
        await harness.monitor.check()
    assert "sending an alert failed" in caplog.text


async def test_run_survives_a_failing_check(caplog):
    calls = []

    async def sleep(seconds: float) -> None:
        calls.append(seconds)
        if len(calls) == 2:
            raise SystemExit

    def broken() -> dict:
        raise ValueError("bad snapshot")

    monitor = AlertMonitor(
        AlertsConfig(urls_file="/x"), ["json://x"], Sender(), broken, sleep=sleep
    )
    with caplog.at_level(logging.ERROR), pytest.raises(SystemExit):
        await monitor.run()
    assert calls == [60.0, 60.0] and "checking for alerts failed" in caplog.text


def test_read_urls(tmp_path):
    path = tmp_path / "urls"
    path.write_text("# alerts\n\njson://a\n  tgram://b  \n")
    assert read_urls(path) == ["json://a", "tgram://b"]
    path.write_text("# only a comment\n")
    with pytest.raises(ConfigError, match="names no Apprise URL"):
        read_urls(path)


async def test_a_source_can_have_its_own_stall_threshold():
    harness = Harness(progress_hours=3, progress_hours_by_source={"plugs": 6})
    harness.sources = [healthy(progress={"dev-a": int((NOW - 4 * 3600) * 1000)})]
    await harness.monitor.check()
    assert harness.sender.sent == []
    harness.sources = [healthy(progress={"dev-a": int((NOW - 7 * 3600) * 1000)})]
    await harness.monitor.check()
    assert harness.titles() == ["ingest-hub: plugs: stalled"]

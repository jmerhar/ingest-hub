"""The stays source, against a stand-in feed of synthetic stays."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx2
import pytest
from jigkit.sinks import Point
from jigkit.testing.http import Received, StubServer, serving
from pydantic import ValidationError

from ingest_hub.adapters import ADAPTERS
from ingest_hub.adapters.stays import STATE_FILE, StaysParams, StaysSource
from ingest_hub.collect import SourceContext

ZONE = "Europe/Amsterdam"
# 18 Nov 2026, 12:00 in Amsterdam.
NOW = dt.datetime(2026, 11, 18, 11, 0, tzinfo=dt.UTC).timestamp()
TOKEN = "synthetic-token"


def ms(day: str, hour: int) -> int:
    """A local day and hour in Amsterdam as Unix milliseconds."""
    moment = dt.datetime.fromisoformat(day).replace(hour=hour, tzinfo=ZoneInfo(ZONE))
    return int(moment.timestamp() * 1000)


def stay(stay_id: str, check_in: str, check_out: str, **extra) -> dict:
    return {
        "id": stay_id,
        "check_in": check_in,
        "check_out": check_out,
        "source": "direct",
        "status": "booked",
        **extra,
    }


class Feed:
    """Answers like a stays feed, checking the token."""

    def __init__(self) -> None:
        self.stays: list[dict] = []
        self.since: str | None = "2026-10-19"
        self.answer: tuple[int, str] | None = None
        self.requests: list[Received] = []

    def __call__(self, request: Received) -> tuple[int, str]:
        self.requests.append(request)
        if self.answer:
            return self.answer
        if request.query.get("token") != TOKEN:
            return 403, json.dumps({"error": "Forbidden"})
        start = request.query.get("since", self.since)
        return 200, json.dumps({"generated_at": "x", "from": start, "stays": self.stays})


class Harness:
    def __init__(self, tmp_path: Path) -> None:
        self.batches: list[list[Point]] = []
        self.progress: list = []
        self.problems: list[str] = []
        self.successes = 0
        self.sleeps: list[float] = []
        self.accept = True
        self.stop_at = 10_000
        self.now = NOW

        async def emit(points):
            self.batches.append([p for p in points if p.table == "stays"])
            self.progress.extend(dict(p.tags)["stream"] for p in points if p.table != "stays")
            return self.accept

        async def sleep(seconds):
            self.sleeps.append(seconds)
            self.now += seconds
            if len([s for s in self.sleeps if s > 60]) >= self.stop_at:
                raise Stop

        def succeeded():
            self.successes += 1

        self.context = SourceContext(
            "rental",
            tmp_path,
            emit,
            on_success=succeeded,
            on_problem=self.problems.append,
            clock=lambda: self.now,
            sleep=sleep,
        )

    def last(self) -> list[Point]:
        return self.batches[-1]


class Stop(Exception):
    pass


def params(url: str, **values) -> StaysParams:
    return StaysParams.model_validate({"url": url, "token_file": "/x", "timezone": ZONE, **values})


async def poll(tmp_path, feed: Feed, harness: Harness | None = None, **values) -> Harness:
    harness = harness or Harness(tmp_path)
    async with serving(StubServer(respond=feed)) as server, httpx2.AsyncClient() as client:
        source = StaysSource(params(server.url + "/api/stays", **values), client, TOKEN)
        await source.poll(harness.context)
    return harness


def by_stay(points: list[Point]) -> dict[str, list[dict]]:
    found: dict[str, list[dict]] = {}
    for point in points:
        found.setdefault(dict(point.tags)["stay"], []).append(
            {"time": point.timestamp, **point.fields}
        )
    return found


async def test_each_stay_is_written_at_its_check_in_moment(tmp_path):
    feed = Feed()
    feed.stays = [
        stay("a1", "2026-11-16", "2026-11-22", nights=6, source="airbnb"),
        stay("b2", "2026-11-22", "2026-11-24", status="manual", source="manual"),
    ]
    harness = await poll(tmp_path, feed)

    assert feed.requests[0].query == {"token": TOKEN, "since": "2025-11-18"}
    points = harness.last()
    assert {p.table for p in points} == {"stays"}
    assert by_stay(points) == {
        "a1": [
            {
                "time": ms("2026-11-16", 16),
                "start": ms("2026-11-16", 16),
                "end": ms("2026-11-22", 11),
                "nights": 6,
                "check_in": "2026-11-16",
                "check_out": "2026-11-22",
                "source": "airbnb",
                "status": "booked",
            }
        ],
        "b2": [
            {
                "time": ms("2026-11-22", 16),
                "start": ms("2026-11-22", 16),
                "end": ms("2026-11-24", 11),
                "nights": 2,
                "check_in": "2026-11-22",
                "check_out": "2026-11-24",
                "source": "manual",
                "status": "manual",
            }
        ],
    }
    assert harness.progress == ["stays"]
    assert set(json.loads((tmp_path / STATE_FILE).read_text())["stays"]) == {"a1", "b2"}


async def test_later_reads_ask_the_window_and_write_only_what_changed(tmp_path):
    feed = Feed()
    feed.stays = [stay("a1", "2026-11-16", "2026-11-22"), stay("b2", "2026-11-25", "2026-11-27")]
    harness = await poll(tmp_path, feed)
    await poll(tmp_path, feed, harness, window_days=7)

    assert feed.requests[1].query["since"] == "2026-11-11"
    assert harness.last() == []
    feed.stays[1] = stay("b2", "2026-11-25", "2026-11-28", status="confirmed")
    await poll(tmp_path, feed, harness)
    written = by_stay(harness.last())
    assert list(written) == ["b2"]
    assert [(p["check_out"], p["status"]) for p in written["b2"]] == [("2026-11-28", "confirmed")]


async def test_a_stay_whose_check_in_moves_leaves_its_old_point_marked_moved(tmp_path):
    feed = Feed()
    feed.stays = [stay("a1", "2026-11-20", "2026-11-22")]
    harness = await poll(tmp_path, feed)
    feed.stays = [stay("a1", "2026-11-21", "2026-11-23")]
    await poll(tmp_path, feed, harness)

    written = by_stay(harness.last())["a1"]
    assert [(p["time"], p["status"]) for p in written] == [
        (ms("2026-11-20", 16), "moved"),
        (ms("2026-11-21", 16), "booked"),
    ]
    # It is never marked cancelled, however long it stays away from the feed.
    feed.stays = []
    await poll(tmp_path, feed, harness)
    assert [(p["time"], p["status"]) for p in by_stay(harness.last())["a1"]] == [
        (ms("2026-11-21", 16), "cancelled")
    ]
    await poll(tmp_path, feed, harness)
    assert harness.last() == []


async def test_a_stay_gone_from_a_read_covering_it_is_cancelled(tmp_path):
    feed = Feed()
    feed.stays = [stay("old", "2026-10-01", "2026-10-05"), stay("new", "2026-12-01", "2026-12-03")]
    harness = await poll(tmp_path, feed)
    feed.stays = []
    feed.since = "2026-10-19"
    await poll(tmp_path, feed, harness, since_param="")

    assert "since" not in feed.requests[1].query
    # The October stay ended before the read's window, so its absence says nothing.
    assert {k: [p["status"] for p in v] for k, v in by_stay(harness.last()).items()} == {
        "new": ["cancelled"]
    }


async def test_times_of_day_can_cover_the_whole_check_in_and_check_out_days(tmp_path):
    feed = Feed()
    feed.stays = [stay("a1", "2026-11-20", "2026-11-22")]
    harness = await poll(tmp_path, feed, check_in_time="00:00", check_out_time="24:00")
    [written] = by_stay(harness.last())["a1"]
    assert (written["start"], written["end"]) == (ms("2026-11-20", 0), ms("2026-11-23", 0))
    with pytest.raises(ValidationError):
        StaysParams(url="https://x.test", check_out_time="25:00")


async def test_a_known_stay_that_turns_unreadable_is_kept_as_it_was(tmp_path):
    feed = Feed()
    feed.stays = [stay("a1", "2026-11-20", "2026-11-22")]
    harness = await poll(tmp_path, feed)
    feed.stays = [stay("a1", "2026-11-20", "garbled"), "not a stay"]
    await poll(tmp_path, feed, harness)
    assert harness.last() == [] and len(harness.problems) == 2


async def test_an_unreadable_stay_is_reported_and_the_rest_written(tmp_path):
    feed = Feed()
    feed.stays = [
        {"id": "x"},
        stay("bad", "someday", "2026-11-22"),
        stay("back", "2026-11-22", "2026-11-20"),
        stay("ok", "2026-11-20", "2026-11-22"),
    ]
    harness = await poll(tmp_path, feed)
    assert set(by_stay(harness.last())) == {"ok"}
    assert harness.problems == [
        "a stay the feed lists cannot be read: 'check_in'",
        "a stay the feed lists cannot be read: check_in 'someday' is not a date",
        "a stay the feed lists cannot be read: "
        "check_out 2026-11-20 is not after check_in 2026-11-22",
    ]


@pytest.mark.parametrize(
    ("answer", "message"),
    [
        ((403, json.dumps({"error": "Forbidden"})), "HTTP 403 Forbidden"),
        ((500, "<html>"), "HTTP 500$"),
        ((200, "[]"), "no list of stays"),
        ((200, json.dumps({"from": "nope", "stays": []})), "from 'nope' is not a date"),
    ],
)
async def test_a_bad_answer_is_an_error_and_writes_nothing(tmp_path, answer, message):
    feed = Feed()
    feed.answer = answer
    harness = Harness(tmp_path)
    with pytest.raises(Exception, match=message):
        await poll(tmp_path, feed, harness)
    assert harness.batches == [] and not (tmp_path / STATE_FILE).exists()


async def test_a_refused_batch_is_an_error_and_keeps_the_old_state(tmp_path):
    feed = Feed()
    feed.stays = [stay("a1", "2026-11-20", "2026-11-22")]
    harness = Harness(tmp_path)
    harness.accept = False
    with pytest.raises(Exception, match="a batch was refused"):
        await poll(tmp_path, feed, harness)
    assert not (tmp_path / STATE_FILE).exists()


async def test_the_state_file_is_checked(tmp_path):
    (tmp_path / STATE_FILE).write_text("[]")
    feed = Feed()
    await poll(tmp_path, feed)
    # A state in the wrong shape is a state with no stays: the window, not the history.
    assert feed.requests[0].query["since"] == "2026-10-19"
    (tmp_path / STATE_FILE).write_text("{oops")
    with pytest.raises(Exception, match=f"cannot read {STATE_FILE}"):
        await poll(tmp_path, feed)


async def test_run_polls_on_schedule_and_reports_a_failure(tmp_path):
    feed = Feed()
    harness = Harness(tmp_path)
    harness.stop_at = 2
    async with serving(StubServer(respond=feed)) as server, httpx2.AsyncClient() as client:
        url = server.url + "/api/stays"
        with pytest.raises(Stop):
            await StaysSource(params(url), client, "wrong").run(harness.context)
        assert harness.problems == ["the feed answered HTTP 403 Forbidden"] * 2
        harness.stop_at, harness.sleeps = 1, []
        with pytest.raises(Stop):
            await StaysSource(params(url), client, TOKEN).run(harness.context)
    assert harness.successes == 1


class Unreachable:
    """A client whose every request times out, as httpx2 reports it: with an empty message."""

    async def get(self, *args, **kwargs):
        raise httpx2.ReadTimeout("")


async def test_an_unreachable_feed_is_reported_by_the_error_type(tmp_path):
    source = StaysSource(params("http://feed.invalid"), Unreachable(), None)  # type: ignore[arg-type]
    with pytest.raises(Exception, match="cannot reach the feed: ReadTimeout"):
        await source.poll(Harness(tmp_path).context)


async def test_a_feed_without_a_token_is_asked_without_one(tmp_path):
    feed = Feed()
    async with serving(StubServer(respond=feed)) as server, httpx2.AsyncClient() as client:
        source = ADAPTERS["stays"].build(
            StaysParams(url=server.url + "/api/stays"), client, "rental"
        )
        with pytest.raises(Exception, match="HTTP 403"):
            await source.poll(Harness(tmp_path).context)
    assert "token" not in feed.requests[0].query


def test_params_and_registration(tmp_path):
    (tmp_path / "token").write_text(TOKEN + "\n")
    adapter = ADAPTERS["stays"]
    built = adapter.build(
        adapter.params.model_validate(
            {"url": "https://x.test", "token_file": str(tmp_path / "token")}
        ),
        None,
        "rental",
    )
    assert isinstance(built, StaysSource) and built._token == TOKEN
    with pytest.raises(ValidationError):
        StaysParams(url="https://x.test", timezone="Mars/Olympus")
    with pytest.raises(ValidationError):
        StaysParams(url="https://x.test", table="no-dashes")

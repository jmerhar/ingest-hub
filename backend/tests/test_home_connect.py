"""Following Home Connect appliances, and signing in to Home Connect, against a stand-in server."""

from __future__ import annotations

import json
import stat
from pathlib import Path

import httpx2
import pytest
from jigkit.sinks import Point
from jigkit.testing.http import Received, StubServer, serving
from pydantic import ValidationError

from ingest_hub import cli
from ingest_hub.adapters import ADAPTERS
from ingest_hub.adapters.home_connect import api, auth, build, source, values
from ingest_hub.adapters.home_connect.api import Appliance, Event, HomeConnectApi
from ingest_hub.adapters.home_connect.auth import DeviceLogin, Tokens
from ingest_hub.adapters.home_connect.source import HomeConnectParams
from ingest_hub.collect import Progress, SourceContext
from ingest_hub.config import ConfigError
from ingest_hub.settings import Settings

CLIENT_ID = "test-client"
HA = "BRAND-DISHWASHER-0000"
OVEN = "BRAND-OVEN-0001"
#: Noon on 15 Nov 2026, UTC.
NOW = 1794744000.0


def sse(*events: tuple[str, dict | None]) -> str:
    """An event stream's body: each event's kind and JSON data, then a blank line."""
    out = []
    for kind, data in events:
        out.append(f"event:{kind}\ndata:{json.dumps(data) if data is not None else ''}\n")
        if data and data.get("haId"):
            out.append(f"id:{data['haId']}\n")
        out.append("\n")
    return "".join(out)


def item(key: str, value, timestamp: float | None = NOW) -> dict:
    found = {"key": key, "value": value}
    if timestamp is not None:
        found["timestamp"] = timestamp
    return found


class FakeHomeConnect:
    """Home Connect's OAuth endpoints and API, enough for the source and the login.

    ``streams`` are the event streams' bodies, one per connection; once they run out, a stream
    answers ``stream_status`` with nothing. ``answers`` overrides a path's answer.
    """

    def __init__(self) -> None:
        self.appliances = [
            {"haId": HA, "type": "Dishwasher", "brand": "Brand", "vib": "DW1", "connected": True}
        ]
        self.streams: list[str] = []
        self.stream_status = 200
        self.answers: dict[str, tuple[int, str]] = {}
        #: Answers given once, before the path answers normally.
        self.once: dict[str, tuple[int, str]] = {}
        self.token_answers: list[tuple[int, dict]] = []
        self.issued = 0

    def __call__(self, received: Received) -> tuple[int, str]:
        if received.path in self.once:
            return self.once.pop(received.path)
        if received.path in self.answers:
            return self.answers[received.path]
        if received.path == "/security/oauth/device_authorization":
            return 200, json.dumps(
                {
                    "device_code": "dev-code",
                    "user_code": "ABCD-1234",
                    "verification_uri": "https://example.test/verify",
                    "verification_uri_complete": "https://example.test/verify?user_code=ABCD-1234",
                    "interval": 0,
                    "expires_in": 600,
                }
            )
        if received.path == "/security/oauth/token":
            if self.token_answers:
                status, body = self.token_answers.pop(0)
                return status, json.dumps(body)
            self.issued += 1
            return 200, json.dumps(
                {"access_token": f"access-{self.issued}", "expires_in": 86400, "scope": auth.SCOPES}
            )
        if received.headers.get("authorization", "").removeprefix("Bearer ") == "expired":
            return 401, json.dumps({"error": {"key": "invalid_token"}})
        if received.path == "/api/homeappliances":
            return 200, json.dumps({"data": {"homeappliances": self.appliances}})
        if received.path == "/api/homeappliances/events":
            if self.streams:
                return 200, self.streams.pop(0)
            return self.stream_status, ""
        if received.path.endswith("/status"):
            return 200, json.dumps(
                {
                    "data": {
                        "status": [
                            item(
                                "BSH.Common.Status.DoorState", "BSH.Common.EnumType.DoorState.Open"
                            ),
                            item("BSH.Common.Status.RemoteControlActive", True),
                        ]
                    }
                }
            )
        if received.path.endswith("/settings"):
            return 200, json.dumps(
                {
                    "data": {
                        "settings": [
                            item(
                                "BSH.Common.Setting.PowerState",
                                "BSH.Common.EnumType.PowerState.Off",
                            )
                        ]
                    }
                }
            )
        if received.path.endswith("/programs/selected"):
            return 200, json.dumps(
                {
                    "data": {
                        "key": "Dishcare.Dishwasher.Program.Eco50",
                        "options": [
                            item("BSH.Common.Option.EnergyForecast", 52),
                            item("Dishcare.Dishwasher.Option.HalfLoad", False),
                        ],
                    }
                }
            )
        if received.path.endswith("/programs/active"):
            return 404, json.dumps({"error": {"key": "SDK.Error.NoProgramActive"}})
        return 500, "unexpected"


class Stop(Exception):
    """Ends a source's run loop in a test."""


class Harness:
    def __init__(self, tmp_path: Path, server: StubServer, *, signed_in: bool = True, **overrides):
        (tmp_path / "secrets").mkdir(parents=True, exist_ok=True)
        (tmp_path / "secrets" / "client_id").write_text(CLIENT_ID + "\n")
        self.params = HomeConnectParams.model_validate(
            {
                "client_id_file": str(tmp_path / "secrets" / "client_id"),
                "url": server.url,
                **overrides,
            }
        )
        self.state = tmp_path / "state"
        if signed_in:
            auth.save_tokens(self.state, Tokens("refresh-1", "access-0", NOW + 86400))
        self.now = NOW
        self.batches: list[list[Point]] = []
        self.progress: list[list[Progress]] = []
        self.accept = True
        self.problems: list[str] = []
        self.successes = 0
        self.sleeps: list[float] = []
        #: Sleeps of this length end the run.
        self.stop_after = source.RECONNECT_SECONDS

        async def emit(points: list[Point]) -> bool:
            self.batches.append(points)
            return self.accept

        async def sleep(seconds: float) -> None:
            self.sleeps.append(seconds)
            if seconds == self.stop_after:
                raise Stop

        def succeeded() -> None:
            self.successes += 1

        self.context = SourceContext(
            "home_connect",
            self.state,
            emit,
            on_success=succeeded,
            on_problem=self.problems.append,
            on_progress=lambda p: None,
            clock=lambda: self.now,
            sleep=sleep,
        )
        self.http = httpx2.AsyncClient()
        self.source = build(self.params, self.http, "home_connect")

    @property
    def points(self) -> list[Point]:
        return [p for batch in self.batches for p in batch if p.table != "ingest_progress"]

    @property
    def progress_points(self) -> list[Point]:
        return [p for batch in self.batches for p in batch if p.table == "ingest_progress"]

    async def run(self) -> None:
        with pytest.raises(Stop):
            await self.source.run(self.context)


def requests(server: StubServer, path: str) -> list[Received]:
    return [r for r in server.requests if r.path == path]


# ---------------------------------------------------------------------------------- values


@pytest.mark.parametrize(
    ("key", "name"),
    [
        ("BSH.Common.Status.DoorState", "status_door_state"),
        ("BSH.Common.Setting.PowerState", "setting_power_state"),
        ("BSH.Common.Option.RemainingProgramTime", "option_remaining_program_time"),
        ("Dishcare.Dishwasher.Event.RinseAidLack", "event_rinse_aid_lack"),
        ("BSH.Common.Root.ActiveProgram", "active_program"),
        ("BSH.Common.Root.SelectedProgram", "selected_program"),
        ("BSH.Common.Root.Other", "root_other"),
        ("Cooking.Oven.Status.CurrentCavityTemperature", "status_current_cavity_temperature"),
        ("Dishcare.Dishwasher.Option.VarioSpeedPlus", "option_vario_speed_plus"),
        ("BSH.Common.Status.CO2Level", "status_co2_level"),
        ("Short.Key", "short_key"),
        ("odd-key", "odd_key"),
    ],
)
def test_keys_become_fields_named_by_their_kind_and_name(key, name):
    assert values.field_name(key) == name


@pytest.mark.parametrize(
    ("key", "raw", "value"),
    [
        ("BSH.Common.Status.DoorState", "BSH.Common.EnumType.DoorState.Open", "Open"),
        ("Dishcare.Dishwasher.Event.SaltLack", "BSH.Common.EnumType.EventPresentState.Off", "Off"),
        ("BSH.Common.Status.RemoteControlActive", True, True),
        ("BSH.Common.Option.EnergyForecast", 52, 52.0),
        ("BSH.Common.Option.ProgramProgress", 12.5, 12.5),
        ("BSH.Common.Status.Name", "plain text", "plain text"),
        ("BSH.Common.Root.ActiveProgram", "Dishcare.Dishwasher.Program.Eco50", "Eco50"),
        ("BSH.Common.Root.SelectedProgram", None, ""),
        ("BSH.Common.Root.ActiveProgram", "NoProgramKey", "NoProgramKey"),
        ("BSH.Common.Status.Anything", None, None),
        ("BSH.Common.Status.Anything", "", None),
        ("BSH.Common.Status.Anything", [1, 2], None),
        ("BSH.Common.Status.Anything", {"a": 1}, None),
    ],
)
def test_values_keep_their_type_without_enumeration_prefixes(key, raw, value):
    assert values.field_value(key, raw) == value
    if value is not None:
        assert type(values.field_value(key, raw)) is type(value)


def test_field_types_are_influxdbs():
    assert [values.type_of(v) for v in (True, 1.0, 1, "x")] == [
        "boolean",
        "float",
        "integer",
        "string",
    ]


# ------------------------------------------------------------------------------------ auth


async def test_a_login_waits_for_the_code_to_be_confirmed():
    fake = FakeHomeConnect()
    fake.token_answers = [
        (400, {"error": "authorization_pending"}),
        (400, {"error": "slow_down"}),
        (200, {"access_token": "a", "refresh_token": "r", "expires_in": 100}),
    ]
    waits: list[float] = []

    async def sleep(seconds: float) -> None:
        waits.append(seconds)

    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as client:
        login = await auth.start_login(client, CLIENT_ID, server.url)
        tokens = await auth.finish_login(
            client, CLIENT_ID, login, now=lambda: NOW, sleep=sleep, url=server.url
        )
    assert login.user_code == "ABCD-1234"
    assert login.verification_url.endswith("user_code=ABCD-1234")
    assert tokens == Tokens("r", "a", NOW + 100)
    assert waits == [0, 0, 5]
    started = requests(server, "/security/oauth/device_authorization")[0]
    assert "scope=IdentifyAppliance+Monitor+Settings" in started.body
    assert "client_id=test-client" in started.body
    polled = requests(server, "/security/oauth/token")[0]
    assert "grant_type=device_code" in polled.body and "device_code=dev-code" in polled.body


async def test_a_login_falls_back_to_the_plain_address_and_default_timing():
    fake = FakeHomeConnect()
    fake.answers["/security/oauth/device_authorization"] = (
        200,
        json.dumps({"device_code": "d", "verification_uri": "https://example.test/v"}),
    )
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as client:
        login = await auth.start_login(client, CLIENT_ID, server.url)
    assert login == DeviceLogin("d", "", "https://example.test/v", 5.0, 600.0)


async def test_a_login_home_connect_refuses_to_start_says_why():
    fake = FakeHomeConnect()
    fake.answers["/security/oauth/device_authorization"] = (
        400,
        json.dumps({"error": "unauthorized_client", "error_description": "unknown client"}),
    )
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as client:
        with pytest.raises(auth.AuthError, match="unauthorized_client unknown client"):
            await auth.start_login(client, CLIENT_ID, server.url)


async def test_a_refused_login_ends_the_wait():
    fake = FakeHomeConnect()
    fake.token_answers = [(400, {"error": "access_denied"})]
    login = DeviceLogin("d", "u", "v", 0, 60)

    async def sleep(_: float) -> None:
        return None

    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as client:
        with pytest.raises(auth.AuthError, match="not completed: HTTP 400 access_denied"):
            await auth.finish_login(
                client, CLIENT_ID, login, now=lambda: NOW, sleep=sleep, url=server.url
            )


async def test_an_unconfirmed_code_expires():
    fake = FakeHomeConnect()
    fake.token_answers = [(400, {"error": "authorization_pending"})] * 3
    clock = [NOW]

    async def sleep(seconds: float) -> None:
        clock[0] += 30

    login = DeviceLogin("d", "u", "v", 30, 60)
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as client:
        with pytest.raises(auth.AuthError, match="expired before it was confirmed"):
            await auth.finish_login(
                client, CLIENT_ID, login, now=lambda: clock[0], sleep=sleep, url=server.url
            )
    assert len(requests(server, "/security/oauth/token")) == 2


async def test_a_refresh_keeps_the_refresh_token_unless_a_new_one_is_given():
    fake = FakeHomeConnect()
    fake.token_answers = [
        (200, {"access_token": "a1", "expires_in": 3600}),
        (200, {"access_token": "a2", "refresh_token": "r2"}),
    ]
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as client:
        first = await auth.refresh(client, CLIENT_ID, Tokens("r1"), NOW, server.url)
        second = await auth.refresh(client, CLIENT_ID, first, NOW, server.url)
    assert first == Tokens("r1", "a1", NOW + 3600)
    assert second == Tokens("r2", "a2", NOW + 86400)
    sent = requests(server, "/security/oauth/token")[0].body
    assert "grant_type=refresh_token" in sent and "refresh_token=r1" in sent
    assert "client_id=test-client" in sent


@pytest.mark.parametrize("error", ["invalid_grant", "unauthorized_client"])
async def test_a_refused_refresh_token_asks_for_a_new_login(error):
    fake = FakeHomeConnect()
    fake.token_answers = [(400, {"error": error})]
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as client:
        with pytest.raises(auth.TokenRefused, match=error):
            await auth.refresh(client, CLIENT_ID, Tokens("r"), NOW, server.url)


async def test_other_refresh_failures_say_what_was_answered():
    fake = FakeHomeConnect()
    fake.answers["/security/oauth/token"] = (503, "<html>down</html>")
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as client:
        with pytest.raises(auth.AuthError, match="answered HTTP 503") as raised:
            await auth.refresh(client, CLIENT_ID, Tokens("r"), NOW, server.url)
    assert not isinstance(raised.value, auth.TokenRefused)


async def test_an_unreachable_home_connect_is_an_auth_error():
    async with httpx2.AsyncClient() as client:
        with pytest.raises(auth.AuthError, match="cannot reach Home Connect"):
            await auth.refresh(client, CLIENT_ID, Tokens("r"), NOW, "http://127.0.0.1:9")


def test_tokens_are_kept_readable_by_their_owner_only(tmp_path):
    path = auth.save_tokens(tmp_path / "state", Tokens("r", "a", NOW))
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert auth.load_tokens(tmp_path / "state") == Tokens("r", "a", NOW)
    assert auth.load_tokens(tmp_path / "elsewhere") is None


def test_unreadable_or_empty_tokens(tmp_path):
    (tmp_path / auth.TOKEN_FILE).write_text("{not json")
    with pytest.raises(auth.AuthError, match="cannot read"):
        auth.load_tokens(tmp_path)
    (tmp_path / auth.TOKEN_FILE).write_text(json.dumps({"access_token": "a"}))
    assert auth.load_tokens(tmp_path) is None
    (tmp_path / auth.TOKEN_FILE).write_text(json.dumps({"refresh_token": "r"}))
    assert auth.load_tokens(tmp_path) == Tokens("r", "", 0.0)


def test_an_access_token_is_replaced_ten_minutes_before_it_expires():
    assert Tokens("r", "a", NOW + 601).fresh(NOW)
    assert not Tokens("r", "a", NOW + 600).fresh(NOW)
    assert not Tokens("r", "", NOW + 86400).fresh(NOW)


# ------------------------------------------------------------------------------------- api


async def lines(*text: str):
    for line in text:
        yield line


async def collect(stream) -> list[Event]:
    return [event async for event in stream]


async def test_server_sent_events_are_parsed_into_kinds_appliances_and_items():
    events = await collect(
        api.parse_events(
            lines(
                "event: STATUS\r\n",
                'data:{"haId":"A","items":[{"key":"k","value":1},{"no":"key"}]}\r\n',
                "id:A\r\n",
                "\r\n",
                "event:KEEP-ALIVE",
                "data:",
                "",
                ": a comment",
                "",
                "event:NOTIFY",
                'data:{"items":',
                'data:[{"key":"j","value":2}]}',
                "id:B",
                "",
                "event:EVENT",
                "data:not json",
                "",
                'data:["a list"]',
                "",
                "event:DISCONNECTED",
                "id:C",
            )
        )
    )
    assert events == [
        Event("STATUS", "A", [{"key": "k", "value": 1}]),
        Event("KEEP-ALIVE", "", []),
        Event("NOTIFY", "B", [{"key": "j", "value": 2}]),
        Event("EVENT", "", []),
        Event("message", "", []),
        Event("DISCONNECTED", "C", []),
    ]


async def test_the_api_reads_appliances_and_their_state():
    fake = FakeHomeConnect()
    fake.appliances.append({"haId": OVEN, "name": "Oven", "connected": False})
    fake.appliances.append({"type": "Broken"})
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as client:
        home = HomeConnectApi(client, "token", server.url)
        appliances = await home.appliances()
        state = await home.state(HA)
    assert appliances == [
        Appliance(HA, "Dishwasher", "Brand", "DW1", "", True),
        Appliance(OVEN, "", "", "", "Oven", False),
    ]
    assert [a.label for a in appliances] == ["Brand DW1 (Dishwasher)", "Oven"]
    assert state == [
        ("BSH.Common.Status.DoorState", "BSH.Common.EnumType.DoorState.Open"),
        ("BSH.Common.Status.RemoteControlActive", True),
        ("BSH.Common.Setting.PowerState", "BSH.Common.EnumType.PowerState.Off"),
        (api.SELECTED_PROGRAM, "Dishcare.Dishwasher.Program.Eco50"),
        ("BSH.Common.Option.EnergyForecast", 52),
        ("Dishcare.Dishwasher.Option.HalfLoad", False),
        (api.ACTIVE_PROGRAM, None),
    ]
    assert home.requests == 5
    sent = server.requests[0].headers
    assert sent["authorization"] == "Bearer token"
    assert sent["accept"] == api.ACCEPT


async def test_an_offline_appliance_gives_no_state():
    fake = FakeHomeConnect()
    for part in ("status", "settings", "programs/selected", "programs/active"):
        fake.answers[f"/api/homeappliances/{HA}/{part}"] = (409, "{}")
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as client:
        state = await HomeConnectApi(client, "token", server.url).state(HA)
    assert state == [(api.SELECTED_PROGRAM, None), (api.ACTIVE_PROGRAM, None)]


@pytest.mark.parametrize(
    ("status", "body", "error", "match"),
    [
        (401, "{}", api.Unauthorized, "refused the access token"),
        (429, "{}", api.RateLimited, "retrying in 60 s"),
        (500, json.dumps({"error": {"key": "SDK.Error.Internal"}}), api.ApiError, "500 SDK.Error"),
        (503, "down", api.ApiError, "HTTP 503$"),
        (200, "not json", api.ApiError, "other than JSON"),
    ],
)
async def test_failed_requests_raise_by_their_kind(status, body, error, match):
    fake = FakeHomeConnect()
    fake.answers["/api/homeappliances"] = (status, body)
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as client:
        with pytest.raises(error, match=match):
            await HomeConnectApi(client, "token", server.url).appliances()


async def test_an_answer_without_data_has_no_appliances():
    fake = FakeHomeConnect()
    fake.answers["/api/homeappliances"] = (200, json.dumps(["not", "an", "object"]))
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as client:
        assert await HomeConnectApi(client, "token", server.url).appliances() == []


@pytest.mark.parametrize(("header", "wait"), [("120", 120.0), ("0", 1.0), ("soon", 60.0)])
def test_a_rate_limit_waits_as_long_as_home_connect_asks(header, wait):
    response = httpx2.Response(429, headers={"Retry-After": header})
    with pytest.raises(api.RateLimited) as raised:
        api._raise_for(response)
    assert raised.value.retry_after == wait


async def test_unreachable_home_connect_is_an_api_error():
    async with httpx2.AsyncClient() as client:
        home = HomeConnectApi(client, "token", "http://127.0.0.1:9")
        with pytest.raises(api.ApiError, match="cannot reach"):
            await home.appliances()
        with pytest.raises(api.ApiError, match="event stream broke"):
            await collect(home.events())


async def test_the_event_stream_yields_events_until_it_closes():
    fake = FakeHomeConnect()
    fake.streams = [sse(("KEEP-ALIVE", None), ("STATUS", {"haId": HA, "items": [item("k", 1)]}))]
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as client:
        events = await collect(HomeConnectApi(client, "token", server.url).events())
    assert [e.kind for e in events] == ["KEEP-ALIVE", "STATUS"]
    assert requests(server, "/api/homeappliances/events")[0].headers["accept"] == (
        "text/event-stream"
    )


async def test_a_refused_event_stream_raises():
    fake = FakeHomeConnect()
    fake.stream_status = 429
    async with serving(StubServer(respond=fake)) as server, httpx2.AsyncClient() as client:
        with pytest.raises(api.RateLimited):
            await collect(HomeConnectApi(client, "token", server.url).events())


# ---------------------------------------------------------------------------------- source


def fields_of(points: list[Point]) -> list[dict]:
    return [p.fields for p in points]


async def test_a_connection_writes_the_state_then_follows_the_stream(tmp_path, caplog):
    caplog.set_level("INFO")
    fake = FakeHomeConnect()
    fake.streams = [
        sse(
            ("KEEP-ALIVE", None),
            (
                "EVENT",
                {
                    "haId": HA,
                    "items": [
                        item(
                            "Dishcare.Dishwasher.Event.RinseAidLack",
                            "BSH.Common.EnumType.EventPresentState.Present",
                            NOW + 5,
                        )
                    ],
                },
            ),
            (
                "STATUS",
                {
                    "haId": HA,
                    "items": [
                        item(
                            "BSH.Common.Status.DoorState",
                            "BSH.Common.EnumType.DoorState.Closed",
                            NOW + 10,
                        ),
                        item(
                            "BSH.Common.Root.ActiveProgram",
                            "Dishcare.Dishwasher.Program.Eco50",
                            NOW + 12,
                        ),
                        item("BSH.Common.Option.ProgramProgress", 3, None),
                    ],
                },
            ),
            ("NOTIFY", {"haId": HA, "items": []}),
        )
    ]
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        await harness.run()
    state, rinse, progress, door, program = harness.points
    assert state.table == "home_connect_dishwasher"
    assert state.tags == (("appliance", HA),)
    assert state.timestamp == NOW * 1000
    assert state.fields == {
        "connected": True,
        "status_door_state": "Open",
        "status_remote_control_active": True,
        "setting_power_state": "Off",
        "selected_program": "Eco50",
        "option_energy_forecast": 52.0,
        "option_half_load": False,
        "active_program": "",
    }
    assert (rinse.timestamp, rinse.fields) == (
        (NOW + 5) * 1000,
        {"event_rinse_aid_lack": "Present"},
    )
    assert door.timestamp == (NOW + 10) * 1000 and door.fields == {"status_door_state": "Closed"}
    assert program.fields == {"active_program": "Eco50"}
    assert progress.timestamp == NOW * 1000 and progress.fields == {"option_program_progress": 3.0}
    reported = harness.progress_points[0]
    assert dict(reported.tags) == {"source": "home_connect", "stream": HA}
    assert reported.fields == {"complete_until": int(NOW * 1000)}
    assert harness.successes == 4
    assert harness.sleeps == [source.RECONNECT_SECONDS]
    assert "appliances: Brand DW1 (Dishwasher)" in caplog.text
    assert requests(server, "/security/oauth/token") == []
    assert "Bearer access-0" in requests(server, "/api/homeappliances")[0].headers["authorization"]


async def test_progress_carries_the_appliance_label(tmp_path):
    fake = FakeHomeConnect()
    reported: list[Progress] = []
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        harness.context._on_progress = reported.append
        await harness.run()
    assert reported == [Progress(HA, int(NOW * 1000), "Brand DW1 (Dishwasher)")]


async def test_without_a_login_the_run_says_how_to_sign_in(tmp_path):
    async with serving(StubServer(respond=FakeHomeConnect())) as server:
        harness = Harness(tmp_path, server, signed_in=False)
        with pytest.raises(
            source.NotSignedIn, match="ingest-hub home-connect-auth --source home_connect"
        ):
            await harness.source.run(harness.context)
    assert server.requests == []


async def test_an_expiring_access_token_is_refreshed_and_kept(tmp_path):
    fake = FakeHomeConnect()
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        auth.save_tokens(harness.state, Tokens("refresh-1", "access-0", NOW + 60))
        await harness.run()
    assert len(requests(server, "/security/oauth/token")) == 1
    assert "Bearer access-1" in requests(server, "/api/homeappliances")[0].headers["authorization"]
    assert auth.load_tokens(harness.state) == Tokens("refresh-1", "access-1", NOW + 86400)


async def test_a_stream_ends_when_the_access_token_is_about_to_expire(tmp_path):
    fake = FakeHomeConnect()
    fake.streams = [sse(("KEEP-ALIVE", None), ("KEEP-ALIVE", None))]
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        auth.save_tokens(harness.state, Tokens("refresh-1", "access-0", NOW + 3600))
        harness.source._params = harness.params.model_copy(update={"sample_minutes": 1000})
        original = harness.context.succeeded

        def later() -> None:
            harness.now = NOW + 3100
            original()

        harness.context.succeeded = later
        await harness.run()
    assert harness.successes == 1


async def test_a_refused_access_token_is_refreshed_once(tmp_path):
    fake = FakeHomeConnect()
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        auth.save_tokens(harness.state, Tokens("refresh-1", "expired", NOW + 86400))
        await harness.run()
    assert len(requests(server, "/security/oauth/token")) == 1
    authorizations = [r.headers["authorization"] for r in requests(server, "/api/homeappliances")]
    assert authorizations == ["Bearer expired", "Bearer access-1"]


async def test_an_access_token_refused_right_after_a_refresh_ends_the_run(tmp_path):
    fake = FakeHomeConnect()
    fake.token_answers = [(200, {"access_token": "expired", "expires_in": 86400})]
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        auth.save_tokens(harness.state, Tokens("refresh-1", "", 0))
        with pytest.raises(api.Unauthorized):
            await harness.source.run(harness.context)


async def test_a_refused_refresh_token_says_how_to_sign_in_again(tmp_path):
    fake = FakeHomeConnect()
    fake.token_answers = [(400, {"error": "invalid_grant"})]
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        auth.save_tokens(harness.state, Tokens("refresh-1", "", 0))
        with pytest.raises(auth.TokenRefused, match="invalid_grant.*home-connect-auth"):
            await harness.source.run(harness.context)


async def test_a_rate_limit_is_waited_out_and_reported(tmp_path):
    fake = FakeHomeConnect()
    fake.once["/api/homeappliances"] = (429, "{}")
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        await harness.run()
    assert harness.problems == ["Home Connect's request limit is reached; retrying in 60 s"]
    assert harness.sleeps == [60.0, source.RECONNECT_SECONDS]
    assert len(requests(server, "/api/homeappliances")) == 2
    assert harness.points[0].fields["connected"] is True


async def test_a_reconnect_soon_after_a_full_read_does_not_read_again(tmp_path):
    fake = FakeHomeConnect()
    fake.streams = [sse(("KEEP-ALIVE", None)), sse(("KEEP-ALIVE", None))]
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        harness.stop_after = -1.0
        reconnects = 0

        async def sleep(seconds: float) -> None:
            nonlocal reconnects
            reconnects += 1
            harness.now += 60
            if reconnects == 3:
                raise Stop

        harness.context.sleep = sleep
        await harness.run()
    assert len(requests(server, "/api/homeappliances")) == 1
    assert len(requests(server, "/api/homeappliances/events")) == 3


async def test_every_appliance_is_written_again_after_sample_minutes(tmp_path):
    fake = FakeHomeConnect()
    fake.streams = [sse(("KEEP-ALIVE", None), ("KEEP-ALIVE", None))]
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server, sample_minutes=1)
        original = harness.context.succeeded

        def later() -> None:
            harness.now += 61
            original()

        harness.context.succeeded = later
        await harness.run()
    assert len(harness.points) == 3
    assert harness.points[0].fields == harness.points[2].fields
    assert harness.points[2].timestamp == (NOW + 122) * 1000


async def test_connecting_and_disconnecting_appliances(tmp_path):
    fake = FakeHomeConnect()
    fake.appliances[0]["connected"] = False
    fake.streams = [sse(("CONNECTED", {"haId": HA}), ("DISCONNECTED", {"haId": HA}))]
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        await harness.run()
    offline, connected, disconnected = harness.points
    assert offline.fields == {"connected": False}
    assert connected.fields["connected"] is True
    assert connected.fields["status_door_state"] == "Open"
    assert disconnected.fields == {"connected": False}
    assert len(requests(server, f"/api/homeappliances/{HA}/status")) == 1


async def test_an_appliance_reconnecting_soon_again_is_not_read_again(tmp_path):
    fake = FakeHomeConnect()
    fake.streams = [
        sse(
            ("DISCONNECTED", {"haId": HA}),
            ("CONNECTED", {"haId": HA}),
            ("DISCONNECTED", {"haId": HA}),
            ("CONNECTED", {"haId": HA}),
        )
    ]
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        await harness.run()
    assert len(requests(server, f"/api/homeappliances/{HA}/status")) == 1
    assert [p.fields for p in harness.points[1:]] == [{"connected": v} for v in (False, True) * 2]


async def test_events_for_an_unknown_appliance_read_the_appliances_at_most_every_resync(tmp_path):
    fake = FakeHomeConnect()
    fake.streams = [sse(*[("STATUS", {"haId": OVEN, "items": [item("k.Status.X", 1)]})] * 5)]
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        original = harness.context.succeeded

        def later() -> None:
            harness.now += 400
            original()

        harness.context.succeeded = later
        await harness.run()
    # events at 400, 800, 1200, 1600 and 2000 s: the first read, and one at 1200 s only
    assert len(requests(server, "/api/homeappliances")) == 2


async def test_a_new_or_paired_appliance_reads_the_appliances_again(tmp_path):
    fake = FakeHomeConnect()
    fake.streams = [
        sse(
            ("STATUS", {"haId": OVEN, "items": [item("k.Status.X", 1)]}),
            ("PAIRED", {"haId": OVEN}),
            ("STATUS", {"haId": "UNKNOWN", "items": []}),
        )
    ]
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server, resync_minutes=0)
        await harness.run()
    assert len(requests(server, "/api/homeappliances")) == 4


async def test_a_field_keeps_its_first_type(tmp_path):
    fake = FakeHomeConnect()
    fake.streams = [
        sse(
            ("STATUS", {"haId": HA, "items": [item("BSH.Common.Status.DoorState", 1)]}),
            ("STATUS", {"haId": HA, "items": [item("BSH.Common.Status.DoorState", 2)]}),
        )
    ]
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        await harness.run()
    assert [p.fields for p in harness.points[1:]] == []
    assert harness.problems == [
        "home_connect_dishwasher.status_door_state arrived as float, but was first written as "
        "string; left out"
    ]
    kept = json.loads((harness.state / source.STATE_FILE).read_text())["field_types"]
    assert kept["home_connect_dishwasher.status_door_state"] == "string"
    assert kept["home_connect_dishwasher.connected"] == "boolean"


async def test_field_types_survive_a_restart(tmp_path):
    fake = FakeHomeConnect()
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        (harness.state / source.STATE_FILE).write_text(
            json.dumps({"field_types": {"home_connect_dishwasher.status_door_state": "float"}})
        )
        await harness.run()
    assert "status_door_state" not in harness.points[0].fields
    assert len(harness.problems) == 1


@pytest.mark.parametrize("text", ["{broken", json.dumps(["no"])])
async def test_an_unreadable_type_file(tmp_path, text):
    async with serving(StubServer(respond=FakeHomeConnect())) as server:
        harness = Harness(tmp_path, server)
        (harness.state / source.STATE_FILE).write_text(text)
        if text.startswith("{"):
            with pytest.raises(api.ApiError, match="cannot read"):
                await harness.source.run(harness.context)
        else:
            await harness.run()


async def test_a_refused_batch_is_reported(tmp_path):
    async with serving(StubServer(respond=FakeHomeConnect())) as server:
        harness = Harness(tmp_path, server)
        harness.accept = False
        await harness.run()
    assert harness.problems == ["a batch was refused; its values are lost"]


async def test_events_for_no_appliance_are_ignored(tmp_path):
    fake = FakeHomeConnect()
    fake.streams = [
        sse(
            ("STATUS", {"items": [item("k.Status.X", 1)]}),
            ("PAIRING", None),
            ("SOMETHING-NEW", {"haId": HA}),
            ("STATUS", {"haId": HA, "items": [item("BSH.Common.Status.Gone", None)]}),
        )
    ]
    async with serving(StubServer(respond=fake)) as server:
        harness = Harness(tmp_path, server)
        await harness.run()
    assert len(harness.points) == 1
    assert "status_gone" not in harness.points[0].fields


def test_params_and_registration(tmp_path):
    assert ADAPTERS["home_connect"].params is HomeConnectParams
    with pytest.raises(ValidationError):
        HomeConnectParams.model_validate({"client_id_file": "x", "extra": 1})
    with pytest.raises(ValidationError):
        HomeConnectParams.model_validate({"client_id_file": "x", "table_prefix": "1bad"})
    with pytest.raises(ValidationError):
        HomeConnectParams.model_validate({"client_id_file": "x", "sample_minutes": 0})
    params = HomeConnectParams.model_validate({"client_id_file": str(tmp_path / "missing")})
    with pytest.raises(ConfigError, match="cannot read secret file"):
        build(params, httpx2.AsyncClient(), "x")


# ------------------------------------------------------------------------------------- cli


def write_config(tmp_path: Path, server: StubServer) -> Settings:
    (tmp_path / "secrets").mkdir(parents=True, exist_ok=True)
    (tmp_path / "secrets" / "client_id").write_text(CLIENT_ID)
    (tmp_path / "secrets" / "influx").write_text("token")
    config = tmp_path / "config" / "sources.toml"
    config.parent.mkdir(parents=True)
    config.write_text(
        f"""
[destinations.home]
kind = "influx3"
url = "http://127.0.0.1:9"
database = "home"
token_file = "{tmp_path / "secrets" / "influx"}"

[sources.appliances]
kind = "home_connect"
destination = "home"

[sources.appliances.params]
client_id_file = "{tmp_path / "secrets" / "client_id"}"
url = "{server.url}"

[sources.other]
kind = "mqtt"
destination = "home"

[sources.other.params]
host = "127.0.0.1"
routes = []
"""
    )
    return Settings(data_dir=tmp_path, config_file=config)


async def test_home_connect_auth_shows_the_code_waits_and_keeps_the_tokens(tmp_path, capsys):
    fake = FakeHomeConnect()
    fake.token_answers = [
        (400, {"error": "authorization_pending"}),
        (200, {"access_token": "a", "refresh_token": "r", "expires_in": 86400}),
    ]
    async with serving(StubServer(respond=fake)) as server:
        settings = write_config(tmp_path, server)
        assert await cli._home_connect_auth(settings, "appliances") == 0
    shown = capsys.readouterr().out
    assert "https://example.test/verify?user_code=ABCD-1234" in shown
    assert "(code ABCD-1234) Open the link" in shown
    assert "Appliances: Brand DW1 (Dishwasher). A running ingest-hub starts using it" in shown
    tokens = auth.load_tokens(tmp_path / "state" / "appliances")
    assert tokens is not None and (tokens.refresh_token, tokens.access_token) == ("r", "a")


async def test_home_connect_auth_with_no_appliances_yet(tmp_path, capsys):
    fake = FakeHomeConnect()
    fake.appliances = []
    fake.token_answers = [(200, {"access_token": "a", "refresh_token": "r"})]
    async with serving(StubServer(respond=fake)) as server:
        settings = write_config(tmp_path, server)
        assert await cli._home_connect_auth(settings, "appliances") == 0
    assert "Appliances: none yet" in capsys.readouterr().out


async def test_home_connect_auth_needs_a_home_connect_source(tmp_path):
    async with serving(StubServer(respond=FakeHomeConnect())) as server:
        settings = write_config(tmp_path, server)
        with pytest.raises(ConfigError, match="no source named"):
            await cli._home_connect_auth(settings, "missing")
        with pytest.raises(ConfigError, match="not a Home Connect source"):
            await cli._home_connect_auth(settings, "other")


def test_the_command_reports_failures_and_exits_2(tmp_path, monkeypatch, capsys):
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "sources.toml").write_text("[destinations]\n[sources]\n")
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.delenv("CONFIG_FILE", raising=False)
    assert cli.main(["home-connect-auth", "--source", "nothing"]) == 2
    assert "no source named 'nothing'" in capsys.readouterr().err


def test_the_command_reports_home_connect_errors(tmp_path, monkeypatch, capsys):
    async def failing(*_args):
        raise api.ApiError("boom")

    monkeypatch.setattr(cli, "_home_connect_auth", failing)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    assert cli.main(["home-connect-auth", "--source", "x"]) == 2
    assert "ingest-hub: boom" in capsys.readouterr().err


async def test_signing_in_needs_a_started_login_and_a_readable_token_file(tmp_path):
    async with serving(StubServer(respond=FakeHomeConnect())) as server:
        harness = Harness(tmp_path, server, signed_in=False)
        assert harness.source.signed_in(harness.state) is False
        with pytest.raises(auth.AuthError, match="no sign-in was started"):
            await harness.source.finish_sign_in(harness.state)
        harness.state.mkdir(parents=True, exist_ok=True)
        (harness.state / auth.TOKEN_FILE).write_text("{broken")
        assert harness.source.signed_in(harness.state) is False

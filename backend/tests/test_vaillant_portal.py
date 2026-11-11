"""Fetching exports from Vaillant's EU Data Act portal, against a mock of the portal and its login
server, with a synthetic account."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import stat
from pathlib import Path
from urllib.parse import parse_qs

import httpx2
import pytest
from jigkit.sinks import Point

from ingest_hub.adapters import ADAPTERS
from ingest_hub.adapters.vaillant import export, portal
from ingest_hub.adapters.vaillant.export import (
    FETCH_FILE,
    VaillantExportParams,
    VaillantExportSource,
)
from ingest_hub.adapters.vaillant.portal import Portal, PortalError, action_result, solve_altcha
from ingest_hub.collect import SourceContext

PORTAL = "https://portal.test"
LOGIN_SERVER = "https://login.test"
USER, PASSWORD = "export@example.org", "synthetic-password"
SIGN_IN, STATS, DATA = "a" * 42, "b" * 42, "c" * 42
CHALLENGE = {
    "parameters": {
        "algorithm": "PBKDF2/SHA-256",
        "cost": 2,
        "keyLength": 32,
        "keyPrefix": "0",
        "nonce": "00112233445566778899aabbccddeeff",
        "salt": "ffeeddccbbaa99887766554433221100",
    },
    "signature": "synthetic-signature",
}
RECORDS = [
    {
        "topic": "gateway.hem.message.received",
        "function": "status_message_mpc",
        "timestamp": "2026-11-10T10:00:00.000+00:00",
        "message_value": '{"power": 1}',
    },
    {
        "topic": "$weird",
        "function": None,
        "timestamp": "2026-11-10T10:01:00.000+00:00",
        "message_value": "x" * 300,
    },
]


def reference(ident: str, name: str) -> str:
    """How the portal's scripts name an action."""
    return f'(0,r.createServerReference)("{ident}",r.callServer,void 0,r.findSourceMapURL,"{name}")'


def stream(value: str) -> str:
    """An action's answer as the portal sends it, with the value in row 1."""
    return f'0:{{"a":"$@1","f":"","b":"build"}}\n1:{value}\n'


def solved(form: str) -> bool:
    """Whether the form's altcha field answers CHALLENGE."""
    payload = json.loads(base64.b64decode(form))
    params, counter = payload["challenge"]["parameters"], payload["solution"]["counter"]
    key = hashlib.pbkdf2_hmac(
        "sha256",
        bytes.fromhex(params["nonce"]) + counter.to_bytes(4, "big"),
        bytes.fromhex(params["salt"]),
        params["cost"],
        params["keyLength"],
    )
    return (
        key.hex().startswith(params["keyPrefix"]) and key.hex() == payload["solution"]["derivedKey"]
    )


class FakePortal:
    """Answers like the portal and its login server, for one account."""

    def __init__(self) -> None:
        self.systems = ["system-1"]
        self.records = RECORDS
        self.login_error = (
            '<span id="input-error" class="x">Invalid username &amp; password.</span>'
        )
        self.data_actions = [("fetchStatistics", STATS), ("fetchSystemData", DATA)]
        self.form_altcha = '<altcha-widget challenge="/api/altcha/challenge" name="altcha">'
        self.challenge: dict = CHALLENGE
        self.statuses: dict[str, int] = {}
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        url, path = str(request.url), request.url.path
        if path in self.statuses:
            return httpx2.Response(self.statuses[path])
        if url.startswith(LOGIN_SERVER):
            return self.login_server(request, path)
        signed_in = "session=ok" in request.headers.get("cookie", "")
        if request.method == "GET" and path == "/en/login":
            return httpx2.Response(
                200, text='<script src="/_next/static/chunks/login.js"></script>'
            )
        if path == "/_next/static/chunks/login.js":
            return httpx2.Response(200, text=reference(SIGN_IN, "doSignIn"))
        if path == "/_next/static/chunks/data.js":
            return httpx2.Response(
                200, text=";".join(reference(i, n) for n, i in self.data_actions)
            )
        if path == "/_next/static/chunks/missing.js":
            return httpx2.Response(404)
        if request.method == "POST" and path == "/en/login":
            assert request.headers["next-action"] == SIGN_IN
            assert json.loads(request.content) == ["vaillant-netherlands-b2c", "/euda-data"]
            return httpx2.Response(
                200, text=stream(json.dumps(f"{LOGIN_SERVER}/auth/realms/r/auth?x=1"))
            )
        if path == "/api/auth/callback":
            return httpx2.Response(
                302,
                headers={"location": f"{PORTAL}/en/euda-data", "set-cookie": "session=ok; Path=/"},
            )
        if path == "/en/euda-data" and not signed_in:
            return httpx2.Response(307, headers={"location": f"{PORTAL}/login"})
        if request.method == "GET" and path == "/en/euda-data":
            return httpx2.Response(
                200,
                text='<script src="/_next/static/chunks/data.js"></script>'
                '<script src="/_next/static/chunks/missing.js"></script>',
            )
        if request.method == "POST" and path == "/en/euda-data":
            action, args = request.headers["next-action"], json.loads(request.content)
            if action == STATS:
                return httpx2.Response(
                    200,
                    text=stream(
                        json.dumps(
                            {"allSystemIds": self.systems, "systemIdCount": len(self.systems)}
                        )
                    ),
                )
            assert action == DATA and args == [self.systems]
            # The records come as references to later rows, one of them long text.
            long = self.records[1]["message_value"].encode()
            second = {
                **self.records[1],
                "topic": "$" + self.records[1]["topic"],
                "message_value": "$4",
                "function": "$undefined",
            }
            body = (
                '0:{"a":"$@1"}\n1:["$2","$3"]\n'
                f"2:{json.dumps(self.records[0])}\n"
                f"3:{json.dumps(second)}\n"
                f"4:T{len(long):x},{long.decode()}"
            )
            return httpx2.Response(200, text=body)
        return httpx2.Response(404)

    def login_server(self, request: httpx2.Request, path: str) -> httpx2.Response:
        if path == "/auth/realms/r/auth":
            return httpx2.Response(
                200,
                text=(
                    '<form id="kc-form" '
                    'action="/auth/realms/r/login-actions/authenticate?s=1&amp;t=2">'
                    f"{self.form_altcha}</form>"
                ),
            )
        if path == "/api/altcha/challenge":
            return httpx2.Response(200, json=self.challenge)
        if path == "/auth/realms/r/login-actions/authenticate":
            assert request.url.params["t"] == "2"
            form = {
                k: v[0]
                for k, v in parse_qs(request.content.decode(), keep_blank_values=True).items()
            }
            if (
                form.get("username") == USER
                and form.get("password") == PASSWORD
                and ("altcha" not in self.form_altcha.lower() or solved(form["altcha"]))
            ):
                return httpx2.Response(
                    302, headers={"location": f"{PORTAL}/api/auth/callback?code=1"}
                )
            return httpx2.Response(200, text=f"<form action='x'>{self.login_error}</form>")
        return httpx2.Response(404)


def make_portal(fake: FakePortal, **options) -> Portal:
    return Portal(
        options.pop("username", USER),
        options.pop("password", PASSWORD),
        realm="vaillant-netherlands-b2c",
        url=PORTAL,
        transport=httpx2.MockTransport(fake),
        **options,
    )


async def test_a_fetch_signs_in_solves_the_challenge_and_saves_what_the_download_would():
    fake = FakePortal()
    data = await make_portal(fake).fetch()
    assert json.loads(data) == RECORDS
    assert data.decode() == json.dumps(RECORDS, indent=2)
    # The session cookie from signing in reached the data page's actions.
    actions = [r for r in fake.requests if r.method == "POST" and r.url.path == "/en/euda-data"]
    assert len(actions) == 2 and all("session=ok" in r.headers["cookie"] for r in actions)


async def test_a_refused_login_gives_the_login_servers_message():
    fake = FakePortal()
    with pytest.raises(PortalError, match="login refused: Invalid username & password."):
        await make_portal(fake, password="wrong").fetch()
    fake.login_error = ""
    with pytest.raises(PortalError, match="came back without a message"):
        await make_portal(fake, password="wrong").fetch()


async def test_an_account_without_systems_is_an_error():
    fake = FakePortal()
    fake.systems = []
    with pytest.raises(PortalError, match="sees no systems"):
        await make_portal(fake).fetch()


async def test_a_form_without_a_widget_uses_the_usual_challenge_path():
    fake = FakePortal()
    fake.form_altcha = "<!-- altcha -->"
    assert json.loads(await make_portal(fake).fetch()) == RECORDS


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (
            lambda f: setattr(f, "data_actions", [("fetchStatistics", STATS)]),
            "no fetchSystemData action",
        ),
        (lambda f: f.statuses.update({"/en/login": 503}), "/en/login answered HTTP 503"),
        (
            lambda f: f.statuses.update({"/api/altcha/challenge": 500}),
            "challenge answered HTTP 500",
        ),
        (
            lambda f: f.statuses.update({"/auth/realms/r/login-actions/authenticate": 502}),
            "login answered HTTP 502",
        ),
        (lambda f: setattr(f, "challenge", {"what": 1}), "unknown format"),
        (lambda f: setattr(f, "systems", "not-a-list"), "did not list the account's systems"),
    ],
)
async def test_failures_are_named(change, message):
    fake = FakePortal()
    change(fake)
    with pytest.raises(PortalError, match=message):
        await make_portal(fake).fetch()


async def test_unexpected_answers_are_errors(monkeypatch):
    fake = FakePortal()
    plain = fake.__call__

    def answer(request):
        if request.url.path == "/auth/realms/r/auth":
            return httpx2.Response(200, text="<p>no form</p>")
        return plain(request)

    with pytest.raises(PortalError, match="no form"):
        await make_portal(answer).fetch()

    def bad_sign_in(request):
        if request.method == "POST" and request.url.path == "/en/login":
            return httpx2.Response(200, text=stream('"not a url"'))
        return plain(request)

    with pytest.raises(PortalError, match="did not give a login address"):
        await make_portal(bad_sign_in).fetch()

    def bad_action(request):
        if request.method == "POST" and request.url.path == "/en/euda-data":
            return httpx2.Response(500)
        return plain(request)

    with pytest.raises(PortalError, match="fetchStatistics answered HTTP 500"):
        await make_portal(bad_action).fetch()

    def records_not_a_list(request):
        if request.method == "POST" and request.headers.get("next-action") == DATA:
            return httpx2.Response(200, text=stream('{"x": 1}'))
        return plain(request)

    with pytest.raises(PortalError, match="not a list of records"):
        await make_portal(records_not_a_list).fetch()

    def challenge_not_json(request):
        if request.url.path == "/api/altcha/challenge":
            return httpx2.Response(200, text="<html>")
        return plain(request)

    with pytest.raises(PortalError, match="challenge cannot be read"):
        await make_portal(challenge_not_json).fetch()


async def test_an_already_signed_in_landing_elsewhere_goes_to_the_data_page():
    fake = FakePortal()
    plain = fake.__call__

    def lands_on_home(request):
        if request.url.path == "/api/auth/callback":
            return httpx2.Response(
                302, headers={"location": f"{PORTAL}/en/home", "set-cookie": "session=ok; Path=/"}
            )
        if request.url.path == "/en/home":
            return httpx2.Response(200, text="home")
        return plain(request)

    assert json.loads(await make_portal(lands_on_home).fetch()) == RECORDS


# ------------------------------------------------------------------ the challenge
def test_the_older_challenge_format_is_solved_too():
    salt, number = "abc?expires=1", 1234
    challenge = {
        "algorithm": "SHA-256",
        "salt": salt,
        "signature": "s",
        "maxnumber": 5000,
        "challenge": hashlib.sha256(f"{salt}{number}".encode()).hexdigest(),
    }
    payload = json.loads(base64.b64decode(solve_altcha(challenge)))
    assert payload["number"] == number and payload["salt"] == salt and payload["signature"] == "s"


@pytest.mark.parametrize(
    ("challenge", "message"),
    [
        (
            {**CHALLENGE, "parameters": {**CHALLENGE["parameters"], "algorithm": "SCRYPT"}},
            "unsupported",
        ),
        ({"algorithm": "SHA-512", "challenge": "00", "salt": "x", "signature": "s"}, "unsupported"),
        (
            {
                "algorithm": "SHA-256",
                "challenge": "ff" * 32,
                "salt": "x",
                "signature": "s",
                "maxnumber": 10,
            },
            "no solution",
        ),
    ],
)
def test_challenges_that_cannot_be_solved(challenge, message):
    with pytest.raises(PortalError, match=message):
        solve_altcha(challenge)


def test_a_prefix_never_found_within_the_limit():
    impossible = {**CHALLENGE, "parameters": {**CHALLENGE["parameters"], "keyPrefix": "0000"}}
    with pytest.raises(PortalError, match="no solution"):
        solve_altcha(impossible, max_counter=20)


# ------------------------------------------------------------------ the stream
def test_stream_rows_resolve():
    body = (
        '0:{"a":"$@1"}\n\n'
        '1:{"x":"$2","y":"$$money","z":"$9","w":"$undefined","n":[1,"$@2"]}\n'
        '2:"two"'
    )
    assert action_result(body) == {"x": "two", "y": "$money", "z": "$9", "w": None, "n": [1, "two"]}
    assert action_result('0:"plain"') == "plain"
    # Rows for the page itself are skipped.
    page = '2:"$Sreact.fragment"\n3:I[1,["/a.js"],"x"]\n:HL["/a.css","style"]\n0:{"a":"$@1"}\n1:5'
    assert action_result(page) == 5
    # A row that refers to itself is left as it is rather than followed for ever.
    assert action_result('0:{"a":"$@1"}\n1:["$1"]') == ["$1"]


@pytest.mark.parametrize(
    ("body", "message"),
    [("1:2", "has no result"), ("zz", "cannot be read"), ("0:{bad", "cannot be read")],
)
def test_unreadable_streams(body, message):
    with pytest.raises(PortalError, match=message):
        action_result(body)


# ------------------------------------------------------------------ the source
class Stop(Exception):
    pass


class FakeFetcher:
    def __init__(self, result: bytes | Exception) -> None:
        self.result = result
        self.calls = 0

    async def fetch(self) -> bytes:
        self.calls += 1
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


# 11 Nov 2026, 12:26:40 UTC: yesterday is 10 Nov.
NOW = 1_794_400_000.0


def harness(tmp_path: Path, fetcher: FakeFetcher, now: float = NOW):
    params = VaillantExportParams(
        inbox=tmp_path / "inbox",
        fetch={
            "username_file": tmp_path / "u",
            "password_file": tmp_path / "p",
            "retry_minutes": 30,
        },
    )
    out = {"batches": [], "problems": [], "successes": 0, "sleeps": [], "now": now}

    async def emit(points: list[Point]) -> bool:
        out["batches"].append(points)
        return True

    async def sleep(seconds: float) -> None:
        out["sleeps"].append(seconds)
        raise Stop

    def succeeded() -> None:
        out["successes"] += 1

    context = SourceContext(
        "heating_export",
        tmp_path / "state",
        emit,
        on_success=succeeded,
        on_problem=out["problems"].append,
        clock=lambda: out["now"],
        sleep=sleep,
    )
    return VaillantExportSource(params, fetcher), context, out


def export_of(day: str) -> bytes:
    """An export with one power reading on ``day``."""
    message = {
        "mpc_device_status": [{"ebus_address": 8, "bus_coupler_address": 0, "current_power": 1200}]
    }
    return json.dumps(
        [
            {
                "topic": "gateway.hem.message.received",
                "function": "status_message_mpc",
                "timestamp": f"{day}T10:00:00.000+00:00",
                "message_value": json.dumps(message),
            }
        ]
    ).encode()


async def run_once(source, context) -> None:
    with pytest.raises(Stop):
        await source.run(context)


async def test_a_missing_yesterday_is_fetched_privately_imported_and_remembered(tmp_path):
    fetcher = FakeFetcher(export_of("2026-11-10"))
    source, context, out = harness(tmp_path, fetcher)
    await run_once(source, context)
    imported = list((tmp_path / "inbox" / "imported").iterdir())
    assert [p.name for p in imported] == ["euda-data-2026-11-11_12-26-40.json"]
    assert stat.S_IMODE(os.stat(imported[0]).st_mode) == 0o600
    assert out["successes"] == 1 and out["problems"] == []
    assert json.loads((tmp_path / "state" / FETCH_FILE).read_text()) == {"newest_day": "2026-11-10"}
    # Yesterday is in, so no fetch is wanted: the scan sets the wait, and later rounds don't fetch.
    assert out["sleeps"] == [600.0]
    out["now"] += 3 * 3600
    await run_once(source, context)
    assert fetcher.calls == 1


async def test_a_dropped_export_of_yesterday_means_no_fetch(tmp_path):
    fetcher = FakeFetcher(export_of("2026-11-10"))
    source, context, out = harness(tmp_path, fetcher)
    (tmp_path / "inbox").mkdir()
    (tmp_path / "inbox" / "mine.json").write_bytes(export_of("2026-11-10"))
    await run_once(source, context)
    assert fetcher.calls == 0 and out["successes"] == 1
    # A restart knows the day from the state; the next day the 11th is missing and is fetched.
    source, context, out = harness(tmp_path, fetcher, now=NOW + 86400)
    await run_once(source, context)
    assert fetcher.calls == 1


async def test_without_state_the_newest_imported_file_says_how_far_it_is(tmp_path):
    fetcher = FakeFetcher(export_of("2026-11-10"))
    imported = tmp_path / "inbox" / "imported"
    imported.mkdir(parents=True)
    (imported / "yesterday.json").write_bytes(export_of("2026-11-10"))
    os.utime(imported / "yesterday.json", (2, 2))
    # More recently imported, but they say nothing about a day.
    (imported / "broken.json").write_text("{not json")
    (imported / "empty.json").write_text("[]")
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / FETCH_FILE).write_text("{oops")
    source, context, _ = harness(tmp_path, fetcher)
    await run_once(source, context)
    assert fetcher.calls == 0
    # What the files said is kept, so a restart reads the state rather than the files.
    assert json.loads((tmp_path / "state" / FETCH_FILE).read_text()) == {"newest_day": "2026-11-10"}


async def test_without_state_an_older_imported_file_means_a_fetch(tmp_path):
    fetcher = FakeFetcher(export_of("2026-11-10"))
    imported = tmp_path / "inbox" / "imported"
    imported.mkdir(parents=True)
    (imported / "old.json").write_bytes(export_of("2026-11-08"))
    source, context, _ = harness(tmp_path, fetcher)
    await run_once(source, context)
    assert fetcher.calls == 1


async def test_an_export_without_yesterday_is_tried_again_after_the_retry_time(tmp_path):
    fetcher = FakeFetcher(export_of("2026-11-09"))
    source, context, out = harness(tmp_path, fetcher)
    await run_once(source, context)
    assert fetcher.calls == 1 and out["successes"] == 1
    # The retry is 30 minutes away, later than the 10-minute scan.
    assert out["sleeps"] == [600.0]
    out["now"] += 1799
    await run_once(source, context)
    assert fetcher.calls == 1 and out["sleeps"][-1] == pytest.approx(1.0)
    out["now"] += 1
    fetcher.result = export_of("2026-11-10")
    await run_once(source, context)
    assert fetcher.calls == 2
    assert json.loads((tmp_path / "state" / FETCH_FILE).read_text()) == {"newest_day": "2026-11-10"}


async def test_a_failed_fetch_is_reported_and_not_a_success(tmp_path):
    fetcher = FakeFetcher(PortalError("login refused: nope"))
    source, context, out = harness(tmp_path, fetcher)
    await run_once(source, context)
    assert out["problems"] == ["fetching from the portal failed: login refused: nope"]
    assert out["successes"] == 0 and fetcher.calls == 1
    out["now"] += 1800
    fetcher.result = httpx2.ConnectError("")
    await run_once(source, context)
    assert out["problems"][-1] == "fetching from the portal failed: ConnectError"


def test_the_portal_account_is_read_when_built(tmp_path):
    (tmp_path / "u").write_text(USER + "\n")
    (tmp_path / "p").write_text(PASSWORD + "\n")
    adapter = ADAPTERS["vaillant_export"]
    params = adapter.params.model_validate(
        {
            "inbox": str(tmp_path / "in"),
            "fetch": {"username_file": str(tmp_path / "u"), "password_file": str(tmp_path / "p")},
        }
    )
    source = adapter.build(params, None, "heating_export")
    assert isinstance(source._portal, Portal) and source._portal._password == PASSWORD
    assert (
        source._portal._url == portal.PORTAL_URL
        and params.fetch.realm == "vaillant-netherlands-b2c"
    )
    assert (
        export.build(adapter.params.model_validate({"inbox": str(tmp_path / "in")}), None)._portal
        is None
    )


async def test_a_form_without_a_challenge_is_submitted_as_it_is():
    fake = FakePortal()
    fake.form_altcha = ""
    assert json.loads(await make_portal(fake).fetch()) == RECORDS
    assert not [r for r in fake.requests if r.url.path == "/api/altcha/challenge"]


async def test_an_empty_error_element_gives_way_to_the_page_alert():
    fake = FakePortal()
    fake.login_error = (
        '<span id="input-error"> </span><div class="alert-error">Account locked</div>'
    )
    with pytest.raises(PortalError, match="login refused: Account locked"):
        await make_portal(fake, password="wrong").fetch()

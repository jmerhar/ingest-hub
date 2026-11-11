"""Fetching an export from Vaillant's EU Data Act portal, the way its web page does.

The portal is a web application: its login page hands over to the myVAILLANT login server, whose
form carries a proof-of-work check (Altcha) that a browser solves before submitting, and once
signed in, its data page asks the server for the account's system IDs and then for their data,
which it offers as the download. :class:`Portal` does the same steps:

1. the portal's sign-in action, with the account's ``realm``, answers with the login server's URL;
2. the login form is fetched, its challenge solved (:func:`solve_altcha`) and the form submitted
   with the credentials; the login server redirects back, which leaves a session cookie;
3. the data page's ``fetchStatistics`` action gives the system IDs, and ``fetchSystemData`` their
   records, written as the download would be: a JSON array, indented by two spaces.

The portal's actions are named in its scripts and their IDs change with every release of the
portal, so they are looked up on each fetch. Every fetch uses a client of its own, so its cookies
are dropped afterwards and never mixed with other requests.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import html
import json
import re
import time
from typing import Any
from urllib.parse import urljoin, urlsplit

import httpx2

PORTAL_URL = "https://www.eu-data-act.vaillant-group.com"
#: Where the login form fetches its challenge, unless the form names another path.
CHALLENGE_PATH = "/api/altcha/challenge"
#: The most counters tried for one challenge; a real one is solved in a few hundred.
MAX_COUNTER = 5_000_000
TIMEOUT = 60.0
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) ingest-hub"
SCRIPT = re.compile(r"/_next/static/chunks/[A-Za-z0-9_./~-]+\.js")
ACTION = re.compile(r'createServerReference\)\("([0-9a-f]{40,})",[^)]*?"(\w+)"\)')
ROW = re.compile(r"([0-9a-f]+):")


class PortalError(Exception):
    """The portal could not be signed in to or read; the message says why."""


def solve_altcha(challenge: dict[str, Any], max_counter: int = MAX_COUNTER) -> str:
    """The form value that answers an Altcha challenge, as its widget would send it.

    Two formats exist. The newer gives ``parameters``: find the counter whose 4-byte big-endian
    form, appended to the hex ``nonce``, yields a PBKDF2 key (over ``salt``, ``cost`` iterations,
    ``keyLength`` bytes) starting with ``keyPrefix``. The older gives ``challenge``, ``salt`` and
    ``maxnumber``: find the number whose SHA-256 of salt and number is the challenge.
    """
    start = time.monotonic()
    if isinstance(challenge.get("parameters"), dict):
        parameters = challenge["parameters"]
        algorithm = str(parameters.get("algorithm", ""))
        if not algorithm.startswith("PBKDF2/SHA-"):
            raise PortalError(f"unsupported challenge algorithm {algorithm!r}")
        digest = "sha" + algorithm.removeprefix("PBKDF2/SHA-")
        nonce = bytes.fromhex(parameters["nonce"])
        salt = bytes.fromhex(parameters["salt"])
        prefix = str(parameters["keyPrefix"]).lower()
        cost, length = int(parameters["cost"]), int(parameters.get("keyLength", 32))
        for counter in range(max_counter):
            key = hashlib.pbkdf2_hmac(
                digest, nonce + counter.to_bytes(4, "big"), salt, cost, length
            )
            if key.hex().startswith(prefix):
                solution = {
                    "counter": counter,
                    "derivedKey": key.hex(),
                    "time": round((time.monotonic() - start) * 1000),
                }
                payload = {
                    "challenge": {"parameters": parameters, "signature": challenge["signature"]},
                    "solution": solution,
                }
                return _encode(payload)
    elif "challenge" in challenge:
        if challenge.get("algorithm", "SHA-256") != "SHA-256":
            raise PortalError(f"unsupported challenge algorithm {challenge['algorithm']!r}")
        salt_text, target = str(challenge["salt"]), str(challenge["challenge"]).lower()
        for number in range(min(int(challenge.get("maxnumber", max_counter)), max_counter) + 1):
            if hashlib.sha256(f"{salt_text}{number}".encode()).hexdigest() == target:
                return _encode(
                    {
                        "algorithm": "SHA-256",
                        "challenge": challenge["challenge"],
                        "number": number,
                        "salt": salt_text,
                        "signature": challenge["signature"],
                        "took": round((time.monotonic() - start) * 1000),
                    }
                )
    else:
        raise PortalError("the login form's challenge is in an unknown format")
    raise PortalError("the login form's challenge has no solution within the counters tried")


def _encode(payload: dict[str, Any]) -> str:
    return base64.b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode()


def action_result(body: str) -> Any:
    """The value a portal action answered with, from its React server component stream.

    The stream is rows ``<hex id>:<JSON>``, or ``<hex id>:T<hex byte length>,<text>`` for long
    text; row 0 names the result (``{"a": "$@1"}``), and ``"$<id>"`` or ``"$@<id>"`` strings
    stand for other rows, ``"$$..."`` for a string starting with ``$``.
    """
    rows = _rows(body)
    if "0" not in rows:
        raise PortalError("the portal's answer has no result")
    head = rows["0"]
    result = head.get("a") if isinstance(head, dict) and "a" in head else head
    return _resolve(result, rows, set())


def _rows(body: str) -> dict[str, Any]:
    data = body.encode()
    rows: dict[str, Any] = {}
    at = 0
    while at < len(data):
        if data[at : at + 1] == b"\n":
            at += 1
            continue
        match = ROW.match(data[at : at + 16].decode("ascii", "replace"))
        if not match:
            raise PortalError("the portal's answer cannot be read")
        key = match.group(1)
        at += match.end()
        if data[at : at + 1] == b"T":
            comma = data.index(b",", at)
            length = int(data[at + 1 : comma], 16)
            rows[key] = data[comma + 1 : comma + 1 + length].decode()
            at = comma + 1 + length
        else:
            end = data.find(b"\n", at)
            end = len(data) if end < 0 else end
            try:
                rows[key] = json.loads(data[at:end])
            except ValueError as error:
                raise PortalError(f"the portal's answer cannot be read: {error}") from None
            at = end + 1
    return {key.lower().lstrip("0") or "0": value for key, value in rows.items()}


def _resolve(value: Any, rows: dict[str, Any], seen: set[str]) -> Any:
    if isinstance(value, str) and value.startswith("$"):
        if value.startswith("$$"):
            return value[1:]
        if value == "$undefined":
            return None
        ref = value[2:] if value.startswith("$@") else value[1:]
        key = ref.lower().lstrip("0") or "0"
        if re.fullmatch(r"[0-9a-f]+", key) and key in rows and key not in seen:
            return _resolve(rows[key], rows, seen | {key})
        return value
    if isinstance(value, list):
        return [_resolve(item, rows, seen) for item in value]
    if isinstance(value, dict):
        return {k: _resolve(v, rows, seen) for k, v in value.items()}
    return value


def _form_error(page: str) -> str:
    """The login server's message on a page that sent the form back, as plain text."""
    for pattern in (
        r'id="input-error[^"]*"[^>]*>(.*?)</',
        r'class="[^"]*(?:alert|feedback|kc-feedback-text)[^"]*"[^>]*>(.*?)</(?:span|div|p)>',
    ):
        match = re.search(pattern, page, re.S)
        if match:
            text = re.sub(r"<[^>]+>|\s+", " ", html.unescape(match.group(1))).strip()
            if text:
                return text
    return "the login form came back without a message"


class Portal:
    """Signs in to the portal with one account and fetches its export."""

    def __init__(
        self,
        username: str,
        password: str,
        *,
        realm: str,
        url: str = PORTAL_URL,
        language: str = "en",
        transport: httpx2.AsyncBaseTransport | None = None,
    ) -> None:
        self._username = username
        self._password = password
        self._realm = realm
        self._url = url.rstrip("/")
        self._language = language
        self._transport = transport

    async def fetch(self) -> bytes:
        """The account's export, as the portal's download would save it."""
        async with httpx2.AsyncClient(
            transport=self._transport,
            follow_redirects=True,
            timeout=TIMEOUT,
            headers={"User-Agent": USER_AGENT},
        ) as client:
            login_page = f"{self._url}/{self._language}/login"
            actions = await self._actions(client, login_page)
            auth_url = await self._call(
                client, login_page, actions, "doSignIn", self._realm, "/euda-data"
            )
            if not isinstance(auth_url, str) or not auth_url.startswith("http"):
                raise PortalError("the portal's sign-in did not give a login address")
            form_page = await self._get(client, auth_url)
            signed_in = await self._sign_in(client, form_page)
            data_page = f"{self._url}/{self._language}/euda-data"
            if urlsplit(str(signed_in.url)).path.rstrip("/") != f"/{self._language}/euda-data":
                signed_in = await self._get(client, data_page)
            actions = self._scripts_actions(await self._scripts(client, signed_in.text))
            stats = await self._call(client, data_page, actions, "fetchStatistics")
            ids = stats.get("allSystemIds") if isinstance(stats, dict) else None
            if not isinstance(ids, list):
                raise PortalError("the portal did not list the account's systems")
            records = (
                await self._call(client, data_page, actions, "fetchSystemData", ids) if ids else []
            )
            if not isinstance(records, list):
                raise PortalError("the portal's data is not a list of records")
            return json.dumps(records, indent=2, ensure_ascii=False).encode()

    async def _get(self, client: httpx2.AsyncClient, url: str) -> httpx2.Response:
        response = await client.get(url)
        if response.status_code != 200:
            raise PortalError(f"{urlsplit(url).path} answered HTTP {response.status_code}")
        return response

    async def _sign_in(
        self, client: httpx2.AsyncClient, form_page: httpx2.Response
    ) -> httpx2.Response:
        """Solve the login form's challenge and submit the form; the page it ends on."""
        page = form_page.text
        form = re.search(r'<form[^>]+action="([^"]+)"', page)
        if not form:
            raise PortalError("the login page has no form")
        action = urljoin(str(form_page.url), html.unescape(form.group(1)))
        fields = {"username": self._username, "password": self._password, "credentialId": ""}
        widget = re.search(r'<altcha-widget[^>]*?challenge="([^"]+)"', page, re.S)
        if widget or "altcha" in page.lower():
            path = html.unescape(widget.group(1)) if widget else CHALLENGE_PATH
            challenge = await client.get(urljoin(str(form_page.url), path))
            if challenge.status_code != 200:
                raise PortalError(
                    f"the login form's challenge answered HTTP {challenge.status_code}"
                )
            try:
                fields["altcha"] = await asyncio.to_thread(solve_altcha, challenge.json())
            except ValueError as error:
                raise PortalError(f"the login form's challenge cannot be read: {error}") from None
        response = await client.post(action, data=fields)
        if response.status_code != 200:
            raise PortalError(f"the login answered HTTP {response.status_code}")
        if urlsplit(str(response.url)).netloc == urlsplit(action).netloc:
            raise PortalError(f"login refused: {_form_error(response.text)}")
        return response

    async def _scripts(self, client: httpx2.AsyncClient, page: str) -> list[str]:
        scripts = []
        for path in dict.fromkeys(SCRIPT.findall(page)):
            response = await client.get(self._url + path)
            if response.status_code == 200:
                scripts.append(response.text)
        return scripts

    @staticmethod
    def _scripts_actions(scripts: list[str]) -> dict[str, str]:
        """Action IDs by name, from the page's scripts."""
        return {name: ident for script in scripts for ident, name in ACTION.findall(script)}

    async def _actions(self, client: httpx2.AsyncClient, url: str) -> dict[str, str]:
        page = await self._get(client, url)
        return self._scripts_actions(await self._scripts(client, page.text))

    async def _call(
        self, client: httpx2.AsyncClient, url: str, actions: dict[str, str], name: str, *args: Any
    ) -> Any:
        """Call one of the portal's actions, as its page would, and give its result."""
        if name not in actions:
            raise PortalError(f"the portal's page has no {name} action")
        response = await client.post(
            url,
            content=json.dumps(list(args)),
            headers={
                "Next-Action": actions[name],
                "Accept": "text/x-component",
                "Content-Type": "text/plain;charset=UTF-8",
            },
        )
        if response.status_code != 200:
            raise PortalError(f"{name} answered HTTP {response.status_code}")
        return action_result(response.text)


__all__ = ["PORTAL_URL", "Portal", "PortalError", "action_result", "solve_altcha"]

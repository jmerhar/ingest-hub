"""The web interface: the status page, its JSON, the health endpoint, uploads and sign-ins.

The application's lifespan runs the sources and alerts, so one process both collects and serves:
``ingest-hub run`` with ``WEB_PORT`` set starts it through ``jigkit.web.serve``.

- ``GET /health``: 200 while the runtime is running and its status is fresh, 503 otherwise. It
  says nothing about destinations, since restarting the process would not bring a database back
  and would only interrupt spooling.
- ``GET /api/status``: every source's status, the destinations by kind and database, and the
  version. No URL, token or other credential appears in it.
- ``POST /api/sources/{name}/files``: hand a file to a source that imports files from a folder
  (``accepts_files`` in its status, with ``upload_max_bytes`` and ``upload_help``), as the request
  body, with its name in ``X-Filename``. It is
  saved in the source's inbox, under the upload time and that name, and the source is woken to
  import it; the answer gives the saved name and the server's time (``at``), which the source's
  next ``last_success`` or ``last_error_at`` follows. The body is written to disk as it arrives,
  up to the source's ``upload_max_bytes`` (``UPLOAD_MAX_BYTES`` by default).
- ``POST /api/sources/{name}/sign-in``: start signing in a source that needs a person to sign in
  through a browser (``sign_in`` in its status). The answer, and the source's ``sign_in.current``
  from then on, say what to open and do. When the step asks for no answer, the sign-in finishes by
  itself once the person has signed in; otherwise ``POST /api/sources/{name}/sign-in/answer`` with
  ``{"answer": "..."}`` finishes it. Either way a finished sign-in is kept by the source and the
  source restarted at once.
- ``GET /``: the status page, which renders ``/api/status`` and refreshes itself, and its icon at
  ``/favicon.svg``.

Every write under ``/api`` carries ``X-Requested-With: ingest-hub`` (:data:`WRITE_HEADER_VALUE`),
and an ``Origin``, if it has one, of the application's own host or ``PUBLIC_URL``; jigkit's write
guard answers anything else 403, so a page on another site cannot make a browser write here.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
import tempfile
import time
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

import httpx2
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from jigkit.web import install_security_headers
from jigkit.web.guard import install_write_guard

from ingest_hub import __version__
from ingest_hub.alerts import Sender
from ingest_hub.collect import AcceptsFiles, SignInStep, SignsIn
from ingest_hub.config import load_config
from ingest_hub.hub import Assembly, assemble
from ingest_hub.settings import Settings

#: The environment variable through which ``run --only`` reaches the application factory.
ONLY_ENV = "INGEST_HUB_ONLY"
#: The ``X-Requested-With`` value every write carries; the status page sends it.
WRITE_HEADER_VALUE = "ingest-hub"
#: The status is stale when the runtime has not refreshed it for this long.
STALE_SECONDS = 180.0

#: The largest file a source is handed through the web interface, unless it sets its own.
UPLOAD_MAX_BYTES = 64 * 1024 * 1024
#: Characters an uploaded file's saved name may keep; others become ``-``.
UNSAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")

#: The page's own policy: its script and styles are inline, and it only fetches from itself.
PAGE_POLICY = (
    "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
    "connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'"
)


@dataclass
class SignIn:
    """A sign-in started through the web interface."""

    step: SignInStep
    #: ``waiting`` (for the person, or for their answer), ``done`` or ``failed``.
    state: str
    #: When the state last changed (Unix seconds).
    at: float
    #: What the source said when it finished, or why it failed.
    message: str | None = None
    #: The background wait of a sign-in whose step asks for no answer.
    task: asyncio.Task[None] | None = None

    def snapshot(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "at": self.at,
            "url": self.step.url,
            "code": self.step.code,
            "instructions": self.step.instructions,
            "answer": self.step.answer,
            "message": self.message,
        }


def status_document(
    assembly: Assembly, revision: str | None, sign_ins: Mapping[str, SignIn] | None = None
) -> dict[str, Any]:
    """The status as ``/api/status`` returns it."""
    snapshot = assembly.runtime.snapshot()
    sign_ins = sign_ins or {}
    return {
        "version": __version__,
        "revision": revision,
        "now": snapshot["updated_at"],
        "started_at": snapshot["started_at"],
        "alerts": assembly.monitor is not None,
        "destinations": {
            name: {"kind": d.kind, "database": d.database}
            for name, d in assembly.config.destinations.items()
        },
        "sources": [
            {
                **source,
                **_upload(assembly.runtime.source(source["name"])),
                "sign_in": _sign_in(assembly, source["name"], sign_ins.get(source["name"])),
            }
            for source in snapshot["sources"]
        ],
    }


def _upload(source: object) -> dict[str, Any]:
    """What the status says about handing a source files: the name endings it takes
    (``accepts_files``), the largest file (``upload_max_bytes``) and what to hand it
    (``upload_help``); all None when it takes none."""
    if not isinstance(source, AcceptsFiles):
        return {"accepts_files": None, "upload_max_bytes": None, "upload_help": None}
    help_text = getattr(source, "upload_help", None)
    return {
        "accepts_files": list(source.file_suffixes),
        "upload_max_bytes": upload_limit(source),
        "upload_help": help_text if isinstance(help_text, str) and help_text else None,
    }


def _sign_in(assembly: Assembly, name: str, current: SignIn | None) -> dict[str, Any] | None:
    """What the status says about signing a source in: whether a sign-in is kept
    (``signed_in``) and the one started through the web interface, if any (``current``); None
    for a source that needs none."""
    source = assembly.runtime.source(name)
    if not isinstance(source, SignsIn):
        return None
    return {
        "signed_in": source.signed_in(assembly.runtime.state_dir(name)),
        "current": current.snapshot() if current else None,
    }


def _message(error: Exception) -> str:
    return str(error) or type(error).__name__


def saved_name(filename: str, suffixes: tuple[str, ...], moment: float) -> str | None:
    """The name an upload is saved under in an inbox: the upload time (UTC) and the file's own
    name reduced to safe characters, or None when the name has none of the accepted endings."""
    name = UNSAFE_NAME.sub("-", Path(filename.replace("\\", "/")).name).strip(".-")
    if not name.lower().endswith(tuple(s.lower() for s in suffixes)):
        return None
    return time.strftime("%Y%m%dT%H%M%SZ-", time.gmtime(moment)) + name


def _partial(inbox: Path) -> Path:
    """A new, empty file in an inbox for an upload to be written into, readable by this user only,
    since an upload may hold anything. Its name starts with a dot and ends in ``.part``, so no
    source imports it."""
    inbox.mkdir(parents=True, exist_ok=True)
    descriptor, path = tempfile.mkstemp(prefix=".upload-", suffix=".part", dir=inbox)
    os.close(descriptor)
    return Path(path)


def _place(inbox: Path, name: str, partial: Path) -> str:
    """Give a written upload its name in the inbox, keeping any file already there under that
    name, so no reader ever sees it half-written; returns the name it was saved under."""
    stem, dot, suffix = name.rpartition(".")
    target, n = inbox / name, 1
    while target.exists():
        n += 1
        target = inbox / f"{stem}-{n}{dot}{suffix}"
    os.replace(partial, target)
    return target.name


def upload_limit(source: AcceptsFiles) -> int:
    """The largest file a source may be handed: its own ``upload_max_bytes``, or
    :data:`UPLOAD_MAX_BYTES`."""
    limit = getattr(source, "upload_max_bytes", None)
    return limit if isinstance(limit, int) and limit > 0 else UPLOAD_MAX_BYTES


def build_app(
    settings: Settings,
    *,
    only: str | None = None,
    sender: Sender | None = None,
    clock: Callable[[], float] = time.time,
) -> FastAPI:
    """The application, which runs the configured sources for as long as it serves."""
    config = load_config(settings.resolved_config_file)
    state: dict[str, Any] = {}
    #: The sign-in started last through the web interface, per source.
    sign_ins: dict[str, SignIn] = {}

    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        async with httpx2.AsyncClient() as client:
            assembly = assemble(
                config,
                client,
                data_dir=settings.data_dir,
                spool_max_bytes=settings.spool_max_bytes,
                only=only,
                sender=sender,
            )
            task = asyncio.create_task(assembly.run(), name="collect")
            state.update(assembly=assembly, task=task)
            try:
                yield
            finally:
                for current in sign_ins.values():
                    if current.task is not None:
                        current.task.cancel()
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    app = FastAPI(
        title="ingest-hub",
        version=__version__,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    install_security_headers(app)
    install_write_guard(app, value=WRITE_HEADER_VALUE, public_url=settings.public_url)

    @app.get("/health")
    async def health() -> JSONResponse:
        """Whether the runtime is running and keeping its status fresh."""
        body: dict[str, Any] = {"version": __version__, "revision": settings.revision}
        task: asyncio.Task[None] | None = state.get("task")
        if task is None or task.done():
            return JSONResponse({**body, "status": "error", "reason": "not running"}, 503)
        age = clock() - state["assembly"].runtime.refreshed_at
        if age > STALE_SECONDS:
            reason = f"status not refreshed for {age:.0f} s"
            return JSONResponse({**body, "status": "error", "reason": reason}, 503)
        return JSONResponse({**body, "status": "ok"})

    @app.get("/api/status")
    async def status() -> JSONResponse:
        """Every source's status, for the page and for scripts."""
        if "assembly" not in state:
            return JSONResponse({"status": "starting"}, 503)
        return JSONResponse(status_document(state["assembly"], settings.revision, sign_ins))

    @app.post("/api/sources/{name}/files")
    async def upload(name: str, request: Request) -> JSONResponse:
        """Save the request body in a source's inbox and wake the source to import it."""
        assembly: Assembly | None = state.get("assembly")
        source = assembly.runtime.source(name) if assembly else None
        if not isinstance(source, AcceptsFiles):
            return JSONResponse({"error": f"{name} does not take files"}, 404)
        filename = request.headers.get("x-filename", "")
        if not filename:
            return JSONResponse({"error": "the X-Filename header is required"}, 400)
        target = saved_name(filename, source.file_suffixes, clock())
        if target is None:
            wanted = ", ".join(source.file_suffixes)
            return JSONResponse({"error": f"{name} takes {wanted} files"}, 415)
        limit = upload_limit(source)
        too_large = JSONResponse(
            {"error": f"{name} takes files of at most {limit // (1024 * 1024)} MiB"}, 413
        )
        with contextlib.suppress(ValueError):
            if int(request.headers.get("content-length", "")) > limit:
                return too_large
        # Written to disk as it arrives, so a large upload never has to fit in memory.
        partial = await asyncio.to_thread(_partial, source.inbox)
        try:
            size = 0
            with partial.open("wb") as file:
                async for chunk in request.stream():
                    size += len(chunk)
                    if size > limit:
                        return too_large
                    await asyncio.to_thread(file.write, chunk)
            if not size:
                return JSONResponse({"error": "the file is empty"}, 400)
            saved = await asyncio.to_thread(_place, source.inbox, target, partial)
        finally:
            partial.unlink(missing_ok=True)
        source.wake()
        return JSONResponse({"saved": saved, "bytes": size, "at": clock()}, 201)

    def signs_in(name: str) -> tuple[Assembly, SignsIn] | None:
        assembly: Assembly | None = state.get("assembly")
        source = assembly.runtime.source(name) if assembly else None
        return (assembly, source) if assembly and isinstance(source, SignsIn) else None

    async def finish(
        assembly: Assembly, name: str, source: SignsIn, current: SignIn, answer: str | None
    ) -> None:
        """Finish a sign-in; once kept, start the source again so it uses it at once."""
        message = await source.finish_sign_in(assembly.runtime.state_dir(name), answer)
        current.state, current.message, current.at = "done", message, clock()
        assembly.runtime.restart(name)

    async def wait_for_person(
        assembly: Assembly, name: str, source: SignsIn, current: SignIn
    ) -> None:
        """Finish a sign-in that asks for no answer, in the background, recording a failure."""
        try:
            await finish(assembly, name, source, current, None)
        except Exception as error:
            current.state, current.message, current.at = "failed", _message(error), clock()

    @app.post("/api/sources/{name}/sign-in")
    async def start_sign_in(name: str) -> JSONResponse:
        """Start signing a source in, replacing a sign-in started before."""
        found = signs_in(name)
        if found is None:
            return JSONResponse({"error": f"{name} needs no sign-in"}, 404)
        assembly, source = found
        if (previous := sign_ins.pop(name, None)) and previous.task is not None:
            previous.task.cancel()
        try:
            step = await source.start_sign_in(assembly.runtime.state_dir(name))
        except Exception as error:
            return JSONResponse({"error": _message(error)}, 502)
        current = sign_ins[name] = SignIn(step, "waiting", clock())
        if step.answer is None:
            current.task = asyncio.create_task(
                wait_for_person(assembly, name, source, current), name=f"{name}:sign-in"
            )
        return JSONResponse(current.snapshot(), 201)

    @app.post("/api/sources/{name}/sign-in/answer")
    async def answer_sign_in(name: str, request: Request) -> JSONResponse:
        """Finish a sign-in with the person's answer."""
        found = signs_in(name)
        if found is None:
            return JSONResponse({"error": f"{name} needs no sign-in"}, 404)
        assembly, source = found
        current = sign_ins.get(name)
        if current is None or current.step.answer is None or current.state != "waiting":
            return JSONResponse({"error": "no sign-in is waiting for an answer; start one"}, 409)
        try:
            body = await request.json()
        except ValueError:
            body = None
        answer = body.get("answer") if isinstance(body, dict) else None
        if not isinstance(answer, str) or not answer.strip():
            return JSONResponse({"error": f"paste {current.step.answer}"}, 400)
        try:
            await finish(assembly, name, source, current, answer)
        except Exception as error:
            current.message = _message(error)
            return JSONResponse({"error": current.message}, 400)
        return JSONResponse(current.snapshot())

    @app.get("/favicon.svg")
    async def favicon() -> Response:
        """The page's icon: streams flowing into a database."""
        svg = resources.files("ingest_hub").joinpath("favicon.svg").read_bytes()
        return Response(
            svg, media_type="image/svg+xml", headers={"Cache-Control": "public, max-age=86400"}
        )

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon_ico() -> RedirectResponse:
        """Browsers ask for this path unprompted; the icon is an SVG."""
        return RedirectResponse("/favicon.svg", status_code=301)

    @app.get("/", response_class=HTMLResponse)
    async def page() -> HTMLResponse:
        """The status page."""
        html = resources.files("ingest_hub").joinpath("status.html").read_text(encoding="utf-8")
        return HTMLResponse(
            html, headers={"Content-Security-Policy": PAGE_POLICY, "Cache-Control": "no-cache"}
        )

    return app


def create_app() -> FastAPI:
    """The factory ``jigkit.web.serve`` imports: settings from the environment."""
    return build_app(Settings(), only=os.environ.get(ONLY_ENV) or None)

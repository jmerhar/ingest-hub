"""The web interface: a read-only status page, its JSON, and the health endpoint.

The application's lifespan runs the sources and alerts, so one process both collects and serves:
``ingest-hub run`` with ``WEB_PORT`` set starts it through ``jigkit.web.serve``.

- ``GET /health``: 200 while the runtime is running and its status is fresh, 503 otherwise. It
  says nothing about destinations, since restarting the process would not bring a database back
  and would only interrupt spooling.
- ``GET /api/status``: every source's status, the destinations by kind and database, and the
  version. No URL, token or other credential appears in it.
- ``POST /api/sources/{name}/files``: hand a file to a source that imports files from a folder
  (``accepts_files`` in its status), as the request body, with its name in ``X-Filename``. It is
  saved in the source's inbox, under the upload time and that name, and the source is woken to
  import it; the answer gives the saved name and the server's time (``at``), which the source's
  next ``last_success`` or ``last_error_at`` follows. The body is written to disk as it arrives,
  up to the source's ``upload_max_bytes`` (``UPLOAD_MAX_BYTES`` by default). The header is
  required, so a browser on another site cannot post one without a CORS preflight, which this
  application never allows.
- ``GET /``: the status page, which renders ``/api/status`` and refreshes itself, and its icon at
  ``/favicon.svg``.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
import tempfile
import time
from collections.abc import AsyncIterator, Callable
from importlib import resources
from pathlib import Path
from typing import Any

import httpx2
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from jigkit.web import install_security_headers

from ingest_hub import __version__
from ingest_hub.alerts import Sender
from ingest_hub.collect import AcceptsFiles
from ingest_hub.config import load_config
from ingest_hub.hub import Assembly, assemble
from ingest_hub.settings import Settings

#: The environment variable through which ``run --only`` reaches the application factory.
ONLY_ENV = "INGEST_HUB_ONLY"
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


def status_document(assembly: Assembly, revision: str | None) -> dict[str, Any]:
    """The status as ``/api/status`` returns it."""
    snapshot = assembly.runtime.snapshot()
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
            {**source, "accepts_files": _accepts(assembly.runtime.source(source["name"]))}
            for source in snapshot["sources"]
        ],
    }


def _accepts(source: object) -> list[str] | None:
    """The name endings of the files a source can be handed, or None when it takes none."""
    return list(source.file_suffixes) if isinstance(source, AcceptsFiles) else None


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
        return JSONResponse(status_document(state["assembly"], settings.revision))

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

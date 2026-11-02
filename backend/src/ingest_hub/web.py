"""The web interface: a read-only status page, its JSON, and the health endpoint.

The application's lifespan runs the sources and alerts, so one process both collects and serves:
``ingest-hub run`` with ``WEB_PORT`` set starts it through ``jigkit.web.serve``.

- ``GET /health``: 200 while the runtime is running and its status is fresh, 503 otherwise. It
  says nothing about destinations, since restarting the process would not bring a database back
  and would only interrupt spooling.
- ``GET /api/status``: every source's status, the destinations by kind and database, and the
  version. No URL, token or other credential appears in it.
- ``GET /``: the status page, which renders ``/api/status`` and refreshes itself, and its icon at
  ``/favicon.svg``.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from collections.abc import AsyncIterator, Callable
from importlib import resources
from typing import Any

import httpx2
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from jigkit.web import install_security_headers

from ingest_hub import __version__
from ingest_hub.alerts import Sender
from ingest_hub.config import load_config
from ingest_hub.hub import Assembly, assemble
from ingest_hub.settings import Settings

#: The environment variable through which ``run --only`` reaches the application factory.
ONLY_ENV = "INGEST_HUB_ONLY"
#: The status is stale when the runtime has not refreshed it for this long.
STALE_SECONDS = 180.0

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
        "sources": snapshot["sources"],
    }


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

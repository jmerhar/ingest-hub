"""What the sources that import files dropped into a folder share: waiting for the next scan or
an upload, moving an imported file aside, and keeping small state files."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from pathlib import Path
from typing import Any

from ingest_hub.collect import SourceContext

#: The folders beside an inbox that its files are moved to once handled.
IMPORTED = "imported"
FAILED = "failed"


async def wait_or_wake(context: SourceContext, woken: asyncio.Event, seconds: float) -> None:
    """Wait ``seconds`` for the next scan, or until ``woken`` is set, such as by an upload."""
    timer = asyncio.ensure_future(context.sleep(seconds))
    wake = asyncio.ensure_future(woken.wait())
    done, pending = await asyncio.wait({timer, wake}, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    for task in done:
        task.result()


def move_aside(path: Path, folder: str) -> None:
    """Move a file into a folder beside it, replacing an earlier file of the same name."""
    target = path.parent / folder
    target.mkdir(exist_ok=True)
    os.replace(path, target / path.name)


def write_json(path: Path, data: Any, *, indent: int | None = None, private: bool = False) -> None:
    """Write a JSON file atomically, creating its folder; with ``private``, readable by its owner
    only, from the moment it is created, as a file holding a token must be."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600 if private else 0o666
    )
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(data, indent=indent))
    os.replace(temporary, path)


__all__ = ["FAILED", "IMPORTED", "move_aside", "wait_or_wake", "write_json"]

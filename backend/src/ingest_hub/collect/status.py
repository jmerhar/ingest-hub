"""Per-source counters and state, for logs, the status file and a status page."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from jigkit.spool import Spool


@dataclass
class SourceStatus:
    """What one source has done so far. Also its delivery's events (``DeliveryEvents``)."""

    name: str
    kind: str
    destination: str
    clock: Callable[[], float] = field(default=time.time, repr=False)

    #: starting, running, backing-off or stopped.
    state: str = "starting"
    restarts: int = 0
    last_success: float | None = None
    last_error: str | None = None
    last_error_at: float | None = None
    #: When a batch of this source was last accepted by its destination.
    last_written: float | None = None
    points_emitted: int = 0
    batches_written: int = 0
    write_failures: int = 0
    batches_spooled: int = 0
    batches_rejected: int = 0
    #: Whether the last heartbeat call got through; None before the first, or without a monitor.
    heartbeat_ok: bool | None = None
    #: Each stream's latest delivered ``complete_until``, in Unix milliseconds.
    progress: dict[str, int] = field(default_factory=dict)

    def record_write(self, ok: bool) -> None:
        """Count a write attempt."""
        if ok:
            self.batches_written += 1
            self.last_written = self.clock()
        else:
            self.write_failures += 1

    def record_spooled(self) -> None:
        """Count a batch queued for later."""
        self.batches_spooled += 1

    def record_rejected_write(self) -> None:
        """Count a batch the destination refused for good."""
        self.batches_rejected += 1

    def record_success(self) -> None:
        """Note a successful round of the source's own work."""
        self.last_success = self.clock()

    def record_error(self, message: str) -> None:
        """Note an error, keeping the latest."""
        self.last_error = message
        self.last_error_at = self.clock()

    def record_progress(self, stream: str, complete_until_ms: int) -> None:
        """Note how far a stream is complete, once the batch carrying it is safe."""
        self.progress[stream] = max(complete_until_ms, self.progress.get(stream, 0))

    def record_heartbeat(self, ok: bool) -> None:
        """Note whether a heartbeat call got through."""
        self.heartbeat_ok = ok

    def snapshot(self, spool: Spool | None = None) -> dict[str, Any]:
        """The status as plain data, with the spool's backlog when given."""
        data = {
            key: getattr(self, key)
            for key in (
                "name",
                "kind",
                "destination",
                "state",
                "restarts",
                "last_success",
                "last_error",
                "last_error_at",
                "last_written",
                "points_emitted",
                "batches_written",
                "write_failures",
                "batches_spooled",
                "batches_rejected",
                "heartbeat_ok",
            )
        }
        data["progress"] = dict(self.progress)
        if spool is not None:
            stats = spool.stats()
            data["spool"] = {
                "files": stats.files,
                "bytes": stats.bytes,
                "oldest_seconds": stats.oldest_seconds,
                "dropped": spool.dropped,
                "rejected_kept": spool.rejected_count(),
            }
        return data

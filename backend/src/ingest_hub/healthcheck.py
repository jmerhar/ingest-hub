"""The container's health probe: ``python -m ingest_hub.healthcheck``.

Healthy while the runtime keeps rewriting its status file. One source failing does not make the
container unhealthy, since restarting it would not help that source; the status file and each
source's heartbeat say which source is in trouble.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from ingest_hub.collect.runtime import STATUS_FILE, STATUS_INTERVAL_SECONDS

#: The status file may be this many rewrite intervals old before the process counts as stuck.
STALE_AFTER_INTERVALS = 4


def check(data_dir: Path, now: float | None = None) -> str | None:
    """None when the status file is fresh, or why the process looks unhealthy."""
    path = data_dir / STATUS_FILE
    try:
        age = (time.time() if now is None else now) - path.stat().st_mtime
    except OSError:
        return f"no status file at {path}"
    limit = STATUS_INTERVAL_SECONDS * STALE_AFTER_INTERVALS
    if age > limit:
        return f"the status file is {age:.0f} s old (limit {limit:.0f} s)"
    return None


def main() -> int:
    """Probe the data directory named by DATA_DIR."""
    reason = check(Path(os.environ.get("DATA_DIR") or "data"))
    if reason:
        print(f"unhealthy: {reason}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

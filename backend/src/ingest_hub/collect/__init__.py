"""The adapter runtime: sources run side by side, each delivering to its own destination.

Self-contained: this package imports only jigkit and the standard library, never the rest of
ingest-hub, so it can move into jigkit unchanged if another application needs it.

- A :class:`~ingest_hub.collect.source.Source` produces points. It runs in its own task, and when
  it fails the runtime restarts it after a back-off of its own, so one failing API never stalls
  another source.
- Each source has its own spool and delivery in front of its destination's sink, so a backlog
  from one source never holds back another's writes, and its own heartbeat, called after its
  points are written.
- A source that knows how far it has fetched reports it as progress per stream, which the runtime
  writes to the ``ingest_progress`` table in the same batch as the points it covers.
- A source that imports files from a folder (:class:`~ingest_hub.collect.source.AcceptsFiles`)
  says so, so files can be handed to it directly.
"""

from ingest_hub.collect.runtime import Runtime, SourceSpec
from ingest_hub.collect.source import (
    PROGRESS_TABLE,
    AcceptsFiles,
    Progress,
    Source,
    SourceContext,
)
from ingest_hub.collect.status import SourceStatus

__all__ = [
    "PROGRESS_TABLE",
    "AcceptsFiles",
    "Progress",
    "Runtime",
    "Source",
    "SourceContext",
    "SourceSpec",
    "SourceStatus",
]

"""Per-source status."""

from __future__ import annotations

from ingest_hub.collect import SourceStatus


def test_snapshot_without_a_spool_and_the_counters():
    status = SourceStatus("p", "tuya", "home", clock=lambda: 42.0)
    status.record_write(True)
    status.record_write(False)
    status.record_spooled()
    status.record_rejected_write()
    status.record_error("boom")
    data = status.snapshot()
    assert "spool" not in data
    assert data["batches_written"] == 1 and data["write_failures"] == 1
    assert data["batches_spooled"] == 1 and data["batches_rejected"] == 1
    assert data["last_written"] == 42.0
    assert data["last_error"] == "boom" and data["last_error_at"] == 42.0


def test_progress_keeps_the_furthest_point_and_the_latest_label():
    status = SourceStatus("p", "tuya", "home")
    status.record_progress("dev", 200, "Kitchen")
    status.record_progress("dev", 100)
    status.record_progress("dev", 150, "Kitchen plug")
    data = status.snapshot()
    assert data["progress"] == {"dev": 200}
    assert data["labels"] == {"dev": "Kitchen plug"}

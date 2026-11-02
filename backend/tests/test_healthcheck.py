"""The container health probe."""

from __future__ import annotations

import os
import time

from ingest_hub import healthcheck


def test_a_fresh_status_file_is_healthy(tmp_path):
    (tmp_path / "status.json").write_text("{}")
    assert healthcheck.check(tmp_path) is None


def test_a_stale_or_missing_status_file_is_not(tmp_path):
    assert "no status file" in healthcheck.check(tmp_path)
    path = tmp_path / "status.json"
    path.write_text("{}")
    old = time.time() - 600
    os.utime(path, (old, old))
    assert "600 s old" in healthcheck.check(tmp_path, now=old + 600)


def test_main_reads_data_dir(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    assert healthcheck.main() == 1
    assert "unhealthy" in capsys.readouterr().err
    (tmp_path / "status.json").write_text("{}")
    assert healthcheck.main() == 0

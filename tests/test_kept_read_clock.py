"""Clock fences observe retained files and atomic publication retirement."""
import os
import time

import pytest

from kept_read_clock import after_paths


def test_a_vanished_staging_file_is_observed_through_its_parent(tmp_path, monkeypatch):
    retained = tmp_path / "retained"
    retained.write_text("retained")
    future = retained.stat().st_mtime_ns + 1_000_000_000
    os.utime(retained, ns=(future, future))
    staging = tmp_path / ".publication.tmp"
    staging.write_text("staging")
    staging.unlink()
    parent = tmp_path.stat()
    after_paths(monkeypatch, retained, staging)
    assert time.clock_gettime_ns(5) > max(
        future, parent.st_mtime_ns, parent.st_ctime_ns)


def test_an_unreadable_missing_parent_is_still_an_error(tmp_path, monkeypatch):
    with pytest.raises(FileNotFoundError):
        after_paths(monkeypatch, tmp_path / "absent-parent" / "absent-file")

"""Clock fences observe retained files and atomic publication retirement."""
import os
import time
from types import SimpleNamespace

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


@pytest.mark.parametrize("name", ["authoritative", ".record.123.tmp"])
def test_strict_capture_does_not_substitute_an_authoritative_paths_parent(
        tmp_path, monkeypatch, name):
    retained = tmp_path / "retained"
    retained.write_text("retained")
    authoritative = tmp_path / name
    authoritative.write_text("record")
    authoritative.unlink()
    with pytest.raises(FileNotFoundError):
        after_paths(monkeypatch, retained, authoritative, strict=True)


def test_strict_capture_fences_existing_paths(tmp_path, monkeypatch):
    retained = tmp_path / "retained"
    retained.write_text("retained")
    observation = retained.stat()
    state = after_paths(monkeypatch, retained, strict=True)
    assert state["now"] == max(
        observation.st_mtime_ns, observation.st_ctime_ns) + 1
    assert time.clock_gettime_ns(5) == state["now"]


@pytest.mark.parametrize("mtime,ctime", [(300, 100), (100, 300)])
def test_strict_capture_uses_the_newest_mtime_or_ctime(
        tmp_path, monkeypatch, mtime, ctime):
    observations = {
        tmp_path / "older": SimpleNamespace(st_mtime_ns=200, st_ctime_ns=200),
        tmp_path / "newer": SimpleNamespace(st_mtime_ns=mtime, st_ctime_ns=ctime),
    }
    with monkeypatch.context() as capture:
        capture.setattr(os, "stat", observations.__getitem__)
        state = after_paths(capture, *observations, strict=True)
        coarse_time = time.clock_gettime_ns(5)
    assert state["now"] == 301
    assert coarse_time == 301

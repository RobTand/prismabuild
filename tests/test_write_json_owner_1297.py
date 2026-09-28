"""Pair 2 of the logical-dedup phase (issue #1297): one owner for rename-atomic JSON.

``materialize._write_json_atomic`` owns the recipe; slurm_lane's
``_write_latest`` and its ``_write_json_atomic`` wrapper are deleted and
their call sites pass ``trailing_newline=True`` (the lane's LF history).
Every test below is tmp_path-only.
"""

from __future__ import annotations

import inspect
import json
import os
import stat

import pytest

from prismabuild import materialize as mz
from prismabuild import slurm_lane as sl


def _spec_bytes(payload: dict, *, newline: bool) -> bytes:
    text = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return (text + "\n").encode("utf-8") if newline else text.encode("utf-8")


def test_default_bytes_are_bare_and_pinned(tmp_path):
    target = tmp_path / "record.json"
    payload = {"b": [1, 2], "a": "x"}
    mz._write_json_atomic(target, payload)
    assert target.read_bytes() == _spec_bytes(payload, newline=False)
    assert not target.read_bytes().endswith(b"\n")


def test_trailing_newline_mode_matches_lane_history(tmp_path):
    target = tmp_path / "latest.json"
    payload = {"schema": "v1", "job_id": "7"}
    mz._write_json_atomic(target, payload, trailing_newline=True)
    assert target.read_bytes() == _spec_bytes(payload, newline=True)
    assert json.loads(target.read_text(encoding="utf-8")) == payload


def _tmp_debris(directory):
    return [p.name for p in directory.iterdir() if p.suffix == ".tmp"]


def test_mode_and_no_debris(tmp_path):
    target = tmp_path / "record.json"
    mz._write_json_atomic(target, {"a": 1})
    assert stat.S_IMODE(target.stat().st_mode) == 0o644
    assert target.exists()
    assert _tmp_debris(tmp_path) == []


def test_failed_rename_still_cleans_its_temp(tmp_path, monkeypatch):
    target = tmp_path / "record.json"

    def failing_replace(source, destination):
        raise OSError("injected rename failure")

    monkeypatch.setattr(mz.os, "replace", failing_replace)
    with pytest.raises(OSError, match="injected rename failure"):
        mz._write_json_atomic(target, {"a": 1})
    assert _tmp_debris(tmp_path) == []
    assert not target.exists()


def test_make_parent_false_still_refuses_a_missing_parent(tmp_path):
    with pytest.raises(FileNotFoundError):
        mz._write_json_atomic(
            tmp_path / "absent" / "record.json", {"a": 1}, make_parent=False
        )


def test_lane_carries_no_duplicate_and_threads_the_flag():
    assert not hasattr(sl, "_write_latest")
    assert sl._write_json_atomic is mz._write_json_atomic
    source = inspect.getsource(sl)
    assert source.count("trailing_newline=True") == 5

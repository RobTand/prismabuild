"""A progress record replaced mid-read is read again, not called tamper (#1017).

Progress records, staged-wait records and a mover's landing report are
whole-record files their writer replaces with ``os.replace`` on every tick.
A reader that opened the previous inode and then finds the name on a newer
regular file has seen a benign replacement, not tamper: it reads again.  The
strict identity check stays for everything else, including CAS objects,
which are never replaced by design.
"""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb, pool  # noqa: E402


def _record(units: int) -> bytes:
    return json.dumps(dict(
        schema=pb.PROGRESS_RECORD_SCHEMA_V1, token="launch", phase="startup",
        units_completed=units, reported_unix=time.time()),
        sort_keys=True).encode() + b"\n"


def _replace(path: Path, raw: bytes) -> None:
    temporary = path.parent / f".{path.name}.tmp"
    temporary.write_bytes(raw)
    os.replace(temporary, path)


def _watch(path: Path) -> "pool.ProgressWatch":
    policy = pool.ProgressPolicy(
        (pool.ProgressPhase("startup", 10, None),), None)
    return pool.ProgressWatch(path, "launch", policy, started=0)


def _replace_after_open(monkeypatch, path: Path, raw: bytes, times: int = 1):
    """Replace ``path`` right after the reader opens it, ``times`` times."""

    original = pb._open_regular_nofollow
    left = [times]

    def opened_then_replaced(candidate, *, where):
        held = original(candidate, where=where)
        if left[0] > 0 and Path(candidate) == path:
            left[0] -= 1
            _replace(path, raw)
        return held

    monkeypatch.setattr(pb, "_open_regular_nofollow", opened_then_replaced)
    return left


def test_a_replace_between_open_and_identity_check_is_read_again(
        tmp_path, monkeypatch):
    path = tmp_path / "action.progress"
    _replace(path, _record(1))
    _replace_after_open(monkeypatch, path, _record(2))
    watch = _watch(path)

    assert watch.sample(now=1) is True
    assert watch.rejected == 0, watch.last_rejection
    assert watch.units_high_water == 2


def test_the_staged_wait_reader_reads_a_replaced_record_again(
        tmp_path, monkeypatch):
    path = tmp_path / "wait.json"
    record = {"schema": pool.pb_progress.STAGED_WAIT_SCHEMA_V1,
              "token": "launch", "movers": ["m"], "since_unix": 1.0}
    _replace(path, json.dumps(record).encode())
    left = _replace_after_open(monkeypatch, path, json.dumps(record).encode())
    found, reason = pool.read_staged_wait(path, token="launch")
    assert left == [0]
    assert reason == ""
    assert found is not None and found["movers"] == ["m"]


def test_the_reread_is_bounded_and_then_still_refused(tmp_path, monkeypatch):
    path = tmp_path / "action.progress"
    _replace(path, _record(1))
    _replace_after_open(monkeypatch, path, _record(2), times=1000)

    with pytest.raises(pb.CASTamperError):
        pb._read_regular_file_nofollow(
            path, where="action progress report", replaced_leaf=True)


def test_strict_readers_keep_the_identity_check(tmp_path, monkeypatch):
    path = tmp_path / "object"
    _replace(path, b"one")
    _replace_after_open(monkeypatch, path, b"two")

    with pytest.raises(pb.CASTamperError):
        pb._read_regular_file_nofollow(path, where="CAS object")


def test_a_replaced_leaf_that_is_not_a_regular_file_is_still_tamper(
        tmp_path, monkeypatch):
    path = tmp_path / "action.progress"
    _replace(path, _record(1))
    original = pb._open_regular_nofollow

    def opened_then_symlinked(candidate, *, where):
        held = original(candidate, where=where)
        link = tmp_path / ".link"
        link.symlink_to(tmp_path / "elsewhere")
        os.replace(link, path)
        return held

    monkeypatch.setattr(pb, "_open_regular_nofollow", opened_then_symlinked)
    with pytest.raises(pb.CASTamperError):
        pb._read_regular_file_nofollow(
            path, where="action progress report", replaced_leaf=True)


WRITER = r'''
import json, os, sys, time
path, stop = sys.argv[1], sys.argv[2]
temporary = os.path.join(os.path.dirname(path), ".writer.tmp")
units = 0
while not os.path.exists(stop):
    units += 1
    with open(temporary, "w") as handle:
        handle.write(json.dumps({"schema": sys.argv[3], "token": "launch",
            "phase": "startup", "units_completed": units,
            "reported_unix": time.time()}, sort_keys=True) + "\n")
    os.replace(temporary, path)
'''


def test_ten_thousand_polls_against_a_tight_replace_loop_reject_nothing(
        tmp_path):
    """The #1017 acceptance: a writer replacing the record in a tight loop,
    a watch polling in a tight loop, zero unreadable samples (so zero tamper
    errors) over 10,000."""

    path = tmp_path / "action.progress"
    _replace(path, _record(0))
    stop = tmp_path / "stop"
    writer = subprocess.Popen(
        [sys.executable, "-c", WRITER, str(path), str(stop),
         pb.PROGRESS_RECORD_SCHEMA_V1])
    try:
        watch = _watch(path)
        reasons: list[str] = []
        for poll in range(10_000):
            watch.sample(now=1)
            # A poll faster than the writer reads the same record again,
            # which is "replayed" by design; what may not appear is a
            # sample that did not read.
            if watch.last_rejection not in (None, "replayed"):
                reasons.append(watch.last_rejection)
            watch.last_rejection = None
        assert writer.poll() is None, "the writer stopped before the polls did"
    finally:
        stop.touch()
        writer.wait(timeout=30)
    assert reasons == [], (len(reasons), reasons[:5])
    assert watch.accepted > 0
    assert watch.units_high_water > 0

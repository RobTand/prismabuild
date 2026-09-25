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


def _replace_before_the_name_moves(monkeypatch, path: Path, times: int):
    """Replace ``path`` during the read, while the name still resolves to the
    inode just unlinked, ``times`` times (#1159).

    ext4's rename drops the old target's link count and moves its ctime
    before the VFS moves the name to the new inode, so a reader can fstat
    its held inode at ``st_nlink == 0`` and still stat the name onto it.
    Only the kernel's name lookup is simulated; every PrismaBuild check runs
    for real against the real files.
    """

    real_read, real_stat = os.read, os.stat
    state = {"left": times, "window": None, "replaced": 0}

    def read(descriptor, count):
        if state["left"] > 0 and state["window"] is None:
            held, named = os.fstat(descriptor), real_stat(path)
            if (held.st_dev, held.st_ino) == (named.st_dev, named.st_ino):
                state["left"] -= 1
                chunk = real_read(descriptor, count)
                state["replaced"] += 1
                _replace(path, _record(100 + state["replaced"]))
                state["window"] = descriptor
                return chunk
        return real_read(descriptor, count)

    def stat_(name, *, dir_fd=None, follow_symlinks=True):
        if (state["window"] is not None and dir_fd is not None
                and name == path.name):
            descriptor, state["window"] = state["window"], None
            return os.fstat(descriptor)
        return real_stat(name, dir_fd=dir_fd, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(pb.os, "read", read)
    monkeypatch.setattr(pb.os, "stat", stat_)
    return state


def test_a_replace_seen_before_the_name_moves_is_read_not_tamper(
        tmp_path, monkeypatch):
    """#1159: the held inode is unlinked by the replace but the name still
    resolves to it, on every fresh read the stable reader makes.  Its bytes
    never changed and the name still named it, so it is the record."""

    path = tmp_path / "action.progress"
    _replace(path, _record(1))
    state = _replace_before_the_name_moves(
        monkeypatch, path, times=pb._STABLE_FILE_READ_ATTEMPTS)
    watch = _watch(path)

    advanced = watch.sample(now=1)
    assert state["replaced"] >= 1
    assert watch.rejected == 0, watch.last_rejection
    assert advanced is True
    assert watch.units_high_water == 1


def test_a_replaced_leaf_rewritten_in_place_is_still_tamper(
        tmp_path, monkeypatch):
    """Bytes that change within one held inode are tamper, whatever the
    name does meanwhile: the #1159 acceptance is for the replace's own
    unlink, never for a changed record."""

    path = tmp_path / "action.progress"
    _replace(path, _record(1))
    state = _replace_before_the_name_moves(monkeypatch, path, times=1)
    windowed_read = pb.os.read
    rewritten = []

    def read_then_rewrite(descriptor, count):
        chunk = windowed_read(descriptor, count)
        if chunk and not rewritten:
            # The held inode itself grows: a write into the very inode the
            # reader holds, not a replace.
            with open(f"/proc/self/fd/{descriptor}", "ab") as held:
                held.write(b" ")
            rewritten.append(descriptor)
        return chunk

    monkeypatch.setattr(pb.os, "read", read_then_rewrite)
    with pytest.raises(pb.CASTamperError, match="changed substantively"):
        pb._read_regular_file_nofollow(
            path, where="action progress report", replaced_leaf=True)
    assert state["replaced"] == 1
    assert len(rewritten) == 1


def test_a_record_replaced_under_every_reread_is_not_a_rejection(
        tmp_path, monkeypatch):
    """#1159: a writer that replaces the record under each of the reader's
    bounded rereads is a live writer, not tamper.  The sample reads nothing
    this poll and the next poll reads again; the phase allowance bounds it,
    as it bounds a report that has not appeared yet."""

    path = tmp_path / "action.progress"
    _replace(path, _record(1))
    _replace_after_open(monkeypatch, path, _record(2), times=1000)
    watch = _watch(path)

    assert watch.sample(now=1) is False
    assert watch.rejected == 0, watch.last_rejection
    assert watch.last_rejection is None

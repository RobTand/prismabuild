"""A waiting client reads its own ending by name; it lists no terminal directory.

2026-10-20, dl380g10: about 100 ``pbrun`` and ``pbtest`` clients were waiting
at once, and the disk sat at 65 % full IO pressure.  Each poll's forked reader
asked ``RecordStore.wait_terminal_at`` whether ``done/<key>.json``,
``failed/<key>.json`` and ``withdrawn/<key>.json`` existed, and it answered by
listing each directory: 87,440, 24,978 and 4,092 names, every 5 s, for every
waiting client.  strace on one of them counted about 700 ``getdents64`` calls
in 10 s.

The listing was there to get past NFS's negative lookup cache.  Opening the
parent gets past it too (#808), for one round trip whatever the directory
holds.  These tests pin both halves: no waiter lists ``done``, ``failed`` or
``withdrawn``, in its own process or in the forked reader, and an ending
filed after a cached miss is still seen on the next poll.

Listings are observed through CPython's audit events, which ``os.listdir`` and
``os.scandir`` raise themselves, so ``Path.glob`` and ``Path.iterdir`` are
seen too.  The pool reader is a forked child, so each event is appended to a
file that the test reads afterwards.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
from prismabuild import action_wait, core as pb, pool  # noqa: E402
from prismabuild import submission as submit_core  # noqa: E402
from prismabuild.record_store import (  # noqa: E402
    DONE, FAILED, WITHDRAWN, Absent, RecordStore,
)
import pbwait  # noqa: E402

KEY = "5e" * 32
GENERATION = 1792500000.0
TERMINAL = (DONE, FAILED, WITHDRAWN)
#: Endings of other actions in each terminal directory.
OTHERS = 200

#: Where the audit hook appends listing events, and the directories it
#: watches.  ``None`` outside the ``listings`` fixture.
_LOG: str | None = None
_WATCHED: frozenset[str] = frozenset()


def _note_listing(event: str, args: tuple) -> None:
    if _LOG is None or event not in ("os.listdir", "os.scandir"):
        return
    target = args[0] if args else None
    if isinstance(target, int):
        try:
            target = os.readlink(f"/proc/self/fd/{target}")
        except OSError:
            return
    directory = os.path.normpath(os.path.abspath(
        os.fsdecode(target if target is not None else ".")))
    if directory not in _WATCHED:
        return
    descriptor = os.open(_LOG, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    try:
        os.write(descriptor, f"{os.getpid()} {event} {directory}\n".encode())
    finally:
        os.close(descriptor)


sys.addaudithook(_note_listing)


def _outcome(key: str, generation: float, *, status: str = "executed",
             returncode: int = 0) -> dict:
    return {
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "action_key": key,
        "status": status,
        "published_unix": generation,
        "finished_unix": generation + 1.0,
        "finished_host": "sparky",
        "attempts": 1,
        "detail": {"returncode": returncode, "elapsed_s": 1.0,
                   "stdout": "", "stderr": ""},
    }


def _file(queue: pool.PoolQueue, state: str, record: dict) -> Path:
    path = queue.records.item_path(state, str(record["action_key"]))
    path.write_text(json.dumps(record), encoding="utf-8")
    return path


@pytest.fixture()
def queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> pool.PoolQueue:
    """A queue whose terminal directories hold other actions' endings."""

    monkeypatch.setattr(submit_core.action_wait, "POLL_S", 0.01)
    made = pool.PoolQueue(tmp_path / "pb-queue")
    made.records.ensure_layout()
    for index in range(OTHERS):
        for state in TERMINAL:
            _file(made, state, _outcome(f"{index:064x}", 1.0))
    return made


@pytest.fixture()
def listings(queue: pool.PoolQueue, tmp_path: Path):
    """Every listing of a terminal directory, from this process or its children."""

    global _LOG, _WATCHED
    log = tmp_path / "terminal-listings.log"
    watched = set()
    for state in TERMINAL:
        directory = queue.records.dir(state)
        watched.add(os.path.normpath(os.path.abspath(directory)))
        watched.add(os.path.realpath(directory))
    _WATCHED, _LOG = frozenset(watched), str(log)
    try:
        yield lambda: log.read_text(encoding="utf-8").splitlines() if log.exists() else []
    finally:
        _WATCHED, _LOG = frozenset(), None


def test_a_pbrun_wait_lists_no_terminal_directory(queue, listings) -> None:
    """``pbtest`` waits through one ``pbrun`` per shard, so this is its wait too."""

    # One observation each, with the reader's full budget: a subsecond wait
    # can read as 74 on a loaded box (#1170).
    assert submit_core.action_client.report_pool_outcome(
        queue, KEY, wait_s=0, generation=GENERATION) == 75
    _file(queue, DONE, _outcome(KEY, GENERATION))
    assert submit_core.action_client.report_pool_outcome(
        queue, KEY, wait_s=0, generation=GENERATION) == 0

    assert listings() == []


def test_a_pbwait_or_pbcampaign_wait_lists_no_terminal_directory(
    queue, listings, tmp_path: Path,
) -> None:
    """``pbcampaign`` waits for its window through ``pbwait.wait_for_keys``."""

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    rows = pbwait.wait_for_keys(queue, [KEY], cas=cas, wait_s=0.0,
                                generations={KEY: GENERATION})
    assert [row["status"] for row in rows] == ["waiting"]
    _file(queue, FAILED, _outcome(KEY, GENERATION, status="failed", returncode=7))
    rows = pbwait.wait_for_keys(queue, [KEY], cas=cas, wait_s=0.0,
                                generations={KEY: GENERATION})
    assert [row["status"] for row in rows] == ["failed"]

    assert listings() == []


def test_a_filed_slot_is_read_by_name(queue, listings) -> None:
    """The SLURM lane files and withdraws through the same by-name reads."""

    path = queue.records.item_path(WITHDRAWN, KEY)
    assert RecordStore.filed_slot(path) is False
    assert RecordStore.polled_object_at(path) is None
    assert list(queue.records.sibling_ending_records(
        queue.records.item_path(DONE, KEY))) == []
    _file(queue, WITHDRAWN, _outcome(KEY, GENERATION, status="withdrawn"))
    assert RecordStore.filed_slot(path) is True
    assert RecordStore.polled_object_at(path)["status"] == "withdrawn"
    assert [record["status"] for record in queue.records.sibling_ending_records(
        queue.records.item_path(DONE, KEY))] == ["withdrawn"]

    assert listings() == []


class _NegativeLookupCache:
    """An NFS client's negative entries in the queue's terminal directories.

    A by-name read or ``lstat`` of an absent name is remembered as absent,
    and answers ``ENOENT`` after the file lands, until this client revalidates
    the directory that holds it: by opening it (close-to-open), listing it or
    scanning it.  The model is the mount's behaviour, as in
    ``test_a_by_key_read_sees_a_record_another_box_filed.py``, not the fix's.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch, queue: pool.PoolQueue) -> None:
        self.directories = {os.path.normpath(os.fspath(queue.records.dir(state)))
                            for state in TERMINAL}
        self.absent: set[str] = set()
        real_read_text, real_lstat = Path.read_text, Path.lstat
        real_open, real_listdir, real_scandir = os.open, os.listdir, os.scandir
        cache = self

        def watched(path) -> str | None:
            name = os.path.normpath(os.fspath(path))
            return name if os.path.dirname(name) in cache.directories else None

        def revalidate(directory) -> None:
            if isinstance(directory, (str, os.PathLike)):
                held = os.path.normpath(os.fspath(directory))
                cache.absent = {name for name in cache.absent
                                if os.path.dirname(name) != held}

        def remembering(real):
            def read(this, *args, **kwargs):
                name = watched(this)
                if name in cache.absent:
                    raise FileNotFoundError(2, "No such file or directory", name)
                try:
                    return real(this, *args, **kwargs)
                except FileNotFoundError:
                    if name is not None:
                        cache.absent.add(name)
                    raise
            return read

        def open_(path, flags, *args, **kwargs):
            descriptor = real_open(path, flags, *args, **kwargs)
            if flags & os.O_DIRECTORY and kwargs.get("dir_fd") is None:
                revalidate(path)
            return descriptor

        def listdir(path="."):
            names = real_listdir(path)
            revalidate(path)
            return names

        def scandir(path="."):
            iterator = real_scandir(path)
            revalidate(path)
            return iterator

        monkeypatch.setattr(Path, "read_text", remembering(real_read_text))
        monkeypatch.setattr(Path, "lstat", remembering(real_lstat))
        monkeypatch.setattr(os, "open", open_)
        monkeypatch.setattr(os, "listdir", listdir)
        monkeypatch.setattr(os, "scandir", scandir)


def test_an_ending_filed_after_a_cached_miss_is_seen_on_the_next_poll(
    queue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The listing was the revalidation; the parent open has to do its job."""

    _NegativeLookupCache(monkeypatch, queue)
    assert action_wait.outcome_poll(
        queue, KEY, GENERATION, report_unreadable=True) == (None, GENERATION)
    filed = _file(queue, DONE, _outcome(KEY, GENERATION))

    landed, generation = action_wait.outcome_poll(
        queue, KEY, GENERATION, report_unreadable=True)

    assert generation == GENERATION
    assert landed is not None and landed[0] == filed


def test_a_slot_filed_after_a_cached_miss_is_seen(
    queue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = queue.records.item_path(WITHDRAWN, KEY)
    _NegativeLookupCache(monkeypatch, queue)
    assert RecordStore.filed_slot(path) is False
    _file(queue, WITHDRAWN, _outcome(KEY, GENERATION, status="withdrawn"))

    assert RecordStore.filed_slot(path) is True


def test_a_terminal_directory_that_cannot_be_opened_still_reads_as_absent(
    queue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """What the listing answered when it failed, so no waiter meets a new error."""

    _file(queue, DONE, _outcome(KEY, GENERATION))
    done = os.fspath(queue.records.dir(DONE))
    real_open, real_listdir = os.open, os.listdir

    def denied(path) -> PermissionError:
        return PermissionError(13, "Permission denied", os.fspath(path))

    def open_(path, flags, *args, **kwargs):
        if flags & os.O_DIRECTORY and os.fspath(path) == done:
            raise denied(path)
        return real_open(path, flags, *args, **kwargs)

    def listdir(path="."):
        if os.fspath(path) == done:
            raise denied(path)
        return real_listdir(path)

    monkeypatch.setattr(os, "open", open_)
    monkeypatch.setattr(os, "listdir", listdir)

    assert isinstance(RecordStore.wait_terminal_at(queue.records.item_path(DONE, KEY)), Absent)

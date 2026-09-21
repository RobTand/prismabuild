"""A record addressed by key is seen when another box files it (#808).

The queue lives on NFS with default attribute caching. A reader that looks a
name up before it exists caches the negative answer, and the client keeps
answering ``ENOENT`` from that cache until the parent directory is revalidated
-- measured on sparky at 26.3 s, 26.5 s and 26.6 s, against 0.01 s to 0.24 s
when the parent is opened or listed first. A produced-output owner polls its
mover's receipt exactly that way, so it waited out the cache on every staged
group.

A unit test has no NFS client, so this file models the one property that
matters: a name that was looked up while absent stays absent to this reader
until it opens the parent directory. The model is the mount's documented
behaviour (``lookupcache=all``), not the fix's implementation -- any read that
revalidates the parent passes, and a read that does not fails for as long as
it polls.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from prismabuild import pool
from prismabuild import produced_output


KEY = "ab" * 32


class NegativeLookupCache:
    """An NFS client's negative dentry cache over one directory tree.

    A by-name read of an absent file remembers the absence. The memory is
    dropped only when the reader opens the parent directory, which is the
    close-to-open revalidation the real client performs.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.remembered_absent: set[str] = set()
        self.directory_opens = 0
        real_read_bytes = Path.read_bytes
        real_open = os.open
        cache = self

        def read_bytes(path: Path) -> bytes:
            name = os.fspath(path)
            if name in cache.remembered_absent:
                raise FileNotFoundError(name)
            try:
                return real_read_bytes(path)
            except FileNotFoundError:
                cache.remembered_absent.add(name)
                raise

        def open_(path, flags, *args, **kwargs):
            if flags & os.O_DIRECTORY:
                cache.directory_opens += 1
                prefix = os.fspath(path).rstrip("/") + "/"
                cache.remembered_absent = {
                    name for name in cache.remembered_absent
                    if not name.startswith(prefix)}
            return real_open(path, flags, *args, **kwargs)

        monkeypatch.setattr(Path, "read_bytes", read_bytes)
        monkeypatch.setattr(os, "open", open_)


def _queue(tmp_path: Path) -> pool.PoolQueue:
    return pool.PoolQueue(tmp_path / "queue")


def _file_receipt(queue: pool.PoolQueue, key: str, **fields) -> None:
    path = queue.move_path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(
        {"schema": pool.POOL_MOVE_SCHEMA_V1, "action_key": key, **fields}))


def test_a_receipt_filed_after_the_first_poll_is_seen_on_the_next(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue = _queue(tmp_path)
    queue.move_path(KEY).parent.mkdir(parents=True, exist_ok=True)
    NegativeLookupCache(monkeypatch)

    assert queue.move_record(KEY) is None       # the owner polls first
    _file_receipt(queue, KEY, complete=True)    # then the tier host files it

    seen = queue.move_record(KEY)
    assert seen is not None, (
        "the receipt is on disk and this reader still answers absent: its "
        "first poll cached the miss and nothing revalidated the directory")
    assert seen["complete"] is True


def test_the_owners_readiness_gate_turns_true_without_waiting_out_the_cache(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue = _queue(tmp_path)
    queue.move_path(KEY).parent.mkdir(parents=True, exist_ok=True)
    NegativeLookupCache(monkeypatch)

    assert produced_output._mover_receipt_complete(queue, KEY) is None
    _file_receipt(queue, KEY, complete=True)

    assert produced_output._mover_receipt_complete(queue, KEY) is True


def test_a_terminal_row_filed_after_the_first_poll_is_named(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue = _queue(tmp_path)
    for state in (pool.READY, pool.CLAIMED, pool.DONE, pool.FAILED,
                  pool.WITHDRAWN):
        queue.dir(state).mkdir(parents=True, exist_ok=True)
    NegativeLookupCache(monkeypatch)

    assert produced_output._mover_live_state(queue, KEY) == "absent"
    queue.item_path(pool.DONE, KEY).write_text(json.dumps({"action_key": KEY}))

    assert produced_output._mover_live_state(queue, KEY) == pool.DONE


def test_a_record_that_is_there_costs_no_directory_open(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The revalidation is the miss path only: a hit is one read."""

    queue = _queue(tmp_path)
    _file_receipt(queue, KEY, complete=True)
    cache = NegativeLookupCache(monkeypatch)

    assert queue.move_record(KEY) is not None
    assert cache.directory_opens == 0


def test_a_directory_that_is_not_there_is_still_absent_not_an_error(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue = _queue(tmp_path)
    NegativeLookupCache(monkeypatch)

    assert queue.move_record(KEY) is None


def test_a_parent_that_cannot_be_opened_is_loud_not_absent(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A by-key caller holds no evidence the mount is live, so it stays loud."""

    queue = _queue(tmp_path)
    queue.move_path(KEY).parent.mkdir(parents=True, exist_ok=True)
    real_open = os.open

    def stale(path, flags, *args, **kwargs):
        if flags & os.O_DIRECTORY:
            raise OSError(116, "Stale file handle", os.fspath(path))
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", stale)

    with pytest.raises(OSError):
        queue.move_record(KEY)

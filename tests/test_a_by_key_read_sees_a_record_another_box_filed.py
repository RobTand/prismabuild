"""A record addressed by key is seen when another box files it (#808).

The queue lives on NFS with default attribute caching. A reader that looks a
name up before it exists caches the negative answer, and the client keeps
answering ``ENOENT`` from that cache until the parent directory is revalidated
-- measured on sparky through ``move_record`` at 26.3 s to 26.6 s, against
0.04 s to 0.25 s with the parent opened first
(``tools/fleet/qualify_record_visibility.py``). A produced-output owner polls
its mover's receipt exactly that way, so it waited out the cache on every
staged group.

A unit test has no NFS client, so this file models the one property that
matters: a name that was looked up while absent stays absent to this reader
until it revalidates the parent directory. The model is the mount's documented
behaviour (``lookupcache=all``), not the fix's implementation: opening the
parent, listing it and scanning it all revalidate, as they do on the real
client, so a fix of any of those shapes passes. Only the names directly inside
the revalidated directory are forgotten -- opening the queue root does not
revalidate ``movers`` -- and a read that revalidates nothing fails for as long
as it polls.
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

    A by-name read of an absent file remembers the absence. The memory of a
    name is dropped only when the reader revalidates the directory that holds
    it: by opening it (close-to-open), listing it or scanning it.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.remembered_absent: set[str] = set()
        self.revalidations = 0
        real_read_bytes = Path.read_bytes
        real_open, real_listdir, real_scandir = os.open, os.listdir, os.scandir
        cache = self

        def revalidate(directory) -> None:
            cache.revalidations += 1
            held = os.path.normpath(os.fspath(directory))
            cache.remembered_absent = {
                name for name in cache.remembered_absent
                if os.path.dirname(name) != held}

        def read_bytes(path: Path) -> bytes:
            name = os.path.normpath(os.fspath(path))
            if name in cache.remembered_absent:
                raise FileNotFoundError(name)
            try:
                return real_read_bytes(path)
            except FileNotFoundError:
                cache.remembered_absent.add(name)
                raise

        def open_(path, flags, *args, **kwargs):
            descriptor = real_open(path, flags, *args, **kwargs)
            if flags & os.O_DIRECTORY:
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

        monkeypatch.setattr(Path, "read_bytes", read_bytes)
        monkeypatch.setattr(os, "open", open_)
        monkeypatch.setattr(os, "listdir", listdir)
        monkeypatch.setattr(os, "scandir", scandir)


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
    assert cache.revalidations == 0


def test_a_directory_that_is_not_there_is_still_absent_not_an_error(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue = _queue(tmp_path)
    NegativeLookupCache(monkeypatch)

    assert queue.move_record(KEY) is None


def test_a_row_claimed_after_the_first_poll_is_not_read_as_absent(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``_publish_output_mover_row`` republishes on "absent": a stale miss on
    ``claimed`` would republish a row that is being copied right now."""

    queue = _queue(tmp_path)
    for state in (pool.READY, pool.CLAIMED, pool.DONE, pool.FAILED,
                  pool.WITHDRAWN):
        queue.dir(state).mkdir(parents=True, exist_ok=True)
    NegativeLookupCache(monkeypatch)

    assert produced_output._mover_live_state(queue, KEY) == "absent"
    queue.item_path(pool.CLAIMED, KEY).write_text(
        json.dumps({"action_key": KEY}))

    assert produced_output._mover_live_state(queue, KEY) == pool.CLAIMED


def test_revalidating_another_directory_does_not_refresh_this_one(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The model is not satisfied by opening just anything."""

    queue = _queue(tmp_path)
    receipt = queue.move_path(KEY)
    receipt.parent.mkdir(parents=True, exist_ok=True)
    NegativeLookupCache(monkeypatch)

    assert pool._read_json(receipt) is None
    _file_receipt(queue, KEY, complete=True)
    os.close(os.open(queue.root, os.O_RDONLY | os.O_DIRECTORY))

    assert pool._read_json(receipt) is None
    os.listdir(receipt.parent)
    assert pool._read_json(receipt) is not None


def test_a_parent_that_cannot_be_opened_leaves_the_first_answer(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Best effort: no caller meets a failure it did not meet before #808."""

    queue = _queue(tmp_path)
    queue.move_path(KEY).parent.mkdir(parents=True, exist_ok=True)
    real_open = os.open

    def stale(path, flags, *args, **kwargs):
        if flags & os.O_DIRECTORY:
            raise OSError(116, "Stale file handle", os.fspath(path))
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", stale)

    assert queue.move_record(KEY) is None

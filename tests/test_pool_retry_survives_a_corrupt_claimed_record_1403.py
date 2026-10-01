"""A bad claimed/ row must not stop the drain's own saved-finish retry (#1403).

Parent review of ``ecd0d3e12a09``: ``retry_own_pending_finishes`` reads
``_read_json(path)`` outside its per-record try, so one claimed file that is
not readable JSON -- or not an object -- aborts the whole pass, and a healthy
later saved finish can stay stuck for as long as the bad row exists.  The
existing malformed coverage only reaches a decoded dict with a bad
``finish_pending``, which is raised inside the try.

These tests drive the narrow retry with a lexically earlier bad row and a real
retained finish, and pin the desired contract: the bad row keeps its bytes,
every other own saved finish still concludes, and a row whose identity cannot
be trusted is left alone rather than concluded under a name it does not carry.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool, resource_scope  # noqa: E402
from test_pool_resource_scope import scoped  # noqa: E402
from test_pool_retry_own_pending_finishes_1403 import _pending_owner  # noqa: E402

BAD_KEY = "0" * 64


def _with_bad_row(scoped, monkeypatch, payload):
    """A real retained finish plus one lexically earlier unreadable row."""

    queue, item, release = _pending_owner(scoped, monkeypatch)
    monkeypatch.setattr(resource_scope.ResourceScope, "release", release)
    bad_path = queue.item_path(pool.CLAIMED, BAD_KEY)
    bad_path.write_bytes(payload)
    assert bad_path.name < queue.item_path(pool.CLAIMED, item["action_key"]).name
    monkeypatch.setattr(queue, "_sweep_due", lambda: True)
    return queue, item, bad_path


@pytest.mark.parametrize("payload,case", [
    (b"{not json", "invalid-json"),
    (b"[1, 2, 3]", "not-an-object"),
    (b'{"finish_pending": "x"} trailing', "trailing-bytes"),
    (b"\xff\xfe\x00", "not-utf8"),
])
def test_a_corrupt_earlier_claimed_record_does_not_stop_the_own_retry(
        scoped, monkeypatch, payload, case):
    """One unreadable row must not pin this box's other saved finishes."""

    queue, item, bad_path = _with_bad_row(scoped, monkeypatch, payload)

    try:
        retried = queue.retry_own_pending_finishes()
    except pool.PoolContractError as exc:
        pytest.fail(f"{case}: one corrupt claimed row aborted the whole retry: {exc}")
    assert retried == []
    assert bad_path.read_bytes() == payload, case
    assert queue.item_path(pool.DONE, item["action_key"]).exists(), case


def test_an_unreadable_claimed_row_does_not_stop_the_own_retry(
        scoped, monkeypatch):
    """A read that fails (ESTALE, EIO, permission) is the same family as bad JSON.

    The failure is injected at ``Path.read_bytes`` for this one path so the
    test does not depend on the runner's uid or an NFS fault.
    """

    queue, item, release = _pending_owner(scoped, monkeypatch)
    monkeypatch.setattr(resource_scope.ResourceScope, "release", release)
    bad_path = queue.item_path(pool.CLAIMED, BAD_KEY)
    bad_path.write_bytes(b"{}")
    assert bad_path.name < queue.item_path(pool.CLAIMED, item["action_key"]).name
    before = bad_path.read_bytes()

    real_read_bytes = Path.read_bytes

    def unreadable(path, *args, **kwargs):
        if path == bad_path:
            raise OSError(5, "Input/output error")
        return real_read_bytes(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_bytes", unreadable)
    monkeypatch.setattr(queue, "_sweep_due", lambda: True)

    try:
        retried = queue.retry_own_pending_finishes()
    except OSError as exc:
        pytest.fail(f"one unreadable claimed row aborted the whole retry: {exc}")
    assert retried == []
    assert real_read_bytes(bad_path) == before
    assert queue.item_path(pool.DONE, item["action_key"]).exists()


def test_an_empty_claimed_entry_is_absent_and_does_not_stop_the_retry(
        scoped, monkeypatch):
    """The one tolerated read: no bytes means no record, not a bad one."""

    queue, item, bad_path = _with_bad_row(scoped, monkeypatch, b"")

    assert queue.retry_own_pending_finishes() == []
    assert bad_path.read_bytes() == b""
    assert queue.item_path(pool.DONE, item["action_key"]).exists()


@pytest.mark.parametrize("mutation,case", [
    ({"action_key": "f" * 64}, "divergent-key"),
    ({}, "no-key"),
])
def test_a_row_whose_identity_disagrees_with_its_name_is_left_alone(
        scoped, monkeypatch, mutation, case):
    """The filename is the identity; an untrusted row is not concluded under it.

    A valid-looking record filed under a name it does not carry is not this
    host's saved finish whichever way it is broken, and the retry must skip it
    without mutating it while the healthy finish still concludes.
    """

    queue, item, release = _pending_owner(scoped, monkeypatch)
    monkeypatch.setattr(resource_scope.ResourceScope, "release", release)
    live = json.loads(queue.item_path(pool.CLAIMED, item["action_key"]).read_text())
    queued_key = "not-a-key"
    path = queue.item_path(pool.CLAIMED, queued_key)
    pool._write_json_atomic(path, {**live, **mutation})
    before = path.read_bytes()

    monkeypatch.setattr(queue, "_sweep_due", lambda: True)
    assert queue.retry_own_pending_finishes() == []
    assert path.read_bytes() == before, case
    assert queue.item_path(pool.DONE, item["action_key"]).exists(), case

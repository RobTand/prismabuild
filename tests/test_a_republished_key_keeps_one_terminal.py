"""A republished key that ends keeps exactly one terminal record (#1117).

GLM Stage B row 37: the first publication of a key failed and filed
``failed/<key>.json``; the byte-identical row was resubmitted, published the
same key again, and succeeded into ``done/<key>.json``.  Both records stayed.
A reader that checks ``done/`` first saw the success; one that asked "is
there a failed record" read a successful key as dead, and
``reclaim_terminal_reservation`` refuses any key with two terminals.  The new
terminal's ``attempts: 1`` did not mention the first failure either.

Now the conclusion of a later generation retires the other terminal record
of an earlier generation into ``withdrawn/superseded/`` -- a name no
terminal-state lookup reads -- and the new terminal names what it
superseded.  The same holds in the other direction: a republished ``done``
key that then fails leaves only ``failed/``.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY = "b" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    value = pool.PoolQueue(tmp_path / "queue")
    value.ensure_layout()
    return value


def _run(queue: pool.PoolQueue, status: str) -> tuple[Path, dict]:
    queue.publish(action_key=KEY, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", max_attempts=1)
    claimed = queue.claim()
    assert claimed is not None and claimed["action_key"] == KEY
    path = queue.finish(KEY, status=status, detail={"returncode": 0})
    return path, claimed


def _terminals(queue: pool.PoolQueue) -> list[str]:
    return [state for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN)
            if queue.item_path(state, KEY).exists()]


@pytest.mark.parametrize("first,second,retired,kept", [
    ("failed", "executed", pool.FAILED, pool.DONE),
    ("executed", "failed", pool.DONE, pool.FAILED),
])
def test_the_later_generation_retires_the_earlier_terminal(
        queue: pool.PoolQueue, first, second, retired, kept) -> None:
    first_path, first_claim = _run(queue, first)
    assert first_path == queue.item_path(retired, KEY)
    earlier = json.loads(first_path.read_text())

    second_path, second_claim = _run(queue, second)
    assert second_path == queue.item_path(kept, KEY)
    assert first_claim["published_unix"] != second_claim["published_unix"]

    # One key, one terminal record.
    assert _terminals(queue) == [kept], (
        "a republished key kept its earlier generation's terminal beside "
        "the new one")

    # The new terminal names the generation it superseded, with the link to
    # that generation's own attempt history.
    terminal = json.loads(second_path.read_text())
    superseded = terminal["supersedes_terminal"]
    assert superseded["state"] == retired
    assert superseded["status"] == earlier["status"]
    assert superseded["published_unix"] == earlier["published_unix"]
    assert superseded["finished_unix"] == earlier["finished_unix"]
    assert superseded["attempts"] == earlier["attempts"]
    assert superseded["attempt_history"] == earlier["attempt_history"]

    # The earlier record is archived whole, where no ``<key>.json`` lookup
    # reads it.
    archived = Path(superseded["superseded_path"])
    assert archived.parent == queue.superseded_dir()
    assert archived.name.startswith(f"{KEY}.")
    assert not archived.name == f"{KEY}.json"
    kept_record = json.loads(archived.read_text())
    assert kept_record["status"] == earlier["status"]
    assert kept_record["published_unix"] == earlier["published_unix"]
    assert kept_record["superseded_by_published_unix"] == second_claim[
        "published_unix"]

    # The ambiguity is what made the reclaim refuse; it no longer does.
    if kept == pool.DONE:
        queue.reclaim_terminal_reservation(KEY)


def test_a_same_state_republish_is_unchanged(queue: pool.PoolQueue) -> None:
    """Two failures of two generations: the later overwrites, as before."""

    _run(queue, "failed")
    path, claimed = _run(queue, "failed")
    assert _terminals(queue) == [pool.FAILED]
    terminal = json.loads(path.read_text())
    assert terminal["published_unix"] == claimed["published_unix"]
    assert "supersedes_terminal" not in terminal


def test_a_republish_alone_retires_nothing(queue: pool.PoolQueue) -> None:
    """Until the new generation ends, the earlier ending is still the key's
    only ending; a waiter on the earlier generation can still read it."""

    first_path, _claim = _run(queue, "failed")
    queue.publish(action_key=KEY, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", max_attempts=1)
    assert first_path.exists()
    assert queue.item_path(pool.READY, KEY).exists()


def test_a_waiter_on_the_earlier_generation_still_reads_its_ending(
        queue: pool.PoolQueue) -> None:
    """The retired row's generation is answered from its immutable attempt."""

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
    import pbrun

    _first_path, first = _run(queue, "failed")
    _second_path, second = _run(queue, "executed")
    assert not queue.item_path(pool.FAILED, KEY).exists()

    landed, generation = pbrun.outcome_poll(
        queue, KEY, float(first["published_unix"]))
    assert landed is not None and generation == float(first["published_unix"])
    assert landed[1]["status"] == "failed"
    assert landed[1]["published_unix"] == first["published_unix"]

    newer, _ = pbrun.outcome_poll(queue, KEY, float(second["published_unix"]))
    assert newer is not None and newer[1]["status"] == "executed"


def test_an_unreadable_earlier_terminal_never_fails_the_conclusion(
        queue: pool.PoolQueue) -> None:
    """The new ending is filed whatever the earlier row holds; a row that
    cannot be read is left as it is, not guessed at."""

    first_path, _claim = _run(queue, "failed")
    queue.publish(action_key=KEY, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", max_attempts=1)
    assert queue.claim() is not None
    # Damaged after the claim, which reads it first (and refuses it).
    first_path.write_text("{not json")
    second_path = queue.finish(KEY, status="executed", detail={})
    assert second_path == queue.item_path(pool.DONE, KEY)
    assert "supersedes_terminal" not in json.loads(second_path.read_text())
    assert first_path.read_text() == "{not json"

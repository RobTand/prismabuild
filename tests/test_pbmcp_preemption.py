"""The preemption successor an agent is told about, read without the lock.

``pbrun._preemption_requeue`` is the authority on which generation a stopped
one was requeued as, and it answers under ``_transition_locked`` because it is
deciding what to tell a waiter.  ``pbmcp`` is reporting, not participating, so
it re-derives the same answer unlocked -- and re-deriving is only honest if it
reads the same evidence.  The evidence that is easy to miss is the immutable
attempt: a mutable ``done``/``failed`` row is one slot per action key, so a
later generation overwrites it and an intermediate handoff survives nowhere
else.  These tests hold the two readers against each other on exactly that
case.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve(strict=True).parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(REPOSITORY / "tests"))

from prismabuild import pool  # noqa: E402
import pbmcp  # noqa: E402
import pbrun  # noqa: E402


@pytest.fixture(autouse=True)
def known_generation_action(monkeypatch: pytest.MonkeyPatch) -> None:
    # These private logical-token fixtures stand for a verified generation
    # request, exactly as the #365 review fixtures do.
    monkeypatch.setattr(pool.cpu_admission, "action_identity",
                        lambda item: ("shape", False))


def _stopped(queue, key, generation):
    """The stopped generation's own ending, as admission filed it.

    A preemption does not leave the ending in ``withdrawn/``: the requeue
    republishes the key immediately, so the account of the stop is the
    immutable withdrawal decision for that generation.
    """

    for _path, record in queue.withdrawal_decisions(key):
        if record.get("published_unix") == generation:
            return record
    raise AssertionError(f"no withdrawal decision for generation {generation}")


def _overwritten_handoff(tmp_path: Path):
    """Stop the holder twice, end the second retry, then bury it.

    G1 is stopped for the foreground item and republished as G2; G2 is stopped
    the same way and republished as G3; G3 runs and ends; an unrelated G4 is
    then published and ends, overwriting the terminal row.  The G2->G3 handoff
    is now recorded only on G3's immutable attempt.
    """

    from test_preemption_review_boundaries import setup_holder

    queue, background, foreground, holder = setup_holder(tmp_path, max_attempts=3)
    generations = [holder["published_unix"]]
    stops = []
    for round_number in range(2):
        assert queue.claim(capacity={"gpu": 1}) is None
        queue.finish(background, status="withdrawn", claim_snapshot=holder)
        stops.append(_stopped(queue, background, generations[-1]))
        running = queue.claim(capacity={"gpu": 1})
        queue.finish(foreground, status="executed", claim_snapshot=running)
        holder = queue.claim(capacity={"gpu": 1})
        generations.append(holder["published_unix"])
        if round_number == 0:
            queue.publish(action_key=foreground, cas_root=tmp_path / "cas",
                          checkout_root=tmp_path,
                          worker_script=tmp_path / "worker.py",
                          resources={"gpu": 1})
    queue.finish(background, status="executed", detail={"stdout": "causal retry"},
                 claim_snapshot=holder)
    queue.publish(action_key=background, cas_root=tmp_path / "cas",
                  checkout_root=tmp_path, worker_script=tmp_path / "worker.py",
                  priority=0, resources={"gpu": 1}, max_attempts=1, retry_safe=False)
    unrelated = queue.claim(capacity={"gpu": 1})
    queue.finish(background, status="executed", detail={"stdout": "unrelated"},
                 claim_snapshot=unrelated)
    return queue, background, generations, stops


def test_the_successor_is_found_where_only_the_immutable_attempt_kept_it(
    tmp_path: Path,
) -> None:
    queue, key, generations, stops = _overwritten_handoff(tmp_path)
    second_stop = stops[1]
    assert second_stop["published_unix"] == generations[1]

    # The terminal row now belongs to the unrelated later generation, and no
    # live row names the retry, so a reader of the mutable queue alone cannot
    # see the handoff at all.
    terminal = json.loads(
        queue.item_path(pool.DONE, key).read_text(encoding="utf-8"))
    assert terminal["published_unix"] not in generations

    found = pbmcp._requeued_as(queue.root, key, second_stop)
    assert found == generations[2], "the G2->G3 handoff was not recovered"
    assert found == pbrun._preemption_requeue(queue, key, second_stop, generations[1])


def test_the_unlocked_reader_agrees_with_the_locked_one_at_every_stop(
    tmp_path: Path,
) -> None:
    queue, key, generations, stops = _overwritten_handoff(tmp_path)
    for stop in stops:
        generation = stop["published_unix"]
        assert (pbmcp._requeued_as(queue.root, key, stop)
                == pbrun._preemption_requeue(queue, key, stop, generation))


def test_an_ordinary_ending_names_no_successor(tmp_path: Path) -> None:
    """No ``preempted_by``, no handoff -- and no attempt scan to pay for."""

    queue, key, _generations, _stops = _overwritten_handoff(tmp_path)
    terminal = json.loads(
        queue.item_path(pool.DONE, key).read_text(encoding="utf-8"))
    assert terminal.get("preempted_by") is None
    assert pbmcp._requeued_as(queue.root, key, terminal) is None


def test_the_attempt_scan_never_opens_a_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The recovery reads attempt records, never the logs they link.

    ``pool.archived_preemption_outcomes`` is the verifying reader for this
    evidence and hashes every linked log to check it.  A status call that did
    the same would cost a gigabyte to ask about an action that printed one.
    """

    queue, key, _generations, stops = _overwritten_handoff(tmp_path)
    logs = sorted((queue.root / pool.ATTEMPTS).rglob("*.log"))
    assert logs, "the fixture published no attempt logs to protect"
    opened: list[str] = []
    real_open = Path.open

    def watched(self, *args, **kwargs):
        opened.append(str(self))
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", watched)
    assert pbmcp._requeued_as(queue.root, key, stops[1]) is not None

    attempts = str(queue.root / pool.ATTEMPTS)
    read = [name for name in opened if name.startswith(attempts)]
    assert [name for name in read if name.endswith(".json")], (
        "the watcher saw no attempt record, so it would not have seen a log")
    assert not [name for name in read if name.endswith(".log")]

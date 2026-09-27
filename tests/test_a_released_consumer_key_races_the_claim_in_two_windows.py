"""A released consumer key can still be claimed in two windows (#964).

#954 (PR #962) refuses a released key's ready row at claim, but both
refusals ride on facts read outside the key's transition lock, and an
operator release can land between them:

*   **Window 1, the submit side.** An old ``pbrun`` takes no lock and makes
    no release check between its declaration and its row. A row it
    publishes after the release has read the consumer's state but before
    the release's record lands is invisible to that state read, so the
    release succeeds while a live row exists. Claiming that row still
    needs the claim's own stale index listing (window 2's read): window 1
    is an old-submit publication race **plus** the stale-scan path, and the
    ``index_then_land`` hook below asserts the release's state read was
    still ``unpublished`` when the row landed.
*   **Window 2, the claim side.** ``PoolQueue._claim`` lists the release
    index once per scan, before the per-key transition lock and before the
    scan's own ``ready/`` listing. A release whose index entry lands after
    that listing is never confirmed for this scan, so the key is claimed
    even though the release record is on disk before the claim takes the
    lock. This test isolates the stale scan: the release completes first,
    without any old row in its blind interval, and the row follows it.

Either way the consumer is claimed and reads as ``live``, and the
retirement tick waits for it, because a claimed consumer reads ``live``
whatever its release says. The only batch at risk is one whose retirement
had already started: its consumer can then read a batch being deleted.

The reverse orderings are the safety controls, and the third test here
covers the claim-first one: a release that meets a claimed or submitting
key is refused by name, so a release can never file over a holder the
claim already owns. The release-completed-first ordering (a fresh scan
refuses the released row) is #954's
``test_the_claim_fails_a_row_published_after_its_release``, run green
beside this file as the same run's control shard.

These are the acceptance fixtures for the issue's fix: each drives the
real interleaving and then requires the named refusal
(``origin-consumer-released``) instead of a claimed row. On unpatched
main each fails at the final assertion, because the claim takes the row.

Fixture concessions, as in #954's tests: owners and consumers are
published, claimed and finished through the real ``PoolQueue``;
submissions and operator releases go through the real ``pbrun.main``; the
older ``pbrun`` is today's with ``submission_window`` a no-op (no lock, as
that code had), paused where it writes its row and resumed with the same
publication; a publish is the fleet's ``repo`` link moving to another
generation. The interleaving is held open by events rather than timing:
the claim's index listing really runs before the release's entry (window
2), or really runs before the release reaches its record while the old
row really lands in between (window 1).
"""
from __future__ import annotations

import threading
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_prepaid_writer_integration as fx  # noqa: E402
from prismabuild import pool, produced_output as po  # noqa: E402
from test_a_released_consumers_row_is_refused_at_claim import (  # noqa: E402
    _assert_refused, _declared_by_old_code,
)
from test_an_unpublished_declaration_is_released_once_nothing_can_publish_it import (  # noqa: E402
    _publish, _release, _release_argv,
)
from test_superseded_origin_consumers import (  # noqa: E402
    TAGS, _events, _refused,
)

_isolated_synthetic_launch_context = fx._isolated_synthetic_launch_context

RETIRED = po.ORIGIN_RETIRED_EVENT
WAIT_S = 30.0


class _ClaimScan:
    """One real ``queue.claim`` whose index listing is held open.

    ``released_origin_consumer_keys`` is the scan's whole view of the
    release index (#954, ``PoolQueue._claim``). This wrapper takes that
    listing for real, records it, and blocks the scan until the test has
    landed the release (and, in window 1, the old row) behind it -- the
    pause a busy scan provides in production.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.listed = threading.Event()
        self.resume = threading.Event()
        self.snapshot: set[str] = set()
        self.claimed: dict[str, object] | None = None
        self.failure: BaseException | None = None
        real = pool.PoolQueue.released_origin_consumer_keys

        def listing(queue: pool.PoolQueue) -> frozenset[str]:
            held = real(queue)
            self.snapshot = set(held)
            self.listed.set()
            if not self.resume.wait(WAIT_S):
                raise AssertionError("the test never released the claim scan")
            return held

        monkeypatch.setattr(pool.PoolQueue, "released_origin_consumer_keys",
                            listing)

    def run(self, queue: pool.PoolQueue) -> None:
        result: dict[str, object] = {}
        failure: list[BaseException] = []

        def claim() -> None:
            try:
                result["claimed"] = queue.claim(
                    owner="w-consumer", tags=list(TAGS))
            except BaseException as exc:      # noqa: BLE001 - re-raised below
                failure.append(exc)

        self._result = result
        self._failure = failure
        self.thread = threading.Thread(target=claim, daemon=True)
        self.thread.start()
        assert self.listed.wait(WAIT_S), "the claim never listed the index"

    def release(self) -> None:
        """Resume and join, so no claim thread outlives the test.

        Idempotent, and called from a ``finally`` in each test: a failed
        fixture assertion must not leave a daemon claim blocked on the
        event while pytest tears the temporary path down.
        """

        self.resume.set()
        thread = getattr(self, "thread", None)
        if thread is None:
            return
        thread.join(WAIT_S)
        assert not thread.is_alive(), "the claim never finished"

    def finish(self) -> dict[str, object] | None:
        self.release()
        if self._failure:
            raise self._failure[0]
        self.claimed = self._result.get("claimed")  # type: ignore[assignment]
        return self.claimed


def _row_is_ready(queue: pool.PoolQueue, key: str) -> None:
    assert queue.item_path(pool.READY, key).exists()
    assert po._key_generation(queue, key)[0] == pool.READY


def _assert_held_by_the_live_consumer(queue, key: str, origin: Path) -> None:
    """The race's consequence: claimed and reading ``live``, batch waits."""

    assert queue.item_path(pool.CLAIMED, key).exists(), (
        "the released row was not claimed")
    assert po._consumer_state(queue, key) == "live"
    assert _events(queue, RETIRED) == []
    assert origin.exists(), "the batch this consumer reads was deleted"


# -- window 1: the old submitter's row lands before the release's record --------


def test_an_old_submitters_row_landing_before_the_record_is_still_refused(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Released while an old submitter is paused, row lands before the record.

    The release has read the consumer's state (``unpublished``) and is at
    its index write; the old row lands before the record write. The scan's
    listing already happened, so its empty snapshot is stale; on unpatched
    main the claim takes the row.
    """

    queue, instance, path, committed, key, resume = _declared_by_old_code(
        tmp_path, monkeypatch)
    _publish(tmp_path, "g-next")
    assert po._key_generation(queue, key)[0] == "absent"

    scan = _ClaimScan(monkeypatch)
    try:
        scan.run(queue)

        # The claim listed the index while the declaration was still the
        # only artifact: the listing cannot see the release its own scan
        # races.
        assert scan.snapshot == set()

        landed_state: dict[str, str] = {}
        real_index = po._index_release

        def index_then_land(*args, **kwargs):
            # Inside ``_release_origin_consumer_locked``, past its state
            # read and at its record threshold: the release has decided.
            landed_state["state"] = po._consumer_state(queue, key)
            real_index(*args, **kwargs)
            resume()

        monkeypatch.setattr(po, "_index_release", index_then_land)
        answer = _release(monkeypatch, capsys, committed, key)
        assert answer["released"] is True and answer["state"] == "unpublished"
        assert landed_state == {"state": "unpublished"}, (
            "the old row landed before the release read the consumer's "
            "state; this fixture is not window 1")
        _row_is_ready(queue, key)
        assert po.origin_consumer_release(queue, key) is not None, (
            "the release record must be on disk before the claim takes "
            "the lock")
        scan.resume.set()

        claimed = scan.finish()
        if claimed is not None:
            _assert_held_by_the_live_consumer(queue, key, path)
        assert claimed is None, (
            f"the released row was claimed "
            f"({str(claimed.get('action_key'))[:12]}); the claim's index "
            "listing predated the entry and it never re-checked under the "
            "key's transition lock")
        _assert_refused(queue, key, committed)
    finally:
        scan.release()


# -- window 2: the scan's index listing lands before the entry ------------------


def test_a_scan_listing_before_the_entry_is_still_refused(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """The release lands between the scan's two listings; the row follows it.

    The scan's index listing is empty and genuinely precedes the entry;
    the release then completes, the old row is published, and the scan's
    ``ready/`` listing sees it. On unpatched main the claim takes the row,
    exactly the second window #962's limits name.
    """

    queue, instance, path, committed, key, resume = _declared_by_old_code(
        tmp_path, monkeypatch)
    _publish(tmp_path, "g-next")
    assert po._key_generation(queue, key)[0] == "absent"

    scan = _ClaimScan(monkeypatch)
    try:
        scan.run(queue)
        assert scan.snapshot == set()
        assert not list(queue.released_origin_consumers_dir().glob(
            f"{key}.*.json")), (
                "the index entry must not exist when the scan lists it")

        answer = _release(monkeypatch, capsys, committed, key)
        assert answer["released"] is True and answer["state"] == "unpublished"
        assert po.origin_consumer_release(queue, key) is not None
        resume()
        _row_is_ready(queue, key)
        scan.resume.set()

        claimed = scan.finish()
        if claimed is not None:
            _assert_held_by_the_live_consumer(queue, key, path)
        assert claimed is None, (
            f"the released row was claimed "
            f"({str(claimed.get('action_key'))[:12]}); the entry landed "
            "after the scan's listing and the claim never re-checked under "
            "the key's transition lock")
        _assert_refused(queue, key, committed)
    finally:
        scan.release()


# -- safety controls ------------------------------------------------------------


def test_a_release_of_a_claimed_consumer_is_refused(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Claim first: the release refuses by name and files nothing.

    The fence proposal must not lose a live holder in the other direction.
    A release that meets a row this claim already owns reads ``live`` and
    refuses ``origin-consumer-live``, so no released record can ever cover
    a claimed generation. Passes on unpatched main.
    """

    queue, instance, path, committed, key, resume = _declared_by_old_code(
        tmp_path, monkeypatch)
    resume()
    _row_is_ready(queue, key)
    claimed = queue.claim(owner="w-consumer", tags=list(TAGS))
    assert claimed is not None and claimed["action_key"] == key
    _publish(tmp_path, "g-next")

    refused = _refused(monkeypatch, *_release_argv(committed, key))
    assert "origin-consumer-live" in refused, refused
    assert po.origin_consumer_release(queue, key) is None
    assert not po._released_consumers_dir(queue.root, instance, "b1").exists()
    assert po._consumer_state(queue, key) == "live"
    assert _events(queue, RETIRED) == []
    assert path.exists()

"""An acquire must not re-parse cover documents that did not change.

``acquire`` reads each covering mover's material sidecar and fragment
twice per call: once as a pre-check, and again under the stage ownership
lock before it pins. A reader that passes a fresh ``context`` on every
call parsed and validated both documents both times, on every acquire,
and half of that work ran while holding the lock that every mover of the
stage root also takes. The documents grow with the mover's window, so the
cost of one acquire grows with the window as well.

The cover lookup already keeps validated documents per process and reuses
one only while the descriptor it just opened shows the identity it
validated (#893). An acquire uses the same store for both of its reads:
every read still opens the file, the locked one under the lock, so every
answer is as fresh as a plain read, and a document is parsed again only
when its file changed. The tests below pin the saving and the freshness:
a repeat acquire of unchanged documents validates nothing; a republish
between acquires, or between the pre-check and the lock, is seen; and the
answers equal those of an acquire with an empty store.

Fixtures use tmp_path roots only. Run via published pbtest at -10.
"""
from __future__ import annotations

import contextlib
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map, reader_lease  # noqa: E402
import stage_release  # noqa: E402

CONSUMER = "c" * 64
MOVER = "e" * 64
TIER = "prismabuild-stage:dl380g10"
MANIFEST = "a" * 64
ATTEMPT = {"nonce": "n1", "scope_id": "s1"}
HOLDER = {"host": "test-host", "pid": 4242}


@pytest.fixture(autouse=True)
def _fresh_store():
    reader_lease.clear_cover_docs_cache()
    yield
    reader_lease.clear_cover_docs_cache()


@pytest.fixture()
def fleet(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    return queue, stage, queue.root / pool.RESIDENCY


@pytest.fixture()
def counted(monkeypatch):
    """Count every sidecar and fragment validation the reader performs."""

    counts = {"material": 0, "fragment": 0}
    real_material = reader_lease.validate_material
    real_fragment = residency_map.validate_fragment

    def material(value):
        counts["material"] += 1
        return real_material(value)

    def fragment(value):
        counts["fragment"] += 1
        return real_fragment(value)

    monkeypatch.setattr(reader_lease, "validate_material", material)
    monkeypatch.setattr(residency_map, "validate_fragment", fragment)
    return counts


def _source(index: int) -> str:
    return f"/mnt/shared/window/entry-{index:04d}.bin"


def _key(index: int) -> str:
    return residency_map.residency_map_key(_source(index), 0)


def _publish(root: Path, stage: Path, indices, *, fill: int = 0) -> str:
    """One publication of ``indices``: fragment first, then its sidecar."""

    fragment, material = {}, {}
    for index in indices:
        staged = stage / f"entry-{index:04d}.bin"
        payload = bytes([(index + fill) % 256]) * 512
        if not staged.exists() or staged.read_bytes() != payload:
            staged.write_bytes(payload)
        digest = f"{index + 1 + fill:064x}"
        fragment[_key(index)] = {"stage_path": str(staged), "bytes": 512,
                                 "sha256": digest, "offset": 0}
        identity = reader_lease.stat_identity(str(staged))
        assert identity is not None
        material[_key(index)] = {"stage_path": str(staged), "bytes": 512,
                                 "sha256": digest, "file_id": identity}
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": MANIFEST, "entries": fragment})
    generation = reader_lease.mint_generation()
    reader_lease.write_material(
        root, consumer_action_key=CONSUMER, mover_action_key=MOVER,
        tier_id=TIER, stage_root=str(stage), manifest_sha256=MANIFEST,
        generation=generation, entries=material)
    return generation


def _acquire(queue, indices, token: str):
    """What a reader does: a fresh context on every call."""

    return reader_lease.acquire(
        queue, consumer_action_key=CONSUMER, attempt=ATTEMPT, tier_id=TIER,
        epoch="", span={"start_bytes": 0, "end_bytes": 512 * len(indices)},
        holder=HOLDER, acquire_token=token,
        covers=[{"mover_action_key": MOVER, "manifest_sha256": MANIFEST}],
        expected={_key(index): {"bytes": 512, "sha256": None}
                  for index in indices},
        context={})


def _release(queue, answer) -> None:
    assert reader_lease.release(queue, answer["pin_id"], answer["ref_id"],
                                consumer_action_key=CONSUMER) is True


def test_a_repeat_acquire_of_unchanged_documents_validates_nothing(
        fleet, counted) -> None:
    queue, stage, root = fleet
    _publish(root, stage, range(64))
    counted.update(material=0, fragment=0)

    first = _acquire(queue, range(0, 16), "window-0")
    assert first["ok"], first
    # The first acquire validates each document once and reuses it under
    # the lock: its file did not change between the two reads.
    assert counted == {"material": 1, "fragment": 1}
    _release(queue, first)

    for number, start in enumerate(range(16, 64, 16), start=1):
        answer = _acquire(queue, range(start, start + 16), f"window-{number}")
        assert answer["ok"], answer
        assert [entry["key"] for entry in answer["pin"]["entries"]] == [
            _key(index) for index in range(start, start + 16)]
        _release(queue, answer)
    assert counted == {"material": 1, "fragment": 1}


def test_answers_equal_an_acquire_with_an_empty_store(fleet) -> None:
    queue, stage, root = fleet
    _publish(root, stage, range(8))
    warm = _acquire(queue, range(8), "warm")
    assert warm["ok"], warm
    _release(queue, warm)
    reused = _acquire(queue, range(8), "reused")
    _release(queue, reused)
    reader_lease.clear_cover_docs_cache()
    cold = _acquire(queue, range(8), "cold")
    _release(queue, cold)
    for answer in (reused, cold):
        assert answer["ok"], answer
    assert reused["pin"]["entries"] == cold["pin"]["entries"]
    assert reused["pin"]["covers"] == cold["pin"]["covers"]
    assert reused["pin_id"] == cold["pin_id"]


def test_a_republish_between_acquires_is_read_again(fleet, counted) -> None:
    queue, stage, root = fleet
    first_generation = _publish(root, stage, range(8))
    first = _acquire(queue, range(4), "before")
    assert first["ok"], first
    _release(queue, first)

    # The mover republishes without entries 4..7 and with new bytes for
    # 0..3: the documents are new files, so both are read and validated
    # again, and the pin names the new generation and the new bytes.
    counted.update(material=0, fragment=0)
    second_generation = _publish(root, stage, range(4), fill=7)
    counted.update(material=0, fragment=0)
    second = _acquire(queue, range(4), "after")
    assert second["ok"], second
    assert counted["material"] >= 1 and counted["fragment"] >= 1
    assert second_generation != first_generation
    assert {entry["generation"] for entry in second["pin"]["entries"]} == {
        second_generation}
    assert [entry["sha256"] for entry in second["pin"]["entries"]] == [
        f"{index + 1 + 7:064x}" for index in range(4)]
    _release(queue, second)
    gone = _acquire(queue, range(4, 8), "gone")
    assert gone == {"ok": False, "refusal": "source-coverage-gap"}


def test_a_republish_after_the_precheck_is_seen_under_the_lock(
        fleet, monkeypatch) -> None:
    """The locked read is a fresh read even when the pre-check was reused."""

    queue, stage, root = fleet
    _publish(root, stage, range(8))
    warm = _acquire(queue, range(4), "warm")
    assert warm["ok"], warm
    _release(queue, warm)

    real_lock = queue.stage_ownership_lock
    republished = []

    @contextlib.contextmanager
    def republish_then_lock(stage_root):
        if not republished:
            republished.append(_publish(root, stage, range(8), fill=3))
        with real_lock(stage_root) as held:
            yield held

    monkeypatch.setattr(queue, "stage_ownership_lock", republish_then_lock)
    raced = _acquire(queue, range(4), "raced")
    assert republished
    # The pre-check proved the old generation; the lock reads the new
    # sidecar and refuses rather than pin a generation that is gone.
    assert raced == {"ok": False, "refusal": "unpublished"}
    monkeypatch.setattr(queue, "stage_ownership_lock", real_lock)
    retried = _acquire(queue, range(4), "retried")
    assert retried["ok"], retried
    assert {entry["generation"] for entry in retried["pin"]["entries"]} == {
        republished[0]}
    _release(queue, retried)


def test_a_missing_or_malformed_document_refuses_as_before(fleet) -> None:
    queue, stage, root = fleet
    _publish(root, stage, range(4))
    warm = _acquire(queue, range(4), "warm")
    assert warm["ok"], warm
    _release(queue, warm)

    fragment = residency_map.fragment_path(root, CONSUMER, MOVER)
    saved = fragment.read_bytes()
    fragment.write_text("{not json")
    broken = _acquire(queue, range(4), "broken")
    assert broken["ok"] is False
    assert broken["refusal"].startswith("ownership-uncertain: ")
    fragment.unlink()
    absent = _acquire(queue, range(4), "absent")
    assert absent == {"ok": False, "refusal": "unpublished"}
    fragment.write_bytes(saved)
    restored = _acquire(queue, range(4), "restored")
    assert restored["ok"], restored
    _release(queue, restored)

    material = reader_lease.material_path(root, CONSUMER, MOVER)
    material.unlink()
    unqualified = _acquire(queue, range(4), "unqualified")
    assert unqualified == {"ok": False, "refusal": "no-file-identity"}

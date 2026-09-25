"""A whole-range egress holds the stage ownership lock for its act, not its census (#988).

Before #988 ``stage_release.evict`` took the stage root's ownership lock and
ran its four censuses, a per-entry judgement (two ``resolve`` calls each) and
a per-entry unlink and directory prune inside one hold.  Every mover's
publication, every reader's pin and every new mover's start gate on that root
waited for the whole range.  Campaign ranges are 1,680 to 161,572 entries.

This is the campaign-shaped case from the audit (WS-DA finding 1): a landed
range of 20,000 one-KiB entries, three real publishers
(``stage_move._StagedPublisher.publish``, each in its own process so the lock
between them is the real inter-process ``fcntl`` lock) publishing 2,000 fresh
entries each, and ``evict(whole=True)`` on the landed range, which is what
the tier loop's beyond-horizon eviction runs.  The publishers start the moment
the egress first holds the lock, so every one of them meets it.

It asserts what the hold does, not how long it takes, because a hold's
seconds depend on the box (#1005: 0.25 s alone, 1.22 s beside a second
20,000-entry egress on the same disk):

* **Structure, the primary assertion.**  While the egress holds the lock it
  parses no census document: no fragment, no pin and no material sidecar.
  The test counts the parses itself, by wrapping the three validators
  (``residency_map.validate_fragment``, ``reader_lease.validate_pin`` and
  ``reader_lease.validate_material``) and counting only the calls made
  while the lock is held, and the receipt must say the same
  (``locked_parses == 0``, with the landed range's fragment and sidecar
  reused by their file version, ``locked_reuses >= 2``).  Before #988 the
  egress parsed every fragment under the lock, so this fails on main
  whatever the box.
* **The records.**  One hold; the receipt's ``lock_held_s`` matches the hold
  measured around the lock from outside; ``entries_judged`` is the range;
  the census before the lock took time; the re-check and the unlinks fit
  inside the hold.
* **A guard against sliding back, not a performance limit.**  The longest
  hold and the publishers' p99 publication latency during it stay under
  ``REGRESSION_GUARD_S``, the shortest hold this test measured on main alone
  (3.52 s, ``RED_MEASURED``).  Only a return to the census-under-lock regime
  reaches it; a slow or shared box does not.

Measured through PrismaBuild on sparky (``--tag sparky``, one pytest worker,
four CPUs reserved; keys in #1005):

* origin/main: one hold of 3.52 s and 3.74 s alone, 5.45 s beside another
  20,000-entry egress; the publishers waited out the rest of it;
* after the fix: one hold of 0.25 s alone (0.16 s of it the 20,000 unlinks,
  which stay under the lock), 1.22 s beside another egress; the 1.05 s
  census runs before the hold.

The hold that remains grows with the range and with the disk, because the
unlinks stay under the lock; see ``docs/design.md``, "The egress holds the
lock for its act, not its census".

The 1.22 s number above is exactly the distortion #1008 item 4 names: this
test's own two 20,000-entry egresses shared one box's disk-metadata
throughput with each other, which neither ``cpu`` nor ``mem_gb`` demand
prices.  Run through ``pbtest`` with the new ``disk_metadata`` reservation
(``pbtest.py --disk-metadata --tag sparky ...``) so a re-measurement is not
also distorted by whatever else is sharing the box.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import math
import multiprocessing
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, reader_lease, residency_map, storage_tiers  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
ENTRY_BYTES = 1024
RANGE_ENTRIES = 20_000
PUBLISHERS = 3
PER_PUBLISHER = 2_000
DIGEST = "a" * 64

#: The runs the guard is taken from; see the module docstring.
RED_MEASURED = {"tree": "origin/main e9b66ea8cee4", "holds": 1,
                "longest_hold_s": 3.5196, "p99_overlapping_s": 3.52}
GREEN_MEASURED = {"tree": "fix/988-egress-lock-scope", "holds": 1,
                  "longest_hold_s": 0.2485, "p99_overlapping_s": 0.2487,
                  "census_s": 1.052, "unlink_s": 0.162}
#: The shortest hold measured on main alone.  A guard against the old regime
#: only: the structure assertions carry the test.
REGRESSION_GUARD_S = RED_MEASURED["longest_hold_s"]


def _key(label: str) -> str:
    return hashlib.sha256(f"test-988:{label}".encode()).hexdigest()


LANDED_CONSUMER = _key("landed-consumer")
LANDED_MOVER = _key("landed-mover")
LANDED_MANIFEST = _key("landed-manifest")


def _land_range(queue: pool.PoolQueue, stage: Path, root: Path) -> None:
    """A complete mover's range: files, fragment, material and its tier token."""

    entries: dict[str, dict[str, object]] = {}
    material: dict[str, dict[str, object]] = {}
    for number in range(RANGE_ENTRIES):
        declared = f"/pool/landed/part-{number}.bin"
        key = residency_map.residency_map_key(declared, 0)
        path = stage / "landed" / f"d{number // 500:03d}" / f"part-{number}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"\0" * ENTRY_BYTES)
        entries[key] = {"stage_path": str(path), "bytes": ENTRY_BYTES,
                        "offset": 0, "sha256": DIGEST}
        material[key] = {"stage_path": str(path), "bytes": ENTRY_BYTES,
                         "sha256": DIGEST,
                         "file_id": reader_lease.stat_identity(str(path))}
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": LANDED_CONSUMER,
        "mover_action_key": LANDED_MOVER,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": LANDED_MANIFEST, "entries": entries})
    reader_lease.write_material(
        root, consumer_action_key=LANDED_CONSUMER,
        mover_action_key=LANDED_MOVER, tier_id=TIER, stage_root=str(stage),
        manifest_sha256=LANDED_MANIFEST,
        generation=reader_lease.mint_generation(), entries=material)
    # The mover that staged it holds its tier token until an egress frees it.
    queue.publish(action_key=LANDED_MOVER, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1, STAGE_KIND: 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "tier_id": TIER,
                             "manifest_sha256": LANDED_MANIFEST,
                             "manifest_bytes": 1 << 40,
                             "range_start_bytes": 0,
                             "range_end_bytes": storage_tiers.GIB},
                  max_attempts=1, retry_safe=False)
    claimed = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claimed is not None and claimed["action_key"] == LANDED_MOVER
    queue.record_move(LANDED_MOVER, {
        "consumer_action_key": LANDED_CONSUMER, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": LANDED_MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": storage_tiers.GIB,
        "bytes_staged": storage_tiers.GIB, "complete": True})
    queue.finish(LANDED_MOVER, status="executed")
    assert queue.tier_ledger(TIER).holder_tokens(LANDED_MOVER) == {"stage_gib": 1}


def _publisher(pool_root: str, stage: str, root: str, index: int,
               pairs: list[tuple[str, str]], go, out: str) -> None:
    """One real publisher: first publication of its own fresh entries."""

    queue = pool.PoolQueue(Path(pool_root))
    publisher = stage_move._StagedPublisher(
        queue=queue, stage_root=stage, residency_root=root,
        mover_action_key=_key(f"publisher-{index}"),
        manifest_sha256=_key(f"publisher-manifest-{index}"), tier_id=TIER,
        cas_root=Path(pool_root).parent / "cas",
        consumer_action_key=_key(f"publisher-consumer-{index}"))
    publisher.begin_material()
    samples: list[tuple[float, float]] = []
    go.wait(300)
    for temporary, destination in pairs:
        entry = {"bytes": ENTRY_BYTES, "sha256": DIGEST, "offset": 0,
                 "path": destination}
        asked = time.monotonic()
        publisher.publish(entry, Path(destination), Path(temporary), DIGEST)
        samples.append((asked, time.monotonic()))
    Path(out).write_text(json.dumps(samples))


def _p99(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(0.99 * len(ordered)) - 1)]


def test_a_whole_range_egress_never_parks_the_publishers_for_its_range(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    stage = tmp_path / "stage"
    stage.mkdir()
    stage = stage.resolve()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=stage) == "registered"
    root = queue.root / pool.RESIDENCY
    _land_range(queue, stage, root)

    # Each publisher's copies are already verified in temporaries; only the
    # publication -- the decision and the rename under the lock -- is timed.
    plans: list[list[tuple[str, str]]] = []
    for index in range(PUBLISHERS):
        pairs = []
        temps = stage / f"publisher-{index}" / "temps"
        temps.mkdir(parents=True)
        for number in range(PER_PUBLISHER):
            destination = (stage / f"publisher-{index}" / f"d{number // 500:03d}"
                           / f"part-{number}.bin")
            destination.parent.mkdir(parents=True, exist_ok=True)
            temporary = temps / f".part-{number}.bin.partial"
            temporary.write_bytes(b"\1" * ENTRY_BYTES)
            pairs.append((str(temporary), str(destination)))
        plans.append(pairs)

    context = multiprocessing.get_context("fork")
    go = context.Event()
    outputs = [tmp_path / f"publisher-{index}.json" for index in range(PUBLISHERS)]
    workers = [context.Process(target=_publisher, args=(
        str(queue.root), str(stage), str(root), index, plans[index], go,
        str(outputs[index]))) for index in range(PUBLISHERS)]
    for worker in workers:
        worker.start()

    # Every census document the egress parses while it holds the lock,
    # counted here from the validators themselves rather than taken from
    # the receipt.  The publishers were forked before this, so only the
    # egress, in this process, is counted.
    holding = [False]
    parsed_under_lock = {"fragment": 0, "pin": 0, "material": 0}

    def counted(kind: str, validate):
        def wrapper(*args, **kwargs):
            if holding[0]:
                parsed_under_lock[kind] += 1
            return validate(*args, **kwargs)
        return wrapper

    monkeypatch.setattr(residency_map, "validate_fragment",
                        counted("fragment", residency_map.validate_fragment))
    monkeypatch.setattr(reader_lease, "validate_pin",
                        counted("pin", reader_lease.validate_pin))
    monkeypatch.setattr(reader_lease, "validate_material",
                        counted("material", reader_lease.validate_material))

    # The lock, measured from outside the egress: every hold's request,
    # grant and release.  The first grant releases the publishers, so each
    # of them meets the egress while it holds the lock.
    holds: list[tuple[float, float, float]] = []
    original = queue.stage_ownership_lock

    @contextmanager
    def measured(stage_root, *, blocking: bool = True):
        asked = time.monotonic()
        with original(stage_root, blocking=blocking) as got:
            granted = time.monotonic()
            holding[0] = True
            go.set()
            try:
                yield got
            finally:
                holding[0] = False
                holds.append((asked, granted, time.monotonic()))

    queue.stage_ownership_lock = measured  # type: ignore[method-assign]
    try:
        began = time.monotonic()
        receipt = stage_release.evict(
            queue, LANDED_MOVER, consumer_action_key=LANDED_CONSUMER,
            stage_root=str(stage), reason="beyond-horizon", whole=True)
        ended = time.monotonic()
    finally:
        go.set()
        for worker in workers:
            worker.join(300)
    assert all(worker.exitcode == 0 for worker in workers), [
        worker.exitcode for worker in workers]

    assert receipt["complete"] is True, receipt
    assert receipt["entries_deleted"] == RANGE_ENTRIES, receipt
    assert receipt["tokens_released"] == 1, receipt
    assert not (stage / "landed").exists() or not any(
        (stage / "landed").rglob("*.bin")), "every landed file is gone"
    for index in range(PUBLISHERS):
        published = list((stage / f"publisher-{index}").rglob("part-*.bin"))
        assert len(published) == PER_PUBLISHER, "every publication landed"

    samples = [tuple(pair) for output in outputs
               for pair in json.loads(output.read_text())]
    held_from = min(granted for _asked, granted, _released in holds)
    held_until = max(released for _asked, _granted, released in holds)
    overlapping = [end - start for start, end in samples
                   if end >= held_from and start <= held_until]
    longest = max(released - granted for _asked, granted, released in holds)
    measured_summary = {
        "egress_s": round(ended - began, 4),
        "holds": len(holds),
        "longest_hold_s": round(longest, 4),
        "total_held_s": round(sum(r - g for _a, g, r in holds), 4),
        "publications_overlapping": len(overlapping),
        "p99_overlapping_s": round(_p99(overlapping), 4) if overlapping else None,
        "max_overlapping_s": round(max(overlapping), 4) if overlapping else None,
        "parsed_under_lock": parsed_under_lock,
        "receipt": {name: receipt.get(name) for name in (
            "lock_wait_s", "lock_held_s", "entries_judged", "census_s",
            "census_validate_s", "unlink_s", "prune_s", "locked_parses",
            "locked_reuses")},
    }
    print("measured-988", json.dumps(measured_summary, sort_keys=True))
    assert len(overlapping) >= PUBLISHERS, (
        f"the publishers never met the egress: {measured_summary}")

    # Structure first: under the lock the census is listings and fstats.
    # Every document it opens there is reused by its file version, because
    # nothing changed it since the census before the lock.
    assert parsed_under_lock == {"fragment": 0, "pin": 0, "material": 0}, (
        f"the egress parsed census documents under the lock: {measured_summary}")
    assert receipt["locked_parses"] == 0, measured_summary
    assert receipt["locked_reuses"] >= 2, measured_summary  # fragment, sidecar

    # The hold is a record, not only a measurement taken from outside: one
    # hold, the one measured around the lock, and the census before it.
    assert len(holds) == 1, measured_summary
    assert receipt["lock_held_s"] <= longest + 0.01, measured_summary
    assert receipt["lock_held_s"] >= longest - 0.05, measured_summary
    assert receipt["entries_judged"] == RANGE_ENTRIES, measured_summary
    assert receipt["census_s"] > 0.0, measured_summary
    assert 0.0 <= receipt["census_validate_s"] <= receipt["lock_held_s"], (
        measured_summary)
    assert 0.0 <= receipt["unlink_s"] <= receipt["lock_held_s"], measured_summary

    # A guard against the old regime, not a limit on this box: the shortest
    # hold main took alone.  A shared disk slows the unlinks that stay in the
    # hold (1.22 s beside another egress); only a census back under the lock
    # reaches this.
    assert longest < REGRESSION_GUARD_S, (
        f"the egress held the stage as long as main did: {measured_summary}")
    assert _p99(overlapping) < REGRESSION_GUARD_S, (
        f"a publication waited as long as it did on main: {measured_summary}")

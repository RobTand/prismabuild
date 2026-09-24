"""A copy a running consumer is blocked on goes first on its tier (#1091, #1090).

On 2026-09-24 a claimed consumer holding a GPU and 94 GB waited 494.8 s on one
14.8 GB stage copy (mover ``31b73cdb5258``).  The copy was one of 7 movers on
the dl380g10 stage, each at depth 16 on four HDDs.  The pool delivered
250.6 MB/s at 88% utilization, the copy got 30.8 MB/s, and the disk pacer held
it 96.5 s for other clients.  Meanwhile the tier kept claiming copies for the
same consumer's later phases, and the landing record promised the copy in 8 s:
it was priced from the leg's ARC-warm copies and the mover sent no reports.

Four cases, each red on ``origin/main`` ``b96ffa59b9bf`` with its own
assertion:

* **Need order.**  A copy that a claimed consumer declared a wait on (#1018)
  is claimed before any later-phase copy, and the later-phase copies are not
  claimed while it waits.
* **Concurrency.**  With more copies ready than the pool can serve at once,
  the tier never has more copies claimed than the cap its own receipts
  measured: the smallest mover count at which the pool's delivered rate stops
  rising.
* **Pacer.**  The disk pacer never holds a copy a claimed consumer waits on,
  and it still paces the others, which also yield to that copy.
* **Expectation.**  A claimed copy reports the bytes it has copied, so its
  expected landing comes from its own live rate.  A cold copy is never priced
  from its leg's warm copies: a copy whose file-side rate beat the whole
  pool's delivery did not read the pool.

Everything runs on ``tmp_path`` queues and stage roots; nothing touches a live
queue or a real stage mountpoint (#628).
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import core as pb  # noqa: E402
from prismabuild import (  # noqa: E402
    pool, progress as pb_progress, residency_map, residency_plan, storage_tiers)
import prewarm_loop  # noqa: E402
import tier_loop  # noqa: E402
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _claim, _fixture_queue, _land, _mover, _plan, _publish_consumer)
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    GIB, PHASE_GIB, STAGE_KIND, TIER, _cycle, _hexkey, _row)

MIB = 1 << 20
READER = _hexkey("blocked-reader")
READER_MANIFEST = "7" * 64
FILL_KIND = f"{storage_tiers.FILL_KIND}{storage_tiers.TIER_DEMAND_SEPARATOR}{TIER}"
#: A capacity the claim pass can never run out of, so only the tier decides.
WORKER = {"cpu": 64, "mem_gb": 512}


def _claim_all(queue: pool.PoolQueue, *, limit: int = 12) -> list[str]:
    """Run claim passes until one claims nothing, the way a worker polls."""

    claimed: list[str] = []
    for _ in range(limit):
        item = queue.claim(capacity=dict(WORKER), tags=["dl380g10"])
        if item is None:
            break
        claimed.append(str(item["action_key"]))
    return claimed


def _declare_wait(queue: pool.PoolQueue, consumer: str, movers: list[str], *,
                  since_unix: float) -> Path:
    """The consumer's reader blocked on ``movers``, as PQ's reader files it."""

    path = Path(pb_progress.staged_wait_path(
        str(queue.action_progress_path(consumer))))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "schema": pb_progress.STAGED_WAIT_SCHEMA_V1, "token": "t" * 32,
        "since_unix": float(since_unix), "movers": list(movers)}))
    return path


def _tier_record(queue: pool.PoolQueue) -> dict[str, object]:
    return json.loads(queue.tier_record_path(TIER).read_text())


# ---------------------------------------------------------------- need order


def test_the_copy_a_consumer_waits_on_is_claimed_first_and_alone(
        tmp_path: Path) -> None:
    """The incident's order: later phases queued first, the waited copy last.

    ``phase-2`` and ``phase-3`` were published before ``phase-1`` (a copy the
    window republished, or one sealed later), so the queue's oldest-first
    order claims them first.  The reader is blocked on ``phase-1``.  Every
    copy claimed while it waits reads the same four spindles and slows it.
    """

    queue, stage = _fixture_queue(tmp_path, 40)
    plan = _plan(queue, READER, label="need", manifest=READER_MANIFEST, phases=4)
    _publish_consumer(queue, READER, plan, manifest=READER_MANIFEST)
    now = time.time()
    _claim(queue, READER, phase="phase-0", claimed_unix=now - 100.0,
           reported_unix=now - 10.0)
    rows = {ordinal: dict(phase["mover_row"])             # type: ignore[index]
            for ordinal, phase in enumerate(plan["phases"])}  # type: ignore[arg-type]
    for ordinal in (2, 3, 1):
        queue.publish(**rows[ordinal])
        time.sleep(0.01)
    waited = str(rows[1]["action_key"])
    later = {str(rows[2]["action_key"]), str(rows[3]["action_key"])}
    _declare_wait(queue, READER, [waited], since_unix=now - 5.0)

    _cycle(queue, stage, gib=40)
    claimed = _claim_all(queue)

    assert claimed and claimed[0] == waited, (
        f"the copy the reader waits on was not claimed first: {claimed}")
    assert not later & set(claimed), (
        f"later-phase copies were claimed while the reader waits: {claimed}")
    plan_record = _tier_record(queue)["reader_plan"]
    assert [row["mover_action_key"] for row in plan_record["declared_wait"]] == [waited]
    assert plan_record["declared_wait"][0]["consumers"] == [READER]


# --------------------------------------------------------------- concurrency

#: Pool delivery by movers claimed on the tier, two 100 s receipts each.  The
#: rate rises to 3 movers and stops: 4 is no faster and 6 is slower.  Per
#: level the duration-weighted mean and its standard error (5 MB/s for two
#: equal-weight receipts 10 apart); the best mean is 405 at 3, and the
#: smallest level whose mean plus its error reaches 405 - 5 is 3.
CURVE = {1: (200.0, 210.0), 2: (300.0, 310.0), 3: (400.0, 410.0),
         4: (395.0, 405.0), 6: (300.0, 310.0)}
KNEE = 3


def _file_curve(queue: pool.PoolQueue, stage: Path) -> None:
    ordinal = 0
    for movers, rates in CURVE.items():
        for rate in rates:
            ordinal += 1
            key = _hexkey(f"curve{ordinal}")
            seconds = 100.0
            staged = int(rate * 1e6 * seconds / movers)
            queue.record_move(key, {
                "schema": storage_tiers.MOVER_RECEIPT_SCHEMA,
                "action_key": key, "consumer_action_key": _hexkey(f"c{ordinal}"),
                "tier_id": TIER, "stage_root": str(stage),
                "manifest_sha256": "8" * 64, "range_start_bytes": 0,
                "range_end_bytes": staged, "range_bytes": staged,
                "bytes_staged": staged, "entries_declared": 4,
                "entries_staged": 4, "complete": True, "seconds": seconds,
                "mb_per_s_file_side": round(staged / 1e6 / seconds, 1),
                "disk_pacing": {"mean_pool_read_mb_s": rate,
                                "pool_read_bytes": int(rate * 1e6 * seconds),
                                "held_seconds": 0.0},
                storage_tiers.MOVER_CONCURRENCY_FIELD: movers,
                "unix": 1000.0 + ordinal})


def test_the_tier_claims_no_more_copies_than_its_measured_knee(
        tmp_path: Path) -> None:
    queue, stage = _fixture_queue(tmp_path, 64)
    _file_curve(queue, stage)
    _cycle(queue, stage, gib=64)
    ready = []
    for ordinal in range(6):
        start, end = ordinal * PHASE_GIB * GIB, (ordinal + 1) * PHASE_GIB * GIB
        row = {**_row(queue, _hexkey(f"ready{ordinal}"),
                      {STAGE_KIND: PHASE_GIB, "cpu": 1, "mem_gb": 1}),
               "residency": {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                             "manifest_sha256": "6" * 64,
                             "manifest_bytes": 64 * GIB,
                             "range_start_bytes": start, "range_end_bytes": end}}
        queue.publish(**row)
        ready.append(row["action_key"])

    _claim_all(queue)

    claimed = queue.movers_claimed_on_tier(TIER)
    assert len(claimed) <= KNEE, (
        f"{len(claimed)} copies read the pool at once; its receipts say the "
        f"rate stops rising at {KNEE}")
    assert len(claimed) == KNEE
    cap = _tier_record(queue)["reader_plan"]["cap"]
    assert cap["movers"] == KNEE and cap["basis"] == "measured"
    assert cap["method"].startswith("smallest mover count")
    assert cap["receipts"] == sum(len(rates) for rates in CURVE.values())
    assert [level["movers"] for level in cap["curve"]] == sorted(CURVE)


# --------------------------------------------------------------------- pacer


def _window(tmp_path: Path, entries: int) -> tuple[Path, str, int]:
    """``entries`` one-MiB files with real bytes, as a data manifest."""

    mount = tmp_path / "sources"
    mount.mkdir()
    listed = []
    for index in range(entries):
        payload = hashlib.sha256(str(index).encode()).digest() * (MIB // 32)
        source = mount / f"shard-{index}.bin"
        source.write_bytes(payload)
        listed.append({"path": str(source), "offset": 0, "bytes": MIB,
                       "sha256": hashlib.sha256(payload).hexdigest()})
    total = entries * MIB
    manifest = {"schema": pb.DATA_MANIFEST_SCHEMA_V1, "produced_by": {},
                "annotations": {"phases": [{"name": "all", "bytes": total,
                                            "cumulative_bytes": total}]},
                "mount_prefix": str(mount), "entries": listed,
                "entry_count": entries, "total_bytes": total}
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    return path, hashlib.sha256(path.read_bytes()).hexdigest(), total


def _two_phase_reader(queue: pool.PoolQueue, stage: Path, digest: str,
                      total: int) -> tuple[str, str]:
    """A claimed reader over one manifest, a phase per half; its two movers.

    Each copy is sealed at ``SEALED_FILL_MB_S`` of pool fill, the rate the
    landing record prices a copy at before one of the plan's copies lands.
    """

    half = total // 2
    phases = []
    for ordinal, (start, end) in enumerate(((0, half), (half, total))):
        phases.append({
            "name": f"layer-{ordinal}", "start_bytes": start, "end_bytes": end,
            "stage_gib": 1,
            "mover_row": {
                **_row(queue, _hexkey(f"pacedmover{ordinal}"),
                       {STAGE_KIND: 1, FILL_KIND: SEALED_FILL_MB_S,
                        "cpu": 1, "mem_gb": 1}),
                "residency": {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                              "manifest_sha256": digest, "manifest_bytes": total,
                              "range_start_bytes": start, "range_end_bytes": end}},
            "egress_row": _row(queue, _hexkey(f"pacedegress{ordinal}"),
                               {"mem_gb": 1})})
    plan = residency_plan.build_plan(
        consumer_action_key=READER, tier_id=TIER, stage_root=str(stage),
        manifest_sha256=digest, manifest_bytes=total, phases=phases)
    _publish_consumer(queue, READER, plan, manifest=digest)
    now = time.time()
    _claim(queue, READER, phase="layer-0", claimed_unix=now - 100.0,
           reported_unix=now - 10.0)
    return (str(phases[0]["mover_row"]["action_key"]),
            str(phases[1]["mover_row"]["action_key"]))


def _move_args(queue: pool.PoolQueue, stage: Path, manifest: Path, digest: str,
               *, key: str, start: int, end: int, readers: int = 1):
    import stage_move

    return stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root), "--action-key", key,
        "--consumer-action-key", READER, "--tier-id", TIER,
        "--stage-root", str(stage), "--manifest-sha256", digest,
        "--manifest", str(manifest),
        "--residency-root", str(queue.residency_fragment_root()),
        "--range-start-bytes", str(start), "--range-end-bytes", str(end),
        "--readers", str(readers), "--max-readers", str(readers),
        "--warm-after-copy", "never", "--unpaced",
        "--progress-interval-s", "0.02"])


class _HurtingPacer(prewarm_loop.DiskPacer):
    """A pacer whose pool is always hurting: every wait holds 50 ms."""

    def wait(self, stop=None, abort=None):
        self._enter_hold()
        try:
            time.sleep(0.05)
        finally:
            self._leave_hold()


def _hurting(args) -> _HurtingPacer:
    return _HurtingPacer([], max_util_pct=100.0, max_read_await_ms=1e9,
                         max_backlog_ms=1e9, readers=args.readers,
                         max_readers=args.max_readers, hold_s=0.02)


def _clear_channel(monkeypatch) -> None:
    for name in pb_progress.ACTION_PROGRESS_ENV:
        monkeypatch.delenv(name, raising=False)


def test_the_pacer_never_holds_the_copy_a_consumer_waits_on(
        tmp_path: Path, monkeypatch) -> None:
    import stage_move

    _clear_channel(monkeypatch)
    monkeypatch.setattr(prewarm_loop, "pacer_from_args", _hurting)
    queue, stage = _fixture_queue(tmp_path, 8)
    manifest, digest, total = _window(tmp_path, entries=6)
    waited, other = _two_phase_reader(queue, stage, digest, total)
    filed = residency_plan.read_filed(queue, READER)[0]
    queue.publish(**dict(filed["phases"][0]["mover_row"]))
    wait_record = _declare_wait(queue, READER, [waited], since_unix=time.time() - 5.0)
    _cycle(queue, stage, gib=8)

    receipt = stage_move.move(_move_args(queue, stage, manifest, digest,
                                         key=waited, start=0, end=total // 2))

    assert receipt["complete"], receipt
    assert receipt["disk_pacing"]["held_seconds"] == 0, (
        "the pacer held the copy a claimed consumer is blocked on: "
        f"{receipt['disk_pacing']['held_seconds']} s")

    # The others are still paced, and they yield to the waited copy until it
    # lands: here, until the reader's next cycle no longer declares it.
    # The window published the other copy; it is claimed and running.
    published = queue.item_path(pool.READY, other)
    row = (json.loads(published.read_text()) if published.exists()
           else dict(filed["phases"][1]["mover_row"]))
    published.unlink(missing_ok=True)
    queue.item_path(pool.CLAIMED, other).write_text(json.dumps({
        **row, "claimed_unix": time.time(), "claimed_by": "fixture",
        "claimed_host": "dl380g10"}))
    finished: dict[str, object] = {}

    def run_other() -> None:
        finished["receipt"] = stage_move.move(_move_args(
            queue, stage, manifest, digest, key=other, start=total // 2,
            end=total))

    worker = threading.Thread(target=run_other, daemon=True)
    worker.start()
    time.sleep(0.6)
    assert worker.is_alive(), "the other copy did not yield to the waited one"
    wait_record.unlink()
    ready = queue.item_path(pool.READY, waited)
    done = queue.item_path(pool.DONE, waited)
    done.parent.mkdir(parents=True, exist_ok=True)
    done.write_text(json.dumps({**json.loads(ready.read_text()), "status": "done"}))
    ready.unlink()
    _cycle(queue, stage, gib=8)
    worker.join(30)
    assert not worker.is_alive()
    other_receipt = finished["receipt"]
    assert other_receipt["complete"], other_receipt   # type: ignore[index]
    pacing = other_receipt["disk_pacing"]              # type: ignore[index]
    assert pacing["held_seconds"] > 0
    assert pacing.get("yielded_seconds", 0) >= 0.4, pacing


# --------------------------------------------------------------- expectation

#: The incident's shape: the leg's first copies came off the ARC at about
#: 860 MB/s while the pool delivered 250 MB/s to everything reading it.
WARM_SECONDS = 2.5
POOL_DELIVERED_MB_S = 250.0
SEALED_FILL_MB_S = 30


def _fill_plan(queue: pool.PoolQueue, consumer: str, *, label: str,
               manifest: str, phases: int) -> dict[str, object]:
    """``_plan`` with each copy sealed at ``SEALED_FILL_MB_S`` of pool fill."""

    plan = _plan(queue, consumer, label=label, manifest=manifest, phases=phases)
    built = []
    for phase in plan["phases"]:                          # type: ignore[union-attr]
        row = dict(phase["mover_row"])
        row["resources"] = {**row["resources"], FILL_KIND: SEALED_FILL_MB_S}
        built.append({**phase, "mover_row": row})
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=manifest, manifest_bytes=int(plan["manifest_bytes"]),
        phases=built)


def test_a_cold_copy_is_not_priced_from_its_legs_warm_copies(
        tmp_path: Path) -> None:
    queue, stage = _fixture_queue(tmp_path, 40)
    plan = _fill_plan(queue, READER, label="warm", manifest=READER_MANIFEST,
                      phases=4)
    _publish_consumer(queue, READER, plan, manifest=READER_MANIFEST)
    size = PHASE_GIB * GIB
    for ordinal in (0, 1):
        mover = _mover("warm", ordinal)
        _land(queue, stage, consumer=READER, manifest=READER_MANIFEST,
              mover=mover, name=f"phase-{ordinal}", start=ordinal * size,
              end=(ordinal + 1) * size, seconds=WARM_SECONDS)
        receipt = queue.move_record(mover)
        queue.record_move(mover, {
            **receipt, "mb_per_s_file_side": round(size / 1e6 / WARM_SECONDS, 1),
            "disk_pacing": {"mean_pool_read_mb_s": POOL_DELIVERED_MB_S,
                            "pool_read_bytes": int(POOL_DELIVERED_MB_S * 1e6
                                                   * WARM_SECONDS),
                            "held_seconds": 0.0}})
    now = time.time()
    _claim(queue, READER, phase="phase-1", claimed_unix=now - 100.0,
           reported_unix=now - 10.0)
    cold = dict(plan["phases"][2]["mover_row"])          # type: ignore[index]
    claimed_unix = now - 1.0
    path = queue.item_path(pool.CLAIMED, str(cold["action_key"]))
    path.write_text(json.dumps({**cold, "claimed_unix": claimed_unix,
                                "claimed_by": "fixture",
                                "claimed_host": "dl380g10"}))

    rate, _basis = tier_loop._plan_landing(queue, plan)
    tier_loop.publish_landing_expectations(
        queue, tiers={TIER: _tier_record_for(stage)},
        consumers=tier_loop.live_consumers(queue), now=now)

    delivered = POOL_DELIVERED_MB_S * 1e6
    assert rate is not None and rate <= delivered, (
        f"the leg's landing rate {rate / 1e6:.0f} MB/s beats the pool's "
        f"whole delivery of {POOL_DELIVERED_MB_S:.0f} MB/s: it was priced "
        f"from copies the ARC served")
    record = residency_map.read_landing(residency_map.landing_path(
        queue.residency_fragment_root(), READER))
    row = next(row for row in record["ranges"]            # type: ignore[union-attr]
               if row["mover_action_key"] == cold["action_key"])
    assert row["state"] == pool.CLAIMED
    assert row["expected_landing_unix"] - claimed_unix >= size / delivered, row


def _tier_record_for(stage: Path) -> dict[str, object]:
    return {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier_id": TIER,
            "host": "dl380g10", "tier": "stage", "mountpoint": str(stage),
            "capacity_bytes": 40 * GIB}


def test_a_claimed_copy_is_priced_from_its_own_live_rate(
        tmp_path: Path, monkeypatch) -> None:
    """A mover with no progress channel still reports what it has copied."""

    import stage_move

    _clear_channel(monkeypatch)
    queue, stage = _fixture_queue(tmp_path, 8)
    manifest, digest, total = _window(tmp_path, entries=4)
    waited, _other = _two_phase_reader(queue, stage, digest, total)
    row = dict(residency_plan.read_filed(queue, READER)[0]["phases"][0]["mover_row"])
    claimed_unix = time.time() - 0.5
    queue.item_path(pool.CLAIMED, waited).write_text(json.dumps({
        **row, "claimed_unix": claimed_unix, "claimed_by": "fixture",
        "claimed_host": "dl380g10"}))
    gate = threading.Event()
    reads = [0]
    lock = threading.Lock()
    real = os.readv

    def gated(fd, buffers):
        with lock:
            reads[0] += 1
            first = reads[0] == 1
        got = real(fd, buffers)
        if not first:
            gate.wait(30)
        return got

    monkeypatch.setattr(os, "readv", gated)
    finished: dict[str, object] = {}

    def run() -> None:
        finished["receipt"] = stage_move.move(_move_args(
            queue, stage, manifest, digest, key=waited, start=0,
            end=total // 2))

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and reads[0] < 2:
            time.sleep(0.01)
        assert reads[0] >= 2, "the copy never reached its second read"
        time.sleep(0.2)
        tier_loop.publish_landing_expectations(
            queue, tiers={TIER: _tier_record_for(stage)},
            consumers=tier_loop.live_consumers(queue))
        record = residency_map.read_landing(residency_map.landing_path(
            queue.residency_fragment_root(), READER))
        found = next(entry for entry in record["ranges"]  # type: ignore[union-attr]
                     if entry["mover_action_key"] == waited)
    finally:
        gate.set()
        worker.join(30)
    assert found["state"] == pool.CLAIMED
    assert found["basis"] == "reported", (
        f"the claimed copy was priced from {found['basis']!r}, not from its "
        f"own progress: {found}")
    assert found["copied_bytes"] >= MIB
    assert found["live_bytes_per_s"] > 0
    assert finished["receipt"]["complete"]               # type: ignore[index]

"""A terminal-failed producer cannot repair its missing READY mover input (#1184).

All queue/CAS/origin paths are private tmp_path fixtures. No stage mover is
executed and no real mountpoint is read or removed.
"""
import json
from pathlib import Path
import subprocess
import sys
import time

from prismabuild import adaptive_cpu, pool, produced_output as po
from prewarm_fixture import Fleet
from test_a_dead_producers_held_mover_is_retired import (
    _World, _about, _isolated, _tier_census, IDLE, KIND, REPO, RETIRED_EVENT,
    TIER)
from test_prepaid_writer_integration import _descriptors, _prewrite, _tier_host


def _warm(world, tmp_path):
    control = Fleet(tmp_path / "prewarm-control")
    control.queue = world.q
    control.cas_root = world.cas_root
    control.mount = Path(world.template["output_prefix"])
    return control.cycle(control.args(readers=1))


def test_terminal_failed_producer_missing_input_ends_ready_mover(tmp_path):
    world = _World(tmp_path)
    origin = Path(world.descs[0]["path"])
    origin.unlink()
    before = json.loads(world.q.item_path(pool.READY, world.mover).read_text())
    assert before["produced_output_batch"]["owner_nonce"] == world.inst["owner_attempt"]["nonce"]
    world.fail_the_producer()
    assert po._producer_attempt_state(world.q, world.inst) == "dead"

    _warm(world, tmp_path)

    assert not world.q.item_path(pool.READY, world.mover).exists(), (
        "terminal-failed owner plus missing sealed input left the mover READY")
    ending = json.loads(world.q.item_path(pool.FAILED, world.mover).read_text())
    assert ending["published_unix"] == before["published_unix"]
    assert ending["status"] == "failed"
    assert ending["detail"]["termination_reason"] == "input_dependency_failed"
    assert ending["detail"]["input_dependency"]["owner_nonce"] == before["produced_output_batch"]["owner_nonce"]
    assert ending["detail"]["input_dependency"]["path"] == str(origin)
    assert ending["detail"].get("action_returncode") is None  # never executed


def test_live_producer_missing_input_remains_ready_and_can_land(tmp_path):
    world = _World(tmp_path)
    origin = Path(world.descs[0]["path"])
    payload = origin.read_bytes()
    origin.unlink()
    assert po._producer_attempt_state(world.q, world.inst) == "live"
    _warm(world, tmp_path)
    assert world.q.item_path(pool.READY, world.mover).exists()
    assert not world.q.item_path(pool.FAILED, world.mover).exists()
    origin.write_bytes(payload)
    _warm(world, tmp_path)
    assert world.q.item_path(pool.READY, world.mover).exists()


def test_terminal_failed_producer_with_present_input_is_not_cancelled(tmp_path):
    world = _World(tmp_path)
    world.fail_the_producer()
    _warm(world, tmp_path)
    assert world.q.item_path(pool.READY, world.mover).exists()
    assert not world.q.item_path(pool.FAILED, world.mover).exists()


def test_unknown_producer_evidence_does_not_prove_death(tmp_path):
    """A republished (queued) producer reads unknown, and unknown retains.

    The producer's key ends READY again -- a retry is queued behind the
    failure -- so its attempt state is unknown, not dead (#1202 review:
    monkeypatching the unit under test was not faithful).
    """
    world = _World(tmp_path)
    Path(world.descs[0]["path"]).unlink()
    world.fail_the_producer()
    # Republish the owner's key as READY: a queued retry of the same work.
    world.q.publish(action_key=world.owner, cas_root=str(world.cas_root),
                    worker_script=str(REPO / "tools" / "prismabuild_worker.py"),
                    checkout_root=str(tmp_path / "mover-checkout"),
                    resources={"cpu": 1, "mem_gb": 1,
                               **po.owner_demand_terms(world.template)},
                    produced_output_template=world.template,
                    max_attempts=1, retry_safe=False)
    state = po._producer_attempt_state(world.q, world.inst)
    assert state == "unknown", state
    _warm(world, tmp_path)
    assert world.q.item_path(pool.READY, world.mover).exists()
    assert not world.q.item_path(pool.FAILED, world.mover).exists()


def test_the_ended_mover_releases_its_funding_and_the_batch_retires(tmp_path):
    """#1202 review finding 1: no stage token or funding may outlive the ending.

    A never-claimed mover's funding stays ``transferring`` and keeps its
    prepaid token unless the ending releases it, and then the dead-producer
    sweep reports the holder unresolved and never retires the batch.
    """
    world = _World(tmp_path)
    Path(world.descs[0]["path"]).unlink()
    world.fail_the_producer()
    _warm(world, tmp_path)
    assert world.q.item_path(pool.FAILED, world.mover).exists()

    funding = world.q.read_output_funding(world.mover, TIER)
    assert funding is not None and funding["state"] == "released", funding
    receipts = world.sweep(IDLE)
    retired = _about(receipts, world.mover)
    assert [entry.get("event") for entry in retired] == [RETIRED_EVENT], receipts
    assert _tier_census(world.ledger) == {
        "capacity": 4, "free": 4, "holders": {}}
    assert world.batch_entry().get("retired") is True
    assert world.q.retire_terminal_output_funding()["retired"] == 1


def test_an_unreadable_origin_is_unknown_not_missing(tmp_path):
    """#1202 review finding 2: EACCES/ESTALE must not file a permanent ending.

    ``os.path.exists`` answers False for an unreadable directory, so a
    root-squash denial or a stale handle read as a missing file while the
    file exists. Only ENOENT is missing; any other OSError is unknown.
    """
    world = _World(tmp_path)
    origin = Path(world.descs[0]["path"])
    world.fail_the_producer()
    parent = origin.parent
    try:
        parent.chmod(0o000)
        _warm(world, tmp_path)
        assert world.q.item_path(pool.READY, world.mover).exists(), (
            "an unreadable origin filed a permanent ending")
        assert not world.q.item_path(pool.FAILED, world.mover).exists()
    finally:
        parent.chmod(0o755)
    # Readable again, the file is present: still READY.
    assert origin.exists()
    _warm(world, tmp_path)
    assert world.q.item_path(pool.READY, world.mover).exists()
    # Genuinely missing now: the ending files.
    origin.unlink()
    _warm(world, tmp_path)
    assert world.q.item_path(pool.FAILED, world.mover).exists()


def test_a_busy_transition_lock_does_not_block_the_claim_pass(tmp_path):
    """#1202 review finding 3: the claim pass must never wait on a foreign lock.

    ``_transition_locked`` blocks by default (#1115 class): another box
    holding the mover key's lock for seconds stalled the whole claim pass.
    The refusal attempt must be non-blocking, recording a ``transition_busy``
    denial instead.
    """
    world = _World(tmp_path)
    Path(world.descs[0]["path"]).unlink()
    world.fail_the_producer()
    lock = (world.q.root / "transition-locks" / (
        __import__("hashlib").sha256(world.mover.encode()).hexdigest() + ".lock"))
    lock.parent.mkdir(parents=True, exist_ok=True)
    # A FOREIGN holder: the lock is per-process (fcntl record locks), so the
    # holder must be another process, exactly as another box would be.
    holder = subprocess.Popen(
        [sys.executable, "-c",
         "import fcntl, sys, time\n"
         "handle = open(sys.argv[1], 'a+')\n"
         "fcntl.lockf(handle, fcntl.LOCK_EX)\n"
         "print('held', flush=True)\n"
         "time.sleep(2.0)", str(lock)],
        stdout=subprocess.PIPE, text=True)
    assert holder.stdout.readline().strip() == "held"
    try:
        started = time.monotonic()
        claimed = world.q.claim(owner="w-mover", tags=[_tier_host(world.q)])
        elapsed = time.monotonic() - started
    finally:
        holder.wait(timeout=10)
    assert elapsed < 1.0, f"the claim pass blocked {elapsed:.2f}s on a foreign lock"
    assert claimed is None
    assert world.q.item_path(pool.READY, world.mover).exists()
    path = (adaptive_cpu.local_state_base(world.q.ledger().base)
            / pool.CLAIM_DENIALS)
    records = adaptive_cpu.read_json(path).get("records", {})
    denial = next(v for v in records.values()
                  if v["action_key"] == world.mover)
    assert denial["reason"] == "transition_busy", denial


def test_a_crashed_dead_input_transition_is_recovered_by_the_sweep(
        tmp_path, monkeypatch):
    """#1202 review finding 4: a crash mid-transition must not lose the row.

    The ending files FAILED last; a crash (or a failed atomic write) before
    it leaves the captured bytes in ``ready-transitions/`` under the
    ``dead-input`` kind, and ``sweep_ready_transitions`` must recover it
    exactly as it recovers the origin-released kind.
    """
    world = _World(tmp_path)
    Path(world.descs[0]["path"]).unlink()
    world.fail_the_producer()
    item = json.loads(world.q.item_path(pool.READY, world.mover).read_text())

    def crash(*args, **kwargs):
        raise OSError("simulated crash before the ending was filed")

    monkeypatch.setattr(pool, "_write_json_atomic", crash)
    try:
        world.q.fail_dead_input_dependency(item, world.mover)
    except OSError:
        pass
    monkeypatch.undo()
    captured = list((world.q.root / "ready-transitions").glob("*.dead-input.json"))
    assert captured, "the crashed transition left no captured bytes"
    assert not world.q.item_path(pool.READY, world.mover).exists()

    assert world.q.sweep_ready_transitions(grace_s=0.0) == [world.mover]
    assert world.q.item_path(pool.READY, world.mover).exists(), (
        "the dead-input kind was not recovered by the sweep")
    assert not world.q.item_path(pool.FAILED, world.mover).exists()


def test_one_claim_pass_reads_each_owner_state_once(tmp_path, monkeypatch):
    """#1202 review finding 5: no per-mover claimed/ listings in the pass.

    R13 published 436 movers of a handful of owners; paying two ``claimed/``
    listings per mover per pass is the #993 class. The pass memoizes each
    owner's generation once and only the dead hint pays for the full proof.
    """
    world = _World(tmp_path)          # the owner stays LIVE
    descs2 = _descriptors(tmp_path, world.template, world.inst, "p2",
                          b"c" * 1024)
    _prewrite(world.q, world.inst, world.template, "b2", TIER, descs2)
    res = po.publish_prepaid_batch(
        world.q, world.inst, world.template, descs2, batch_id="b2",
        tier=TIER, cas_root=world.cas_root,
        producer_action_key=world.owner, command_extra=["--unpaced"])
    assert res.get("ok") is True, res
    mover2 = str(res["mover_key"])

    calls = []
    real = po._key_generation

    def counting(queue, key):
        calls.append(key)
        return real(queue, key)

    monkeypatch.setattr(po, "_key_generation", counting)
    # One prewarm pass scans every READY row: both movers' checks share one
    # owner-state read.
    _warm(world, tmp_path)
    owner_reads = [key for key in calls if key == world.owner]
    assert len(owner_reads) == 1, calls
    for mover in (world.mover, mover2):
        assert world.q.item_path(pool.READY, mover).exists()
        assert not world.q.item_path(pool.FAILED, mover).exists()


def test_claim_fails_the_mover_before_any_admission(tmp_path):
    """The claim path refuses the same row a warm would (#1184).

    A claim that reaches the mover without a prewarm cycle must not admit
    it either: the serialized refusal runs before the key's transition
    hold, so a mover whose dead producer's bound origin is gone is failed
    without executing anything.
    """
    world = _World(tmp_path)
    Path(world.descs[0]["path"]).unlink()
    world.fail_the_producer()
    claimed = world.q.claim(owner="w-mover", tags=[_tier_host(world.q)])
    assert claimed is None, "a dead producer's missing-input mover was admitted"
    assert not world.q.item_path(pool.READY, world.mover).exists()
    ending = json.loads(world.q.item_path(pool.FAILED, world.mover).read_text())
    assert ending["status"] == "failed"
    assert ending["detail"]["termination_reason"] == "input_dependency_failed"
    assert ending["detail"].get("action_returncode") is None  # never executed

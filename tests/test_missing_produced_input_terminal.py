"""A terminal-failed producer cannot repair its missing READY mover input (#1184).

All queue/CAS/origin paths are private tmp_path fixtures. No stage mover is
executed and no real mountpoint is read or removed.
"""
import json
import os
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

    An in-process write failure is guarded (#1202 review round 2): the row
    is restored at once and the pass records why.  A real crash between the
    capture and the filing leaves the captured bytes in
    ``ready-transitions/`` under the ``dead-input`` kind, and
    ``sweep_ready_transitions`` must recover it exactly as it recovers the
    origin-released kind.
    """
    world = _World(tmp_path)
    Path(world.descs[0]["path"]).unlink()
    world.fail_the_producer()
    item = json.loads(world.q.item_path(pool.READY, world.mover).read_text())

    def crash(*args, **kwargs):
        raise OSError("simulated write fault before the ending was filed")

    monkeypatch.setattr(pool, "_write_json_atomic", crash)
    # The guard restores the row instead of raising out of the pass.
    assert world.q.fail_dead_input_dependency(item, world.mover) is False
    monkeypatch.undo()
    assert world.q.item_path(pool.READY, world.mover).exists(), (
        "a failed ending write did not restore the row")
    assert not world.q.item_path(pool.FAILED, world.mover).exists()

    # A real crash -- the process dies between the capture and the filing --
    # leaves the captured bytes behind; the sweep recovers them.
    captured_dir = world.q.root / "ready-transitions"
    captured_dir.mkdir(parents=True, exist_ok=True)
    captured = (captured_dir / f"{world.mover}.1790000000000000."
                f"{'0' * 32}.dead-input.json")
    os.link(world.q.item_path(pool.READY, world.mover), captured)
    world.q.item_path(pool.READY, world.mover).unlink()
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


def test_a_malformed_template_does_not_end_the_claim_pass(tmp_path):
    """#1202 review N1: a null or non-object template body must not raise.

    ``dict()`` on a non-mapping raises TypeError out of the claim pass, and
    every later pass re-raises on the same READY row.  validate_template
    refuses a non-mapping itself; the proof answers None and the row stays
    READY.
    """
    world = _World(tmp_path)
    Path(world.descs[0]["path"]).unlink()
    world.fail_the_producer()
    template_file = (world.q.root / "residency" / "produced-output-templates"
                     / f"{world.inst['template_id']}.json")
    template_file.write_text("null")
    # Neither the prewarm cycle nor the claim pass raises; the row stays.
    _warm(world, tmp_path)
    assert world.q.item_path(pool.READY, world.mover).exists()
    assert not world.q.item_path(pool.FAILED, world.mover).exists()
    claimed = world.q.claim(owner="w-mover", tags=[_tier_host(world.q)])
    assert claimed is None or claimed["action_key"] != world.mover


def test_a_live_producer_pays_no_transition_lock(tmp_path, monkeypatch):
    """#1202 review N2: the hint is consulted before the mover's lock.

    A mover of a LIVE producer is the common case; its dead-input check must
    take no transition lock at all, or every READY produced mover pays a
    second NFS lock cycle per pass on top of the claim's own.
    """
    import hashlib
    world = _World(tmp_path)          # the producer stays LIVE
    lock_of = lambda key: (world.q.root / "transition-locks" / (
        hashlib.sha256(key.encode()).hexdigest() + ".lock"))
    taken = []
    original = world.q._transition_locked

    def counting(key, **kwargs):
        if key == world.mover and lock_of(key).exists():
            taken.append(key)
        return original(key, **kwargs)

    monkeypatch.setattr(world.q, "_transition_locked", counting)
    _warm(world, tmp_path)
    assert world.q.item_path(pool.READY, world.mover).exists()
    assert taken == [], f"the live-producer check took the mover's lock {len(taken)}x"


def test_the_dead_input_hold_is_timed_into_the_pass(tmp_path, monkeypatch):
    """#1202 review N2 / finding 3: the proof's hold is timed (#1029).

    With a dead hint the check takes the key's lock through the pass's timed
    hold, so a slow proof under it is visible in ``last_claim_pass`` instead
    of hiding inside a raw lock.
    """
    import time as _time
    world = _World(tmp_path)
    Path(world.descs[0]["path"]).unlink()
    world.fail_the_producer()
    real = pool.PoolQueue.claim

    def slowed(self, **kwargs):
        original_proof = po.dead_input_dependency

        def slow_proof(queue, item):
            _time.sleep(0.5)
            return original_proof(queue, item)

        import prismabuild.produced_output as po_mod
        po_mod.dead_input_dependency = slow_proof
        try:
            return real(self, **kwargs)
        finally:
            po_mod.dead_input_dependency = original_proof

    monkeypatch.setattr(pool.PoolQueue, "claim", slowed)
    claimed = world.q.claim(owner="w-mover", tags=[_tier_host(world.q)])
    assert claimed is None
    monkeypatch.undo()
    summary = world.q.last_claim_pass or {}
    assert summary.get("transition_held_s", 0.0) >= 0.4, summary
    assert summary.get("transition_holds", 0) >= 1, summary


def test_a_transient_funding_read_fault_retries_instead_of_stranding(
        tmp_path, monkeypatch):
    """#1202 review N3: one ESTALE inside the ending must not strand the token.

    A read fault while releasing the funding restores the row and records a
    denial instead of filing FAILED: a FAILED row makes the public release
    refuse forever, so the token would strand.  The next pass, with the
    fault gone, files the ending and releases.
    """
    world = _World(tmp_path)
    Path(world.descs[0]["path"]).unlink()
    world.fail_the_producer()
    faults = {"left": 1}

    real_state = world.q.output_funding_file_state
    real_read = world.q.read_output_funding

    def faulty_state(*args, **kwargs):
        if faults["left"]:
            faults["left"] -= 1
            raise OSError("simulated ESTALE on the funding record")
        return real_state(*args, **kwargs)

    def faulty_read(*args, **kwargs):
        if faults["left"]:
            faults["left"] -= 1
            return None
        return real_read(*args, **kwargs)

    monkeypatch.setattr(world.q, "output_funding_file_state", faulty_state)
    monkeypatch.setattr(world.q, "read_output_funding", faulty_read)
    _warm(world, tmp_path)
    assert world.q.item_path(pool.READY, world.mover).exists(), (
        "a transient funding read fault filed a permanent ending")
    assert not world.q.item_path(pool.FAILED, world.mover).exists()

    monkeypatch.undo()
    _warm(world, tmp_path)
    assert world.q.item_path(pool.FAILED, world.mover).exists()
    funding = world.q.read_output_funding(world.mover, TIER)
    assert funding is not None and funding["state"] == "released", funding

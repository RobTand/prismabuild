"""R6: shared tier mutation exclusion + authoritative census (issue #733).

Drives the REAL tier ledger at tiny scale through the REAL mint path
(``mint_tier_capacity`` -> ``_apply_tier_capacity`` -> reclaim/ensure/
retire) and the REAL claim boundary (published READY row with a
residency block -> ``PoolQueue.claim`` -> tier begin/commit).  The ONE
modelled number is the stage backing count, labelled everywhere: 1
token == 1 GiB of writable stage room.

Backing arithmetic for the interleaving test (wanted == backing == 2):

* live:  H holds ``stage_gib-0000`` (1 GiB staged), free holds
  ``stage_gib-0002`` (1 GiB writable).  Total 2 == wanted, headroom 0.
* dead: ``stage_gib-0001`` (a destroyed duplicate whose bytes still sit
  under a co-owner: reissue is honest only on growth, and there is
  none here).
* claimant C: a published mover row (1 MiB range, floor 1 token)
  demanding 2 tokens -- the whole honest supply.

The injected race is held -> free BETWEEN the reclaim's free listing
and its holder listing, performed by a helper thread calling the REAL
``release`` with no probe and no voluntary serialization: on pre-R6
production that call takes no guard and lands mid-scan; on fixed code
it blocks on the new guard outside the whole apply.  The test hook
only observes (completed vs still-blocked after a bounded wait) and
never steers the helper.  H's token is then missed by both listings,
headroom reads 2 - 1 = 1, and the dead name is reissued with no
backing -- 3 free against backing 2 at that prefix.  The real
claimant, in its own thread through ``PoolQueue.claim``, takes 2
(including the phantom) before the same apply's retire can trim it;
the retire (free-only) removes the honest remainder instead, so the
books converge at total 2 while C holds the phantom -- persistent
unbacked admission the trim never repairs.

With the guard (``_guarded_mutation`` via ``PoolQueue.tier_ledger``)
the release serializes outside the apply, headroom reads exact, the
real claim declines, the dead name stays dead, and the books close
exact with no unbacked prefix.

RED provenance: the conformance test below FAILS on the actual base
commit ``3420951679`` (new tests only, no source change -- see the R6
report for the action key): prefix 3, claim wins, claimant holds the
reissued dead name, total converges at 2.  No test proves the race by
disabling the mechanism under test.  GREEN is the same test passing
on the guarded tree.

The direct ``begin_acquire`` contention case is ledger-component
evidence and is labelled as such; the claim-boundary case above is
the end-to-end one.

Runs under pbtest at priority -10; never executed locally.
"""
from __future__ import annotations

import os
import threading
import time
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402

TIER = "prismabuild-stage:r6excl"
KIND = "stage_gib"
HOLDER_H = "h" * 64
HOLDER_X = "x" * 64
CLAIM_KEY = "d" * 64
MIB = 1 << 20
WANTED = {KIND: 2}
HOST_CAP = {"cpu": 8, "mem_gb": 16}
HOOK_WAIT_S = 5.0


def _shaped_queue(tmp_path: Path) -> tuple[pool.PoolQueue, str]:
    """Build the wanted==backing==2 shape described above.

    Three markers from an earlier 3-wide mint; current wanted 2: H
    holds one live token, one token is free, and one destroyed name
    waits in the dead set with no honest headroom to reissue it.
    Returns the queue and the dead name.
    """
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    ledger = queue.tier_ledger(TIER)
    queue.mint_tier_capacity(TIER, {KIND: 3})
    assert ledger.acquire(HOLDER_H, {KIND: 1}) is True
    assert ledger.acquire(HOLDER_X, {KIND: 1}) is True
    assert ledger.retire_held(HOLDER_X, {KIND: 1}) == {KIND: 1}
    assert ledger.capacity().get(KIND) == 2
    assert ledger.available().get(KIND) == 1
    dead = sorted(path.name for path in (ledger.minted_dir / "dead").iterdir())
    assert len(dead) == 1
    return queue, dead[0]


def _publish_claimant(queue: pool.PoolQueue) -> None:
    """A real READY mover demanding more than the two backed credits."""
    queue.publish(
        action_key=CLAIM_KEY, cas_root=str(queue.root / "cas"),
        checkout_root=str(queue.root / "co"),
        worker_script=str(queue.root / "worker.py"),
        resources={"cpu": 1, "mem_gb": 1, f"{KIND}@{TIER}": 3},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": "f" * 64, "manifest_bytes": MIB,
                   "range_start_bytes": 0, "range_end_bytes": MIB},
        max_attempts=1, retry_safe=False)


def _run_scan_gap_interleaving(queue: pool.PoolQueue, monkeypatch) -> dict:
    """Run one mint with a real held->free release armed between the
    reclaim's two listings, a prefix free snapshot plus a real claim
    right after the reclaim, and report the observed outcome."""
    free_dir = queue.root / pool.TIER_RESERVATIONS / TIER / "free"
    fired = {"glob": False}
    helper_done = threading.Event()
    helper_attempted = threading.Event()
    observed: dict = {}

    def _helper() -> None:
        # The REAL release call, blind: no probe, no voluntary honoring
        # of any lock.  Pre-R6 it takes no guard and lands mid-scan;
        # fixed code blocks it on the new guard outside the apply.
        helper_attempted.set()
        queue.tier_ledger(TIER).release(HOLDER_H)
        helper_done.set()

    real_glob = pool._glob

    def _observe_free_scan(path, pattern, result):
        if (not fired["glob"] and str(path) == str(free_dir)
                and str(pattern) == "*-*"):
            fired["glob"] = True
            thread = threading.Thread(target=_helper, daemon=True)
            thread.start()
            # Bounded observation only: fast-complete means the release
            # landed inside the scan gap; elapsed means it is blocked on
            # the new guard.  Either way the apply proceeds unsteered.
            deadline = time.monotonic() + HOOK_WAIT_S
            while not helper_done.is_set() and time.monotonic() < deadline:
                time.sleep(0.005)
            observed["helper_blocked"] = not helper_done.is_set()
        return result

    def _hooked_glob(path, pattern):
        return _observe_free_scan(path, pattern, real_glob(path, pattern))

    monkeypatch.setattr(pool, "_glob", _hooked_glob)
    # The fixed minter uses the strict census. Interrupt the same actual
    # headroom scan on both revisions, before either scans held tokens.
    if hasattr(pool, "_glob_visible"):
        real_visible = pool._glob_visible

        def _hooked_visible(path, pattern):
            return _observe_free_scan(path, pattern, real_visible(path, pattern))

        monkeypatch.setattr(pool, "_glob_visible", _hooked_visible)
    real_reclaim = pool.PoolQueue._reclaim_dead_markers

    def _hooked_reclaim(self, ledger, wanted):
        outcome = real_reclaim(self, ledger, wanted)
        if str(ledger.base) == str(queue.root / pool.TIER_RESERVATIONS / TIER):
            probe_ledger = self.tier_ledger(TIER)
            observed["prefix_free"] = int(
                probe_ledger.available().get(KIND, 0))
            # The REAL claim boundary, in its own thread: same-thread
            # execution would re-acquire the held guard and mask
            # contention, so the claimant runs as production does.
            claim_out: dict = {}

            def _claimant() -> None:
                claim_out["row"] = self.claim(
                    owner="worker:1:abcd0001", capacity=dict(HOST_CAP),
                    tags=["dl380g10"])

            thread = threading.Thread(target=_claimant, daemon=True)
            thread.start()
            thread.join(timeout=60.0)
            assert not thread.is_alive(), "claimant hung"
            observed["claim_row"] = claim_out.get("row")
        return outcome

    monkeypatch.setattr(
        pool.PoolQueue, "_reclaim_dead_markers", _hooked_reclaim)
    _publish_claimant(queue)
    result = queue.mint_tier_capacity(TIER, dict(WANTED))
    observed["result"] = result
    assert fired["glob"], "free-scan hook never fired"
    assert helper_attempted.is_set(), "helper never attempted its release"
    assert helper_done.wait(timeout=60.0), "helper release never finished"
    probe = queue.tier_ledger(TIER)
    observed["final_total"] = int(probe.capacity().get(KIND, 0))
    observed["final_free"] = int(probe.available().get(KIND, 0))
    holder_dir = probe.held_dir / CLAIM_KEY
    observed["claimant_names"] = (
        sorted(path.name for path in holder_dir.glob("*-*"))
        if holder_dir.is_dir() else [])
    return observed


def _brief(observed: dict) -> tuple:
    """Compact failure evidence that pytest never truncates."""
    result = observed.get("result", {})
    return (observed.get("helper_blocked"), observed.get("prefix_free"),
            observed.get("claim_row", {}).get("action_key")
            if isinstance(observed.get("claim_row"), dict) else None,
            observed.get("claimant_names"), observed.get("final_total"),
            observed.get("final_free"),
            (result.get("reclaimed"), result.get("retired"))
            if isinstance(result, dict) else None)


def test_r6_reclaim_headroom_atomic_with_exclusion(tmp_path: Path,
                                                   monkeypatch) -> None:
    """GREEN: the real race under the guard closes exact through the
    real claim boundary.  RED (same test on base ``3420951679``): prefix
    3, claim wins holding all three credits against backing of two, and
    free-only trimming cannot repair the over-admission."""
    queue, dead_name = _shaped_queue(tmp_path)
    observed = _run_scan_gap_interleaving(queue, monkeypatch)
    # The release serialized outside the apply (helper blocked on the
    # new guard -- recorded, not steered): the scan saw both live
    # tokens and reissued nothing.
    assert observed["prefix_free"] == 1, _brief(observed)
    # The real claim declined: nothing held, dead name stays dead.
    assert observed["claim_row"] is None, _brief(observed)
    assert observed["claimant_names"] == [], _brief(observed)
    assert observed["result"]["reclaimed"] == {}, _brief(observed)
    assert (queue.tier_ledger(TIER).minted_dir / "dead" / dead_name).exists()
    # Books exact after the serialized release lands.
    assert observed["final_total"] == 2, _brief(observed)
    assert observed["final_free"] == 2, _brief(observed)


def test_r6_tier_begin_declines_on_contended_guard(tmp_path: Path) -> None:
    """Ledger-component evidence: admission never waits -- a contended
    guard declines with the existing unavailable vocabulary (no new
    tier_busy)."""
    queue, _ = _shaped_queue(tmp_path)
    outcome: dict = {}

    def _contender() -> None:
        handles: dict = {}
        outcome["shortage"] = queue._begin_tier_acquire(
            CLAIM_KEY, {TIER: {KIND: 1}}, handles, {})
        outcome["handles"] = handles

    with queue.tier_mint_lock(TIER):
        thread = threading.Thread(target=_contender, daemon=True)
        thread.start()
        thread.join(timeout=60.0)
        assert not thread.is_alive(), "contender hung on admission"
    assert outcome["shortage"] is not None
    assert outcome["shortage"]["reason"] == "tier_reservation_unavailable", outcome
    assert outcome["handles"] == {}, outcome


def test_r6_commit_and_abandon_complete_under_contention(
        tmp_path: Path) -> None:
    """Ledger-component evidence: past admission, contention waits and
    completes -- the exact count is earned under the lock, never
    reported without it."""
    queue, _ = _shaped_queue(tmp_path)
    ledger = queue.tier_ledger(TIER)
    handle = ledger.begin_acquire(HOLDER_X, {KIND: 1})
    assert handle is not None
    moved: dict = {}
    probed = threading.Event()

    def _committer() -> None:
        with queue.tier_mint_lock(TIER, blocking=False) as ok:
            moved["probe"] = bool(ok)
        probed.set()
        moved["count"] = queue.tier_ledger(TIER).commit_acquire(
            HOLDER_X, handle)

    thread = threading.Thread(target=_committer, daemon=True)
    with queue.tier_mint_lock(TIER):
        thread.start()
        # The probe ran while this thread held the lock: contention is
        # observed, not timed.  Releasing below lets the blocking
        # commit earn its exact count under the lock.
        assert probed.wait(timeout=60.0), "committer never probed"
    thread.join(timeout=60.0)
    assert not thread.is_alive(), "committer hung"
    assert moved.get("probe") is False, moved
    assert moved.get("count") == 1, moved
    assert ledger.holder_tokens(HOLDER_X).get(KIND) == 1

    handle_b = ledger.begin_acquire(HOLDER_X, {KIND: 1})
    # HOLDER_X already holds 1 and free holds 0: the second begin must
    # decline rather than invent capacity.
    assert handle_b is None
    assert ledger.release(HOLDER_X) == 1
    handle_c = ledger.begin_acquire(HOLDER_X, {KIND: 1})
    assert handle_c is not None
    returned: dict = {}
    abandon_probed = threading.Event()

    def _abandoner() -> None:
        with queue.tier_mint_lock(TIER, blocking=False) as ok:
            returned["probe"] = bool(ok)
        abandon_probed.set()
        returned["count"] = queue.tier_ledger(TIER).abandon_acquire(handle_c)

    thread = threading.Thread(target=_abandoner, daemon=True)
    with queue.tier_mint_lock(TIER):
        thread.start()
        assert abandon_probed.wait(timeout=60.0), "abandoner never probed"
    thread.join(timeout=60.0)
    assert not thread.is_alive(), "abandoner hung"
    assert returned.get("probe") is False, returned
    assert returned.get("count") == 1, returned
    assert ledger.available().get(KIND) == 1


def test_r6_unreadable_holder_refuses_reissue_and_recovers(
        tmp_path: Path) -> None:
    """A live holder hidden by permissions reads as unknown, never as
    empty: no reissue, every valid token preserved, honest regrowth on
    retry after the permissions heal."""
    queue, dead_name = _shaped_queue(tmp_path)
    ledger = queue.tier_ledger(TIER)
    holder_dir = ledger.held_dir / HOLDER_H
    if os.geteuid() == 0:
        pytest.skip("chmod unreadability needs a non-root worker")
    os.chmod(holder_dir, 0o000)
    try:
        result = queue.mint_tier_capacity(TIER, dict(WANTED))
    finally:
        os.chmod(holder_dir, 0o755)
    assert result["reclaimed"] == {}, result
    assert (ledger.minted_dir / "dead" / dead_name).exists()
    assert ledger.capacity().get(KIND) == 2, "valid tokens must be preserved"
    assert ledger.holder_tokens(HOLDER_H).get(KIND) == 1
    # Retry with honest growth reissues exactly the dead name.
    result = queue.mint_tier_capacity(TIER, {KIND: 3})
    assert result["reclaimed"] == {KIND: 1}, result
    assert not (ledger.minted_dir / "dead" / dead_name).exists()
    assert ledger.capacity().get(KIND) == 3
    assert ledger.available().get(KIND) == 2


def test_r6_unreadable_free_refuses_mint(tmp_path: Path) -> None:
    """An unreadable free directory aborts the mint with everything
    retained; the next cycle after healing is exact."""
    queue, dead_name = _shaped_queue(tmp_path)
    ledger = queue.tier_ledger(TIER)
    if os.geteuid() == 0:
        pytest.skip("chmod unreadability needs a non-root worker")
    os.chmod(ledger.free_dir, 0o000)
    try:
        result = queue.mint_tier_capacity(TIER, dict(WANTED))
    finally:
        os.chmod(ledger.free_dir, 0o755)
    assert result["reclaimed"] == {}, result
    assert (ledger.minted_dir / "dead" / dead_name).exists()
    assert ledger.holder_tokens(HOLDER_H).get(KIND) == 1
    result = queue.mint_tier_capacity(TIER, dict(WANTED))
    assert result["reclaimed"] == {}, result
    assert ledger.capacity().get(KIND) == 2
    assert ledger.available().get(KIND) == 1


def test_r6_dead_path_enotdir_refuses_dead_decision(tmp_path: Path) -> None:
    """A dead namespace that is a file (ENOTDIR), not a directory,
    refuses the dead decision instead of crashing the mint; free and
    held stay exact and the healed retry is exact."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    ledger = queue.tier_ledger(TIER)
    queue.mint_tier_capacity(TIER, {KIND: 2})
    assert ledger.acquire(HOLDER_H, {KIND: 1}) is True
    dead_dir = ledger.minted_dir / "dead"
    assert not dead_dir.exists()
    dead_dir.write_text("")  # a file where the namespace belongs: ENOTDIR
    try:
        result = queue.mint_tier_capacity(TIER, {KIND: 2})
    finally:
        dead_dir.unlink(missing_ok=True)
    assert result["reclaimed"] == {}, result
    assert ledger.capacity().get(KIND) == 2
    assert ledger.available().get(KIND) == 1
    assert ledger.holder_tokens(HOLDER_H).get(KIND) == 1
    result = queue.mint_tier_capacity(TIER, {KIND: 2})
    assert ledger.capacity().get(KIND) == 2


def test_r6_visible_enumeration_contract(tmp_path: Path) -> None:
    """Pins the enumeration contract the census refusal rests on: the
    legacy listing suppresses a file-as-directory, the authoritative
    one raises.  Skipped where the strict census predates the tree."""
    if getattr(pool, "_scan_visible", None) is None:
        pytest.skip("strict census absent pre-R6")
    probe = tmp_path / "probe"
    probe.write_text("")
    assert pool._scan(probe) == []
    with pytest.raises(OSError):
        pool._scan_visible(probe)
    assert pool._glob(probe, "*-*") == []
    with pytest.raises(OSError):
        pool._glob_visible(probe, "*-*")


def test_r6_mutation_guard_overhead_sample(tmp_path: Path) -> None:
    """Overhead sample (evidence, not a gate): guarded tier ops vs
    unguarded host ops.  The generous bound below is deadlock
    detection, not a performance claim."""
    queue, _ = _shaped_queue(tmp_path)
    tier_ledger = queue.tier_ledger(TIER)
    host_ledger = queue.ledger("r6bench")
    host_ledger.ensure_capacity({"cpu": 2})
    repeats = 30
    start = time.monotonic()
    for _ in range(repeats):
        handle = tier_ledger.begin_acquire("k" * 64, {KIND: 1})
        assert handle is not None
        assert tier_ledger.abandon_acquire(handle) == 1
    guarded_s = time.monotonic() - start
    start = time.monotonic()
    for _ in range(repeats):
        handle = host_ledger.begin_acquire("k" * 64, {"cpu": 1})
        assert handle is not None
        assert host_ledger.abandon_acquire(handle) == 1
    unguarded_s = time.monotonic() - start
    print(f"\nr6-guard-overhead: guarded-tier {guarded_s / repeats * 1e3:.2f}ms/op, "
          f"unguarded-host {unguarded_s / repeats * 1e3:.2f}ms/op "
          f"({repeats} begin+abandon pairs each)")
    assert guarded_s < 120.0, guarded_s

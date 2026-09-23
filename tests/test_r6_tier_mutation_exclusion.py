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
  demanding 3 tokens -- more than the two backed credits.

The injected race is held -> free BETWEEN the reclaim's free listing
and its holder listing, performed by a helper thread calling the REAL
``release`` with no probe and no voluntary serialization: on pre-R6
production that call takes no guard and lands mid-scan; on fixed code
it blocks on the new guard outside the whole apply.  The test hook
only observes (completed vs still-blocked after a bounded wait) and
never steers the helper.  H's token is then missed by both listings,
headroom reads 2 - 1 = 1, and the dead name is reissued with no
backing -- 3 free against backing 2 at that prefix.  The real
claimant, in its own thread through ``PoolQueue.claim``, takes all 3
before the same apply's retire can trim the excess. The free-only
retire cannot remove held credits, so the final total remains 3
against backing of 2: persistent over-admission.

With the guard (``_guarded_mutation`` via ``PoolQueue.tier_ledger``)
the release serializes outside the apply, headroom reads exact, the
real claim declines, the dead name stays dead, and the books close
exact with no unbacked prefix.

RED provenance: the conformance test below FAILS on the actual base
commit ``3420951679`` (new tests only, root action ``dbea093b40f9``):
prefix 3, claim wins, claimant holds all three credits, total stays 3.
The hook interrupts the actual headroom census in both implementations.
No test proves the race by
disabling the mechanism under test.  GREEN is the same test passing
on the guarded tree.

The direct ``begin_acquire`` contention case is ledger-component
evidence and is labelled as such; the claim-boundary case above is
the end-to-end one.

Runs under pbtest at priority -10; never executed locally.
"""
from __future__ import annotations

import errno
import os
import stat
import threading
import time
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import adaptive_cpu, pool  # noqa: E402
import pbstatus  # noqa: E402

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


# --- An unreadable holder (#936) ------------------------------------------
#
# The fleet runs two interpreters whose ``Path.is_dir`` disagree: Python
# 3.12 (the Sparks) re-raises an EIO from ``stat``, and Python 3.14
# (dl380g10, where the tier loop runs) is ``os.path.isdir``, which returns
# False on every ``OSError``.  So the fault is injected at ``os.stat``,
# which both ``Path.stat`` and ``os.path.isdir`` call, and every test runs
# twice: on this interpreter's own ``Path.is_dir``, and on dl380g10's,
# emulated verbatim (its source, read off the box, is below).  The
# emulation is what shows the live tier-loop host's failure on any
# interpreter: before #936 the strict census there read an unreadable
# holder as absent and reissued a dead name with no backing.

EIO_TEXT = "injected holder metadata I/O error"


def _python314_is_dir(self, *, follow_symlinks=True):
    """``pathlib.Path.is_dir`` exactly as Python 3.14.4 on dl380g10 has it."""
    if follow_symlinks:
        return os.path.isdir(self)
    try:
        return stat.S_ISDIR(self.stat(follow_symlinks=follow_symlinks).st_mode)
    except (OSError, ValueError):
        return False


def _python314_exists(self, *, follow_symlinks=True):
    """``pathlib.Path.exists`` exactly as Python 3.14.4 has it."""
    if follow_symlinks:
        return os.path.exists(self)
    return os.path.lexists(self)


PATH_IMPLS = ("interpreter", "python314")


def _unreadable(guarded, paths, path_impl: str) -> None:
    """Make ``stat`` of each path fail with EIO, on the chosen ``Path``."""
    targets = {os.fspath(path) for path in paths}
    original = os.stat

    def failing(path, *args, **kwargs):
        if isinstance(path, (str, os.PathLike)) and os.fspath(path) in targets:
            raise OSError(errno.EIO, EIO_TEXT, os.fspath(path))
        return original(path, *args, **kwargs)

    guarded.setattr(os, "stat", failing)
    if path_impl == "python314":
        guarded.setattr(Path, "is_dir", _python314_is_dir)
        guarded.setattr(Path, "exists", _python314_exists)


def _eio(holder: Path) -> dict:
    return {"holder": str(holder), "errno": errno.EIO, "error": EIO_TEXT}


@pytest.mark.parametrize("path_impl", PATH_IMPLS)
def test_metadata_stat_error_cannot_hide_a_live_holder(tmp_path, monkeypatch,
                                                       path_impl):
    """An unreadable holder type is unknown, not proof it holds no tokens.

    RED on pre-#936 main: on 3.12 the mint raises the EIO out of
    ``capacity()`` (the tier loop loses the whole cycle); on the 3.14
    emulation the strict reclaim reads H as absent and reissues the dead
    name with no backing (``reclaimed == {stage_gib: 1}``).
    """
    queue, dead_name = _shaped_queue(tmp_path)
    ledger = queue.tier_ledger(TIER)
    holder = ledger.held_dir / HOLDER_H
    with monkeypatch.context() as guarded:
        _unreadable(guarded, [holder], path_impl)
        result = queue.mint_tier_capacity(TIER, dict(WANTED))
        free_during = ledger.available().get(KIND, 0)
    # The unknown census reissued nothing: the dead name stays dead.
    assert result["reclaimed"] == {}
    assert (ledger.minted_dir / "dead" / dead_name).exists()
    # H counted as HELD: the one free token was retired, never read as
    # headroom, and the report names H with its errno.
    assert result["retired"] == {KIND: 1}
    assert free_during == 0
    assert result["census_unreadable"]["unreadable"] == [_eio(holder)]
    assert result["census_unreadable"]["retired"] == {KIND: 1}
    # The books never exceed the backing of 2: only H's live token is left.
    assert ledger.capacity().get(KIND) == 1
    # Readable again: the next mint restores exactly the backed two and
    # removes the report.
    result = queue.mint_tier_capacity(TIER, dict(WANTED))
    assert ledger.capacity().get(KIND) == 2
    assert ledger.available().get(KIND) == 1
    assert result["census_unreadable"] is None
    assert not ledger.census_report_path.exists()


@pytest.mark.parametrize("path_impl", PATH_IMPLS)
def test_unreadable_holder_still_retires_under_a_falling_wanted(
        tmp_path, monkeypatch, path_impl):
    """A lower-bound total must not skip the shrink.

    wanted falls 2 -> 1 while H (1 live token) is unreadable.  The truth
    is held 1, free 1, wanted 1: the free token is unbacked and must go.
    RED on pre-#936 main: 3.12 raises out of ``capacity()``; on the 3.14
    emulation the total reads 1 (H hidden), ``1 > 1`` is false, the
    retire never runs and the unbacked free token stays admissible.
    """
    queue, _dead = _shaped_queue(tmp_path)
    ledger = queue.tier_ledger(TIER)
    holder = ledger.held_dir / HOLDER_H
    with monkeypatch.context() as guarded:
        _unreadable(guarded, [holder], path_impl)
        result = queue.mint_tier_capacity(TIER, {KIND: 1})
    assert result["retired"] == {KIND: 1}
    assert ledger.available().get(KIND, 0) == 0
    assert ledger.capacity().get(KIND) == 1


@pytest.mark.parametrize("path_impl", PATH_IMPLS)
def test_a_holder_that_stays_unreadable_is_reported_every_cycle(
        tmp_path, monkeypatch, path_impl):
    """Loud, not silent: a persistent fault is a stall with a named cause.

    Every mint cycle reports the ledger, the holder, its errno, the kinds
    asked and what was retired, with a consecutive-cycle count; the claim
    that the stall refuses is denied as ``tier_census_unreadable`` naming
    the holder; and ``pbstatus --starvation`` attributes the stall to it.
    """
    queue, _dead = _shaped_queue(tmp_path)
    ledger = queue.tier_ledger(TIER)
    holder = ledger.held_dir / HOLDER_H
    _publish_claimant(queue)
    reports = []
    with monkeypatch.context() as guarded:
        _unreadable(guarded, [holder], path_impl)
        for _cycle in range(3):
            reports.append(queue.mint_tier_capacity(TIER, dict(WANTED))["census_unreadable"])
        row = queue.claim(owner="worker:1:abcd0001", capacity=dict(HOST_CAP),
                          tags=["dl380g10"])
        starvation = pbstatus.read_starvation(queue.root)
    assert [report["cycles"] for report in reports] == [1, 2, 3]
    for report in reports:
        assert report["ledger"] == str(ledger.base)
        assert report["unreadable"] == [_eio(holder)]
        assert report["kinds_asked"] == [KIND]
    assert [report["retired"] for report in reports] == [{KIND: 1}, {}, {}]
    # The claim is refused, and the refusal names the holder.
    assert row is None
    denials = adaptive_cpu.read_json(
        adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS)
    (denial,) = [value for value in denials.get("records", {}).values()
                 if value["action_key"] == CLAIM_KEY]
    assert denial["reason"] == "tier_census_unreadable"
    shortage = denial["evidence"]["tier_shortage"]
    assert shortage["tier_id"] == TIER
    assert shortage["census_unreadable"] == [_eio(holder)]
    # The starvation census attributes the stall to the holder.
    (entry,) = [entry for entry in starvation["census_unreadable"]
                if entry["ledger_id"] == TIER]
    assert entry["ledger_kind"] == "tier"
    assert entry["cycles"] == 3
    assert entry["unreadable"] == [_eio(holder)]
    assert any(str(holder) in note for note in starvation["notes"])


@pytest.mark.parametrize("path_impl", PATH_IMPLS)
def test_retire_all_touches_only_the_unreadable_ledger_and_kinds_asked(
        tmp_path, monkeypatch, path_impl):
    """Blast radius: one ledger, the kinds its retire was asked about.

    Two host ledgers in the same state -- free cpu 5, held cpu 3 -- asked
    the same shrink to cpu 4.  The readable one retires exactly 4
    (``excess = 5 + 3 - 4``).  The one with the unreadable holder counts it
    as held with an unknown count and retires all 5 free cpu, and leaves
    mem_gb, which it was not asked about, alone.  Skipping the holder
    instead would retire 1 and leave the true total at 7.  A second tier
    ledger is untouched by the first tier's fault.
    """
    root = tmp_path / "reservations"
    sick, well = pool.ResourceLedger(root, "boxa"), pool.ResourceLedger(root, "boxb")
    key = "k" * 64
    for ledger in (sick, well):
        ledger.ensure_capacity({"cpu": 8, "mem_gb": 4})
        assert ledger.acquire(key, {"cpu": 3}) is True
    queue, _dead = _shaped_queue(tmp_path)
    other_tier = "prismabuild-stage:r6other"
    queue.mint_tier_capacity(other_tier, {KIND: 2})
    with monkeypatch.context() as guarded:
        _unreadable(guarded, [sick.held_dir / key,
                              queue.tier_ledger(TIER).held_dir / HOLDER_H], path_impl)
        assert sick.retire_free_capacity({"cpu": 4}) == {"cpu": 5}
        assert well.retire_free_capacity({"cpu": 4}) == {"cpu": 4}
        queue.mint_tier_capacity(TIER, dict(WANTED))
    assert sick.available() == {"mem_gb": 4}
    assert well.available() == {"cpu": 1, "mem_gb": 4}
    report = pool._read_json(sick.census_report_path)
    assert report["kinds_asked"] == ["cpu"]
    assert report["retired"] == {"cpu": 5}
    assert report["unreadable"] == [_eio(sick.held_dir / key)]
    assert not well.census_report_path.exists()
    other = queue.tier_ledger(other_tier)
    assert other.available() == {KIND: 2}
    assert not other.census_report_path.exists()


@pytest.mark.parametrize("path_impl", PATH_IMPLS)
def test_an_unreadable_token_check_never_strands_a_mint_marker(
        tmp_path, monkeypatch, path_impl):
    """Host ledger grow: an unknown held check removes its marker.

    ``ensure_capacity`` creates the index marker before asking whether a
    holder has the name.  Answering "held" would leave a marker with no
    token, which is skipped forever; answering "not held" would mint a
    free duplicate of a name the holder may hold.  RED on pre-#936 main:
    3.12 raised with the marker left behind (the index lost for good), and
    3.14 answered "not held" and minted without knowing.
    """
    ledger = pool.ResourceLedger(tmp_path / "reservations", "boxa")
    key = "k" * 64
    ledger.ensure_capacity({"cpu": 1})
    assert ledger.acquire(key, {"cpu": 1}) is True
    with monkeypatch.context() as guarded:
        _unreadable(guarded, [ledger.held_dir / key / "cpu-0001"], path_impl)
        with pytest.raises(OSError) as raised:
            ledger.ensure_capacity({"cpu": 2})
    assert raised.value.errno == errno.EIO
    assert not (ledger.minted_dir / "cpu-0001").exists()
    assert ledger.available() == {}
    # Readable again: the index is minted, not lost.
    ledger.ensure_capacity({"cpu": 2})
    assert ledger.capacity() == {"cpu": 2}
    assert ledger.available() == {"cpu": 1}


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

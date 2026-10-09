"""Ledger primitives behind the prelaunch resident group (#1594).

The tier loop splits a prelaunch group's whole demand across its chunk
movers, and reconciles a crash between ``begin_acquire`` and
``commit_acquire`` by census.  Both need ledger-level primitives with exact
semantics, tested here first (design R1''' exact handle parse, R1' group
split): the private-handle census ``acquisitions_of`` /
``unparsed_acquisitions_of``, the bounded split ``transfer_count``, and the
queue-level ``transfer_tier_reservation_count``.  Tier loop, plans,
manifests and admission are non-goals here.
"""

from __future__ import annotations

import os
import socket
from pathlib import Path

import pytest

import prismabuild.pool as pool
import prismabuild.storage_tiers as storage_tiers

TIER = "prismabuild-stage:dl380g10"
HOST = "dl380g10"
RAM_TIER = storage_tiers.tier_id("ram", HOST)

#: A dot-free group holder of the contract's shape
#: ``prelaunch-<unit16>-<tier digest12>-<phase digest12>``.
HOLDER = "prelaunch-" + "a" * 16 + "-" + "b" * 12 + "-" + "c" * 12
OTHER = "prelaunch-" + "d" * 16 + "-" + "e" * 12 + "-" + "f" * 12
#: A strict dot-free prefix of HOLDER: exact equality must not match it.
PREFIX = "prelaunch-" + "a" * 16

SRC = "1" * 64
DST = "2" * 64

USEC = 1791327299000000
PID = 12345
NONCE = "abcdef01"


def _handle(holder: str, *, usec: object = USEC, host: str = "node01",
            pid: object = PID, nonce: str = NONCE) -> str:
    """A private-handle name of the ``_begin_acquire_locked`` shape."""

    return f"claiming.{usec}.{holder}.{host}.{pid}.{nonce}"


def _forge(ledger: pool.ResourceLedger, name: str, tokens: int) -> Path:
    """A handle directory with real-shaped token files, never acquired."""

    directory = ledger.held_dir / name
    directory.mkdir(parents=True, exist_ok=True)
    for index in range(tokens):
        (directory / f"stage_gib-{index:04d}").write_text("forged")
    return directory


def _held_total(ledger: pool.ResourceLedger) -> int:
    """Every real token under ``held/``, whatever holder names it.

    Counted straight off the directory -- not through the readers under
    test -- so the sum invariant below is independent of them.  Metadata
    files travel with a holder but are not capacity, the same split
    ``transfer`` documents.
    """

    total = 0
    for holder in ledger.held_dir.iterdir():
        if not holder.is_dir():
            continue
        for token in holder.iterdir():
            if token.name in (pool.cpu_admission.METADATA,
                              pool.gpu_admission.METADATA):
                continue
            if "-" not in token.name:
                continue
            total += 1
    return total


def _assert_conserved(ledger: pool.ResourceLedger, kind: str,
                      capacity: int) -> None:
    """Held plus free is the minted capacity: no move creates or loses."""

    assert (_held_total(ledger) + ledger.available().get(kind, 0)
            == capacity == ledger.capacity().get(kind, 0))


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {"stage_gib": 8})
    return q


@pytest.fixture()
def ram_queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-ram-queue")
    q.ensure_layout()
    q.ledger(HOST).ensure_capacity({"cpu": 80, "mem_gb": 64})
    q.mint_tier_capacity(RAM_TIER, {"ram_gib": 12})
    return q


# ----------------------------------------------- acquisitions_of: the parse


def test_a_real_begin_is_visible_to_the_holders_census(queue) -> None:
    """Ground the forged-handle tests below against the real writer."""

    ledger = queue.tier_ledger(TIER)
    handle = ledger.begin_acquire(SRC, {"stage_gib": 2})
    assert handle is not None, "fixture: the take must succeed"
    try:
        found = ledger.acquisitions_of(SRC)
        assert len(found) == 1
        name, usec, host, pid, count = found[0]
        assert name == handle
        assert usec == int(handle.split(".")[1])
        assert host == socket.gethostname()
        assert pid == os.getpid()
        assert count == 2
        assert ledger.unparsed_acquisitions_of(SRC) == []
    finally:
        assert ledger.abandon_acquire(handle) == 2


def test_a_dotted_hostname_parses_to_the_whole_host(queue) -> None:
    """R1''': the host is the re-joined middle, not the third field."""

    ledger = queue.tier_ledger(TIER)
    name = _handle(HOLDER, host="node.example.org")
    _forge(ledger, name, 3)

    assert ledger.acquisitions_of(HOLDER) == [
        (name, USEC, "node.example.org", PID, 3)]
    assert ledger.unparsed_acquisitions_of(HOLDER) == []


def test_a_short_hostname_parses(queue) -> None:
    """The single-label end of the hostname range."""

    ledger = queue.tier_ledger(TIER)
    name = _handle(HOLDER, host="h9")
    _forge(ledger, name, 1)

    assert ledger.acquisitions_of(HOLDER) == [(name, USEC, "h9", PID, 1)]
    assert ledger.unparsed_acquisitions_of(HOLDER) == []


def test_a_holder_never_matches_a_prefix_of_another(queue) -> None:
    """R1''': the holder matches by exact equality (holders are dot-free)."""

    ledger = queue.tier_ledger(TIER)
    long_name = _handle(HOLDER, host="node01")
    short_name = _handle(PREFIX, host="node01")
    _forge(ledger, long_name, 2)
    _forge(ledger, short_name, 1)

    assert [entry[0] for entry in ledger.acquisitions_of(PREFIX)] == [
        short_name]
    assert [entry[0] for entry in ledger.acquisitions_of(HOLDER)] == [
        long_name]
    assert ledger.unparsed_acquisitions_of(PREFIX) == []
    assert ledger.unparsed_acquisitions_of(HOLDER) == []


def test_another_holders_handle_belongs_to_neither_set(queue) -> None:
    """Attributed to its own holder only: not counted, not unparsed here."""

    ledger = queue.tier_ledger(TIER)
    _forge(ledger, _handle(OTHER, host="node.example.org"), 2)

    assert ledger.acquisitions_of(HOLDER) == []
    assert ledger.unparsed_acquisitions_of(HOLDER) == []
    assert len(ledger.acquisitions_of(OTHER)) == 1


@pytest.mark.parametrize("nonce", ["abcdef0", "abcdef012", "ABCDEF01",
                                   "abcdef0g", ""])
def test_a_malformed_nonce_is_unparsed_never_counted(
        queue, nonce: str) -> None:
    """R1''': the nonce is eight lowercase hex digits, nothing looser."""

    ledger = queue.tier_ledger(TIER)
    name = _handle(HOLDER, nonce=nonce)
    _forge(ledger, name, 2)

    assert ledger.acquisitions_of(HOLDER) == []
    assert ledger.unparsed_acquisitions_of(HOLDER) == [name]


@pytest.mark.parametrize("pid", ["12x", "", "0x10", "-5"])
def test_a_malformed_pid_is_unparsed_never_counted(
        queue, pid: object) -> None:
    """R1''': the pid is all decimal digits."""

    ledger = queue.tier_ledger(TIER)
    name = _handle(HOLDER, pid=pid)
    _forge(ledger, name, 2)

    assert ledger.acquisitions_of(HOLDER) == []
    assert ledger.unparsed_acquisitions_of(HOLDER) == [name]


@pytest.mark.parametrize("usec", ["abc", "", "17913x7299"])
def test_a_malformed_usec_is_unparsed_never_counted(
        queue, usec: object) -> None:
    """The clock field is the same one ``_acquisition_clock`` reads."""

    ledger = queue.tier_ledger(TIER)
    name = _handle(HOLDER, usec=usec)
    _forge(ledger, name, 2)

    assert ledger.acquisitions_of(HOLDER) == []
    assert ledger.unparsed_acquisitions_of(HOLDER) == [name]


@pytest.mark.parametrize("name", [
    f"claiming.{USEC}.{HOLDER}.node01",
    f"claiming.{USEC}.{HOLDER}",
    f"claiming.{USEC}.{HOLDER}.node01.{PID}",
    f"claiming.{USEC}.{HOLDER}..{PID}.{NONCE}",
])
def test_a_handle_with_too_few_or_empty_fields_is_unparsed(
        queue, name: str) -> None:
    """Fewer than six fields, or an empty host, names nothing countable."""

    ledger = queue.tier_ledger(TIER)
    _forge(ledger, name, 2)

    assert ledger.acquisitions_of(HOLDER) == []
    assert ledger.unparsed_acquisitions_of(HOLDER) == [name]


def test_names_no_holder_can_own_are_in_no_holders_unparsed_set(
        queue) -> None:
    """A ``claiming.*`` name too short to name a holder is unattributable.

    It belongs to no holder's census and to no holder's unparsed set; the
    stale sweep still owns it.  An action key belongs to neither set either.
    """

    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(SRC, {"stage_gib": 1})
    _forge(ledger, "claiming.ponder", 0)
    (ledger.held_dir / "claiming.").mkdir(exist_ok=True)

    assert ledger.acquisitions_of(HOLDER) == []
    assert ledger.unparsed_acquisitions_of(HOLDER) == []


def test_token_count_excludes_admission_metadata(queue) -> None:
    """The count is real tokens the way ``commit_acquire`` counts them."""

    ledger = queue.tier_ledger(TIER)
    name = _handle(HOLDER, host="node.example.org")
    directory = _forge(ledger, name, 4)
    (directory / pool.cpu_admission.METADATA).write_text(
        '{"borrowed_cpu": 0}')
    (directory / pool.gpu_admission.METADATA).write_text(
        '{"borrowed_gpu": 0}')

    assert ledger.acquisitions_of(HOLDER) == [
        (name, USEC, "node.example.org", PID, 4)]


def test_handles_report_in_sorted_order(queue) -> None:
    """Deterministic census order for the reconcile that reads it."""

    ledger = queue.tier_ledger(TIER)
    second = _handle(HOLDER, usec=USEC + 2, host="b", nonce="00000002")
    first = _handle(HOLDER, usec=USEC + 1, host="a", nonce="00000001")
    _forge(ledger, second, 1)
    _forge(ledger, first, 2)

    assert [entry[0] for entry in ledger.acquisitions_of(HOLDER)] == [
        first, second]


# ------------------------------------------------------- transfer_count


def test_a_full_count_move_hands_every_token_over(queue) -> None:
    """The whole-reservation case, bounded form: nothing stays, none free."""

    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(SRC, {"stage_gib": 3})
    _assert_conserved(ledger, "stage_gib", 8)

    assert ledger.transfer_count(SRC, DST, 3) == 3

    assert ledger.holder_tokens(SRC) == {}
    assert ledger.holder_tokens(DST) == {"stage_gib": 3}
    _assert_conserved(ledger, "stage_gib", 8)


def test_a_partial_move_then_resumes_to_the_rest(queue) -> None:
    """R1': the chunk split -- sorted order, so a re-call continues it."""

    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(SRC, {"stage_gib": 5})

    assert ledger.transfer_count(SRC, DST, 2) == 2
    assert sorted((ledger.held_dir / DST).iterdir(),
                  key=lambda entry: entry.name) == [
        ledger.held_dir / DST / "stage_gib-0000",
        ledger.held_dir / DST / "stage_gib-0001"]
    assert ledger.holder_tokens(SRC) == {"stage_gib": 3}
    _assert_conserved(ledger, "stage_gib", 8)

    assert ledger.transfer_count(SRC, DST, 3) == 3
    assert ledger.holder_tokens(SRC) == {}
    assert ledger.holder_tokens(DST) == {"stage_gib": 5}
    _assert_conserved(ledger, "stage_gib", 8)


def test_a_move_never_moves_more_than_count_or_source(queue) -> None:
    """Bounded on both ends: at most ``count``, never more than held."""

    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(SRC, {"stage_gib": 4})

    assert ledger.transfer_count(SRC, DST, 2) == 2
    assert ledger.transfer_count(SRC, DST, 100) == 2
    assert ledger.holder_tokens(DST) == {"stage_gib": 4}
    _assert_conserved(ledger, "stage_gib", 8)


def test_a_destination_collision_is_left_in_place(queue) -> None:
    """The ``transfer`` rule: one token per index, so a collision stays.

    The forged same-named file stands for an earlier incarnation's leftover,
    the case ``transfer`` and ``commit_acquire`` refuse to rename over.  The
    short count is what the caller fails closed on; a re-call still moves
    nothing, so the stranded name never cycles.
    """

    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(SRC, {"stage_gib": 3})
    destination = ledger.held_dir / DST
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "stage_gib-0001").write_text("earlier incarnation")

    assert ledger.transfer_count(SRC, DST, 10) == 2
    assert sorted(path.name for path in (ledger.held_dir / SRC).iterdir()) == [
        "stage_gib-0001"]
    assert sorted(path.name for path in destination.iterdir()) == [
        "stage_gib-0000", "stage_gib-0001", "stage_gib-0002"]

    assert ledger.transfer_count(SRC, DST, 10) == 0


def test_an_interrupted_move_keeps_the_sum_and_resumes(
        queue, monkeypatch) -> None:
    """R1': a death mid-move splits holder and mover with the sum unchanged.

    The injected ``RuntimeError`` (not ``OSError``, which the per-token loop
    tolerates the way ``transfer`` does) stands for the crash: the move
    stops part-way, the census still adds up, and a re-call with the
    remaining count finishes it.
    """

    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(SRC, {"stage_gib": 5})
    real_rename = os.rename
    calls = {"n": 0}

    def _die_mid_move(src, dst):
        calls["n"] += 1
        if calls["n"] == 3:
            raise RuntimeError("injected mid-move interrupt")
        return real_rename(src, dst)

    monkeypatch.setattr(pool.os, "rename", _die_mid_move)
    with pytest.raises(RuntimeError, match="injected mid-move"):
        ledger.transfer_count(SRC, DST, 5)
    monkeypatch.setattr(pool.os, "rename", real_rename)

    assert ledger.holder_tokens(SRC) == {"stage_gib": 3}
    assert ledger.holder_tokens(DST) == {"stage_gib": 2}
    _assert_conserved(ledger, "stage_gib", 8)

    assert ledger.transfer_count(SRC, DST, 3) == 3
    assert ledger.holder_tokens(DST) == {"stage_gib": 5}
    _assert_conserved(ledger, "stage_gib", 8)


def test_transfer_count_refusals(queue) -> None:
    """The ``transfer`` refusals: empty or equal keys move nothing."""

    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(SRC, {"stage_gib": 2})

    assert ledger.transfer_count("", DST, 2) == 0
    assert ledger.transfer_count(SRC, "", 2) == 0
    assert ledger.transfer_count(SRC, SRC, 2) == 0
    assert ledger.transfer_count("9" * 64, DST, 2) == 0
    assert ledger.transfer_count(SRC, DST, 0) == 0
    assert ledger.transfer_count(SRC, DST, -1) == 0
    with pytest.raises(pool.PoolContractError):
        ledger.transfer_count(SRC, DST, "two")
    assert ledger.holder_tokens(SRC) == {"stage_gib": 2}
    _assert_conserved(ledger, "stage_gib", 8)


def test_transfer_count_refuses_a_claimant_private_acquisition(
        queue) -> None:
    """Those are named for a claimant, whose action is not decided yet."""

    ledger = queue.tier_ledger(TIER)
    handle = ledger.begin_acquire(SRC, {"stage_gib": 1})
    assert handle is not None, "fixture: the take must succeed"
    try:
        with pytest.raises(pool.PoolContractError,
                           match="never a claimant-private"):
            ledger.transfer_count(handle, DST, 1)
        with pytest.raises(pool.PoolContractError,
                           match="never a claimant-private"):
            ledger.transfer_count(SRC, handle, 1)
    finally:
        ledger.abandon_acquire(handle)


def test_transfer_count_leaves_admission_metadata_with_the_source(
        queue) -> None:
    """The deliberate ``transfer`` difference: a partial split moves tokens.

    ``transfer`` carries the seat description along because the whole
    reservation changes owner; here the funding record (which names the
    token set) stays the authority, so the metadata stays put and is never
    counted on either side.
    """

    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(SRC, {"stage_gib": 2})
    (ledger.held_dir / SRC / pool.cpu_admission.METADATA).write_text(
        '{"borrowed_cpu": 0}')
    (ledger.held_dir / SRC / pool.gpu_admission.METADATA).write_text(
        '{"borrowed_gpu": 0}')

    assert ledger.transfer_count(SRC, DST, 5) == 2
    assert sorted(path.name for path in (ledger.held_dir / SRC).iterdir()) == [
        pool.cpu_admission.METADATA, pool.gpu_admission.METADATA]
    assert sorted(path.name for path in (ledger.held_dir / DST).iterdir()) == [
        "stage_gib-0000", "stage_gib-0001"]
    _assert_conserved(ledger, "stage_gib", 8)


# ---------------------------------- transfer_tier_reservation_count


def test_the_queue_count_move_mirrors_the_whole_move(queue) -> None:
    """Stage tier: a partial hand-over, then the remainder, no faults."""

    assert queue.tier_ledger(TIER).acquire(SRC, {"stage_gib": 5})
    faults: list = []

    assert queue.transfer_tier_reservation_count(
        TIER, SRC, DST, 2, faults=faults) == 2
    assert queue.tier_ledger(TIER).holder_tokens(DST) == {"stage_gib": 2}
    assert faults == []

    assert queue.transfer_tier_reservation_count(
        TIER, SRC, DST, 3, faults=faults) == 3
    assert queue.tier_ledger(TIER).holder_tokens(DST) == {"stage_gib": 5}
    assert faults == []
    _assert_conserved(queue.tier_ledger(TIER), "stage_gib", 8)


def test_a_partial_ram_split_moves_both_halves_by_the_moved_count(
        ram_queue) -> None:
    """RAM tier: the host hold moves exactly what the tier half moved.

    The host mirror is count-aligned with the tier reservation (the sync
    makes each live tier holder's host hold equal to its occupancy tokens),
    so moving the tier-moved count -- never the requested one -- keeps the
    two halves attributable and both sums unchanged.  No ``PoolContractError``
    for RAM tiers: the partial split is consistent there too.
    """

    tier = ram_queue.tier_ledger(RAM_TIER)
    host = ram_queue.ledger(HOST)
    assert tier.acquire(SRC, {"ram_gib": 6})
    assert host.acquire("ram-host:" + SRC, {"mem_gb": 6})
    faults: list = []

    assert ram_queue.transfer_tier_reservation_count(
        RAM_TIER, SRC, DST, 4, faults=faults) == 4
    assert faults == []
    assert tier.holder_tokens(SRC) == {"ram_gib": 2}
    assert tier.holder_tokens(DST) == {"ram_gib": 4}
    assert host.holder_tokens("ram-host:" + SRC) == {"mem_gb": 2}
    assert host.holder_tokens("ram-host:" + DST) == {"mem_gb": 4}
    _assert_conserved(tier, "ram_gib", 12)

    assert ram_queue.transfer_tier_reservation_count(
        RAM_TIER, SRC, DST, 2, faults=faults) == 2
    assert tier.holder_tokens(DST) == {"ram_gib": 6}
    assert host.holder_tokens("ram-host:" + DST) == {"mem_gb": 6}
    assert faults == []


def test_a_failed_ram_host_half_is_named_not_swallowed(
        ram_queue, monkeypatch) -> None:
    """The whole-move shape: the tier half moves, the fault is named."""

    tier = ram_queue.tier_ledger(RAM_TIER)
    host = ram_queue.ledger(HOST)
    assert tier.acquire(SRC, {"ram_gib": 6})
    assert host.acquire("ram-host:" + SRC, {"mem_gb": 6})
    real_count = pool.ResourceLedger.transfer_count

    def _host_side_transfer(self, from_holder: str, to_holder: str,
                            count: int) -> int:
        if from_holder.startswith("ram-host:"):
            raise OSError("boom: host transfer unreadable")
        return real_count(self, from_holder, to_holder, count)

    monkeypatch.setattr(pool.ResourceLedger, "transfer_count",
                        _host_side_transfer)
    faults: list = []

    assert ram_queue.transfer_tier_reservation_count(
        RAM_TIER, SRC, DST, 4, faults=faults) == 4
    assert [fault["reason"] for fault in faults] == [
        "ram_host_transfer_failed"]
    assert tier.holder_tokens(DST) == {"ram_gib": 4}
    assert host.holder_tokens("ram-host:" + SRC) == {"mem_gb": 6}
    _assert_conserved(tier, "ram_gib", 12)

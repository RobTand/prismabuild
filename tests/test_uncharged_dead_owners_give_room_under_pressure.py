"""An uncharged dead owner's bytes are room the tier can take back (#1061).

Live shape (2026-09-23, owner ``ff696cef``): a failed consumer's executed
DONE mover filed a move receipt that never completed (4 of 5 entries), so
the ledger released its stage token when the mover finished.  Its staged
files, its fragment and its material sidecar stayed, and they are coherent:
nothing in them is stale, so the stale-mention prune (#853) has nothing to
do.  The held-key orphan pass reads only held keys, so it never sees the
owner, and the zero-charge dead-owner branch (#839, #866) excludes an owner
that carries material.  Those bytes can never be given back, however much
room the tier's window needs.

The mint already counts them correctly: supply is ZFS writable plus landed,
and a holder with an incomplete receipt counts as in flight, so bytes no
token holds sit outside writable and outside every token.  Charging these
owners would subtract their bytes twice.  They stay uncharged; what changes
is that the held-key pass takes them as candidates under pressure, oldest
first, crediting the bytes each eviction deletes against the room it needs.

Every fixture is a temp stage root registered to a fake queue (never a real
/stage or /ram).  The staged files are sparse: a 1 GiB file is a length,
not a payload, and nothing hashes it.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

import test_dead_owner_fragment_blocks_then_retires as base  # noqa: E402
import test_stale_material_done_owner_retires as red  # noqa: E402
from test_dead_owner_fragment_blocks_then_retires import fleet  # noqa: E402,F401
from prismabuild import reader_lease, residency_map, storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_release  # noqa: E402

TIER = base.TIER
GIB = storage_tiers.GIB
DIGEST = "b" * 64
MANIFEST = "a" * 64


@pytest.fixture(autouse=True)
def _fresh_process_state():
    stage_release.reset_skip_checkpoints()
    stage_release.reset_holder_reports()
    yield
    stage_release.reset_skip_checkpoints()
    stage_release.reset_holder_reports()


def _staged(stage: Path, name: str, size: int) -> Path:
    """One sparse staged file in its own ``.pbrange`` directory, marked.

    The prewarm mark is what every stage publication sets; without it the
    reconciliation could remove the file for a reason this file is not
    about.
    """

    path = stage / "models" / f"{name}.pbrange" / f"0-{size}"
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with open(path, "wb") as stream:
            stream.truncate(size)
        os.setxattr(path, prewarm_loop.STAGE_SOURCE_XATTR,
                    f"/originals/{name}@0".encode())
    return path


def dead_owner(fleet, names, *, unix: float, size: int = GIB,
               charged: bool = False) -> tuple[str, str]:
    """A failed consumer's executed DONE mover, coherent, with material.

    ``charged=False`` is the #1061 shape: the move receipt did not complete,
    so no stage token is held.  Each staged file is ``size`` bytes, and the
    fragment and the material date every one, so the owner is coherent and
    the stale-mention prune leaves it whole.  ``unix`` is the receipt's time,
    which orders the orphan pass.
    """

    queue, stage, _ = fleet
    root = queue.residency_fragment_root()
    consumer, _generation = base._fail_consumer(queue)
    mover = base._key()
    base._publish(queue, mover, max_attempts=1)
    mentions: dict[str, dict[str, object]] = {}
    entries: dict[str, dict[str, object]] = {}
    for name in names:
        path = _staged(stage, name, size)
        key = residency_map.residency_map_key(f"/pool/models/{name}", 0)
        mentions[key] = {"stage_path": str(path), "bytes": size,
                         "sha256": DIGEST,
                         "file_id": reader_lease.stat_identity(str(path))}
        entries[key] = {"stage_path": str(path), "bytes": size,
                        "sha256": DIGEST, "offset": 0}
    reader_lease.write_material(
        root, consumer_action_key=consumer, mover_action_key=mover,
        tier_id=TIER, stage_root=str(stage), manifest_sha256=MANIFEST,
        generation="c" * 32, entries=mentions)
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": MANIFEST, "entries": entries})
    queue.record_move(mover, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": size * (len(names) + 1),
        "range_bytes": size * (len(names) + 1),
        "bytes_staged": size * len(names),
        "entries_declared": len(names) + 1, "entries_staged": len(names),
        "complete": charged, "errors": [], "seconds": 20.0, "unix": unix})
    queue.finish(mover, status="executed", detail={"returncode": 0})
    if charged:
        queue.mint_tier_capacity(TIER, {"stage_gib": len(names)})
        assert queue.tier_ledger(TIER).acquire(
            mover, {"stage_gib": len(names)}) is True
    else:
        assert mover not in queue.tier_ledger(TIER).held_keys()
    return consumer, mover


def _sweep(queue, stage, pressure):
    return stage_release.sweep(queue, stage_roots={TIER: str(stage)},
                               pressure=pressure)


def _owned(queue, consumer: str, mover: str) -> bool:
    """Whether the owner's fragment and material are both still filed."""

    root = queue.residency_fragment_root()
    fragment = residency_map.fragment_path(root, consumer, mover).exists()
    material = reader_lease.material_path(root, consumer, mover).exists()
    assert fragment == material, (fragment, material)
    return fragment


def _files(stage: Path, names, size: int = GIB) -> list[bool]:
    return [(stage / "models" / f"{name}.pbrange" / f"0-{size}").exists()
            for name in names]


def test_an_uncharged_coherent_owner_gives_its_room_under_pressure(fleet):
    """The #1061 red: under pressure, the owner's bytes must come back."""

    queue, stage, _ = fleet
    consumer, mover = dead_owner(fleet, ["solo-00001"], unix=100.0)
    # The dead-owner pass alone keeps it: coherent, so nothing to prune.
    first = stage_release.sweep_dead_owner_fragments(
        queue, stage_roots={TIER: str(stage)})
    assert [entry.get("retained_reason") for entry in first
            if entry.get("action_key") == mover] == [""], first
    assert _owned(queue, consumer, mover)

    receipts = _sweep(queue, stage, {TIER: 1})

    assert not _owned(queue, consumer, mover), (
        f"uncharged coherent dead owner {mover[:12]} kept its 1 GiB while "
        f"the tier needed 1 GiB: {receipts}")
    assert _files(stage, ["solo-00001"]) == [False]


def test_three_owners_and_one_owners_room_evicts_exactly_the_oldest(fleet):
    """Oldest first, and stop once the deleted bytes cover the room."""

    queue, stage, _ = fleet
    # Published out of age order, so the order is the receipt's time and
    # not the order the owners were built in.
    middle = dead_owner(fleet, ["age-00200"], unix=200.0)
    newest = dead_owner(fleet, ["age-00300"], unix=300.0)
    oldest = dead_owner(fleet, ["age-00100"], unix=100.0)

    receipts = _sweep(queue, stage, {TIER: 1})

    assert [_owned(queue, *owner) for owner in (oldest, middle, newest)] == [
        False, True, True], receipts
    assert _files(stage, ["age-00100", "age-00200", "age-00300"]) == [
        False, True, True]


def _reports(receipts) -> list[dict]:
    return [entry for entry in receipts
            if entry.get("event") == stage_release.UNCHARGED_OWNERS_EVENT]


def _for(receipts, mover: str) -> list[dict]:
    return [entry for entry in receipts if entry.get("action_key") == mover
            and entry.get("reason") == "uncharged-owner-sweep"]


@pytest.mark.parametrize("needed,owned", [
    (2, [False, False, True]),
    (3, [False, False, False]),
    (9, [False, False, False]),
])
def test_more_room_takes_more_owners_in_age_order(fleet, needed, owned):
    queue, stage, _ = fleet
    middle = dead_owner(fleet, ["more-00200"], unix=200.0)
    newest = dead_owner(fleet, ["more-00300"], unix=300.0)
    oldest = dead_owner(fleet, ["more-00100"], unix=100.0)

    receipts = _sweep(queue, stage, {TIER: needed})

    assert [_owned(queue, *owner) for owner in (oldest, middle, newest)] == (
        owned), receipts


@pytest.mark.parametrize("pressure", [{}, {TIER: 0}, None],
                         ids=["empty", "zero", "not-given"])
def test_no_pressure_evicts_none(fleet, pressure):
    """Without a window waiting, an uncharged dead owner is cache."""

    queue, stage, _ = fleet
    owners = [dead_owner(fleet, [f"idle-{index:05d}"], unix=100.0 + index)
              for index in range(3)]

    for _ in range(2):
        receipts = _sweep(queue, stage, pressure)
        assert all(_owned(queue, *owner) for owner in owners), receipts
        assert not [entry for entry in receipts
                    if entry.get("reason") == "uncharged-owner-sweep"]
    assert _files(stage, [f"idle-{index:05d}" for index in range(3)]) == [
        True, True, True]


def test_room_the_ledger_already_has_evicts_none(fleet):
    queue, stage, _ = fleet
    owner = dead_owner(fleet, ["roomy-00001"], unix=100.0)
    queue.mint_tier_capacity(TIER, {"stage_gib": 1})
    assert queue.tier_ledger(TIER).available().get("stage_gib") == 1

    receipts = _sweep(queue, stage, {TIER: 1})

    assert _owned(queue, *owner), receipts
    assert _files(stage, ["roomy-00001"]) == [True]


def test_the_credit_is_the_sum_of_deleted_bytes_in_whole_gib(fleet):
    """Two half-GiB owners make one GiB of room between them, not zero."""

    queue, stage, _ = fleet
    half = GIB // 2
    first = dead_owner(fleet, ["half-00100"], unix=100.0, size=half)
    second = dead_owner(fleet, ["half-00200"], unix=200.0, size=half)
    third = dead_owner(fleet, ["half-00300"], unix=300.0, size=half)

    receipts = _sweep(queue, stage, {TIER: 1})

    assert [_owned(queue, *owner) for owner in (first, second, third)] == [
        False, False, True], receipts
    assert [receipt["bytes_deleted"] for receipt in (
        _for(receipts, first[1]) + _for(receipts, second[1]))] == [half, half]


def test_charged_and_uncharged_orphans_share_one_age_order(fleet):
    """The oldest receipt goes first, whichever kind it is."""

    queue, stage, _ = fleet
    uncharged_old = dead_owner(fleet, ["mix-00100"], unix=100.0)
    charged_new = dead_owner(fleet, ["mix-00200"], unix=200.0, charged=True)
    assert queue.tier_ledger(TIER).available().get("stage_gib", 0) == 0

    receipts = _sweep(queue, stage, {TIER: 1})

    assert not _owned(queue, *uncharged_old), receipts
    assert _owned(queue, *charged_new), receipts
    assert charged_new[1] in queue.tier_ledger(TIER).held_keys()


def test_an_older_charged_orphan_goes_first_and_covers_the_room(fleet):
    queue, stage, _ = fleet
    charged_old = dead_owner(fleet, ["mix-00100"], unix=100.0, charged=True)
    uncharged_new = dead_owner(fleet, ["mix-00200"], unix=200.0)

    receipts = _sweep(queue, stage, {TIER: 1})

    assert not _owned(queue, *charged_old), receipts
    assert queue.tier_ledger(TIER).available().get("stage_gib") == 1
    assert _owned(queue, *uncharged_new), receipts


def test_a_live_pin_protects_every_byte_and_the_next_owner_goes(
        fleet, monkeypatch):
    queue, stage, _ = fleet
    pinned = dead_owner(fleet, ["pin-00100"], unix=100.0)
    later = dead_owner(fleet, ["pin-00200"], unix=200.0)
    last = dead_owner(fleet, ["pin-00300"], unix=300.0)
    pin = reader_lease.acquire(
        queue, consumer_action_key=pinned[0],
        attempt={"nonce": "n1", "scope_id": "s1"}, tier_id=TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": GIB},
        holder={"host": "fixture", "pid": os.getpid()}, acquire_token="p1",
        covers=[{"mover_action_key": pinned[1], "manifest_sha256": MANIFEST}])
    assert pin["ok"], pin

    # The first pass sees the pin in the stale-mention census and leaves the
    # owner out.  The second reaches it through a skip checkpoint (forced
    # here, since a checkpoint on a directory changed this clock tick is
    # refused), and the whole eviction declines it.  Each pass takes the
    # next owner instead.
    first = _sweep(queue, stage, {TIER: 1})
    assert _owned(queue, *pinned) and not _owned(queue, *later), first
    assert _for(first, pinned[1]) == []
    monkeypatch.setattr(stage_release, "_skip_checkpoint_hit",
                        lambda *args: True)
    second = _sweep(queue, stage, {TIER: 1})
    assert _owned(queue, *pinned) and not _owned(queue, *last), second
    assert _files(stage, ["pin-00100"]) == [True]
    (receipt,) = _for(second, pinned[1])
    assert receipt["complete"] is False and receipt["bytes_deleted"] == 0
    assert receipt["declined"] == ["pinned"], receipt


@pytest.mark.parametrize("claim", ["foreign", "same-key"])
def test_a_live_claim_keeps_the_file(fleet, monkeypatch, claim):
    queue, stage, _ = fleet
    claimed = dead_owner(fleet, ["claim-00100"], unix=100.0)
    later = dead_owner(fleet, ["claim-00200"], unix=200.0)
    relative = f"models/claim-00100.pbrange/0-{GIB}"

    def attributed(*args, own_key: str = "", **kwargs):
        # (foreign claimed paths, taints, the evicted mover's own claimed
        # paths): a same-key claim is the claimed owner's own live copy.
        if claim == "foreign":
            return {relative}, [], set()
        return set(), [], ({relative} if own_key == claimed[1] else set())

    monkeypatch.setattr(stage_release, "_claimed_paths_attributed", attributed)

    first = _sweep(queue, stage, {TIER: 1})
    assert _owned(queue, *claimed) and not _owned(queue, *later), first
    assert _for(first, claimed[1]) == []
    # Through a (forced) skip checkpoint, the eviction's own census decides.
    monkeypatch.setattr(stage_release, "_skip_checkpoint_hit",
                        lambda *args: True)
    second = _sweep(queue, stage, {TIER: 1})
    assert _files(stage, ["claim-00100"]) == [True], second
    (receipt,) = _for(second, claimed[1])
    assert receipt["bytes_deleted"] == 0, receipt
    if claim == "foreign":
        # A claimed copy is about to land these bytes: the dead vouch goes
        # and the file stays, as for a charged orphan (#733).
        assert receipt["entries_shared"] == 1, receipt
        assert not _owned(queue, *claimed)
    else:
        assert receipt["declined"] == ["own"], receipt
        assert _owned(queue, *claimed)


def test_dead_co_owners_settle_through_shared(fleet):
    """The first to leave drops its vouch; the last deletes and covers."""

    queue, stage, _ = fleet
    first = dead_owner(fleet, ["shared-00001"], unix=100.0)
    second = dead_owner(fleet, ["shared-00001"], unix=200.0)

    receipts = _sweep(queue, stage, {TIER: 1})

    assert not _owned(queue, *first) and not _owned(queue, *second), receipts
    assert _files(stage, ["shared-00001"]) == [False]
    (one,), (two,) = _for(receipts, first[1]), _for(receipts, second[1])
    assert (one["entries_shared"], one["bytes_deleted"]) == (1, 0), one
    assert (two["entries_deleted"], two["bytes_deleted"]) == (1, GIB), two
    assert one["complete"] is True and two["complete"] is True


def test_a_live_co_owner_keeps_the_file(fleet):
    queue, stage, _ = fleet
    dead = dead_owner(fleet, ["live-00001"], unix=100.0)
    live_consumer, live_mover = base._key(), base._key()
    base._publish(queue, live_consumer, max_attempts=1)
    fragment = residency_map.validate_fragment(json.loads(
        residency_map.fragment_path(
            queue.residency_fragment_root(), *dead).read_text()))
    residency_map.write_fragment(queue.residency_fragment_root(), {
        **{key: value for key, value in fragment.items()
           if key not in ("consumer_action_key", "mover_action_key")},
        "consumer_action_key": live_consumer,
        "mover_action_key": live_mover})

    receipts = _sweep(queue, stage, {TIER: 1})

    assert not _owned(queue, *dead), receipts
    assert _files(stage, ["live-00001"]) == [True]
    (one,) = _for(receipts, dead[1])
    assert (one["entries_shared"], one["bytes_deleted"]) == (1, 0), one


def test_a_consumer_queued_again_is_not_evicted(fleet, monkeypatch):
    """The death proof is taken again under the consumer's lock."""

    queue, stage, _ = fleet
    consumer, mover = dead_owner(fleet, ["again-00001"], unix=100.0)
    discovered = stage_release.sweep_dead_owner_fragments
    revived: set[str] = set()

    def discover_then_revive(*args, **kwargs):
        receipts = discovered(*args, **kwargs)
        revived.add(consumer)
        return receipts

    real_live_state = stage_release.residency_plan.live_state

    def live_state(queue_, key):
        if key in revived:
            return "ready", ""
        return real_live_state(queue_, key)

    monkeypatch.setattr(stage_release, "sweep_dead_owner_fragments",
                        discover_then_revive)
    monkeypatch.setattr(stage_release.residency_plan, "live_state", live_state)

    receipts = _sweep(queue, stage, {TIER: 1})

    assert _owned(queue, consumer, mover), receipts
    assert _files(stage, ["again-00001"]) == [True]
    assert _for(receipts, mover) == []


def test_the_sweep_reports_the_uncharged_owners_count_and_bytes(fleet):
    queue, stage, _ = fleet
    small = dead_owner(fleet, ["report-00100"], unix=100.0)
    large = dead_owner(fleet, ["report-00200", "report-00201"], unix=200.0)

    (first,) = _reports(_sweep(queue, stage, {}))
    assert (first["owners"], first["bytes"]) == (2, 3 * GIB), first
    assert (first["owners_evicted"], first["bytes_evicted"]) == (0, 0)
    assert first["tier_id"] == TIER and first["stage_root"] == str(stage)
    assert first["pressure_gib"] == 0 and first["complete"] is True
    assert _reports(_sweep(queue, stage, {})) == [], "once per change"

    (second,) = _reports(_sweep(queue, stage, {TIER: 1}))
    assert not _owned(queue, *small) and _owned(queue, *large)
    assert (second["owners"], second["bytes"]) == (1, 2 * GIB), second
    assert (second["owners_evicted"], second["bytes_evicted"]) == (1, GIB)
    assert second["pressure_gib"] == 1
    assert _reports(_sweep(queue, stage, {})) == []

    (third,) = _reports(_sweep(queue, stage, {TIER: 5}))
    assert not _owned(queue, *large)
    assert (third["owners"], third["bytes"]) == (0, 0), third
    assert (third["owners_evicted"], third["bytes_evicted"]) == (1, 2 * GIB)
    assert _reports(_sweep(queue, stage, {TIER: 5})) == []


def test_no_report_for_a_tier_that_never_had_one(fleet):
    queue, stage, _ = fleet
    dead_owner(fleet, ["charged-00001"], unix=100.0, charged=True)

    assert _reports(_sweep(queue, stage, {})) == []


def test_an_unreadable_discovery_files_no_report_and_evicts_none(
        fleet, monkeypatch):
    queue, stage, _ = fleet
    owner = dead_owner(fleet, ["taint-00001"], unix=100.0)
    assert len(_reports(_sweep(queue, stage, {}))) == 1
    real = stage_release._fragment_census

    def tainted(root, index=None):
        fragments, _ = real(root, index)
        return fragments, ["fixture: unreadable fragment"]

    monkeypatch.setattr(stage_release, "_fragment_census", tainted)
    receipts = _sweep(queue, stage, {TIER: 1})

    assert _owned(queue, *owner), receipts
    assert _reports(receipts) == [], "unknown is not none"


def test_an_owner_found_and_evicted_in_one_pass_is_reported(fleet):
    queue, stage, _ = fleet
    owner = dead_owner(fleet, ["once-00001"], unix=100.0)

    (report,) = _reports(_sweep(queue, stage, {TIER: 1}))

    assert not _owned(queue, *owner)
    assert (report["owners"], report["bytes"]) == (0, 0), report
    assert (report["owners_evicted"], report["bytes_evicted"]) == (1, GIB)


def test_a_partially_pruned_owner_is_collected_from_its_rewritten_fragment(
        fleet):
    """A partial prune is not coherence: the next pass reads the survivors.

    One coherent path and one positively stale path (#853).  The first pass
    prunes the stale one and rewrites the fragment; the owner it read is no
    longer the one on disk, so it is neither evicted nor reported from that
    read.  The next pass collects the survivor alone.
    """

    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={red.NAMES[0]},
                                      charged=False)

    first = _sweep(queue, stage, {TIER: 1})
    (pruned,) = [entry for entry in first if entry.get("action_key") == mover
                 and entry.get("event") == stage_release.STALE_MENTION_EVENT]
    assert pruned["partial"] is True and pruned["entries_pruned"] == 1, pruned
    assert _for(first, mover) == [] and _reports(first) == [], first
    assert _owned(queue, consumer, mover)
    assert (stage / red.staged_name(red.NAMES[0])).exists()

    (report,) = _reports(_sweep(queue, stage, {}))
    assert (report["owners"], report["bytes"]) == (1, red.SIZE), report

    third = _sweep(queue, stage, {TIER: 1})
    (evicted,) = _for(third, mover)
    assert evicted["complete"] is True and evicted["bytes_deleted"] == red.SIZE
    assert not _owned(queue, consumer, mover)
    (report,) = _reports(third)
    assert (report["owners"], report["owners_evicted"],
            report["bytes_evicted"]) == (0, 1, red.SIZE), report


class _RefusingBudget:
    """A cycle budget (#1077) that lets every dead-owner unit start but one.

    Orphan evictions are units too (#1136); every one of them starts.
    ``started`` names the dead-owner units only.
    """

    KINDS = (stage_release.DEAD_OWNER_UNIT, stage_release.ORPHAN_EVICT_UNIT)

    def __init__(self, refused: str):
        self.refused = refused
        self.started: list[str] = []

    def order(self, kind, keys):
        assert kind in self.KINDS
        return list(keys)

    def start(self, kind, key):
        assert kind in self.KINDS
        if kind != stage_release.DEAD_OWNER_UNIT:
            return True
        if key == self.refused:
            return False
        self.started.append(key)
        return True

    def done(self, kind):
        assert kind in self.KINDS


def test_a_consumer_the_budget_did_not_reach_withholds_its_tiers_report(fleet):
    """What the pass did not examine is not a census.

    With one consumer deferred, the tier's collection holds only the other
    owner.  Reporting it would announce a drop that never happened; the
    tier's report waits for a pass that reaches both, and pressure still
    evicts the owner the pass did prove.
    """

    queue, stage, _ = fleet
    reached = dead_owner(fleet, ["budget-00100"], unix=100.0)
    deferred = dead_owner(fleet, ["budget-00200"], unix=200.0)
    budget = _RefusingBudget(deferred[0])

    quiet = stage_release.sweep(queue, stage_roots={TIER: str(stage)},
                                pressure={}, budget=budget)
    assert budget.started == [reached[0]], budget.started
    assert _reports(quiet) == [], "a partial pass is not a census"
    assert _owned(queue, *reached) and _owned(queue, *deferred)

    pressed = stage_release.sweep(queue, stage_roots={TIER: str(stage)},
                                  pressure={TIER: 1},
                                  budget=_RefusingBudget(deferred[0]))
    assert not _owned(queue, *reached) and _owned(queue, *deferred), pressed
    assert [r["action_key"] for r in _for(pressed, reached[1])] == [
        reached[1]]
    assert _reports(pressed) == []

    (full,) = _reports(_sweep(queue, stage, {}))
    assert (full["owners"], full["bytes"]) == (1, GIB), full

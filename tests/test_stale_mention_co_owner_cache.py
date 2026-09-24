"""A co-owned dead owner is censused once, then skipped until a document moves (#1056).

The live shape on 2026-09-24: 35 to 47 dead owners, most of them co-owning
each other's files after an adoption, each re-censused by the stale-mention
prune on every tier-loop cycle.  The co-owner verdict changed nothing and
cached nothing, so every cycle paid a full ownership census and a stage lock
hold per owner: 1.85 s each, 56 s per cycle at the median, 108.6 s at most.

The verdict depends only on documents the skip checkpoint can fence: this
owner's fragment and material, the co-owner fragments that protect its
files, and the parent directories of its paths.  These tests hold the cache
to that: an unchanged co-owned owner is censused once and then skipped, and
removing or rewriting a co-owner's fragment, rewriting this owner's own
fragment, or changing a path's directory re-runs the census on the next
pass, where the co-owner's removal can prune or evict.

The same checkpoint stands for an owner the transaction retains for another
reason (a taint, a live pin, a promotion handoff) when its paths are
otherwise idle: the checkpoint certifies the path classification, and a skip
only ever retains.  An owner with a stale path is never cached that way.

The bounds are held to the live shape too: since #889 every staged entry has
its own ``.pbrange`` parent directory, so an owner of 2,575 entries needs
2,575 directory stamps, and forty owners of 1,680 entries must fit together.
The cache holds one checkpoint per owner the sweep discovers, forgets the
owners it no longer discovers, and refuses a newcomer when full rather than
evicting an owner it will visit again.
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
from test_stale_material_done_owner_retires import fleet  # noqa: E402,F401
from prismabuild import reader_lease, residency_map  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

NAMES = red.NAMES
SIZE = red.SIZE
TIER = red.TIER


@pytest.fixture(autouse=True)
def _fresh_checkpoints():
    stage_release.reset_skip_checkpoints()
    yield
    stage_release.reset_skip_checkpoints()


def _path(stage: Path, name: str) -> Path:
    return stage / red.staged_name(name)


def _fragment_entries(material_entries: dict) -> dict:
    return {key: {"stage_path": entry["stage_path"], "bytes": entry["bytes"],
                  "sha256": entry["sha256"], "offset": 0}
            for key, entry in material_entries.items()}


def _co_owner(fleet, names) -> tuple[str, str]:
    """Another dead owner whose documents date ``names``' current files.

    A failed consumer's executed DONE mover, charged, with a fragment and a
    material sidecar naming the same staged paths as the owner under test:
    the pair an adoption leaves behind once both consumers have died.
    """

    queue, stage, _ = fleet
    consumer, _generation = base._fail_consumer(queue)
    mover = base._key()
    base._publish(queue, mover, max_attempts=1)
    wanted = {str(_path(stage, name)) for name in names}
    entries = {key: mention for key, mention in red._entries(stage).items()
               if mention["stage_path"] in wanted}
    red._write_sidecar(queue, stage, consumer, mover, entries)
    _write_fragment(queue, stage, consumer, mover, _fragment_entries(entries))
    queue.finish(mover, status="executed", detail={"returncode": 0})
    # The owner under test holds the one token its fixture minted; the mint
    # sets the tier's capacity, so this owner's token needs a second one.
    queue.mint_tier_capacity(TIER, {"stage_gib": 2})
    assert queue.tier_ledger(TIER).acquire(mover, {"stage_gib": 1}) is True
    return consumer, mover


def _write_fragment(queue, stage, consumer, mover, entries) -> None:
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64, "entries": entries})


def _rewrite_fragment(queue, consumer, mover) -> None:
    """The same document filed again: a new inode, the same content."""

    root = queue.residency_fragment_root()
    path = residency_map.fragment_path(root, consumer, mover)
    residency_map.write_fragment(root, json.loads(path.read_text()))


def _drop_owner(queue, consumer, mover) -> None:
    """What an owner's egress leaves: no fragment and no material."""

    root = queue.residency_fragment_root()
    residency_map.fragment_path(root, consumer, mover).unlink()
    reader_lease.material_path(root, consumer, mover).unlink()


def _pass(queue, stage, index=None):
    return stage_release.sweep_dead_owner_fragments(
        queue, stage_roots={TIER: str(stage)},
        residency_root=queue.residency_fragment_root(), index=index)


def _receipts_for(receipts, mover: str) -> list[dict]:
    return [entry for entry in receipts
            if entry.get("event") == stage_release.STALE_MENTION_EVENT
            and entry.get("action_key") == mover]


def _one(receipts, mover: str) -> dict:
    found = _receipts_for(receipts, mover)
    assert len(found) == 1, receipts
    return found[0]


def _checkpoint_key(queue, stage, consumer, mover) -> tuple:
    return stage_release._skip_checkpoint_key(
        queue, queue.residency_fragment_root(), stage, TIER, consumer, mover)


def _count_destination_lstats(monkeypatch, paths) -> list[int]:
    watched = {os.path.normpath(str(path)) for path in paths}
    seen = [0]
    real = os.lstat

    def counting(path, *args, **kwargs):
        try:
            name = os.path.normpath(os.fspath(path))
        except TypeError:
            name = ""
        if name in watched:
            seen[0] += 1
        return real(path, *args, **kwargs)

    monkeypatch.setattr(os, "lstat", counting)
    return seen


# --------------------------------------------------------------------------
# The co-owner verdict is cached, and every input it read invalidates it
# --------------------------------------------------------------------------

def test_a_co_owned_owner_is_censused_once_then_skipped(fleet, monkeypatch):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                      replace=False)
    co_consumer, co_mover = _co_owner(fleet, NAMES)
    destinations = [_path(stage, name) for name in NAMES]
    seen = _count_destination_lstats(monkeypatch, destinations)

    first = _pass(queue, stage)

    for owner in (mover, co_mover):
        receipt = _one(first, owner)
        assert receipt["retained_reason"] == "co-owner", receipt
        assert receipt["entries_pruned"] == 0
        assert receipt["cacheable"] is True, (
            "an unchanged co-owned owner must install its skip checkpoint")
    assert seen[0] >= len(destinations)

    seen[0] = 0
    second = _pass(queue, stage)

    assert _receipts_for(second, mover) == [], (
        "a co-owned owner on an unchanged stage was censused again")
    assert _receipts_for(second, co_mover) == [], second
    assert seen[0] == 0, "a skipped owner must not scan its entries"
    assert all(path.exists() for path in destinations)


def test_removing_the_co_owner_reruns_the_census_and_prunes(fleet):
    queue, stage, _ = fleet
    # NAMES[1] was replaced under the owner: its mention is stale, and only
    # the co-owner's fragment protects the file.
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    co_consumer, co_mover = _co_owner(fleet, [NAMES[1]])

    first = _pass(queue, stage)
    assert _one(first, mover)["retained_reason"] == "co-owner"
    assert _one(first, mover)["cacheable"] is True
    assert _receipts_for(_pass(queue, stage), mover) == []

    _drop_owner(queue, co_consumer, co_mover)
    third = _pass(queue, stage)

    receipt = _one(third, mover)
    assert receipt["partial"] is True and receipt["entries_pruned"] == 1, (
        "the co-owner's removal must lead to the prune it was blocking: "
        f"{receipt}")
    assert not _path(stage, NAMES[1]).exists()
    assert _path(stage, NAMES[0]).exists()


def test_removing_the_co_owner_can_evict_the_whole_owner(fleet):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet)      # every path replaced
    co_consumer, co_mover = _co_owner(fleet, NAMES)
    root = queue.residency_fragment_root()

    assert _one(_pass(queue, stage), mover)["retained_reason"] == "co-owner"
    assert _receipts_for(_pass(queue, stage), mover) == []

    _drop_owner(queue, co_consumer, co_mover)
    third = _pass(queue, stage)

    assert [entry for entry in third if entry.get("action_key") == mover
            and entry.get("complete") is True], third
    assert not residency_map.fragment_path(root, consumer, mover).exists()
    assert not any(_path(stage, name).exists() for name in NAMES)
    assert mover not in queue.tier_ledger(TIER).held_keys()


def test_rewriting_the_co_owner_fragment_reruns_the_census(fleet):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                      replace=False)
    co_consumer, co_mover = _co_owner(fleet, NAMES)
    _pass(queue, stage)
    assert _receipts_for(_pass(queue, stage), mover) == []

    _rewrite_fragment(queue, co_consumer, co_mover)
    third = _pass(queue, stage)

    assert _one(third, mover)["retained_reason"] == "co-owner", (
        "a rewritten co-owner fragment must re-run this owner's census")


def test_rewriting_this_owners_fragment_reruns_the_census(fleet):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                      replace=False)
    co_consumer, co_mover = _co_owner(fleet, NAMES)
    _pass(queue, stage)
    assert _receipts_for(_pass(queue, stage), mover) == []

    _rewrite_fragment(queue, consumer, mover)
    third = _pass(queue, stage)

    assert _one(third, mover)["retained_reason"] == "co-owner"
    # The rewritten fragment is also a co-owner document of the other
    # owner, whose checkpoint fences it: that owner is censused again too.
    assert _one(third, co_mover)["retained_reason"] == "co-owner"
    assert _receipts_for(_pass(queue, stage), mover) == [], (
        "the census after the rewrite installs a fresh checkpoint")


def test_a_removed_path_under_a_co_owned_owner_reruns_the_census(fleet):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                      replace=False)
    _co_owner(fleet, [NAMES[1]])
    _pass(queue, stage)
    assert _receipts_for(_pass(queue, stage), mover) == []

    os.unlink(_path(stage, NAMES[0]))
    third = _pass(queue, stage)

    receipt = _one(third, mover)
    assert receipt["entries_already_absent"] == 1 and receipt["partial"], receipt


# --------------------------------------------------------------------------
# Owners retained for another reason
# --------------------------------------------------------------------------

def test_a_tainted_census_still_caches_an_idle_owner(fleet, monkeypatch):
    """The live taint: a claimed export that seals no range (#1056)."""

    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                      replace=False)
    _co_owner(fleet, NAMES)
    monkeypatch.setattr(
        stage_release, "_claimed_paths_attributed",
        lambda *args, **kwargs: (set(), ["46da18322b2d: mover seals no range"],
                                 set()))

    receipt = _one(_pass(queue, stage), mover)
    assert receipt["retained_reason"] == "ownership-uncertain", receipt
    assert receipt["errors"] == [
        "ownership uncertain: 46da18322b2d: mover seals no range"]
    assert receipt["cacheable"] is True, (
        "a taint retains, and so does a skip: an idle owner caches")
    assert _receipts_for(_pass(queue, stage), mover) == []

    monkeypatch.undo()
    assert _receipts_for(_pass(queue, stage), mover) == [], (
        "the taint ending changes nothing the checkpoint stands for")


def test_a_live_pin_on_an_idle_owner_caches(fleet):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                      replace=False)
    pin = reader_lease.acquire(
        queue, consumer_action_key=consumer,
        attempt={"nonce": "n1", "scope_id": "s1"}, tier_id=TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": SIZE},
        holder={"host": "fixture", "pid": os.getpid()}, acquire_token="p1",
        covers=[{"mover_action_key": mover, "manifest_sha256": "a" * 64}])
    assert pin["ok"], pin

    receipt = _one(_pass(queue, stage), mover)
    assert receipt["retained_reason"] == "live-pin", receipt
    assert receipt["cacheable"] is True
    assert _receipts_for(_pass(queue, stage), mover) == []

    reader_lease.release(queue, pin["pin_id"], pin["ref_id"],
                         consumer_action_key=consumer)
    assert _receipts_for(_pass(queue, stage), mover) == []
    assert all(_path(stage, name).exists() for name in NAMES)


def test_a_promotion_handoff_on_an_idle_owner_caches(fleet, monkeypatch):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                      replace=False)
    handoff = os.path.normpath(str(_path(stage, NAMES[1]).resolve()))
    monkeypatch.setattr(stage_release, "_claimed_source_paths",
                        lambda *args, **kwargs: ({handoff}, []))

    receipt = _one(_pass(queue, stage), mover)
    assert receipt["retained_reason"] == "promotion-handoff", receipt
    assert receipt["cacheable"] is True
    assert _receipts_for(_pass(queue, stage), mover) == []


def test_a_retained_owner_with_a_stale_path_is_never_cached(fleet, monkeypatch):
    """A skip must never hide a prune the retention is only postponing."""

    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    monkeypatch.setattr(
        stage_release, "_claimed_paths_attributed",
        lambda *args, **kwargs: (set(), ["46da18322b2d: mover seals no range"],
                                 set()))

    first = _one(_pass(queue, stage), mover)
    assert first["retained_reason"] == "ownership-uncertain"
    assert first["cacheable"] is False, first
    assert _one(_pass(queue, stage), mover)["cacheable"] is False

    monkeypatch.undo()
    pruned = _one(_pass(queue, stage), mover)
    assert pruned["partial"] is True and pruned["entries_pruned"] == 1, pruned


# --------------------------------------------------------------------------
# The cache holds the live shape and follows the owners the sweep sees
# --------------------------------------------------------------------------

def _stamps(prefix: str, count: int) -> dict[str, tuple[int, int, int, int]]:
    # A live parent is ``<stage>/models/<model>/<shard>.pbrange``: 242 bytes
    # on average on 2026-09-24.
    stem = f"/stage/prewarm/models/{prefix}/" + "m" * 180
    return {f"{stem}-{index:06d}-of-000120.safetensors.pbrange": (1, index, 2, 3)
            for index in range(count)}


def test_the_checkpoint_holds_the_live_owner_shape():
    """Forty-one discovered owners, one of 2,575 entries, all fit together."""

    owners = [("big",)] + [("owner", index) for index in range(40)]
    # The sweep names the owners it discovered before it consults the cache
    # (#1056); main has no such step and caches under its fixed literals.
    discovered = getattr(stage_release, "_retain_skip_checkpoints", None)
    if discovered is not None:
        discovered(owners)
    fragment, material = (1, 2, 3, 4, 5), (1, 2, 3, 4, 6)
    for key in owners:
        count = 2575 if key == ("big",) else 1680
        assert stage_release._install_skip_checkpoint(
            key, fragment, material, _stamps(str(key), count),
            documents={}), (
            f"{key}: a discovered owner of {count} entries, one parent "
            f"directory each, must fit")
    assert set(stage_release._skip_checkpoints) == set(owners)


def test_overflow_refuses_and_the_refused_owner_is_still_censused(
        fleet, monkeypatch):
    """A full cache refuses the newcomer; it never evicts to make room.

    The sweep visits its owners in one order every cycle, so evicting the
    oldest entry to admit the newest evicts, in turn, every owner before it
    is visited again, and no owner ever hits.  The refused owner takes the
    uncached census every pass, which is slower, never weaker.
    """

    queue, stage, _ = fleet
    _consumer_a, mover_a = red.stale_owner(fleet, coherent_names=set(NAMES),
                                           replace=False)
    _consumer_b, mover_b = _co_owner(fleet, NAMES)
    # Room for one checkpoint: the fixed literal on main, the discovered
    # count (#1056) here.
    monkeypatch.setattr(stage_release, "SKIP_CHECKPOINT_MAX_ENTRIES", 1,
                        raising=False)
    monkeypatch.setattr(stage_release, "_skip_checkpoint_capacity",
                        lambda discovered: 1, raising=False)

    first = _pass(queue, stage)
    cached = [mover for mover in (mover_a, mover_b)
              if _one(first, mover)["cacheable"] is True]
    assert len(cached) == 1, first
    refused = mover_b if cached == [mover_a] else mover_a

    second = _pass(queue, stage)
    assert _receipts_for(second, cached[0]) == [], (
        "the owner the cache holds must keep its skip")
    again = _one(second, refused)
    assert again["cacheable"] is False, (
        "the refused owner must not evict the cached one")
    assert _receipts_for(_pass(queue, stage), cached[0]) == []


def test_a_checkpoint_installed_under_a_taint_sees_a_replaced_leaf(
        fleet, monkeypatch):
    """The directory stamp still bites after a retain-reason install."""

    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                      replace=False)
    monkeypatch.setattr(
        stage_release, "_claimed_paths_attributed",
        lambda *args, **kwargs: (set(), ["46da18322b2d: mover seals no range"],
                                 set()))
    assert _one(_pass(queue, stage), mover)["cacheable"] is True
    assert _receipts_for(_pass(queue, stage), mover) == []

    replacement = red._stage(stage, NAMES[1] + ".later", red.NEW_PAYLOAD)
    os.replace(replacement, _path(stage, NAMES[1]))
    tainted = _one(_pass(queue, stage), mover)
    assert tainted["retained_reason"] == "ownership-uncertain", tainted
    assert tainted["cacheable"] is False, (
        "a stale path under a taint is retained, never cached")

    monkeypatch.undo()
    pruned = _one(_pass(queue, stage), mover)
    assert pruned["partial"] is True and pruned["entries_pruned"] == 1, pruned
    assert not _path(stage, NAMES[1]).exists()
    assert _path(stage, NAMES[0]).exists()


def test_a_sweep_forgets_the_checkpoint_of_an_owner_that_left(fleet):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                      replace=False)
    other_consumer, other_mover = _co_owner(fleet, [NAMES[1]])
    _pass(queue, stage)
    key = _checkpoint_key(queue, stage, other_consumer, other_mover)
    assert key in stage_release._skip_checkpoints

    _drop_owner(queue, other_consumer, other_mover)
    _pass(queue, stage)

    assert key not in stage_release._skip_checkpoints, (
        "a checkpoint must not outlive the owner the sweep discovers")
    assert _checkpoint_key(queue, stage, consumer, mover) in (
        stage_release._skip_checkpoints)


def test_the_cycle_line_counts_skips_and_censuses(fleet):
    queue, stage, _ = fleet
    red.stale_owner(fleet, coherent_names=set(NAMES), replace=False)
    _co_owner(fleet, NAMES)
    cache = tier_loop.ReceiptCache()

    before = tier_loop._read_counts(cache)
    _pass(queue, stage, index=cache.census)
    middle = tier_loop._read_counts(cache)
    _pass(queue, stage, index=cache.census)
    after = tier_loop._read_counts(cache)

    assert middle["census_stale_censused"] - before.get(
        "census_stale_censused", 0) == 2, middle
    assert middle["census_stale_skipped"] - before.get(
        "census_stale_skipped", 0) == 0, middle
    assert after["census_stale_skipped"] - middle["census_stale_skipped"] == 2
    assert after["census_stale_censused"] == middle["census_stale_censused"]

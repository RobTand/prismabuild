"""The publication gate reuses a claimed mover's derived paths (#1089).

``stage_move._StagedPublisher._live_claim_cover`` called
``stage_release._claimed_paths`` with no ``memo=``, unlike every other caller
of that function (the orphaned-range recovery, the egress ownership census).
So every claim-cover check -- once per clean entry at content adoption, once
per publish poll while a name waits, once per divergence invalidation -- fully
re-derived every claimed range mover's staged paths from its sealed request
and its data manifest, even when nothing about the claimed set had changed
since the last check. The cost of one check grows with the entries of every
claimed mover; the cost of a whole range grows with that times the range's
own entries.

``stage_release._CensusMemo.claims`` already exists to make this safe: a
claimed range mover's *derived* stage paths depend only on its sealed
request and its data manifest, both immutable under their digests, so once
derived for a claim key the answer can never go stale. What must stay fresh,
by the memo's own contract (see ``_CensusMemo``'s and
``_claimed_paths_attributed``'s docstrings), is the claim *listing* and each
claim *record*: both are read straight off disk on every call, before the
memo is ever consulted, so a claim that appears, ends or has its resources
rewritten is seen at once. Only the per-entry path derivation for a claim
this memo has already resolved is skipped on a repeat. That is exactly why
one memo may live for a whole ``_StagedPublisher`` run -- across every
destination it publishes, not just one -- instead of one per call.

These tests hold the fix to that contract from both sides: the claimed-set
work must collapse from O(checks x entries) to O(entries) when the claimed
set does not change, and a claim that appears or ends between two checks
must still be seen by the second one.

Every fixture is a temp queue/stage/CAS registered fresh per test (via
``test_dead_owner_fragment_blocks_then_retires.fleet``), never the live
queue, stage or CAS.
"""
from __future__ import annotations

import hashlib
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
from test_dead_owner_fragment_blocks_then_retires import fleet  # noqa: E402,F401
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

TIER = base.TIER
SIZE = base.SIZE
MOUNT_PREFIX = "/mnt/shared"


def _claim_range(queue: pool.PoolQueue, cas: Path, key: str,
                 entries: list[dict[str, object]], start: int, end: int,
                 ) -> None:
    """Seal one claimed range mover: request + manifest blob + claim row.

    Shaped the way a real range mover's claim is sealed (see
    ``_write_claimed_copy_shape`` in
    ``test_a_shared_staged_path_has_two_owners.py``): the request names the
    range through ``--range-start-bytes``/``--range-end-bytes`` and the
    manifest through ``PBCAMPAIGN_DATA_MANIFEST_INPUT_ID``, both read fresh
    by ``_claimed_paths`` on every call regardless of any memo.
    """

    manifest = {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {}, "annotations": {},
        "mount_prefix": MOUNT_PREFIX,
        "entries": entries,
        "entry_count": len(entries),
        "total_bytes": sum(int(entry["bytes"]) for entry in entries),
    }
    blob = json.dumps(manifest).encode("utf-8")
    digest = hashlib.sha256(blob).hexdigest()
    shard = cas / "blobs" / digest[:2]
    shard.mkdir(parents=True, exist_ok=True)
    (shard / digest).write_bytes(blob)
    request = {
        "action_key": key,
        "params": {"command": ["python3", "stage_move.py",
                               "--range-start-bytes", str(start),
                               "--range-end-bytes", str(end)]},
        "inputs": [{"id": pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID,
                    "sha256": digest, "bytes": len(blob)}],
    }
    shard = cas / "requests" / key[:2]
    shard.mkdir(parents=True, exist_ok=True)
    (shard / f"{key}.json").write_text(json.dumps(request))
    claimed_dir = queue.dir(pool.CLAIMED)
    claimed_dir.mkdir(parents=True, exist_ok=True)
    record = {
        "action_key": key,
        "resources": {"cpu": 2, "mem_gb": 1, f"stage_gib@{TIER}": 1},
        "cas_root": str(cas),
    }
    (claimed_dir / f"{key}.json").write_text(json.dumps(record))


def _publisher(queue: pool.PoolQueue, stage: Path, cas: Path,
              mover: str) -> "stage_move._StagedPublisher":
    return stage_move._StagedPublisher(
        queue=queue, stage_root=stage, residency_root=queue.root / pool.RESIDENCY,
        mover_action_key=mover, manifest_sha256="a" * 64,
        tier_id=TIER, cas_root=str(cas))


def _norm(stage: Path, path: str, offset: int, size: int) -> str:
    relative = stage_move.stage_relative(path, offset, size,
                                         mount_prefix=MOUNT_PREFIX)
    return os.path.normpath(os.path.join(str(stage), relative))


# ---------------------------------------------------------------------------
# The cost: an unchanged claimed set must be derived once, not once per check
# ---------------------------------------------------------------------------

def test_an_unchanged_claim_is_derived_once_across_many_checks(
        fleet, monkeypatch) -> None:
    """O(checks x entries) collapses to O(entries) when nothing changes.

    ``stage_release.stage_relative`` runs exactly once per manifest entry
    inside a derivation that actually resolves a claim's window (the memo
    skips the whole window computation on a hit), so counting its calls
    across repeated, unchanged checks is the direct measure of how many
    times the derivation itself ran.
    """

    queue, stage, cas = fleet
    own_mover = base._key()
    other_mover = base._key()
    entries = [{"path": f"{MOUNT_PREFIX}/model/part-{index}.bin",
               "offset": 0, "bytes": SIZE, "sha256": None}
              for index in range(3)]
    _claim_range(queue, cas, other_mover, entries, 0, len(entries) * SIZE)
    publisher = _publisher(queue, stage, cas, own_mover)
    norm = _norm(stage, entries[1]["path"], 0, SIZE)

    derivations: list[int] = []
    real_stage_relative = stage_release.stage_relative

    def counting(*args, **kwargs):
        derivations.append(1)
        return real_stage_relative(*args, **kwargs)

    monkeypatch.setattr(stage_release, "stage_relative", counting)

    checks = 5
    for _ in range(checks):
        cover, detail = publisher._live_claim_cover(norm)
        assert cover is True, detail

    # Fixed: one derivation of the unchanged claim's three entries.  Unfixed
    # (no memo passed), this is ``checks * len(entries)`` == 15.
    assert len(derivations) == len(entries), (
        f"expected {len(entries)} stage_relative calls (one derivation) "
        f"across {checks} unchanged claim-cover checks, saw "
        f"{len(derivations)} -- the claim is being re-derived on every check")


# ---------------------------------------------------------------------------
# Correctness: the memo must never hide a claim that appears, changes or ends
# ---------------------------------------------------------------------------

def test_a_claim_that_appears_between_checks_is_seen_at_once(fleet) -> None:
    queue, stage, cas = fleet
    own_mover = base._key()
    other_mover = base._key()
    entry = {"path": f"{MOUNT_PREFIX}/model/part.bin", "offset": 0,
             "bytes": SIZE, "sha256": None}
    publisher = _publisher(queue, stage, cas, own_mover)
    norm = _norm(stage, entry["path"], 0, SIZE)

    before, detail = publisher._live_claim_cover(norm)
    assert before is False, detail

    _claim_range(queue, cas, other_mover, [entry], 0, SIZE)

    after, detail = publisher._live_claim_cover(norm)
    assert after is True, (
        f"a claim sealed after the first check must still be seen by the "
        f"second: {detail}")


def test_a_claim_that_ends_between_checks_is_seen_at_once(fleet) -> None:
    queue, stage, cas = fleet
    own_mover = base._key()
    other_mover = base._key()
    entry = {"path": f"{MOUNT_PREFIX}/model/part.bin", "offset": 0,
             "bytes": SIZE, "sha256": None}
    _claim_range(queue, cas, other_mover, [entry], 0, SIZE)
    publisher = _publisher(queue, stage, cas, own_mover)
    norm = _norm(stage, entry["path"], 0, SIZE)

    before, detail = publisher._live_claim_cover(norm)
    assert before is True, detail

    (queue.dir(pool.CLAIMED) / f"{other_mover}.json").unlink()

    after, detail = publisher._live_claim_cover(norm)
    assert after is False, (
        f"a claim that ended after the first check must not still cover "
        f"the name on the second: {detail}")


def test_a_second_unrelated_claim_does_not_disturb_the_first(fleet) -> None:
    """Two distinct claim keys memoize independently (#1089's per-key cache)."""

    queue, stage, cas = fleet
    own_mover = base._key()
    first_mover = base._key()
    second_mover = base._key()
    first_entry = {"path": f"{MOUNT_PREFIX}/model/first.bin", "offset": 0,
                   "bytes": SIZE, "sha256": None}
    second_entry = {"path": f"{MOUNT_PREFIX}/model/second.bin", "offset": 0,
                    "bytes": SIZE, "sha256": None}
    _claim_range(queue, cas, first_mover, [first_entry], 0, SIZE)
    publisher = _publisher(queue, stage, cas, own_mover)
    first_norm = _norm(stage, first_entry["path"], 0, SIZE)
    second_norm = _norm(stage, second_entry["path"], 0, SIZE)

    first_before, detail = publisher._live_claim_cover(first_norm)
    assert first_before is True, detail
    second_before, detail = publisher._live_claim_cover(second_norm)
    assert second_before is False, detail

    _claim_range(queue, cas, second_mover, [second_entry], 0, SIZE)

    first_after, detail = publisher._live_claim_cover(first_norm)
    assert first_after is True, detail
    second_after, detail = publisher._live_claim_cover(second_norm)
    assert second_after is True, detail

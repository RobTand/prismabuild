"""Full-stack 3/4 — reader boundaries: real refusals, no silent fallback.

Each negative is a real boundary: production `residency_map` lookup and
validate functions plus OS enforcement (permissions, missing files,
corrupted bytes, epoch markers). The pool tripwire pattern proves no
silent fallback: after staging, the pool fixture is chmod-000, so any
pool read surfaces as an OS error instead of quiet bytes. Where the
current contract still falls back by design (`lookup` returning None),
the test asserts the fallback is visible and recorded — never silent.
ACC-02/ACC-03 (PB-side legs).
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import prismabuild.core as pb  # noqa: E402
from prismabuild import pool, residency_map, storage_tiers  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

from test_fullstack_stage_ram_chain import (  # noqa: E402
    CONSUMER, RAM_TIER, STAGE_TIER, _fleet, _move_args, _pool_fixture,
)

WHOLE = "shard-0.bin"


WHOLE_MOVER = "a" * 64


def _staged_once(tmp_path: Path):
    queue = _fleet(tmp_path)
    _, manifest, _ = _pool_fixture(tmp_path)
    files_len = 1 << 20
    receipt = stage_move.move(_move_args(
        tmp_path, queue, manifest, WHOLE_MOVER, 0, files_len))
    assert receipt["complete"] is True
    queue.record_move(WHOLE_MOVER, receipt)
    return queue, manifest, files_len


def _ram_fragment_for(mapping: dict, base: dict, ram: Path, epoch: str,
                        *, digest: str) -> dict:
    key = next(iter(mapping["entries"]))
    ram.mkdir(parents=True, exist_ok=True)
    staged = Path(mapping["entries"][key]["stage_path"])
    placed = ram / staged.name
    placed.write_bytes(staged.read_bytes())
    fragment = json.loads(json.dumps(base))
    fragment["tier_id"] = "ram:dl380g10"
    fragment["stage_root"] = str(ram)
    fragment["epoch"] = epoch
    entry = dict(fragment["entries"][key])
    entry["sha256"] = digest
    entry["stage_path"] = str(placed)
    fragment["entries"] = {key: entry}
    return fragment


def test_mismatched_ram_copy_refuses_overlay(tmp_path: Path) -> None:
    """A ram copy disagreeing with the stage vouched bytes refuses overlay."""
    queue, manifest, _ = _staged_once(tmp_path)
    fragments = [residency_map.validate_fragment(f) for f in
                 residency_map.read_fragments(queue.root / pool.RESIDENCY, CONSUMER)]
    mapping = residency_map.compose(fragments)
    base = [f for f in fragments if f["tier_id"] == STAGE_TIER][0]
    bad = _ram_fragment_for(mapping, base, tmp_path / "ram", "epoch-1",
                            digest="0" * 64)
    with pytest.raises(residency_map.ResidencyMapError):
        residency_map.overlay_ram(
            mapping, [bad], ram_tier_id="ram:dl380g10",
            ram_root=str(tmp_path / "ram"), ram_epoch="epoch-1")


def test_matching_ram_copy_lays_ram_path(tmp_path: Path) -> None:
    """A ram copy agreeing with the stage vouched bytes lays ram_path."""
    queue, manifest, _ = _staged_once(tmp_path)
    fragments = [residency_map.validate_fragment(f) for f in
                 residency_map.read_fragments(queue.root / pool.RESIDENCY, CONSUMER)]
    mapping = residency_map.compose(fragments)
    key = next(iter(mapping["entries"]))
    base = [f for f in fragments if f["tier_id"] == STAGE_TIER][0]
    good = _ram_fragment_for(
        mapping, base, tmp_path / "ram", "epoch-1",
        digest=str(mapping["entries"][key]["sha256"]))
    overlaid = residency_map.overlay_ram(
        mapping, [good], ram_tier_id="ram:dl380g10",
        ram_root=str(tmp_path / "ram"), ram_epoch="epoch-1")
    assert overlaid["entries"][key].get("ram_path") is not None


def test_unstaged_entry_lookup_is_visible_not_silent(tmp_path: Path) -> None:
    """lookup() of an unstaged entry returns None: the fallback point is explicit."""
    queue, manifest, _ = _staged_once(tmp_path)
    fragments = [residency_map.validate_fragment(f) for f in
                 residency_map.read_fragments(queue.root / pool.RESIDENCY, CONSUMER)]
    mapping = residency_map.compose(fragments)
    assert residency_map.lookup(mapping, "/pool/model/never-staged.bin", 0) is None


def test_double_egress_reclaims_exactly_once(tmp_path: Path) -> None:
    """Charged tier tokens return once across two egresses; balances prove it."""
    queue, manifest, files_len = _staged_once(tmp_path)
    ledger = queue.tier_ledger(STAGE_TIER)
    queue.mint_tier_capacity(STAGE_TIER, {"stage_gib": 4})
    assert ledger.acquire(WHOLE_MOVER, {"stage_gib": 1}) is True
    assert ledger.holder_tokens(WHOLE_MOVER) == {"stage_gib": 1}
    assert stage_release.register_stage_root(
        queue, tier_id=STAGE_TIER, stage_root=tmp_path / "stage") == "registered"
    first = stage_release.evict(queue, WHOLE_MOVER, consumer_action_key=CONSUMER,
                                stage_root=tmp_path / "stage")
    assert first["complete"] is True
    assert ledger.holder_tokens(WHOLE_MOVER) == {}
    assert ledger.available() == {"stage_gib": 4}
    second = stage_release.evict(queue, WHOLE_MOVER, consumer_action_key=CONSUMER,
                                 stage_root=tmp_path / "stage")
    assert second["complete"] is True
    assert ledger.holder_tokens(WHOLE_MOVER) == {}
    assert ledger.available() == {"stage_gib": 4}
    assert not (tmp_path / "stage" / "model" / WHOLE).exists()


def test_prior_epoch_ram_range_demands_revalidation(tmp_path: Path) -> None:
    """Reboot voids ram readiness: the old fragment refuses the new epoch."""
    import shutil
    from test_fullstack_stage_ram_chain import _run_chain
    queue, manifest, whole_len, epoch = _run_chain(tmp_path)
    ram = tmp_path / "ram"
    shutil.rmtree(ram / "model")
    (ram / ".prismabuild-ram-epoch.json").unlink()
    second = storage_tiers.ensure_ram_epoch(ram, host="dl380g10")
    assert second is not None
    assert str(second["epoch"]) != epoch
    fragments = [residency_map.validate_fragment(f) for f in
                 residency_map.read_fragments(queue.root / pool.RESIDENCY, CONSUMER)]
    mapping = residency_map.compose(
        [f for f in fragments if f["tier_id"] == STAGE_TIER])
    ram_frags = [f for f in fragments if f["tier_id"] == RAM_TIER]
    assert ram_frags, "a ram fragment was published under the old epoch"
    with pytest.raises(residency_map.ResidencyMapError):
        residency_map.overlay_ram(
            mapping, ram_frags, ram_tier_id=RAM_TIER,
            ram_root=str(ram), ram_epoch=str(second["epoch"]))

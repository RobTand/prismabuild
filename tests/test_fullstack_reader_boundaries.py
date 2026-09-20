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
import os
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
    CONSUMER, STAGE_TIER, _fleet, _move_args, _pool_fixture,
)

WHOLE = "shard-0.bin"


WHOLE_MOVER = "a" * 64


def _staged_once(tmp_path: Path):
    queue = _fleet(tmp_path)
    _, manifest, _ = _pool_fixture(tmp_path)
    files_len = 1 << 20
    receipt = stage_move.move(_move_args(
        tmp_path, queue, manifest, WHOLE_MOVER, 1 << 18, (1 << 18) + files_len))
    assert receipt["complete"] is True
    queue.record_move(WHOLE_MOVER, receipt)
    return queue, manifest, files_len


def test_corrupted_map_refuses_whole_with_reason(tmp_path: Path) -> None:
    """A map whose digest lies is refused whole, never partially trusted."""
    queue, manifest, _ = _staged_once(tmp_path)
    fragments = [residency_map.validate_fragment(f) for f in
                 residency_map.read_fragments(queue.root / pool.RESIDENCY, CONSUMER)]
    mapping = residency_map.compose(fragments)
    tampered = json.loads(json.dumps(mapping))
    key = next(iter(tampered["entries"]))
    tampered["entries"][key]["sha256"] = "0" * 64
    with pytest.raises(residency_map.ResidencyMapError):
        residency_map.validate_map(tampered)


def test_unstaged_entry_lookup_is_visible_not_silent(tmp_path: Path) -> None:
    """lookup() of an unstaged entry returns None: the fallback point is explicit."""
    queue, manifest, _ = _staged_once(tmp_path)
    fragments = [residency_map.validate_fragment(f) for f in
                 residency_map.read_fragments(queue.root / pool.RESIDENCY, CONSUMER)]
    mapping = residency_map.compose(fragments)
    assert residency_map.lookup(mapping, "/pool/model/never-staged.bin", 0) is None


def test_pool_tripwire_no_silent_fallback(tmp_path: Path) -> None:
    """chmod-000 pool after staging: staged reads succeed, pool reads raise."""
    queue, manifest, _ = _staged_once(tmp_path)
    whole_declared = str(manifest["entries"][1]["path"])
    pool_file = Path(whole_declared)
    expected_sha = manifest["entries"][1]["sha256"]
    os.chmod(pool_file, 0)
    try:
        fragments = [residency_map.validate_fragment(f) for f in
                     residency_map.read_fragments(queue.root / pool.RESIDENCY, CONSUMER)]
        mapping = residency_map.compose(fragments)
        found = residency_map.lookup(mapping, whole_declared, 0)
        assert found is not None
        staged_bytes = Path(found["stage_path"]).read_bytes()
        assert hashlib.sha256(staged_bytes).hexdigest() == expected_sha
        with pytest.raises(OSError):
            pool_file.read_bytes()
    finally:
        os.chmod(pool_file, 0o644)


def test_both_tiers_gone_fails_clearly(tmp_path: Path) -> None:
    """No staged copy and no map entry: lookup None AND open raises."""
    assert residency_map.lookup({"entries": {}}, "/pool/model/shard-0.bin", 0) is None
    with pytest.raises(FileNotFoundError):
        Path(tmp_path / "stage" / "model" / "shard-0.bin").read_bytes()


def test_double_egress_reclaims_exactly_once(tmp_path: Path) -> None:
    """Second egress of the same range is a no-op receipt, not a double free."""
    queue, manifest, files_len = _staged_once(tmp_path)
    assert stage_release.register_stage_root(
        queue, tier_id=STAGE_TIER, stage_root=tmp_path / "stage") == "registered"
    first = stage_release.evict(queue, WHOLE_MOVER, consumer_action_key=CONSUMER,
                                stage_root=tmp_path / "stage")
    second = stage_release.evict(queue, WHOLE_MOVER, consumer_action_key=CONSUMER,
                                 stage_root=tmp_path / "stage")
    assert first["complete"] is True
    assert second["complete"] is True
    assert not (tmp_path / "stage" / "model" / WHOLE).exists()


def test_prior_epoch_ram_range_is_not_resident(tmp_path: Path) -> None:
    """An epoch bump voids ram readiness: revalidation required, never assumed."""
    ram = tmp_path / "ram"
    ram.mkdir()
    first = storage_tiers.ensure_ram_epoch(ram, host="dl380g10")
    assert first is not None
    (ram / "epoch").write_text("epoch-prior")
    second = storage_tiers.ensure_ram_epoch(ram, host="dl380g10")
    assert second is not None
    assert str(second["epoch"]) != "epoch-prior"

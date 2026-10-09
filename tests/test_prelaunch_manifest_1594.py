"""The prelaunch-resident manifest declaration, v1 and v2 (#1594).

A phase declares with a literal ``resident_before_launch: true``: on a v1
``annotations.phases`` entry, on a v2 ``read_plan.phases`` entry.  Only a
literal boolean true declares.  Other values refuse.  Non-prefix sets
refuse in ``storage_tiers.manifest_prelaunch_phases``.
"""
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402

V1 = pb.DATA_MANIFEST_SCHEMA_V1
V2 = pb.DATA_MANIFEST_SCHEMA_V2
GIB = storage_tiers.GIB


def _entries(sizes):
    return [{"path": f"/data/f{index}", "offset": 0, "bytes": size,
             "sha256": None}
            for index, size in enumerate(sizes)]


def _v1_manifest(sizes, flags):
    """Build a v1 manifest with one entry per phase.

    flags gives the ``resident_before_launch`` value per phase, or None to
    omit it.
    """
    total = 0
    phases = []
    for name_index, (size, flag) in enumerate(zip(sizes, flags)):
        total += size
        entry = {"name": f"phase-{name_index}", "cumulative_bytes": total}
        if flag is not None:
            entry["resident_before_launch"] = flag
        phases.append(entry)
    return {"schema": V1, "produced_by": {}, "annotations": {"phases": phases},
            "mount_prefix": "/data", "entries": _entries(sizes),
            "entry_count": len(sizes), "total_bytes": total}


def _v2_manifest(sizes, flags):
    """Build a v2 manifest with one entry per phase, like ``_v1_manifest``."""
    total = 0
    phases = []
    for name_index, (size, flag) in enumerate(zip(sizes, flags)):
        total += size
        entry = {"name": f"phase-{name_index}", "entry_indices": [name_index],
                 "bytes": size, "cumulative_bytes": total}
        if flag is not None:
            entry["resident_before_launch"] = flag
        phases.append(entry)
    return {"schema": V2, "produced_by": {}, "annotations": {},
            "mount_prefix": "/data", "entries": _entries(sizes),
            "entry_count": len(sizes), "total_bytes": total,
            "read_plan": {"phases": phases, "read_bytes": total}}


def test_v1_declared_prefix_reads_in_order() -> None:
    manifest = _v1_manifest([10 * GIB, 20 * GIB, 30 * GIB],
                            [True, False, None])
    assert storage_tiers.manifest_prelaunch_phases(manifest) == ["phase-0"]


def test_v1_multi_phase_prefix_reads_in_order() -> None:
    manifest = _v1_manifest([10 * GIB, 20 * GIB, 30 * GIB],
                            [True, True, None])
    assert storage_tiers.manifest_prelaunch_phases(manifest) == [
        "phase-0", "phase-1"]


def test_v1_undeclared_manifest_reads_empty() -> None:
    manifest = _v1_manifest([10 * GIB, 20 * GIB], [None, False])
    assert storage_tiers.manifest_prelaunch_phases(manifest) == []


def test_v1_non_boolean_declaration_refuses() -> None:
    manifest = _v1_manifest([10 * GIB, 20 * GIB], [1, None])
    with pytest.raises(ValueError, match="must be a boolean"):
        storage_tiers.manifest_prelaunch_phases(manifest)


def test_v1_non_prefix_declaration_refuses() -> None:
    manifest = _v1_manifest([10 * GIB, 20 * GIB, 30 * GIB],
                            [None, True, None])
    with pytest.raises(ValueError, match="contiguous prefix"):
        storage_tiers.manifest_prelaunch_phases(manifest)


def test_v1_gap_declaration_refuses() -> None:
    manifest = _v1_manifest([10 * GIB, 20 * GIB, 30 * GIB],
                            [True, None, True])
    with pytest.raises(ValueError, match="contiguous prefix"):
        storage_tiers.manifest_prelaunch_phases(manifest)


def test_v1_manifest_validates_and_declares() -> None:
    # v1 annotations stay free-form in ``core``.  The normalized manifest
    # keeps the declaration for the reader.
    normalized = pb.validate_data_manifest(
        _v1_manifest([10 * GIB, 20 * GIB], [True, None]))
    assert storage_tiers.manifest_prelaunch_phases(normalized) == ["phase-0"]


def test_v2_declared_phase_normalizes_and_reads() -> None:
    normalized = pb.validate_data_manifest(
        _v2_manifest([10 * GIB, 20 * GIB], [True, None]))
    assert normalized["read_plan"]["phases"][0][
        "resident_before_launch"] is True
    assert "resident_before_launch" not in normalized["read_plan"]["phases"][1]
    assert storage_tiers.manifest_prelaunch_phases(normalized) == ["phase-0"]


def test_v2_undeclared_manifest_normalizes_byte_identically() -> None:
    normalized = pb.validate_data_manifest(
        _v2_manifest([10 * GIB, 20 * GIB], [None, None]))
    for phase in normalized["read_plan"]["phases"]:
        assert "resident_before_launch" not in phase
    assert storage_tiers.manifest_prelaunch_phases(normalized) == []


def test_v2_false_declaration_refuses() -> None:
    with pytest.raises(pb.ActionContractError, match="must be true"):
        pb.validate_data_manifest(
            _v2_manifest([10 * GIB, 20 * GIB], [False, None]))


def test_v2_truthy_non_boolean_declaration_refuses() -> None:
    with pytest.raises(pb.ActionContractError, match="must be true"):
        pb.validate_data_manifest(
            _v2_manifest([10 * GIB, 20 * GIB], [1, None]))


def test_v2_unknown_phase_key_still_refused() -> None:
    manifest = _v2_manifest([10 * GIB, 20 * GIB], [None, None])
    manifest["read_plan"]["phases"][0]["bogus"] = 1
    with pytest.raises(pb.ActionContractError, match="fields differ"):
        pb.validate_data_manifest(manifest)


def test_v2_non_prefix_declaration_refuses_at_read() -> None:
    # ``core`` checks shape, not order.  The prefix reader refuses.  pbrun
    # turns that refusal into SystemExit.
    normalized = pb.validate_data_manifest(
        _v2_manifest([10 * GIB, 20 * GIB, 30 * GIB], [None, True, None]))
    with pytest.raises(ValueError, match="contiguous prefix"):
        storage_tiers.manifest_prelaunch_phases(normalized)


def test_v2_non_boolean_declaration_refuses_at_read() -> None:
    # A manifest that skipped ``core`` still refuses in the reader.
    manifest = _v2_manifest([10 * GIB, 20 * GIB], [None, None])
    manifest["read_plan"]["phases"][0]["resident_before_launch"] = "true"
    with pytest.raises(ValueError, match="must be a boolean"):
        storage_tiers.manifest_prelaunch_phases(manifest)


def test_manifest_without_phase_table_declares_nothing() -> None:
    manifest = {"schema": V1, "produced_by": {}, "annotations": {},
                "mount_prefix": "/data", "entries": _entries([10 * GIB]),
                "entry_count": 1, "total_bytes": 10 * GIB}
    assert storage_tiers.manifest_prelaunch_phases(manifest) == []

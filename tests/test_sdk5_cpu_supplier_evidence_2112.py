"""Offline acceptance of the bounded PB2112 supplier evidence handoff.

The handoff is data, not a new qualification framework. These checks never
build, install, submit, read the live store, or require historical Git objects.
A documented unresolved boundary passes *inventory* acceptance, not SDK
qualification. Independent review must authenticate retained evidence and
judge applicability; a JSON assertion is not an execution receipt.
"""
from __future__ import annotations

import json
from email.parser import BytesParser
from pathlib import Path
import re
import zipfile

import pytest

from prismabuild.core import raw_sha256


REPO = Path(__file__).resolve().parents[1]
HANDOFF = REPO / "docs/evidence/pb2112-sdk5-cpu.json"
FROZEN = "027103d9a8417e06c7f13356e58779a313cd7088"
CHILD = "855032b1f3f2edcef0b6cc49c16078942ea1644e"
PACKET = "50e87c70ea4c5c64161c7f7e5e77a424685ac42b26a53fdb6c26f276b445f46b"
ARCHIVE = "94727274fd0515c5f1ee7c031c90c1106793ef22b38b286caf44f1755675677d"
BUNDLE_ACTION = "1a525af7c723625c16b9279b726c66120f07b5c5945008ceeea186b9964b42b5"
GUARD_ACTION = "20cb79a0504632a78f6412465012eb868f56f5a432b4717cba6ff3c619560bb6"


def _supplier_handoff_2112():
    assert HANDOFF.is_file(), (
        "PB2112 has no supplier evidence handoff at docs/evidence/pb2112-sdk5-cpu.json; "
        "supply authenticated evidence or exact unresolved boundaries, not a passing claim"
    )
    return json.loads(HANDOFF.read_text(encoding="utf-8"))


def _retained_copy_2112(record):
    """Small reviewer-readable copies are committed; large objects may stay in CAS."""
    assert record["reference"].strip()
    assert re.fullmatch(r"[0-9a-f]{64}", record["sha256"])
    assert isinstance(record["bytes"], int) and record["bytes"] >= 0
    relative = Path(record["copy"])
    assert not relative.is_absolute() and ".." not in relative.parts
    path = REPO / relative
    assert path.is_file(), f"retained evidence copy is missing: {relative}"
    payload = path.read_bytes()
    assert len(payload) == record["bytes"]
    assert raw_sha256(payload) == record["sha256"]
    return path


def _evidence_disposition_2112(entry):
    assert entry["status"] in {"verified", "unresolved"}
    assert entry["scope"].strip()
    if entry["status"] == "verified":
        assert entry["evidence"], "verified needs retained readable proof, not an action ID alone"
        for evidence in entry["evidence"]:
            _retained_copy_2112(evidence)
        assert not entry.get("unresolved")
    else:
        assert entry["unresolved"], "unavailable prerequisites must not become a passing qualification"
        for boundary in entry["unresolved"]:
            for field in ("reference", "operation", "reader_context", "result", "prerequisite"):
                assert boundary[field].strip(), f"unresolved boundary lacks {field}"
    return entry["status"]


def test_2112_carries_accepted_baseline_and_evidence_backed_deltas():
    report = _supplier_handoff_2112()
    assert report["source_commit"] == FROZEN
    assert report["accepted_child"] == CHILD
    baseline = report["compatibility_packet"]
    assert baseline["reference"] == "local://pb1485-sdk5-publication-compatibility-predecision.json#3441"
    assert baseline["sha256"] == PACKET
    _evidence_disposition_2112(baseline)
    for delta in report["consumer_dependency_deltas"]:
        assert delta["before"].strip() and delta["after"].strip()
        assert delta["evidence"]
        for evidence in delta["evidence"]:
            _retained_copy_2112(evidence)
    references = report["prior_documentary_references"]
    assert references["source_helper_archive_sha256"] == ARCHIVE
    assert references["source_helper_action"] == BUNDLE_ACTION
    assert references["x86_git_install_guard_action"] == GUARD_ACTION
    assert report["prior_qualification_replayed"] is False


@pytest.mark.parametrize("kind", ["wheel", "git_install", "source_helper_archive"])
def test_2112_distinguishes_packaging_artifacts_and_their_provenance(kind):
    entry = _supplier_handoff_2112()["artifacts"][kind]
    assert entry["source_commit"] == FROZEN
    status = _evidence_disposition_2112(entry)
    if status == "unresolved":
        return  # Explicitly NOT a successful wheel/Git/archive qualification.
    if kind == "wheel":
        assert entry["filename"] == "prismabuild-0.1.0-py3-none-any.whl"
        assert entry["build_backend"] == "setuptools.build_meta"
        path = _retained_copy_2112(entry["artifact"])
        with zipfile.ZipFile(path) as wheel:
            metadata = BytesParser().parsebytes(wheel.read("prismabuild-0.1.0.dist-info/METADATA"))
            assert metadata["Name"] == "prismabuild"
            assert metadata["Version"] == "0.1.0"
            assert metadata["Requires-Python"] == ">=3.12"
            assert all("extra ==" in dependency for dependency in metadata.get_all("Requires-Dist", []))
            tags = BytesParser().parsebytes(wheel.read("prismabuild-0.1.0.dist-info/WHEEL"))
            assert tags["Root-Is-Purelib"] == "true"
            assert tags.get_all("Tag") == ["py3-none-any"]
            assert "prismabuild/client.py" in wheel.namelist()
            assert "prismabuild/core.py" in wheel.namelist()
    elif kind == "git_install":
        direct = json.loads(_retained_copy_2112(entry["direct_url"]).read_bytes())
        assert direct["vcs_info"]["vcs"] == "git"
        assert direct["vcs_info"]["commit_id"] == FROZEN
        assert not direct.get("dir_info", {}).get("editable", False)
        _retained_copy_2112(entry["record"])
        assert entry["guard"]["module"] == "prismabuild"
        assert entry["guard"]["installed_commit"] == FROZEN
        assert entry["guard"]["expected_commit"] == FROZEN
        assert entry["guard"]["verified_files"] > 0
        assert "identity_drift_tolerated" not in entry["guard"]
        assert entry["guard"]["origin"].strip()
    else:
        assert re.fullmatch(r"[0-9a-f]{64}", entry["archive_sha256"])
        assert entry["identity_evidence"], "link archive bytes, exact source and helper inventory"


def test_2112_layout_and_custody_are_exact_source_and_not_publication():
    report = _supplier_handoff_2112()
    layout = report["layout"]
    assert layout["source_commit"] == FROZEN
    _evidence_disposition_2112(layout)
    assert set(layout["required_helpers"]) == {
        "injected_context", "acquire_for", "open_pinned", "release", "covers_for_keys", "READER_LEASE_TAG"
    }
    for concern in ("import_origins", "core_package_coherence", "native_producer_context", "shape"):
        _evidence_disposition_2112(layout[concern])
    custody = report["safe_route"]
    assert set(custody["dependency_issues"]) == {2045, 2046, 2047, 2048, 2049, 2050}
    _evidence_disposition_2112(custody)
    # A new causal run needs a qualified route; a retained proof may instead be reused.
    if layout["status"] == "verified" and not layout["reused_retained_evidence"]:
        assert custody["status"] == "verified"
        assert layout["supplier_operation"]["action_key"]
        assert layout["supplier_operation"]["receipt_sha256"]
    qualified = all(report["artifacts"][kind]["status"] == "verified"
                    for kind in ("wheel", "git_install", "source_helper_archive"))
    qualified = qualified and layout["status"] == "verified"
    qualified = qualified and all(layout[name]["status"] == "verified" for name in (
        "import_origins", "core_package_coherence", "native_producer_context", "shape"))
    qualified = qualified and report["compatibility_packet"]["status"] == "verified"
    assert report["cpu_tuple_qualified"] is qualified
    assert report["consumer_pin_changes"] is False
    assert report["runtime_publication"] is False

"""Reporting acceptance for #2160, not a storage probe or recovery certificate.

The incident's deliverable is an issue-note-ready evidence handoff. These cheap
checks require its essential topics and explicit limits; independent review must
still verify the provenance, attribution, and minimum external prerequisite.
No historical checkout, fleet path, host access, Git history, or live I/O is
needed to run them. Existing source-sealing diagnostics are not repeated here.
"""
from __future__ import annotations

from pathlib import Path
import re


REPORT = (
    Path(__file__).resolve().parents[1]
    / "docs/evidence/campaign_read_stall_disposition_2160.md"
)


def _section(heading: str) -> str:
    assert REPORT.is_file(), (
        "#2160 still needs its evidence-only disposition and storage-owner "
        f"handoff: {REPORT.relative_to(REPORT.parents[2])}"
    )
    text = REPORT.read_text(encoding="utf-8")
    match = re.search(
        rf"^## {re.escape(heading)}\s*\n(.*?)(?=^## |\Z)",
        text,
        flags=re.MULTILINE | re.DOTALL,
    )
    assert match, f"Missing handoff section: {heading}"
    return match.group(1).casefold()


def _mentions(text: str, *topics: str) -> None:
    for topic in topics:
        assert topic.casefold() in text, f"Missing disposition topic: {topic}"


def test_evidence_ledger_distinguishes_observation_attribution_and_unknowns() -> None:
    disposition = _section("Disposition")
    _mentions(
        disposition, "availability", "before publication", "no action key",
        "no admission", "no profile", "no CAS result", "unverified",
    )
    ledger = _section("Evidence ledger")
    _mentions(
        ledger, "provenance", "readable", "unavailable", "private-index",
        "source-index", "I/O pressure", "synchronous", "pool progress",
        "contention", "hypothesis", "coordinator", "s27", "CD5", "nfsd",
        "pb-ts-14", "another worktree",
        "source_sealing_family_1409_1410_2026-10-01.md",
    )


def test_report_records_inspected_and_deployed_identity_without_old_timeout_assumption() -> None:
    identities = _section("Inspected and deployed identities")
    _mentions(
        identities, "inspected", "deployed", "historical", "bdc0795c2",
        "bd5d11a934cb49fed5aeca9d3da1633c51c9847e",
        "core.py", "git_checkout_identity", "tracked", "untracked", "bytes",
        "submission.py", "snapshot_checkout", "before", "while",
        "deadline", "30", "120", "1888",
    )
    assert re.search(r"\b[0-9a-f]{40}\b", identities), "Record inspected full revision"


def test_handoff_names_first_path_external_boundary_and_existing_owners() -> None:
    recovery = _section("Recovery decision and external prerequisite")
    _mentions(
        recovery, "quieter-window", "private-tree recreation", "first",
        "coordinator", "storage-owner", "prerequisite", "stop", "unchanged",
        "read-only", "bounded", "Netdata", "fresh", "holder", "#2161",
    )
    ownership = _section("Ownership and non-goals")
    _mentions(
        ownership, "Sol", "startup slice", "#1409", "issues-pb", "#1398",
        "issues-ts", "#940", "#2117", "#2118", "#2119", "#2120",
        "#2121", "#2122", "datasets", "mounts", "kernel", "pool settings",
        "foreign", "drains", "identity", "deadlines", "admission",
        "benchmark", "TrustLadder", "publication", "parent acceptance",
    )

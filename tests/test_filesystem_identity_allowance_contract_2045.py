"""#2045 is a reviewable contract prerequisite, NOT a second guard implementation.

The JSON is a review artifact under docs/, not a runtime registry or new ledger.
These checks enforce minimum coverage/explicit unknowns only. They cannot prove
live mapping, closed populations, bounds, parent acceptance or review approval;
those require independent reading of the cited evidence. Missing evidence may
be reported as blocked, never reconstructed from hostnames or fresh test runs.
"""
from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "docs" / "filesystem_identity_allowance_contract_2045.json"


def read_review_contract():
    assert CONTRACT.is_file(), (
        "#2045 needs a parent-reviewable existing-owner contract at "
        "docs/filesystem_identity_allowance_contract_2045.json, including explicit "
        "supplier/unblock records for unavailable evidence; do not repair or "
        "enable the guard to satisfy this documentation prerequisite"
    )
    return json.loads(CONTRACT.read_text(encoding="utf-8"))


def require_explanation(record, *fields):
    for field in fields:
        value = record.get(field)
        assert isinstance(value, str) and value.strip(), field


def test_contract_reconciles_main_and_preserves_existing_custody():
    contract = read_review_contract()
    assert contract["issue"] == 2045
    assert contract["parent_issue"] == 1483
    provenance = contract["provenance"]
    assert provenance["original_base"] == "be8c2e28a1d53984bad992d8a8928a25a66fbb28"
    assert provenance["issue_inspected_main"] == "a85f3886c3c12ce55f05ae3be66ba2c3fef3fe36"
    require_explanation(provenance, "reconciled_main", "isolated_worktree", "later_main_changes")
    adopted = contract["existing_work"]
    for seam in ("filesystem_floor", "submission_write_window", "tier_serve"):
        require_explanation(adopted[seam], "source", "present_behavior", "remaining_gap")
    require_explanation(contract, "c437_evidence_boundary", "pr1490_disposition", "scope_exclusions")
    require_explanation(contract["custody"], "integration_owner", "guard_implementer",
                        "security_reviewer", "diagnostic_assessor", "parent_review")
    assert contract["runtime_changes_authorized"] is False


def test_contract_covers_domains_and_names_unresolved_existing_suppliers():
    contract = read_review_contract()
    domains = contract["domains"]
    assert isinstance(domains, list) and domains
    ids = [domain["id"] for domain in domains]
    assert len(ids) == len(set(ids)), "each capacity-domain entry needs a unique identity"
    assert {"coordinator", "worker", "cas", "queue", "checkout", "logs",
            "stage", "prewarm", "scratch", "spool"} <= set(ids)
    for domain in domains:
        require_explanation(
            domain, "write_paths", "descriptor_fsid_authority", "allowance_owner",
            "reservation_lifecycle", "freshness_rule", "complete_growth_bound",
            "population_closure", "alias_deduplication",
        )
        assert domain["evidence_status"] in {"established", "blocked"}
        refs = domain["source_refs"]
        assert isinstance(refs, list) and refs, domain["id"]
        for ref in refs:
            path = Path(ref)
            assert not path.is_absolute() and ".." not in path.parts
            assert (ROOT / path).is_file(), ref
        if domain["evidence_status"] == "blocked":
            require_explanation(domain, "existing_supplier", "unblock_condition")
        else:
            require_explanation(domain, "evidence_reference")
    population = contract["population_contract"]
    require_explanation(
        population, "existing_owner", "membership_and_version", "private_and_committed",
        "locking_and_recovery", "partial_or_unreadable", "authenticated_nfs_mapping",
    )
    assert population["unknown_allowance_policy"] == "hold"
    assert population["unknown_mapping_policy"] == "hold"
    gaps = contract["unresolved_dependencies"]
    assert isinstance(gaps, list)
    for gap in gaps:
        require_explanation(gap, "existing_supplier", "missing_evidence", "unblock_condition")
        assert gap["tracking_issue"] == 1483


def test_contract_uses_honest_whole_bounds_and_nonduplicable_credits():
    contract = read_review_contract()
    accounting = contract["accounting"]
    assert accounting["approach"] == "whole-reservation-upper-bound"
    assert accounting["unknown_materialization_credit_bytes"] == 0
    assert accounting["verified_exact_materialization_accounting"] is False
    assert accounting["floor_divisor"] == 20
    require_explanation(
        accounting, "aggregate_rule", "same_owner_same_filesystem_evidence",
        "credit_identity_and_cap", "transfer_rule", "release_rule",
        "sample_and_credit_ordering", "partial_failure_rule",
        "complete_bound_inclusions", "operation_vs_action_custody",
    )
    acceptance = accounting["parent_acceptance"]
    assert acceptance["status"] in {"accepted", "pending"}
    if acceptance["status"] == "accepted":
        require_explanation(acceptance, "attributed_reference", "accepted_scope")
    else:
        require_explanation(acceptance, "existing_supplier", "unblock_condition")
    readiness = contract["readiness"]
    require_explanation(readiness, "parent_packet", "implementation_unblock_condition",
                        "activation_unblock_condition")
    gaps = contract["unresolved_dependencies"]
    assert all(gap["gates"] in {"implementation", "activation"} for gap in gaps)
    implementation_blocked = (acceptance["status"] == "pending"
                              or any(gap["gates"] == "implementation" for gap in gaps))
    activation_blocked = (implementation_blocked
                          or bool(gaps)
                          or any(d["evidence_status"] == "blocked" for d in contract["domains"]))
    assert readiness["dependent_implementation_must_wait"] is implementation_blocked
    assert readiness["activation_must_wait"] is activation_blocked
    assert readiness["deployment_authorized"] is False
    assert readiness["physical_rob_action_established"] is False

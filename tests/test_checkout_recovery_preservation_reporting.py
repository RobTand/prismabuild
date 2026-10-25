"""Checkout-recovery drift diagnostics must describe the snapshot that failed.

Synthetic snapshots isolate reporting from real queue publishers. Inode-only
replacement deliberately keeps the file's size and digest unchanged.
"""
from types import SimpleNamespace

import pytest

import test_checkout_recovery_1465 as contract


@pytest.mark.parametrize("helper", ["planning", "refusal"])
@pytest.mark.parametrize("change", ["inode-only", "content"])
def test_recovery_preservation_reports_one_captured_delta(monkeypatch, helper, change):
    root = "queue"
    leaf = "reservations/fixture-host/adaptive/claim-denials.json"
    identity = (7, 101, 1000, 0o100644)
    old_file = ("file", identity, 32, "a" * 64)
    new_file = (
        "file",
        (7, 102, 1000, 0o100644) if change == "inode-only" else identity,
        32,
        "a" * 64 if change == "inode-only" else "b" * 64,
    )
    before = {root: {leaf: old_file}}
    after = {root: {leaf: new_file}}
    expected_delta = {root: {leaf: (old_file, new_file)}}
    captures = []
    reports = []
    real_delta = contract._snapshot_delta

    def capture_preserved():
        snapshot = before if not captures else after
        captures.append(snapshot)
        return snapshot

    def record_delta(old, new):
        reports.append((old, new))
        return real_delta(old, new)

    monkeypatch.setattr(contract, "_snapshot_delta", record_delta)
    result = (
        {"schema": contract.PLAN_SCHEMA, "complete": True, "status": "planned"}
        if helper == "planning" else
        {"schema": contract.RESULT_SCHEMA, "complete": False, "status": "refused",
         "errors": ["injected preflight refusal"], "removed": []}
    )
    case = SimpleNamespace(
        cleanup_calls=[], preserved=capture_preserved, prepare=lambda: result,
    )

    with pytest.raises(AssertionError) as failure:
        if helper == "planning":
            contract._planned(case)
        else:
            contract._refused(case, lambda: result)

    assert len(captures) == 2, "report the captured post-call snapshot, not a later rescan"
    assert captures[0] is before and captures[1] is after
    assert len(reports) == 1, "both helpers must report their leaf-level snapshot delta"
    assert reports[0][0] is before and reports[0][1] is after
    assert str(expected_delta) in str(failure.value), (
        "report the changed root/path and both full identity/size/digest tuples"
    )

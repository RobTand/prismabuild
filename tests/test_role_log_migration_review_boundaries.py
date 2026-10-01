"""Review-boundary RED controls against the actual unchanged migration consumer.

Parent-admitted PrismaBuild CPU execution only. These controls add no source fix,
mock no qualification verdict and preserve the original fixture/assertions.
Malformed real receipts must yield CLI JSON, including after a genuine first
fchmod. Missing proof is uncertainty, not the absence of a diagnostic leaf.
"""
from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest

from test_exact_role_log_metadata_migration import (
    ROLES,
    cli,
    fingerprint,
    migration,
)
from test_exact_role_log_metadata_migration import box as box


def _valid_plan(box, capsys):
    before = {role: fingerprint(path) for role, path in box["paths"].items()}
    code, result = cli(capsys)
    assert code == 0 and result["status"] == "planned"
    assert not result["effect_attempted"]
    assert [leaf["role"] for leaf in result["leaves"]] == list(ROLES)
    assert all(leaf["disposition"] == "would-change" for leaf in result["leaves"])
    _physical_readback(box, before)
    return before


def _physical_readback(box, before, *, first_changed=False):
    # Full private digests AND inode/UID/GID/nlink, independently of JSON verdicts.
    for role, path in box["paths"].items():
        assert fingerprint(path) == before[role]
        expected_mode = 0o600 if first_changed and role == "storage" else 0o664
        assert stat.S_IMODE(path.stat().st_mode) == expected_mode


def _damage_receipt(box, value):
    # Reuse the actual fixture's unseal/seal operations, then change ONLY the
    # actual receipt bytes and restore its sealed mode. No parser/verdict mock.
    box["unseal"]()
    box["seal"]()
    receipt = box["generation"] / "RUNTIME_VERSION.json"
    raw = json.dumps(value).encode("utf-8")
    receipt.chmod(0o644)
    receipt.write_bytes(raw)
    receipt.chmod(0o444)
    assert receipt.read_bytes() == raw
    assert json.loads(receipt.read_bytes()) == value
    assert not box["generation"].stat().st_mode & 0o222
    assert not receipt.stat().st_mode & 0o222
    return receipt


def _receipt_read_witness(monkeypatch, receipt):
    """Count successful raw opens of the real damaged receipt, without replacing bytes."""
    original = Path.open
    reads = []

    def opened(path, mode="r", *args, **kwargs):
        stream = original(path, mode, *args, **kwargs)
        if path == receipt and mode in ("r", "rb"):
            reads.append(mode)
        return stream

    monkeypatch.setattr(Path, "open", opened)
    return reads


@pytest.mark.parametrize("receipt_value", [[], None, "not-a-receipt-object"],
                         ids=["array", "null", "string"])
@pytest.mark.parametrize("phase", ["initial", "after-first-effect"])
def test_nonobject_receipt_returns_cli_json_even_after_first_real_fchmod(
    box, capsys, monkeypatch, receipt_value, phase,
):
    before = _valid_plan(box, capsys)
    receipt = box["generation"] / "RUNTIME_VERSION.json"
    original_fchmod = os.fchmod
    effects = []

    def fchmod(fd, mode):
        held = os.fstat(fd)
        role = next(role for role, expected in before.items()
                    if (held.st_dev, held.st_ino) == expected[:2])
        assert mode == 0o600
        # Genuine syscall on the actual originally-qualified private inode.
        original_fchmod(fd, mode)
        effects.append(role)
        if phase == "after-first-effect" and len(effects) == 1:
            assert role == "storage"
            _physical_readback(box, before, first_changed=True)
            _damage_receipt(box, receipt_value)
            # Install after damage, so this witness cannot count fixture reads.
            witness = _receipt_read_witness(monkeypatch, receipt)
            witnesses.append(witness)

    witnesses = []
    monkeypatch.setattr(os, "fchmod", fchmod)
    if phase == "initial":
        _damage_receipt(box, receipt_value)
        witnesses.append(_receipt_read_witness(monkeypatch, receipt))
    try:
        # Desired public contract, not pytest.raises(AttributeError), xfail or
        # a substitute no-op consumer. Unchanged source currently escapes here.
        code, result = cli(capsys, apply=True)
    finally:
        # Even an escaping exception must have reached the intended receipt
        # boundary, not a fixture permission failure; physical effects are real.
        assert witnesses and any(witness for witness in witnesses)
        expected_effects = ["storage"] if phase == "after-first-effect" else []
        assert effects == expected_effects
        _physical_readback(box, before, first_changed=phase == "after-first-effect")
    assert code == 2
    assert result["status"] == ("partial-uncertain" if phase == "after-first-effect"
                                else "refused")
    assert result["effect_attempted"] is (phase == "after-first-effect")
    if phase == "after-first-effect":
        assert result["stage"] == "storage:postcheck"
        assert result["leaves"][0]["disposition"] == "effect-uncertain"
        assert all(leaf["disposition"] == "not-applied" for leaf in result["leaves"][1:])
    else:
        assert result["stage"] == "qualification"
        assert result["leaves"] == []


@pytest.fixture
def private_proc(box, tmp_path, monkeypatch):
    """Private names reference the finite children’s real current proc facts.

    Only private symlinks/files are removed in missing-proof controls; no live
    proc node is modified. Both FD observations still resolve to real append FDs.
    """
    real_proc = migration.supervise.PROC
    private = tmp_path / "review-proc"
    boot = private / "sys/kernel/random/boot_id"
    boot.parent.mkdir(parents=True)
    boot.write_bytes((real_proc / "sys/kernel/random/boot_id").read_bytes())
    for child in box["children"].values():
        observed = private / str(child.pid)
        observed.mkdir()
        actual = real_proc / str(child.pid)
        for field in ("stat", "status", "cmdline", "environ", "cwd"):
            (observed / field).symlink_to(actual / field)
        for field in ("fd", "fdinfo"):
            (observed / field).mkdir()
            for descriptor in (1, 2):
                (observed / field / str(descriptor)).symlink_to(
                    actual / field / str(descriptor))
    monkeypatch.setattr(migration.supervise, "PROC", private)
    return private


@pytest.mark.parametrize("missing", ["boot-id", "published-runtime-link", "writer-status"])
def test_missing_qualification_proof_is_refused_not_leaf_absence(
    box, private_proc, capsys, monkeypatch, missing,
):
    before = _valid_plan(box, capsys)
    effects = []
    original_fchmod = os.fchmod

    def fchmod(fd, mode):
        effects.append((os.fstat(fd).st_dev, os.fstat(fd).st_ino))
        original_fchmod(fd, mode)

    monkeypatch.setattr(os, "fchmod", fchmod)
    if missing == "boot-id":
        proof = private_proc / "sys/kernel/random/boot_id"
    elif missing == "published-runtime-link":
        proof = box["generation"].parent.parent / "repo"
    else:
        proof = private_proc / str(box["children"]["storage"].pid) / "status"
    proof.unlink()  # actual private observation name, never a live proc node
    assert not proof.exists()
    assert all(path.exists() for path in box["paths"].values())
    try:
        code, result = cli(capsys, apply=True)
    finally:
        assert effects == []
        _physical_readback(box, before)
    assert code == 2
    assert result["status"] == "refused"
    assert result["stage"] == "qualification"
    assert not result["effect_attempted"]
    assert result["leaves"] == []


def test_actual_missing_leaf_remains_absent_no_effect(box, capsys, monkeypatch):
    before = _valid_plan(box, capsys)
    path = box["paths"]["metrics"]
    held_original = path.with_name("private-metrics-held-original")
    path.rename(held_original)
    assert not path.exists()
    effects = []
    original_fchmod = os.fchmod

    def fchmod(fd, mode):
        effects.append((os.fstat(fd).st_dev, os.fstat(fd).st_ino))
        original_fchmod(fd, mode)

    monkeypatch.setattr(os, "fchmod", fchmod)
    code, result = cli(capsys, apply=True)
    assert code == 2 and result["status"] == "absent-no-effect"
    assert result["stage"] == "metrics:preflight"
    assert not result["effect_attempted"] and effects == []
    assert not path.exists()
    assert fingerprint(held_original) == before["metrics"]
    assert stat.S_IMODE(held_original.stat().st_mode) == 0o664
    for role in ("storage", "tiers"):
        assert fingerprint(box["paths"][role]) == before[role]
        assert stat.S_IMODE(box["paths"][role].stat().st_mode) == 0o664

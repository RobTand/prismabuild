"""Readers accept the optional ``digest_primitives`` runtime identity (#1547 rollout).

PR 1553 moves the primitive digest helpers into ``digest_primitives.py`` and has
a worker attest that file in its runtime body.  Readers that predate it demand
the exact old key set and refuse the receipt as tamper.  This reader-only change
lands and is published first, so every reader tolerates the new key before any
worker emits it.  A retained body without the key must keep validating exactly.
"""

from __future__ import annotations

import copy
from pathlib import Path

import pytest

from prismabuild import core as pb
from test_core import _action


def _attested(tmp_path: Path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _action(checkout)
    attestation = pb.preflight_action(
        action, cas_root=tmp_path / "cas", checkout_root=checkout
    )
    return action, attestation


def _seal_attestation(attestation: dict) -> dict:
    body = {k: v for k, v in attestation.items() if k != "attestation_sha256"}
    attestation["attestation_sha256"] = pb.canonical_sha256(body)
    return attestation


def _seal_runtime(attestation: dict) -> dict:
    body = {k: v for k, v in attestation["runtime"].items() if k != "runtime_sha256"}
    attestation["runtime"]["runtime_sha256"] = pb.canonical_sha256(body)
    return _seal_attestation(attestation)


def _owner_identity(tmp_path: Path) -> dict:
    owner = tmp_path / "digest_primitives.py"
    owner.write_text("# the digest owner\n", encoding="utf-8")
    return pb._identify_runtime_source(owner, where="test digest owner")


def test_a_runtime_that_also_attests_the_digest_owner_validates(tmp_path):
    action, attestation = _attested(tmp_path)
    new = copy.deepcopy(attestation)
    new["runtime"]["digest_primitives"] = _owner_identity(tmp_path)
    _seal_runtime(new)
    assert pb.validate_worker_attestation(new, action=action) == new
    assert (pb._validate_worker_runtime(new["runtime"])["digest_primitives"]
            == new["runtime"]["digest_primitives"])


def test_a_retained_runtime_without_the_digest_owner_keeps_validating(tmp_path):
    action, attestation = _attested(tmp_path)
    assert "digest_primitives" not in attestation["runtime"]
    assert pb.validate_worker_attestation(attestation, action=action) == attestation
    assert "digest_primitives" not in pb._validate_worker_runtime(attestation["runtime"])


def test_the_runtime_digest_binds_the_digest_owner_identity(tmp_path):
    action, attestation = _attested(tmp_path)
    new = copy.deepcopy(attestation)
    new["runtime"]["digest_primitives"] = _owner_identity(tmp_path)
    # runtime_sha256 left as it was computed WITHOUT the new key
    _seal_attestation(new)
    with pytest.raises(pb.ActionContractError, match="digest does not match"):
        pb.validate_worker_attestation(new, action=action)


def test_a_malformed_digest_owner_identity_is_refused(tmp_path):
    action, attestation = _attested(tmp_path)
    new = copy.deepcopy(attestation)
    identity = _owner_identity(tmp_path)
    identity["sha256"] = "not-a-digest"
    new["runtime"]["digest_primitives"] = identity
    _seal_runtime(new)
    with pytest.raises(pb.ActionContractError, match="runtime.digest_primitives"):
        pb.validate_worker_attestation(new, action=action)


def test_any_other_unrecognized_runtime_key_is_still_refused(tmp_path):
    action, attestation = _attested(tmp_path)
    new = copy.deepcopy(attestation)
    new["runtime"]["digest_primitives"] = _owner_identity(tmp_path)
    new["runtime"]["unrecognized"] = True
    _seal_runtime(new)
    with pytest.raises(pb.ActionContractError, match="runtime fields differ"):
        pb.validate_worker_attestation(new, action=action)

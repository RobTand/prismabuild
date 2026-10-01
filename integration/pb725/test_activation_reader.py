"""Refs #725: exact-activation write -> real pinned mover -> actual reader.

Opt-in only via run_activation.py. Accepted strict activation semantics remain
unchanged; only private queue/mover/map/restoration infrastructure is shared.
"""
from __future__ import annotations

import importlib
import json
import os
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import pytest
from pb725_scaffold import (
    OriginPayloadOpened,
    PayloadTripwire,
    move_manifest,
    persist_case,
    private_runtime,
    setup_complete,
)


@pytest.fixture
def reader_policy():
    """Activate the reviewed ram,ssd default for this bounded reader fixture."""
    from prismaquant.staged_tier_policy import (
        DEFAULT_ALLOWED_TIERS,
        staged_tier_policy_test_context,
    )
    return staged_tier_policy_test_context(DEFAULT_ALLOWED_TIERS)


@pytest.fixture
def chain(tmp_path, monkeypatch, request):
    if not os.environ.get("PB725_BINDING_PATH"):
        pytest.fail("opt-in setup missing: use the admitted run_activation.py launcher")
    binding = json.loads(Path(os.environ["PB725_BINDING_PATH"]).read_text())
    evidence = {"case": request.node.name, "binding_scope": binding["scope"]}
    installed_root = Path(os.environ["PB725_PACKAGE_ROOT"]).resolve()
    observed_chain = None
    with private_runtime(monkeypatch, binding) as runtime:
        try:
            import torch
            exact = importlib.import_module("prismaquant.perturbed_x_cache")
            operands = binding["operands"]
            session = operands["session"]
            tensor = torch.arange(operands["tensor_elements"], dtype=torch.float32).reshape(
                operands["shape"])
            nbytes = tensor.numel() * tensor.element_size()
            origin = tmp_path / "origin"
            origin.mkdir()
            print("PB725_FIXTURE_PHASE producer " + request.node.name, flush=True)
            refs = []
            for name in operands["names"]:
                refs.append(exact.write_exact_activation_cache_entry(
                    origin, name, tensor, identity={"session": session, "slot": name,
                        "kind": "boundary", "coordinates": {"batch": 0, "boundary": 0,
                                                               "probe": None}},
                    max_tensor_bytes=nbytes, max_file_bytes=operands["max_file_bytes"],
                    release_file_pages=False))
            covered, omitted = refs
            moved = move_manifest(runtime, tmp_path, monkeypatch,
                entries=[{"path": covered.path, "offset": 0,
                          "bytes": covered.file_bytes, "sha256": covered.sha256}],
                origin=origin, tier=operands["tier_id"], scope="activation-only",
                producer="pb725-activation-integration", case_name=request.node.name,
                evidence=evidence)
            resolver = moved.resolver
            assert resolver.staged_read(Path(covered.path), expected_sha256=covered.sha256)
            assert resolver.staged_read(Path(omitted.path), expected_sha256=omitted.sha256) is None
            (key,) = moved.keys
            (staged_path,) = moved.staged_paths
            tripwire = PayloadTripwire([ref.path for ref in refs], staged_path, moved.leases)
            evidence.update({"reference_sha256": covered.sha256,
                             "omitted_sha256": omitted.sha256,
                             "exact_references": [asdict(ref) for ref in refs]})
            setup_complete(evidence, request.node.name)
            observed_chain = SimpleNamespace(
                exact=exact, torch=torch, tensor=tensor, covered=covered,
                omitted=omitted, nbytes=nbytes, session=session, resolver=resolver,
                tripwire=tripwire, leases=moved.leases, key=key, tier=operands["tier_id"])
            yield observed_chain
        finally:
            persist_case(evidence, observed_chain, installed_root, request.node.name)


def _prefetch(chain, reference):
    policy = importlib.import_module("prismaquant.staged_tier_policy").active_policy()
    chain.allowed_tiers_at_prefetch = None if policy is None else sorted(policy)
    return chain.exact.prefetch_exact_activation_cache_entries(
        [reference], max_tensor_bytes=chain.nbytes, expected_session=chain.session,
        local_paths=None, release_file_pages=False)


def test_exact_activation_serves_ssd_without_origin_payload(chain, reader_policy):
    with reader_policy, chain.tripwire, _prefetch(chain, chain.covered) as window:
        assert chain.torch.equal(window._tensors[chain.covered], chain.tensor)
    report = chain.resolver.report()
    assert chain.tripwire.origin_opens == []
    assert chain.tripwire.staged_opens
    assert chain.tripwire.pins_at_open, "actual SDK lease must exist at actual staged open"
    serving = report["serving_tiers"][-1]
    assert serving["serving_tier"] == "stage" and serving["tier_id"] == chain.tier
    assert serving["pin_id"] and serving["range_ref"] == chain.key
    assert serving["lease_id"] == serving["pin_id"]
    assert any(pin["pin_id"] == serving["pin_id"] and pin["refs"]
               for pin in chain.tripwire.pins_at_open)
    assert report["bytes_from_stage"] == chain.covered.file_bytes
    assert report["bytes_from_pool"] == 0
    assert list(chain.leases.glob("*.lease.json")) == []


def test_valid_omitted_reference_refuses_before_origin_payload(chain, reader_policy):
    policy = importlib.import_module("prismaquant.staged_tier_policy")
    with (reader_policy, chain.tripwire,
          pytest.raises(policy.TierPolicyRefused, match="staged-not-serving"),
          _prefetch(chain, chain.omitted)):
        pytest.fail("omitted valid exact reference was exposed")
    assert chain.tripwire.origin_opens == []
    assert chain.tripwire.staged_opens == []
    assert chain.resolver.report()["bytes_from_pool"] == 0
    assert list(chain.leases.glob("*.lease.json")) == []


def test_old_inactive_boundary_is_independently_caught(chain):
    policy = importlib.import_module("prismaquant.staged_tier_policy")
    assert policy.active_policy() is None
    with (chain.tripwire,
          pytest.raises(OriginPayloadOpened, match="forbidden origin payload open"),
          _prefetch(chain, chain.covered)):
        pytest.fail("inactive origin read escaped the independent tripwire")
    assert chain.tripwire.origin_opens == [chain.covered.path]
    assert chain.tripwire.staged_opens == []
    assert list(chain.leases.glob("*.lease.json")) == []
    print("PB725_OLD_BOUNDARY_CAUGHT", flush=True)

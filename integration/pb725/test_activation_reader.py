"""Refs #725: exact-activation write -> real pinned mover -> actual reader.

Opt-in only via run_activation.py. RED leaves the integration policy inactive;
only OriginPayloadOpened is a behavioral RED witness, never setup/import error.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import os
import socket
import sys
from contextlib import nullcontext
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import pytest


class OriginPayloadOpened(AssertionError):
    """Independent CPython open audit tripwire, not a reader verdict."""


class PayloadTripwire:
    def __init__(self, origins, staged_path, leases):
        self.origins = {os.path.abspath(os.fspath(path)) for path in origins}
        self.staged_path = os.path.abspath(os.fspath(staged_path))
        self.leases = leases
        self.active = False
        self.busy = False
        self.origin_opens = []
        self.staged_opens = []
        self.pins_at_open = []
        sys.addaudithook(self.observe)

    def observe(self, event, args):
        if not self.active or self.busy or event != "open":
            return
        operand = args[0]
        if not isinstance(operand, (str, bytes, os.PathLike)):
            return
        path = os.path.abspath(os.fsdecode(operand))
        if path in self.origins:
            self.origin_opens.append(path)
            print("PB725_FORBIDDEN_ORIGIN_OPEN " + json.dumps({"path": path}), flush=True)
            raise OriginPayloadOpened(f"forbidden origin payload open: {path}")
        if path == self.staged_path:
            self.staged_opens.append(path)
            self.busy = True
            try:
                # Read only actual SDK records while its descriptor is opening.
                self.pins_at_open.extend(json.loads(p.read_text())
                                         for p in self.leases.glob("*.lease.json"))
            finally:
                self.busy = False

    def __enter__(self):
        self.active = True
        return self

    def __exit__(self, *_):
        self.active = False


@pytest.fixture
def reader_policy():
    """Activate the reviewed ram,ssd default for this bounded reader fixture."""
    from prismaquant.staged_tier_policy import (
        DEFAULT_ALLOWED_TIERS, staged_tier_policy_test_context,
    )
    return staged_tier_policy_test_context(DEFAULT_ALLOWED_TIERS)


@pytest.fixture
def chain(tmp_path, monkeypatch, request):
    if not os.environ.get("PB725_BINDING_PATH"):
        pytest.fail("opt-in setup missing: use the admitted run_activation.py launcher")
    binding = json.loads(Path(os.environ["PB725_BINDING_PATH"]).read_text())
    # Strip *all* live action/helper/progress/queue identity before injection
    # and before importing tools that capture ambient state. monkeypatch restores.
    for name in list(os.environ):
        if name.startswith("PRISMABUILD_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("PRISMAQUANT_LAYER_READ_THREADS", "1")
    import torch
    exact = importlib.import_module("prismaquant.perturbed_x_cache")
    pq_map = importlib.import_module("prismaquant.residency_map")
    lease = importlib.import_module("prismaquant.staged_lease")
    policy = importlib.import_module("prismaquant.staged_tier_policy")

    # Preserve the actual helper, injection, resolver and policy state. Nothing
    # in this private fixture may rebind a live SDK3 helper or live queue.
    helper_state = (lease._HELPER_ROOT, lease._INJECTED, lease._INJECTED_MODULES_BEFORE)
    resolver_state = pq_map._RESOLVER_FOR
    with policy._LOCK:
        policy_state = (policy._ACTIVE, policy._OWNER, list(policy._STACK))
    evidence = {"case": request.node.name, "binding_scope": binding["scope"]}
    installed_root = Path(os.environ["PB725_PACKAGE_ROOT"]).resolve()
    observed_chain = None
    try:
        assert policy.active_policy() is None, "fixture requires an inactive prior policy"
        lease.set_lease_helper_root(None)
        vars(lease).update(_INJECTED=None, _INJECTED_MODULES_BEFORE=None)
        client = lease.inject_installed_sdk_for_tests()
        assert binding["pb_sdk_version"] == client.SDK_VERSION
        core = importlib.import_module("prismabuild.core")
        pool = importlib.import_module("prismabuild.pool")
        material = importlib.import_module("prismabuild.reader_lease")
        pb_map = importlib.import_module("prismabuild.residency_map")
        stage_move = importlib.import_module("stage_move")
        tool_root = Path(os.environ["PB725_TOOLS_ROOT"]).resolve()
        assert Path(stage_move.__file__ or "").resolve() == tool_root / "tools/fleet/stage_move.py"
        assert not (tool_root / "src").exists()
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
        manifest = {"schema": "prismaquant.prismabuild.data_manifest.v1",
                    "produced_by": {"tool": "pb725-activation-integration"},
                    "annotations": {},
                    "mount_prefix": str(origin),
                    "entries": [{"path": covered.path, "offset": 0,
                                 "bytes": covered.file_bytes, "sha256": covered.sha256}],
                    "entry_count": 1, "total_bytes": covered.file_bytes}
        core.validate_data_manifest(manifest)
        raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        manifest_sha = hashlib.sha256(raw).hexdigest()
        manifest_file = tmp_path / "manifest.json"
        manifest_file.write_bytes(raw)
        queue = pool.PoolQueue(tmp_path / "queue")
        queue.ensure_layout()
        consumer = hashlib.sha256((str(tmp_path) + ":consumer").encode()).hexdigest()
        mover = hashlib.sha256((str(tmp_path) + ":mover").encode()).hexdigest()
        stage = tmp_path / "stage"
        stage.mkdir()
        residency = queue.root / pool.RESIDENCY
        cas = queue.root / "cas"
        blob = cas / "blobs" / manifest_sha[:2] / manifest_sha
        blob.parent.mkdir(parents=True)
        blob.write_bytes(raw)  # Metadata only, never activation payload copies.
        host, nonce, scope = socket.gethostname(), "pb725-private-nonce", "activation-only"
        claim = {"action_key": consumer, "claimed_by": f"{host}:{os.getpid()}:pb725",
                 "claimed_host": host, "cas_root": str(cas),
                 "residency": {"manifest_sha256": manifest_sha, "manifest_bytes": len(raw)},
                 "resource_scope": {"action_key": consumer, "nonce": nonce, "scope_id": scope}}
        queue.item_path(pool.CLAIMED, consumer).write_text(json.dumps(claim))
        args = stage_move.build_parser().parse_args([
            "--pool-root", str(queue.root), "--cas-root", str(cas),
            "--action-key", mover, "--consumer-action-key", consumer,
            "--tier-id", operands["tier_id"], "--stage-root", str(stage),
            "--manifest-sha256", manifest_sha, "--range-start-bytes", "0",
            "--range-end-bytes", str(covered.file_bytes), "--manifest", str(manifest_file),
            "--residency-root", str(residency), "--block", "65536",
            "--readers", "1", "--max-readers", "1", "--unpaced"])
        print("PB725_FIXTURE_PHASE mover-start " + request.node.name, flush=True)
        receipt = stage_move.move(args)
        print("PB725_FIXTURE_PHASE mover-return " + request.node.name, flush=True)
        assert receipt["complete"] is True and not receipt.get("refusal"), receipt
        queue.record_move(mover, receipt)
        fragments = pb_map.read_fragments(residency, consumer)
        assert len(fragments) == 1
        composed = pb_map.compose(fragments)
        assert composed["manifest_sha256"] == manifest_sha
        key = pb_map.residency_map_key(covered.path, 0)
        assert set(composed["entries"]) == {key}
        staged_path = Path(composed["entries"][key]["stage_path"])
        assert staged_path.is_relative_to(stage)
        assert hashlib.sha256(staged_path.read_bytes()).hexdigest() == covered.sha256
        material_path = material.material_path(residency, consumer, mover)
        material_record = json.loads(material_path.read_text())
        assert material_record["generation"]
        assert key in material_record["entries"]
        map_path = residency / f"{consumer}.map.json"
        map_path.write_text(json.dumps(composed))
        for name, value in {"PRISMABUILD_QUEUE_ROOT": str(queue.root),
                            "PRISMABUILD_ACTION_KEY": consumer,
                            "PRISMABUILD_ACTION_NONCE": nonce,
                            "PRISMABUILD_ACTION_SCOPE": scope,
                            pq_map.ENV_VAR: str(map_path)}.items():
            monkeypatch.setenv(name, value)
        pq_map.reset_residency_resolver_for_tests()
        pq_map.bind_residency_manifest(manifest_sha)
        resolver = pq_map.residency_resolver()
        assert resolver is not None
        assert client.injected_context()["ok"] is True
        assert resolver.declared_readset()["state"] == "bound", resolver.declared_readset()
        assert resolver.staged_read(Path(covered.path), expected_sha256=covered.sha256)
        assert resolver.staged_read(Path(omitted.path), expected_sha256=omitted.sha256) is None
        leases = residency / "leases" / consumer
        tripwire = PayloadTripwire([ref.path for ref in refs], staged_path, leases)
        evidence.update({"manifest_sha256": manifest_sha, "manifest": manifest,
                         "claim": claim, "mover_receipt": receipt, "fragments": fragments,
                         "material": material_record, "composed_map": composed,
                         "reference_sha256": covered.sha256, "omitted_sha256": omitted.sha256,
                         "exact_references": [asdict(ref) for ref in refs],
                         "setup_complete": True})
        print("PB725_SETUP_COMPLETE " + json.dumps({"case": request.node.name,
              "manifest_sha256": manifest_sha, "material_generation": material_record["generation"]}),
              flush=True)
        observed_chain = SimpleNamespace(
            exact=exact, torch=torch, tensor=tensor, covered=covered,
            omitted=omitted, nbytes=nbytes, session=session, resolver=resolver,
            tripwire=tripwire, leases=leases, key=key, tier=operands["tier_id"])
        yield observed_chain
    finally:
        try:
            _persist_case(evidence, observed_chain, installed_root, request.node.name)
        finally:
            lease.clear_injected_sdk_for_tests()
            with lease._HELPER_LOCK:
                for name, value in zip(("_HELPER_ROOT", "_INJECTED", "_INJECTED_MODULES_BEFORE"),
                                       helper_state, strict=True):
                    setattr(lease, name, value)
            with pq_map._RESOLVER_LOCK:
                vars(pq_map)["_RESOLVER_FOR"] = resolver_state
            with policy._LOCK:
                active, owner, stack = policy_state
                vars(policy).update(_ACTIVE=active, _OWNER=owner)
                policy._STACK[:] = stack


def _persist_case(evidence, chain, installed_root, case_name):
    if chain is not None:
        chain.tripwire.active = False
        evidence.update({"origin_opens": chain.tripwire.origin_opens,
                         "staged_opens": chain.tripwire.staged_opens,
                         "pins_at_open": chain.tripwire.pins_at_open,
                         "reader_report": chain.resolver.report(),
                         "allowed_tiers_at_prefetch": getattr(chain, "allowed_tiers_at_prefetch", None),
                         "leases_after_window": [p.name for p in chain.leases.glob("*.lease.json")]})
    origins = {}
    pq_root = Path(os.environ["PB725_PQ_ROOT"]) / "prismaquant"
    tool_root = Path(os.environ["PB725_TOOLS_ROOT"])
    for name, module in list(sys.modules.items()):
        filename = getattr(module, "__file__", None)
        if name == "prismabuild" or name.startswith("prismabuild."):
            expected = installed_root
        elif name == "prismaquant" or name.startswith("prismaquant."):
            expected = pq_root
        elif ((tool_root / "tools/fleet" / (name.split(".")[0] + ".py")).is_file()
              or (filename and Path(filename).resolve().is_relative_to(tool_root))):
            expected = tool_root
        else:
            continue
        location = Path(filename or "").resolve()
        assert location.is_relative_to(expected), (name, location, expected)
        origins[name] = str(location)
    evidence["behavioral_import_origins"] = origins
    output = Path(os.environ["PB725_PRIVATE_ROOT"]) / f"{case_name}.json"
    output.write_text(json.dumps(evidence, sort_keys=True, indent=2))
    print("PB725_CASE_EVIDENCE " + json.dumps(evidence, sort_keys=True), flush=True)


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

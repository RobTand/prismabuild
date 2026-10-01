"""Private SDK1 queue/mover/map fixture infrastructure; no domain producers."""
from __future__ import annotations

import hashlib
import importlib
import json
import os
import socket
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace


class OriginPayloadOpened(AssertionError):
    """Independent actual-open observation, not a reader verdict."""


class PayloadTripwire:
    def __init__(self, origins, staged_path, leases):
        self.origins = {os.path.abspath(os.fspath(path)) for path in origins}
        paths = [staged_path] if isinstance(staged_path, (str, os.PathLike)) else staged_path
        self.staged_paths = {os.path.abspath(os.fspath(path)) for path in paths}
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
        if path in self.staged_paths:
            self.staged_opens.append(path)
            self.busy = True
            try:
                self.pins_at_open.extend(json.loads(p.read_text())
                                         for p in self.leases.glob("*.lease.json"))
            finally:
                self.busy = False

    def __enter__(self):
        self.active = True
        return self

    def __exit__(self, *_):
        self.active = False


@contextmanager
def private_runtime(monkeypatch, binding):
    # Strip live identity before importing tools that capture ambient state.
    for name in list(os.environ):
        if name.startswith("PRISMABUILD_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("PRISMAQUANT_LAYER_READ_THREADS", "1")
    pq_map = importlib.import_module("prismaquant.residency_map")
    lease = importlib.import_module("prismaquant.staged_lease")
    policy = importlib.import_module("prismaquant.staged_tier_policy")
    helper_state = (lease._HELPER_ROOT, lease._INJECTED, lease._INJECTED_MODULES_BEFORE)
    resolver_state = pq_map._RESOLVER_FOR
    with policy._LOCK:
        policy_state = (policy._ACTIVE, policy._OWNER, list(policy._STACK))
    try:
        assert policy.active_policy() is None, "fixture requires an inactive prior policy"
        lease.set_lease_helper_root(None)
        vars(lease).update(_INJECTED=None, _INJECTED_MODULES_BEFORE=None)
        client = lease.inject_installed_sdk_for_tests()
        assert binding["pb_sdk_version"] == client.SDK_VERSION
        modules = {name: importlib.import_module("prismabuild." + name)
                   for name in ("core", "pool", "reader_lease", "residency_map")}
        stage_move = importlib.import_module("stage_move")
        tool_root = Path(os.environ["PB725_TOOLS_ROOT"]).resolve()
        assert Path(stage_move.__file__ or "").resolve() == tool_root / "tools/fleet/stage_move.py"
        assert not (tool_root / "src").exists()
        yield SimpleNamespace(client=client, pq_map=pq_map, stage_move=stage_move,
                              core=modules["core"], pool=modules["pool"],
                              material=modules["reader_lease"], pb_map=modules["residency_map"])
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


def move_manifest(runtime, tmp_path, monkeypatch, *, entries, origin, tier, scope, producer,
                  case_name, evidence):
    core, pool, material, pb_map = (runtime.core, runtime.pool, runtime.material, runtime.pb_map)
    total = sum(row["bytes"] for row in entries)
    manifest = {"schema": "prismaquant.prismabuild.data_manifest.v1",
                "produced_by": {"tool": producer}, "annotations": {},
                "mount_prefix": str(origin), "entries": entries,
                "entry_count": len(entries), "total_bytes": total}
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
    blob.write_bytes(raw)  # Metadata only: payload moves exclusively through stage_move.
    host, nonce = socket.gethostname(), "pb725-private-nonce"
    claim = {"action_key": consumer, "claimed_by": f"{host}:{os.getpid()}:pb725",
             "claimed_host": host, "cas_root": str(cas),
             "residency": {"manifest_sha256": manifest_sha, "manifest_bytes": len(raw)},
             "resource_scope": {"action_key": consumer, "nonce": nonce, "scope_id": scope}}
    queue.item_path(pool.CLAIMED, consumer).write_text(json.dumps(claim))
    args = runtime.stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root), "--cas-root", str(cas),
        "--action-key", mover, "--consumer-action-key", consumer,
        "--tier-id", tier, "--stage-root", str(stage),
        "--manifest-sha256", manifest_sha, "--range-start-bytes", "0",
        "--range-end-bytes", str(total), "--manifest", str(manifest_file),
        "--residency-root", str(residency), "--block", "65536",
        "--readers", "1", "--max-readers", "1", "--unpaced",
        # This tiny private fixture serves no storage-role clients. In the
        # pinned mover, --unpaced skips the topology requirement but still
        # constructs its pacer; explicitly supply the supported empty pool
        # to avoid discovering and waiting on unrelated host ZFS devices.
        "--pace-pool", "", "--disks", "", "--nfsd-io", ""])
    print("PB725_FIXTURE_PHASE mover-start " + case_name, flush=True)
    receipt = runtime.stage_move.move(args)
    print("PB725_FIXTURE_PHASE mover-return " + case_name, flush=True)
    assert receipt["complete"] is True and not receipt.get("refusal"), receipt
    queue.record_move(mover, receipt)
    fragments = pb_map.read_fragments(residency, consumer)
    assert len(fragments) == 1
    composed = pb_map.compose(fragments)
    assert composed["manifest_sha256"] == manifest_sha
    keys = {pb_map.residency_map_key(row["path"], row["offset"]) for row in entries}
    assert set(composed["entries"]) == keys
    staged_paths = []
    for entry in composed["entries"].values():
        staged_path = Path(entry["stage_path"])
        assert staged_path.is_relative_to(stage)
        assert hashlib.sha256(staged_path.read_bytes()).hexdigest() == entry["sha256"]
        staged_paths.append(staged_path)
    material_record = json.loads(material.material_path(residency, consumer, mover).read_text())
    assert material_record["generation"]
    assert keys <= set(material_record["entries"])
    map_path = residency / f"{consumer}.map.json"
    map_path.write_text(json.dumps(composed))
    for name, value in {"PRISMABUILD_QUEUE_ROOT": str(queue.root),
                        "PRISMABUILD_ACTION_KEY": consumer,
                        "PRISMABUILD_ACTION_NONCE": nonce, "PRISMABUILD_ACTION_SCOPE": scope,
                        runtime.pq_map.ENV_VAR: str(map_path)}.items():
        monkeypatch.setenv(name, value)
    runtime.pq_map.reset_residency_resolver_for_tests()
    runtime.pq_map.bind_residency_manifest(manifest_sha)
    resolver = runtime.pq_map.residency_resolver()
    assert resolver is not None
    assert runtime.client.injected_context()["ok"] is True
    assert resolver.declared_readset()["state"] == "bound", resolver.declared_readset()
    evidence.update({"manifest_sha256": manifest_sha, "manifest": manifest,
                     "claim": claim, "mover_receipt": receipt, "fragments": fragments,
                     "material": material_record, "composed_map": composed})
    return SimpleNamespace(resolver=resolver, staged_paths=staged_paths,
                           leases=residency / "leases" / consumer, keys=keys, tier=tier)


def setup_complete(evidence, case_name):
    evidence["setup_complete"] = True
    print("PB725_SETUP_COMPLETE " + json.dumps({"case": case_name,
          "manifest_sha256": evidence["manifest_sha256"],
          "material_generation": evidence["material"]["generation"]}), flush=True)


def persist_case(evidence, chain, installed_root, case_name):
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

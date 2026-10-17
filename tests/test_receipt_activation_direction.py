"""Receipt source transport never substitutes for current installed authority."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from prismabuild.compatibility_window import WindowError, require_reachable
from generation_fixture import generation_receipt
from receipt_reader_fixture import RECEIPT_LEAVES, prepared_receipt_bridge, receipt_source_artifact
from test_rollout_coordinator import publisher

ROOT = Path(__file__).resolve().parents[1]


def source(kind):
    if kind == "bridge":
        return prepared_receipt_bridge()
    if kind == "old":
        return receipt_source_artifact("base/tools/fleet/upgrade_client.py")
    return (ROOT / "tools/fleet/upgrade_client.py").read_bytes()


def generation(store, name, kind, leaves):
    root = store / name
    files = {"tools/upgrade_client.py": source(kind),
             "tools/fleet/publish_runtime.py": (ROOT / "tools/fleet/publish_runtime.py").read_bytes(),
             "tools/fleet/fleet_boxes.json": b'{"boxes":{"one":{},"two":{}}}'}
    files.update({member: (ROOT / member).read_bytes() for leaf, member in RECEIPT_LEAVES.items()
                  if leaf in leaves})
    for member, raw in files.items():
        path = root / member
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    receipt = generation_receipt(name, "a" * 40,
                                 {member: hashlib.sha256(raw).hexdigest() for member, raw in files.items()})
    (root / "RUNTIME_VERSION.json").write_text(json.dumps(receipt))
    return root


@pytest.fixture
def private_store(tmp_path, monkeypatch):
    store = tmp_path / "fleet/runtime-generations"
    store.mkdir(parents=True)
    monkeypatch.setattr(publisher, "MIRROR", store.parent / "repo")
    monkeypatch.setattr(publisher, "CHECKOUT", ROOT)
    yield store
    for path in store.rglob("*"):
        if path.is_dir():
            path.chmod(0o755)


@pytest.mark.parametrize("before,after,prior_leaves,next_leaves,error", [
    ("bridge", "bridge", (), tuple(RECEIPT_LEAVES), None),
    ("bridge", "bridge", tuple(RECEIPT_LEAVES), (), None),
    ("bridge", "final", tuple(RECEIPT_LEAVES), tuple(RECEIPT_LEAVES), None),
    ("old", "final", (), tuple(RECEIPT_LEAVES), "receipt-member"),
    ("final", "old", tuple(RECEIPT_LEAVES), (), "receipt-member"),
    ("bridge", "final", (), (), "requires both"),
    ("bridge", "bridge", (), ("generation_receipt.py",), "incomplete receipt"),
    ("bridge", "bridge", (), ("digest_primitives.py",), "incomplete receipt"),
])
def test_exact_source_transport_direction(private_store, before, after, prior_leaves, next_leaves, error):
    previous = generation(private_store, "previous", before, prior_leaves)
    target = generation(private_store, "target", after, next_leaves)
    if error:
        with pytest.raises(WindowError, match=error):
            require_reachable(target, previous)
    else:
        assert require_reachable(target, previous)["physical_rollback"] == "separate qualification required"


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize("installed_source", ["old", "bridge"])
def test_rolling_activation_needs_current_installed_receipt_authority(private_store, dry_run, installed_source):
    previous = generation(private_store, "previous", installed_source,
                          tuple(RECEIPT_LEAVES) if installed_source == "bridge" else ())
    target = generation(private_store, "target", "final", tuple(RECEIPT_LEAVES))
    for root in (previous, target):
        publisher._seal_generation(root)
    publisher.MIRROR.symlink_to(previous)
    agent = publisher._agent_definitions()
    for host in ("one", "two"):
        for raw in (source(installed_source), source("final")):
            digest = hashlib.sha256(raw).hexdigest()
            agent.post_marker(private_store.parent / "rollout", "agents/" + agent.attestation_name(host, digest),
                              agent.canonical_json(agent.attestation_body(host, digest)))
    before = set(private_store.iterdir())
    with pytest.raises(SystemExit, match="receipt-member|current installed updater"):
        publisher._activate_existing(target.name, dry_run=dry_run, rollout="rolling",
                                     rollout_reason="private direction control")
    assert publisher.MIRROR.resolve() == previous
    assert set(private_store.iterdir()) == before


def publication_args(*, dry_run, stage_only=False):
    return SimpleNamespace(rollout="rolling", rollout_reason="private direction control",
                           resume_barrier=None, rollback_barrier=None, rollback_reason=None,
                           dry_run=dry_run, stage_only=stage_only, activate_generation=None,
                           canary=False, no_canary=True, allow_dirty=False, default_transport=None,
                           migrate_directory=False, barrier_wait_s=0,
                           shape_gate_action=None, shape_gate_waiver="private source qualification",
                           drop_roster_option_by=None, drop_roster_option_reason=None)


@pytest.mark.parametrize("dry_run", [False, True])
def test_fresh_publication_refuses_unaware_installed_reader_before_writes(private_store, dry_run):
    previous = generation(private_store, "previous", "old", ())
    publisher._seal_generation(previous)
    publisher.MIRROR.symlink_to(previous)
    before = set(private_store.iterdir())
    with pytest.raises(SystemExit, match="receipt-member|bridge rollback target"):
        publisher._run_publication(publication_args(dry_run=dry_run))
    assert publisher.MIRROR.resolve() == previous
    assert set(private_store.iterdir()) == before

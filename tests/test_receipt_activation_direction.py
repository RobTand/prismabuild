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
    import time
    offers = private_store.parent / "pb-queue/workers"
    offers.mkdir(parents=True)
    for host in ("one", "two"):
        (offers / f"{host}.json").write_text(json.dumps({
            "schema": "prismaquant.prismabuild.pool_offer.v1", "host": host,
            "announced_unix": time.time()}))
        for raw in (source(installed_source), source("final")):
            digest = hashlib.sha256(raw).hexdigest()
            agent.post_marker(private_store.parent / "rollout", "agents/" + agent.attestation_name(host, digest),
                              agent.canonical_json(agent.attestation_body(host, digest)))
    before = set(private_store.iterdir())
    # Historical attestations are not current proof: every participant is named.
    with pytest.raises(SystemExit, match=r"receipt-member|current installed updater proof.*\['one', 'two'\]"):
        publisher._activate_existing(target.name, dry_run=dry_run, rollout="rolling",
                                     rollout_reason="private direction control")
    assert publisher.MIRROR.resolve() == previous
    assert set(private_store.iterdir()) == before


def publication_args(*, dry_run, stage_only=False):
    return SimpleNamespace(rollout="rolling", rollout_reason="private direction control",
                           resume_barrier=None, rollback_barrier=None, rollback_reason=None,
                           dry_run=dry_run, stage_only=stage_only, activate_generation=None,
                           canary=False, no_canary=True, allow_dirty=True, default_transport=None,
                           migrate_directory=False, barrier_wait_s=0,
                           shape_gate_action=None, shape_gate_waiver="private source qualification",
                           drop_roster_option_by=None, drop_roster_option_reason=None)


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize("candidate_dirty", [False, True])
def test_fresh_publication_refuses_unaware_installed_reader_before_writes(
        private_store, monkeypatch, dry_run, candidate_dirty):
    monkeypatch.setattr(publisher, "_commit_identity", lambda: "a" * 40)
    monkeypatch.setattr(publisher, "_working_tree_dirty", lambda: candidate_dirty)
    previous = generation(private_store, "previous", "old", ())
    publisher._seal_generation(previous)
    publisher.MIRROR.symlink_to(previous)
    before = set(private_store.iterdir())
    with pytest.raises(SystemExit, match="receipt-member|bridge rollback target"):
        publisher._run_publication(publication_args(dry_run=dry_run))
    assert publisher.MIRROR.resolve() == previous
    assert set(private_store.iterdir()) == before


@pytest.mark.parametrize("remove_pair", [False, True])
def test_same_bridge_pair_transition_requires_its_current_complete_epoch(private_store, monkeypatch, remove_pair):
    import time
    from test_rollout_coordinator import post_host

    bridge = prepared_receipt_bridge()
    previous = generation(private_store, "old", "bridge", tuple(RECEIPT_LEAVES) if remove_pair else ())
    target = generation(private_store, "new", "bridge", () if remove_pair else tuple(RECEIPT_LEAVES))
    for root in (previous, target):
        publisher._seal_generation(root)
    publisher.MIRROR.symlink_to(previous)
    checkout = private_store.parent / "bridge-source"
    updater = checkout / "tools/fleet/upgrade_client.py"
    updater.parent.mkdir(parents=True)
    updater.write_bytes(bridge)
    monkeypatch.setattr(publisher, "CHECKOUT", checkout)
    monkeypatch.setattr(publisher, "FINAL_BARRIER_QUALIFICATION_GUARD", False)
    offers = private_store.parent / "pb-queue/workers"
    offers.mkdir(parents=True)
    for host in ("one", "two"):
        (offers / (host + ".json")).write_text(json.dumps({
            "schema": "prismaquant.prismabuild.pool_offer.v1", "host": host,
            "announced_unix": time.time()}))
    agent = publisher._agent_definitions()
    digest = hashlib.sha256(bridge).hexdigest()
    for host in ("one", "two"):
        agent.post_marker(private_store.parent / "rollout", "agents/" + agent.attestation_name(host, digest),
                          agent.canonical_json(agent.attestation_body(host, digest)))
    assert publisher._activate_existing("new", dry_run=False, wait_s=0) == 75
    epoch = publisher._rollout_view()["intent"]["epoch"]
    post_host(epoch, "one", "drained")
    assert publisher._barrier_step(epoch)["missing"] == ["two"]
    assert publisher.MIRROR.resolve() == previous
    post_host(epoch, "two", "drained")
    assert publisher._barrier_step(epoch)["state"] == "activated"
    assert publisher.MIRROR.resolve() == target
    post_host(epoch, "one", "rotated")
    assert publisher._barrier_step(epoch)["missing"] == ["two"]
    assert "resume.json" not in publisher._rollout_view(epoch)["markers"]
    post_host(epoch, "two", "rotated")
    assert publisher._barrier_step(epoch)["state"] == "resume_authorized"
    for host in ("one", "two"):
        post_host(epoch, host, "resumed")
    assert publisher._barrier_step(epoch)["state"] == "completed"


def test_final_updater_does_not_relax_the_same_agent_barrier(private_store, monkeypatch):
    previous = generation(private_store, "previous", "bridge", tuple(RECEIPT_LEAVES))
    target = generation(private_store, "target", "final", tuple(RECEIPT_LEAVES))
    for root in (previous, target):
        publisher._seal_generation(root)
    publisher.MIRROR.symlink_to(previous)
    monkeypatch.setattr(publisher, "FINAL_BARRIER_QUALIFICATION_GUARD", False)
    assert require_reachable(target, previous)
    with pytest.raises(SystemExit, match="same rollout-aware updater"):
        publisher._activate_existing("target", dry_run=False, wait_s=0)
    assert publisher.MIRROR.resolve() == previous
    assert not (private_store.parent / "rollout/epochs").exists()


def test_predeploy_refuses_the_actual_a174_direction_before_issuing_approval(tmp_path):
    import runpy
    import sys
    import time
    from prismabuild.compatibility_window import content_digest

    sys.path.insert(0, str(ROOT / "tests/compat/support"))
    from record_compat import retained

    validate = runpy.run_path(str(ROOT / "tests/compat/support/compat_predeploy.py"))["validate"]
    window = ROOT / "tests/compat/generations/window.json"
    declared = json.loads(window.read_bytes())["rollback"]
    with retained() as generations:
        name, old, _ = generations[0]
        assert name == declared["generation"]
        receipt_raw = (old / "RUNTIME_VERSION.json").read_bytes()
        receipt = json.loads(receipt_raw)
        designation = tmp_path / "designation.json"
        designation.write_text(json.dumps({"rollback": {**declared, "directory": str(old.resolve()),
            "receipt_sha256": hashlib.sha256(receipt_raw).hexdigest(),
            "content_sha256": content_digest(receipt["files"])}}))
        raw = json.dumps({"complete": True, "nodes": [
            {"node": "one", "state": "live", "runtime_commit": declared["commit"]}]}).encode()
        (tmp_path / "status.json").write_bytes(raw)
        census = tmp_path / "census.json"
        census.write_text(json.dumps({"status_file": "status.json", "status_sha256": hashlib.sha256(raw).hexdigest(),
                                      "status_exit": 0, "captured_unix": time.time()}))
        out = tmp_path / "predeploy-approval.json"
        args = SimpleNamespace(census=census, window=window, designation=designation,
                               generations=old.parent, candidate=ROOT, out=out)
        # The retained source archive preserves the actual full receipt;
        # omitted publication docs cannot stand in for the deployed directory.
        with pytest.raises(FileNotFoundError, match="SKILL.md"):
            validate(args)
        with pytest.raises(WindowError, match="receipt-member"):
            require_reachable(ROOT, old)
        assert not out.exists()


def test_installed_bootstrap_does_not_change_automatic_transport(tmp_path, monkeypatch):
    from bootstrap_fixture import bootstrap_case, assess_case
    case = bootstrap_case(tmp_path, monkeypatch)
    assert assess_case(case)['automatic_reachability'] is False
    with pytest.raises(WindowError, match='receipt-member'):
        require_reachable(Path(case[0]['target']['directory']), Path(case[0]['source']['directory']))
    case[1].rows['one']['executing_sha256'] = case[0]['source_files']['upgrade_client.py']
    with pytest.raises(WindowError, match='reader/leaf closure'):
        assess_case(case)

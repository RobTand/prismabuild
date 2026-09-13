"""A publisher must not bypass a fleet epoch through its rolling path."""
from __future__ import annotations

import importlib.util
import hashlib
import json
import os
from pathlib import Path
import time

import pytest


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "rollout_publisher_test", ROOT / "tools/fleet/publish_runtime.py")
publisher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)


@pytest.mark.parametrize("intent_bytes", [b"{broken", b"{}"])
def test_rolling_activation_cannot_bypass_unresolved_epoch(tmp_path, monkeypatch, intent_bytes):
    fleet = tmp_path / "fleet"
    store = fleet / "runtime-generations"
    old, new = store / "old", store / "new"
    for generation in (old, new):
        generation.mkdir(parents=True)
        (generation / "RUNTIME_VERSION.json").write_text(json.dumps({
            "schema": "prismaquant.prismabuild.runtime_version.v1",
            "generation": generation.name, "commit": "a" * 40, "files": {}}))
    mirror = fleet / "repo"
    mirror.symlink_to(old)
    epoch = fleet / "rollout/epochs" / ("a" * 32)
    epoch.mkdir(parents=True)
    (epoch / "intent.json").write_bytes(intent_bytes)
    monkeypatch.setattr(publisher, "MIRROR", mirror)
    with pytest.raises(SystemExit, match="rollout|epoch|barrier"):
        publisher._activate_existing("new", dry_run=False, rollout="rolling",
                                     rollout_reason="private regression")
    assert mirror.resolve() == old


@pytest.fixture
def fleet(tmp_path, monkeypatch):
    root = tmp_path / "fleet"
    store = root / "runtime-generations"
    updater = ROOT / "tools/fleet/upgrade_client.py"
    files = {"tools/upgrade_client.py": updater.read_bytes(),
             "tools/fleet/publish_runtime.py": (ROOT / "tools/fleet/publish_runtime.py").read_bytes(),
             "tools/fleet/fleet_boxes.json": json.dumps({"boxes": {
                 "one": {}, "former-two": {"_alias": "two"}}}).encode()}
    for generation in ("old", "new"):
        directory = store / generation
        for name, raw in files.items():
            path = directory / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            path.chmod(0o444)
        (directory / "RUNTIME_VERSION.json").write_text(json.dumps({
            "schema": "prismaquant.prismabuild.runtime_version.v1",
            "generation": generation, "commit": ("a" if generation == "old" else "b") * 40,
            "files": {name: hashlib.sha256(raw).hexdigest() for name, raw in files.items()}}))
        (directory / "RUNTIME_VERSION.json").chmod(0o444)
        for path in sorted(directory.rglob("*"), reverse=True):
            if path.is_dir():
                path.chmod(0o555)
        directory.chmod(0o555)
    (root / "repo").symlink_to(store / "old")
    offers = root / "pb-queue/workers"
    offers.mkdir(parents=True)
    for host in ("one", "two"):
        (offers / (host + ".json")).write_text(json.dumps({
            "schema": "prismaquant.prismabuild.pool_offer.v1",
            "host": host, "announced_unix": time.time()}))
    monkeypatch.setattr(publisher, "MIRROR", root / "repo")
    monkeypatch.setattr(publisher, "CHECKOUT", ROOT)
    # This private fixture exercises the internal state machine.  The public
    # CLI remains qualification-guarded everywhere.
    monkeypatch.setattr(publisher, "FINAL_BARRIER_QUALIFICATION_GUARD", False)
    agent = publisher._agent_definitions()
    sha = hashlib.sha256(files["tools/upgrade_client.py"]).hexdigest()
    for host in ("one", "two"):
        agent.post_marker(root / "rollout", "agents/" + agent.attestation_name(host, sha),
                          agent.canonical_json(agent.attestation_body(host, sha)))
    yield root
    # tmp_path's own cleanup needs writable private fixture directories.
    for path in root.rglob("*"):
        if path.is_dir():
            path.chmod(0o755)


def arm(fleet):
    assert publisher._activate_existing("new", dry_run=False, wait_s=0) == 75
    return publisher._rollout_view()["intent"]["epoch"]


def post_host(epoch, host, phase, **extra):
    view = publisher._rollout_view(epoch)
    agent = publisher._agent_definitions()
    generation = "old" if phase in ("drained", "rolled-back") else "new"
    if phase == "resumed":
        generation = view["markers"]["resume.json"]["generation"]
    value = agent.make_marker(view["intent"], phase, host=host, generation=generation,
                              posted_unix=time.time(), active_scopes=0,
                              drain_changed_unix=458.0, rollout_protocol=1,
                              installed_agent_sha256=view["intent"]["agent_sha256"],
                              **extra)
    name = agent.marker_name(host, phase)
    agent.validate_marker(name, value, view["intent"], view["intent_sha256"])
    assert agent.post_marker(publisher.MIRROR.parent / "rollout", f"epochs/{epoch}/{name}",
                             agent.canonical_json(value))


def advance_to_rotation(fleet):
    epoch = arm(fleet)
    for host in ("one", "two"):
        post_host(epoch, host, "drained")
    assert publisher._barrier_step(epoch)["state"] == "activated"
    assert publisher.MIRROR.resolve().name == "new"
    return epoch


def test_missing_participants_never_swap_or_resume_even_after_wait_expiry(fleet):
    epoch = arm(fleet)
    post_host(epoch, "one", "drained")
    assert publisher._wait_barrier(epoch, wait_s=0) == 75
    assert publisher.MIRROR.resolve().name == "old"
    view = publisher._rollout_view(epoch)
    assert set(view["markers"]) == {"one.drained.json"}
    with pytest.raises(SystemExit, match="remains active"):
        publisher._activate_existing("new", dry_run=False, rollout="rolling",
                                     rollout_reason="a competing publisher")


def test_both_quorums_are_recorded_before_resumption(fleet):
    epoch = advance_to_rotation(fleet)
    post_host(epoch, "one", "rotated")
    assert publisher._barrier_step(epoch)["missing"] == ["two"]
    assert "resume.json" not in publisher._rollout_view(epoch)["markers"]
    post_host(epoch, "two", "rotated")
    assert publisher._barrier_step(epoch)["state"] == "resume_authorized"
    view = publisher._rollout_view(epoch)
    assert view["markers"]["activated.json"]["observed"] == publisher._quorum(view, "drained")[0]
    assert view["markers"]["resume.json"]["observed"] == publisher._quorum(view, "rotated")[0]
    post_host(epoch, "one", "resumed")
    assert publisher._barrier_step(epoch)["state"] == "resuming"
    post_host(epoch, "two", "resumed")
    assert publisher._barrier_step(epoch)["state"] == "completed"
    assert publisher._rollout_view() is None


def test_failed_rotation_rolls_back_at_the_same_barrier(fleet):
    epoch = advance_to_rotation(fleet)
    post_host(epoch, "one", "failed", error="injected member copy failure")
    assert publisher._barrier_step(epoch)["state"] == "rollback_declared"
    assert publisher.MIRROR.resolve().name == "new"
    assert publisher._barrier_step(epoch)["state"] == "reverted"
    assert publisher.MIRROR.resolve().name == "old"
    post_host(epoch, "one", "rolled-back")
    assert publisher._barrier_step(epoch)["missing"] == ["two"]
    post_host(epoch, "two", "rolled-back")
    assert publisher._barrier_step(epoch)["state"] == "resume_authorized"
    for host in ("one", "two"):
        post_host(epoch, host, "resumed")
    assert publisher._barrier_step(epoch)["state"] == "rolled_back"
    assert publisher._wait_barrier(epoch, wait_s=0) == 1


def test_resume_decision_prevents_a_late_rollback(fleet):
    epoch = advance_to_rotation(fleet)
    for host in ("one", "two"):
        post_host(epoch, host, "rotated")
    publisher._barrier_step(epoch)
    with pytest.raises(SystemExit, match="after the fleet resume"):
        publisher._barrier_step(epoch, rollback_reason="too late")
    assert publisher.MIRROR.resolve().name == "new"
    assert "rollback.json" not in publisher._rollout_view(epoch)["markers"]


def test_coordinator_recovers_a_swap_before_its_activation_record(fleet, monkeypatch):
    epoch = arm(fleet)
    for host in ("one", "two"):
        post_host(epoch, host, "drained")
    decision = publisher._decision
    monkeypatch.setattr(publisher, "_decision", lambda *a, **k: (_ for _ in ()).throw(OSError("crash")))
    with pytest.raises(OSError, match="crash"):
        publisher._barrier_step(epoch)
    assert publisher.MIRROR.resolve().name == "new"
    monkeypatch.setattr(publisher, "_decision", decision)
    assert publisher._barrier_step(epoch)["state"] == "activated"


def test_a_third_generation_fences_recovery(fleet):
    epoch = arm(fleet)
    third = fleet / "runtime-generations/third"
    third.mkdir()
    publisher.MIRROR.unlink()
    publisher.MIRROR.symlink_to(third)
    with pytest.raises(SystemExit, match="rollout|generation"):
        publisher._barrier_step(epoch)
    assert publisher.MIRROR.resolve() == third


def test_stale_epoch_marker_cannot_supply_a_quorum(fleet):
    epoch = arm(fleet)
    post_host(epoch, "one", "drained")
    path = fleet / "rollout/epochs" / epoch / "one.drained.json"
    value = json.loads(path.read_text())
    value["epoch"] = "0" * 32
    path.write_text(json.dumps(value))
    with pytest.raises(SystemExit, match="rollout"):
        publisher._barrier_step(epoch)
    assert publisher.MIRROR.resolve().name == "old"


def test_lock_contention_refuses_before_epoch_creation(fleet, monkeypatch):
    monkeypatch.setattr(publisher.fcntl, "lockf", lambda *a: (_ for _ in ()).throw(BlockingIOError()))
    with pytest.raises(SystemExit, match="publication lock"):
        publisher._activate_existing("new", dry_run=False, wait_s=0)
    assert not (fleet / "rollout/epochs").exists()


def test_roster_alias_is_resolved_from_live_offers_and_frozen(fleet):
    epoch = arm(fleet)
    assert publisher._rollout_view(epoch)["intent"]["roster"] == ["one", "two"]


def test_undeclared_live_host_refuses_arming(fleet):
    (fleet / "pb-queue/workers/stranger.json").write_text(json.dumps({
        "schema": "prismaquant.prismabuild.pool_offer.v1", "host": "stranger",
        "announced_unix": time.time()}))
    with pytest.raises(SystemExit, match="undeclared live"):
        publisher._activate_existing("new", dry_run=False, wait_s=0)
    assert not (fleet / "rollout/epochs").exists()

"""A client updater participates in a durable rollout epoch.

All fixtures are private directories and a fake broker.  No host service,
maintenance gate, or published generation is changed by these tests.
"""
import json
from pathlib import Path
import shutil
import socket

import pytest

from test_client_upgrade import CLIENT, setup, upgrade


def _intent(updater, generation, *, epoch="4" * 32, target="target-generation"):
    agent_bytes = Path(upgrade.__file__).read_bytes()
    updater_sha = upgrade.digest(agent_bytes)
    coordinator_sha = upgrade.digest(
        (Path(__file__).resolve().parents[1] / "tools/fleet/publish_runtime.py").read_bytes())
    member = upgrade.MEMBERS["upgrade_client.py"]
    (generation / member).write_bytes(agent_bytes)
    (updater.install / "upgrade_client.py").write_bytes(agent_bytes)
    receipt = json.loads((generation / "RUNTIME_VERSION.json").read_text())
    receipt["files"][member] = updater_sha
    (generation / "RUNTIME_VERSION.json").write_text(json.dumps(receipt))
    value = {
        "schema": "prismabuild.rollout_barrier.intent.v1",
        "epoch": epoch,
        "from_generation": generation.name,
        "to_generation": target,
        "roster": [socket.gethostname()],
        "agent_sha256": updater_sha,
        "coordinator_sha256": coordinator_sha,
        "drain_policy": "wait",
        "armed_unix": 458.0,
        "armed_by": "rollout-test",
    }
    epoch_root = upgrade.rollout_root(updater.config) / "epochs" / epoch
    epoch_root.mkdir(parents=True)
    (epoch_root / "intent.json").write_bytes(upgrade.canonical_json(value))
    return value


def _durable(updater):
    original_rpc = updater.rpc

    def durable_rpc(endpoint, operation, **fields):
        return {**original_rpc(endpoint, operation, **fields),
                "maintenance_durable_protocol": 1,
                "maintenance_state_path": "/private/maintenance.json"}

    updater.rpc = durable_rpc
    updater.poster = lambda relpath, content: upgrade.post_marker(
        upgrade.rollout_root(updater.config), relpath, content)


def _post(updater, intent, phase, *, host=None, **evidence):
    value = upgrade.make_marker(intent, phase, host=host, **evidence)
    name = upgrade.marker_name(host, phase)
    assert upgrade.post_marker(
        upgrade.rollout_root(updater.config),
        f"epochs/{intent['epoch']}/{name}", upgrade.canonical_json(value))
    return value


def _decision(updater, intent, phase, source_phase, *, direction, **extra):
    view = upgrade.read_rollout(updater.config, intent["epoch"])
    observed = {
        host: upgrade.marker_sha256(view["markers"][upgrade.marker_name(host, source_phase)])
        for host in intent["roster"]
    }
    generation = (intent["to_generation"] if direction == "forward"
                  else intent["from_generation"])
    return _post(updater, intent, phase, generation=generation, direction=direction,
                 observed=observed, decided_unix=459.0 + len(view["markers"]),
                 decided_by="rollout-test", **extra)


def _target(updater, source, name="target-generation", *, changed=None):
    target = Path(updater.config["generation_store"]) / name
    shutil.copytree(source, target)
    receipt = json.loads((target / "RUNTIME_VERSION.json").read_text())
    receipt["generation"] = name
    if changed is not None:
        member, data = changed
        (target / member).write_bytes(data)
        receipt["files"][member] = upgrade.digest(data)
    (target / "RUNTIME_VERSION.json").write_text(json.dumps(receipt))
    return target


def _rotation_processes(updater, generation, *, old_generation=None, one_shot=False):
    root = Path(updater.config["generation_store"]) / (old_generation or generation)
    rows = [
        (101, "1001", ["python3", str(root / "tools/supervise.py")]),
        (102, "1002", ["python3", str(root / "tools/worker_loop.py")]),
        (103, "1003", ["python3", str(root / "tools/prewarm_loop.py")]),
    ]
    if one_shot:
        rows.append((104, "1004", ["python3", str(root / "tools/worker.py")]))
    updater.parked_root.mkdir(parents=True, exist_ok=True)
    for pid, starttime, _ in rows:
        if pid != 101 and (pid != 104 or not one_shot):
            (updater.parked_root / upgrade.park_marker_name(pid, starttime, 458.0)).touch()
    updater.procs = lambda: rows
    return rows


def test_current_updater_does_not_release_its_drain_during_an_active_epoch(setup):
    updater, broker, generation = setup
    for name, member in upgrade.MEMBERS.items():
        (updater.install / name).write_bytes((generation / member).read_bytes())
    broker.holder = CLIENT
    updater.gate.write_text(json.dumps({
        "draining": True,
        "changed_unix": 458.0,
        "owner": CLIENT,
    }))
    updater.procs = lambda: []
    _durable(updater)
    _intent(updater, generation)

    result = updater.run()

    assert result["state"] == "rollout_drained"
    assert broker.holder == CLIENT
    assert "maintenance_end" not in broker.operations
    assert (updater.state / "rollout-epoch.json").is_file()


def test_reader_returns_none_without_an_epoch(setup):
    updater, _, _ = setup
    assert upgrade.read_rollout(updater.config) is None


@pytest.mark.parametrize("fault", ["missing-intent", "corrupt-intent", "multiple"])
def test_corrupt_missing_or_multiple_active_intents_fail_closed(setup, fault):
    updater, _, generation = setup
    root = upgrade.rollout_root(updater.config) / "epochs"
    if fault == "missing-intent":
        epoch_root = root / ("1" * 32)
        epoch_root.mkdir(parents=True)
        (epoch_root / "activated.json").write_text("{}\n")
    elif fault == "corrupt-intent":
        epoch = "2" * 32
        (root / epoch).mkdir(parents=True)
        (root / epoch / "intent.json").write_text("{bad json\n")
    else:
        _intent(updater, generation, epoch="1" * 32, target="target-one")
        _intent(updater, generation, epoch="2" * 32, target="target-two")
    with pytest.raises(ValueError):
        upgrade.read_rollout(updater.config)


def test_empty_preintent_epoch_from_a_crashed_arm_is_ignored(setup):
    updater, _, _ = setup
    (upgrade.rollout_root(updater.config) / "epochs" / ("1" * 32)).mkdir(parents=True)
    assert upgrade.read_rollout(updater.config) is None


def test_stale_epoch_or_intent_hash_marker_is_rejected(setup):
    updater, _, generation = setup
    intent = _intent(updater, generation)
    marker = upgrade.make_marker(
        intent, "drained", host=socket.gethostname(),
        generation=intent["from_generation"], posted_unix=459.0,
        active_scopes=0, drain_changed_unix=458.0, rollout_protocol=1)
    marker["intent_sha256"] = "0" * 64
    root = upgrade.rollout_root(updater.config)
    assert upgrade.post_marker(
        root, f"epochs/{intent['epoch']}/{upgrade.marker_name(socket.gethostname(), 'drained')}",
        upgrade.canonical_json(marker))
    with pytest.raises(ValueError, match="marker identity"):
        upgrade.read_rollout(updater.config)


def test_decision_must_hash_the_actual_complete_quorum(setup):
    updater, _, generation = setup
    intent = _intent(updater, generation)
    _post(updater, intent, "drained", host=socket.gethostname(),
          generation=intent["from_generation"], posted_unix=459.0,
          active_scopes=0, drain_changed_unix=458.0, rollout_protocol=1)
    _post(updater, intent, "activated", generation=intent["to_generation"],
          direction="forward", observed={socket.gethostname(): "0" * 64},
          decided_unix=460.0, decided_by="rollout-test")
    with pytest.raises(ValueError, match="observation does not match"):
        upgrade.read_rollout(updater.config)


def test_a_third_generation_pointer_is_never_interpreted(setup):
    updater, _, generation = setup
    _intent(updater, generation)
    third = Path(updater.config["generation_store"]) / "third-generation"
    third.mkdir()
    updater.config["runtime"] = str(third)
    with pytest.raises(ValueError, match="third generation"):
        upgrade.read_rollout(updater.config)


def test_unsupported_requeue_intent_is_rejected(setup):
    updater, _, generation = setup
    intent = _intent(updater, generation)
    intent["drain_policy"] = "requeue"
    with pytest.raises(ValueError, match="unsupported rollout drain policy"):
        upgrade.validate_intent(intent)


def test_corrupt_shared_epoch_closes_admission(setup):
    updater, broker, _ = setup
    epoch_root = upgrade.rollout_root(updater.config) / "epochs" / ("1" * 32)
    epoch_root.mkdir(parents=True)
    (epoch_root / "intent.json").write_text("{bad json\n")

    result = updater.run()

    assert result["state"] == "rollout_error"
    assert broker.holder == CLIENT
    assert "maintenance_end" not in broker.operations


def test_persisted_epoch_survives_shared_read_failure(setup):
    updater, broker, source = setup
    for name, member in upgrade.MEMBERS.items():
        (updater.install / name).write_bytes((source / member).read_bytes())
    broker.holder = CLIENT
    updater.gate.write_text(json.dumps({"draining": True, "changed_unix": 458.0}))
    _intent(updater, source)
    _durable(updater)
    updater.procs = lambda: []
    updater.run()
    updater.rollout_reader = lambda config, epoch=None: (_ for _ in ()).throw(
        OSError("rollout mount unavailable"))
    broker.operations.clear()

    assert updater.run()["state"] == "rollout_error"
    assert updater.rollout_state.is_file()
    assert broker.holder == CLIENT
    assert "maintenance_end" not in broker.operations


def test_desired_member_read_failure_occurs_after_durable_epoch_hold(setup):
    updater, broker, source = setup
    _intent(updater, source)
    _durable(updater)
    updater.reader = lambda config: (_ for _ in ()).throw(
        OSError("generation export unavailable"))

    with pytest.raises(OSError, match="generation export unavailable"):
        updater.run()

    assert broker.holder == CLIENT
    assert updater.rollout_state.is_file()
    assert "maintenance_end" not in broker.operations


def test_converged_rollout_agent_refuses_epoch_unaware_downgrade(setup):
    updater, broker, _ = setup
    (updater.install / "upgrade_client.py").write_bytes(Path(upgrade.__file__).read_bytes())

    with pytest.raises(ValueError, match="epoch-unaware candidate"):
        updater.run()

    assert broker.operations == []


def test_forward_rollout_releases_only_after_rotation_and_resume(setup):
    updater, broker, source = setup
    for name, member in upgrade.MEMBERS.items():
        (updater.install / name).write_bytes((source / member).read_bytes())
    broker.holder = CLIENT
    updater.gate.write_text(json.dumps({"draining": True, "changed_unix": 458.0,
                                        "owner": CLIENT}))
    intent = _intent(updater, source)
    _durable(updater)
    updater.procs = lambda: []
    assert updater.run()["state"] == "rollout_drained"

    _decision(updater, intent, "activated", "drained", direction="forward")
    target = _target(updater, source)
    updater.config["runtime"] = str(target)
    _rotation_processes(updater, target.name)
    assert updater.run()["state"] == "rollout_rotated"
    assert broker.holder == CLIENT
    assert "maintenance_end" not in broker.operations

    _decision(updater, intent, "resume", "rotated", direction="forward")
    assert updater.run()["state"] == "rollout_resumed"
    assert broker.holder is None
    resumed = upgrade.read_rollout(updater.config, intent["epoch"])["markers"]
    resumed_name = upgrade.marker_name(socket.gethostname(), "resumed")
    assert resumed_name in resumed

    # Crash-gap reconstruction: local release acknowledgement reached disk but
    # the shared resumed marker did not. New work must stay open while the next
    # tick idempotently republishes that proof.
    (upgrade.rollout_root(updater.config) / "epochs" / intent["epoch"] / resumed_name).unlink()
    broker.active = 1
    broker.operations.clear()
    assert updater.run()["state"] == "rollout_resumed"
    assert broker.operations == ["maintenance_status"]
    assert broker.active == 1
    assert resumed_name in upgrade.read_rollout(
        updater.config, intent["epoch"])["markers"]

    _decision(updater, intent, "terminal", "resumed", direction="forward",
              outcome="completed")
    later = Path(updater.config["generation_store"]) / "later-generation"
    later.mkdir()
    updater.config["runtime"] = str(later)
    broker.operations.clear()
    assert updater.run()["state"] == "rollout_terminal"
    assert broker.operations == []
    assert not updater.rollout_state.exists()


def test_pointer_activation_between_desired_and_epoch_reads_keeps_gate_without_failure(setup):
    """A target pointer can become visible after this tick read source bytes."""
    updater, broker, source = setup
    for name, member in upgrade.MEMBERS.items():
        (updater.install / name).write_bytes((source / member).read_bytes())
    broker.holder = CLIENT
    updater.gate.write_text(json.dumps({"draining": True, "changed_unix": 458.0,
                                        "owner": CLIENT}))
    intent = _intent(updater, source)
    _durable(updater)
    updater.procs = lambda: []
    assert updater.run()["state"] == "rollout_drained"

    _decision(updater, intent, "activated", "drained", direction="forward")
    target = _target(updater, source)
    read_source = updater.reader

    def source_read_then_target_pointer(config):
        version, blobs = read_source(config)
        assert version["generation"] == source.name
        updater.config["runtime"] = str(target)
        return version, blobs

    # `_run_once` already read the epoch from source.  Simulate an atomic
    # pointer activation before its separately-read desired receipt refreshes.
    updater.reader = source_read_then_target_pointer
    result = updater.run()

    assert result["state"] == "rollout_transitioning"
    assert broker.holder == CLIENT
    assert "maintenance_end" not in broker.operations
    markers = upgrade.read_rollout(updater.config, intent["epoch"])["markers"]
    assert upgrade.marker_name(socket.gethostname(), "failed") not in markers

    updater.reader = read_source
    _rotation_processes(updater, target.name)
    assert updater.run()["state"] == "rollout_rotated"
    assert broker.holder == CLIENT
    assert "maintenance_end" not in broker.operations
    markers = upgrade.read_rollout(updater.config, intent["epoch"])["markers"]
    assert upgrade.marker_name(socket.gethostname(), "failed") not in markers


def test_rebooted_resumed_host_requires_fresh_proof_before_reopening(setup):
    updater, broker, source = setup
    for name, member in upgrade.MEMBERS.items():
        (updater.install / name).write_bytes((source / member).read_bytes())
    broker.holder = CLIENT
    updater.gate.write_text(json.dumps({"draining": True, "changed_unix": 458.0}))
    intent = _intent(updater, source)
    _durable(updater)
    updater.procs = lambda: []
    updater.run()
    _decision(updater, intent, "activated", "drained", direction="forward")
    target = _target(updater, source)
    updater.config["runtime"] = str(target)
    _rotation_processes(updater, target.name)
    updater.run()
    _decision(updater, intent, "resume", "rotated", direction="forward")
    updater.run()

    broker.holder = CLIENT  # the durable broker's boot hold
    updater.gate.unlink()
    broker.operations.clear()
    assert updater.run()["state"] == "rollout_resumed"
    assert "maintenance_end" in broker.operations
    assert broker.holder is None


def test_coordinated_rollback_rotates_to_source_before_resume(setup):
    updater, broker, source = setup
    intent = _intent(updater, source)
    for name, member in upgrade.MEMBERS.items():
        (updater.install / name).write_bytes((source / member).read_bytes())
    broker.holder = CLIENT
    updater.gate.write_text(json.dumps({"draining": True, "changed_unix": 458.0}))
    _durable(updater)
    updater.procs = lambda: []
    updater.run()
    _decision(updater, intent, "activated", "drained", direction="forward")
    target = _target(updater, source)
    updater.config["runtime"] = str(target)
    _rotation_processes(updater, target.name)
    updater.run()

    _decision(updater, intent, "rollback", "drained", direction="rollback",
              reason="target failed qualification")
    updater.config["runtime"] = str(source)
    _decision(updater, intent, "reverted", "drained", direction="rollback")
    _rotation_processes(updater, source.name)
    assert updater.run()["state"] == "rollout_rolled_back"
    assert broker.holder == CLIENT
    _decision(updater, intent, "resume", "rolled-back", direction="rollback")

    assert updater.run()["state"] == "rollout_resumed"
    assert broker.holder is None


@pytest.mark.parametrize("fault", ["old-path", "unknown-path", "one-shot", "no-supervisor"])
def test_rotation_proof_rejects_old_unknown_or_unparked_paths(setup, fault):
    updater, broker, source = setup
    for name, member in upgrade.MEMBERS.items():
        (updater.install / name).write_bytes((source / member).read_bytes())
    updater.gate.write_text(json.dumps({"draining": True, "changed_unix": 458.0}))
    broker.holder = CLIENT
    status = updater.rpc(updater.endpoint, "maintenance_status")
    if fault == "old-path":
        _rotation_processes(updater, source.name, old_generation="old-generation")
    elif fault == "unknown-path":
        updater.procs = lambda: [
            (101, "1001", ["python3", "supervise.py"]),
            (102, "1002", ["python3", "worker_loop.py"]),
        ]
    elif fault == "one-shot":
        _rotation_processes(updater, source.name, one_shot=True)
    else:
        rows = _rotation_processes(updater, source.name)
        updater.procs = lambda: rows[1:]
    assert updater.rotation_evidence(status, source.name)["rotated"] is False


def test_prewarm_role_must_park_before_rollout_drain_proof(setup):
    updater, broker, source = setup
    broker.holder = CLIENT
    updater.gate.write_text(json.dumps({"draining": True, "changed_unix": 458.0}))
    updater.procs = lambda: [
        (103, "1003", ["python3", str(source / "tools/prewarm_loop.py")]),
    ]
    status = updater.rpc(updater.endpoint, "maintenance_status")

    # Storage readers are now always part of the census (PR #525), so no
    # rollout-only switch can accidentally certify this active reader.
    assert updater.drain_evidence(status)["drained"] is False


def test_failed_epoch_install_restores_bytes_but_keeps_the_gate(setup):
    updater, broker, source = setup
    for name, member in upgrade.MEMBERS.items():
        (updater.install / name).write_bytes((source / member).read_bytes())
    original = updater.installed()
    broker.holder = CLIENT
    updater.gate.write_text(json.dumps({"draining": True, "changed_unix": 458.0}))
    intent = _intent(updater, source)
    original = updater.installed()
    _durable(updater)
    updater.procs = lambda: []
    updater.run()
    _decision(updater, intent, "activated", "drained", direction="forward")
    target = _target(
        updater, source,
        changed=(upgrade.MEMBERS["resource_broker.py"], b"requires_gpu_capacity"))
    updater.config["runtime"] = str(target)

    result = updater.run()

    assert result["state"] == "rollout_failed"
    assert updater.installed() == original
    assert broker.holder == CLIENT
    assert "maintenance_end" not in broker.operations
    assert not updater.journal.exists()
    markers = upgrade.read_rollout(updater.config, intent["epoch"])["markers"]
    assert upgrade.marker_name(socket.gethostname(), "failed") in markers


def test_interrupted_epoch_transaction_recovers_without_releasing(setup):
    updater, broker, source = setup
    intent = _intent(updater, source)
    for name, member in upgrade.MEMBERS.items():
        (updater.install / name).write_bytes((source / member).read_bytes())
    broker.holder = CLIENT
    updater.gate.write_text(json.dumps({"draining": True, "changed_unix": 458.0}))
    _durable(updater)
    updater.procs = lambda: []
    updater.run()
    _decision(updater, intent, "activated", "drained", direction="forward")
    target = _target(
        updater, source,
        changed=(upgrade.MEMBERS["resource_payload.py"], b"new target payload"))
    updater.config["runtime"] = str(target)
    original_copy = updater.copy_files

    def crash(source_dir, files):
        raise KeyboardInterrupt()

    updater.copy_files = crash
    with pytest.raises(KeyboardInterrupt):
        updater.run()
    assert updater.journal.is_file()
    assert broker.running is False

    updater.copy_files = original_copy
    broker.operations.clear()
    assert updater.run()["state"] == "rollout_failed"
    assert broker.running is True
    assert broker.holder == CLIENT
    assert "maintenance_end" not in broker.operations
    assert not updater.journal.exists()

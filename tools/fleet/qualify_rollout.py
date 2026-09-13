#!/usr/bin/python3
"""Qualify a private two-host rollout barrier through PrismaBuild.

This is deliberately an actor, not a local test runner.  ``--emit-manifest``
creates paired x86/GB10 campaign rows; each admitted actor reads and writes
only its fresh root below ``/mnt/shared/pb-qualification``.  The shared tree
uses the real rollout marker transport and the real coordinator and participant
algorithms.  The broker, systemctl result, and process census are narrow local
simulations: PB actions cannot obtain host root.  They model gate state,
installed-file health, and the versioned supervisor/worker/parked census.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import socket
import stat
import sys
import time
import uuid


QUALIFICATION_ROOT = Path("/mnt/shared/pb-qualification")
SCENARIOS = ("success", "missing", "rollback", "swap-crash")
THREAD_ENV = {"OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1",
              "OPENBLAS_NUM_THREADS": "1"}


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=True) + "\n").encode("ascii")


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def write_once(path, data):
    """Publish immutable fixture evidence using the rollout link discipline."""
    path = Path(path)
    data = bytes(data)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / (".tmp-" + uuid.uuid4().hex)
    try:
        with temporary.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o444)
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != data:
                raise RuntimeError("immutable rendezvous collision: " + str(path))
            return False
    finally:
        temporary.unlink(missing_ok=True)
    return True


def event(root, name, **value):
    record = {"schema": "prismabuild.rollout_qualification.event.v1",
              "name": name, "posted_unix": time.time(), **value}
    write_once(Path(root) / "events" / (name + ".json"), canonical(record))
    return record


def wait_for(predicate, description, *, seconds=80):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value is not None:
            return value
        time.sleep(0.10)
    raise RuntimeError("timed out waiting for " + description)


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot import " + str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def harness_modules():
    here = Path(__file__).resolve().parent
    return (load(here / "upgrade_client.py", "qualification_upgrade_client"),
            load(here / "publish_runtime.py", "qualification_publish_runtime"))


def ensure_admitted(root):
    if not __debug__:
        raise RuntimeError("qualification requires Python assertions")
    if not os.environ.get("PRISMABUILD_CONTAINER_OWNER"):
        raise RuntimeError("qualification requires PRISMABUILD_CONTAINER_OWNER")
    cgroup = Path("/proc/self/cgroup").read_text()
    if re.search(r"prismabuild-job[0-9a-f]{32}\.slice", cgroup) is None:
        raise RuntimeError("qualification requires an exact prismabuild-job cgroup")
    root = Path(root)
    try:
        root.resolve().relative_to(QUALIFICATION_ROOT.resolve())
    except ValueError as exc:
        raise RuntimeError("qualification root is outside private qualification root") from exc
    if root == QUALIFICATION_ROOT.resolve():
        raise RuntimeError("qualification root must be a fresh child")
    if root.is_symlink():
        raise RuntimeError("qualification root must not be a symlink")
    return cgroup


def readonly_tree(path):
    for entry in sorted(Path(path).rglob("*"), key=lambda item: len(item.parts), reverse=True):
        if entry.is_file():
            entry.chmod(0o444)
        elif entry.is_dir():
            entry.chmod(0o555)
    Path(path).chmod(0o555)


def build_generation(root, name, source, upgrade, hosts, *, changed_member=None):
    """Build the smallest sealed publisher-shaped generation from source bytes."""
    generation = root / "runtime-generations" / name
    generation.mkdir(parents=True)
    files = {}
    for member in (set(upgrade.MEMBERS.values())
                   | {"tools/fleet/fleet_boxes.json", "tools/fleet/publish_runtime.py"}):
        destination = generation / member
        destination.parent.mkdir(parents=True, exist_ok=True)
        if member == "tools/fleet/fleet_boxes.json":
            data = canonical({"boxes": {host: {"roles": ["qualification"]}
                                         for host in hosts}})
        else:
            source_member = source / member
            if not source_member.is_file() and member.startswith("tools/"):
                source_member = source / "tools" / "fleet" / Path(member).name
            data = source_member.read_bytes()
        if member == changed_member:
            data += b"\n# private qualification successor\n"
        destination.write_bytes(data)
        destination.chmod(0o444)
        files[member] = sha256(data)
    receipt = {"schema": "prismaquant.prismabuild.runtime_version.v1",
               "commit": "4" * 40, "dirty": True, "generation": name,
               "published_unix": time.time(), "published_by": socket.gethostname(),
               "rollout": "barrier", "files": files}
    (generation / "RUNTIME_VERSION.json").write_bytes(canonical(receipt))
    (generation / "RUNTIME_VERSION.json").chmod(0o444)
    readonly_tree(generation)
    return generation, receipt


class FakeBroker:
    """Private boundary for root-only broker/systemctl/process observations."""

    def __init__(self, root, config, upgrade, *, fail_first_start=False):
        self.root, self.config, self.upgrade = Path(root), config, upgrade
        self.draining = False
        self.owner = None
        self.changed = None
        self.operations = []
        self.fail_first_start = fail_first_start
        self.failed_start = False
        self.failure_active = False

    @property
    def gate(self):
        return Path(self.config["maintenance_gate"])

    def _write_gate(self):
        self.gate.parent.mkdir(parents=True, exist_ok=True)
        self.gate.write_bytes(canonical({"draining": self.draining,
                                         "changed_unix": self.changed or time.time(),
                                         "owner": self.owner}))
        self._park_processes()

    def _park_processes(self):
        if not self.draining or self.changed is None:
            return
        parked = self.gate.parent / "rollout" / "parked"
        parked.mkdir(parents=True, exist_ok=True)
        for pid, starttime, _ in self.procs():
            if pid != 1001:
                (parked / self.upgrade.park_marker_name(pid, starttime, self.changed)).touch()

    def procs(self):
        generation = Path(self.config["runtime"]).resolve(strict=True)
        return [(1001, "101", ["/usr/bin/python3", str(generation / "tools/supervise.py")]),
                (1002, "102", ["/usr/bin/python3", str(generation / "tools/worker_loop.py")]),
                (1003, "103", ["/usr/bin/python3", str(generation / "tools/prewarm_loop.py")])]

    def rpc(self, endpoint, operation, **fields):
        del endpoint
        self.operations.append(operation)
        if operation == "maintenance_begin":
            if not self.draining:
                self.draining, self.changed = True, time.time()
                self.owner = fields.get("owner", self.upgrade.MAINTENANCE_UNOWNED)
                self._write_gate()
        elif operation == "maintenance_end":
            if fields.get("owner") not in (None, self.owner):
                raise RuntimeError("simulated broker rejects another drain owner")
            self.draining, self.owner = False, None
            self._write_gate()
        elif operation != "maintenance_status":
            raise RuntimeError("unexpected simulated broker operation: " + operation)
        installed = {key: value for key, value in self.installed().items()
                     if key != "upgrade_client.py"}
        return {"health": not self.failure_active,
                "draining": self.draining, "active_scopes": 0,
                "maintenance_protocol": 2, "maintenance_owner": self.owner,
                "maintenance_durable_protocol": 1,
                "maintenance_state_path": str(self.root / "durable-maintenance.json"),
                "installed_sha256": installed}

    def installed(self):
        install = Path(self.config["install_dir"])
        return {name: sha256((install / name).read_bytes())
                for name in self.upgrade.MEMBERS}

    def command(self, argv, **kwargs):
        del kwargs
        verb = argv[1]
        self.operations.append("systemctl:" + verb)
        if verb == "start" and self.fail_first_start and not self.failed_start:
            self.failed_start = True
            self.failure_active = True
        elif verb == "start":
            self.failure_active = False
        return type("Result", (), {"returncode": 0, "stdout": "", "stderr": ""})()


def private_config(root, host):
    local = Path(root) / "hosts" / host
    return {"runtime": str(Path(root) / "repo"),
            "generation_store": str(Path(root) / "runtime-generations"),
            "rollout_root": str(Path(root) / "rollout"),
            "install_dir": str(local / "install"), "state_dir": str(local / "state"),
            "maintenance_gate": str(local / "run" / "maintenance.json"),
            "reader_uid": os.getuid()}


def make_upgrader(root, host, upgrade, *, fail_first_start=False):
    config = private_config(root, host)
    install, state = Path(config["install_dir"]), Path(config["state_dir"])
    install.mkdir(parents=True, exist_ok=True)
    state.mkdir(parents=True, exist_ok=True)
    source = Path(config["runtime"]).resolve(strict=True)
    for name, member in upgrade.MEMBERS.items():
        (install / name).write_bytes((source / member).read_bytes())
    broker = FakeBroker(root, config, upgrade, fail_first_start=fail_first_start)

    def poster(relpath, content):
        return upgrade.post_marker(Path(config["rollout_root"]), relpath, content)

    return (upgrade.Upgrader(config, rpc=broker.rpc, command=broker.command,
                             sleep=lambda _: None, reader=upgrade.desired,
                             procs=broker.procs, poster=poster,
                             rollout_reader=upgrade.read_rollout), broker)


def actor_ready(root, role, cgroup):
    host = socket.gethostname()
    value = {"schema": "prismabuild.rollout_qualification.ready.v1", "role": role,
             "host": host, "container_owner": os.environ["PRISMABUILD_CONTAINER_OWNER"],
             "cgroup": cgroup, "source_sha256": sha256(Path(__file__).read_bytes())}
    write_once(Path(root) / "ready" / (role + ".json"), canonical(value))
    return value


def readiness(root):
    records = []
    for role in ("coordinator", "peer"):
        path = Path(root) / "ready" / (role + ".json")
        if not path.is_file():
            return None
        records.append(json.loads(path.read_text()))
    if records[0]["host"] == records[1]["host"]:
        raise RuntimeError("qualification requires actors on distinct real hosts")
    return records


def initialise(root, source, upgrade, publisher, scenario, records):
    root = Path(root)
    write_once(root / "run.json", canonical({
        "schema": "prismabuild.rollout_qualification.run.v1", "scenario": scenario,
        "created_unix": time.time(), "boundary": {
            "real": "two independently admitted PB actors, distinct hosts, shared-NFS write-once markers and coordinator algorithm",
            "simulated": "private broker gate/installed-file health, systemctl, and local process census; no host root or live fleet service"},
        "source": {"harness": sha256(Path(__file__).read_bytes()),
                   "upgrade_client": sha256((source / "tools/fleet/upgrade_client.py").read_bytes()),
                   "publish_runtime": sha256((source / "tools/fleet/publish_runtime.py").read_bytes())}}))
    hosts = sorted(item["host"] for item in records)
    old, old_receipt = build_generation(root, "old", source, upgrade, hosts)
    changed = "tools/resource_broker.py" if scenario == "rollback" else None
    new, new_receipt = build_generation(root, "new", source, upgrade, hosts,
                                        changed_member=changed)
    os.symlink("runtime-generations/old", root / "repo")
    offers = root / "pb-queue" / "workers"
    offers.mkdir(parents=True)
    for host in hosts:
        (offers / (host + ".json")).write_bytes(canonical({
            "schema": "prismaquant.prismabuild.pool_offer.v1", "host": host,
            "announced_unix": time.time()}))
    agent_sha = new_receipt["files"]["tools/upgrade_client.py"]
    for host in hosts:
        body = upgrade.attestation_body(host, agent_sha)
        upgrade.post_marker(root / "rollout", "agents/" + upgrade.attestation_name(host, agent_sha),
                            upgrade.canonical_json(body))
    publisher.MIRROR, publisher.CHECKOUT = root / "repo", source
    write_once(root / "generation-evidence.json", canonical({
        "schema": "prismabuild.rollout_qualification.generation.v1", "hosts": hosts,
        "old": old_receipt, "new": new_receipt,
        "modes": {"old": oct(stat.S_IMODE(old.stat().st_mode)),
                  "new": oct(stat.S_IMODE(new.stat().st_mode))}}))
    event(root, "initialized", hosts=hosts, changed_member=changed)


def view(publisher, epoch):
    value = publisher._rollout_view(epoch)
    if value is None:
        raise RuntimeError("coordinator lost active epoch")
    return value


def host_marker(upgrade, snapshot, host, phase):
    return upgrade.marker_name(host, phase) in snapshot["markers"]


def participant_tick(upgrader, broker, root, role):
    try:
        result = upgrader.run()
    except Exception as exc:
        # Match the installed service's error boundary and subsequent timer
        # tick. Separate target/epoch reads can straddle a pointer move; an
        # uncertain tick must retain its hold, then retry fresh observations.
        # A persistent defect still fails the actor's terminal deadline.
        assert broker.draining is True, "participant error released admission"
        result = upgrader.report("error", error=str(exc),
                                 recovery_pending=upgrader.journal.exists())
    event(root, role + "-" + result["state"] + "-" + uuid.uuid4().hex,
          host=result["host"], state=result["state"],
          gate_closed=broker.draining, error=result.get("error"),
          operations=broker.operations)
    return result


def peer_actor(root, scenario, upgrade):
    cgroup = ensure_admitted(root)
    wait_for(lambda: Path(root) if Path(root).is_dir() else None,
             "coordinator private-root creation")
    ready = actor_ready(root, "peer", cgroup)
    wait_for(lambda: (Path(root) / "events" / "initialized.json")
             if (Path(root) / "events" / "initialized.json").is_file() else None,
             "coordinator initialisation")
    fail = scenario == "rollback"
    upgrader, broker = make_upgrader(root, ready["host"], upgrade, fail_first_start=fail)
    wait_for(lambda: (Path(root) / "events" / "allow-peer-drain.json")
             if (Path(root) / "events" / "allow-peer-drain.json").is_file() else None,
             "coordinator drain permission")
    terminal = None
    for _ in range(500):
        terminal = participant_tick(upgrader, broker, root, "peer")
        if terminal["state"] in {"rollout_terminal"}:
            break
        time.sleep(.05)
    if terminal is None or terminal["state"] != "rollout_terminal":
        raise RuntimeError("peer did not reach terminal rollout state")
    write_once(Path(root) / "actors" / "peer.json", canonical({"ready": ready, "terminal": terminal,
                                                                     "broker_operations": broker.operations}))


def wait_marker(upgrade, publisher, epoch, host, phase):
    return wait_for(lambda: view(publisher, epoch) if host_marker(upgrade, view(publisher, epoch), host, phase)
                    else None, f"{host} {phase}")


def coordinator_actor(root, scenario, source, upgrade, publisher):
    cgroup = ensure_admitted(root)
    root = Path(root)
    if root.exists():
        raise RuntimeError("coordinator requires a fresh qualification root")
    root.parent.mkdir(parents=True, exist_ok=True)
    # The peer may arrive first and publish its immutable readiness into a parent
    # created by the campaign environment only after this check; coordinate root
    # ownership via the private run claim, not a fleet process.
    root.mkdir(mode=0o755)
    private_root = Path("/mnt/shared/pb-qualification").resolve()
    if (root.parent.parent.resolve() != private_root
            or root.parent.parent / root.parent.name != root.parent
            or root.name not in SCENARIOS):
        raise RuntimeError("qualification root escaped its private campaign scenario")
    publisher.FINAL_BARRIER_QUALIFICATION_GUARD = False
    ready = actor_ready(root, "coordinator", cgroup)
    records = wait_for(lambda: readiness(root), "peer immutable readiness")
    initialise(root, source, upgrade, publisher, scenario, records)
    host = ready["host"]
    peer = next(item["host"] for item in records if item["role"] == "peer")
    upgrader, broker = make_upgrader(root, host, upgrade)
    # _arm_barrier invokes the real coordinator, returns 75 because no host can
    # have drained before the intent exists, and leaves every gate closed.
    assert publisher._activate_existing("new", dry_run=False, wait_s=0) == 75
    epoch = publisher._rollout_view()["intent"]["epoch"]
    participant_tick(upgrader, broker, root, "coordinator")
    held = publisher._barrier_step(epoch)
    assert held["state"] == "draining" and peer in held["missing"]
    assert (root / "repo").resolve().name == "old"
    assert broker.draining is True
    event(root, "held-before-second-drain", epoch=epoch, held=held)
    if scenario == "missing":
        assert publisher._wait_barrier(epoch, wait_s=0) == 75
        assert broker.draining is True and (root / "repo").resolve().name == "old"
        event(root, "missing-host-stall", epoch=epoch, exit_status=75)
    event(root, "allow-peer-drain", epoch=epoch)
    wait_marker(upgrade, publisher, epoch, peer, "drained")

    if scenario == "swap-crash":
        original = publisher._decision
        publisher._decision = lambda *args, **kwargs: (_ for _ in ()).throw(OSError("injected activation-record crash"))
        try:
            publisher._barrier_step(epoch)
        except OSError:
            pass
        else:
            raise AssertionError("injected coordinator crash did not fire")
        assert (root / "repo").resolve().name == "new"
        assert "activated.json" not in view(publisher, epoch)["markers"]
        event(root, "swap-before-activated-record", epoch=epoch)
        publisher._decision = original

    step = publisher._barrier_step(epoch)
    assert step["state"] in {"activated", "reverted"}
    participant_tick(upgrader, broker, root, "coordinator")

    if scenario == "rollback":
        wait_marker(upgrade, publisher, epoch, peer, "failed")
        declared = publisher._barrier_step(epoch)
        assert declared["state"] == "rollback_declared"
        reverted = publisher._barrier_step(epoch)
        assert reverted["state"] == "reverted"
        participant_tick(upgrader, broker, root, "coordinator")
        wait_marker(upgrade, publisher, epoch, peer, "rolled-back")
        wait_marker(upgrade, publisher, epoch, host, "rolled-back")
        assert publisher._barrier_step(epoch)["state"] == "resume_authorized"
    else:
        wait_marker(upgrade, publisher, epoch, peer, "rotated")
        wait_marker(upgrade, publisher, epoch, host, "rotated")
        assert "resume.json" not in view(publisher, epoch)["markers"]
        assert publisher._barrier_step(epoch)["state"] == "resume_authorized"

    participant_tick(upgrader, broker, root, "coordinator")
    wait_marker(upgrade, publisher, epoch, peer, "resumed")
    wait_marker(upgrade, publisher, epoch, host, "resumed")
    final = publisher._barrier_step(epoch)
    expected = "rolled_back" if scenario == "rollback" else "completed"
    assert final["state"] == expected and final["complete"] is True
    wait_status = publisher._wait_barrier(epoch, wait_s=0)
    assert wait_status == (1 if scenario == "rollback" else 0)
    terminal = participant_tick(upgrader, broker, root, "coordinator")
    assert terminal["state"] == "rollout_terminal"
    result = {"schema": "prismabuild.rollout_qualification.result.v1", "scenario": scenario,
              "epoch": epoch, "outcome": expected, "wait_status": wait_status,
              "coordinator_host": host, "peer_host": peer, "source_binding": json.loads((root / "run.json").read_text())["source"]}
    write_once(root / "result.json", canonical(result))
    write_once(root / "actors" / "coordinator.json", canonical({"ready": ready, "terminal": terminal,
                                                                    "broker_operations": broker.operations}))
    print(json.dumps(result, sort_keys=True), flush=True)


def actor(args):
    root = Path(args.root)
    upgrade, publisher = harness_modules()
    source = Path(__file__).resolve().parents[2]
    if args.role == "coordinator":
        coordinator_actor(root, args.scenario, source, upgrade, publisher)
    else:
        peer_actor(root, args.scenario, upgrade)


def emit_manifest(path, root):
    root = Path(root)
    if root.exists() or root.parent.resolve() != QUALIFICATION_ROOT.resolve():
        raise SystemExit("--run-root must be a fresh direct child of /mnt/shared/pb-qualification")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", root.name):
        raise SystemExit("--run-root has an unsafe name")
    script = "tools/fleet/qualify_rollout.py"
    rows = []
    for scenario in SCENARIOS:
        scenario_root = root / scenario
        for role, tag in (("coordinator", "x86"), ("peer", "gb10")):
            rows.append({"argv": ["/usr/bin/python3", script, "--role", role,
                                 "--scenario", scenario, "--root", str(scenario_root)],
                         "cwd": str(Path(__file__).resolve().parents[2]),
                         "demand": {"cpu": 1, "mem_gb": 2}, "tags": [tag],
                         "env": THREAD_ENV, "priority": -10, "timeout_s": 240})
    Path(path).write_bytes(canonical(rows))
    print(json.dumps({"manifest": str(path), "run_root": str(root), "rows": len(rows)}, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--role", choices=("coordinator", "peer"))
    parser.add_argument("--scenario", choices=SCENARIOS)
    parser.add_argument("--root")
    parser.add_argument("--emit-manifest")
    parser.add_argument("--run-root")
    args = parser.parse_args()
    if args.emit_manifest:
        if args.role or args.scenario or args.root or not args.run_root:
            parser.error("--emit-manifest requires only --run-root")
        emit_manifest(args.emit_manifest, args.run_root)
        return 0
    if not (args.role and args.scenario and args.root):
        parser.error("actor requires --role, --scenario, and --root")
    actor(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Real prepaid-output writers and broker protocol over a private cgroup double."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import select
import tempfile

from prismabuild import core as pb, pool, produced_output as po, resource_scope
import resource_broker


class _CgroupKernel:
    def __init__(self):
        self.groups = {}

    def create(self, scope, budget):
        self.groups[scope] = {"populated": False}
        return {"cgroup_path": f"/sys/fs/cgroup/prismabuild.slice/{scope}"}

    def stop(self, scope):
        self.groups[scope]["populated"] = False

    def empty(self, scope):
        return scope not in self.groups or not self.groups[scope]["populated"]

    def exists(self, scope):
        return scope in self.groups

    def release(self, scope):
        if self.groups[scope]["populated"]:
            raise ValueError("scope still populated")
        self.groups.pop(scope)


BROKER_STARTUP_S = 10
BROKER_STOP_S = 5
BROKER_TERMINATE_S = 1
BROKER_KILL_S = 1


def _run_private_broker(socket_path, state_root, uid, control):
    server = None
    try:
        authority = resource_broker.Authority(
            state_root, uid, _CgroupKernel(), max_memory_bytes=1024 ** 3)
        server = resource_broker.Server(socket_path, resource_broker.Handler)
        server.authority = authority
        os.write(control.fileno(), b"R")
        while True:
            ready, _, _ = select.select([control, server], [], [])
            if control in ready:
                packet = os.read(control.fileno(), 1)
                if packet in (b"S", b""):
                    return
                raise RuntimeError("invalid private broker control byte")
            if server in ready:
                server.handle_request()
    except BaseException:
        try:
            os.write(control.fileno(), b"E")
        except OSError:
            pass
        raise
    finally:
        if server is not None:
            server.server_close()
        control.close()


class Broker:
    def __init__(self, root, monkeypatch):
        self.root, self.patch = root, monkeypatch
        self.process = self.control = self.directory = self.socket = None
        self.scopes = []

    def scope_for_claim(self, queue, claimed):
        if self.process is None:
            child_control = None
            try:
                self.directory = tempfile.TemporaryDirectory(prefix="pb-fixture-", dir="/tmp")
                self.socket = Path(self.directory.name) / "broker.sock"
                context = multiprocessing.get_context("spawn")
                self.control, child_control = context.Pipe()
                self.process = context.Process(
                    target=_run_private_broker,
                    args=(str(self.socket), str(self.root / "broker-state"),
                          os.getuid(), child_control))
                expected_affinity = os.sched_getaffinity(0)
                self.process.start()
                child_control.close()
                if not self.control.poll(BROKER_STARTUP_S):
                    raise TimeoutError("private broker startup timed out")
                if os.read(self.control.fileno(), 1) != b"R":
                    raise RuntimeError("private broker startup failed")
                if os.sched_getaffinity(self.process.pid) != expected_affinity:
                    raise RuntimeError("private broker changed assigned CPU affinity")
                self.patch.setattr(resource_scope, "BROKER_SOCKET", self.socket)
                defaults = dict(resource_scope.ResourceScope.__init__.__kwdefaults__)
                self.patch.setattr(resource_scope.ResourceScope.__init__, "__kwdefaults__",
                                   {**defaults, "socket_path": self.socket})
            except BaseException as original:
                if child_control is not None:
                    child_control.close()
                try:
                    self._stop_process()
                except BaseException as cleanup:
                    original.add_note(f"private broker cleanup: {cleanup}")
                raise
        scope = queue._start_resource_scope(claimed)
        self.scopes.append(scope)
        control = scope.control_record()
        assert control["socket_path"] == str(self.socket)
        live = pool.read_queue_record(queue.item_path(pool.CLAIMED, claimed["action_key"]))
        assert live is not None
        assert all(live["resource_scope"].get(key) == value
                   for key, value in control.items())
        return scope

    def _stop_process(self):
        process = self.process
        failure = None
        try:
            if process is not None:
                forced = False
                if process.pid is not None:
                    if process.is_alive():
                        try:
                            os.write(self.control.fileno(), b"S")
                        except OSError:
                            pass
                        process.join(BROKER_STOP_S)
                    if process.is_alive():
                        forced = True
                        process.terminate()
                        process.join(BROKER_TERMINATE_S)
                    if process.is_alive():
                        process.kill()
                        process.join(BROKER_KILL_S)
                    if process.is_alive():
                        failure = RuntimeError(f"private broker survived kill: pid {process.pid}")
                    elif forced or process.exitcode != 0:
                        failure = RuntimeError(
                            f"private broker exit {process.exitcode}, forced={forced}, pid={process.pid}")
                if failure is None or not process.is_alive():
                    process.close()
                    self.process = None
        finally:
            if self.control is not None:
                self.control.close()
                self.control = None
            if self.directory is not None and self.process is None:
                self.directory.cleanup()
                self.directory = None
        if failure is not None:
            raise failure

    def close(self):
        if self.process is None:
            return
        failure = None
        try:
            for scope in self.scopes:
                scope.terminate_owned("private fixture teardown")
                scope.release()
        except BaseException as original:
            failure = original
            raise
        finally:
            try:
                self._stop_process()
            except BaseException as cleanup:
                if failure is None:
                    raise
                failure.add_note(f"private broker cleanup: {cleanup}")


@dataclass(frozen=True)
class FundedBatch:
    mover: str
    staged: dict
    funded: dict


class Outputs:
    def __init__(self, queue, root, broker):
        self.queue, self.root, self.broker = queue, root, broker

    def template(self, tier):
        return po.validate_template({
            "schema": po.TEMPLATE_SCHEMA_V1, "version": 1,
            "template_id": "prepaid-v1", "output_prefix": str(self.root / "outputs"),
            "slots": {"s0": {"class": "payload"}},
            "durable_maxima": {"payload_max_bytes": 1 << 20,
                               "checkpoint_max_bytes": 1 << 20,
                               "temp_max_bytes": 1 << 20},
            "working_demands": {tier: {"minimum_gib": 1, "window_gib": 2}},
            "permitted_tiers": [tier],
        })

    def producer_request(self, template, seed):
        """Seal a real CAS request for the contained producer's actual identity."""
        checkout = self.root / "producer-checkout"
        checkout.mkdir()
        worker = checkout / "worker.py"
        worker.write_text("raise SystemExit(0)\n", encoding="utf-8")
        cas_root = self.root / "cas"
        cas = pb.PrismaBuildCAS(cas_root)
        envelope = self.root / "produced-template.json"
        envelope.write_text(json.dumps(template, sort_keys=True), encoding="utf-8")
        template_input, _ = cas.ingest_input(
            envelope, input_id=pb.PRODUCED_OUTPUT_TEMPLATE_INPUT_ID)
        action = pb.seal_action({
            "schema": pb.ACTION_SCHEMA_V2,
            "task": {"definition_id": "tests/fixture-output-producer",
                     "definition_version": "v1", "task_class": "generation",
                     "determinism": "deterministic", "artifact_family": "generic",
                     "artifact_kind": "generic", "argv": ["/bin/true", seed],
                     "working_directory": ".", "result_path": "result"},
            "inputs": [template_input],
            "code_closure": pb.build_code_closure(checkout, ["worker.py"]),
            "params": {"cwd": ".", "command": ["/bin/true", seed],
                       "produced_output_template": po.build_declaration(template, template_input)},
            "environment": {"variables": {"PATH": "/usr/bin:/bin"}, "toolchain": {}},
            "execution_scope": {"portability": "portable", "platform_key": None,
                                "host_class": None},
        })
        cas.publish_action_request(action)
        return action["action_key"], cas_root, checkout, worker

    def bind(self, template, seed):
        q = self.queue
        owner, cas_root, checkout, worker = self.producer_request(template, seed)
        q.publish(action_key=owner, cas_root=str(cas_root), checkout_root=str(checkout),
                  worker_script=str(worker),
                  resources={"cpu": 1, "mem_gb": 1, **po.owner_demand_terms(template)},
                  produced_output_template=template)
        claimed = q.claim(owner="w-owner")
        assert claimed is not None and claimed["action_key"] == owner
        scope = self.broker.scope_for_claim(q, claimed)
        control = scope.control_record()
        env = {"PRISMABUILD_ACTION_KEY": owner,
               "PRISMABUILD_ACTION_NONCE": control["nonce"],
               "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
        po.declare_template(q.root, template)
        inst = po.bind_instance(q, template, owner_action_key=owner,
                                claim_snapshot=claimed, env=env)
        po.declare_instance(q.root, inst)
        assert po.admit_instance(q, inst, template)["ok"] is True
        return inst

    def fund_ready(self, *, tier, owner_seed, mover_seed):
        q = self.queue
        kind = "stage_gib"
        q.mint_tier_capacity(tier, {kind: 4})
        template = self.template(tier)
        inst = self.bind(template, owner_seed)
        owner = inst["owner_action_key"]
        mover = hashlib.sha256(mover_seed.encode()).hexdigest()
        origin = self.root / "outputs"
        origin.mkdir()
        payload = b"x" * 1024
        path = origin / "a.bin"
        path.write_bytes(payload)
        descs = [po.validate_descriptor({
            "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "s0", "artifact_class": "payload",
            "path": str(path), "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "producer_generation": po.mint_generation(), "owner_action_key": owner,
            "owner_attempt": dict(inst["owner_attempt"]),
        }, template, inst)]
        prewrite = po.require_prewrite(
            q, inst, template, batch_id="b1", tier=tier,
            class_bytes={"payload": len(payload), "checkpoint": 0, "temp": 0},
            paths=[str(path)])
        assert prewrite.get("ok") is True, prewrite
        staged = q.stage_output_intent(
            tier_id=tier, owner_key=owner, mover_key=mover, instance=inst,
            template=template, batch_id="b1", descriptors=descs)
        ref = pool.PoolQueue.build_produced_output_batch_ref(
            instance=inst, template=template, batch_id="b1", descriptors=descs, tier_id=tier)
        q.publish(action_key=mover, cas_root=str(self.root / "cas"), worker_script="/w.py",
                  checkout_root="/co", resources={"cpu": 1, "mem_gb": 1, f"{kind}@{tier}": 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": tier,
                             "manifest_sha256": po.output_manifest_sha256(descs),
                             "manifest_bytes": len(payload), "range_start_bytes": 0,
                             "range_end_bytes": len(payload)}, produced_output_batch=ref)
        row = pool.read_queue_record(q.item_path(pool.READY, mover))
        assert isinstance(row, dict)
        funded = q.fund_output_batch(
            tier_id=tier, owner_key=owner, mover_key=mover, instance=inst,
            template=template, batch_id="b1", descriptors=descs)
        committed = po.commit_batch(q, inst, template, descs, batch_id="b1",
                                    tier=tier, mover_key=mover)
        assert committed.get("ok") is True, committed
        return FundedBatch(mover, staged, funded)

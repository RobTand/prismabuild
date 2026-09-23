"""A measurement consumer's movers are ordinary movement work (#944).

On generation ``81d95cba8d91`` the PrismaQuant measurement run d54952c1fcac sat
in ``ready`` with nothing staged.  Its movers 68fdb8728f38 and 750a4f8c65eb
had copied the consumer's task, so they carried ``task_class: measurement``,
and ``adaptive_cpu`` held each of them to a measurement's rule: an idle host.
The stage host is dl380g10, which always runs the tier loop, the broker and
pbmetrics, so every pass refused them ``measurement_host_not_idle`` (1,596
passes in 134 s).  They had copied the consumer's ``platform_keyed`` scope and
toolchain too, the Spark's platform key and shell digest, which an x86_64
worker's preflight would refuse next.

A mover copies bytes on the box that owns the stage.  Its task class,
artifact family, scope and toolchain are now a mover's own; the consumer's
isolation is still its own host's.

Fixture concessions: the tier is announced by a real ``tier_loop.cycle``, the
rows are sealed by the real ``pbrun.residency_stage_rows`` and
``pbrun.seal_action_from_template``, and the sealed requests are published to
a real CAS, so ``adaptive_cpu.action_identity`` reads the task class off the
sealed bytes.  Only the CPU sample is stated: eight CPUs, one of them busy
with load the pool does not own.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import time
import types

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import adaptive_cpu, core as pb  # noqa: E402
from prismabuild import movement_actions, pool, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
GIB = storage_tiers.GIB
PHASE_BYTES = 2 * GIB
READERS = 4
#: The submitter's platform: a GB10 Spark.
SPARK_PLATFORM = "linux-aarch64-sm121"
SPARK_TOOLCHAIN = {"argv0.sha256": "e" * 64, "argv0.bytes": "1446024",
                   "system": "linux", "machine": "aarch64",
                   "libc": "glibc-2.39", "cuda_compute_capability": "12.1"}
#: The stage host, sampled with one CPU in eight busy: more than the 5% a
#: measurement allows, nothing an ordinary action minds.
CAPACITY = {"cpu": 8, "mem_gb": 16}
CPU_TIERS = {"preferred": list(range(8)), "fallback": []}
FOREIGN_BUSY_CPUS = 1.0


def _manifest() -> dict[str, object]:
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {},
        "annotations": {"phases": [{"name": "phase-0", "bytes": PHASE_BYTES,
                                    "cumulative_bytes": PHASE_BYTES}]},
        "mount_prefix": "/mnt/shared",
        "entries": [{"path": "/mnt/shared/part-0", "offset": 0,
                     "bytes": PHASE_BYTES, "sha256": None}],
        "entry_count": 1, "total_bytes": PHASE_BYTES,
    }


class _Cas:
    """The manifest input off disk; every sealed request into a real CAS."""

    def __init__(self, manifest_path: Path, root: Path) -> None:
        self._manifest = manifest_path
        self.cas = pb.PrismaBuildCAS(root)
        self.actions: dict[str, dict] = {}

    def input_path(self, entry):
        return self._manifest

    def publish_action_request(self, action) -> None:
        self.cas.publish_action_request(action)
        self.actions[str(action["action_key"])] = dict(action)


def _template(digest: str, size: int, *, measurement: bool) -> dict[str, object]:
    """A ``pbrun`` template, shaped as ``--measurement`` seals it or not."""

    import pbrun

    if measurement:
        scope = {"portability": "platform_keyed", "platform_key": SPARK_PLATFORM,
                 "host_class": None}
        toolchain = dict(SPARK_TOOLCHAIN)
    else:
        scope = {"portability": "portable", "platform_key": None,
                 "host_class": None}
        toolchain = {}
    return {
        "cas": None, "marker_root": Path("/home/rob/tmp/markers"),
        "checkout_identity": {"commit": "a" * 40},
        "log_name": "x.log", "stamp_name": "pbrun.stamp",
        "task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                 "task_class": "measurement" if measurement else "generation",
                 "determinism": "stochastic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "working_directory": "."},
        "inputs": [{"id": "pbrun.checkout-snapshot", "sha256": "b" * 64,
                    "bytes": 4096}],
        "code_closure": pbrun.build_stamp_closure("pbrun.stamp", "{}"),
        "params": {"command": ["python", "measure.py"], "cwd": "/home/rob",
                   "demand": {"cpu": 1, "mem_gb": 1},
                   "placement": {"required_tags": ["sparky"]},
                   "checkout_snapshot": {
                       "schema": pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1,
                       "commit": "a" * 40, "subdirectory": ".",
                       "input": {"id": pb.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID,
                                 "sha256": "b" * 64, "bytes": 4096}},
                   "retry_policy": {"max_attempts": 1, "retry_safe": False},
                   "data_manifest": {"input": {"sha256": digest, "bytes": size}}},
        "environment": {"variables": {"PATH": "/usr/bin"}, "toolchain": toolchain},
        "execution_scope": scope,
    }


def _seal(tmp_path: Path, monkeypatch, *, measurement: bool = True):
    """Seal a consumer and its stage rows the way ``pbrun`` does."""

    import pbrun

    # The rows name ``SH / "cas"`` as their CAS; point it at this test's.
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(_manifest()))
    digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()

    def discover(**_kwargs):
        return {TIER: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                       "tier_id": TIER, "host": "dl380g10", "tier": "stage",
                       "mountpoint": str(tmp_path / "stage"),
                       "capacity_bytes": 8 * PHASE_BYTES}}

    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=discover)
    template = _template(digest, manifest_path.stat().st_size,
                         measurement=measurement)
    cas = _Cas(manifest_path, tmp_path / "cas")
    consumer = pbrun.seal_action_from_template(template)
    cas.publish_action_request(consumer)
    args = types.SimpleNamespace(
        priority=-10, max_attempts=1, retry_safe=False,
        residency="stage", residency_tier=None, residency_mover_mem_gb=1,
        residency_mover_readers=READERS, residency_mover_max_attempts=3)
    staged = pbrun.residency_stage_rows(
        template, consumer_action_key=str(consumer["action_key"]),
        tier=pbrun.resolve_stage_tier(queue, None), args=args, queue=queue,
        cas=cas)
    phase = staged["plan"]["phases"][0]
    return queue, template, consumer, phase["mover_row"], phase["egress_row"], cas


def _busy_stage_host(monkeypatch) -> None:
    """dl380g10 as it always is: sampled fresh, one CPU of eight busy."""

    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": time.time(), "cpu_count": 8, "interval_s": 1.,
        "busy_cpus": FOREIGN_BUSY_CPUS, "psi_some": 0.})


def _claim(queue: pool.PoolQueue):
    return queue.claim(capacity=CAPACITY, tags=["dl380g10"], cpu_tiers=CPU_TIERS,
                       adaptive_cpu=True)


def _denial(queue: pool.PoolQueue, key: str) -> dict:
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(value for value in records.values() if value["action_key"] == key)


def test_a_measurement_consumers_mover_is_admitted_on_a_busy_stage_host(
        tmp_path, monkeypatch) -> None:
    """The live wedge, through one admission path.

    The consumer and its mover are ready on the same busy host.  The
    consumer is refused ``measurement_host_not_idle``, as before; the mover
    is admitted beside the load, because it is not a measurement.
    """

    queue, _template_, consumer, mover_row, _egress, cas = _seal(tmp_path, monkeypatch)
    _busy_stage_host(monkeypatch)
    consumer_key = str(consumer["action_key"])
    queue.publish(action_key=consumer_key, cas_root=str(cas.cas.root),
                  checkout_root=str(tmp_path), worker_script="worker.py",
                  resources={"cpu": 1, "mem_gb": 1}, tags=["dl380g10"])
    assert adaptive_cpu.action_identity(
        {"action_key": consumer_key, "cas_root": str(cas.cas.root)})[1] is True

    queue.publish(**mover_row)
    claim = _claim(queue)
    assert claim is not None, (
        "a measurement consumer's mover must run on a stage host with foreign load")
    assert claim["action_key"] == mover_row["action_key"]

    decision = _denial(queue, consumer_key)["evidence"]["decision"]
    assert decision["reason"] == "measurement_host_not_idle", decision
    assert queue.item_path(pool.READY, consumer_key).exists()


def test_the_consumer_alone_is_still_held_to_an_idle_host(
        tmp_path, monkeypatch) -> None:
    """The consumer's isolation does not change: busy refuses, idle admits."""

    queue, _template_, consumer, _mover, _egress, cas = _seal(tmp_path, monkeypatch)
    consumer_key = str(consumer["action_key"])
    queue.publish(action_key=consumer_key, cas_root=str(cas.cas.root),
                  checkout_root=str(tmp_path), worker_script="worker.py",
                  resources={"cpu": 1, "mem_gb": 1}, tags=["dl380g10"])
    _busy_stage_host(monkeypatch)
    assert _claim(queue) is None
    decision = _denial(queue, consumer_key)["evidence"]["decision"]
    assert decision["reason"] == "measurement_host_not_idle", decision

    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": time.time(), "cpu_count": 8, "interval_s": 1.,
        "busy_cpus": 0., "psi_some": 0.})
    claim = _claim(queue)
    assert claim is not None and claim["action_key"] == consumer_key


@pytest.mark.parametrize("row", ["mover_row", "egress_row"])
def test_every_movement_node_is_sealed_as_portable_generation_work(
        tmp_path, monkeypatch, row: str) -> None:
    """The sealed bodies: a mover's class, scope and toolchain, not the Spark's.

    With the consumer's scope, an x86_64 worker's preflight refuses the mover
    on the platform key, as it refuses the consumer; with a mover's, it does
    not.
    """

    _queue, _template_, consumer, mover_row, egress_row, cas = _seal(
        tmp_path, monkeypatch)
    body = cas.actions[str({"mover_row": mover_row,
                            "egress_row": egress_row}[row]["action_key"])]
    assert body["task"]["task_class"] == "generation"
    assert (body["task"]["artifact_family"], body["task"]["artifact_kind"]) == (
        "generic", "generic")
    assert body["execution_scope"] == movement_actions.MOVEMENT_EXECUTION_SCOPE
    assert body["environment"]["toolchain"] == {}
    assert adaptive_cpu.action_identity(
        {"action_key": body["action_key"], "cas_root": str(cas.cas.root)})[1] is False

    pb._validate_scope_labels(body, platform_key="linux-x86_64", host_class=None)
    with pytest.raises(pb.ActionContractError, match="platform_key"):
        pb._validate_scope_labels(consumer, platform_key="linux-x86_64",
                                  host_class=None)


def test_a_generation_consumers_movers_keep_their_keys(tmp_path, monkeypatch) -> None:
    """Off for everything that already worked: a generation consumer's mover
    carries exactly the task, scope and toolchain it carried before, so its
    key is the one it always had."""

    _queue, template, _consumer, mover_row, egress_row, cas = _seal(
        tmp_path, monkeypatch, measurement=False)
    for row in (mover_row, egress_row):
        body = cas.actions[str(row["action_key"])]
        task = {name: value for name, value in body["task"].items()
                if name not in ("argv", "result_path")}
        assert task == template["task"]
        assert body["execution_scope"] == template["execution_scope"]
        assert body["environment"]["toolchain"] == template["environment"]["toolchain"]

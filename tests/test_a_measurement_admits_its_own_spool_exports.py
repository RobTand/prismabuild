"""A measurement's own spool exports run under its isolation (#982).

On 2026-09-23 two Stage A runs submitted as ``--measurement`` were ended by
their own 900 s stall watchdog.  Each one counts a unit of progress when a
64-entry group's spool export lands, and each export is its own action pinned
to the measurement's host at one CPU and one GiB.  While the measurement held
the host, ``adaptive_cpu`` refused every other action there
``measurement_holder`` -- the measurement's own exports included.  Run (a)
``86a9e247b13e`` published twenty exports on sparklina between 14:43:04Z and
14:56:18Z; none was claimed until the watchdog ended it at 14:56:32Z.

The export's sealed request names its producer in
``params.produced_spool.owner`` (``ProducedSpool.submit_group``).  An action
whose owner is the measurement holding the host is admitted beside it on the
host's free tokens; every other action is still refused, and the refusal's
denial record names the holder.

Fixture concessions: the requests are sealed by the real
``pbrun.seal_action_from_template``, ``produced_output._producer_movement_template``
and ``movement_actions.seal_movement_action`` and published to a real CAS, so
admission reads the task class and the owner off the sealed bytes.  The
measurement is claimed through the real adaptive path, so its holder carries
the metadata a live measurement does.  Only the CPU sample is stated.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import adaptive_cpu, core as pb  # noqa: E402
from prismabuild import movement_actions, pool  # noqa: E402
from prismabuild import produced_output as po  # noqa: E402

HOST = "sparklina"
#: Sized like the live Spark: 20 CPUs and 104 GiB offered.
CAPACITY = {"cpu": 20, "mem_gb": 104}
CPU_TIERS = {"preferred": list(range(20)), "fallback": []}
#: Run (a)'s reservation: half the CPUs, all but three GiB of the memory.
MEASUREMENT_DEMAND = {"cpu": 10, "mem_gb": 101}
#: What the running measurement burns, as its live telemetry reports it.
MEASUREMENT_CPU_PER_S = 8.0
#: What ``ProducedSpool.submit_group`` seals for an unpaced export.
EXPORT_DEMAND = {"cpu": 1, "mem_gb": 1}


def _template(*, measurement: bool, command: list[str]) -> dict[str, object]:
    """A ``pbrun`` template, shaped as ``--measurement`` seals it or not."""

    import pbrun

    if measurement:
        scope = {"portability": "platform_keyed",
                 "platform_key": "linux-aarch64-sm121", "host_class": None}
        toolchain = {"argv0.sha256": "e" * 64, "argv0.bytes": "1446024",
                     "system": "linux", "machine": "aarch64",
                     "libc": "glibc-2.39", "cuda_compute_capability": "12.1"}
        demand = MEASUREMENT_DEMAND
    else:
        scope = {"portability": "portable", "platform_key": None, "host_class": None}
        toolchain = {}
        demand = EXPORT_DEMAND
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
        "params": {"command": command, "cwd": "/home/rob",
                   "demand": dict(demand),
                   "placement": {"required_tags": [HOST]},
                   "checkout_snapshot": {
                       "schema": pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1,
                       "commit": "a" * 40, "subdirectory": ".",
                       "input": {"id": pb.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID,
                                 "sha256": "b" * 64, "bytes": 4096}},
                   "retry_policy": {"max_attempts": 1, "retry_safe": False}},
        "environment": {"variables": {"PATH": "/usr/bin"}, "toolchain": toolchain},
        "execution_scope": scope,
    }


def _sample(monkeypatch, busy_cpus: float) -> None:
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": time.time(), "cpu_count": 20, "interval_s": 1.,
        "busy_cpus": busy_cpus, "psi_some": 0.})


def _claim(queue: pool.PoolQueue):
    return queue.claim(capacity=CAPACITY, tags=[HOST], cpu_tiers=CPU_TIERS,
                       adaptive_cpu=True)


def _publish(queue: pool.PoolQueue, cas: pb.PrismaBuildCAS, action, demand) -> str:
    cas.publish_action_request(action)
    key = str(action["action_key"])
    queue.publish(action_key=key, cas_root=str(cas.root), checkout_root="/co",
                  worker_script="worker.py", resources=dict(demand), tags=[HOST],
                  priority=-10)
    return key


def _export(queue: pool.PoolQueue, producer: dict, *, owner: str,
            batch_id: str) -> dict:
    """One spool export, sealed exactly as ``ProducedSpool.submit_group`` does."""

    manifest_input = {"id": "produced-spool-manifest",
                      "sha256": hashlib.sha256(batch_id.encode()).hexdigest(),
                      "bytes": 4096}
    templated = po._producer_movement_template(
        queue, producer, str(producer["action_key"]), extra_inputs=[manifest_input])
    assert templated["ok"], templated
    return movement_actions.seal_movement_action(
        templated["template"],
        command=["/usr/bin/python3", "tools/fleet/produced_export.py",
                 "--queue", str(queue.root), "--manifest", f"/spool/{batch_id}.json",
                 "--manifest-sha256", manifest_input["sha256"]],
        demand=EXPORT_DEMAND, tags=[HOST],
        log_name=f"produced-export-{batch_id}.log",
        retry_policy={"max_attempts": 3, "retry_safe": True},
        extra_params={"produced_spool": {"manifest_sha256": manifest_input["sha256"],
                                         "owner": owner, "batch_id": batch_id}})


def _denial(queue: pool.PoolQueue, key: str) -> dict:
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(value for value in records.values() if value["action_key"] == key)


def _running(queue: pool.PoolQueue, key: str, monkeypatch) -> None:
    """The measurement is running, as every live holder is.

    Its CPUs are busy, and its worker's sampler reports what it burns (fresh
    to ~2 s for every live holder on both Sparks, 2026-09-23): two readings
    five seconds apart, the second current, both after its admission.
    """

    _sample(monkeypatch, MEASUREMENT_CPU_PER_S)
    meta = adaptive_cpu.read_json(queue.ledger().held_dir / key / adaptive_cpu.METADATA)
    base = adaptive_cpu.local_state_base(queue.ledger().base)
    current = {"action_key": key, "complete": True, "nonce": "live",
               "sampled_unix": time.time(), "wall_seconds": 15.,
               "cpu_seconds": 15. * MEASUREMENT_CPU_PER_S, "memory_peak_bytes": 0}
    adaptive_cpu.write_json(base / "telemetry" / f"{key}.json", current)
    adaptive_cpu.write_json(base / "jobs.json", {key: dict(
        current, sampled_unix=meta["admitted_unix"], wall_seconds=10.,
        cpu_seconds=10. * MEASUREMENT_CPU_PER_S)})


def _holding_measurement(tmp_path: Path, monkeypatch):
    """A measurement claimed on an idle host, now running and holding it."""

    import pbrun

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    measurement = pbrun.seal_action_from_template(
        _template(measurement=True, command=["python", "stage_a.py"]))
    key = _publish(queue, cas, measurement, MEASUREMENT_DEMAND)
    assert adaptive_cpu.action_identity(
        {"action_key": key, "cas_root": str(cas.root)})[1] is True
    _sample(monkeypatch, 0.)
    claim = _claim(queue)
    assert claim is not None and claim["action_key"] == key
    meta = adaptive_cpu.read_json(queue.ledger().held_dir / key / adaptive_cpu.METADATA)
    assert meta.get("measurement") is True, meta
    _running(queue, key, monkeypatch)
    return queue, cas, measurement, key


def test_a_measurements_own_pinned_export_is_admitted_beside_it(
        tmp_path, monkeypatch) -> None:
    """The live wedge: the holder's own export, pinned to its host, is claimed.

    On main this export was refused ``measurement_holder`` on every pass.
    It is admitted on the host's free tokens; the measurement's reservation
    is untouched and none of its CPUs is lent.
    """

    queue, cas, measurement, holder = _holding_measurement(tmp_path, monkeypatch)
    export = _export(queue, measurement, owner=holder, batch_id="g0")
    export_key = _publish(queue, cas, export, EXPORT_DEMAND)

    _running(queue, holder, monkeypatch)
    claim = _claim(queue)
    if claim is None:
        decision = _denial(queue, export_key)["evidence"]["decision"]
        raise AssertionError(
            "a measurement's own spool export must run under its isolation; "
            f"it was refused {decision.get('reason')} by {decision.get('holder')}")
    assert claim["action_key"] == export_key
    assert adaptive_cpu.dependent_owner(
        {"action_key": export_key, "cas_root": str(cas.root)}) == holder

    held = queue.ledger().held_dir
    measurement_cpus = set(queue.ledger().cpu_allocation(holder, CPU_TIERS)["preferred"])
    export_cpus = set(queue.ledger().cpu_allocation(export_key, CPU_TIERS)["preferred"])
    assert len(list((held / holder).glob("cpu-*"))) == MEASUREMENT_DEMAND["cpu"]
    assert len(list((held / holder).glob("mem_gb-*"))) == MEASUREMENT_DEMAND["mem_gb"]
    assert len(list((held / export_key).glob("cpu-*"))) == EXPORT_DEMAND["cpu"]
    assert len(list((held / export_key).glob("mem_gb-*"))) == EXPORT_DEMAND["mem_gb"]
    assert not measurement_cpus & export_cpus
    meta = adaptive_cpu.read_json(held / export_key / adaptive_cpu.METADATA)
    assert meta["serves_measurement"] == holder and meta["borrowed_cpu"] == 0


def test_foreign_work_is_still_refused_and_the_refusal_names_the_holder(
        tmp_path, monkeypatch) -> None:
    """Isolation holds for everything else, and every refusal is recorded.

    Two foreign actions: an ordinary ``pbrun`` action, and an export whose
    owner is some other producer.  Each is refused ``measurement_holder``
    with a denial record naming the holder's key.  They are ahead of the
    holder's own export in the ready order, and this measurement declares no
    run bound, so it reads as a transient holder: under #924 alone each
    refusal would withhold the box and end the scan before the export.  The
    export is admitted past them, and they keep their passes, denied starved.
    """

    import pbrun

    queue, cas, measurement, holder = _holding_measurement(tmp_path, monkeypatch)
    plain = pbrun.seal_action_from_template(
        _template(measurement=False, command=["python", "foreign.py"]))
    stranger = _export(queue, measurement, owner="c" * 64, batch_id="g-stranger")
    foreign = [_publish(queue, cas, plain, EXPORT_DEMAND),
               _publish(queue, cas, stranger, EXPORT_DEMAND)]
    for key in foreign:
        assert _claim(queue) is None
        denial = _denial(queue, key)
        assert denial["reason"].startswith("adaptive_cpu_refused"), denial
        decision = denial["evidence"]["decision"]
        assert decision["reason"] == "measurement_holder", decision
        assert decision["holder"] == holder, decision
        assert queue.item_path(pool.READY, key).exists()

    own_key = _publish(queue, cas, _export(queue, measurement, owner=holder, batch_id="g1"),
                       EXPORT_DEMAND)
    _running(queue, holder, monkeypatch)
    claim = _claim(queue)
    assert claim is not None and claim["action_key"] == own_key
    for key in foreign:
        assert queue.item_path(pool.READY, key).exists()
        assert queue.passes(key) >= 2
        denial = _denial(queue, key)
        assert denial["reason"] == "adaptive_cpu_refused_starved", denial
        assert denial["evidence"]["withhold"]["why"] == "measurement_admits_only_its_dependents"
        decision = denial["evidence"]["decision"]
        assert decision["holder"] == holder and decision["isolated_by"] == holder
    assert _denial(queue, foreign[1])["evidence"]["decision"]["dependent_of"] == "c" * 64
    assert _denial(queue, foreign[0])["evidence"]["decision"]["dependent_of"] is None


def test_a_waiting_measurement_is_never_a_dependent(tmp_path, monkeypatch) -> None:
    """A second measurement naming the holder as its owner is still refused:
    a measurement never runs beside another holder."""

    import pbrun

    queue, cas, _measurement, holder = _holding_measurement(tmp_path, monkeypatch)
    template = _template(measurement=True, command=["python", "second.py"])
    template["params"]["produced_spool"] = {"owner": holder, "batch_id": "x",
                                            "manifest_sha256": "d" * 64}
    second = pbrun.seal_action_from_template(template)
    key = _publish(queue, cas, second, EXPORT_DEMAND)
    assert adaptive_cpu.dependent_owner({"action_key": key, "cas_root": str(cas.root)}) is None
    # Idle, so the refusal is the holder's and not the busy host's.
    _sample(monkeypatch, 0.)
    assert _claim(queue) is None
    decision = _denial(queue, key)["evidence"]["decision"]
    assert decision["reason"] == "measurement_holder" and decision["holder"] == holder
    assert decision["isolated_by"] == holder


def test_a_measurement_waiting_behind_ordinary_work_is_not_isolated(tmp_path) -> None:
    """``isolated_by`` is set only by a measurement holder, so a measurement
    waiting behind ordinary work keeps the #924 exclusive withhold."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 2})
    assert ledger.acquire("0" * 64, {"cpu": 1})
    controller = adaptive_cpu.Controller(ledger, {"preferred": [0, 1], "fallback": []})
    controller._host_sample = {"sampled_unix": time.time(), "cpu_count": 2,
                               "interval_s": 1., "busy_cpus": 0., "psi_some": 0.}
    assert controller.decision({"action_key": "a" * 64}, {"cpu": 1},
                               identity=("shape", True)) is None
    decision = controller.last_decision
    assert decision["reason"] == "measurement_holder" and decision["holder"] == "0" * 64
    assert decision["isolated_by"] is None
    assert pool._adaptive_refusal_drains(
        "adaptive_cpu_refused", decision, demand={"cpu": 1}, measurement=True,
        cpu_count=2) == ("exclusive", False)

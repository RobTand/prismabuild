"""A producer's spool exports run on room it reserved at claim (#985).

On 2026-09-23 run (a) ``fea8c30f64b9``, a chained Stage A producer, held 10
CPUs and 101 of sparklina's 104 GiB and committed a unit as each group's spool
export landed.  Each export is its own ``cpu 1, mem_gb 1`` action tagged with
the producer's host.  For 13 minutes none was claimed:

- The producer's threads, pinned to its 10 CPUs, held host ``psi_some`` near
  0.10.  The export's first free CPU was CPU 0, whose housekeeping ran at
  6.67%, above ``IDLE_BUSY_FRACTION``.  Every export was refused
  ``host_pressure``, before the holder loop where #984's dependent exemption
  lives.
- Two foreign 2-CPU/3 GiB actions took the last 3 GiB in turn, and the #924
  withhold drained the box for them.

A producer whose sealed environment configures a spool now reserves one
export's room with its own claim, and its exports run on that room: host
pressure on its CPUs does not refuse them, and no foreign action can take it.
Everything else is admitted exactly as before.

Fixture concessions: the producer's request is sealed and filed in a real
CAS by the produced-output fixtures, it is claimed through the real adaptive
path on sparklina's live CPU map, and its exports are published by the real
``ProducedSpool.submit_group``.  Only the CPU sample and the producer's live
telemetry are stated.
"""
from __future__ import annotations

from pathlib import Path
import time

import test_prepaid_writer_integration as fx
import test_produced_spool as sp
from prismabuild import adaptive_cpu, core, pool, produced_output as po
from prismabuild import produced_spool as ps

_isolated_synthetic_launch_context = fx._isolated_synthetic_launch_context

#: Sized like the live Spark: 20 CPUs and 104 GiB offered.
CAPACITY = {"cpu": 20, "mem_gb": 104}
#: sparklina's ``reservations/sparklina/cpu-map.json``, 2026-09-23.  A
#: 10-CPU producer takes the ten preferred CPUs, so the next free token is
#: fallback CPU 0 -- the export's CPU in the live wedge.
CPU_TIERS = {"preferred": [5, 6, 7, 8, 9, 15, 16, 17, 18, 19],
             "fallback": [0, 1, 2, 3, 4, 10, 11, 12, 13, 14]}
#: Run (a)'s reservation.
PRODUCER_DEMAND = {"cpu": 10, "mem_gb": 101}
#: What the producer burns on its pinned CPUs, as its telemetry reports it.
PRODUCER_CPU_PER_S = 8.0
#: CPU 0's housekeeping in the live denial ``a4c82c7a8a86`` (0.0667).
HOUSEKEEPING = 0.07
MEASUREMENT_SCOPE = {"portability": "platform_keyed",
                     "platform_key": "linux-aarch64-sm121", "host_class": None}
MEASUREMENT_TOOLCHAIN = {"argv0.sha256": "e" * 64, "argv0.bytes": "1446024",
                         "system": "linux", "machine": "aarch64",
                         "libc": "glibc-2.39", "cuda_compute_capability": "12.1"}


def _sample(monkeypatch, *, psi: float, per_cpu: dict[int, float] | None = None) -> None:
    busy = {str(cpu): float((per_cpu or {}).get(cpu, 0.)) for cpu in range(20)}
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": time.time(), "cpu_count": 20, "interval_s": 1.,
        "busy_cpus": sum(busy.values()), "psi_some": psi, "per_cpu_busy": dict(busy)})


def _claim(queue: pool.PoolQueue, host: str):
    return queue.claim(capacity=CAPACITY, tags=[host], cpu_tiers=CPU_TIERS,
                       adaptive_cpu=True)


def _meta(queue: pool.PoolQueue, key: str) -> dict:
    return adaptive_cpu.read_json(queue.ledger().held_dir / key / adaptive_cpu.METADATA)


def _tokens(queue: pool.PoolQueue, key: str, kind: str) -> int:
    return len(list((queue.ledger().held_dir / key).glob(f"{kind}-*")))


def _denial(queue: pool.PoolQueue, key: str) -> dict:
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(value for value in records.values() if value["action_key"] == key)


def _producer(tmp_path: Path, monkeypatch, *, measurement: bool = False,
              demand: dict[str, int] = PRODUCER_DEMAND):
    """A spool producer claimed through the adaptive path on an idle host.

    Its sealed environment names a spool root, as every Stage A chain's
    does, and nothing else about the allowance: the default applies.
    """

    cas_root = tmp_path / "cas"
    template = fx._template(str(tmp_path / "canonical"))
    initial = fx._producer_request(tmp_path, cas_root, template)
    cas, request = po._read_producer_request(cas_root, initial)
    request.pop("action_key")
    request["environment"]["variables"].update({ps.ROOT_ENV: str(tmp_path / "local"),
                                                ps.MAX_ENV: "256"})
    if measurement:
        request["task"].update(task_class="measurement", determinism="stochastic")
        request["execution_scope"] = dict(MEASUREMENT_SCOPE)
        request["environment"]["toolchain"] = dict(MEASUREMENT_TOOLCHAIN)
    action = core.seal_action(request)
    cas.publish_action_request(action)
    owner = str(action["action_key"])
    queue = fx._queue(tmp_path)
    queue.publish(action_key=owner, cas_root=str(cas_root),
                  worker_script=str(fx.REPO / "tools" / "prismabuild_worker.py"),
                  checkout_root=str(tmp_path / "mover-checkout"),
                  resources={**demand, **po.owner_demand_terms(template)},
                  produced_output_template=template)
    _sample(monkeypatch, psi=0.)
    claimed = queue.claim(owner="spool-producer", capacity=CAPACITY,
                          cpu_tiers=CPU_TIERS, adaptive_cpu=True)
    assert claimed is not None and claimed["action_key"] == owner
    assert bool(_meta(queue, owner).get("measurement")) is measurement
    control = fx._broker_control(queue, owner)
    po.declare_template(queue.root, template)
    instance = po.bind_instance(queue, template, owner_action_key=owner,
        claim_snapshot=claimed, env={"PRISMABUILD_ACTION_KEY": owner,
            "PRISMABUILD_ACTION_NONCE": control["nonce"],
            "PRISMABUILD_ACTION_SCOPE": control["scope_id"]})
    po.declare_instance(queue.root, instance)
    assert po.admit_instance(queue, instance, template)["ok"]
    fx._announce_tier(queue, tmp_path / "stage")
    spool = ps.ProducedSpool(queue, instance, template, cas_root=cas_root,
                             root=tmp_path / "local", max_bytes=256)
    return spool, cas


def _running(spool, monkeypatch, *, psi: float, cpu_per_s: float = PRODUCER_CPU_PER_S,
             telemetry_age_s: float = 0.) -> None:
    """The producer is running: its pinned CPUs are loaded, CPU 0 and CPU 1
    run housekeeping, and its worker's sampler reports what it burns (fresh
    to ~2 s for every live holder on both Sparks, 2026-09-23).

    ``telemetry_age_s`` ages the sampler's last record instead: the producer
    was admitted a minute ago, and its record is that many seconds old.
    """

    queue, owner = spool.queue, spool.owner
    meta = _meta(queue, owner)
    if telemetry_age_s:
        meta["admitted_unix"] -= 60.
        adaptive_cpu.write_json(queue.ledger().held_dir / owner / adaptive_cpu.METADATA, meta)
    pinned = meta["allocation"]["preferred"] + meta["allocation"]["fallback"]
    load = {cpu: cpu_per_s / len(pinned) for cpu in pinned}
    # CPU 1 is set busy too, so the foreign action the tests offer beside the
    # export (its first free CPU is 1 once CPU 0 is the producer's) meets the
    # unchanged pressure gate with something to refuse.
    _sample(monkeypatch, psi=psi, per_cpu={0: HOUSEKEEPING, 1: HOUSEKEEPING, **load})
    base = adaptive_cpu.local_state_base(queue.ledger().base)
    current = {"action_key": owner, "complete": True, "nonce": "live",
               "sampled_unix": time.time() - telemetry_age_s, "wall_seconds": 15.,
               "cpu_seconds": 15. * cpu_per_s, "memory_peak_bytes": 0}
    adaptive_cpu.write_json(base / "telemetry" / f"{owner}.json", current)
    adaptive_cpu.write_json(base / "jobs.json", {owner: dict(
        current, sampled_unix=meta["admitted_unix"], wall_seconds=10.,
        cpu_seconds=10. * cpu_per_s)})


def _export(spool, batch: str) -> str:
    _source, _destination, entries = sp.prepare(spool, batch=batch)
    handle = spool.submit_group(batch, entries)
    assert handle["ok"], handle
    return str(handle["export_key"])


def _plain_request(cas, definition_id: str, variables: dict[str, str]) -> str:
    """Seal and file an ordinary action's request; return its key."""

    request = {
        "schema": core.ACTION_SCHEMA_V2,
        "task": {"definition_id": definition_id, "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": ["/bin/true"], "working_directory": ".", "result_path": "result"},
        "inputs": [],
        "code_closure": core.build_code_closure(fx.REPO, ["tools/fleet/stage_release.py"]),
        "params": {"cwd": ".", "command": ["/bin/true"]},
        "environment": {"variables": {"PATH": "/usr/bin:/bin", **variables},
                        "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    }
    action = core.seal_action(request)
    cas.publish_action_request(action)
    return str(action["action_key"])


def _foreign(spool, cas, name: str, demand: dict[str, int]) -> str:
    """An ordinary action tagged with the producer's host, as the two live
    2-CPU/3 GiB actions were.  Same priority as the producer and so as its
    exports, and published first: it is ahead of them in the ready order."""

    key = _plain_request(cas, f"tests/foreign-{name}", {})
    spool.queue.publish(action_key=key, cas_root=str(cas.root), checkout_root="/co",
                        worker_script="worker.py", resources=dict(demand),
                        tags=[spool.host])
    return key


def _assert_on_allowance(spool, export_key: str) -> None:
    """The export runs on the producer's reserved CPU and takes no token."""

    queue, owner = spool.queue, spool.owner
    producer = _meta(queue, owner)
    # The producer paid for the room at claim: one CPU and one GiB beyond its
    # sealed demand, and the allowance CPU is out of its own affinity.
    assert _tokens(queue, owner, "cpu") == PRODUCER_DEMAND["cpu"] + 1
    assert _tokens(queue, owner, "mem_gb") == PRODUCER_DEMAND["mem_gb"] + 1
    assert producer["dependent_allowance"] == {"slots": 1, "cpus": [0], "mem_gb": 1}
    assert producer["allocation"] == {"preferred": CPU_TIERS["preferred"], "fallback": []}
    export = _meta(queue, export_key)
    assert export["funded_by"] == owner
    assert export["funded"] == {"cpus": [0], "mem_gb": 1}
    assert export["allocation"] == {"preferred": [], "fallback": [0]}
    assert export["borrowed_cpu"] == 0
    assert _tokens(queue, export_key, "cpu") == _tokens(queue, export_key, "mem_gb") == 0


def test_host_pressure_on_the_producers_cpus_does_not_refuse_its_export(
        tmp_path, monkeypatch) -> None:
    """The live wedge: ``psi_some`` 0.12 from the producer's pinned CPUs, and
    CPU 0 at 7%.  A foreign 1-CPU action ahead of the export is still refused
    ``host_pressure``; the export is claimed in the same pass."""

    spool, cas = _producer(tmp_path, monkeypatch)
    foreign = _foreign(spool, cas, "one-cpu", {"cpu": 1, "mem_gb": 1})
    export_key = _export(spool, "g0")
    _running(spool, monkeypatch, psi=0.12)

    claim = _claim(spool.queue, spool.host)
    if claim is None or claim["action_key"] != export_key:
        decision = _denial(spool.queue, export_key)["evidence"]["decision"]
        raise AssertionError(
            "a producer's own export must not be refused by pressure on the "
            f"producer's CPUs; it was refused {decision.get('reason')} "
            f"on CPUs {decision.get('cpus')}")
    _assert_on_allowance(spool, export_key)
    # The row names its producer, as ``submit_group`` published it.
    assert claim["dependent_of"] == spool.owner

    assert spool.queue.item_path(pool.READY, foreign).exists()
    decision = _denial(spool.queue, foreign)["evidence"]["decision"]
    assert decision["reason"] == "host_pressure" and decision["cpus"] == [1], decision
    assert decision.get("dependent_of") is None

    # A second export while the first holds the one slot is past the
    # allowance.  It meets the ordinary gates on free tokens -- its first
    # free CPU is 1, busy -- and its refusal names the producer and why the
    # allowance did not cover it.
    second = _export(spool, "g1")
    assert _claim(spool.queue, spool.host) is None
    denial = _denial(spool.queue, second)
    assert denial["evidence"]["dependent_of"] == spool.owner, denial
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "host_pressure" and decision["cpus"] == [1], decision
    assert decision["dependent_of"] == spool.owner
    assert decision["allowance"] == "allowance_in_use"


def test_a_foreign_action_cannot_take_the_exports_room(tmp_path, monkeypatch) -> None:
    """A foreign 2-CPU/3 GiB action arrives before the export.  The export is
    claimed within one pass and the foreign action waits.  A second export
    while the first holds the one slot falls back to free tokens, as every
    export did before the allowance existed."""

    spool, cas = _producer(tmp_path, monkeypatch)
    foreign = _foreign(spool, cas, "three-gib", {"cpu": 2, "mem_gb": 3})
    export_key = _export(spool, "g0")
    _running(spool, monkeypatch, psi=0.)

    claim = _claim(spool.queue, spool.host)
    assert claim is not None and claim["action_key"] == export_key, (
        "the foreign action took the export's room" if claim else "nothing was claimed")
    _assert_on_allowance(spool, export_key)
    assert spool.queue.item_path(pool.READY, foreign).exists()

    second = _export(spool, "g1")
    claim = _claim(spool.queue, spool.host)
    assert claim is not None and claim["action_key"] == second
    # Ordinary admission: its memory is a free token, and its CPU is a free
    # one or, as here, one borrowed from the producer's idle reservation --
    # never the allowance CPU the first export runs on.
    meta = _meta(spool.queue, second)
    assert "funded_by" not in meta
    assert _tokens(spool.queue, second, "mem_gb") == 1
    cpus = meta["allocation"]["preferred"] + meta["allocation"]["fallback"]
    assert len(cpus) == 1 and 0 not in cpus, meta["allocation"]
    assert spool.queue.item_path(pool.READY, foreign).exists()


def test_a_measurements_export_runs_under_pressure_it_makes(tmp_path, monkeypatch) -> None:
    """A measurement holding the host, loading its own CPUs to ``psi_some``
    0.12: its export is claimed beside it, which #984 alone did not do
    because the pressure gate ran first."""

    spool, _cas = _producer(tmp_path, monkeypatch, measurement=True)
    export_key = _export(spool, "g0")
    _running(spool, monkeypatch, psi=0.12)

    claim = _claim(spool.queue, spool.host)
    if claim is None:
        decision = _denial(spool.queue, export_key)["evidence"]["decision"]
        raise AssertionError(
            "a measurement's own export must run under the pressure it makes; "
            f"it was refused {decision.get('reason')}")
    assert claim["action_key"] == export_key
    _assert_on_allowance(spool, export_key)
    assert _meta(spool.queue, export_key)["serves_measurement"] == spool.owner


def test_an_export_published_without_the_hint_is_funded_from_its_sealed_owner(
        tmp_path, monkeypatch) -> None:
    """Mid-campaign publish: a producer sealed under an older generation runs
    that generation's ``submit_group``, which publishes its exports without
    the row's ``dependent_of``.  The sealed request names the owner either
    way, so the export is funded from the allowance all the same."""

    spool, cas = _producer(tmp_path, monkeypatch)
    publish = spool.queue.publish

    def as_older_generation(**kwargs):
        kwargs.pop("dependent_of", None)
        return publish(**kwargs)

    monkeypatch.setattr(spool.queue, "publish", as_older_generation)
    export_key = _export(spool, "g0")
    row = pool._read_json(spool.queue.item_path(pool.READY, export_key))
    assert "dependent_of" not in row
    _running(spool, monkeypatch, psi=0.12)

    claim = _claim(spool.queue, spool.host)
    if claim is None:
        decision = _denial(spool.queue, export_key)["evidence"]["decision"]
        raise AssertionError(
            "an export without the row hint must be funded from its sealed owner; "
            f"it was refused {decision.get('reason')} on CPUs {decision.get('cpus')}")
    assert claim["action_key"] == export_key
    _assert_on_allowance(spool, export_key)


def test_a_withholding_foreign_action_does_not_hold_the_export_out(
        tmp_path, monkeypatch) -> None:
    """The 15:52-16:05Z window: a foreign 2-CPU/3 GiB action ahead of the
    export reaches the #924 floor and withholds the box for the producer to
    drain.  The scan goes on past it for the producer's export, which takes
    nothing the foreign action waits for.  A second export, past the
    allowance, would take free tokens: it is deferred behind the withhold
    with no pass, and a non-dependent behind it is not considered at all."""

    spool, cas = _producer(tmp_path, monkeypatch)
    foreign = _foreign(spool, cas, "three-gib", {"cpu": 2, "mem_gb": 3})
    _running(spool, monkeypatch, psi=0.)
    for _ in range(pool.STARVATION_FLOOR + 1):
        assert _claim(spool.queue, spool.host) is None, "the foreign action took the export's room"
        if _denial(spool.queue, foreign)["reason"].endswith("_withholding"):
            break
    assert _denial(spool.queue, foreign)["reason"] == "reservation_unavailable_withholding"

    export_key = _export(spool, "g0")
    _running(spool, monkeypatch, psi=0.)
    claim = _claim(spool.queue, spool.host)
    if claim is None:
        denial = _denial(spool.queue, export_key)
        raise AssertionError(f"the export was held out behind the withhold: {denial['reason']}")
    assert claim["action_key"] == export_key
    _assert_on_allowance(spool, export_key)
    assert spool.queue.item_path(pool.READY, foreign).exists()

    second = _export(spool, "g1")
    bystander = _foreign(spool, cas, "bystander", {"cpu": 1, "mem_gb": 1})
    assert _claim(spool.queue, spool.host) is None
    assert _denial(spool.queue, foreign)["reason"] == "reservation_unavailable_withholding"
    denial = _denial(spool.queue, second)
    assert denial["reason"] == "deferred_behind_withholding", denial
    assert denial["evidence"]["withheld_for"] == foreign
    assert denial["evidence"]["dependent_of"] == spool.owner
    assert spool.queue.passes(second) == 0
    assert spool.queue.item_path(pool.READY, bystander).exists()
    assert spool.queue.passes(bystander) == 0


def test_stale_owner_telemetry_does_not_project_the_export_against_its_owner(
        tmp_path, monkeypatch) -> None:
    """WS-DA finding 13b: a measurement loading all ten of its CPUs, whose
    sampler's last record is 6 s old (past ``MAX_SAMPLE_AGE_S``).  Its whole
    reservation is charged as pending, so an export priced beside it projects
    10 busy + 10 pending + 1 > 20 and is refused ``projected_cpu_cost``.  On
    the allowance it is not projected against its owner at all."""

    spool, _cas = _producer(tmp_path, monkeypatch, measurement=True)
    export_key = _export(spool, "g0")
    _running(spool, monkeypatch, psi=0., cpu_per_s=10., telemetry_age_s=6.)
    assert 6. > adaptive_cpu.MAX_SAMPLE_AGE_S

    claim = _claim(spool.queue, spool.host)
    if claim is None:
        decision = _denial(spool.queue, export_key)["evidence"]["decision"]
        raise AssertionError(
            "a producer's export must not be projected against its producer's "
            f"reservation; it was refused {decision.get('reason')}")
    assert claim["action_key"] == export_key
    _assert_on_allowance(spool, export_key)


def test_a_spool_producer_that_fits_only_without_the_allowance_runs_as_before(
        tmp_path, monkeypatch) -> None:
    """The allowance is never what keeps a producer off a box: one that fits
    only without it is claimed without it, and its exports use free tokens."""

    spool, _cas = _producer(tmp_path, monkeypatch, demand={"cpu": 10, "mem_gb": 104})
    assert _tokens(spool.queue, spool.owner, "mem_gb") == 104
    assert _tokens(spool.queue, spool.owner, "cpu") == 10
    assert _meta(spool.queue, spool.owner).get("dependent_allowance") is None


def test_the_allowance_is_derived_only_from_a_sealed_spool_root(tmp_path) -> None:
    """``producer_allowance`` reads the sealed environment and nothing else."""

    cas = core.PrismaBuildCAS(tmp_path / "cas")

    def item(variables):
        return {"action_key": _plain_request(cas, "tests/allowance", variables),
                "cas_root": str(cas.root)}

    root = {adaptive_cpu.SPOOL_ROOT_ENV: "/spool"}
    assert adaptive_cpu.producer_allowance(item({})) is None
    assert adaptive_cpu.producer_allowance(item(root)) == {"slots": 1, "cpu": 1, "mem_gb": 1}
    assert adaptive_cpu.producer_allowance(
        item({**root, adaptive_cpu.EXPORT_SLOTS_ENV: "2"})) == {"slots": 2, "cpu": 2, "mem_gb": 2}
    for opted_out in ("0", "-1", "x", ""):
        assert adaptive_cpu.producer_allowance(
            item({**root, adaptive_cpu.EXPORT_SLOTS_ENV: opted_out})) is None
    assert adaptive_cpu.producer_allowance({"action_key": "0" * 64,
                                            "cas_root": str(cas.root)}) is None

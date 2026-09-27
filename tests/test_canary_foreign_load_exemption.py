"""A verified publication canary crosses foreign CPU load; payload rows do not (#1239).

The 2026-09-27 incident: generation ``25d16e734cc2`` published at 15:60Z and
minted the GPU canary leg on sparky (cpu 1 / gpu 1 / mem 16, priority -10,
promoted to the front of every scan by #1213).  The canary never ran: 1072
passes from 16:00:25Z, its persistent refusal at the first post-publish GPU
boundary was ``adaptive_cpu_refused host_pressure`` -- the operator sessions'
load sat on the box's first free CPUs and no pool holder could drain it --
while a same-band G2 row claimed the boundary 90 ms after the old holder
ended.  The slot pinned the canary to the front of the scan, but nothing
carried it across the foreign-load gate.

Ruling (Claude, 2026-09-27, m10978): the host-pressure gate protects payload
rows' performance from foreign CPU contention.  A canary's verdict is
correctness-only -- receipts, envelopes and bitwise equality; foreign CPU
load cannot corrupt that verdict, only delay it, and delaying it is exactly
the #1239 loss.  So a row that ``publication_canary.verified(...)`` answers
True for is exempt from the foreign-load half of the pressure gate.  Token
accounting is unchanged (the canary still needs its free CPU, GPU and memory
tokens); every other gate stands, including the raw .95 saturation refusal,
the unproven-evidence refusals, and every GPU gate.  An unverified or foreign
row carrying the capability gets no exemption.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import socket
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
from prismabuild import adaptive_cpu, core as pb, pool  # noqa: E402
from prismabuild.publication_canary import (  # noqa: E402
    CAPABILITY, SCHEMA,
)

from test_a_gpu_refused_row_is_not_overtaken_by_gpu_rows import box as gpu_box  # noqa: E402

GENERATION = "82fc269b459f-1790490962-0092ccf606e9"
DEADLINE_S = 32


def _key(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


def _denial(q: pool.PoolQueue, key: str) -> dict:
    path = adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(value for value in records.values() if value["action_key"] == key)


def _slot_path(q: pool.PoolQueue, generation: str, host: str) -> Path:
    identity = hashlib.sha256(f"{generation}\0{host}".encode()).hexdigest()
    return q.root / "publication-canaries" / "v1" / f"{identity}.json"


def _seal(tmp_path: Path, name: str, *, slot=True):
    checkout = tmp_path / name
    checkout.mkdir()
    (checkout / "task.py").write_text("print('private canary fixture')\n")
    host = socket.gethostname()
    params = {"execution_timeout_s": DEADLINE_S, "gpu_exclusive": True}
    if slot:
        params["publication_canary"] = {"generation": GENERATION, "host": host}
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": f"tests/{name}", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": params,
        "environment": {"variables": {"PBCANARY_GENERATION": GENERATION},
                        "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    return action, cas, checkout


def _mint(q, action, *, generation=GENERATION, host=None) -> Path:
    """Stand in for the publisher, not an ordinary producer's authority."""
    host = host or socket.gethostname()
    path = _slot_path(q, generation, host)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as stream:
        json.dump({"schema": SCHEMA, "generation": generation, "host": host,
                   "action_key": action["action_key"],
                   "execution_timeout_s": DEADLINE_S,
                   "run_id": "private-publish-canary", "published_unix": None}, stream)
    return path


def _publish_canary(q, action, cas, checkout):
    return q.publish(
        action_key=action["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script=checkout / "worker.py",
        tags=[socket.gethostname(), CAPABILITY, f"runtime-generation:{GENERATION}"],
        needs_gpu=True, priority=-10, resources={"cpu": 1, "gpu": 1, "mem_gb": 16},
        retry_safe=False, max_attempts=1,
    )


def _foreign_on_every_cpu(clock, *, level=0.43, psi=0.30):
    """The incident's sample: operator load on every CPU, none of it held.

    ``psi_some`` is at .30 so the pressure corroboration path runs, every
    per-CPU reading is attributed foreign (no pool holder exists to own it),
    and the summed busy stays far below the raw .95 saturation gate.
    """
    per_cpu = {str(cpu): level for cpu in range(20)}
    return {
        "sampled_unix": clock[0], "cpu_count": 20, "interval_s": 1.,
        "psi_some": psi, "busy_cpus": 20 * level,
        "per_cpu_busy": dict(per_cpu), "foreign_per_cpu_busy": dict(per_cpu),
    }


def _saturated(clock) -> dict:
    per_cpu = {str(cpu): 1.0 for cpu in range(20)}
    return {
        "sampled_unix": clock[0], "cpu_count": 20, "interval_s": 1.,
        "psi_some": 0.30, "busy_cpus": 20.0,
        "per_cpu_busy": dict(per_cpu), "foreign_per_cpu_busy": dict(per_cpu),
    }


def test_a_verified_canary_is_admitted_through_foreign_load(
        gpu_box, tmp_path, monkeypatch) -> None:
    """The incident, answered: the promoted canary crosses the foreign gate.

    RED (the defect): with operator load on every CPU the canary was refused
    ``host_pressure`` forever -- 1072 passes -- while the boundary went to an
    ordinary row.  GREEN: the verified canary is admitted on its free tokens;
    the exemption is the ruling's scope, not a priority change.
    """
    q, clock, _contracts, _publish, tick, claim = gpu_box
    monkeypatch.setattr(adaptive_cpu.Controller, "sample",
                        lambda self: _foreign_on_every_cpu(clock))
    action, cas, checkout = _seal(tmp_path, "canary")
    _mint(q, action)
    _publish_canary(q, action, cas, checkout)
    assert claim() == action["action_key"]


def test_a_payload_row_in_the_same_state_is_still_refused(
        gpu_box, monkeypatch) -> None:
    """The exemption is the canary's, not the gate's abolition.

    An ordinary 1-CPU row meets the same all-foreign sample and keeps the
    ``host_pressure`` refusal with the foreign split named, exactly as
    before: payload performance is still what the gate protects.
    """
    q, clock, _contracts, publish, _tick, claim = gpu_box
    monkeypatch.setattr(adaptive_cpu.Controller, "sample",
                        lambda self: _foreign_on_every_cpu(clock))
    payload = publish("payload-row", {"cpu": 1, "gpu": 1, "mem_gb": 16})
    assert claim() is None
    denial = _denial(q, payload)
    assert denial["reason"] == "adaptive_cpu_refused", denial
    assert denial["evidence"]["decision"]["reason"] == "host_pressure", denial
    assert denial["evidence"]["decision"]["foreign_cpus"], denial


def test_the_unverified_tag_case_is_refused(
        gpu_box, tmp_path, monkeypatch) -> None:
    """A spent or tampered grant is no longer a verified canary.

    The queue fail-closes the row as ``publication_canary_authority_invalid``
    before any admission decision, so the foreign-load state cannot exempt
    it: only a validated, exact generation earns the exemption.
    """
    q, clock, _contracts, _publish, _tick, claim = gpu_box
    monkeypatch.setattr(adaptive_cpu.Controller, "sample",
                        lambda self: _foreign_on_every_cpu(clock))
    action, cas, checkout = _seal(tmp_path, "spent")
    grant = _mint(q, action)
    _publish_canary(q, action, cas, checkout)
    # Tamper after publish: the grant names another action key.
    grant.write_text(json.dumps({
        **json.loads(grant.read_text()), "action_key": _key("another-action")}))
    assert claim() is None
    denial = _denial(q, action["action_key"])
    assert denial["reason"] == "publication_canary_authority_invalid", denial


def test_raw_saturation_still_refuses_even_the_canary(
        gpu_box, tmp_path, monkeypatch) -> None:
    """Every other gate stands: .95 raw busy is not foreign load.

    The exemption answers foreign load only.  A box whose every CPU is
    saturated refuses the canary exactly as before; there is no token the
    exemption could honestly hand it.
    """
    q, clock, _contracts, _publish, _tick, claim = gpu_box
    monkeypatch.setattr(adaptive_cpu.Controller, "sample",
                        lambda self: _saturated(clock))
    action, cas, checkout = _seal(tmp_path, "saturated")
    _mint(q, action)
    _publish_canary(q, action, cas, checkout)
    assert claim() is None
    denial = _denial(q, action["action_key"])
    assert denial["reason"] == "adaptive_cpu_refused", denial
    assert denial["evidence"]["decision"]["reason"] == "host_pressure", denial

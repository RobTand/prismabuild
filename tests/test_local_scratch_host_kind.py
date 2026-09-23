"""Bounded local scratch is a host resource admitted at claim (#911).

An action bounds its local scratch with sealed ROOT/MAX_BYTES pairs.  It now
names them in ``PRISMABUILD_LOCAL_SCRATCH_PAIRS``, pbrun derives
``ceil(MAX / 2**30)`` per pair into the sealed demand, and the claim charges
the sum to ``spool_gb`` -- the one local-disk kind a box declares with
``--spool-gb`` (#910), which the #747 spool window draws from too.

The tests pin the off path first (nothing declared: the demand is exactly
what it was), then the submit-time derivation and refusals, the freeze
re-check, and admission on ``tmp_path`` queues: a box too small never claims
the action and a box it fits does, and two holders cannot jointly exceed one
box.
"""
from __future__ import annotations

import json
from pathlib import Path
import socket
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import local_scratch as ls, pool  # noqa: E402
from prismabuild import produced_spool as ps  # noqa: E402
import pbrun  # noqa: E402
from test_pbrun_detach import _checkout  # noqa: E402

GIB = 1 << 30
KIND = "spool_gb"
PAIRS = "PRISMABUILD_LOCAL_SCRATCH_PAIRS"
SPILL = ("PQ_SPILL_ROOT", "PQ_SPILL_MAX_BYTES")
SINK = ("PQ_SINK_ROOT", "PQ_SINK_MAX_BYTES")
REPO = Path(__file__).resolve().parents[1]
#: The GLM Stage B spill the issue cites: about 178 GB per layer.
SPILL_BYTES = 178 * 10**9
SPILL_GIB = -(-SPILL_BYTES // GIB)          # 166


def _env(*items: str) -> list[str]:
    return [part for item in items for part in ("--env", item)]


def _declared(spill=SPILL_BYTES, sink=None):
    """``--env`` items declaring the spill, and the sink when given."""

    items = [f"{SPILL[0]}=/home/rob/pq-scratch/spill", f"{SPILL[1]}={spill}"]
    names = [":".join(SPILL)]
    if sink is not None:
        items += [f"{SINK[0]}=/home/rob/pq-scratch/sink", f"{SINK[1]}={sink}"]
        names.append(":".join(SINK))
    return items + [f"{PAIRS}={','.join(names)}"]


def _build(tmp_path, monkeypatch, *extra: str, transport: str = "pool"):
    work = _checkout(tmp_path)
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    args = pbrun.parse_args([
        "--cwd", str(work), "--detach", "--transport", transport, *extra,
        "--", "/bin/bash", "-lc", "true",
    ])
    return pbrun.prepare_submission(args)["template"]


# -- off: nothing declared ----------------------------------------------------


@pytest.mark.parametrize("extra", [
    (),
    # Scratch-shaped variables without a declaration are not inferred.
    (f"{SPILL[0]}=/home/rob/pq-scratch/spill", f"{SPILL[1]}={SPILL_BYTES}"),
    (f"{PAIRS}=",),
])
def test_nothing_declared_seals_the_demand_it_always_did(tmp_path, monkeypatch, extra):
    template = _build(tmp_path, monkeypatch, *_env(*extra))
    assert template["params"]["demand"] == {"cpu": 1, "mem_gb": 4}
    assert KIND not in json.dumps(pbrun.seal_action_from_template(template), default=str)


def test_the_undeclared_path_does_not_import_the_scratch_module(monkeypatch):
    monkeypatch.delitem(sys.modules, "prismabuild.local_scratch", raising=False)
    assert pbrun.scratch_window_terms({"X": "1"}, transport="pool") == {}
    assert "prismabuild.local_scratch" not in sys.modules


# -- on: the derivation -------------------------------------------------------


def test_one_pair_derives_its_ceiling_in_whole_gib(tmp_path, monkeypatch):
    template = _build(tmp_path, monkeypatch, *_env(*_declared()))
    assert template["params"]["demand"] == {"cpu": 1, "mem_gb": 4, KIND: SPILL_GIB}
    sealed = pbrun.seal_action_from_template(template)
    assert sealed["params"]["demand"][KIND] == SPILL_GIB


def test_pairs_round_up_each_and_sum(tmp_path, monkeypatch):
    template = _build(tmp_path, monkeypatch, *_env(*_declared(spill=GIB + 1, sink=1)))
    assert template["params"]["demand"][KIND] == 2 + 1


def test_scratch_and_the_spool_window_share_one_kind(tmp_path, monkeypatch):
    spool = ["PRISMABUILD_PRODUCED_SPOOL_HOST_WINDOW=1",
             f"PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES={32 * GIB}"]
    template = _build(tmp_path, monkeypatch, *_env(*_declared(), *spool))
    assert template["params"]["demand"][KIND] == SPILL_GIB + 32
    # The spool's own check still holds: its claimed row reserves at least
    # its window, since the row reserves the window and the scratch together.
    variables = template["environment"]["variables"]
    assert ps.host_window_terms(variables) == {KIND: 32}


@pytest.mark.parametrize("pairs, env, match", [
    # A declared pair without a positive bound.
    (":".join(SPILL), {SPILL[0]: "/s"}, SPILL[1]),
    (":".join(SPILL), {SPILL[0]: "/s", SPILL[1]: "0"}, "positive"),
    (":".join(SPILL), {SPILL[0]: "/s", SPILL[1]: "-5"}, "positive"),
    (":".join(SPILL), {SPILL[0]: "/s", SPILL[1]: "1e9"}, "positive"),
    (":".join(SPILL), {SPILL[0]: "/s", SPILL[1]: " 5"}, "positive"),
    # A root that is not a canonical absolute path.
    (":".join(SPILL), {SPILL[0]: "rel/s", SPILL[1]: "5"}, "absolute"),
    (":".join(SPILL), {SPILL[0]: "/", SPILL[1]: "5"}, "absolute"),
    (":".join(SPILL), {SPILL[0]: "/a/../b", SPILL[1]: "5"}, "absolute"),
    (":".join(SPILL), {SPILL[1]: "5"}, SPILL[0]),
    # A malformed list.
    ("PQ_SPILL_ROOT", {SPILL[0]: "/s", SPILL[1]: "5"}, "ROOT_ENV:MAX_ENV"),
    ("A:B:C", {"A": "/s", "B": "5"}, "ROOT_ENV:MAX_ENV"),
    ("1X:PQ_SPILL_MAX_BYTES", {"1X": "/s", SPILL[1]: "5"}, "variable name"),
    (f"{':'.join(SPILL)},", {SPILL[0]: "/s", SPILL[1]: "5"}, "ROOT_ENV:MAX_ENV"),
    (f"{':'.join(SPILL)},{':'.join(SPILL)}", {SPILL[0]: "/s", SPILL[1]: "5"}, "twice"),
    ("PQ_A:PQ_B,PQ_C:PQ_D", {"PQ_A": "/s", "PQ_B": "5", "PQ_C": "/s", "PQ_D": "5"},
     "twice"),
    # The spool window's pair is charged by its own switch.
    ("PRISMABUILD_PRODUCED_SPOOL_ROOT:PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES",
     {"PRISMABUILD_PRODUCED_SPOOL_ROOT": "/s",
      "PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES": "5"}, "HOST_WINDOW"),
])
def test_a_pair_that_cannot_be_charged_is_refused_at_submit(
        tmp_path, monkeypatch, pairs, env, match):
    extra = [f"{name}={value}" for name, value in env.items()] + [f"{PAIRS}={pairs}"]
    with pytest.raises(SystemExit, match=match):
        _build(tmp_path, monkeypatch, *_env(*extra))


def test_a_typed_local_disk_demand_is_still_refused(tmp_path, monkeypatch):
    with pytest.raises(SystemExit, match=KIND):
        _build(tmp_path, monkeypatch, *_env(*_declared()), "--demand", f"{KIND}=4")
    assert pbrun._FLEET_DEMAND_KINDS == frozenset({"cpu", "gpu", "mem_gb"})


def test_slurm_refuses_declared_scratch(tmp_path, monkeypatch):
    with pytest.raises(SystemExit, match="pull queue"):
        _build(tmp_path, monkeypatch, *_env(*_declared()), transport="slurm")


# -- freeze re-checks the sealed demand against the environment --------------


def _freeze(tmp_path, *, demand, variables, transport="pool"):
    repo = _checkout(tmp_path)
    return pbrun.freeze_action_template(
        command=["/bin/bash", "-lc", "true"], cwd=repo, logical_cwd=".",
        demand=demand, placement={"required_tags": []},
        variables={"PATH": "/usr/bin:/bin", **variables}, determinism="stochastic",
        retry_policy={"max_attempts": 1, "retry_safe": False},
        host_class=None, measurement=False, transport=transport,
        pool_measurement_class=False, data_manifest_path=None,
        checkout_snapshot_max_bytes=512 * 1024 * 1024, snapshot_refs=[],
        exclusive=False, gpu_memory_gb=None, execution_timeout_s=None,
        progress=None, profile=None, container_image_refs=(),
        wrapper_dir=tmp_path / "wrapper")


DECLARED = {SPILL[0]: "/home/rob/pq-scratch/spill", SPILL[1]: str(GIB + 1),
            PAIRS: ":".join(SPILL)}


@pytest.mark.parametrize("demand, variables", [
    # Scratch declared, but the sealed demand omits or misstates it.
    ({"cpu": 1, "mem_gb": 1}, DECLARED),
    ({"cpu": 1, "mem_gb": 1, KIND: 1}, DECLARED),
    # A local-disk reservation nothing in the environment explains.
    ({"cpu": 1, "mem_gb": 1, KIND: 2}, {SPILL[0]: "/s", SPILL[1]: str(GIB + 1)}),
])
def test_freeze_refuses_a_sealed_demand_that_disagrees_with_the_pairs(
        tmp_path, monkeypatch, demand, variables):
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    with pytest.raises(SystemExit, match=KIND):
        _freeze(tmp_path, demand=demand, variables=variables)


def test_freeze_accepts_the_derived_demand(tmp_path, monkeypatch):
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    frozen = _freeze(tmp_path, demand={"cpu": 1, "mem_gb": 1, KIND: 2},
                     variables=DECLARED)
    assert frozen["params"]["demand"][KIND] == 2
    assert ls.scratch_terms(frozen["environment"]["variables"]) == {KIND: 2}


# -- admission ----------------------------------------------------------------


def _publish(queue, key, need_gib):
    queue.publish(action_key=key, cas_root="/cas",
                  worker_script=str(REPO / "tools" / "prismabuild_worker.py"),
                  checkout_root=str(Path(queue.root).parent / "checkout"),
                  resources={"cpu": 1, "mem_gb": 1, KIND: need_gib})


def _denials(queue):
    local = pool.cpu_admission.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    return {record["action_key"]: record for record in
            pool.cpu_admission.read_json(local)["records"].values()}


def _on(monkeypatch, host):
    monkeypatch.setattr(socket, "gethostname", lambda: host)


def test_scratch_larger_than_a_box_is_never_claimed_there_and_is_claimed_where_it_fits(
        tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    key = "a" * 64
    need = ls.scratch_terms({SPILL[0]: "/s", SPILL[1]: str(SPILL_BYTES),
                             PAIRS: ":".join(SPILL)})[KIND]
    _publish(queue, key, need)

    _on(monkeypatch, "small")                       # a box declaring --spool-gb 32
    assert queue.claim(owner="w-small", capacity={"cpu": 2, "mem_gb": 4, KIND: 32}) is None
    denial = _denials(queue)[key]
    assert denial["reason"] == "never_fits_capacity"
    assert denial["evidence"]["demand"][KIND] == SPILL_GIB
    assert denial["evidence"]["capacity_total"][KIND] == 32

    _on(monkeypatch, "large")                       # a box declaring --spool-gb 200
    claimed = queue.claim(owner="w-large", capacity={"cpu": 2, "mem_gb": 4, KIND: 200})
    assert claimed is not None and claimed["action_key"] == key
    assert claimed["resources"][KIND] == SPILL_GIB
    assert queue.ledger("large").held()[KIND] == SPILL_GIB


def test_two_scratch_holders_cannot_jointly_exceed_one_box(tmp_path, monkeypatch):
    _on(monkeypatch, "box")
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    first, second = "b" * 64, "c" * 64
    _publish(queue, first, SPILL_GIB)
    _publish(queue, second, SPILL_GIB)
    capacity = {"cpu": 4, "mem_gb": 8, KIND: 200}
    claimed = queue.claim(owner="w1", capacity=capacity)
    assert claimed is not None
    held_by = claimed["action_key"]
    waiting = second if held_by == first else first
    assert queue.claim(owner="w2", capacity=capacity) is None
    denial = _denials(queue)[waiting]
    assert denial["reason"].startswith("reservation_unavailable")
    assert denial["evidence"]["token_shortage"]["resource"] == KIND
    assert queue.ledger().held()[KIND] == SPILL_GIB <= capacity[KIND]
    assert queue.item_path(pool.READY, waiting).exists()


def test_two_holders_that_fit_together_are_both_claimed(tmp_path, monkeypatch):
    _on(monkeypatch, "box")
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _publish(queue, "d" * 64, 100)
    _publish(queue, "e" * 64, 100)
    capacity = {"cpu": 4, "mem_gb": 8, KIND: 200}
    assert queue.claim(owner="w1", capacity=capacity) is not None
    assert queue.claim(owner="w2", capacity=capacity) is not None
    assert queue.ledger().held()[KIND] == 200


def test_submission_is_refused_when_every_matching_box_declares_too_little(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    for host in ("sparky", "sparklina"):
        queue.announce(host=host, tags=["gb10", host], has_gpu=False,
                       capacity={"cpu": 2, "mem_gb": 4, KIND: 32})
    intent = {"tags": ["gb10"], "needs_gpu": False,
              "resources": {"cpu": 1, "mem_gb": 1, KIND: SPILL_GIB}}
    assert queue.placeable(intent) is False
    queue.announce(host="sparklina", tags=["gb10", "sparklina"], has_gpu=False,
                   capacity={"cpu": 2, "mem_gb": 4, KIND: 200})
    assert queue.placeable(intent) is True

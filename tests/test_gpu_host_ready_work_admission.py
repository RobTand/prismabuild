"""Portable CPU work cannot overtake a GPU row eligible for this host (#1526)."""

from admitted_queue_fixture import simulated_admission

from admitted_queue_fixture import simulated_capacity
from contextlib import contextmanager
import sys

import pytest

from prismabuild import capability_eligibility
from prismabuild import contracts
from prismabuild import core as pb, pool


CAPACITY = {"cpu": 8, "gpu": 1, "mem_gb": 32}
TIERS = {"preferred": list(range(8)), "fallback": []}
CPU_KEY = "c" * 64
GPU_KEY = "a" * 64


@pytest.fixture(params=["sparky", "sparklina"])
def fleet(tmp_path, monkeypatch, request):
    host = request.param
    monkeypatch.setattr(pool.socket, "gethostname", lambda: host)
    queue = pool.PoolQueue(tmp_path / "queue")
    worker_tags = [host, "gb10", contracts.INTERPRETER_TAG, contracts.CONTAINER_IMAGE_TAG]
    queue.announce(
        host=host,
        tags=worker_tags,
        has_gpu=True,
        capacity=CAPACITY,
        observed_capacity=CAPACITY,
        capabilities=capability_eligibility.OfferCapabilities(
            interpreters=capability_eligibility.InterpreterAnswers.from_observed(
                [sys.executable],
                None,
            ),
            images=capability_eligibility.ImageAnswers.from_observed(
                [],
                None,
            ),
        ),
    )

    def publish(key, *, gpu=False, tags=None, resources=None, priority=0, **fields):
        queue.publish(
            action_key=key, cas_root=str(tmp_path / "cas"),
            checkout_root=str(tmp_path), worker_script="worker.py",
            tags=tags if tags is not None else (["gb10"] if gpu else ["interpreter-path-v1"]),
            needs_gpu=gpu, priority=priority,
            resources=resources or ({"cpu": 2, "gpu": 1, "mem_gb": 8} if gpu
                                    else {"cpu": 4, "mem_gb": 8}),
            interpreter=fields.pop("interpreter", sys.executable if not gpu else None),
            **fields,
        )

    def claim(*, has_gpu=True, tags=None):
        return queue.claim(
            tags=tags or worker_tags,
            has_gpu=has_gpu, **simulated_admission(queue, simulated_capacity(CAPACITY)), cpu_tiers=TIERS,
        )

    return queue, publish, claim, host


@pytest.mark.parametrize("cpu_priority", [0, 1])
@pytest.mark.parametrize("gpu_interpreter", [False, True], ids=["bare-gpu", "named-interpreter"])
def test_eligible_ready_gpu_precedes_portable_cpu(fleet, cpu_priority, gpu_interpreter):
    queue, publish, claim, _host = fleet
    publish(CPU_KEY, priority=cpu_priority)
    publish(GPU_KEY, gpu=True, priority=0,
            interpreter=sys.executable if gpu_interpreter else None)
    claimed = claim()
    assert claimed and claimed["action_key"] == GPU_KEY
    queue.finish(GPU_KEY, status="executed")
    claimed = claim()
    assert claimed and claimed["action_key"] == CPU_KEY


def test_ready_gpu_keeps_portable_cpu_back_without_taking_its_lock(fleet, monkeypatch):
    queue, publish, claim, _host = fleet
    publish(CPU_KEY, priority=1)
    publish(GPU_KEY, gpu=True)
    transition_hold = queue._timed_transition_hold
    acquired_keys = []
    reason_transitions = []
    record_transition = queue._record_denial_transition

    def transition(item, **kwargs):
        reason_transitions.append(item["action_key"])
        return record_transition(item, **kwargs)

    @contextmanager
    def contested(key, holds):
        acquired_keys.append(key)
        if key == GPU_KEY:
            yield False
        else:
            with transition_hold(key, holds) as acquired:
                yield acquired

    monkeypatch.setattr(queue, "_timed_transition_hold", contested)
    monkeypatch.setattr(queue, "_record_denial_transition", transition)
    assert claim() is None
    assert acquired_keys == [GPU_KEY]
    assert reason_transitions == []  # no unlocked reason-ring writer either
    assert not queue.records.passes_path(CPU_KEY).exists()
    assert not queue.ledger().held_keys()


@pytest.mark.parametrize("pin", ["explicit-tag", "here"])
def test_spark_pinned_cpu_is_unaffected(fleet, pin):
    queue, publish, claim, host = fleet
    # --here seals the same hostname tag as an explicit --tag HOST; --tag
    # gb10 may be combined with --here without changing the host pin.
    tags = [host] if pin == "explicit-tag" else [host, "gb10"]
    publish(CPU_KEY, tags=tags, priority=1)
    publish(GPU_KEY, gpu=True)
    claimed = claim()
    assert claimed and claimed["action_key"] == CPU_KEY
    assert queue.records.item_path(pool.record_store.READY, GPU_KEY).exists()


def test_class_scoped_cpu_prerequisite_fits_beside_ready_gpu(fleet, tmp_path):
    queue, publish, claim, _host = fleet
    queue.announce(
        host="dl380g10", tags=["x86", contracts.INTERPRETER_TAG],
        has_gpu=False, capacity={"cpu": 8, "mem_gb": 32},
        observed_capacity={"cpu": 8, "mem_gb": 32},
    )
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    (tmp_path / "task.py").write_text("print('fixture')\n")

    def sealed_key(name):
        action = pb.seal_action({
            "schema": pb.ACTION_SCHEMA_V2,
            "task": {"definition_id": f"tests/{name}", "definition_version": "v1",
                     "task_class": "generation", "determinism": "deterministic",
                     "artifact_family": "generic", "artifact_kind": "generic",
                     "argv": ["/bin/true"], "working_directory": ".",
                     "result_path": "result"},
            "inputs": [], "code_closure": pb.build_code_closure(tmp_path, ["task.py"]),
            "params": {}, "environment": {"variables": {}, "toolchain": {}},
            "execution_scope": {"portability": "portable", "platform_key": None,
                                "host_class": None},
        })
        cas.publish_action_request(action)
        return action["action_key"]

    cpu_key, gpu_key = sealed_key("class-prerequisite"), sealed_key("waiting-gpu")
    publish(cpu_key, tags=["gb10", contracts.INTERPRETER_TAG], priority=1)
    publish(gpu_key, gpu=True)

    claimed = claim()

    assert claimed and claimed["action_key"] == cpu_key
    assert queue.records.item_path(pool.record_store.READY, gpu_key).exists()


@pytest.mark.parametrize("ineligible", [
    "wrong-tag", "wrong-class", "cpu", "memory", "gpu", "interpreter", "image",
])
def test_ineligible_ready_gpu_does_not_starve_cpu(fleet, ineligible, tmp_path):
    queue, publish, claim, _host = fleet
    kwargs = {}
    if ineligible == "wrong-tag":
        kwargs["tags"] = ["other-host"]
    elif ineligible == "wrong-class":
        kwargs["tags"] = ["x86"]
    elif ineligible == "interpreter":
        kwargs["interpreter"] = str(tmp_path / "missing-python")
    elif ineligible == "image":
        kwargs["container_images"] = ["sha256:" + "d" * 64]
    else:
        kind = {"memory": "mem_gb"}.get(ineligible, ineligible)
        kwargs["resources"] = {"cpu": 2, "gpu": 1, "mem_gb": 8,
                               kind: CAPACITY[kind] + 1}
    publish(GPU_KEY, gpu=True, **kwargs)
    # Leave the ineligible GPU row READY across repeated CPU claims: neither
    # an aging counter nor a timer is needed to let CPU work make progress.
    for digit in "cdef":
        key = digit * 64
        publish(key, priority=1)
        claimed = claim()
        assert claimed and claimed["action_key"] == key
        queue.finish(key, status="executed")
        assert queue.records.item_path(pool.record_store.READY, GPU_KEY).exists()


def test_non_gpu_host_is_unaffected(fleet, monkeypatch):
    _queue, publish, claim, _host = fleet
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "dl380g10")
    publish(CPU_KEY, priority=1)
    publish(GPU_KEY, gpu=True)
    claimed = claim(has_gpu=False, tags=["dl380g10", "x86", "interpreter-path-v1"])
    assert claimed and claimed["action_key"] == CPU_KEY

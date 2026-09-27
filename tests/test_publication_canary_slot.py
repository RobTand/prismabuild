"""#1213: a publisher-owned, single-use GPU boundary slot, never self-labeling.

All CAS, queue, grants and fake device observations are private tmp_path data.
No worker, live store, container or device is contacted by these fixtures.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import socket
import sys

import pytest

from prismabuild import core as pb, pool
from test_a_gpu_refused_row_is_not_overtaken_by_gpu_rows import box as gpu_box

GENERATION = "82fc269b459f-1790490962-0092ccf606e9"
CAPABILITY = "publication-canary-slot-v1"
DEADLINE_S = 32
SCHEMA = "prismabuild.publication_canary_slot.v1"


def _slot_path(q: pool.PoolQueue, generation: str, host: str) -> Path:
    identity = hashlib.sha256(f"{generation}\0{host}".encode()).hexdigest()
    return q.root / "publication-canaries" / "v1" / f"{identity}.json"


def _seal(tmp_path: Path, name: str, *, slot=True, deadline=DEADLINE_S):
    checkout = tmp_path / name
    checkout.mkdir()
    (checkout / "task.py").write_text("print('private canary fixture')\n")
    host = socket.gethostname()
    params = {"execution_timeout_s": deadline, "gpu_exclusive": True}
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


def _mint_fixture(q, action, *, generation=GENERATION, host=None):
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


def _publish(q, action, cas, checkout, **kw):
    return q.publish(
        action_key=action["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script=checkout / "worker.py",
        tags=[socket.gethostname(), CAPABILITY, f"runtime-generation:{GENERATION}"],
        needs_gpu=True, priority=-10, resources={"cpu": 1, "gpu": 1, "mem_gb": 16},
        retry_safe=False, max_attempts=1, **kw,
    )


@pytest.fixture
def q(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    return queue


def test_an_ordinary_action_cannot_claim_a_slot_by_sealing_its_own_label(q, tmp_path):
    action, cas, checkout = _seal(tmp_path, "forged")
    with pytest.raises(pool.PoolContractError, match="canary.*(grant|mint|authority)"):
        _publish(q, action, cas, checkout)
    assert not q.item_path(pool.READY, action["action_key"]).exists()


def test_changed_payload_cannot_borrow_a_publisher_grant(q, tmp_path):
    original, _, _ = _seal(tmp_path, "original")
    _mint_fixture(q, original)
    impostor, cas, checkout = _seal(tmp_path, "changed-payload")
    with pytest.raises(pool.PoolContractError, match="canary.*(key|action|bound)"):
        _publish(q, impostor, cas, checkout)
    assert not q.item_path(pool.READY, impostor["action_key"]).exists()


@pytest.mark.parametrize("ordinary_priority", [-10, 0, 10, 1000000])
def test_minted_canary_precedes_every_ordinary_priority_band(q, tmp_path, ordinary_priority):
    ordinary = hashlib.sha256(b"older ordinary row").hexdigest()
    q.publish(action_key=ordinary, cas_root=tmp_path / "ordinary-cas",
              checkout_root=tmp_path, worker_script=tmp_path / "worker.py",
              priority=ordinary_priority, resources={"gpu": 1}, needs_gpu=True)
    action, cas, checkout = _seal(tmp_path, "minted")
    _mint_fixture(q, action)
    _publish(q, action, cas, checkout)
    assert q.ready_items()[0]["action_key"] == action["action_key"]


@pytest.mark.parametrize("deadline", [None, 0, 33, 600])
def test_grant_cannot_be_used_without_its_short_hard_deadline(q, tmp_path, deadline):
    action, cas, checkout = _seal(tmp_path, "deadline", deadline=deadline)
    _mint_fixture(q, action)
    with pytest.raises(pool.PoolContractError, match="canary.*(deadline|timeout)"):
        _publish(q, action, cas, checkout)


def test_same_action_cannot_spend_the_slot_again_after_terminalization(q, tmp_path):
    action, cas, checkout = _seal(tmp_path, "once")
    grant = _mint_fixture(q, action)
    _publish(q, action, cas, checkout)
    key = action["action_key"]
    queued = json.loads(q.item_path(pool.READY, key).read_text())
    # A private terminal fixture; no process, signal or actual GPU execution.
    q.item_path(pool.READY, key).rename(q.item_path(pool.DONE, key))
    terminal = q.item_path(pool.DONE, key).read_bytes()
    with pytest.raises(pool.PoolContractError, match="canary.*(spent|used|published)"):
        _publish(q, action, cas, checkout)
    assert q.item_path(pool.DONE, key).read_bytes() == terminal
    assert json.loads(grant.read_text())["published_unix"] == queued["published_unix"]


def test_second_distinct_canary_for_same_runtime_and_host_is_refused(q, tmp_path):
    first, cas, checkout = _seal(tmp_path, "first")
    _mint_fixture(q, first)
    _publish(q, first, cas, checkout)
    second, cas, checkout = _seal(tmp_path, "second")
    with pytest.raises(pool.PoolContractError, match="canary.*(key|action|bound|spent)"):
        _publish(q, second, cas, checkout)
    assert not q.item_path(pool.READY, second["action_key"]).exists()


def test_canary_waits_for_existing_gpu_holder_then_wins_next_boundary(gpu_box, tmp_path):
    q, clock, contracts, publish, tick, _claim = gpu_box
    holder = publish("running-quantum", {"cpu": 2, "gpu": 1, "mem_gb": 16},
                     priority=0, retry_safe=False, max_attempts=1)
    assert _claim() == holder
    action, cas, checkout = _seal(tmp_path, "next-boundary")
    key = action["action_key"]
    contracts[key] = ("shape", True)
    _mint_fixture(q, action)
    _publish(q, action, cas, checkout)
    tags = [socket.gethostname(), CAPABILITY, f"runtime-generation:{GENERATION}"]

    def claim():
        return q.claim(tags=tags, has_gpu=True, adaptive_cpu=True,
                       capacity={"cpu": 20, "gpu": 1, "mem_gb": 120},
                       cpu_tiers={"preferred": list(range(20)), "fallback": []})

    tick()
    assert claim() is None
    assert q.withdrawal_decisions(holder) == []
    assert q.item_path(pool.CLAIMED, holder).exists()
    ordinary = publish("foreground-after-holder", {"cpu": 2, "gpu": 1, "mem_gb": 16},
                       priority=10)
    q.finish(holder, status="executed", detail={})
    tick()
    taken = claim()
    assert taken is not None and taken["action_key"] == key
    assert q.item_path(pool.READY, ordinary).exists()


@pytest.mark.parametrize("foreign_axis", ["runtime", "host"])
def test_unplaceable_slot_cannot_occupy_claimants_bounded_prefix(q, tmp_path, foreign_axis):
    ordinary = hashlib.sha256(b"placeable ordinary row").hexdigest()
    q.publish(action_key=ordinary, cas_root=tmp_path / "ordinary-cas",
              checkout_root=tmp_path, worker_script=tmp_path / "worker.py",
              priority=0, resources={"gpu": 1}, needs_gpu=True)
    action, cas, checkout = _seal(tmp_path, "old-or-other-host-slot")
    _mint_fixture(q, action)
    _publish(q, action, cas, checkout)
    host = "different-host" if foreign_axis == "host" else socket.gethostname()
    generation = "82fc269b459f-1790490963-0092ccf606e9" if foreign_axis == "runtime" else GENERATION
    with pool.ready_placement((host, CAPABILITY, f"runtime-generation:{generation}"), True):
        prefix = q.ready_items()[:1]
    assert prefix[0]["action_key"] == ordinary


def test_foreign_capability_only_row_is_denied_without_aborting_scan(gpu_box, tmp_path):
    q, clock, contracts, publish, tick, _claim = gpu_box
    action, cas, checkout = _seal(tmp_path, "foreign-row")
    key = action["action_key"]
    contracts[key] = ("shape", True)
    _mint_fixture(q, action)
    _publish(q, action, cas, checkout)
    path = q.item_path(pool.READY, key)
    foreign = json.loads(path.read_text())
    del foreign["publication_canary"]
    path.write_text(json.dumps(foreign))
    ordinary = publish("ordinary-after-foreign-row", {"cpu": 2, "gpu": 1, "mem_gb": 16},
                       priority=-20)
    tick()
    taken = q.claim(tags=[socket.gethostname(), CAPABILITY, f"runtime-generation:{GENERATION}"],
                    has_gpu=True, adaptive_cpu=True,
                    capacity={"cpu": 20, "gpu": 1, "mem_gb": 120},
                    cpu_tiers={"preferred": list(range(20)), "fallback": []})
    assert taken is not None and taken["action_key"] == ordinary
    assert q.latest_denials({key})[key][0]["reason"] == "publication_canary_authority_invalid"
    assert path.exists()

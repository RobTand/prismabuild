"""#1403: a drained owner retries its own saved finish under the closed gate.

On 2026-10-01 a coordinated window drain landed on a withdrawn claim
(``c7cdbaa9...``) whose ``finish()`` had saved ``finish_pending`` and failed
its container cleanup once -- the same transient docker shape this fixture
models through the broker boundary.  ``reap_stale`` is the only retry path
for ``finish_pending``/``container_cleanup_pending`` and its only caller is
``serve_once`` (``pool.py:24813`` in the deployed generation).  Once a box is
drained, ``worker_loop``'s maintenance branch never reaches ``serve_once``,
so the owner never retried the saved outcome: the claim kept its scope and
its tokens, the drain waited for the scope to empty, and the scope waited for
the drain to end.

These tests drive the real ``worker_loop._run_loop`` drain branch against a
``tmp_path`` queue and a private broker boundary that refuses cleanup once.
They pin the two halves of the fix the issue asks for -- the owner-host retry
concludes the saved finish in one drain poll, and a foreign host's saved
finish is left exactly as it is -- with no source-string or call-order
assertions and no admission reopening.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import socket
import sys
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
WORKER_LOOP = REPO / "tools" / "fleet" / "worker_loop.py"

sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "tools" / "fleet"))
from prismabuild import pool, resource_scope  # noqa: E402

HOST = socket.gethostname()
DRAIN_GATE = {"draining": True, "owner": "u4-R2-20261001T0334Z",
              "reason": "coordinated window drain", "changed_unix": 1.0}
OWNER_KEY = "1" * 64
FOREIGN_KEY = "2" * 64
DECOY_KEY = "3" * 64
RESOURCES = {"cpu": 1, "mem_gb": 2}
#: The offer the owner claim is taken against: two claims fit, so the second
#: does not depend on the first's release.
HOST_CAPACITY = {"cpu": 2, "mem_gb": 4}


def _scope_control(key: str, nonce: str) -> dict:
    """A valid durable scope identity, exactly as ``_scope_from_record`` derives it."""

    unit = ("prismabuild-job"
            + hashlib.sha256((key + nonce).encode()).hexdigest()[:32] + ".slice")
    return {
        "action_key": key,
        "nonce": nonce,
        "scope_id": unit,
        "cgroup_path": "/sys/fs/cgroup/prismabuild.slice/" + unit,
        "socket_path": str(resource_scope.BROKER_SOCKET),
        "token": "d" * 64,
        "memory_max_bytes": RESOURCES["mem_gb"] * 1024 ** 3,
        "started_monotonic": time.monotonic(),
        "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
    }


def _fake_broker(monkeypatch):
    """A private broker boundary that cannot yet prove the exact scope empty.

    The first cleanup call fails with the docker in-progress shape from the
    issue, which the pool records as ``container_cleanup_pending`` and never
    concludes.  Healing it models the container actually going away: from
    there the next sweep must prove cleanup instead of recording the failure
    forever.
    """

    state = {"failing": True, "calls": []}

    def request(scope, op, **extra):
        state["calls"].append((scope.action_key, op))
        if state["failing"]:
            raise OSError(
                "docker cleanup failed (1): Error response from daemon: "
                "removal of container fae18385af61 is already in progress")
        return {"ok": True}

    monkeypatch.setattr(resource_scope.ResourceScope, "_request", request)
    return state


@pytest.fixture
def drained_box(tmp_path, monkeypatch):
    """A drained box's own queue: one saved finish it owns, one a foreign box owns.

    The owner's ``finish_pending`` is produced by the real ``withdraw`` and
    ``finish`` paths, not hand-written: the claim is withdrawn, then the
    payload's finish is attempted while the broker refuses cleanup, which is
    exactly the state the incident left on sparky.  The foreign row is the
    same saved shape under a foreign claim identity, so an owner-filter that
    is missing or too wide is visible as a changed record.
    """

    broker = _fake_broker(monkeypatch)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()

    queue.publish(action_key=OWNER_KEY, cas_root=tmp_path / "cas",
                  checkout_root=tmp_path / "checkout",
                  worker_script=str(tmp_path / "worker.py"),
                  resources=RESOURCES, max_attempts=1)
    owner = queue.claim(capacity=HOST_CAPACITY)
    assert owner is not None and owner["action_key"] == OWNER_KEY
    assert queue.ledger().held() == RESOURCES

    # Claimable by this box on purpose: if a retry implementation reopened
    # admission, this row is what it would take, and ``execute`` below fails
    # the test loudly rather than pretending the poll was harmless.
    queue.publish(action_key=DECOY_KEY, cas_root=tmp_path / "cas",
                  checkout_root=tmp_path / "checkout",
                  worker_script=str(tmp_path / "worker.py"),
                  resources=RESOURCES, max_attempts=1)

    record = pool._read_json(queue.item_path(pool.CLAIMED, OWNER_KEY))
    record["resource_scope"] = _scope_control(OWNER_KEY, "c" * 32)
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, OWNER_KEY), record)

    queue.withdraw(OWNER_KEY, reason="drained for a U4 window",
                   by="u4-R2-20261001T0334Z", signal_child=False)
    record = pool._read_json(queue.item_path(pool.CLAIMED, OWNER_KEY))
    path = queue.finish(OWNER_KEY, status="withdrawn",
                        detail={"returncode": -15,
                                "termination_reason": "withdrawn"},
                        claim_snapshot=record)
    pending = json.loads(path.read_text())
    assert pending["finish_pending"]["status"] == "withdrawn"
    assert pending["container_cleanup_pending"]["complete"] is False
    assert pending["container_cleanup_attempts"] == 1

    foreign = json.loads(path.read_text())
    foreign["action_key"] = FOREIGN_KEY
    foreign["claimed_by"] = "foreign-box:1:feedface"
    foreign["claimed_host"] = "foreign-box"
    foreign["resource_scope"] = _scope_control(FOREIGN_KEY, "e" * 32)
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, FOREIGN_KEY), foreign)
    pool._write_json_atomic(queue.lease_path(FOREIGN_KEY), {
        "schema": pool.POOL_LEASE_SCHEMA_V1,
        "action_key": FOREIGN_KEY,
        "owner": "foreign-box:1:feedface",
        "host": "foreign-box",
        "pid": 424242,
        "child_pid": None,
        "heartbeat_unix": pool._now(),
    })

    # The container has gone: the next cleanup attempt can prove it empty.
    broker["failing"] = False
    return queue, broker


def _load_loop(tmp_path: Path, monkeypatch, gate_value) -> object:
    """The real loop with its gate, queue root and writer lock made private.

    Follows the #1204 drain-visibility fixture: the gate path is read at
    import, the publication lock is host-local per uid, and neither a live
    loop nor a sibling pytest worker may be contended with for the real one.
    """

    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps(gate_value))
    monkeypatch.setenv("PRISMABUILD_MAINTENANCE_GATE", str(gate))
    spec = importlib.util.spec_from_file_location("drain_pending_loop_1403",
                                                  WORKER_LOOP)
    loop = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loop)
    monkeypatch.setattr(loop, "SH", tmp_path)
    monkeypatch.setattr(loop, "MAINTENANCE_GATE", gate)
    monkeypatch.setattr(loop, "PARKED_ROOT", tmp_path / "rollout" / "parked")
    monkeypatch.setattr(loop, "PUBLICATION_LOCK_ROOT",
                        tmp_path / "offer-publish")
    monkeypatch.setattr(loop, "loaded_runtime_commit", lambda: "test")
    monkeypatch.setattr(loop, "published_commit", lambda: "test")
    monkeypatch.setattr(loop, "_generation_at", lambda path: "test")
    monkeypatch.setattr(loop, "generation_drift",
                        lambda loaded_commit=None, loaded_generation=None: None)
    monkeypatch.setattr(loop.cpu_topology, "inherited_tiers", lambda: None)
    monkeypatch.setattr(loop.container_images.InventoryCache, "get",
                        lambda self, *a, **k: None)
    monkeypatch.setattr(loop.box_capacity, "ipv4_addresses", lambda: None)
    monkeypatch.setattr(sys, "argv", [
        "worker_loop.py", "--all-cores", "--cpu-slots", "1", "--mem-gb", "2",
        "--class", "x86", "--assume-idle", "--poll-s", "0", "--once"])
    return loop


def _forbid_execute(monkeypatch) -> None:
    def forbidden(*args, **kwargs):
        pytest.fail("the drain reopened admission and claimed ready work")
    monkeypatch.setattr(pool.PoolQueue, "execute", forbidden)


def _drain_one_poll(tmp_path, monkeypatch) -> int:
    return _load_loop(tmp_path, monkeypatch, DRAIN_GATE)._run_loop(
        lambda: False)


def test_a_drained_box_concludes_its_own_saved_finish(
        drained_box, tmp_path, monkeypatch):
    """One drain poll must retry the owner's saved finish and conclude it.

    The gate must stay closed (the offer still reads draining with zero live
    capacity), the claimable ready row must stay untouched, and the claim --
    with its lease and its tokens -- must be concluded to ``withdrawn/``.
    """

    queue, broker = drained_box
    _forbid_execute(monkeypatch)
    before = {pair: broker["calls"].count(pair)
              for pair in ((OWNER_KEY, "stop"), (OWNER_KEY, "release"))}

    assert _drain_one_poll(tmp_path, monkeypatch) == 0

    assert not queue.item_path(pool.CLAIMED, OWNER_KEY).exists(), (
        "the drained box never retried its own saved finish: the withdrawn "
        "claim is still held in claimed/ with its saved outcome")
    assert not queue.lease_path(OWNER_KEY).exists()
    assert queue.ledger().held() == {}
    assert queue.item_path(pool.WITHDRAWN, OWNER_KEY).exists()

    offer = json.loads((queue.root / pool.WORKERS / f"{HOST}.json").read_text())
    assert offer["state"] == "draining"
    assert all(value == 0 for value in offer["observed_capacity"].values())

    decoy = json.loads(queue.item_path(pool.READY, DECOY_KEY).read_text())
    assert decoy["attempts"] == 0

    owner_stops = broker["calls"].count((OWNER_KEY, "stop"))
    owner_releases = broker["calls"].count((OWNER_KEY, "release"))
    assert owner_stops == before[(OWNER_KEY, "stop")] + 1, (
        "the saved finish's failed cleanup was retried exactly once")
    assert owner_releases == before[(OWNER_KEY, "release")] + 1


def test_a_drained_box_leaves_a_foreign_hosts_saved_finish_alone(
        drained_box, tmp_path, monkeypatch):
    """Only the owner concludes a saved finish; a foreign row is untouched.

    A box that retried another box's ``finish_pending`` would run a foreign
    container cleanup and release tokens against the wrong ledger.  The
    record and its lease must be byte-identical after the drain poll, and no
    broker call may name the foreign attempt.
    """

    queue, broker = drained_box
    _forbid_execute(monkeypatch)
    record_before = queue.item_path(pool.CLAIMED, FOREIGN_KEY).read_bytes()
    lease_before = queue.lease_path(FOREIGN_KEY).read_bytes()

    assert _drain_one_poll(tmp_path, monkeypatch) == 0

    assert queue.item_path(pool.CLAIMED, FOREIGN_KEY).read_bytes() == record_before
    assert queue.lease_path(FOREIGN_KEY).read_bytes() == lease_before
    assert not [op for key, op in broker["calls"] if key == FOREIGN_KEY]

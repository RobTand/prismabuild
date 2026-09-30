"""A parked loop keeps announcing, so a drain reads as a drain (#1204).

The drain branch slept without calling ``queue.announce``, so after one offer
TTL a deliberately drained box read exactly like a crashed worker, an NFS
stall or a client clock fault: same ``stale`` state, same reason, while the
gate's owner and reason lived only in a root-owned file no reader reads.  The
loop now republishes a draining offer on every drain poll -- fresh, named for
the holder, and carrying zero admittable capacity -- so staleness again means
the loop stopped.

These tests drive the real ``worker_loop._run_loop`` against a ``tmp_path``
queue and a private gate, and read the result back through ``pbstatus``' own
census.  Nothing touches ``/run``, a live queue or a real broker; the reopened
poll only ever serves an already-empty queue.
"""

from __future__ import annotations

import importlib.util
import json
import socket
from pathlib import Path
import sys
import time

import pytest

REPO = Path(__file__).resolve().parents[1]
WORKER_LOOP = REPO / "tools" / "fleet" / "worker_loop.py"

sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "tools" / "fleet"))
from prismabuild import pool  # noqa: E402
import pbstatus  # noqa: E402

HOST = socket.gethostname()
DRAIN_GATE = {"draining": True, "owner": "x", "reason": "y",
              "changed_unix": 123.0}


def _load_loop(tmp_path: Path, monkeypatch, gate_value,
               extra_args: tuple = (), once: bool = True) -> object:
    """The real loop with its gate, queue root and writer lock made private.

    The gate path is read at import, so it is set before the module executes.
    The publication lock is redirected too: it is host-local per uid, and a
    test must not contend with a live loop -- or a sibling pytest worker --
    for the real one.
    """

    gate = tmp_path / "maintenance.json"
    if gate_value is not None:
        gate.write_text(gate_value if isinstance(gate_value, str)
                        else json.dumps(gate_value))
    monkeypatch.setenv("PRISMABUILD_MAINTENANCE_GATE", str(gate))
    spec = importlib.util.spec_from_file_location("drain_visibility_loop",
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
    # No Docker probe and no `ip` invocation on the offer path: both are
    # capability fields this test does not assert, and neither should make a
    # unit test depend on the host's tooling.
    monkeypatch.setattr(loop.container_images.InventoryCache, "get",
                        lambda self, *a, **k: None)
    monkeypatch.setattr(loop.box_capacity, "ipv4_addresses", lambda: None)
    argv = ["worker_loop.py", "--all-cores", "--cpu-slots", "1", "--mem-gb", "2",
            "--class", "x86", "--assume-idle", "--poll-s", "0", *extra_args]
    if once:
        argv.append("--once")
    monkeypatch.setattr(sys, "argv", argv)
    return loop


def _offer_path(queue: pool.PoolQueue) -> Path:
    return queue.root / pool.WORKERS / f"{HOST}.json"


def test_a_draining_loop_keeps_a_fresh_named_offer_and_reopens(tmp_path, monkeypatch):
    """One drain poll leaves a fresh named offer; opening the gate reopens it.

    This is the issue's acceptance test: the gate carries an owner and a
    reason, one poll must leave ``state: draining`` with that owner, and
    ``pbstatus`` must report the node as draining rather than stale.  With an
    explicitly open gate the next poll announces live capacity again -- the
    same record, rewritten.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    assert _load_loop(tmp_path, monkeypatch, DRAIN_GATE)._run_loop(lambda: False) == 0

    assert list(queue.dir(pool.READY).glob("*.json")) == []
    assert list(queue.dir(pool.CLAIMED).glob("*.json")) == []
    offer = json.loads(_offer_path(queue).read_text())
    assert offer["state"] == "draining"
    assert offer["drain_owner"] == "x"
    assert offer["drain_reason"] == "y"
    assert offer["drain_changed_unix"] == 123.0
    # Identity and capability are preserved...
    assert offer["host"] == HOST and HOST in offer["tags"] and "x86" in offer["tags"]
    assert offer["has_gpu"] is False
    assert offer["capacity"] == {"gpu": 0, "cpu": 1, "mem_gb": 2, "disk_metadata": 0}
    # ...and the live figure offers nothing: zero on every declared kind.
    assert set(offer["observed_capacity"]) == set(offer["capacity"])
    assert all(value == 0 for value in offer["observed_capacity"].values())
    timing = pool.offer_timing(offer["announced_unix"], now=time.time())
    assert timing.age_s is not None and timing.age_s <= pool.OFFER_TIMEOUT_S
    assert [entry["host"] for entry in queue.offers()] == [HOST]

    # The declared capability survives, so a submission that waits on the
    # drain stays queueable instead of being refused for want of a worker.
    assert queue.placeable({"resources": {"cpu": 1}, "tags": ["x86"]}) is True

    census = pbstatus.read_pool(queue.root)
    node = next(n for n in census["nodes"] if n["node"] == HOST)
    assert node["state"] == "draining" and node["healthy"] is True
    assert node["draining"] is True
    assert node["drain_owner"] == "x"
    assert node["drain_reason"] == "y"
    assert node["drain_changed_unix"] == 123.0
    # The census reports the offer's live figure as written: zero everywhere,
    # never the declared capability beside it.
    assert node["observed_capacity"] == {"gpu": 0, "cpu": 0, "mem_gb": 0, "disk_metadata": 0}
    assert node["capacity"] == {"gpu": 0, "cpu": 1, "mem_gb": 2, "disk_metadata": 0}
    assert "draining for maintenance" in node["reason"]
    assert "owner x" in node["reason"] and "y" in node["reason"]
    table = "\n".join(pbstatus.pool_node_lines(census["nodes"]))
    assert "draining" in table and "owner x" in table and "y" in table

    # Reopening is an explicit open gate, not a removed file: a missing gate
    # keeps meaning "admission was never initialized" and stays a drain, which
    # is the existing safety polarity this change must not touch.
    (tmp_path / "maintenance.json").write_text(
        json.dumps({"draining": False, "changed_unix": 124.0}))
    assert _load_loop(tmp_path, monkeypatch, None)._run_loop(lambda: False) == 0

    reopened = json.loads(_offer_path(queue).read_text())
    assert reopened.get("state") in (None, "live")
    assert "drain_owner" not in reopened and "drain_reason" not in reopened
    assert reopened["observed_capacity"] == {
        "gpu": 0, "cpu": 1, "mem_gb": 2, "disk_metadata": 0}
    node = next(n for n in pbstatus.read_pool(queue.root)["nodes"]
                if n["node"] == HOST)
    assert node["state"] == "live" and node["healthy"] is True
    assert node["draining"] is False and node["drain_owner"] is None


@pytest.mark.parametrize("contents,reason", [
    (None, "maintenance gate not initialized"),
    ("{not json", None),
    ('{"draining": true, "owner": "x"}', None),
    ('{"draining": "yes", "changed_unix": 5}', None),
])
def test_missing_or_malformed_gates_still_announce_draining(tmp_path, monkeypatch,
                                                             contents, reason):
    """The safety polarity is unchanged: no readable open gate means draining.

    A missing gate (admission not initialized) and an unparsable or malformed
    one are all still a drain, exactly as ``read_maintenance_gate`` reads
    them; the offer says so without inventing an owner, reason or change time
    the gate does not carry.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    loop = _load_loop(tmp_path, monkeypatch, contents)
    assert loop._run_loop(lambda: False) == 0

    offer = json.loads(_offer_path(queue).read_text())
    assert offer["state"] == "draining"
    assert all(value == 0 for value in offer["observed_capacity"].values())
    assert offer.get("drain_reason") == reason
    node = next(n for n in pbstatus.read_pool(queue.root)["nodes"]
                if n["node"] == HOST)
    assert node["state"] == "draining" and node["healthy"] is True


def test_a_stale_draining_offer_reads_as_a_stopped_loop(tmp_path):
    """The point of the republish: an expired draining offer is not a drain.

    A loop that dies (or is stopped) mid-drain stops refreshing the record,
    and the reader must see that as the loop having stopped -- not as a live
    drain.  Only freshness makes the drain evidence.
    """

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.announce(host="parkedbox", tags=["x86"], has_gpu=False,
                   capacity={"cpu": 4}, observed_capacity={"cpu": 0},
                   state="draining", drain_owner="x", drain_reason="y",
                   drain_changed_unix=10.0)
    path = queue.root / pool.WORKERS / "parkedbox.json"
    record = json.loads(path.read_text())
    record["announced_unix"] = time.time() - pool.OFFER_TIMEOUT_S - 1
    path.write_text(json.dumps(record))

    node = next(n for n in pbstatus.read_pool(queue.root)["nodes"]
                if n["node"] == "parkedbox")
    assert node["state"] == "stale" and node["healthy"] is False
    assert node["draining"] is False
    assert node["reason"] == "offer expired or timestamp invalid"


def test_a_drain_carries_the_last_declared_spool_forward(tmp_path, monkeypatch):
    """The last open poll's raised spool declaration survives the gate closing.

    A supervisor can raise the live ``spool_gb`` offer well above this loop's
    startup ``--spool-gb``.  If a drain re-declared only the startup base, a
    spool-heavy submission that fitted the box would read as never-fits for
    the length of the drain.  One ``_run_loop`` invocation opens the gate,
    publishes the raised declaration, then closes the gate inside the same
    process; the draining record must carry the raised figure forward while
    its live capacity reads zero.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": False, "changed_unix": 1.0}))
    loop = _load_loop(tmp_path, monkeypatch, None,
                      extra_args=("--spool-gb", "10"), once=False)
    monkeypatch.setattr(loop.local_scratch, "OFFER_ROOT",
                        tmp_path / "spool-offer")
    loop.local_scratch.write_spool_offer(queue.ledger().base, 30,
                                         "a supervisor's later measurement")

    real_publish = loop.publish_offer
    statuses: list[str] = []

    def publish(announce, **kwargs):
        result = real_publish(announce, **kwargs)
        statuses.append(result.status)
        if len(statuses) == 1:
            # The gate closes between the open poll and the next one.
            gate.write_text(json.dumps(DRAIN_GATE))
        return result

    monkeypatch.setattr(loop, "publish_offer", publish)
    assert loop._run_loop(lambda: len(statuses) >= 2) == 0
    assert statuses == ["published", "published"]

    offer = json.loads(_offer_path(queue).read_text())
    assert offer["state"] == "draining"
    assert offer["capacity"]["spool_gb"] == 30
    assert all(value == 0 for value in offer["observed_capacity"].values())


def test_live_and_foreign_malformed_offers_stay_readable(tmp_path):
    """An absent ``state`` is live, and a malformed drain field is unknown.

    Older loops and ordinary open gates publish no ``state``, and a record
    with a drain field of the wrong shape must not crash or mislead the
    census: the box still reads as draining, with the unusable field reported
    as unknown rather than rendered.
    """

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.announce(host="openbox", tags=["x86"], has_gpu=False,
                   capacity={"cpu": 4}, observed_capacity={"cpu": 4})
    (queue.root / pool.WORKERS / "foreign.json").write_text(json.dumps({
        "schema": pool.POOL_OFFER_SCHEMA_V1, "host": "foreign",
        "tags": ["x86"], "has_gpu": False, "capacity": {"cpu": 4},
        "observed_capacity": {"cpu": 0}, "announced_unix": time.time(),
        "state": "draining", "drain_owner": 5, "drain_reason": ["y"],
        "drain_changed_unix": "soon",
    }))

    nodes = {n["node"]: n for n in pbstatus.read_pool(queue.root)["nodes"]}
    assert nodes["openbox"]["state"] == "live"
    assert nodes["openbox"]["draining"] is False
    assert nodes["foreign"]["state"] == "draining"
    assert nodes["foreign"]["draining"] is True
    assert nodes["foreign"]["drain_owner"] is None
    assert nodes["foreign"]["drain_reason"] is None
    assert nodes["foreign"]["drain_changed_unix"] is None

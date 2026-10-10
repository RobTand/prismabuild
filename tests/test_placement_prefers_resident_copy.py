"""prismabuild#1733: offers and the gang vote prefer resident hosts, admit without copy.

A row that names a resident set runs faster where its copy is resident,
but the copy is a preference, never admission: a fit host without one
still claims, and a row without a set keeps the prior order exactly.
"""
from __future__ import annotations

import json
import secrets
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from prismabuild import _gang, core as pb, pool
from admitted_queue_fixture import AdmittedQueueFixture
from test_resident_sets_records import source as resident_source

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbgang  # noqa: E402
import pbrun  # noqa: E402

HOSTS = ("ahost", "zhost")
SET_ID = "c" * 64


def _resident_world(tmp_path, *, resident_host="zhost", lease_now=100):
    """A queue with a two-host set; a resident copy only where asked."""
    from prismabuild import resident_sets as rs
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    for host in HOSTS:
        queue.mint_tier_capacity("local:" + host, {"local_gib": 1})
    root, manifest = resident_source(tmp_path)
    store = rs.ResidentSets(tmp_path / "queue")
    record = store.publish(manifest=manifest, canonical_root=str(root),
                           hosts=list(HOSTS),
                           lease={"until": lease_now + 3600, "hard_max": lease_now + 7200},
                           created_by="test", now=lease_now)
    set_id = record["set_id"]
    if resident_host is not None:
        store.write_copy(set_id, resident_host, {"state": "resident", "local_root": "/tmp/x",
                                                 "verification": [], "bytes": 7,
                                                 "completed_unix": lease_now + 20})
    return queue, store, set_id


def _announce(queue, host):
    queue.announce(host=host, tags=["x86"], has_gpu=False,
                   capacity={"cpu": 4, "mem_gb": 16})


def _publish(queue, key, **kwargs):
    queue.publish(action_key=key, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", resources={"cpu": 1, "mem_gb": 1},
                  tags=["x86"], **kwargs)


def test_offer_sort_puts_the_resident_host_first(tmp_path):
    queue, store, set_id = _resident_world(tmp_path)
    for host in HOSTS:
        _announce(queue, host)
    _publish(queue, "a" * 64, resident_set=set_id)
    item = json.loads(queue.item_path(pool.READY, "a" * 64).read_text())
    assert queue.placeable_hosts(item) == ["zhost", "ahost"]


def test_a_row_without_a_set_keeps_the_prior_order(tmp_path):
    queue, store, set_id = _resident_world(tmp_path)
    for host in HOSTS:
        _announce(queue, host)
    _publish(queue, "b" * 64)
    item = json.loads(queue.item_path(pool.READY, "b" * 64).read_text())
    assert queue.placeable_hosts(item) == ["ahost", "zhost"]


def test_a_row_with_a_set_but_no_copy_keeps_order_and_still_claims(tmp_path):
    queue, store, set_id = _resident_world(tmp_path, resident_host=None)
    for host in HOSTS:
        _announce(queue, host)
    queue.publish(action_key="d" * 64, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", resources={"cpu": 1, "mem_gb": 1},
                  resident_set=set_id)
    item = json.loads(queue.item_path(pool.READY, "d" * 64).read_text())
    assert queue.placeable_hosts(item) == ["ahost", "zhost"]
    admitted = AdmittedQueueFixture(queue, capacity={"cpu": 1, "mem_gb": 2},
                                    default_demand={"cpu": 1, "mem_gb": 1})
    claimed = admitted.claim()
    assert claimed is not None and claimed["action_key"] == "d" * 64
    assert claimed["served_from"] == "canonical"


from test_measurement_drains_gpu_backfill import fleet as fleet_fixture

fleet = fleet_fixture
CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 120}
TIERS = {"preferred": list(range(20)), "fallback": []}
GHOSTS = ("sparklina", "sparky")


@pytest.fixture()
def resident_gang(fleet, tmp_path, monkeypatch):
    """An idle two-host gang fleet with a set resident only on sparky."""
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    from prismabuild import resident_sets as rs
    for host in GHOSTS:
        queue.mint_tier_capacity("local:" + host, {"local_gib": 1})
        queue.announce(host=host, tags=["gb10", host, _gang.TAG], has_gpu=True,
                       capacity=dict(CAPACITY))
    root, manifest = resident_source(tmp_path)
    store = rs.ResidentSets(queue.root)
    record = store.publish(manifest=manifest, canonical_root=str(root),
                           hosts=list(GHOSTS),
                           lease={"until": clock[0] + 3600, "hard_max": clock[0] + 7200},
                           created_by="test", now=clock[0])
    set_id = record["set_id"]
    store.write_copy(set_id, "sparky", {"state": "resident", "local_root": "/tmp/x",
                                        "verification": [], "bytes": 7,
                                        "completed_unix": clock[0]})

    def gclaim(host):
        monkeypatch.setattr(pool.socket, "gethostname", lambda: host)
        tick(0.01)
        result = queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
                             has_gpu=True, tags=["gb10", host, _gang.TAG])
        return None if result is None else result["action_key"]

    def members(name):
        """Two portable members: either host may take either member."""
        group = secrets.token_hex(16)
        cas = pb.PrismaBuildCAS(tmp_path / "cas")
        checkout = tmp_path / "checkout"
        rows = []
        for index in range(2):
            clock[0] += 0.001
            gang = {"group": group, "size": 2, "index": index}
            action = pb.seal_action({
                "schema": pb.ACTION_SCHEMA_V2,
                "task": {"definition_id": "tests/gang-member", "definition_version": "v1",
                         "task_class": "generation", "determinism": "deterministic",
                         "artifact_family": "generic", "artifact_kind": "generic",
                         "argv": [sys.executable, "task.py"], "working_directory": ".",
                         "result_path": f"{name}-{index}"},
                "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
                "params": {"gpu_exclusive": False, "execution_timeout_s": 3600, "gang": gang},
                "environment": {"variables": {}, "toolchain": {}},
                "execution_scope": {"portability": "portable", "platform_key": None,
                                    "host_class": None},
            })
            cas.publish_action_request(action)
            key = action["action_key"]
            queue.publish(action_key=key, cas_root=str(cas.root), checkout_root=str(checkout),
                          worker_script="worker.py",
                          resources={"cpu": 2, "gpu": 1, "mem_gb": 100},
                          needs_gpu=True, tags=["gb10"], priority=0, gang=gang,
                          max_attempts=1, resident_set=set_id)
            rows.append(pool._read_json(queue.item_path(pool.READY, key)))
        _gang.publish_group(queue, group, rows)
        return group, [row["action_key"] for row in rows]

    return queue, clock, tick, gclaim, denial, members


def test_gang_vote_waits_for_the_resident_host_then_starts_canonical(resident_gang,
                                                                     monkeypatch):
    queue, clock, tick, gclaim, denial, members = resident_gang
    group, (first, second) = members("resident-vote")
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "deferred_for_resident_copy"
    tick(pool.RESIDENT_COPY_PREFER_S + 1)
    assert gclaim("sparky") is None
    assert gclaim("sparklina") == second
    assert gclaim("sparky") == first
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
    terminal = json.loads(queue.finish(second, status="executed").read_text())
    assert queue.attempt_outcomes(terminal)[0]["served_from"] == "canonical"


def test_gang_skips_a_resident_host_that_holds_a_sibling(resident_gang):
    """A non-resident host elects at once when the resident host is busy."""
    queue, clock, tick, gclaim, denial, members = resident_gang
    group, (first, second) = members("resident-busy")
    assert gclaim("sparky") is None
    assert denial(first, "sparky")["reason"] == "gang_waiting_for_peers"
    assert _gang.elections(queue, group, 2)[0]["host"] == "sparky"
    assert gclaim("sparklina") == second
    assert gclaim("sparky") == first


def test_gang_waits_while_the_resident_host_is_free(resident_gang):
    """The reverse poll order still waits for the free resident host."""
    queue, clock, tick, gclaim, denial, members = resident_gang
    group, (first, second) = members("resident-free")
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "deferred_for_resident_copy"
    assert _gang.elections(queue, group, 2) == {}
    assert gclaim("sparky") is None
    assert denial(first, "sparky")["reason"] == "gang_waiting_for_peers"
    assert gclaim("sparklina") == second
    assert gclaim("sparky") == first


def test_resident_hold_ignores_a_future_publish_stamp(tmp_path):
    """A publisher clock ahead of this host buys no extra wait (#1733)."""
    import time
    queue, store, set_id = _resident_world(tmp_path, lease_now=100)
    for host in HOSTS:
        _announce(queue, host)
    _publish(queue, "e" * 64, resident_set=set_id)
    item = json.loads(queue.item_path(pool.READY, "e" * 64).read_text())
    record = {"group": "f" * 32, "size": 2}
    entry = {"index": 0, "action_key": "e" * 64}
    live = queue.offers()
    now = time.time()
    assert _gang.resident_hold(
        queue, record, entry, {**item, "published_unix": now - 1},
        "ahost", now, live=live, prefer_s=pool.RESIDENT_COPY_PREFER_S) is not None
    assert _gang.resident_hold(
        queue, record, entry, {**item, "published_unix": now + 3600},
        "ahost", now, live=live, prefer_s=pool.RESIDENT_COPY_PREFER_S) is None


def test_pbgang_forwards_the_resident_set_to_pbrun(tmp_path):
    members = [{"tag": "sparky", "argv": ["/bin/true"], "resident_set": SET_ID},
               {"tag": "sparklina", "argv": ["/bin/true"], "resident_set": SET_ID}]
    path = tmp_path / "gang.json"
    path.write_text(json.dumps({"members": members}))
    manifest = pbgang._pbgang_load_manifest(path)
    args = SimpleNamespace(cwd=tmp_path)
    command = pbgang.member_command(args, manifest, manifest["members"][0],
                                    group="0" * 32, index=0)
    flags = command[:command.index("--")]
    parsed = pbrun.parse_args([*flags[2:], "--", "/bin/true"])
    assert parsed.resident_set == SET_ID

"""PB #1517: a gang's members are admitted together or not at all.

The real queue, ledgers, census and CPU/GPU controllers of the #1185/#1419
fixture, with two hosts (sparklina, sparky). Only the clock and sampler are
controlled. Members are real sealed actions carrying ``params.gang``; the
group record is filed by ``_gang.publish_group`` after both rows exist.
"""
from __future__ import annotations

import json
import platform
import secrets
import sys

import pytest
from test_measurement_drains_gpu_backfill import fleet as fleet_fixture

from prismabuild import _gang, adaptive_gpu, core as pb, pool

fleet = fleet_fixture
CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 120}
TIERS = {"preferred": list(range(20)), "fallback": []}
HOSTS = ("sparklina", "sparky")


@pytest.fixture()
def gang_fleet(fleet, tmp_path, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet

    def finish(key, host):
        monkeypatch.setattr(pool.socket, "gethostname", lambda: host)
        queue.finish(key, status="executed")
        tick(adaptive_gpu.PSI_AVG10_S + 1)

    def gclaim(host):
        """A gang-enabled worker on ``host``: the fixture's claim plus the tag."""
        monkeypatch.setattr(pool.socket, "gethostname", lambda: host)
        tick(0.01)
        result = queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
                             has_gpu=True, tags=["gb10", host, _gang.TAG])
        return None if result is None else result["action_key"]

    def members(name, *, priority=0, mem_gb=100, skew_s=_gang.DEFAULT_SKEW_S, file_group=True,
                residency=None, residency_all=False, declares_manifest=False, inputs=None):
        """Seal and publish a two-member gang, one member pinned per host.

        ``residency`` is published as a row residency block (the
        ``--residency stage`` submitter's row) -- member 0 only, or every
        member with ``residency_all``; ``declares_manifest`` seals
        member 0 with a ``pbcampaign.data-manifest`` input and
        ``params.data_manifest`` instead (the #1247 planner's row, no
        residency block).  ``inputs`` are extra CAS input entries for both
        members.
        """
        group = secrets.token_hex(16)
        cas = pb.PrismaBuildCAS(tmp_path / "cas")
        checkout = tmp_path / "checkout"
        rows = []
        for index, host in enumerate(HOSTS):
            clock[0] += 0.001
            gang = {"group": group, "size": len(HOSTS), "index": index}
            member_inputs = list(inputs or [])
            params: dict = {"gpu_exclusive": False, "execution_timeout_s": 3600, "gang": gang}
            if declares_manifest and index == 0:
                manifest = {
                    "schema": pb.DATA_MANIFEST_SCHEMA_V1,
                    "produced_by": {"tool": "tests"},
                    "annotations": {"phases": [
                        {"name": "phase-0", "bytes": 2 << 30, "cumulative_bytes": 2 << 30},
                        {"name": "phase-1", "bytes": 2 << 30, "cumulative_bytes": 4 << 30},
                        {"name": "phase-2", "bytes": 2 << 30, "cumulative_bytes": 6 << 30}]},
                    "mount_prefix": "/data",
                    "entries": [
                        {"path": "/data/blob-0", "offset": 0, "bytes": 2 << 30, "sha256": None},
                        {"path": "/data/blob-1", "offset": 0, "bytes": 2 << 30, "sha256": None},
                        {"path": "/data/blob-2", "offset": 0, "bytes": 2 << 30, "sha256": None}],
                    "entry_count": 3, "total_bytes": 6 << 30,
                }
                manifest = pb.validate_data_manifest(manifest)
                blob = tmp_path / f"{name}-manifest.json"
                blob.write_text(json.dumps(manifest))
                entry, _ = cas.ingest_input(blob, input_id=pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
                snap_blob = tmp_path / f"{name}-snapshot.json"
                snap_blob.write_text("{}")
                snap_entry, _ = cas.ingest_input(snap_blob, input_id="pbrun.checkout-snapshot")
                member_inputs += [entry, snap_entry]
                params.update({
                    "command": [sys.executable, "task.py"], "cwd": str(checkout),
                    "demand": {"cpu": 2, "gpu": 1, "mem_gb": mem_gb},
                    "placement": {"required_tags": [host]},
                    "retry_policy": {"max_attempts": 1},
                    "data_manifest": {
                        "input": entry, "mount_prefix": manifest["mount_prefix"],
                        "entry_count": manifest["entry_count"],
                        "total_bytes": manifest["total_bytes"]},
                    "checkout_snapshot": {
                        "schema": "prismaquant.prismabuild.pbrun_checkout_snapshot.v2",
                        "commit": "0" * 40, "input": snap_entry, "parent": "0" * 40,
                        "refs": {}, "subdirectory": "."}})
            action = pb.seal_action({
                "schema": pb.ACTION_SCHEMA_V2,
                "task": {"definition_id": "tests/gang-member", "definition_version": "v1",
                         "task_class": "generation", "determinism": "deterministic",
                         "artifact_family": "generic", "artifact_kind": "generic",
                         "argv": [sys.executable, "task.py"], "working_directory": ".",
                         "result_path": f"{name}-{index}"},
                "inputs": member_inputs, "code_closure": pb.build_code_closure(checkout, ["task.py"]),
                "params": params,
                "environment": {"variables": {}, "toolchain": {}},
                "execution_scope": {"portability": "portable", "platform_key": None,
                                    "host_class": None},
            })
            cas.publish_action_request(action)
            key = action["action_key"]
            queue.publish(action_key=key, cas_root=str(cas.root), checkout_root=str(checkout),
                          worker_script="worker.py",
                          resources={"cpu": 2, "gpu": 1, "mem_gb": mem_gb},
                          needs_gpu=True, tags=[host], priority=priority, gang=gang,
                          max_attempts=1,
                          **({} if residency is None or (index and not residency_all)
                             else {"residency": residency}))
            rows.append(pool._read_json(queue.item_path(pool.READY, key)))
        if file_group:
            _gang.publish_group(queue, group, rows, skew_s=skew_s)
        return group, [row["action_key"] for row in rows]

    return queue, clock, publish, finish, gclaim, denial, members


def _busy_both(publish, gclaim):
    incumbents = {}
    for host in HOSTS:
        incumbents[host] = publish(f"incumbent-{host}", priority=-10, timeout_s=None,
                                   cpu=2, gpu=1, mem_gb=48)
        assert gclaim(host) == incumbents[host]
    return incumbents


def test_both_hosts_busy_fences_each_host_and_starts_nothing(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    group, keys = members("busy")
    for host in HOSTS:
        assert gclaim(host) is None
    elections = _gang.elections(queue, group, 2)
    assert {index: election["host"] for index, election in elections.items()} == {
        0: "sparklina", 1: "sparky"}
    # A gang fences even host-pinned CPU work, independently of #1526's
    # portable-CPU deferral. Running work drains.
    for host in HOSTS:
        refill = publish(f"refill-{host}", priority=-10, timeout_s=None,
                         cpu=1, gpu=0, mem_gb=1, tags=[host])
        assert gclaim(host) is None, f"{host} admitted lower-priority work past the gang fence"
        # The member's own token-shortage withhold may hold the box first.
        assert denial(refill, host)["reason"] in (
            "deferred_for_gang_reservation", "deferred_behind_withheld_row"), denial(refill, host)
        assert queue.item_path(pool.READY, refill).exists()
        assert queue.ledger(host).held_keys() == [incumbents[host]]
    for key in keys:
        assert queue.item_path(pool.READY, key).exists()
        assert not queue.item_path(pool.CLAIMED, key).exists()


def test_a_gang_fences_later_equal_priority_singles_on_both_hosts(gang_fleet):
    """A waiting gang is not jumped by singles of its own priority (the 2026-10-06 starvation).

    A -10 gang whose members are whole-box wide waited 14 minutes on two Sparks
    while -10 singles, smaller and scanned first, kept taking the hosts.  The
    fence covered only strictly lower priority, so same-priority singles were
    never fenced.  Singles published after the gang's first member now wait
    behind it; running work still drains.
    """
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    group, keys = members("equal-priority", priority=-10)
    for host in HOSTS:
        assert gclaim(host) is None
    for host in HOSTS:
        late = publish(f"late-single-{host}", priority=-10, timeout_s=None,
                       cpu=1, gpu=0, mem_gb=1, tags=[host])
        assert gclaim(host) is None, f"{host} admitted a later same-priority single past the gang"
        assert denial(late, host)["reason"] in (
            "deferred_for_gang_reservation", "deferred_behind_withheld_row"), denial(late, host)
        assert queue.item_path(pool.READY, late).exists()
        assert queue.ledger(host).held_keys() == [incumbents[host]]


def test_a_single_published_before_the_gang_is_not_fenced_by_it(gang_fleet):
    """Arrival order is kept: only singles that came after the gang wait."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    earlier = publish("earlier-single", priority=-10, timeout_s=None,
                      cpu=1, gpu=0, mem_gb=1, tags=["sparky"])
    group, keys = members("later-gang", priority=-10)
    assert gclaim("sparky") == earlier, denial(earlier, "sparky")


def test_a_higher_priority_single_is_not_fenced_by_a_lower_priority_gang(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    group, keys = members("low-gang", priority=-10)
    for host in HOSTS:
        assert gclaim(host) is None
    urgent = publish("urgent-single", priority=0, timeout_s=None,
                     cpu=1, gpu=0, mem_gb=1, tags=["sparky"])
    assert gclaim("sparky") == urgent, denial(urgent, "sparky")


def test_the_host_that_frees_first_does_not_start_its_member_alone(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    group, (first, second) = members("first-free")
    for host in HOSTS:
        assert gclaim(host) is None

    finish(incumbents["sparklina"], "sparklina")
    assert gclaim("sparklina") is None, "a gang member started without its peer"
    waiting = denial(first, "sparklina")
    assert waiting["reason"] == "gang_waiting_for_peers", waiting
    assert waiting["evidence"]["waiting"][0]["index"] == 1
    # Ready holds nothing: the freed host carries no member tokens.
    assert queue.ledger("sparklina").held_keys() == []
    assert queue.item_path(pool.READY, first).exists()
    # The gang fence also holds back host-pinned CPU work on the freed host.
    refill = publish("refill-freed", priority=-10, timeout_s=None,
                     cpu=1, gpu=0, mem_gb=1, tags=["sparklina"])
    assert gclaim("sparklina") is None
    assert denial(refill, "sparklina")["reason"] == "deferred_for_gang_reservation"

    finish(incumbents["sparky"], "sparky")
    assert gclaim("sparky") == second, denial(second, "sparky")
    assert gclaim("sparklina") == first, denial(first, "sparklina")
    assert queue.ledger("sparklina").held_keys() == [first]
    assert queue.ledger("sparky").held_keys() == [second]
    assert queue.item_path(pool.READY, refill).exists()


def test_a_stale_ready_mark_does_not_commit_the_peer(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    group, (first, second) = members("stale")
    finish(incumbents["sparklina"], "sparklina")
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "gang_waiting_for_peers"
    # sparklina's loop goes quiet; its ready mark ages past freshness.
    clock[0] += _gang.READY_FRESH_S + 5
    finish(incumbents["sparky"], "sparky")
    assert gclaim("sparky") is None, "committed against a stale peer ready mark"
    assert denial(second, "sparky")["reason"] == "gang_waiting_for_peers"
    assert queue.ledger("sparky").held_keys() == []

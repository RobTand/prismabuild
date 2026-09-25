"""An egress declares the CPU it owns, or it never runs where it must (#607).

The night of 2026-09-18, campaign ``397b8f851004``'s movement window wedged on
dl380g10: its three ``stage-release`` egresses were denied
``adaptive_cpu_refused`` with ``decision: unbounded_cpu_not_exclusive`` at
``psi_some 0.043`` and ``busy_cpus 3.61 of 80`` -- an idle box by every
threshold -- because the box *held* eight worker reservations, and a file
server's loops mean it always does.  The row those loops refused was
``{"mem_gb": 1}``: sealed with no ``cpu`` key, so ``adaptive_cpu`` read it as
unknown CPU use and serialized it on a host that never empties.

That is the mover bug of #603/#607 again, on the node the fix skipped, and it
is worse there.  A concluding mover *pins* its range's stage tokens; the
egress is the only node that returns them.  A release that can only claim an
empty box therefore deadlocks the tier exactly when it is full -- the one
moment the row's own "no tier demand" comment says it must not -- and the
window behind it stalls on ``tier_reservation_unavailable`` until an operator
intervenes.

So the row declares one CPU: a *declared bound*, the width of the
single-process unlink-and-record an egress is, exactly as a mover's no-receipt
fallback declares ``readers``.  Not a measurement -- an egress files no
receipts, so there is nothing honest to price it off, and pricing it off the
movers' copy receipts would measure the wrong node entirely (#655's lesson).
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import time
import types

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

CONSUMER = "c" * 64
HOLDER = "d" * 64
TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
GIB = storage_tiers.GIB
PHASE_BYTES = 2 * GIB
READERS = 4
#: Two CPUs, because the admission proof needs a host the test can state: one
#: for the holder the egress must run beside, one for the egress.
CAPACITY = {"cpu": 2, "mem_gb": 8}


def _manifest(phases: int = 2) -> dict[str, object]:
    entries, table, running = [], [], 0
    for index in range(phases):
        entries.append({"path": f"/mnt/shared/part-{index}", "offset": 0,
                        "bytes": PHASE_BYTES, "sha256": None})
        running += PHASE_BYTES
        table.append({"name": f"phase-{index}", "bytes": PHASE_BYTES,
                      "cumulative_bytes": running})
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {}, "annotations": {"phases": table},
        "mount_prefix": "/mnt/shared", "entries": entries,
        "entry_count": len(entries), "total_bytes": running,
    }


class _Cas:
    def __init__(self, manifest_path: Path) -> None:
        self._manifest = manifest_path
        self.requested: list[str] = []
        self.actions: dict[str, dict] = {}

    def input_path(self, entry):
        return self._manifest

    def publish_action_request(self, action) -> None:
        self.requested.append(str(action["action_key"]))
        self.actions[str(action["action_key"])] = dict(action)


def _template(digest: str, size: int) -> dict[str, object]:
    import pbrun

    return {
        "cas": None, "marker_root": Path("/home/rob/tmp/markers"),
        "checkout_identity": {"commit": "a" * 40},
        "log_name": "x.log", "stamp_name": "pbrun.stamp",
        "task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                 "task_class": "generation", "determinism": "stochastic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "working_directory": "."},
        "inputs": [{"id": "pbrun.checkout-snapshot", "sha256": "b" * 64,
                    "bytes": 4096}],
        "code_closure": pbrun.build_stamp_closure("pbrun.stamp", "{}"),
        "params": {"command": ["true"], "cwd": "/home/rob", "demand": {"cpu": 1},
                   "placement": {"required_tags": []},
                   "checkout_snapshot": {
                       "schema": pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1,
                       "commit": "a" * 40, "subdirectory": ".",
                       "input": {"id": pb.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID,
                                 "sha256": "b" * 64, "bytes": 4096}},
                   "retry_policy": {"max_attempts": 1, "retry_safe": False},
                   "data_manifest": {"input": {"sha256": digest, "bytes": size}}},
        "environment": {"variables": {"PATH": "/usr/bin"}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    }


def _ram_tier(tmp_path: Path) -> dict[str, object]:
    return {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
            "tier_id": RAM_TIER, "host": "dl380g10", "tier": "ram",
            "mountpoint": str(tmp_path / "ram"),
            "capacity_bytes": 8 * PHASE_BYTES,
            "mover_python": "/usr/bin/python3",
            "mover_tools_root": "/opt/prismabuild/tools"}


def _seal(tmp_path: Path, queue: pool.PoolQueue, *, ram=False, **overrides):
    """Seal one window's rows the way ``pbrun`` does, against a live tier."""

    import pbrun

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(_manifest()))
    digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()

    def discover(**_kwargs):
        return {TIER: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                       "tier_id": TIER, "host": "dl380g10", "tier": "stage",
                       "mountpoint": str(tmp_path / "stage"),
                       "capacity_bytes": 8 * PHASE_BYTES}}

    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=discover)
    tier = pbrun.resolve_stage_tier(queue, None)
    if ram:
        # The ram leg is exercised through the resolver, not through a second
        # discovered tier: what is under test is the row it seals, not the
        # loop that announces the mountpoint.
        monkey_target = overrides.pop("monkeypatch", None)
        if monkey_target is not None:
            monkey_target.setattr(pbrun, "resolve_ram_tier",
                                  lambda queue_, stage_tier: _ram_tier(tmp_path))
    args = types.SimpleNamespace(
        priority=-10, max_attempts=1, retry_safe=False,
        residency="stage", residency_tier=None, residency_mover_mem_gb=1,
        residency_mover_readers=READERS, residency_mover_max_attempts=3)
    for name, value in overrides.items():
        setattr(args, name, value)
    cas = _Cas(manifest_path)
    staged = pbrun.residency_stage_rows(
        _template(digest, manifest_path.stat().st_size),
        consumer_action_key=CONSUMER, tier=tier, args=args, queue=queue,
        cas=cas)
    staged["cas"] = cas
    return staged


def _idle_two_cpu_host(monkeypatch):
    """A host with two CPUs, freshly sampled, doing nothing measurable."""

    from prismabuild import adaptive_cpu
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", False))
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": time.time(), "cpu_count": 2, "interval_s": 1.,
        "busy_cpus": 0., "psi_some": 0.})


def _hold_one_cpu(queue: pool.PoolQueue, tiers) -> dict[str, object]:
    queue.publish(action_key=HOLDER, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root), worker_script="worker.py",
                  resources={"cpu": 1})
    claim = queue.claim(capacity=CAPACITY, tags=["dl380g10"], cpu_tiers=tiers,
                        adaptive_cpu=True)
    assert claim is not None and claim["action_key"] == HOLDER
    return claim


def test_the_egress_row_is_admitted_beside_a_holder_and_the_old_row_is_not(
        tmp_path, monkeypatch):
    """The fix, and the bug, through one admission path.

    The A side is the row ``residency_stage_rows`` seals today.  The B side is
    the same row with its ``cpu`` key removed -- the shipped shape -- published
    under its own key on the same host with the same holder.  A claims the one
    CPU the holder left; B is refused as ``unbounded_cpu_not_exclusive`` and
    waits for a host that never empties.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    tiers = {"preferred": [0], "fallback": [1]}
    staged = _seal(tmp_path, queue)
    _idle_two_cpu_host(monkeypatch)
    _hold_one_cpu(queue, tiers)

    row = dict(staged["plan"]["phases"][0]["egress_row"])
    assert row["resources"]["cpu"] == 1, row["resources"]
    assert row["resources"]["mem_gb"] == 1, row["resources"]
    queue.publish(**row)
    claim = queue.claim(capacity=CAPACITY, tags=["dl380g10"], cpu_tiers=tiers,
                        adaptive_cpu=True)
    assert claim is not None, "a bounded egress must run beside one holder"
    assert claim["action_key"] == row["action_key"]

    unbounded = dict(row)
    unbounded["action_key"] = "e" * 64
    unbounded["resources"] = {key: value
                              for key, value in row["resources"].items()
                              if key != "cpu"}
    queue.publish(**unbounded)
    assert queue.claim(capacity=CAPACITY, tags=["dl380g10"], cpu_tiers=tiers,
                       adaptive_cpu=True) is None
    assert queue.item_path(pool.READY, unbounded["action_key"]).exists()


def test_the_egress_keeps_no_tier_demand_and_its_own_retry_policy(
        tmp_path):
    """The fix adds a CPU, not a tier leg: an egress returns capacity.

    Its attempt policy is a movement node's, not the submission's (#950): a
    single-attempt consumer's egress that met one transient unlink error
    ended ``failed`` and left the range's bytes holding their tokens.  The
    release tool counts a file already gone as released, so a second attempt
    is safe.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    staged = _seal(tmp_path, queue)
    row = staged["plan"]["phases"][0]["egress_row"]
    assert set(row["resources"]) == {"cpu", "mem_gb"}, row["resources"]
    assert (row["max_attempts"], row["retry_safe"]) == (3, True)
    key = str(row["action_key"])
    body = staged["cas"].actions[key]
    assert body["params"]["demand"] == {"cpu": 1, "mem_gb": 1}
    assert body["params"]["retry_policy"] == {"max_attempts": 3, "retry_safe": True}


def test_the_ram_egress_declares_one_cpu_too(tmp_path, monkeypatch):
    """The ram tier's release node (#640) is an egress of the same shape."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    staged = _seal(tmp_path, queue, ram=True, monkeypatch=monkeypatch)
    phase = staged["plan"]["phases"][0]
    assert "ram_egress_row" in phase, "the ram leg must seal for this test"
    row = phase["ram_egress_row"]
    assert row["resources"] == {"cpu": 1, "mem_gb": 1}, row["resources"]
    key = str(row["action_key"])
    assert staged["cas"].actions[key]["params"]["demand"] == {
        "cpu": 1, "mem_gb": 1}
    # And the movement node's retry policy, on the row and the body (#950).
    assert (row["max_attempts"], row["retry_safe"]) == (3, True)
    assert staged["cas"].actions[key]["params"]["retry_policy"] == {
        "max_attempts": 3, "retry_safe": True}


def test_the_egress_row_carries_the_cpu_on_the_sealed_body(tmp_path):
    """The row and the sealed CAS request agree, because a row is a spelling.

    ``publication_row`` reads the demand off the sealed body, so this is one
    assertion that the fix landed where the worker reads it, not only where
    the queue row does.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    staged = _seal(tmp_path, queue)
    row = staged["plan"]["phases"][0]["egress_row"]
    key = str(row["action_key"])
    body = staged["cas"].actions[key]
    assert int(body["params"]["demand"]["cpu"]) == 1

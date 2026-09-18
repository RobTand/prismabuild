"""A mover's ``cpu``, ``mem_gb`` and retry policy are its own (#603, #607).

The first live #583 window ran one large mover at a time with 17 idle worker
loops on the file server and 674 GiB free on the tier.  Nothing about the
window refused the others: the mover row declared ``{"mem_gb": 1}`` and no
``cpu`` at all, and ``adaptive_cpu`` reads an absent ``cpu`` as *unknown* CPU
use -- legacy producers that reserved only GPU and memory -- so it serializes
such a claim on a freshly idle host (``unbounded_cpu_not_exclusive``,
``adaptive_cpu.py``).  dl380g10's own denial file named exactly that for
``layer-2`` and ``layer-3`` with ``holders: 1`` and ``busy_cpus: 3.3 of 80``.

So this file drives the real ``residency_stage_rows`` and the real
``queue.claim``: the demand a row carries is asserted through an admission
decision rather than by reading the dict back, and the inverse -- the demand
the shipped row had -- is asserted to refuse, so the test would have failed
before the fix and fails again if the ``cpu`` key is dropped.
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
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

CONSUMER = "c" * 64
HOLDER = "d" * 64
TIER = "prismabuild-stage:dl380g10"
GIB = storage_tiers.GIB
PHASE_BYTES = 2 * GIB
READERS = 4
#: Two CPUs, because the CPU proof needs a host the test can state, and enough
#: memory that the mover's own ``mem_gb`` is never what refuses it.
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
                   # The consumer's: one attempt, not safe to retry.  What a
                   # mover must not inherit.
                   "retry_policy": {"max_attempts": 1, "retry_safe": False},
                   "data_manifest": {"input": {"sha256": digest, "bytes": size}}},
        "environment": {"variables": {"PATH": "/usr/bin"}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    }


def _seal(tmp_path: Path, queue: pool.PoolQueue, **overrides):
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


def test_a_mover_row_is_admitted_beside_a_holder_and_the_old_row_is_not(
        tmp_path, monkeypatch):
    """The fix, and the bug, through one admission path.

    The A side is the row ``residency_stage_rows`` seals today.  The B side is
    the same row with its ``cpu`` key removed -- the shipped shape -- published
    under its own key on the same host with the same holder.  A passes, B is
    refused as ``unbounded_cpu_not_exclusive``.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    tiers = {"preferred": [0], "fallback": [1]}
    staged = _seal(tmp_path, queue)
    _idle_two_cpu_host(monkeypatch)
    _hold_one_cpu(queue, tiers)

    row = dict(staged["plan"]["phases"][0]["mover_row"])
    assert row["resources"]["cpu"] == READERS, row["resources"]
    # The mover asks for more CPU than this two-CPU host has, which is not what
    # is under test: the demand a *row* carries is the fleet's, and the
    # admission proof needs a host the test can state.  Ask for what fits and
    # keep the key, because the key is the whole difference.
    row["resources"] = {**row["resources"], "cpu": 1}
    queue.publish(**row)
    claim = queue.claim(capacity=CAPACITY, tags=["dl380g10"], cpu_tiers=tiers,
                        adaptive_cpu=True)
    assert claim is not None, "a bounded mover must run beside one holder"
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


def test_a_mover_carries_its_own_retry_policy_not_the_consumers(tmp_path):
    """#603's other half: three attempts and ``retry_safe``, on row and body."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    staged = _seal(tmp_path, queue)
    row = staged["plan"]["phases"][0]["mover_row"]
    assert row["max_attempts"] == 3
    assert row["retry_safe"] is True
    # The consumer's policy is untouched by the mover's, and the egress node
    # reserves nothing and keeps the submission's.
    assert staged["plan"]["phases"][0]["egress_row"]["max_attempts"] == 1


def test_a_mover_prices_itself_from_receipts_once_any_exist(tmp_path):
    """``cpu`` and ``mem_gb`` come off ``pb-queue/movers/`` when they can.

    Two receipts, the second greedier.  A reservation takes the maximum: a
    number half the movers exceed is a reservation half of them run outside.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.record_move("1" * 64, {
        "tier_id": TIER, "consumer_action_key": CONSUMER, "complete": True,
        "seconds": 100.0, "cpu_seconds": 250.0,
        "peak_rss_bytes": 3 * GIB // 2, "unix": 10.0})
    queue.record_move("2" * 64, {
        "tier_id": TIER, "consumer_action_key": CONSUMER, "complete": True,
        "seconds": 100.0, "cpu_seconds": 610.0,
        "peak_rss_bytes": 2 * GIB, "unix": 20.0})
    # Another tier's disks say nothing about this one's, and a refusal
    # measured a refusal.
    queue.record_move("3" * 64, {
        "tier_id": "prismabuild-stage:elsewhere", "complete": True,
        "seconds": 1.0, "cpu_seconds": 64.0, "peak_rss_bytes": 40 * GIB,
        "unix": 30.0})
    queue.record_move("4" * 64, {
        "tier_id": TIER, "refusal": "residency_overran_reservation",
        "seconds": 1.0, "cpu_seconds": 99.0, "peak_rss_bytes": 50 * GIB,
        "unix": 40.0})

    staged = _seal(tmp_path, queue)
    resources = staged["plan"]["phases"][0]["mover_row"]["resources"]
    assert resources["cpu"] == 7        # ceil(610 / 100)
    assert resources["mem_gb"] == 2     # ceil(2 GiB / GiB)
    source = staged["plan"]["demand_source"]
    assert source["cpu"] == "receipts" and source["mem_gb"] == "receipts"
    assert source["receipts_read"] == 2
    assert source["cpu_receipts"] == ["1" * 64, "2" * 64]


def test_with_no_receipt_the_row_declares_the_width_its_command_runs_at(tmp_path):
    """No measurement, so the demand is a declared bound the command states."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    staged = _seal(tmp_path, queue, residency_mover_readers=6,
                   residency_mover_mem_gb=3)
    phase = staged["plan"]["phases"][0]
    assert phase["mover_row"]["resources"]["cpu"] == 6
    assert phase["mover_row"]["resources"]["mem_gb"] == 3
    source = staged["plan"]["demand_source"]
    assert source["cpu"] == "declared_readers"
    assert source["mem_gb"] == "declared_fallback"
    assert source["receipts_read"] == 0


def test_the_declared_width_and_the_width_the_copy_runs_at_are_one_number(
        tmp_path):
    """The row's ``cpu`` is on the mover's own command line, so they cannot drift.

    A demand that reserved four CPUs while the copy ran at sixteen would be a
    reservation the action runs outside, which is the failure ``demand`` exists
    to prevent.  So ``--readers`` is stated on the sealed command and the same
    number is the row's ``cpu``.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    staged = _seal(tmp_path, queue, residency_mover_readers=5)
    phase = staged["plan"]["phases"][0]
    key = str(phase["mover_row"]["action_key"])
    command = staged["cas"].actions[key]["params"]["command"]
    assert "--readers" in command
    assert command[command.index("--readers") + 1] == "5"
    assert phase["mover_row"]["resources"]["cpu"] == 5
    # And the sealed body's own retry policy is the mover's, not the template's.
    assert staged["cas"].actions[key]["params"]["retry_policy"] == {
        "max_attempts": 3, "retry_safe": True}


def test_a_zero_or_absent_field_never_prices_a_mover(tmp_path):
    """A receipt that measured nothing contributes nothing, rather than zero."""

    records = [
        {"tier_id": TIER, "seconds": 10.0, "cpu_seconds": 0.0,
         "peak_rss_bytes": 0, "unix": 1.0},
        {"tier_id": TIER, "seconds": 0.0, "cpu_seconds": 90.0,
         "peak_rss_bytes": 90 * GIB, "unix": 2.0},
    ]
    priced = storage_tiers.mover_demand_from_receipts(
        records, tier_id=TIER, readers=4, fallback_mem_gb=1)
    assert priced["cpu"] == 4 and priced["mem_gb"] == 1
    assert priced["demand_source"]["cpu"] == "declared_readers"


@pytest.mark.parametrize("readers,mem", [(0, 1), (4, 0), (-1, 1)])
def test_a_fallback_that_is_not_a_positive_bound_is_refused(readers, mem):
    with pytest.raises(ValueError):
        storage_tiers.mover_demand_from_receipts(
            [], tier_id=TIER, readers=readers, fallback_mem_gb=mem)


def test_a_movers_fill_demand_is_one_receipts_share_never_a_mix(tmp_path):
    """Each receipt bounds one mover; the maximum is over those, not across them.

    A bootstrap window runs several movers at once, so its receipts report the
    *pool's* delivery -- three copies' worth.  Reading that as one mover's rate
    and pairing it with some other receipt's file-side rate reserves the whole
    pool per mover, which re-serializes movers through the fill token: the same
    failure the cpu demand fixes, arriving by another resource kind.
    """

    solo = {"action_key": "1" * 64, "tier_id": TIER, "seconds": 44.5,
            "mb_per_s_file_side": 229.4,
            storage_tiers.MOVER_CONCURRENCY_FIELD: 1,
            "disk_pacing": {storage_tiers.POOL_FILL_FIELD: 166.0}}
    shared = {"action_key": "2" * 64, "tier_id": TIER, "seconds": 44.5,
              "mb_per_s_file_side": 300.0,
              storage_tiers.MOVER_CONCURRENCY_FIELD: 3,
              "disk_pacing": {storage_tiers.POOL_FILL_FIELD: 498.0}}
    # min(229.4, 166/1) = 166 ; min(300, 498/3) = 166 ; max = 166.
    assert storage_tiers.mover_fill_demand_from_receipts(
        [solo, shared], tier_id=TIER) == 166
    # An ARC-warm receipt from the same shared window is still bounded by the
    # share, not by the 1478 MB/s the disks never produced.
    warm = {**shared, "action_key": "3" * 64, "mb_per_s_file_side": 1477.9}
    assert storage_tiers.mover_fill_demand_from_receipts(
        [solo, warm], tier_id=TIER) == 166


def test_a_receipt_without_its_concurrency_count_prices_nothing(tmp_path):
    """The pool's delivery is unreadable as one mover's without the count."""

    record = {"action_key": "1" * 64, "tier_id": TIER, "seconds": 44.5,
              "mb_per_s_file_side": 229.4,
              "disk_pacing": {storage_tiers.POOL_FILL_FIELD: 498.0}}
    assert storage_tiers.mover_fill_demand_from_receipts(
        [record], tier_id=TIER) is None
    assert storage_tiers.mover_fill_demand_from_receipts(
        [{**record, storage_tiers.MOVER_CONCURRENCY_FIELD: 0}],
        tier_id=TIER) is None


def test_a_second_submission_reuses_the_frozen_plan_rather_than_repricing(tmp_path):
    """A frozen window is not repartitioned because a receipt landed since.

    The demand is read off live receipts, so a resubmission of the same consumer
    after one more mover filed would seal different mover keys and a different
    plan -- and ``residency_plan.freeze`` is first-writer, so it would refuse the
    whole submission with both bodies in hand.
    """

    from prismabuild import residency_plan as rp

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    first = _seal(tmp_path, queue)
    rp.freeze(queue, first["plan"])

    queue.record_move("9" * 64, {
        "tier_id": TIER, "consumer_action_key": CONSUMER, "complete": True,
        "seconds": 100.0, "cpu_seconds": 800.0, "peak_rss_bytes": 9 * GIB,
        "unix": 500.0})
    second = _seal(tmp_path, queue)
    assert second.get("reused_frozen_plan") is True
    assert second["plan"] == first["plan"]
    # And freezing it again is the no-op the first-writer contract promises.
    rp.freeze(queue, second["plan"])
    assert second["residency"]["leads"] == rp.leads_for(first["plan"])

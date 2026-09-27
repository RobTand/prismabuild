"""#1247: a READY row's declared manifest becomes a tier residency plan.

The planner is the submitter's own sealing path invoked by the tier role
(``tools/fleet/manifest_promotion.py``): for the first READY rows in claim
order it seals movement nodes off the row's sealed request and freezes the
plan, and the tier loop's ordinary adoption pass publishes the movers.  A row
with a filed plan stands down; a row without a manifest is not the planner's;
the bound is rows, not bytes, because the resident-byte bound is the tier's
window and eviction.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import prismabuild.core as pb
import prismabuild.storage_tiers as storage_tiers  # noqa: F401
from prismabuild import residency_plan

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from prewarm_fixture import Fleet, data_manifest, phase_table  # noqa: E402
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import manifest_promotion  # noqa: E402

HOST = "dl380g10"
STAGE_TIER = "prismabuild-stage:dl380g10"
MIB = 1024 * 1024

NAMED_FILES = [("model-00001.safetensors", 64 * MIB),
               ("model-00002.safetensors", 64 * MIB)]


def _stage_tier(fleet: Fleet) -> dict:
    return {
        "schema": "prismabuild.storage_tier.v1",
        "tier": "stage",
        "tier_id": STAGE_TIER,
        "host": HOST,
        "mountpoint": str(fleet.root / "stage"),
        "mover_python": sys.executable,
        "mover_tools_root": str(Path(__file__).resolve().parents[1]
                                / "tools" / "fleet"),
    }


def _ready_item(fleet: Fleet, key: str) -> dict:
    return json.loads(
        (fleet.queue.root / "ready" / f"{key}.json").read_text())


def _manifest_row(fleet: Fleet, seed: str, *, annotations=None,
                  files=None, priority: int = 0) -> str:
    """Seal and publish a READY manifest row that carries a checkout snapshot.

    The movers a plan names materialize the consumer's sealed snapshot, so
    the row the planner feeds them must address one exactly as a snapshot
    submitter's does.
    """

    files = files if files is not None else [
        fleet.file(name, size) for name, size in NAMED_FILES]
    manifest = data_manifest(files, prefix=str(fleet.mount),
                             annotations=annotations)
    manifest = pb.validate_data_manifest(manifest)
    blob = fleet.root / f"{seed}.manifest.json"
    blob.write_text(json.dumps(manifest))
    entry, _ = fleet.cas.ingest_input(
        blob, input_id=pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
    snap = fleet.root / f"{seed}.snapshot.json"
    snap.write_text("{}")
    snap_entry, _ = fleet.cas.ingest_input(
        snap, input_id="pbrun.checkout-snapshot")
    snapshot = {
        "schema": "prismaquant.prismabuild.pbrun_checkout_snapshot.v2",
        "commit": "0" * 40, "input": snap_entry, "parent": "0" * 40,
        "refs": {}, "subdirectory": ".",
    }
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "fleet/tests", "definition_version": "v1",
                 "task_class": "generation", "determinism": "stochastic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": ["/bin/true", seed], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [entry, snap_entry],
        "code_closure": fleet.code_closure,
        "params": {
            "command": ["/bin/true", seed], "cwd": str(fleet.root),
            "demand": {"cpu": 1}, "placement": {"required_tags": []},
            "retry_policy": {"max_attempts": 1},
            "data_manifest": {
                "input": entry, "mount_prefix": manifest["mount_prefix"],
                "entry_count": manifest["entry_count"],
                "total_bytes": manifest["total_bytes"],
            },
            "checkout_snapshot": snapshot,
        },
        "environment": {"variables": {"PATH": "/usr/bin"}, "toolchain": {}},
        "execution_scope": {"portability": "portable",
                            "platform_key": None, "host_class": None},
    })
    key = str(action["action_key"])
    request = (fleet.cas_root / "requests" / key[:2]
               / f"{key}.json")
    request.parent.mkdir(parents=True, exist_ok=True)
    request.write_text(json.dumps(action))
    fleet.queue.publish(
        action_key=key, cas_root=fleet.cas_root,
        worker_script=str(fleet.root / "worker.py"),
        checkout_root=str(fleet.root), priority=priority)
    return key


def test_a_declaring_row_gains_a_filed_plan_and_a_tier_receipt(tmp_path):
    """One READY manifest row: plan filed, movers sealed, receipt stamped."""

    fleet = Fleet(tmp_path)
    key = _manifest_row(fleet, "row-a",
                        annotations={"phases": phase_table(NAMED_FILES)})

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, key)])

    assert len(outcomes) == 1, outcomes
    assert outcomes[0]["outcome"] == "planned", outcomes[0]
    assert outcomes[0]["phases"] == 2
    # The plan is filed under the consumer's key, first-writer.
    plan = residency_plan.read(fleet.queue, key)
    assert plan is not None
    assert str(plan["tier_id"]) == STAGE_TIER
    # Every phase names a mover; the movement requests are sealed in the CAS
    # for the tier loop's adoption pass to publish.
    for phase in plan["phases"]:
        mover = str(phase["mover_row"]["action_key"])
        assert (Path(fleet.cas_root) / "requests" / mover[:2]
                / f"{mover}.json").exists()
    # The receipt carries the additive tier block.
    record = fleet.queue.prewarm(key)
    assert record is not None
    tier = record.get("tier")
    assert isinstance(tier, dict)
    assert tier["destination"] == manifest_promotion.TIER_RECEIPT_DESTINATION
    assert tier["status"] == "planned"
    assert tier["phases"] == 2


def test_a_row_with_a_filed_plan_stands_down(tmp_path):
    """The planner never touches a consumer that already has a plan."""

    fleet = Fleet(tmp_path)
    key = _manifest_row(fleet, "row-b",
                        annotations={"phases": phase_table(NAMED_FILES)})
    assert manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, key)])[0]["outcome"] == "planned"

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, key)])

    assert outcomes[0]["outcome"] == "stands_down"


def test_a_row_without_a_manifest_is_not_the_planners(tmp_path):
    """A plain READY row is examined and passed over, never refused."""

    fleet = Fleet(tmp_path)
    files = [fleet.file(name, size) for name, size in NAMED_FILES]
    key = fleet.action("row-c", files, with_manifest=False)

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, key)])

    assert outcomes[0]["outcome"] == "no_manifest"
    assert residency_plan.read(fleet.queue, key) is None


def test_the_streaming_rule_plans_one_row_per_cycle(tmp_path):
    """The bound is rows: the second row waits for the next cycle."""

    fleet = Fleet(tmp_path)
    annotations = {"phases": phase_table(NAMED_FILES)}
    first = _manifest_row(fleet, "row-d", annotations=annotations)
    second = _manifest_row(fleet, "row-e", annotations=annotations)

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, first), _ready_item(fleet, second)])

    assert [outcome["outcome"] for outcome in outcomes] == ["planned"]
    assert residency_plan.read(fleet.queue, first) is not None
    assert residency_plan.read(fleet.queue, second) is None


def test_a_refusal_is_receipted_and_never_raises(tmp_path):
    """A tier with no mountpoint refuses the row and records the reason."""

    fleet = Fleet(tmp_path)
    key = _manifest_row(fleet, "row-f",
                        annotations={"phases": phase_table(NAMED_FILES)})
    broken = _stage_tier(fleet)
    broken["mountpoint"] = "relative/and/refused"

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, broken,
        ready=[_ready_item(fleet, key)])

    assert outcomes[0]["outcome"] == "refused"
    assert outcomes[0]["reason"]
    tier = (fleet.queue.prewarm(key) or {}).get("tier")
    assert isinstance(tier, dict) and tier["status"] == "refused"
    assert residency_plan.read(fleet.queue, key) is None


# --- review round 1 (#1252): containment, priority, deferral, memoization ---

def test_a_corrupt_request_never_stops_the_cycle(tmp_path):
    """REVIEW-1252 item 1: rows whose requests go bad are refused by name.

    The planner reads the request file raw -- no validate_action between the
    CAS and it -- so a corrupted-but-JSON body (a deleted ``task`` here, a
    non-mapping one in the next case) must surface as a receipted refusal,
    never as an exception out of the tier role's single writer, and the next
    row in claim order must still be planned.
    """

    fleet = Fleet(tmp_path)
    annotations = {"phases": phase_table(NAMED_FILES)}
    first = _manifest_row(fleet, "bad-no-task", annotations=annotations)
    second = _manifest_row(fleet, "bad-task-shape", annotations=annotations)
    good = _manifest_row(fleet, "good-row", annotations=annotations)
    for key, mutate in ((first, lambda body: body.pop("task")),
                        (second, lambda body: body.update(task=["not", "a", "map"]))):
        path = fleet.cas_root / "requests" / key[:2] / f"{key}.json"
        body = json.loads(path.read_text())
        mutate(body)
        path.write_text(json.dumps(body))

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, first), _ready_item(fleet, second),
               _ready_item(fleet, good)])

    assert [outcome["outcome"] for outcome in outcomes] == \
        ["refused", "refused", "planned"]
    for outcome in outcomes[:2]:
        assert outcome["reason"]
    assert residency_plan.read(fleet.queue, good) is not None


def test_a_mover_inherits_its_consumers_priority(tmp_path):
    """REVIEW-1252 item 4: staging rides the consumer's own band."""

    fleet = Fleet(tmp_path)
    key = _manifest_row(fleet, "row-priority",
                        annotations={"phases": phase_table(NAMED_FILES)},
                        priority=5)

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, key)])

    assert outcomes[0]["outcome"] == "planned", outcomes[0]
    plan = residency_plan.read(fleet.queue, key)
    assert plan is not None
    for phase in plan["phases"]:
        assert int(phase["mover_row"]["priority"]) == 5


def test_a_busy_transition_lock_defers_not_refuses(tmp_path, monkeypatch):
    """REVIEW-1252 item 6: a busy lock is transient, and says so."""

    import prismabuild.pool as pool_mod
    fleet = Fleet(tmp_path)
    key = _manifest_row(fleet, "row-busy",
                        annotations={"phases": phase_table(NAMED_FILES)})

    def _busy(self, action_key, **kwargs):
        raise pool_mod.TransitionLockBusy(action_key, {"holder_pid": 1})

    monkeypatch.setattr(pool_mod.PoolQueue, "_transition_locked", _busy)
    outcomes = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, key)])
    monkeypatch.undo()

    assert outcomes[0]["outcome"] == "deferred"
    assert outcomes[0]["reason"]
    tier = (fleet.queue.prewarm(key) or {}).get("tier")
    assert isinstance(tier, dict) and tier["status"] == "deferred"
    assert residency_plan.read(fleet.queue, key) is None


def test_decisions_are_memorized_and_receipts_not_rewritten(
        tmp_path, monkeypatch):
    """REVIEW-1252 item 3: one request read per key, one receipt per change."""

    fleet = Fleet(tmp_path)
    plain = fleet.action("row-plain", [
        fleet.file(name, size) for name, size in NAMED_FILES],
        with_manifest=False)
    reads: list[str] = []
    real = manifest_promotion.row_request

    def counting(cas_root, action_key):
        reads.append(action_key)
        return real(cas_root, action_key)

    monkeypatch.setattr(manifest_promotion, "row_request", counting)
    first = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, plain)])
    assert reads and reads[-1] == plain
    reads.clear()

    second = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, plain)])

    assert second == first
    assert reads == []          # the decision is remembered, not re-read
    record = fleet.queue.prewarm(plain)
    assert record is None       # a no_manifest row receipts nothing at all

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

    def decision(outcome):
        return {name: value for name, value in outcome.items()
                if name != "fresh"}

    assert [decision(o) for o in second] == [decision(o) for o in first]
    assert reads == []          # the decision is remembered, not re-read
    record = fleet.queue.prewarm(plain)
    assert record is None       # a no_manifest row receipts nothing at all


# --- review round 2 (#1252): the memo and the examination budget ---

def test_an_unreadable_request_is_not_a_permanent_decision(tmp_path):
    """REVIEW-1252-r2 item A: a read failure is not a no-manifest fact.

    One unreadable request passes the row over this cycle without remembering
    anything; the row is planned the moment its request reads again.
    """

    import os
    fleet = Fleet(tmp_path)
    key = _manifest_row(fleet, "row-flaky",
                        annotations={"phases": phase_table(NAMED_FILES)})
    request = fleet.cas_root / "requests" / key[:2] / f"{key}.json"
    mode = request.stat().st_mode
    os.chmod(request, 0)

    first = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, key)])
    os.chmod(request, mode)
    second = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, key)])

    assert first[0]["outcome"] == "unreadable"
    assert second[0]["outcome"] == "planned", second[0]


def test_remembered_rows_do_not_spend_the_examination_budget(tmp_path):
    """REVIEW-1252-r2 item B: the budget bounds reads, not lookups.

    A hundred remembered plain rows ahead of a manifest row: the manifest row
    is still planned in the same cycle, because a memo answer costs no read.
    """

    fleet = Fleet(tmp_path)
    files = [fleet.file(name, size) for name, size in NAMED_FILES]
    plain = [fleet.action(f"r2-plain-{index:03d}", files, with_manifest=False)
             for index in range(100)]
    annotations = {"phases": phase_table(NAMED_FILES)}
    manifest_rows = [
        _manifest_row(fleet, f"r2-mrow-{index:03d}", annotations=annotations,
                      files=[fleet.file(f"r2-{index}-{name}", size)
                             for name, size in NAMED_FILES])
        for index in range(2)]

    # First cycle: only the plain rows are offered, so the planner spends its
    # reads on them and remembers every answer.
    manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, key) for key in plain])

    # Second cycle: the remembered hundred cost nothing, so the budget still
    # reaches the manifest row behind them.
    second = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, key)
               for key in plain + manifest_rows])

    assert second[-1]["outcome"] == "planned", second[-1]


def test_priority_zero_is_not_minus_ten(tmp_path):
    """REVIEW-1252-r2 item C: 0 is a band, not a missing value."""

    fleet = Fleet(tmp_path)
    key = _manifest_row(fleet, "row-zero",
                        annotations={"phases": phase_table(NAMED_FILES)},
                        priority=0)

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, key)])

    assert outcomes[0]["outcome"] == "planned", outcomes[0]
    plan = residency_plan.read(fleet.queue, key)
    assert plan is not None
    for phase in plan["phases"]:
        assert int(phase["mover_row"]["priority"]) == 0


# --- review round 3 (#1252): event churn, receipt gating, unreadable ---


def _driven_cycle(fleet, tmp_path, stage_root):
    """One real tier_loop cycle over a stub stage tier (never the live one)."""

    import tier_loop
    stage = tmp_path / "stage"
    stage.mkdir(exist_ok=True)

    def discover(**_kwargs):
        return {_stage_tier(fleet)["tier_id"]: {
            "schema": "prismabuild.storage_tier.v1",
            "tier_id": _stage_tier(fleet)["tier_id"],
            "host": HOST, "tier": "stage",
            "mountpoint": str(stage),
            "capacity_bytes": 8 * (1 << 30)}}

    tier_loop.cycle(fleet.queue, host=HOST, source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=discover)


def test_remembered_rows_emit_one_summary_line_per_cycle(tmp_path):
    """REVIEW-1252-r3 [P2]: replays are summarized, not re-emitted.

    150 remembered no-manifest rows, two driven tier-loop cycles: at most one
    host-event line per cycle for them -- the summary -- counted in the real
    event file the cycle appends to.
    """

    fleet = Fleet(tmp_path)
    files = [fleet.file(name, size) for name, size in NAMED_FILES]
    keys = [fleet.action(f"r3-quiet-{index:03d}", files, with_manifest=False)
            for index in range(150)]
    # Remember every answer, outside the cycles being counted: the
    # examination cap reads 64 a call, so loop until nothing new is read.
    ready = [_ready_item(fleet, key) for key in keys]
    while True:
        reads = manifest_promotion.promote_ready_manifest_rows(
            fleet.queue, fleet.cas_root, _stage_tier(fleet), ready=ready)
        if not any(outcome.get("fresh") for outcome in reads):
            break

    events = fleet.queue.root / "residency-events" / "_host" / f"{HOST}.jsonl"
    _driven_cycle(fleet, tmp_path, None)
    first = (events.read_text().splitlines()
             if events.exists() else [])
    _driven_cycle(fleet, tmp_path, None)
    second = (events.read_text().splitlines()
              if events.exists() else [])

    def fresh_lines(lines):
        return [line for line in lines
                if '"manifest-row-promotion"' in line]

    def summary_lines(lines):
        return [line for line in lines
                if "manifest-row-promotion-summary" in line]

    # A replayed answer is not an event: no fresh line, exactly one summary,
    # and an identical cycle appends nothing at all (r4 nit: the summary is
    # content-gated on its triple like the receipts are).
    assert fresh_lines(first) == [], fresh_lines(first)
    assert len(summary_lines(first)) == 1
    assert fresh_lines(second) == []
    assert len(summary_lines(second)) == len(summary_lines(first))
    summary = summary_lines(first)[-1]
    assert '"replayed": 150' in summary


def test_a_repeated_identical_refusal_writes_one_receipt(tmp_path):
    """REVIEW-1252-r3 [P3]: the receipt gate is real, not a comment."""

    import os
    import time
    fleet = Fleet(tmp_path)
    key = _manifest_row(fleet, "row-stuck",
                        annotations={"phases": phase_table(NAMED_FILES)})
    broken = _stage_tier(fleet)
    broken["mountpoint"] = "relative/and/refused"
    record = fleet.queue.root / "prewarm" / f"{key}.json"

    manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, broken,
        ready=[_ready_item(fleet, key)])
    assert record.exists()
    before = record.stat().st_mtime_ns
    content = record.read_text()
    time.sleep(0.05)

    manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, broken,
        ready=[_ready_item(fleet, key)])

    assert record.read_text() == content
    assert record.stat().st_mtime_ns == before


def test_a_movement_row_is_machinery_not_a_consumer(tmp_path):
    """REVIEW-1252-r3 follow-up: the tier's own movers are never planned.

    A mover or egress row carries its consumer's manifest as its own input,
    so judging by the declaration alone would seal a plan for one of the
    tier's own phase-0 rows -- and interfere with the lifecycle of the
    window it belongs to.
    """

    fleet = Fleet(tmp_path)
    files = [fleet.file(name, size) for name, size in NAMED_FILES]
    consumer = _manifest_row(fleet, "mv-consumer",
                             annotations={"phases": phase_table(NAMED_FILES)})
    assert manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, consumer)])[0]["outcome"] == "planned"
    plan = residency_plan.read(fleet.queue, consumer)
    lead = str(plan["phases"][0]["mover_row"]["action_key"])
    egress = str(plan["phases"][0]["egress_row"]["action_key"])

    for child in (lead, egress):
        item = _ready_item(fleet, child) if (
            fleet.queue.root / "ready" / f"{child}.json").exists() else {
            "action_key": child, "cas_root": str(fleet.cas_root),
            "priority": 0}
        outcomes = manifest_promotion.promote_ready_manifest_rows(
            fleet.queue, fleet.cas_root, _stage_tier(fleet), ready=[item])
        assert outcomes[0]["outcome"] == "no_manifest", outcomes[0]
        assert residency_plan.read(fleet.queue, child) is None


def test_the_superseded_directory_is_listed_once_per_call(tmp_path, monkeypatch):
    """REVIEW-1252-r4 [P2]: one listing answers every row's history question.

    The directory holds ~500 markers live and the loop runs every 5 s; a
    per-row listing would sort ~78k entries between two cycles.
    """

    fleet = Fleet(tmp_path)
    files = [fleet.file(name, size) for name, size in NAMED_FILES]
    keys = [fleet.action(f"r4-plain-{index}", files, with_manifest=False)
            for index in range(5)]
    listings: list[str] = []
    real_scan = pool_scan = __import__("prismabuild.pool", fromlist=["_scan"])._scan

    def counting(directory):
        listings.append(str(directory))
        return real_scan(directory)

    monkeypatch.setattr(
        __import__("prismabuild.pool", fromlist=["_scan"]), "_scan", counting)

    outcomes = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, _stage_tier(fleet),
        ready=[_ready_item(fleet, key) for key in keys])

    assert {outcome["outcome"] for outcome in outcomes} == {"no_manifest"}
    superseded = [name for name in listings if name.endswith("superseded")]
    assert len(superseded) == 1, superseded

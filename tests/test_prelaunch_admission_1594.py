"""Prelaunch admission: the shape check and the election deferral (#1594).

R4 refuses a consumer whose sealed data manifest declares a prelaunch
prefix while its filed plan does not carry it. R3 defers a gang
member's election while a sibling's verdict is unresolved, for gangs
with a declared prefix only. Tests use the two-host fixture from
``test_gang_reservation_1517`` with real sealed actions and filed plans.
"""
from __future__ import annotations

import json
import secrets
import sys
from pathlib import Path

from test_gang_reservation_1517 import HOSTS, gang_fleet  # noqa: F401

from prismabuild import _gang, core as pb, pool, residency_map, residency_plan

TIER = "prismabuild-stage:sparklina"
STAGE_KIND = f"stage_gib@{TIER}"
GIB = 1 << 30


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _manifest_doc(*, declare: bool, tag: str) -> dict[str, object]:
    """One v1 manifest with three phases, optionally declaring phase 0."""
    total = 0
    phases = []
    for index in range(3):
        total += 2 * GIB
        phase: dict[str, object] = {
            "name": f"phase-{index}", "bytes": 2 * GIB,
            "cumulative_bytes": total}
        if declare and index == 0:
            phase["resident_before_launch"] = True
        phases.append(phase)
    return {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": f"tests-1594-{tag}"},
        "annotations": {"phases": phases},
        "mount_prefix": "/data",
        "entries": [
            {"path": f"/data/{tag}-blob-{index}", "offset": 0,
             "bytes": 2 * GIB, "sha256": None}
            for index in range(3)],
        "entry_count": 3, "total_bytes": 6 * GIB}


def _seal_member(cas, checkout: Path, queue, group: str, index: int,
                 name: str, *, declare_manifest: bool,
                 declare_plan: bool, tag: str) -> tuple[str, str]:
    """Seal and publish one planner row with a manifest input and a plan.

    The row carries no residency block: the plan is filed, never rewritten
    onto the row. Returns the member key and its lead key.
    """
    manifest = pb.validate_data_manifest(
        _manifest_doc(declare=declare_manifest, tag=f"{tag}-{index}"))
    blob = checkout.parent / f"{name}-{index}-manifest.json"
    blob.write_text(json.dumps(manifest))
    entry, _ = cas.ingest_input(
        blob, input_id=pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
    snap_blob = checkout.parent / f"{name}-{index}-snapshot.json"
    snap_blob.write_text("{}")
    snap_entry, _ = cas.ingest_input(
        snap_blob, input_id="pbrun.checkout-snapshot")
    gang = {"group": group, "size": 2, "index": index}
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/gang-member",
                 "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"], "working_directory": ".",
                 "result_path": f"{name}-{index}"},
        "inputs": [entry, snap_entry],
        "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {"gpu_exclusive": False, "execution_timeout_s": 3600,
                   "gang": gang,
                   "command": [sys.executable, "task.py"],
                   "cwd": str(checkout),
                   "demand": {"cpu": 2, "gpu": 1, "mem_gb": 100},
                   "placement": {"required_tags": [HOSTS[index]]},
                   "retry_policy": {"max_attempts": 1},
                   "data_manifest": {
                       "input": entry,
                       "mount_prefix": manifest["mount_prefix"],
                       "entry_count": manifest["entry_count"],
                       "total_bytes": manifest["total_bytes"]},
                   "checkout_snapshot": {
                       "schema": "prismaquant.prismabuild.pbrun_checkout_snapshot.v2",
                       "commit": "0" * 40, "input": snap_entry,
                       "parent": "0" * 40, "refs": {},
                       "subdirectory": "."}},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas.publish_action_request(action)
    key = action["action_key"]
    lead = _hexkey(f"{name}-{index}-lead")
    queue.publish(action_key=key, cas_root=str(cas.root),
                  checkout_root=str(checkout), worker_script="worker.py",
                  resources={"cpu": 2, "gpu": 1, "mem_gb": 100},
                  needs_gpu=True, tags=[HOSTS[index]], priority=0,
                  gang=gang, max_attempts=1)
    phase: dict[str, object] = {
        "name": "phase-0", "start_bytes": 0, "end_bytes": 2 * GIB,
        "mover_row": {"action_key": lead,
                      "cas_root": str(queue.root / "cas"),
                      "checkout_root": str(queue.root / "co"),
                      "worker_script": str(queue.root / "worker.py"),
                      "tags": ["sparklina"],
                      "resources": {"cpu": 1, "mem_gb": 1, STAGE_KIND: 2},
                      "residency": {"schema": pool.RESIDENCY_SCHEMA_V1,
                                    "tier_id": TIER,
                                    "manifest_sha256": entry["sha256"],
                                    "manifest_bytes": 6 * GIB,
                                    "range_start_bytes": 0,
                                    "range_end_bytes": 2 * GIB}},
        "egress_row": {"action_key": _hexkey(f"{name}-{index}-egress"),
                       "cas_root": str(queue.root / "cas"),
                       "checkout_root": str(queue.root / "co"),
                       "worker_script": str(queue.root / "worker.py"),
                       "tags": ["sparklina"],
                       "resources": {"mem_gb": 1}},
    }
    if declare_plan:
        phase["resident_before_launch"] = True
    residency_plan.freeze(queue, residency_plan.build_plan(
        consumer_action_key=key, tier_id=TIER, stage_root="/stage/tests",
        manifest_sha256=str(entry["sha256"]), manifest_bytes=6 * GIB,
        phases=[phase]))
    return key, lead


def _file_map(queue, key: str) -> Path:
    """File an empty composed map for one consumer; return its path."""
    path = queue.residency_map_path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}\n")
    return path


def _compose_maps(monkeypatch, queue, table: dict[str, list[str]]) -> None:
    """File one composed map per consumer; reads dispatch by path."""
    paths = {str(_file_map(queue, key)): list(leads)
             for key, leads in table.items()}
    monkeypatch.setattr(residency_map, "read_map",
                        lambda p: {"leads": paths[str(p)]})


def _compose_map(monkeypatch, queue, key: str, leads: list[str]) -> None:
    """File a composed map naming the given leads."""
    _compose_maps(monkeypatch, queue, {key: leads})


def _gang_pair(gang_fleet, tmp_path, name, *, declare_manifest: bool,
               declare_plan: bool) -> tuple:
    """Publish a two-member prelaunch gang; file the group record."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    cas = pb.PrismaBuildCAS(tmp_path / f"{name}-cas")
    checkout = tmp_path / f"{name}-checkout"
    group = secrets.token_hex(16)
    keys = []
    leads = []
    for index in range(2):
        clock[0] += 0.001
        key, lead = _seal_member(
            cas, checkout, queue, group, index, name,
            declare_manifest=declare_manifest, declare_plan=declare_plan,
            tag=name)
        keys.append(key)
        leads.append(lead)
    rows = [pool._read_json(queue.item_path(pool.READY, key)) for key in keys]
    _gang.publish_group(queue, group, rows)
    return queue, clock, publish, finish, gclaim, denial, group, keys, leads


def _publish_lead(queue, clock, lead: str, sha: str) -> None:
    """Publish one movement node for the plan's phase 0."""
    clock[0] += 0.001
    queue.publish(action_key=lead, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root / "co"),
                  worker_script=str(queue.root / "worker.py"),
                  resources={"cpu": 1, "mem_gb": 1, STAGE_KIND: 2},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "tier_id": TIER, "manifest_sha256": sha,
                             "manifest_bytes": 6 * GIB,
                             "range_start_bytes": 0,
                             "range_end_bytes": 2 * GIB},
                  max_attempts=1, retry_safe=False, tags=["sparklina"])


def _stage_lead(queue, monkeypatch, lead: str, consumer: str, sha: str,
                stage: Path) -> None:
    """Claim one mover and finish it executed and pinned."""
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
    claimed = queue.claim(capacity={"cpu": 8, "mem_gb": 16},
                          tags=["sparklina"])
    assert claimed is not None and claimed["action_key"] == lead, claimed
    queue.record_move(lead, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": sha,
        "range_start_bytes": 0, "range_end_bytes": 2 * GIB,
        "bytes_staged": 2 * GIB, "complete": True})
    queue.finish(lead, status="executed")


def _consumer_sha(queue, key: str) -> str:
    """The manifest digest the consumer's filed plan names."""
    plan = residency_plan.read(queue, key)
    assert plan is not None
    return str(plan["manifest_sha256"])


def _delete_manifest_blob(row: dict[str, object], key: str) -> None:
    """Delete the sealed manifest blob one consumer's request names."""
    cas = pb.PrismaBuildCAS(Path(str(row["cas_root"])))
    request = json.loads(
        (Path(str(row["cas_root"])) / "requests" / key[:2]
         / f"{key}.json").read_bytes())
    manifest_entry = next(
        entry for entry in request["inputs"]
        if entry["id"] == pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
    cas.input_path(manifest_entry).unlink()


# ------------------------------------------------------- R4: the shape check


def test_declared_manifest_with_streaming_plan_is_refused(
        gang_fleet, tmp_path):
    """R4: a declared manifest with an undeclared plan refuses pre-token."""
    queue, clock, publish, finish, gclaim, denial, group, keys, leads = \
        _gang_pair(gang_fleet, tmp_path, "shape-refused",
                   declare_manifest=True, declare_plan=False)
    row = pool._read_json(queue.item_path(pool.READY, keys[0]))
    verdict = queue.residency_verdict(row)
    assert verdict["state"] == "prelaunch_undeclared", verdict
    assert verdict["manifest_declares"] == ["phase-0"], verdict
    assert verdict["plan_declares"] == [], verdict


def test_matching_declaration_passes_shape_check(gang_fleet, tmp_path):
    """R4: equal manifest and plan declarations reach the normal wait."""
    queue, clock, publish, finish, gclaim, denial, group, keys, leads = \
        _gang_pair(gang_fleet, tmp_path, "shape-match",
                   declare_manifest=True, declare_plan=True)
    row = pool._read_json(queue.item_path(pool.READY, keys[0]))
    verdict = queue.residency_verdict(row)
    assert verdict["state"] == "lead_not_resident", verdict
    assert verdict["pending"] == [{"lead": leads[0], "status": "absent"}]


def test_no_declaration_keeps_todays_verdict(gang_fleet, tmp_path):
    """R4: no declaration on either side changes nothing observable."""
    queue, clock, publish, finish, gclaim, denial, group, keys, leads = \
        _gang_pair(gang_fleet, tmp_path, "shape-clean",
                   declare_manifest=False, declare_plan=False)
    row = pool._read_json(queue.item_path(pool.READY, keys[0]))
    verdict = queue.residency_verdict(row)
    assert verdict["state"] == "lead_not_resident", verdict
    assert verdict["pending"] == [{"lead": leads[0], "status": "absent"}]
    text = Path(queue.residency_plan_path(keys[0])).read_text()
    assert "resident_before_launch" not in text


def test_unreadable_manifest_is_not_a_pass(gang_fleet, tmp_path):
    """R4: a manifest blob that cannot be read refuses; never admits."""
    queue, clock, publish, finish, gclaim, denial, group, keys, leads = \
        _gang_pair(gang_fleet, tmp_path, "shape-unreadable",
                   declare_manifest=True, declare_plan=True)
    row = pool._read_json(queue.item_path(pool.READY, keys[0]))
    _delete_manifest_blob(row, keys[0])
    verdict = queue.residency_verdict(row)
    assert verdict["state"] in pool.RESIDENCY_REFUSAL_STATES, verdict
    assert verdict["state"] != "resident", verdict


def test_manifest_parses_once_per_blob(gang_fleet, tmp_path, monkeypatch):
    """R4: repeated verdicts over one manifest blob read it once."""
    queue, clock, publish, finish, gclaim, denial, group, keys, leads = \
        _gang_pair(gang_fleet, tmp_path, "shape-memo",
                   declare_manifest=True, declare_plan=True)
    row = pool._read_json(queue.item_path(pool.READY, keys[0]))
    calls = []
    original = pb.read_data_manifest

    def counting(path):
        calls.append(str(path))
        return original(path)

    monkeypatch.setattr(pb, "read_data_manifest", counting)
    assert queue.residency_verdict(row)["state"] == "lead_not_resident"
    assert queue.residency_verdict(row)["state"] == "lead_not_resident"
    assert len(calls) == 1, calls


# ----------------------------------------------- R3: the election deferral


def test_asymmetric_prelaunch_gang_defers_election(
        gang_fleet, monkeypatch, tmp_path):
    """R3: one non-resident sibling defers the election; no file lands."""
    queue, clock, publish, finish, gclaim, denial, group, keys, leads = \
        _gang_pair(gang_fleet, tmp_path, "deferral",
                   declare_manifest=True, declare_plan=True)
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    stage = tmp_path / "stage"
    stage.mkdir()
    first, second = keys
    _publish_lead(queue, clock, leads[0], _consumer_sha(queue, first))
    _stage_lead(queue, monkeypatch, leads[0], first,
                _consumer_sha(queue, first), stage)
    _compose_map(monkeypatch, queue, first, [leads[0]])

    assert gclaim("sparklina") is None
    waiting = denial(first, "sparklina")
    assert waiting["reason"] == "deferred_for_gang_prelaunch", waiting
    assert waiting["evidence"]["sibling_index"] == 1, waiting
    assert waiting["evidence"]["sibling_state"] == "ready", waiting
    assert waiting["evidence"]["sibling_verdict"] == "lead_not_resident"
    assert 0 not in _gang.elections(queue, group, 2)


def test_all_resident_prelaunch_gang_elects(
        gang_fleet, monkeypatch, tmp_path):
    """R3: resident siblings elect and claim on both hosts."""
    queue, clock, publish, finish, gclaim, denial, group, keys, leads = \
        _gang_pair(gang_fleet, tmp_path, "elected",
                   declare_manifest=True, declare_plan=True)
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    stage = tmp_path / "stage"
    stage.mkdir()
    for index, key in enumerate(keys):
        _publish_lead(queue, clock, leads[index], _consumer_sha(queue, key))
        _stage_lead(queue, monkeypatch, leads[index], key,
                    _consumer_sha(queue, key), stage)
    _compose_maps(monkeypatch, queue,
                  {key: [leads[index]] for index, key in enumerate(keys)})

    assert gclaim("sparklina") is None
    assert gclaim("sparky") == keys[1], denial(keys[1], "sparky")
    assert gclaim("sparklina") == keys[0], denial(keys[0], "sparklina")
    elections = _gang.elections(queue, group, 2)
    assert elections[0]["host"] == "sparklina"
    assert elections[1]["host"] == "sparky"


def test_unreadable_sibling_evidence_defers(
        gang_fleet, monkeypatch, tmp_path):
    """R3: a sibling whose manifest cannot be read defers the election."""
    queue, clock, publish, finish, gclaim, denial, group, keys, leads = \
        _gang_pair(gang_fleet, tmp_path, "sibling-unreadable",
                   declare_manifest=True, declare_plan=True)
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    stage = tmp_path / "stage"
    stage.mkdir()
    for index, key in enumerate(keys):
        _publish_lead(queue, clock, leads[index], _consumer_sha(queue, key))
        _stage_lead(queue, monkeypatch, leads[index], key,
                    _consumer_sha(queue, key), stage)
    _compose_maps(monkeypatch, queue,
                  {key: [leads[index]] for index, key in enumerate(keys)})

    first, second = keys
    sibling_row = pool._read_json(queue.item_path(pool.READY, second))
    _delete_manifest_blob(sibling_row, second)

    assert gclaim("sparklina") is None
    waiting = denial(first, "sparklina")
    assert waiting["reason"] == "deferred_for_gang_prelaunch", waiting
    assert waiting["evidence"]["sibling_index"] == 1, waiting
    assert 0 not in _gang.elections(queue, group, 2)


def test_undeclared_gang_reads_no_manifest_and_no_sibling_verdict(
        gang_fleet, monkeypatch, tmp_path):
    """R3: an undeclared gang elects with no manifest or sibling reads."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, keys = members("undeclared-1594", declares_manifest=True)
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    record = _gang.read_group(queue, group)
    assert record is not None

    manifest_calls: list[str] = []
    original_manifest = pb.read_data_manifest

    def counting_manifest(path):
        manifest_calls.append(str(path))
        return original_manifest(path)

    verdict_calls: list[str] = []
    original_verdict = queue.residency_verdict

    def counting_verdict(item):
        verdict_calls.append(str(item.get("action_key")))
        return original_verdict(item)

    monkeypatch.setattr(pb, "read_data_manifest", counting_manifest)
    monkeypatch.setattr(queue, "residency_verdict", counting_verdict)
    assert queue._gang_prelaunch_blocker(
        record, record["members"][0]) is None
    assert manifest_calls == []
    assert verdict_calls == []

    assert gclaim("sparklina") is None
    assert gclaim("sparky") == keys[1], denial(keys[1], "sparky")
    assert gclaim("sparklina") == keys[0], denial(keys[0], "sparklina")
    assert manifest_calls == [], manifest_calls


def test_election_boundary_rereads_sibling_verdict(
        gang_fleet, monkeypatch, tmp_path):
    """R3: the blocker reads the sibling verdict anew on every call."""
    queue, clock, publish, finish, gclaim, denial, group, keys, leads = \
        _gang_pair(gang_fleet, tmp_path, "boundary",
                   declare_manifest=True, declare_plan=True)
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    stage = tmp_path / "stage"
    stage.mkdir()
    for index, key in enumerate(keys):
        _publish_lead(queue, clock, leads[index], _consumer_sha(queue, key))
        _stage_lead(queue, monkeypatch, leads[index], key,
                    _consumer_sha(queue, key), stage)
    _compose_maps(monkeypatch, queue,
                  {key: [leads[index]] for index, key in enumerate(keys)})

    record = _gang.read_group(queue, group)
    assert record is not None
    assert queue._gang_prelaunch_blocker(
        record, record["members"][0]) is None

    original = queue.residency_verdict

    def flaky(item):
        if item.get("action_key") == keys[1]:
            return {"state": "lead_not_resident", "pending": [],
                    "leads": [leads[1]]}
        return original(item)

    monkeypatch.setattr(queue, "residency_verdict", flaky)
    blocker = queue._gang_prelaunch_blocker(record, record["members"][0])
    assert blocker is not None, "a fresh sibling refusal must defer"
    assert blocker["sibling_index"] == 1, blocker
    assert blocker["sibling_verdict"] == "lead_not_resident", blocker

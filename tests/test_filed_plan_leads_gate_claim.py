"""A filed first-window plan is an admission prerequisite (PB #1350).

Use the real #1332 planner fixture, not a hand-written consumer block. A
planner leaves the READY row bare; discovery and claim must read its same
filed leads. Plain/advisory rows with no filed plan remain unchanged.
"""
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from test_planner_filed_rows_are_staged import (  # noqa: E402
    Fleet, _planner_row, pool, residency_plan,
)


def _item(fleet, key):
    return json.loads(fleet.queue.item_path(pool.READY, key).read_text())


def test_bare_planner_row_waits_on_its_first_filed_lead(tmp_path):
    fleet = Fleet(tmp_path)
    key, plan = _planner_row(fleet, "gate-first-lead")
    item = _item(fleet, key)
    assert "residency" not in item
    before = fleet.queue.item_path(pool.READY, key).read_bytes()
    verdict = fleet.queue.residency_verdict(item)
    assert verdict == {
        "state": "lead_not_resident",
        "pending": [{"lead": residency_plan.leads_for(plan)[0], "status": "absent"}],
        "leads": residency_plan.leads_for(plan)}
    assert verdict["state"] in pool.RESIDENCY_REFUSAL_STATES
    assert fleet.queue.item_path(pool.READY, key).read_bytes() == before


def test_unreadable_filed_plan_is_a_denial_not_advisory(tmp_path):
    fleet = Fleet(tmp_path)
    key, _ = _planner_row(fleet, "gate-unreadable")
    path = fleet.queue.residency_plan_path(key)
    # freeze publishes a read-only seal; replace it to simulate corruption.
    path.unlink()
    path.write_text("not json\n")
    verdict = fleet.queue.residency_verdict(_item(fleet, key))
    assert verdict["state"] == "plan_unreadable"
    assert verdict["error"]
    assert verdict["state"] in pool.RESIDENCY_REFUSAL_STATES


def test_plain_row_and_advisory_receipt_without_a_plan_stay_plain(tmp_path):
    fleet = Fleet(tmp_path)
    key = fleet.action("plain-advisory", [fleet.file("plain.bin", 8)])
    fleet.queue.record_prewarm(key, {"tier": {"status": "planned"}})
    assert fleet.queue.residency_verdict(_item(fleet, key)) == {"state": "not_requested"}


def test_explicit_consumer_leads_are_not_replaced_by_a_filed_plan(tmp_path):
    fleet = Fleet(tmp_path)
    key, plan = _planner_row(fleet, "gate-explicit")
    item = _item(fleet, key)
    residency = residency_plan.consumer_residency(fleet.queue, item)[0]
    assert residency is not None
    explicit = "a" * 64
    item["residency"] = {**residency, "leads": [explicit]}
    verdict = fleet.queue.residency_verdict(item)
    assert verdict["state"] == "lead_not_resident"
    assert verdict["leads"] == [explicit]
    assert verdict["leads"] != residency_plan.leads_for(plan)


def test_claim_does_not_take_a_bare_consumer_before_its_lead(tmp_path):
    fleet = Fleet(tmp_path)
    key, _ = _planner_row(fleet, "gate-real-claim")
    claimed = fleet.queue.claim(owner="test-consumer", capacity={"cpu": 2, "mem_gb": 2})
    assert claimed is None
    assert fleet.queue.item_path(pool.READY, key).exists()
    assert not fleet.queue.item_path(pool.CLAIMED, key).exists()


def test_derived_leads_use_the_same_map_gates_and_only_the_first_phase(tmp_path, monkeypatch):
    from prismabuild import residency_map

    fleet = Fleet(tmp_path)
    key, plan = _planner_row(fleet, "gate-map")
    item = _item(fleet, key)
    block, _ = residency_plan.consumer_residency(fleet.queue, item)
    assert block is not None
    explicit = {**item, "residency": block}
    leads = residency_plan.leads_for(plan)
    assert len(leads) == 1
    monkeypatch.setattr(fleet.queue, "_lead_was_adopted", lambda *args: True)
    assert fleet.queue.residency_verdict(item) == fleet.queue.residency_verdict(explicit)
    assert fleet.queue.residency_verdict(item)["state"] == "map_not_composed"
    path = fleet.queue.residency_map_path(key)
    path.write_text("fixture map\n")
    monkeypatch.setattr(residency_map, "read_map", lambda path: {"leads": []})
    assert fleet.queue.residency_verdict(item) == fleet.queue.residency_verdict(explicit)
    assert fleet.queue.residency_verdict(item)["state"] == "map_stale"
    monkeypatch.setattr(residency_map, "read_map", lambda path: {"leads": leads})
    assert fleet.queue.residency_verdict(item) == fleet.queue.residency_verdict(explicit)
    assert fleet.queue.residency_verdict(item)["state"] == "resident"
    # Later phase movers remain absent: only the published first phase gates.
    for phase in plan["phases"][1:]:
        assert not fleet.queue.item_path(pool.DONE, phase["mover_row"]["action_key"]).exists()


def test_a_movers_range_block_is_not_turned_into_a_consumer(tmp_path):
    fleet = Fleet(tmp_path)
    key, plan = _planner_row(fleet, "gate-range")
    item = _item(fleet, key)
    item["residency"] = plan["phases"][0]["mover_row"]["residency"]
    assert fleet.queue.residency_verdict(item) == {"state": "no_leads"}


def test_scoped_acceptance_record_has_no_deployment_or_whole_contract_claim():
    root = Path(__file__).resolve().parents[1]
    record = json.loads((root / "docs/evidence/issue1350_filed_leads_merge_acceptance_2026-09-29.json").read_text())
    assert record["schema"] == "pb.staged_read_acceptance.v1"
    assert record["step"] == "merge"
    assert set(record) == {"schema", "step", "contract_commit", "ledger_commit",
                           "scope", "prelaunch", "merge", "deploy", "complete", "exceptions"}
    assert record["scope"] == ["INV-02", "SC-01", "SC-02"]
    assert {repair["id"] for repair in record["merge"]["validated_repairs"]} == set(record["scope"])
    assert record["merge"]["issue"] == "https://github.com/RobTand/prismabuild/issues/1350"
    for step in ("prelaunch", "deploy", "complete"):
        assert record[step] == {"status": "not-this-step"}
    assert record["exceptions"] == []
    assert any("deployment axis: pending" in gap for gap in record["merge"]["remaining_gaps"])

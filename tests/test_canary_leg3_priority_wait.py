"""A feasible priority wait may outlast leg 3's observation interval (#1772)."""

import json
from types import SimpleNamespace

import pbcanary
from prismabuild.tier_holdings import Unspent
import test_stalled_leader_2060_contract as probe


def test_leg3_keeps_observing_its_action_until_higher_priority_work_drains(
        tmp_path, monkeypatch):
    queue, stage, leader_mover, canary_mover = probe.probe_queue(tmp_path)
    elapsed = [0.0]
    submissions = []
    observations = []
    receipt = {"returncode": 0}
    payload = b"fixture staged read completed"
    receipt_path = tmp_path / "cas" / "receipt.json"
    leg_dir = tmp_path / "canary"
    leg_dir.mkdir()

    monkeypatch.setattr(pbcanary, "time", SimpleNamespace(
        monotonic=lambda: elapsed[0]))

    def submit(*args, **kwargs):
        submissions.append(probe.CANARY)
        return probe.CANARY, {"action_key": probe.CANARY,
                              "published_unix": 1234.5}

    def wait(paths, leg, action_key, wait_s):
        assert action_key == probe.CANARY
        elapsed[0] += wait_s
        observations.append(elapsed[0])
        if len(observations) == 1:
            probe.run_tier_cycle(queue, stage, gib=20)
            assert not probe.ready(queue, leader_mover)
            assert not probe.ready(queue, canary_mover)
            events = [json.loads(line) for line in (
                queue.root / "residency-events" / probe.CANARY / "dl380g10.jsonl"
            ).read_text().splitlines()]
            assert any(e.get("reason") == "higher-priority-window-waiting"
                       and e["waiting_on"] == {
                           "consumer": probe.LEADER, "reason": "joint-fit-stall"}
                       for e in events)
            return {"returncode": 75, "record": {}, "stdout": "",
                    "stderr": "observation budget expired"}

        assert elapsed[0] > 900
        queue.tier(probe.base.TIER).release_unspent(
            probe.STATIC, authority=Unspent.NEVER_STARTED)
        queue.withdraw(probe.LEADER, by="test:1772",
                       reason="fixture workload ends", signal_child=False)
        probe.run_tier_cycle(queue, stage, gib=20)
        assert probe.ready(queue, canary_mover)
        return {"returncode": 0, "record": {}, "stdout": "", "stderr": ""}

    def load_receipt(paths, action_key):
        assert probe.ready(queue, canary_mover)
        return receipt, receipt_path, payload

    monkeypatch.setattr(pbcanary, "submit_leg", submit)
    monkeypatch.setattr(pbcanary, "wait_leg", wait)
    monkeypatch.setattr(pbcanary, "load_verified_receipt", load_receipt)
    monkeypatch.setattr(pbcanary, "terminal_progress_observation",
                        lambda *args, **kwargs: None)

    envelope, receipt_ref, blob, action_key = pbcanary._execute_side(
        {}, leg="leg-3", spec={"name": "leg-3"}, argv=None,
        checkout=tmp_path, run_id="priority-wait", generation=None,
        priority=-10, fleet_root=tmp_path, leg_dir=leg_dir, side=None,
        extra_flags=[], extra_env={}, manifest=None, wait_s=900)

    assert submissions == [probe.CANARY]
    assert observations == [900, 1800]
    assert probe.ready(queue, canary_mover)
    assert action_key == probe.CANARY
    assert envelope["receipt"] == receipt
    assert receipt_ref == "cas/receipt.json"
    assert blob == payload

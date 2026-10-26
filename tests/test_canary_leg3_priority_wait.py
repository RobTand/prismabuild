"""A feasible priority wait may outlast leg 3's observation interval (#1772)."""

import json
from types import SimpleNamespace

import pbcanary
import pytest
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
        assert json.loads((leg_dir / "pbwait.json").read_text())["returncode"] == 75
        queue.tier(probe.base.TIER).release_unspent(
            probe.STATIC, authority=Unspent.NEVER_STARTED)
        probe.run_tier_cycle(queue, stage, gib=20)
        assert probe.ready(queue, leader_mover)
        assert not probe.ready(queue, canary_mover)
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

    _, receipt_ref, _, action_key = pbcanary._execute_side(
        {}, leg="leg-3", spec={"name": "leg-3"}, argv=None,
        checkout=tmp_path, run_id="priority-wait", generation=None,
        priority=-10, fleet_root=tmp_path, leg_dir=leg_dir, side=None,
        extra_flags=[], extra_env={}, manifest=None, wait_s=900)

    assert submissions == [probe.CANARY]
    assert observations == [900, 1800]
    assert probe.ready(queue, canary_mover)
    assert action_key == probe.CANARY
    assert receipt_ref == "cas/receipt.json"


def test_explicit_leg3_budget_is_one_deadline_across_observations(tmp_path, monkeypatch):
    elapsed = [0.0]
    observations = []
    monkeypatch.setattr(pbcanary, "time", SimpleNamespace(
        monotonic=lambda: elapsed[0]))

    def wait(paths, leg, action_key, wait_s, *, timeout_s):
        observations.append((wait_s, timeout_s))
        elapsed[0] += wait_s
        return {"returncode": 75, "record": {}, "stdout": "", "stderr": "expired"}

    monkeypatch.setattr(pbcanary, "wait_leg", wait)
    pbcanary._wait_for_side(
        {}, leg="leg-3", label="leg-3", action_key=probe.CANARY,
        wait_s=900, leg_dir=tmp_path, file_tag="", observation_wait_s=1200)

    assert observations == [(900, 1200), (300, 300)]
    assert elapsed[0] == 1200


def test_explicit_leg3_budget_caps_the_observer_process(tmp_path, monkeypatch):
    elapsed = [0.0]
    timeouts = []
    monkeypatch.setattr(pbcanary, "time", SimpleNamespace(
        monotonic=lambda: elapsed[0]))

    def process(argv, *, timeout_s):
        timeouts.append(timeout_s)
        elapsed[0] += timeout_s
        raise pbcanary.subprocess.TimeoutExpired(cmd=argv, timeout=timeout_s)

    monkeypatch.setattr(pbcanary, "run_process", process)
    with pytest.raises(pbcanary._SideUnverified) as error:
        pbcanary._wait_for_side(
            {"pbwait": tmp_path / "pbwait.py"}, leg="leg-3", label="leg-3",
            action_key=probe.CANARY, wait_s=900, leg_dir=tmp_path, file_tag="",
            observation_wait_s=100)
    assert error.value.reason == "wait_budget_exhausted"
    assert timeouts == [100]


def test_expired_leg3_budget_starts_no_observation(tmp_path, monkeypatch):
    clock = iter([0.0, 2.0])
    monkeypatch.setattr(pbcanary, "time", SimpleNamespace(
        monotonic=lambda: next(clock)))

    def unexpected_wait(*args, **kwargs):
        pytest.fail("expired observation budget started a new read")

    monkeypatch.setattr(pbcanary, "wait_leg", unexpected_wait)
    with pytest.raises(pbcanary._SideUnverified) as error:
        pbcanary._wait_for_side(
            {}, leg="leg-3", label="leg-3", action_key=probe.CANARY,
            wait_s=900, leg_dir=tmp_path, file_tag="", observation_wait_s=1)
    assert error.value.reason == "wait_budget_exhausted"


@pytest.mark.parametrize("leg,returncode,staging,error_type,reason", [
    ("leg-3", 74, None, pbcanary._SideUnverified, "outcome_unobserved"),
    ("leg-3", 1, False, pbcanary._SideFailed, None),
    ("leg-3", 1, True, pbcanary._SideUnverified, "staging_wait"),
    ("leg-3", 1, None, pbcanary._SideUnverified, "outcome_unobserved"),
    ("leg-1", 75, None, pbcanary._SideUnverified, "wait_budget_exhausted"),
    ("leg-2", 75, None, pbcanary._SideUnverified, "wait_budget_exhausted"),
    ("leg-4", 75, None, pbcanary._SideUnverified, "wait_budget_exhausted"),
])
def test_only_leg3_observation_expiry_continues(
        tmp_path, monkeypatch, leg, returncode, staging, error_type, reason):
    observations = []
    monkeypatch.setattr(pbcanary, "submit_leg", lambda *a, **k: (
        probe.CANARY, {"published_unix": 1234.5}))

    def wait(*args, **kwargs):
        observations.append(args[2])
        return {"returncode": returncode, "record": {}, "stdout": "", "stderr": "fixture"}

    monkeypatch.setattr(pbcanary, "wait_leg", wait)
    monkeypatch.setattr(pbcanary, "staging_wait_evidence", lambda *a: staging)
    with pytest.raises(error_type) as error:
        pbcanary._execute_side(
            {}, leg=leg, spec={"name": leg}, argv=None, checkout=tmp_path,
            run_id="bounded-control", generation=None, priority=-10,
            fleet_root=tmp_path, leg_dir=tmp_path, side=None,
            extra_flags=[], extra_env={}, manifest=None, wait_s=900)
    assert observations == [probe.CANARY]
    if reason is not None:
        assert error.value.reason == reason
        assert error.value.action_key == probe.CANARY


@pytest.mark.parametrize("value", ["0", "-1", "inf", "nan"])
def test_cli_refuses_nonpositive_or_nonfinite_leg3_budget(value):
    with pytest.raises(SystemExit) as error:
        pbcanary.main(["--leg3-wait-s", value])
    assert error.value.code == 2


def test_cli_preserves_fractional_leg3_budget(monkeypatch):
    budgets = []

    def run(args):
        budgets.append(args.leg3_wait_s)
        return 0

    monkeypatch.setattr(pbcanary, "run_canary", run)
    pbcanary.main(["--leg3-wait-s", "1.5"])
    assert budgets == [1.5]

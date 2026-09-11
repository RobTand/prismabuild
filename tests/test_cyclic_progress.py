"""Repeated long phases retain their allowance without granting endless quiet."""
import itertools
import json
from pathlib import Path

import pytest

from test_progress_keeps_a_working_action_alive import pb, pool, pbrun, _claimed


def _policy(cycle=True):
    return pbrun.parse_progress_phases(["encode=200", "publish=5"], cycle=cycle)


def _watch(tmp_path, *, cycle=True, phases=None):
    policy = pool.ProgressPolicy(
        phases or (pool.ProgressPhase("encode", 200, None),
                   pool.ProgressPhase("publish", 5, None)), None, cycle=cycle)
    return pool.ProgressWatch(tmp_path / "progress", "launch", policy, started=0)


def _report(watch, now, phase, units, **changes):
    watch.path.write_text(json.dumps({
        "schema": pb.PROGRESS_RECORD_SCHEMA_V1, "token": "launch",
        "phase": phase, "units_completed": units, **changes,
    }))
    return watch.sample(now=now)


@pytest.mark.parametrize("cycle", [True, False])
def test_publish_then_encode_uses_the_encode_allowance(tmp_path, cycle):
    watch = _watch(tmp_path, cycle=cycle)
    assert _report(watch, 1, "encode", 1)
    assert _report(watch, 2, "publish", 2)
    assert _report(watch, 3, "encode", 2) is cycle
    assert watch.grace_s == (200 if cycle else 5), watch.as_record(now=3)
    assert watch.stall_deadline() == (203 if cycle else 7)


def test_many_cycles_keep_cumulative_units_and_restore_each_allowance(tmp_path):
    watch = _watch(tmp_path)
    for cycle in range(20):
        now = cycle * 210 + 1
        assert _report(watch, now, "publish", cycle + 1)
        assert watch.grace_s == 5
        assert _report(watch, now + 1, "encode", cycle + 1)
        assert watch.grace_s == 200
        assert watch.stall_deadline() == now + 201
    observed = watch.as_record(now=now + 150)
    assert observed["phase"] == "encode"
    assert observed["last_accepted"]["units_completed"] == 20
    assert observed["quiet_s"] == 149


@pytest.mark.parametrize("units", [0, 1, 2**53 + 1])
def test_phase_only_reports_cannot_exceed_sum_of_allowances(tmp_path, units):
    # All permutations include skipped phases and backwards reports. Repeating
    # any order at an unchanged count must exhaust the same finite allowance.
    phases = tuple(pool.ProgressPhase(name, grace, None)
                   for name, grace in zip(("load", "encode", "publish"), (20, 200, 5)))
    for order in itertools.permutations(("load", "encode", "publish")):
        watch = _watch(tmp_path, phases=phases)
        _report(watch, 0, "load", units)
        for name in order * 3:
            now = watch.stall_deadline() - 0.01
            _report(watch, now, name, units)
            assert watch.stall_deadline() <= watch.policy.no_progress_bound_s
        assert watch.accepted <= 3
        assert watch.last_rejection == "replayed"


@pytest.mark.parametrize("units", [1, 0, -1, True, float("inf")])
def test_invalid_or_regressing_count_cannot_change_phase_or_grace(tmp_path, units):
    watch = _watch(tmp_path)
    assert _report(watch, 1, "publish", 2)
    assert not _report(watch, 2, "encode", units)
    assert watch.grace_s == 5
    assert watch.stall_deadline() == 6


def test_foreign_or_undeclared_report_cannot_spend_or_renew_a_phase(tmp_path):
    watch = _watch(tmp_path)
    assert _report(watch, 1, "publish", 2)
    assert not _report(watch, 2, "encode", 3, token="other")
    assert not _report(watch, 2, "unknown", 3)
    assert watch.stall_deadline() == 6
    assert _report(watch, 3, "encode", 2)
    assert watch.stall_deadline() == 203


def test_single_phase_requires_new_units_to_renew(tmp_path):
    watch = _watch(tmp_path, phases=(pool.ProgressPhase("encode", 200, None),))
    assert not _report(watch, 1, "encode", 0)
    assert _report(watch, 2, "encode", 2**53)
    assert not _report(watch, 3, "encode", 2**53)
    assert _report(watch, 4, "encode", 2**53 + 1)
    assert watch.stall_deadline() == 204


@pytest.mark.parametrize("cycle", [None, 0, 1, "true", [], {}])
def test_cycle_policy_requires_a_boolean(cycle):
    with pytest.raises(pb.ActionContractError, match="cycle must be a boolean"):
        pb.validate_progress_policy({**_policy(False), "cycle": cycle})


def test_cycle_false_preserves_linear_policy_bytes():
    assert pb.validate_progress_policy({**_policy(False), "cycle": False}) == _policy(False)
    assert _policy()["cycle"] is True
    assert pb.validate_progress_policy(_policy()) == _policy()


def test_cycle_without_phases_is_refused_before_submission():
    with pytest.raises(SystemExit, match="requires --progress-phase"):
        pbrun.parse_progress_phases(None, cycle=True)


def _announce(queue, host, *, cycle, ceiling=7200):
    tags = ["x86", host, pb.PROGRESS_TAG, pb.PROGRESS_HELPER_TAG]
    if cycle:
        tags.append(pb.PROGRESS_CYCLE_TAG)
    queue.announce(host=host, tags=tags, has_gpu=False,
                   capacity={"cpu": 4, "mem_gb": 16, "gpu": 0},
                   timeout_ceiling_s=ceiling,
                   progress_contracts=[pb.PROGRESS_RECORD_SCHEMA_V1])


@pytest.mark.parametrize("old_offer", [False, True])
def test_cyclic_submission_requires_known_capable_workers(tmp_path, old_offer):
    queue = pool.PoolQueue(tmp_path / "queue")
    if old_offer:
        _announce(queue, "old", cycle=False)
    with pytest.raises(SystemExit, match="no eligible worker offers progress-cycle-v1"):
        pbrun.progress_contract_notice(queue, {"tags": ["x86"]}, policy=_policy())


def test_cyclic_notice_excludes_old_worker_ceiling(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    _announce(queue, "old", cycle=False, ceiling=1)
    _announce(queue, "new", cycle=True, ceiling=100)
    notice = pbrun.progress_contract_notice(queue, {"tags": ["x86"]}, policy=_policy())
    assert "old do not offer progress-cycle-v1" in notice
    assert "limits encode grace to 100s" in notice
    assert "limits encode grace to 1s" not in notice
    assert "205s of quiet after the count stops increasing" in notice


def test_cycle_flag_seals_policy_and_blocks_an_old_worker(tmp_path, monkeypatch, capsys):
    from test_pbrun_detach import _checkout, _one_json_line, _run_pbrun

    work = _checkout(tmp_path)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    _announce(queue, "sparky", cycle=True)
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach", "--progress-cycle",
                      "--progress", "encode=200", "--progress", "publish=5") == 0
    submission = _one_json_line(capsys.readouterr())
    item = json.loads(Path(submission["submission"]).read_text())
    assert pb.PROGRESS_CYCLE_TAG in item["tags"]
    key = item["action_key"]
    request = json.loads((Path(item["cas_root"]) / "requests" / key[:2] / f"{key}.json").read_text())
    assert request["params"][pb.PROGRESS_PARAM] == _policy()
    linear = {k: v for k, v in request.items() if k != "action_key"}
    linear["params"] = {**linear["params"], pb.PROGRESS_PARAM: _policy(False)}
    assert pb.seal_action(linear)["action_key"] != key
    old_tags = [tag for tag in item["tags"] if tag != pb.PROGRESS_CYCLE_TAG]
    assert queue.claim(tags=old_tags) is None
    claimed = queue.claim(tags=item["tags"])
    assert claimed["action_key"] == key
    policy = pool.progress_policy(claimed, 100)
    assert policy.cycle is True
    assert policy.as_record()["progress_cycle"] is True
    assert policy.no_progress_bound_s == 105


def test_campaign_cycle_field_roundtrips_and_refuses_missing_phases_or_slurm():
    import pbcampaign

    row = {"argv": ["/bin/true"], "progress_phases": ["encode=200", "publish=5"],
           "progress_cycle": True}
    pbcampaign._require_submittable_row(row, index=0, transport="pool")
    assert "--progress-cycle" in pbcampaign.pbrun_argv(row)
    with pytest.raises(pbcampaign.ManifestError, match="requires pool transport"):
        pbcampaign._require_submittable_row(row, index=0, transport="slurm")
    with pytest.raises(pbcampaign.ManifestError, match="requires --progress-phase"):
        pbcampaign._require_submittable_row(
            {"argv": ["/bin/true"], "progress_cycle": True}, index=0, transport="pool")
    for bad in (1, "true", None):
        with pytest.raises(pbcampaign.ManifestError, match="progress_cycle"):
            pbcampaign._require_row_shape({**row, "progress_cycle": bad}, index=0)


CYCLIC_REPORTER = '''
import os, runpy, sys, time
commit = runpy.run_path(os.environ["PRISMABUILD_ACTION_PROGRESS_HELPER"])["commit"]
units = 0
for cycle in range(4):
    commit(units, "encode")
    time.sleep(0.8)
    with open("journal", "a") as f:
        f.write(str(cycle) + "\\n")
        f.flush()
        os.fsync(f.fileno())
    units += 1
    commit(units, "publish")
    time.sleep(0.1)
open("result", "w").write(str(units))
'''


@pytest.mark.parametrize("cycle,deadline", [(True, None), (False, None), (True, 2.5)])
def test_claimed_cyclic_action_completes_and_still_honours_deadline(tmp_path, cycle, deadline):
    policy = pbrun.parse_progress_phases(
        ["startup=3", "encode=3", "publish=0.3"], cycle=cycle)
    queue, item = _claimed(tmp_path, mode="report", seconds=4, policy=policy,
                           timeout_s=deadline, source=CYCLIC_REPORTER)
    outcome = queue.execute(item, timeout_s=3, heartbeat_s=0.05, timeout_grace_s=0.2)
    if cycle and deadline is None:
        assert outcome["status"] == "executed", outcome
        request = json.loads((Path(item["cas_root"]) / "requests" /
                              item["action_key"][:2] / f'{item["action_key"]}.json').read_text())
        cas = pb.PrismaBuildCAS(item["cas_root"])
        receipt = cas.lookup(request)
        assert receipt is not None
        assert cas._verified_receipt_result_path(receipt).read_text() == "4"
        assert outcome["progress_observation"]["last_accepted"]["units_completed"] == 4
        assert outcome["elapsed_s"] > 3
    else:
        assert outcome["status"] == "timeout", outcome
        assert outcome["termination_reason"] == ("execution_deadline" if deadline else "no_progress")
    assert outcome["progress_cycle"] is cycle


def test_live_phase_switching_without_new_units_still_times_out(tmp_path):
    source = '''
import os, runpy, time
commit = runpy.run_path(os.environ["PRISMABUILD_ACTION_PROGRESS_HELPER"])["commit"]
for i in range(200):
    commit(0, "encode" if i % 2 == 0 else "publish")
    print("still alive, no committed work", flush=True)
    time.sleep(0.1)
'''
    policy = pbrun.parse_progress_phases(
        ["startup=3", "encode=0.8", "publish=0.3"], cycle=True)
    queue, item = _claimed(tmp_path, mode="report", seconds=20, policy=policy, source=source)
    outcome = queue.execute(item, timeout_s=3, heartbeat_s=0.05, timeout_grace_s=0.2)
    assert outcome["status"] == "timeout", outcome
    assert outcome["termination_reason"] == "no_progress"
    assert outcome["elapsed_s"] < 5
    assert outcome["progress_observation"]["last_accepted"]["units_completed"] == 0
    assert outcome["progress_observation"]["last_rejection"] == "replayed"

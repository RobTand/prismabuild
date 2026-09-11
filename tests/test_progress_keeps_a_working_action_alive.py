"""An action that keeps committing work is not killed for taking a long time.

Two GLM-5.3 pricing rows were terminated at a 14,400 s limit while publishing
measured anchors, with 5,952 and 5,920 of them durably journalled at the
moment they were killed (#480).  Nothing about either run was stuck; the only
thing that had happened was time.  These tests fix both halves of the answer:
a run that advances outlives the limit that killed those, and a run that only
*looks* busy still ends.
"""
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb, pool, progress  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402
import pbstatus  # noqa: E402


#: The action side of the contract, written without importing PrismaBuild on
#: purpose: what the worker enforces is the record on disk, so the fixture
#: writes that record rather than calling the helper that produces it.
#: ``prismabuild.progress`` is tested separately, against the same bytes.
REPORTER = '''
import json, os, sys, time
path = os.environ.get("PRISMABUILD_ACTION_PROGRESS_PATH")
token = os.environ.get("PRISMABUILD_ACTION_PROGRESS_TOKEN")
mode = sys.argv[1]
units = 0
deadline = time.monotonic() + float(sys.argv[2])
while time.monotonic() < deadline:
    time.sleep(0.05)
    if mode == "silent":
        continue
    if mode == "chatty":
        print("still working", flush=True)
        continue
    if not path or not token:
        # The control run: the same argv doing the same work, told nothing
        # because it did not declare the contract.  Reporting into a void is
        # what the fixture must not silently do -- that is the difference the
        # test is measuring.
        continue
    units += 1
    record = {"schema": "prismabuild.action_progress.v1",
              "token": token, "phase": "run",
              "units_completed": 1 if mode == "replay" else units,
              "unit": "widgets", "reported_unix": time.time()}
    if mode == "foreign":
        record["token"] = "0" * 32
    if mode == "undeclared":
        record["phase"] = "somewhere-else"
    if mode == "regressing":
        record["units_completed"] = 100 - units
    if mode == "torn":
        record = "{not json"
    tmp = path + ".tmp"
    with open(tmp, "w") as handle:
        handle.write(record if isinstance(record, str) else json.dumps(record))
    os.replace(tmp, path)
open("result", "w").write("ok")
'''

PHASES = ("startup", "run", "publish")


def _policy(*graces):
    return {"schema": pb.PROGRESS_POLICY_SCHEMA_V1,
            "phases": [{"name": name, "grace_s": grace}
                       for name, grace in zip(PHASES, graces)]}


def _claimed(tmp_path, *, mode, seconds, policy, timeout_s=None, source=REPORTER):
    """Claim one action whose ``task.py`` is ``source``, run as ``mode``.

    ``source`` is a parameter so a fixture that reaches the contract another
    way -- through the helper the worker points it at, rather than by writing
    the record itself -- runs on this same harness (#488).
    """

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text(source)
    params = {}
    if policy is not None:
        params[pb.PROGRESS_PARAM] = policy
    if timeout_s is not None:
        params["execution_timeout_s"] = timeout_s
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/progress", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py", mode, str(seconds)],
                 "working_directory": ".", "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": params,
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
                  checkout_root=checkout,
                  worker_script=Path(__file__).resolve().parents[1]
                  / "tools" / "prismabuild_worker.py")
    return queue, queue.claim()


def _run(tmp_path, **kwargs):
    ceiling = kwargs.pop("ceiling", 0.4)
    queue, item = _claimed(tmp_path, **kwargs)
    return queue.execute(item, timeout_s=ceiling, heartbeat_s=0.05,
                         timeout_grace_s=0.2)


# -- the regression itself ------------------------------------------------

def test_a_progressing_action_outlives_the_ceiling_that_killed_the_glm_rows(tmp_path):
    """The whole issue in one assertion: advancing beats elapsed time."""

    # A fresh Python interpreter can spend longer than 0.4 s importing the
    # worker on a loaded fleet box.  Give startup a realistic allowance while
    # keeping the action duration materially beyond the deadline it defeats.
    outcome = _run(tmp_path, mode="report", seconds=3.5, ceiling=2.0,
                   policy=_policy(2.0, 2.0, 2.0))
    assert outcome["status"] == "executed", repr(outcome)
    # It ran materially beyond the limit a box without the contract enforces.
    assert outcome["elapsed_s"] > 3.0
    assert outcome["execution_governed_by"] == "progress"
    # And the ceiling is still on the record, aimed at the quiet instead.
    assert outcome["worker_timeout_ceiling_s"] == 2.0
    assert outcome["execution_timeout_ceiling_s"] is None
    assert outcome["progress_stall_ceiling_s"] == 2.0
    progress = outcome["progress_observation"]
    assert progress["accepted_count"] > 1
    assert progress["last_accepted"]["units_completed"] > 1
    assert progress["last_accepted"]["unit"] == "widgets"


def test_the_same_action_without_the_contract_is_killed_by_the_ceiling(tmp_path):
    """The control: nothing about the *work* is what makes the difference."""

    outcome = _run(tmp_path, mode="report", seconds=1.5, ceiling=0.4, policy=None)
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "execution_deadline"
    assert outcome["execution_governed_by"] == "deadline"
    assert "progress_observation" not in outcome


# -- what must still end ---------------------------------------------------

@pytest.mark.parametrize("mode,rejection,accepted", [
    ("silent", None, 0),
    ("chatty", None, 0),
    # A first reading of a counter is always advancement -- there is nothing
    # yet to have beaten.  What must not keep the action alive is the second.
    ("replay", "replayed", 1),
    ("regressing", "replayed", 1),
    ("foreign", "foreign token", 0),
    ("undeclared", "undeclared phase", 0),
    ("torn", "unparsable", 0),
])
def test_looking_busy_is_not_progress(tmp_path, mode, rejection, accepted):
    """Bytes, heartbeats, replayed counters and foreign records are not work."""

    outcome = _run(tmp_path, mode=mode, seconds=30, ceiling=5.0,
                   policy=_policy(0.4, 0.4, 0.4))
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    assert outcome["returncode"] is None
    # Bounded by the declared allowance, not by the 30 s the action wanted.
    assert outcome["elapsed_s"] < 5.0
    progress = outcome["progress_observation"]
    assert progress["accepted_count"] == accepted
    assert (progress["last_accepted"] is None) is (accepted == 0)
    if rejection is None:
        assert progress["rejected_count"] == 0
    else:
        assert progress["rejected_count"] > 0
        assert progress["last_rejection"] == rejection


def test_a_declaring_action_that_never_reports_ends_within_the_stated_bound(tmp_path):
    """Point 5: missing progress has a documented, computable bounded outcome."""

    outcome = _run(tmp_path, mode="silent", seconds=30, ceiling=5.0,
                   policy=_policy(0.3, 0.3, 0.3))
    assert outcome["status"] == "timeout"
    # The first phase's allowance governs from the launch; the sum is the
    # bound only for an action that walks through its phases.
    assert outcome["progress_no_progress_bound_s"] == pytest.approx(0.9)
    assert outcome["elapsed_s"] < 5.0


# -- precedence ------------------------------------------------------------

def test_an_explicitly_requested_deadline_still_ends_a_progressing_action(tmp_path):
    """Point 1: the contract must not silently override a submitted budget."""

    outcome = _run(tmp_path, mode="report", seconds=30, ceiling=None,
                   timeout_s=0.5, policy=_policy(60, 60, 60))
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "execution_deadline"
    assert outcome["execution_timeout_requested_s"] == 0.5
    assert outcome["elapsed_s"] < 5.0
    # It really was advancing; the deadline ended it anyway.
    assert outcome["progress_observation"]["accepted_count"] > 0


def test_a_withdrawal_beats_a_progressing_action(tmp_path):
    """Point 5: progress does not defeat cancellation."""

    queue, item = _claimed(tmp_path, mode="report", seconds=30,
                           policy=_policy(60, 60, 60))
    queue.withdraw(item["action_key"], reason="operator", signal_child=False)
    outcome = queue.execute(item, timeout_s=None, heartbeat_s=0.05,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "withdrawn"


# -- the clamp -------------------------------------------------------------

def test_the_worker_ceiling_clamps_the_allowance_and_says_so(tmp_path):
    """Point 6, the honest half: a box may shorten the quiet, never in silence."""

    outcome = _run(tmp_path, mode="silent", seconds=30, ceiling=0.3,
                   policy=_policy(600, 600, 600))
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    assert outcome["progress_stall_clamped"] is True
    first = outcome["progress_phases"][0]
    assert first["grace_requested_s"] == 600
    assert first["grace_ceiling_s"] == 0.3
    assert first["grace_s"] == 0.3
    assert first["grace_clamped"] is True
    assert outcome["progress_no_progress_bound_s"] == pytest.approx(0.9)


# -- the channel -----------------------------------------------------------

def test_the_action_is_told_where_to_report_only_when_it_declared(tmp_path):
    """The env forwarding is the whole channel; nothing else reaches the action."""

    sealed = {"schema": pb.ACTION_SCHEMA_V2,
              "task": {"definition_id": "t", "definition_version": "v1",
                       "task_class": "generation", "determinism": "deterministic",
                       "artifact_family": "generic", "artifact_kind": "generic",
                       "argv": ["/bin/true"], "working_directory": ".",
                       "result_path": "result"},
              "inputs": [], "code_closure": [],
              "params": {}, "environment": {"variables": {}, "toolchain": {}},
              "execution_scope": {"portability": "portable", "platform_key": None,
                                  "host_class": None}}
    os.environ[pb.ACTION_PROGRESS_PATH_ENV] = "/queue/k.progress"
    os.environ[pb.ACTION_PROGRESS_TOKEN_ENV] = "deadbeef"
    try:
        assert pb._progress_environment(sealed, {}) == {}
        declaring = {**sealed, "params": {pb.PROGRESS_PARAM: _policy(1)}}
        assert pb._progress_environment(declaring, {}) == {
            pb.ACTION_PROGRESS_PATH_ENV: "/queue/k.progress",
            pb.ACTION_PROGRESS_TOKEN_ENV: "deadbeef"}
        with pytest.raises(pb.ActionContractError, match="seals"):
            pb._progress_environment(
                declaring, {pb.ACTION_PROGRESS_PATH_ENV: "/elsewhere"})
        del os.environ[pb.ACTION_PROGRESS_TOKEN_ENV]
        with pytest.raises(pb.ActionContractError, match="must set both"):
            pb._progress_environment(declaring, {})
    finally:
        os.environ.pop(pb.ACTION_PROGRESS_PATH_ENV, None)
        os.environ.pop(pb.ACTION_PROGRESS_TOKEN_ENV, None)


def test_a_launcher_with_no_channel_refuses_rather_than_running_unbounded(tmp_path):
    """The dangerous half of a missing channel is the quiet one.

    Half a channel was already refused.  *No* channel used to return ``{}``,
    which launched an action admitted under the contract with nothing watching
    it -- and on a transport that sends no deadline of its own, nothing bounding
    it at all.  Point 5 says a missing progress channel has a documented bounded
    outcome; refusing the launch is that outcome.
    """

    for name in (pb.ACTION_PROGRESS_PATH_ENV, pb.ACTION_PROGRESS_TOKEN_ENV):
        assert name not in os.environ
    declaring = {"params": {pb.PROGRESS_PARAM: _policy(1)}}
    with pytest.raises(pb.ActionContractError) as raised:
        pb._progress_environment(declaring, {})
    said = str(raised.value)
    assert pb.ACTION_PROGRESS_PATH_ENV in said
    assert pb.ACTION_PROGRESS_TOKEN_ENV in said
    assert "Nothing would bound this run" in said


def test_the_reporter_writes_what_the_worker_accepts(tmp_path):
    """Both sides of the wire, checked against each other rather than a fixture."""

    path = tmp_path / "k.progress"
    os.environ[pb.ACTION_PROGRESS_PATH_ENV] = str(path)
    os.environ[pb.ACTION_PROGRESS_TOKEN_ENV] = "tok"
    try:
        assert progress.report_action_progress("run", 7, unit="anchors") is True
    finally:
        del os.environ[pb.ACTION_PROGRESS_PATH_ENV]
        del os.environ[pb.ACTION_PROGRESS_TOKEN_ENV]
    assert progress.report_action_progress("run", 8) is False  # no channel, no-op
    declared = pb.validate_progress_policy(_policy(60, 60, 60))
    watch = pool.ProgressWatch(path, "tok", pool.ProgressPolicy(
        tuple(pool.ProgressPhase(p["name"], p["grace_s"], None)
              for p in declared["phases"]), None), started=0.0)
    assert watch.sample(now=1.0) is True
    assert watch.last_accepted["units_completed"] == 7
    assert watch.last_accepted["phase"] == "run"
    # The same record twice is not twice the work.
    assert watch.sample(now=2.0) is False
    assert watch.last_rejection == "replayed"


def test_a_wall_clock_jump_cannot_extend_or_end_a_run(tmp_path):
    """Point: the watchdog times with the monotonic clock, never the record's."""

    path = tmp_path / "k.progress"
    declared = pb.validate_progress_policy(_policy(60, 60, 60))
    watch = pool.ProgressWatch(path, "tok", pool.ProgressPolicy(
        tuple(pool.ProgressPhase(p["name"], p["grace_s"], None)
              for p in declared["phases"]), None), started=100.0)
    path.write_text(json.dumps({
        "schema": pb.PROGRESS_RECORD_SCHEMA_V1, "token": "tok", "phase": "run",
        "units_completed": 3, "reported_unix": 0}))          # 1970, and irrelevant
    assert watch.sample(now=150.0) is True
    assert watch.stall_deadline() == 210.0


def test_entering_a_later_phase_rearms_the_allowance_once(tmp_path):
    """Point 3: finalization is quiet on purpose, and is declared, not guessed."""

    path = tmp_path / "k.progress"
    declared = pb.validate_progress_policy(_policy(10, 20, 30))
    watch = pool.ProgressWatch(path, "tok", pool.ProgressPolicy(
        tuple(pool.ProgressPhase(p["name"], p["grace_s"], None)
              for p in declared["phases"]), None), started=0.0)
    assert watch.stall_deadline() == 10.0                    # startup's
    for phase, units, now, deadline in (("run", 1, 5.0, 25.0),
                                        ("publish", 1, 8.0, 38.0)):
        path.write_text(json.dumps({
            "schema": pb.PROGRESS_RECORD_SCHEMA_V1, "token": "tok",
            "phase": phase, "units_completed": units}))
        assert watch.sample(now=now) is True
        assert watch.stall_deadline() == deadline
    # Naming an earlier phase again buys neither advancement nor its allowance.
    path.write_text(json.dumps({
        "schema": pb.PROGRESS_RECORD_SCHEMA_V1, "token": "tok",
        "phase": "startup", "units_completed": 1}))
    assert watch.sample(now=9.0) is False
    assert watch.stall_deadline() == 38.0


# -- submission ------------------------------------------------------------

@pytest.mark.parametrize("bad,message", [
    (["run"], "NAME=SECONDS"),
    (["run=soon"], "non-numeric"),
    (["run=0"], "positive finite"),
    (["run=1", "run=2"], "repeats"),
])
def test_an_unusable_phase_declaration_is_refused_at_submit(bad, message):
    with pytest.raises(SystemExit, match=message):
        pbrun.parse_progress_phases(bad)


def test_phase_order_is_the_order_it_was_declared():
    policy = pbrun.parse_progress_phases(["startup=1800", "encode=900"])
    assert [phase["name"] for phase in policy["phases"]] == ["startup", "encode"]
    assert policy["schema"] == pb.PROGRESS_POLICY_SCHEMA_V1


# -- admission: a box that does not know the policy must not be given one ---

FITS = {"cpu": 1, "mem_gb": 1}


def _intent(tags):
    return {"tags": list(tags), "needs_gpu": False, "resources": dict(FITS)}


def _fleet(tmp_path, boxes):
    """A queue whose boxes announce ``{host: contracts or None}``."""

    queue = pool.PoolQueue(tmp_path / "queue")
    for host, contracts in boxes.items():
        queue.announce(host=host, tags=["cpu", host, "x86"], has_gpu=False,
                       capacity={"gpu": 0, "mem_gb": 60, "cpu": 80},
                       timeout_ceiling_s=7200.0, progress_contracts=contracts)
    return queue


def test_a_box_announces_the_contracts_it_can_honour(tmp_path):
    queue = _fleet(tmp_path, {"dl380g10": [pb.PROGRESS_RECORD_SCHEMA_V1]})
    assert queue.offers()[0]["progress_contracts"] == [pb.PROGRESS_RECORD_SCHEMA_V1]


def test_an_offer_predating_the_field_carries_none(tmp_path):
    """Silence is "did not say", never "supports it"."""

    queue = _fleet(tmp_path, {"dl380g10": None})
    assert "progress_contracts" not in queue.offers()[0]
    assert queue.placement_progress_contracts(_intent([])) == {"dl380g10": None}


def test_a_fleet_that_cannot_honour_the_contract_refuses_the_submission(tmp_path):
    """Point 6: no hidden whole-run ceiling under a contract that promised none."""

    queue = _fleet(tmp_path, {"dl380g10": None, "sparky": []})
    with pytest.raises(SystemExit, match="no eligible worker announces"):
        pbrun.progress_contract_notice(
            queue, _intent([]), policy=_policy(1800, 900))


def test_a_mixed_fleet_is_narrowed_rather_than_refused(tmp_path):
    """A rolling upgrade is admitted, and named -- the old boxes are passed by."""

    queue = _fleet(tmp_path, {"dl380g10": [pb.PROGRESS_RECORD_SCHEMA_V1],
                              "sparky": None})
    said = pbrun.progress_contract_notice(
        queue, _intent([]), policy=_policy(1800, 900))
    assert "sparky do not announce" in said
    assert f"requires the {pb.PROGRESS_TAG} tag" in said
    assert "waits for a box that does" in said


def test_the_submitter_is_told_the_total_quiet_it_just_asked_for(tmp_path):
    queue = _fleet(tmp_path, {"dl380g10": [pb.PROGRESS_RECORD_SCHEMA_V1]})
    said = pbrun.progress_contract_notice(
        queue, _intent([]), policy=_policy(3600, 900, 1800))
    assert "startup 3600s, run 900s, publish 1800s" in said
    assert "at most 6300s of quiet in total" in said
    assert "no total-duration limit while it does" in said


def test_no_policy_says_nothing(tmp_path):
    queue = _fleet(tmp_path, {"dl380g10": None})
    assert pbrun.progress_contract_notice(queue, _intent([]), policy=None) == ""


def test_a_box_that_cannot_keep_the_policy_cannot_claim_the_work(tmp_path):
    """The closure the notice above can only report.

    During a rolling upgrade both generations poll the same queue, and a
    printed warning cannot decline a claim.  An old loop offers no
    ``PROGRESS_TAG``, and item tags must be a subset of the worker's, so the
    matcher that already exists is what keeps a progress-admitted action away
    from a box whose whole-run ceiling would end it.
    """

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text("print('ok')\n")
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/progress-tag", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"],
                 "working_directory": ".", "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {pb.PROGRESS_PARAM: _policy(1800)},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
                  checkout_root=checkout,
                  tags=["x86", pb.PROGRESS_TAG],
                  worker_script=Path(__file__).resolve().parents[1]
                  / "tools" / "prismabuild_worker.py")

    previous_generation = ["x86", "dl380g10", "cpu"]
    assert queue.claim(tags=previous_generation) is None
    claimed = queue.claim(tags=[*previous_generation, pb.PROGRESS_TAG])
    assert claimed is not None
    assert claimed["action_key"] == action["action_key"]


# -- transports that cannot enforce it are not offered it ------------------

def test_the_slurm_lane_refuses_a_contract_it_cannot_enforce():
    """The watchdog is the pull-queue worker's; SLURM has only ``--time``.

    Sealing the policy there admitted the action on the promise that its own
    advancement bounds it, and then ran it with no watchdog -- and, absent
    ``--timeout-s``, no deadline either.
    """

    policy = pbrun.parse_progress_phases(["startup=1800", "run=900"])
    pbrun.require_progress_scope(progress=policy, transport="pool")
    pbrun.require_progress_scope(progress=None, transport="slurm")
    with pytest.raises(ValueError, match="requires pool transport"):
        pbrun.require_progress_scope(progress=policy, transport="slurm")


def test_a_campaign_row_cannot_carry_phases_to_slurm():
    """Asked of pbrun in pbrun's own words, like every other row refusal."""

    import pbcampaign  # noqa: PLC0415  -- tools/fleet is on sys.path above

    row = {"argv": ["/bin/true"], "progress_phases": ["startup=1800"]}
    pbcampaign._require_submittable_row(row, index=3, transport="pool")
    with pytest.raises(pbcampaign.ManifestError, match="requires pool transport"):
        pbcampaign._require_submittable_row(row, index=3, transport="slurm")
    # And a phase the flag would refuse is refused with the rest of the
    # manifest, rather than at the submission of row 41.
    with pytest.raises(pbcampaign.ManifestError, match="NAME=SECONDS"):
        pbcampaign._require_submittable_row(
            {"argv": ["/bin/true"], "progress_phases": ["startup"]},
            index=3, transport="pool")


def test_a_progress_submission_seals_the_capability_it_needs(
    tmp_path, monkeypatch, capsys
):
    """End to end: the flag reaches the queue item as a placement requirement."""

    from test_pbrun_detach import _checkout, _one_json_line, _run_pbrun  # noqa: PLC0415

    work = _checkout(tmp_path)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(
        host="sparky", tags=["sparky", "gb10", pb.PROGRESS_TAG], has_gpu=True,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
        timeout_ceiling_s=7200.0,
        progress_contracts=[pb.PROGRESS_RECORD_SCHEMA_V1],
    )
    assert _run_pbrun(
        tmp_path, monkeypatch, work,
        "--detach", "--progress-phase", "startup=1800",
        "--progress-phase", "run=900",
    ) == 0

    captured = capsys.readouterr()
    line = _one_json_line(captured)
    item = json.loads(Path(line["submission"]).read_text(encoding="utf-8"))
    assert pb.PROGRESS_TAG in item["tags"]
    assert "at most 2700s of quiet in total" in captured.err

    # And the sealed request carries the policy the receipt will name -- read
    # off disk by the same path ``pool._sealed_progress_policy`` reads.
    key = line["action_key"]
    request = json.loads(
        (Path(item["cas_root"]) / "requests" / key[:2] / f"{key}.json")
        .read_text(encoding="utf-8"))
    assert request["params"][pb.PROGRESS_PARAM]["phases"] == [
        {"name": "startup", "grace_s": 1800.0},
        {"name": "run", "grace_s": 900.0},
    ]


# --------------------------------------------------------------------------
# What an operator sees while it runs
# --------------------------------------------------------------------------

def _watched(tmp_path) -> tuple[pool.ProgressWatch, pool.ProgressPolicy]:
    """A real watch that has accepted twelve real records, in the second phase.

    Built through ``sample`` rather than by assigning its fields, so what
    ``pbstatus`` reads is what the worker actually writes.  The two halves of
    this contract were added in one change and could drift in one change.
    """

    policy = pool.ProgressPolicy(
        (pool.ProgressPhase("startup", 3600.0, None),
         pool.ProgressPhase("pricing", 900.0, None)),
        None,
    )
    record = tmp_path / "progress.json"
    watch = pool.ProgressWatch(record, "tok", policy, started=0.0)
    for unit in range(1, 13):
        record.write_text(json.dumps({
            "schema": pb.PROGRESS_RECORD_SCHEMA_V1, "token": "tok",
            "phase": "pricing", "units_completed": unit,
            "unit": "anchor", "reported_unix": 1.0 * unit,
        }), encoding="utf-8")
        assert watch.sample(now=float(unit)) is True
    assert watch.accepted == 12 and watch.grace_s == 900.0
    return watch, policy


def _claim_with(queue, key, observation, *, tmp_path, overrides=()):
    """Publish, claim, and put ``observation`` on the lease of that attempt."""

    queue.publish(action_key=key, cas_root=tmp_path / "cas",
                  checkout_root=tmp_path, worker_script="/worker.py")
    item = queue.claim(owner="worker")
    assert item is not None
    lease = json.loads(queue.lease_path(key).read_text(encoding="utf-8"))
    lease["published_unix"] = item["published_unix"]
    lease["progress_observation"] = observation
    lease.update(overrides)
    queue.lease_path(key).write_text(json.dumps(lease), encoding="utf-8")
    return next(row for row in pbstatus.read_pool(queue.root)["jobs"]
                if row["action_key"] == key)


def test_status_says_how_quiet_a_progress_action_is_against_its_allowance(
    tmp_path
):
    """``OUTPUT`` is the age #480 says proves nothing.  ``PROGRESS`` is the
    reading the watchdog acts on, and an operator can now see both."""

    watch, _ = _watched(tmp_path)
    queue = pool.PoolQueue(tmp_path / "queue")
    # 41 s of monotonic time after the twelfth record, inside a 900 s phase.
    row = _claim_with(queue, "d" * 64, watch.as_record(now=53.0),
                      tmp_path=tmp_path)

    assert row["progress_observation"] == "pricing quiet 41/900s (12 accepted)"
    rendered = "\n".join(pbstatus.pool_job_lines([row], {"empty": False}))
    assert "PROGRESS" in rendered
    assert "pricing quiet 41/900s (12 accepted)" in rendered


@pytest.mark.parametrize("field", ["owner", "host", "claimed_unix",
                                   "published_unix", "action_key"])
def test_a_lease_naming_another_attempt_reports_no_progress(tmp_path, field):
    """The same gate the execution observation is held to.  A record left by
    the attempt before this one describes work this claim is not doing, and a
    column that showed it would say a stuck action was advancing."""

    watch, _ = _watched(tmp_path)
    queue = pool.PoolQueue(tmp_path / "queue")
    row = _claim_with(queue, "e" * 64, watch.as_record(now=53.0),
                      tmp_path=tmp_path, overrides={field: "elsewhere"})

    assert row["progress_observation"] is None
    assert "quiet" not in "\n".join(
        pbstatus.pool_job_lines([row], {"empty": False}))


@pytest.mark.parametrize("observation", [
    {"quiet_s": -1.0, "grace_s": 900.0},            # before the first record
    {"quiet_s": float("inf"), "grace_s": 900.0},    # a clock that jumped
    {"quiet_s": 41.0, "grace_s": None},             # a policy with no number
    {"quiet_s": "41", "grace_s": 900.0},            # a string from somewhere
    "pricing quiet 41s",                            # not a record at all
])
def test_an_unreadable_progress_record_reports_nothing_rather_than_a_number(
    tmp_path, observation
):
    """A malformed observation has one bounded outcome, and it is silence:
    ``KILL AT`` remains the number to read, which is true of an action that
    declared no policy either."""

    queue = pool.PoolQueue(tmp_path / "queue")
    row = _claim_with(queue, "f" * 64, observation, tmp_path=tmp_path)
    assert row["progress_observation"] is None

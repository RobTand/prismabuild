"""An action can report committed work without being a PrismaBuild process.

#481 gave the fleet a watchdog that keeps a working action alive.  It left the
action side reachable only by `import prismabuild`, which fails in every
interpreter the fleet actually runs work under -- `/home/rob/venvs/pb-cpu`,
the system python3, a pinned producer image -- because the package lives on
the shared mount rather than in a venv.  The first consumer wrote its own copy
of the record against the wire format for exactly that reason, and a second
copy of a versioned schema is how the two drift.

These tests hold every documented way of reporting to one standard: the bytes
the worker's own ``ProgressWatch`` accepts (#488).
"""
import ast
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb, pool, progress  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_progress_keeps_a_working_action_alive import (  # noqa: E402
    PHASES, _claimed, _policy,
)

REPO = Path(__file__).resolve().parents[1]
SKILL = REPO / "skills" / "prismabuild" / "SKILL.md"

#: The action side of the contract for a run that cannot import PrismaBuild:
#: it asks the worker where the helper is and runs that, so the writer is the
#: launching generation's own rather than a copy of it.  This is the fixture
#: form of the two lines the skill tells an agent to paste.
HELPER_REPORTER = '''
import os, runpy, sys, time
commit = runpy.run_path(os.environ["PRISMABUILD_ACTION_PROGRESS_HELPER"])["commit"]

try:
    import prismabuild                       # noqa: F401
except ImportError:
    pass                                     # the ordinary case, and the point
else:                                        # pragma: no cover - would void the test
    raise SystemExit("this fixture must not be able to import prismabuild")

units = 0
deadline = time.monotonic() + float(sys.argv[2])
while time.monotonic() < deadline:
    time.sleep(0.05)
    if sys.argv[1] == "helper-stall" and units >= 3:
        # Alive, printing, committing nothing.  The control: reaching the
        # contract the easy way must not make stopping cost less.
        print("still here", flush=True)
        continue
    units += 1
    open("units", "w").write(str(units))     # durable first
    commit(units, unit="widgets")            # then reported
open("result", "w").write("ok")
'''


def _watch(path, token, graces=(60, 60, 60)):
    declared = pb.validate_progress_policy(_policy(*graces))
    policy = pool.ProgressPolicy(
        tuple(pool.ProgressPhase(phase["name"], phase["grace_s"], None)
              for phase in declared["phases"]), None)
    return pool.ProgressWatch(Path(path), token, policy, started=0.0)


@pytest.fixture
def channel(tmp_path, monkeypatch):
    """An action's environment, exactly as ``pool.execute`` sets it up."""

    path = tmp_path / "k.progress"
    monkeypatch.setenv(pb.ACTION_PROGRESS_PATH_ENV, str(path))
    monkeypatch.setenv(pb.ACTION_PROGRESS_TOKEN_ENV, "tok")
    monkeypatch.setenv(pb.ACTION_PROGRESS_PHASES_ENV, json.dumps(list(PHASES)))
    monkeypatch.setenv(pb.ACTION_PROGRESS_HELPER_ENV, progress.__file__)
    return path


# -- one writer behind every spelling -------------------------------------

def test_the_package_helper_writes_what_the_worker_accepts(channel):
    assert progress.commit(7, "run", unit="anchors") is True
    watch = _watch(channel, "tok")
    assert watch.sample(now=1.0) is True
    assert watch.last_accepted["units_completed"] == 7
    assert watch.last_accepted["phase"] == "run"
    # The same count again is not more work, whichever spelling wrote it.
    assert progress.commit(7, "run") is False or watch.sample(now=2.0) is False
    assert watch.last_rejection == "replayed"


def test_the_original_spelling_is_the_same_writer(channel):
    assert progress.report_action_progress("run", 3, unit="anchors") is True
    first = json.loads(Path(channel).read_text())
    assert progress.commit(4, "run", unit="anchors") is True
    second = json.loads(Path(channel).read_text())
    assert first.keys() == second.keys()
    assert first["schema"] == second["schema"] == pb.PROGRESS_RECORD_SCHEMA_V1
    assert prismabuild_exports() == (
        progress.commit, progress.report_action_progress)


def prismabuild_exports():
    import prismabuild

    return prismabuild.commit, prismabuild.report_action_progress


def test_one_definition_of_the_schema_and_the_channel():
    """``core`` mirrors these rather than importing them, so check the mirror.

    It cannot import: the worker attestation hashes the launcher and
    ``core.py``, so a repository import there would be code the worker runs
    and the attestation does not cover
    (``test_worker_core_has_no_unattested_repository_imports``).  The action
    cannot import ``core`` either, which is why the writer lives in the leaf.
    Two files state these strings; this is what stops them disagreeing.
    """

    assert pb.PROGRESS_RECORD_SCHEMA_V1 == progress.PROGRESS_RECORD_SCHEMA_V1
    assert pb.MAX_ACTION_PROGRESS_BYTES == progress.MAX_ACTION_PROGRESS_BYTES
    assert pb.ACTION_PROGRESS_ENV == progress.ACTION_PROGRESS_ENV
    assert pb.ACTION_PROGRESS_ENV == (
        pb.ACTION_PROGRESS_PATH_ENV, pb.ACTION_PROGRESS_TOKEN_ENV,
        pb.ACTION_PROGRESS_PHASES_ENV, pb.ACTION_PROGRESS_HELPER_ENV)
    relative = [node for node in ast.walk(ast.parse(Path(pb.__file__).read_text()))
                if isinstance(node, ast.ImportFrom) and node.level > 0]
    assert relative == []


def test_no_channel_is_a_no_op_rather_than_an_error(monkeypatch):
    for name in pb.ACTION_PROGRESS_ENV:
        monkeypatch.delenv(name, raising=False)
    assert progress.commit(1, "run") is False
    assert progress.channel() is None
    assert progress.declared_phases() is None


# -- the phase, defaulted and checked where the typo is --------------------

def test_the_phase_defaults_to_the_first_one_declared(channel):
    assert progress.commit(2) is True
    assert json.loads(Path(channel).read_text())["phase"] == PHASES[0]


def test_an_undeclared_phase_is_refused_here_rather_than_ignored_there(channel):
    """A typo used to read as silence, and silence ends at the stall bound."""

    with pytest.raises(ValueError, match="not one this action declared"):
        progress.commit(1, "encode")
    assert not Path(channel).exists()


def test_an_older_worker_publishes_no_phases_and_the_phase_is_required(
    channel, monkeypatch,
):
    """A generation that predates the export still bounds the run correctly."""

    monkeypatch.delenv(pb.ACTION_PROGRESS_PHASES_ENV)
    assert progress.declared_phases() is None
    with pytest.raises(ValueError, match="phase is required"):
        progress.commit(1)
    # Naming one explicitly still works: nothing local can check it, and the
    # worker enforces the policy it holds.
    assert progress.commit(1, "run") is True


@pytest.mark.parametrize("raw", ["", "[]", "{}", "not json", '["ok", 3]'])
def test_an_unusable_phase_list_is_read_as_unknown(channel, monkeypatch, raw):
    monkeypatch.setenv(pb.ACTION_PROGRESS_PHASES_ENV, raw)
    assert progress.declared_phases() is None
    assert progress.commit(1, "run") is True


@pytest.mark.parametrize("units", [-1, float("nan"), float("inf"), "3", None])
def test_a_count_that_is_not_a_count_is_refused(channel, units):
    with pytest.raises(ValueError, match="units_completed"):
        progress.commit(units, "run")


def test_a_whole_count_keeps_its_precision(channel):
    big = 2 ** 53 + 1
    assert progress.commit(big, "run") is True
    watch = _watch(channel, "tok")
    assert watch.sample(now=1.0) is True
    assert watch.last_accepted["units_completed"] == big


def test_an_unwritable_queue_reads_as_a_stall_rather_than_a_failure(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv(pb.ACTION_PROGRESS_PATH_ENV,
                       str(tmp_path / "gone" / "k.progress"))
    monkeypatch.setenv(pb.ACTION_PROGRESS_TOKEN_ENV, "tok")
    monkeypatch.setattr(progress.Path, "mkdir",
                        lambda *a, **k: (_ for _ in ()).throw(OSError()))
    assert progress.commit(1, "run") is False


# -- reaching the helper without importing it ------------------------------

def test_running_the_helper_as_a_program_reports(channel):
    """The path for a shell loop, or any language that can run a process."""

    assert subprocess.run(
        [sys.executable, os.environ[pb.ACTION_PROGRESS_HELPER_ENV],
         "--phase", "run", "--units", "12", "--unit", "widgets"],
        capture_output=True, text=True, check=False,
    ).returncode == 0
    watch = _watch(channel, "tok")
    assert watch.sample(now=1.0) is True
    assert watch.last_accepted["units_completed"] == 12
    assert watch.last_accepted["unit"] == "widgets"


def test_the_program_keeps_a_whole_count_exact(channel):
    """A shell loop past 2**53 units is the same count the package writes."""

    big = 2 ** 53 + 1
    assert subprocess.run(
        [sys.executable, os.environ[pb.ACTION_PROGRESS_HELPER_ENV],
         "--phase", "run", "--units", str(big)],
        capture_output=True, text=True, check=False).returncode == 0
    watch = _watch(channel, "tok")
    assert watch.sample(now=1.0) is True
    assert watch.last_accepted["units_completed"] == big


def test_the_program_says_nothing_and_succeeds_without_a_channel(monkeypatch):
    for name in pb.ACTION_PROGRESS_ENV:
        monkeypatch.delenv(name, raising=False)
    ran = subprocess.run(
        [sys.executable, str(progress.__file__), "--units", "1", "--phase", "run"],
        capture_output=True, text=True, check=False)
    assert ran.returncode == 0 and ran.stdout == ""
    required = subprocess.run(
        [sys.executable, str(progress.__file__), "--units", "1", "--phase", "run",
         "--require-channel"],
        capture_output=True, text=True, check=False)
    assert required.returncode == 1
    assert "no progress channel" in required.stderr


def test_the_program_refuses_an_undeclared_phase_loudly(channel):
    ran = subprocess.run(
        [sys.executable, os.environ[pb.ACTION_PROGRESS_HELPER_ENV],
         "--phase", "encode", "--units", "1"],
        capture_output=True, text=True, check=False)
    assert ran.returncode == 2
    assert "not one this action declared" in ran.stderr


# -- the documented fallback, held to the same bytes ------------------------

def _skill_snippet() -> str:
    """The python block the skill tells an agent to paste, read from the skill.

    Read rather than restated: a snippet in a document that nothing executes
    is a copy of the schema waiting to go stale, which is the defect this
    whole file exists to close.
    """

    text = SKILL.read_text()
    start = text.rindex("```python\n", 0, text.index("# pb-progress-snippet"))
    start += len("```python\n")
    return text[start:text.index("```", start)]


def test_the_snippet_in_the_skill_writes_what_the_worker_accepts(channel):
    """For a container that cannot see the mount at all."""

    namespace: dict[str, object] = {}
    exec(compile(_skill_snippet(), str(SKILL), "exec"), namespace)  # noqa: S102
    assert namespace["pb_commit"](5, "run") is True
    watch = _watch(channel, "tok")
    assert watch.sample(now=1.0) is True
    assert watch.last_accepted["units_completed"] == 5
    assert watch.sample(now=2.0) is False
    assert watch.last_rejection == "replayed"


def test_the_snippet_is_a_no_op_off_the_fleet(monkeypatch):
    for name in pb.ACTION_PROGRESS_ENV:
        monkeypatch.delenv(name, raising=False)
    namespace: dict[str, object] = {}
    exec(compile(_skill_snippet(), str(SKILL), "exec"), namespace)  # noqa: S102
    assert namespace["pb_commit"](5, "run") is False


# -- end to end, on a worker, through the exported environment --------------

def test_an_action_that_cannot_import_prismabuild_outlives_the_ceiling(tmp_path):
    """The whole point of #488, run rather than described.

    The fixture has no access to the package: it reads the helper's location
    out of its own environment, runs it, and is kept alive by what it commits
    -- past a ceiling that would have killed it four times over.

    The clamp is seconds rather than the fractions the #481 tests use, and
    deliberately: this action pays for a real interpreter start and a
    ``runpy`` of the helper before its first commit, and it runs on a test box
    that is busy with eleven other shards.  A clamp tight enough to be
    startup-sensitive would fail for a reason that is not the contract.
    """

    queue, item = _claimed(tmp_path, mode="helper", seconds=8.0,
                           policy=_policy(60, 60, 60), source=HELPER_REPORTER)
    outcome = queue.execute(item, timeout_s=2.0, heartbeat_s=0.05,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "executed", outcome.get("stderr")
    assert outcome["elapsed_s"] > 6.0
    assert outcome["execution_governed_by"] == "progress"
    observed = outcome["progress_observation"]
    assert observed["accepted_count"] >= 5
    # A watcher polling faster than the action commits re-reads the record it
    # already accepted, and that is a replay like any other.  What must not
    # appear is a rejection that says the helper wrote something else.
    assert observed["last_rejection"] in (None, "replayed")
    assert observed["phase"] == PHASES[0]
    assert observed["last_accepted"]["unit"] == "widgets"


def test_an_action_that_reports_the_easy_way_still_dies_when_it_stops(tmp_path):
    """The other half: the helper buys time for work, not for being alive."""

    queue, item = _claimed(tmp_path, mode="helper-stall", seconds=120,
                           policy=_policy(3.0, 60, 60), source=HELPER_REPORTER)
    outcome = queue.execute(item, timeout_s=600, heartbeat_s=0.05,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    # Two minutes of declared work, no total-duration limit worth the name,
    # and it is gone in single-digit seconds: the allowance it stopped
    # advancing in is the whole bound.
    assert outcome["elapsed_s"] < 30
    observed = outcome["progress_observation"]
    assert observed["last_accepted"]["units_completed"] == 3
    assert observed["quiet_s"] >= 3.0


def test_the_worker_tells_a_declaring_action_where_the_helper_is(tmp_path):
    queue, item = _claimed(tmp_path, mode="report", seconds=0.1,
                           policy=_policy(60, 60, 60))
    launched: dict[str, str] = {}
    original = pool.subprocess.Popen

    def record(argv, **kwargs):
        launched.update(kwargs["env"])
        return original(argv, **kwargs)

    pool.subprocess.Popen = record
    try:
        queue.execute(item, timeout_s=5, heartbeat_s=0.05, timeout_grace_s=0.2)
    finally:
        pool.subprocess.Popen = original
    assert launched[pb.ACTION_PROGRESS_HELPER_ENV] == str(
        Path(progress.__file__).resolve())
    assert json.loads(launched[pb.ACTION_PROGRESS_PHASES_ENV]) == list(PHASES)
    assert Path(launched[pb.ACTION_PROGRESS_HELPER_ENV]).is_file()


def test_an_action_that_did_not_declare_is_told_nothing_new(tmp_path):
    """No contract, no variables: an ordinary action's launch is unchanged."""

    queue, item = _claimed(tmp_path, mode="report", seconds=0.1, policy=None)
    launched: dict[str, str] = {}
    original = pool.subprocess.Popen

    def record(argv, **kwargs):
        launched.update(kwargs["env"])
        return original(argv, **kwargs)

    pool.subprocess.Popen = record
    try:
        queue.execute(item, timeout_s=5, heartbeat_s=0.05, timeout_grace_s=0.2)
    finally:
        pool.subprocess.Popen = original
    assert not [name for name in pb.ACTION_PROGRESS_ENV if name in launched]


@pytest.mark.parametrize("name", list(pb.ACTION_PROGRESS_ENV))
def test_an_action_that_seals_any_of_them_is_refused(name, monkeypatch):
    declaring = {"params": {pb.PROGRESS_PARAM: _policy(1)}}
    monkeypatch.setenv(pb.ACTION_PROGRESS_PATH_ENV, "/queue/k.progress")
    monkeypatch.setenv(pb.ACTION_PROGRESS_TOKEN_ENV, "deadbeef")
    with pytest.raises(pb.ActionContractError, match="seals"):
        pb._progress_environment(declaring, {name: "mine"})


# -- the campaign row's opt-in ---------------------------------------------

def test_a_manifest_row_declares_the_contract_the_same_way_pbrun_does():
    """A GPU row opts in through the manifest, not through a second mechanism."""

    sys.path.insert(0, str(REPO / "tools" / "fleet"))
    import pbcampaign

    row = {"argv": ["/bin/true"], "demand": {"gpu": 1, "mem_gb": 102},
           "progress_phases": ["startup=1800", "pricing=900", "finalize=1800"]}
    argv = pbcampaign.pbrun_argv(row)
    assert argv.count("--progress-phase") == 3
    assert argv[argv.index("--progress-phase") + 1] == "startup=1800"
    # No --timeout-s unless the row asked for one: a progressing row is bounded
    # by its own advancement, and the box ceiling then clamps the allowances
    # rather than the run.
    assert "--timeout-s" not in argv
    assert pbcampaign.pbrun.parse_progress_phases(row["progress_phases"]) == {
        "schema": pb.PROGRESS_POLICY_SCHEMA_V1,
        "phases": [{"name": "startup", "grace_s": 1800.0},
                   {"name": "pricing", "grace_s": 900.0},
                   {"name": "finalize", "grace_s": 1800.0}]}

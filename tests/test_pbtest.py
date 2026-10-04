"""The suite dispatcher relies on pbrun's immutable checkout transport."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys

from pbtest_shard_output import ShardProcess, ONE_PASS, admitted_child, shard_environment  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pbtest", ROOT / "tools" / "fleet" / "pbtest.py"
)
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)  # type: ignore[union-attr]


def test_box_local_git_checkout_is_dispatched_without_leaking_its_path(
    tmp_path: Path, monkeypatch,
) -> None:
    """A local source tree is portable; only pbrun's --cwd may name it."""

    checkout = tmp_path / "checkout"
    test_file = checkout / "tests" / "test_one.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("def test_one():\n    assert True\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    calls: list[list[str]] = []

    class FinishedProcess(ShardProcess):
        returncode = 0

        def communicate(self):
            return ONE_PASS, None

    def popen(command, **_kwargs):
        calls.append(command)
        return FinishedProcess()

    monkeypatch.setattr(pbtest.subprocess, "Popen", popen)
    monkeypatch.setattr(pbtest, "RUNTIME_ROOT", PUBLISHED_RUNTIME)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pbtest.py", "--checkout", str(checkout),
            "--python", "/target/python", "--shards", "1", "tests",
        ],
    )

    assert pbtest.main() == 0
    assert len(calls) == 1
    command = calls[0]
    separator = command.index("--")
    assert command[command.index("--cwd") + 1] == str(checkout.resolve())
    assert str(checkout.resolve()) not in " ".join(command[separator + 1:])
    assert "PYTHONPATH=src:experiments" in command


#: The runtime these shards are dispatched from.  ``pbtest`` names no tag when
#: its own runtime is box-local, because ``pbrun`` seals that runtime's worker
#: launcher into the action and only one box can open it (#292).  The subject
#: of this file is the shard argv, so the runtime is declared instead of being
#: whatever ``tmp_path`` happens to make it.
PUBLISHED_RUNTIME = Path(
    "/mnt/shared/prismabuild-fleet/runtime-generations/test-generation")

def _dispatch(tmp_path: Path, monkeypatch, extra: list[str]) -> list[str]:
    """Run one shard's worth of dispatch and return the pbrun argv it built."""

    checkout = tmp_path / "checkout"
    test_file = checkout / "tests" / "test_one.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("def test_one():\n    assert True\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    calls: list[list[str]] = []

    class FinishedProcess(ShardProcess):
        returncode = 0

        def communicate(self):
            return ONE_PASS, None

    def popen(command, **_kwargs):
        calls.append(command)
        return FinishedProcess()

    monkeypatch.setattr(pbtest.subprocess, "Popen", popen)
    monkeypatch.setattr(pbtest, "RUNTIME_ROOT", PUBLISHED_RUNTIME)
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
         "--shards", "1", *extra, "tests"],
    )
    assert pbtest.main() == 0
    assert len(calls) == 1
    return calls[0]


def test_the_transport_reaches_pbrun(tmp_path: Path, monkeypatch) -> None:
    """A suite fanned out under SLURM must not shard into the pull queue.

    ``pbtest`` builds pbrun's argv itself, so a flag it does not forward is a
    flag the shards never see -- and twenty shards submitted to a queue with no
    workers wait out ``--wait-s`` before anybody learns which transport they
    were on.
    """

    monkeypatch.delenv("PRISMABUILD_TRANSPORT", raising=False)
    command = _dispatch(tmp_path, monkeypatch, ["--transport", "slurm"])
    assert command[command.index("--transport") + 1] == "slurm"
    # Before the payload, like every other pbrun flag.
    assert command.index("--transport") < command.index("--")


def test_the_priority_reaches_pbrun(tmp_path: Path, monkeypatch) -> None:
    """A queue hint ``pbtest`` does not forward is one the shards never carry.

    Test shards are the bulk of agent self-validation, and #362 asks that they
    be submittable at a priority that yields to campaign work.  Until this
    flag existed every shard entered at 0.
    """

    command = _dispatch(tmp_path, monkeypatch, ["--priority", "-10"])
    assert command[command.index("--priority") + 1] == "-10"
    assert command.index("--priority") < command.index("--")


def test_the_default_priority_is_left_to_pbrun(tmp_path: Path, monkeypatch) -> None:
    """Zero is pbrun's own default; not forwarding it keeps existing argv intact."""

    command = _dispatch(tmp_path, monkeypatch, [])
    assert "--priority" not in command


def test_a_checkout_with_no_receipt_stays_on_the_pull_queue(
    tmp_path: Path, monkeypatch,
) -> None:
    """The default is the fleet's, and a checkout is not a generation.

    This test used to say "the cutover is one environment variable", which
    described one shell and no part of a fleet whose agents start pbtest from
    cron, from systemd user units and from each other. The default rides in
    the published runtime generation's receipt; a checkout has none, so it
    keeps the pull queue until somebody says otherwise. The lane case is
    ``test_default_transport_travels_with_the_generation``.
    """

    monkeypatch.delenv("PRISMABUILD_TRANSPORT", raising=False)
    command = _dispatch(tmp_path, monkeypatch, [])
    assert command[command.index("--transport") + 1] == "pool"


def test_the_environment_can_carry_the_whole_suite_onto_the_lane(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setenv("PRISMABUILD_TRANSPORT", "slurm")
    command = _dispatch(tmp_path, monkeypatch, [])
    assert command[command.index("--transport") + 1] == "slurm"


def test_a_shard_states_its_placement_once(tmp_path: Path, monkeypatch) -> None:
    """Portable CPU tests carry the interpreter requirement without a class."""

    command = _dispatch(tmp_path, monkeypatch, [])

    assert "--tag" not in command and "--anywhere" in command


def test_worker_visible_tmpdir_reaches_the_shard_as_one_argument(
    tmp_path: Path, monkeypatch,
) -> None:
    # This need not exist on the coordinator, and its shell characters are data.
    worker_path = "/worker scratch/$(literal);[directory]"
    command = _dispatch(tmp_path, monkeypatch, ["--tmpdir", worker_path])
    assert shard_environment(command)["TMPDIR"] == worker_path
    assert sum(part.startswith("TMPDIR=") for part in command) == 1


def test_default_tmpdir_keeps_the_existing_shard_environment(
    tmp_path: Path, monkeypatch,
) -> None:
    command = _dispatch(tmp_path, monkeypatch, [])
    assert shard_environment(command)["TMPDIR"] == "/home/rob/tmp"


def test_relative_tmpdir_refuses_before_submission(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    for invalid in ("", "scratch", "../scratch"):
        calls = []
        monkeypatch.setattr(
            pbtest.subprocess, "Popen",
            lambda *args, **kwargs: calls.append(args),
        )
        monkeypatch.setattr(sys, "argv", [
            "pbtest.py", "--checkout", str(tmp_path),
            "--python", "/target/python", "--tmpdir", invalid,
        ])
        assert pbtest.main() == 2
        assert not calls
        assert "--tmpdir must be an absolute worker-visible path" in capsys.readouterr().err


def test_two_shards_have_distinct_tmpdir_children_and_output(
    tmp_path: Path, monkeypatch,
) -> None:
    import json

    checkout = tmp_path / "checkout"
    tests = checkout / "tests"
    tests.mkdir(parents=True)
    worker_path = tmp_path / "worker scratch;[literal]"
    worker_path.mkdir()
    for name in ("a", "b"):
        (tests / f"test_{name}.py").write_text(
            "import json, os\n"
            "from pathlib import Path\n"
            "def test_one(tmp_path):\n"
            "    root = Path(os.environ['TMPDIR'])\n"
            "    assert tmp_path.is_relative_to(root)\n"
            "    (tmp_path / 'owned.txt').write_text('owned')\n"
            f"    (root / 'shard-{name}.json').write_text(json.dumps({{\n"
            "        'tmp_path': str(tmp_path), 'pid': os.getpid(),\n"
            "    }))\n"
        )
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    actual_popen = subprocess.Popen
    calls = []

    def execute_admitted_child(command, **kwargs):
        # The test itself is already admitted. Run exactly the prepared child,
        # rather than recursively submitting its mocked pbrun transport.
        calls.append(command)
        argv, environment = admitted_child(command)
        return actual_popen(argv, cwd=checkout, env=environment, **kwargs)

    with monkeypatch.context() as scoped:
        scoped.setattr(pbtest.subprocess, "Popen", execute_admitted_child)
        scoped.setattr(pbtest, "RUNTIME_ROOT", PUBLISHED_RUNTIME)
        scoped.setattr(sys, "argv", [
            "pbtest.py", "--checkout", str(checkout), "--python", sys.executable,
            "--shards", "2", "--workers-per-shard", "1",
            "--threads-per-shard", "1", "--test-timeout-s", "0",
            "--tmpdir", str(worker_path), "tests",
        ])
        assert pbtest.main() == 0
    assert len(calls) == 2
    records = [json.loads((worker_path / f"shard-{name}.json").read_text())
               for name in ("a", "b")]
    assert records[0]["pid"] != records[1]["pid"]
    assert records[0]["tmp_path"] != records[1]["tmp_path"]
    assert all((Path(record["tmp_path"]) / "owned.txt").read_text() == "owned"
               for record in records)
    submitted_files = [
        [part for part in command[command.index("--") + 1:]
         if part.startswith("tests/") and part.endswith(".py")]
        for command in calls
    ]
    assert sorted(submitted_files) == [["tests/test_a.py"], ["tests/test_b.py"]]


def test_explicit_tmpdir_refuses_on_the_worker_before_pytest(
    tmp_path: Path, monkeypatch,
) -> None:
    for kind in ("missing", "file"):
        case = tmp_path / kind
        case.mkdir()
        requested = case / "worker scratch"
        if kind == "file":
            requested.write_text("not a directory")
        with monkeypatch.context() as scoped:
            command = _dispatch(case, scoped, ["--tmpdir", str(requested)])
        checkout = case / "checkout"
        marker = checkout / "pytest-started"
        (checkout / "tests" / "test_one.py").write_text(
            "from pathlib import Path\n"
            "def test_one():\n"
            "    Path('pytest-started').touch()\n"
        )
        payload, environment = admitted_child(command)
        payload[payload.index("/target/python")] = sys.executable
        result = subprocess.run(payload, cwd=checkout, env=environment,
                                capture_output=True, text=True)
        assert result.returncode != 0
        assert "pbtest: cannot use requested --tmpdir:" in result.stderr
        assert not marker.exists()
        assert "pbtest-outcomes" not in result.stdout
        # Causal control: the same worker path under the unchanged legacy
        # entry falls back to another parent and reaches pytest. The explicit
        # preflight, rather than a pytest collection error, causes refusal.
        payload[payload.index("-c") + 1] = pbtest.shard_entry(
            sys.executable, checkout, collection=True,
        )[2]
        legacy = subprocess.run(payload, cwd=checkout, env=environment,
                                capture_output=True, text=True)
        assert legacy.returncode == 0, legacy.stdout + legacy.stderr
        assert marker.exists()


def test_client_help_needs_no_pytest_on_the_coordinator():
    # -S removes installed site packages; the blocker also catches accidental
    # pytest imports through this checkout or an inherited PYTHONPATH.
    script = """
import importlib.abc, runpy, sys
class NoPytest(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "pytest" or fullname.startswith("pytest."):
            raise ImportError("pytest is worker-only")
sys.meta_path.insert(0, NoPytest())
sys.argv = [sys.argv[1], "--help"]
runpy.run_path(sys.argv[0], run_name="__main__")
"""
    result = subprocess.run(
        [sys.executable, "-S", "-c", script, str(ROOT / "tools/fleet/pbtest.py")],
        cwd=ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "--checkout" in result.stdout
    assert "--tag" in result.stdout

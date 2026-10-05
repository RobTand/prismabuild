"""pbtest seals a pytest basetemp separately from the compiler TMPDIR (#1469).

``--tmpdir`` seals the shard's whole process ``TMPDIR``, which is also where
torch's compiler and native caches land, so selecting real test scratch
through TMPDIR silently moves that context too.  ``--basetemp`` is the tests'
own scratch: the coordinator seals the root into the shard's request, and the
worker derives ``ROOT/<action-key>/<attempt>/pytest`` from the action's own
identity and hands that leaf to pytest's ``--basetemp`` -- never the sealed
root itself, because pytest deletes its basetemp at startup.

The sealed root is never injected as ``PYTEST_DEBUG_TEMPROOT``, never
forwarded through an arbitrary ``--env`` addition, and never changes the
compiler TMPDIR: the worker's own plumbing stays on the sealed TMPDIR while
the tests' scratch moves to the action-owned namespace.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from pbtest_shard_output import (  # noqa: E402
    ONE_PASS, ShardProcess, admitted_child, shard_environment)
from test_pbtest import PUBLISHED_RUNTIME, pbtest  # noqa: E402


MARKER_TEST = ("from pathlib import Path\n"
               "def test_one():\n"
               "    Path('pytest-started').touch()\n")

SCRATCH_RECORDER = (
    "import json, os\n"
    "from pathlib import Path\n"
    "def test_one(tmp_path):\n"
    "    (tmp_path / 'sentinel').write_text('kept')\n"
    "    (tmp_path / 'record.json').write_text(json.dumps({\n"
    "        'tmp_path': str(tmp_path), 'pid': os.getpid(),\n"
    "        'debug_temproot': os.environ.get('PYTEST_DEBUG_TEMPROOT'),\n"
    "        'tmpdir': os.environ.get('TMPDIR'),\n"
    "    }))\n"
)

#: A sealed action identity for the worker-side tests: the derivation reads
#: the action's own identity from its environment, so the tests name one
#: instead of depending on what the surrounding action's launcher injected.
TEST_ACTION_KEY = "1" * 64
TEST_ACTION_NONCE = "2" * 32


def _checkout(tmp_path: Path, source: str, name: str = "checkout") -> Path:
    """A one-file git checkout whose single test runs ``source``."""

    checkout = tmp_path / name
    tests = checkout / "tests"
    tests.mkdir(parents=True, exist_ok=True)
    (tests / "test_one.py").write_text(source)
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    return checkout


def _dispatch(tmp_path: Path, monkeypatch, extra: list[str]) -> list[str]:
    """Run one shard's worth of dispatch and return the pbrun argv it built.

    The stand-in Popen patch is scoped to the dispatch, so a test may call
    this repeatedly and still run real subprocesses afterwards.
    """

    checkout = _checkout(tmp_path, "def test_one():\n    assert True\n")
    calls: list[list[str]] = []

    class FinishedProcess(ShardProcess):
        returncode = 0

        def communicate(self):
            return ONE_PASS, None

    def popen(command, **_kwargs):
        calls.append(command)
        return FinishedProcess()

    with monkeypatch.context() as scoped:
        scoped.setattr(pbtest.subprocess, "Popen", popen)
        scoped.setattr(pbtest, "RUNTIME_ROOT", PUBLISHED_RUNTIME)
        scoped.setattr(
            sys, "argv",
            ["pbtest.py", "--checkout", str(checkout), "--python",
             "/target/python", "--shards", "1", *extra, "tests"],
        )
        assert pbtest.main() == 0
    assert len(calls) == 1
    return calls[0]


def _real_dispatch(monkeypatch, checkout: Path, extra: list[str],
                   shards: int = 1) -> list[list[str]]:
    """Dispatch with every shard's payload executed inside this action."""

    actual_popen = subprocess.Popen
    calls: list[list[str]] = []

    def execute_admitted_child(command, **kwargs):
        calls.append(command)
        argv, environment = admitted_child(command)
        return actual_popen(argv, cwd=checkout, env=environment, **kwargs)

    with monkeypatch.context() as scoped:
        scoped.setattr(pbtest.subprocess, "Popen", execute_admitted_child)
        scoped.setattr(pbtest, "RUNTIME_ROOT", PUBLISHED_RUNTIME)
        scoped.setattr(sys, "argv", [
            "pbtest.py", "--checkout", str(checkout), "--python", sys.executable,
            "--shards", str(shards), "--threads-per-shard", "1",
            "--test-timeout-s", "0", *extra, "tests",
        ])
        assert pbtest.main() == 0
    return calls


def _records(root: Path) -> dict[Path, dict]:
    """Every scratch record the run left under the sealed root."""

    return {record.parent: json.loads(record.read_text())
            for record in root.rglob("record.json")}


def test_basetemp_refuses_missing_empty_nul_and_traversal(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """Parser refusals come before any shard, each naming what was wrong."""

    for invalid, why in (
        ("", "nonempty"),
        ("sp\0ot", "NUL"),
        ("../escape", "traverse"),
        ("scratch/../up", "traverse"),
        ("..", "traverse"),
    ):
        calls: list = []
        monkeypatch.setattr(
            pbtest.subprocess, "Popen",
            lambda *args, **kwargs: calls.append(args),
        )
        monkeypatch.setattr(sys, "argv", [
            "pbtest.py", "--checkout", str(tmp_path),
            "--python", "/target/python", "--basetemp", invalid,
        ])
        assert pbtest.main() == 2
        assert not calls, invalid
        err = capsys.readouterr().err
        assert "--basetemp" in err, err
        assert why in err, err


def test_basetemp_requires_a_value(tmp_path: Path, monkeypatch) -> None:
    """A valueless --basetemp is an argparse refusal, like any option."""

    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(tmp_path),
        "--python", "/target/python", "--basetemp",
    ])
    with pytest.raises(SystemExit) as refusal:
        pbtest.main()
    assert refusal.value.code == 2


def test_basetemp_accepts_absolute_and_relative_roots(
    tmp_path: Path, monkeypatch,
) -> None:
    """Both spellings are sealable: absolute for a qualified mount, relative
    for scratch confined to the attempt's own materialized checkout."""

    for root in ("/qualified/scratch", "scratch"):
        command = _dispatch(tmp_path, monkeypatch, ["--basetemp", root])
        program = command[command.index("-c") + 1]
        assert root in program


def test_sealed_basetemp_keeps_the_compiler_tmpdir_environment(
    tmp_path: Path, monkeypatch,
) -> None:
    """The scratch choice is sealed into the request, not the environment."""

    root = "/qualified scratch/$(literal);[mount]"
    command = _dispatch(tmp_path, monkeypatch, ["--basetemp", root])
    program = command[command.index("-c") + 1]
    assert root in program
    environment = shard_environment(command)
    assert environment["TMPDIR"] == "/home/rob/tmp"
    assert not any(name.startswith("PYTEST_DEBUG_TEMPROOT")
                   for name in environment)
    # The root travels inside the sealed program as worker-derived data;
    # pbrun gains no flag for it.
    assert "--basetemp" not in command[:command.index("--")]


def test_basetemp_identity_moves_with_the_sealed_root(
    tmp_path: Path, monkeypatch,
) -> None:
    """A different root is a different sealed request; the same root reseals
    byte for byte, so a re-run stays a cache hit."""

    def program_for(root: str) -> str:
        command = _dispatch(tmp_path, monkeypatch, ["--basetemp", root])
        return command[command.index("-c") + 1]

    assert program_for("/scratch-a") != program_for("/scratch-b")
    assert program_for("/scratch-a") == program_for("/scratch-a")


def test_tmpdir_option_gains_no_basetemp(tmp_path: Path, monkeypatch) -> None:
    """--tmpdir keeps its single meaning: the process TMPDIR, and no more."""

    worker_path = "/worker scratch/$(literal);[directory]"
    command = _dispatch(tmp_path, monkeypatch, ["--tmpdir", worker_path])
    program = command[command.index("-c") + 1]
    assert shard_environment(command)["TMPDIR"] == worker_path
    assert "--basetemp" not in program


def test_unsupported_pytest_options_stay_refused(
    tmp_path: Path, monkeypatch,
) -> None:
    """The closed vocabulary does not grow: callers cannot reach the same
    lever through --pytest-args."""

    with pytest.raises(ValueError, match="unsupported pytest option"):
        pbtest.parse_pytest_args('["--basetemp", "x"]', gpu=False, workers=1)
    with pytest.raises(ValueError, match="unsupported pytest option"):
        pbtest.parse_pytest_args('["-o", "addopts=--basetemp=x"]', gpu=False,
                                 workers=1)


def test_worker_derives_the_action_owned_basetemp(
    tmp_path: Path, monkeypatch,
) -> None:
    """The worker hands pytest ``ROOT/<action-key>/<attempt>/pytest`` -- never
    the sealed root -- and the compiler TMPDIR stays what was sealed."""

    root = tmp_path / "qualified"
    root.mkdir()
    checkout = _checkout(tmp_path, SCRATCH_RECORDER)
    monkeypatch.setenv("PRISMABUILD_ACTION_KEY", TEST_ACTION_KEY)
    monkeypatch.setenv("PRISMABUILD_ACTION_NONCE", TEST_ACTION_NONCE)
    assert _real_dispatch(monkeypatch, checkout, ["--basetemp", str(root)])
    records = _records(root)
    assert len(records) == 1
    record = next(iter(records.values()))
    scratch = Path(record["tmp_path"])
    namespace = scratch.parent.relative_to(root).parts
    assert namespace == (TEST_ACTION_KEY, TEST_ACTION_NONCE, "pytest")
    assert (scratch / "sentinel").read_text() == "kept"
    assert record["debug_temproot"] is None
    assert record["tmpdir"] == "/home/rob/tmp"


def test_xdist_children_remain_under_the_action_root(
    tmp_path: Path, monkeypatch,
) -> None:
    """Every pytest-xdist worker's scratch is inside the one derived leaf."""

    root = tmp_path / "qualified"
    root.mkdir()
    checkout = _checkout(
        tmp_path,
        SCRATCH_RECORDER.replace("test_one", "test_a")
        + SCRATCH_RECORDER.replace("test_one", "test_b"))
    monkeypatch.setenv("PRISMABUILD_ACTION_KEY", TEST_ACTION_KEY)
    monkeypatch.setenv("PRISMABUILD_ACTION_NONCE", TEST_ACTION_NONCE)
    assert _real_dispatch(
        monkeypatch, checkout,
        ["--basetemp", str(root), "--workers-per-shard", "2"])
    records = _records(root)
    assert len(records) == 2
    # Each xdist worker numbers its own popen-gw directory under the one
    # derived leaf, so the workers' common ancestor is the action root.
    derived = {scratch.parent.parent for scratch in records}
    assert len(derived) == 1
    namespace = next(iter(derived)).relative_to(root).parts
    assert namespace == (TEST_ACTION_KEY, TEST_ACTION_NONCE, "pytest")
    for scratch in records:
        assert scratch.parent.name.startswith("popen-gw")
        assert scratch.is_relative_to(next(iter(derived)))
        assert (scratch / "sentinel").read_text() == "kept"


def test_simultaneous_shards_and_attempts_cannot_delete_each_other(
    tmp_path: Path, monkeypatch,
) -> None:
    """Two shards at once, then a second attempt of each: pytest deletes its
    basetemp at startup, so no derived namespace may overlap another's."""

    root = tmp_path / "qualified"
    root.mkdir()
    checkout = tmp_path / "two"
    tests = checkout / "tests"
    tests.mkdir(parents=True)
    for name in ("a", "b"):
        (tests / f"test_{name}.py").write_text(
            SCRATCH_RECORDER.replace("test_one", f"test_{name}"))
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    # Each shard is its own action on the real fleet, so each child gets its
    # own identity here, assigned per shard command; attempt names the nonce
    # so a second run of the same actions is a second attempt.
    identities: list[tuple[str, str]] = []
    assigned: list[int] = []
    actual_popen = subprocess.Popen

    def with_identity(command, **kwargs):
        index = len(assigned)
        assigned.append(index)
        key, nonce = identities[index]
        environment = {
            name: value for name, value in os.environ.items()
            if not name.startswith("PRISMABUILD_ACTION_")}
        environment["PRISMABUILD_ACTION_KEY"] = key
        environment["PRISMABUILD_ACTION_NONCE"] = nonce
        argv, shard_env = admitted_child(command)
        return actual_popen(argv, cwd=checkout,
                            env={**environment, **shard_env}, **kwargs)

    def run_once(attempt: str) -> dict[Path, dict]:
        identities.clear()
        identities.extend([("a" * 64, attempt + "0" * 31),
                           ("c" * 64, attempt + "f" * 31)])
        assigned.clear()
        with monkeypatch.context() as scoped:
            scoped.setattr(pbtest.subprocess, "Popen", with_identity)
            scoped.setattr(pbtest, "RUNTIME_ROOT", PUBLISHED_RUNTIME)
            scoped.setattr(sys, "argv", [
                "pbtest.py", "--checkout", str(checkout),
                "--python", sys.executable,
                "--shards", "2", "--threads-per-shard", "1",
                "--test-timeout-s", "0", "--basetemp", str(root), "tests",
            ])
            assert pbtest.main() == 0
        return _records(root)

    first = run_once("b")
    assert len(first) == 2
    assert len({scratch.parent for scratch in first}) == 2, first

    # Same shards, second attempt each: fresh namespaces, first untouched.
    second = run_once("9")
    assert len(second) == 4
    second_new = {scratch: record for scratch, record in second.items()
                  if scratch not in first}
    assert len(second_new) == 2
    assert not ({scratch.parent for scratch in first}
                & {scratch.parent for scratch in second_new})
    for scratch, record in first.items():
        assert (scratch / "sentinel").read_text() == "kept", record

    # Each namespace nests under its own action key and attempt component.
    for scratch in second_new:
        namespace = scratch.parent.relative_to(root).parts
        assert namespace[0] in {key for key, _ in identities}
        assert namespace[1].startswith("9")
        assert namespace[2] == "pytest"

    # The sentinels survive every later wipe in every other namespace.
    for scratch in first:
        assert (scratch / "sentinel").read_text() == "kept"
    for scratch, record in second_new.items():
        assert (scratch / "sentinel").read_text() == "kept", record


def test_unusable_basetemp_fails_before_pytest(
    tmp_path: Path, monkeypatch,
) -> None:
    """A missing, non-directory or symlinked root refuses on the worker,
    before pytest starts, with no fallback."""

    real_target = tmp_path / "real"
    real_target.mkdir()
    symlinked = tmp_path / "link"
    symlinked.symlink_to(real_target, target_is_directory=True)
    requested_by_kind = {
        "missing": tmp_path / "absent" / "scratch",
        "file": tmp_path / "plain",
        "symlink": symlinked,
    }
    for kind, requested in requested_by_kind.items():
        if kind == "file":
            requested.write_text("not a directory")
        case = tmp_path / f"case-{kind}"
        case.mkdir()
        command = _dispatch(case, monkeypatch, ["--basetemp", str(requested)])
        checkout = case / "checkout"
        (checkout / "tests" / "test_one.py").write_text(MARKER_TEST)
        payload, environment = admitted_child(command)
        payload[payload.index("/target/python")] = sys.executable
        result = subprocess.run(payload, cwd=checkout, env=environment,
                                capture_output=True, text=True)
        assert result.returncode != 0, kind
        assert "pbtest: cannot use requested --basetemp:" in result.stderr
        assert not (checkout / "pytest-started").exists(), kind
        assert "pbtest-outcomes" not in result.stdout, kind


def test_unowned_basetemp_root_refuses(tmp_path: Path, monkeypatch) -> None:
    """A root this action's user does not own is refused with no fallback;
    the same root under the action's own user derives and seals the pair."""

    root = tmp_path / "qualified"
    root.mkdir()
    source = pbtest.shard_entry(sys.executable, tmp_path,
                                basetemp=str(root), collection=True)[2]
    source = source.split("# A pbtest shard:", 1)[0]
    argv = ["-c", json.dumps({}), "-q", "tests/test_one.py"]
    monkeypatch.setattr(sys, "argv", list(argv))
    monkeypatch.setenv("PRISMABUILD_ACTION_KEY", "testaction")
    monkeypatch.delenv("PRISMABUILD_ACTION_NONCE", raising=False)
    monkeypatch.setattr(os, "geteuid", lambda: 9999)
    with pytest.raises(SystemExit, match="owned by uid"):
        exec(compile(source, "<pbtest basetemp>", "exec"), {"__name__": "x"})
    assert sys.argv == argv

    monkeypatch.setattr(os, "geteuid", lambda: os.stat(root).st_uid)
    exec(compile(source, "<pbtest basetemp>", "exec"), {"__name__": "x"})
    assert sys.argv[2] == "--basetemp"
    derived = Path(sys.argv[3])
    namespace = derived.relative_to(root).parts
    assert namespace[0] == "testaction" and namespace[2] == "pytest"
    assert len(namespace[1]) == 32
    assert derived.is_dir()
    assert sys.argv[4:] == argv[2:]


def test_missing_action_identity_refuses_before_pytest(
    tmp_path: Path, monkeypatch,
) -> None:
    """Without the action's own identity the worker cannot own a namespace,
    so it refuses rather than deriving somebody else's scratch."""

    root = tmp_path / "qualified"
    root.mkdir()
    command = _dispatch(tmp_path, monkeypatch, ["--basetemp", str(root)])
    checkout = tmp_path / "checkout"
    (checkout / "tests" / "test_one.py").write_text(MARKER_TEST)
    payload, environment = admitted_child(command)
    payload[payload.index("/target/python")] = sys.executable
    environment.pop("PRISMABUILD_ACTION_KEY", None)
    result = subprocess.run(payload, cwd=checkout, env=environment,
                            capture_output=True, text=True)
    assert result.returncode != 0
    assert "PRISMABUILD_ACTION_KEY" in result.stderr
    assert not (checkout / "pytest-started").exists()

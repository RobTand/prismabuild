"""Where an action may run is derived from the checkout, not asked of the caller."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

# Imported BEFORE ``pbrun`` is exec'd, on purpose.  ``pbrun`` puts the
# published mirror (``/mnt/shared/prismabuild-fleet/repo/src``) at the front of
# ``sys.path`` so a submitter runs the fleet's bytes, which means a bare
# ``pytest tests/test_pbrun_placement.py`` would otherwise test THIS checkout's
# pbrun against the MIRROR's pool -- and report a missing method as a failure
# of code that is right here.  Binding the package first makes the file
# self-contained however it is invoked.
from prismabuild import core as core_module  # noqa: E402
from prismabuild import pool as pool_module  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "pbrun", Path(__file__).resolve().parents[1] / "tools" / "fleet" / "pbrun.py"
)
pbrun = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbrun)                       # type: ignore[union-attr]

HOST = "sparky"


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(cwd), *args], capture_output=True, text=True,
        check=False,
    )


#: A runtime every box can open.  These tests submit from ``tmp_path``, which
#: is box-local, so without saying otherwise they would also be asserting that
#: a worktree ``pbrun`` may send work to another box -- and it may not
#: (``require_reachable_runtime``, #292).  Placement identity is the subject
#: here; where this pbrun is installed is not, so it is declared rather than
#: inherited from wherever the suite happens to be checked out.
PUBLISHED_RUNTIME = Path(
    "/mnt/shared/prismabuild-fleet/runtime-generations/test-generation")

def _git_checkout(tmp_path: Path) -> Path:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    assert _git(checkout, "init", "-q").returncode == 0
    assert _git(checkout, "config", "user.email", "test@example.invalid").returncode == 0
    assert _git(checkout, "config", "user.name", "PrismaBuild test").returncode == 0
    (checkout / "task.py").write_text("VALUE = 'sealed'\n")
    assert _git(checkout, "add", "task.py").returncode == 0
    assert _git(checkout, "commit", "-qm", "sealed tree").returncode == 0
    return checkout


def _sealed_pbrun_action(checkout: Path, stamp_name: str) -> dict[str, object]:
    return core_module.seal_action(
        {
            "schema": core_module.ACTION_SCHEMA_V2,
            "task": {
                "definition_id": "fleet/pbrun",
                "definition_version": "v1",
                "task_class": "generation",
                "determinism": "stochastic",
                "artifact_family": "generic",
                "artifact_kind": "generic",
                "argv": ["/bin/true"],
                "working_directory": ".",
                "result_path": "result.txt",
            },
            "inputs": [],
            "code_closure": core_module.build_code_closure(
                checkout, [stamp_name]
            ),
            "params": {
                "command": ["/bin/true"],
                "cwd": str(checkout),
                "demand": {"cpu": 1},
            },
            "environment": {"variables": {}, "toolchain": {}},
            "execution_scope": {
                "portability": "portable",
                "platform_key": None,
                "host_class": None,
            },
        }
    )


def _tags(cwd: str, **kw: object) -> list[str]:
    return pbrun.placement_tags(
        Path(cwd),
        explicit=kw.pop("explicit", []),          # type: ignore[arg-type]
        here=bool(kw.pop("here", False)),
        hostname=HOST,
    )


def test_a_shared_checkout_is_free_to_run_on_any_box() -> None:
    """The case that used to need a flag nobody remembered.

    A checkout under the shared mount is at the same path on every box, so
    pinning it to the submitter is a pure loss: the work ran correctly on one
    box while the others sat idle, and nothing anywhere reported that.  An
    empty tag list means the queue places it from the demand alone.
    """

    assert _tags("/mnt/shared/tessera-x86") == []
    assert _tags("/mnt/shared/prismabuild-fleet/repo") == []


def test_a_box_local_checkout_is_pinned_to_that_box() -> None:
    """The pin is a fact about the path, not a preference.

    ``/home/rob/tessera`` exists on every box and holds *different* bytes on
    each; an action that runs there and lands elsewhere does not fail loudly,
    it silently operates on another box's tree.
    """

    assert _tags("/home/rob/tessera") == [HOST]
    assert _tags("/home/rob/tmp/ts50") == [HOST]


def test_portable_snapshot_keeps_a_box_local_executable_host_pin(
    tmp_path: Path,
) -> None:
    """Transporting source does not transport a user-local interpreter."""

    checkout = _git_checkout(tmp_path)
    interpreter = tmp_path / "venv" / "bin" / "python"
    interpreter.parent.mkdir(parents=True)
    interpreter.write_bytes(b"ELF test executable")
    interpreter.chmod(0o755)

    assert pbrun.placement_tags(
        checkout,
        explicit=[],
        here=False,
        hostname=HOST,
        portable_checkout=True,
        command=[str(interpreter), "-V"],
        repository_root=checkout,
        environment={"PATH": "/usr/bin:/bin"},
    ) == [HOST]


def test_explicit_tag_owns_a_missing_external_executable_path(
    tmp_path: Path,
) -> None:
    """A caller may name the worker class that owns a box-absent interpreter."""

    checkout = _git_checkout(tmp_path)
    assert pbrun.placement_tags(
        checkout,
        explicit=["dl380g10"],
        here=False,
        hostname=HOST,
        portable_checkout=True,
        command=["/home/rob/venvs/pb-cpu/bin/python", "-V"],
        repository_root=checkout,
        environment={"PATH": "/usr/bin:/bin"},
    ) == ["dl380g10"]


def test_portable_snapshot_refuses_an_unplaced_missing_executable(
    tmp_path: Path,
) -> None:
    checkout = _git_checkout(tmp_path)

    with pytest.raises(SystemExit, match="--tag"):
        pbrun.placement_tags(
            checkout,
            explicit=[],
            here=False,
            hostname=HOST,
            portable_checkout=True,
            command=["/home/rob/venvs/missing/bin/python", "-V"],
            repository_root=checkout,
            environment={"PATH": "/usr/bin:/bin"},
        )


def test_portable_snapshot_resolves_a_bare_executable_through_declared_path(
    tmp_path: Path,
) -> None:
    checkout = _git_checkout(tmp_path)
    binary = tmp_path / "venv" / "bin" / "python3"
    binary.parent.mkdir(parents=True)
    binary.write_text("#!/bin/sh\nexit 0\n")
    binary.chmod(0o755)

    assert pbrun.placement_tags(
        checkout,
        explicit=[],
        here=False,
        hostname=HOST,
        portable_checkout=True,
        command=["python3", "-V"],
        repository_root=checkout,
        environment={"PATH": str(binary.parent)},
    ) == [HOST]


def test_portable_snapshot_pins_a_direct_flag_value_outside_the_snapshot(
    tmp_path: Path,
) -> None:
    checkout = _git_checkout(tmp_path)
    executable = checkout / "task.py"
    executable.chmod(0o755)
    model = tmp_path / "model" / "config.json"
    model.parent.mkdir()
    model.write_text("{}\n")

    assert pbrun.placement_tags(
        checkout,
        explicit=[],
        here=False,
        hostname=HOST,
        portable_checkout=True,
        command=["./task.py", f"--model={model}"],
        repository_root=checkout,
        environment={"PATH": "/usr/bin:/bin"},
    ) == [HOST]


def test_portable_snapshot_screens_caller_environment_paths(tmp_path: Path) -> None:
    checkout = _git_checkout(tmp_path)
    executable = checkout / "task.py"
    executable.chmod(0o755)
    cache = tmp_path / "model-cache"
    cache.mkdir()

    assert pbrun.placement_tags(
        checkout,
        explicit=[],
        here=False,
        hostname=HOST,
        portable_checkout=True,
        command=["./task.py"],
        repository_root=checkout,
        environment={"PATH": "/usr/bin:/bin"},
        caller_environment={"MODEL_CACHE": str(cache)},
    ) == [HOST]


def test_anywhere_is_an_explicit_external_portability_assertion(tmp_path: Path) -> None:
    checkout = _git_checkout(tmp_path)

    assert pbrun.placement_tags(
        checkout,
        explicit=[],
        here=False,
        hostname=HOST,
        portable_checkout=True,
        command=["/worker/owned/python", "--model=/worker/owned/model"],
        repository_root=checkout,
        environment={"PATH": "/usr/bin:/bin"},
        anywhere=True,
    ) == []


def test_here_pins_a_shared_checkout_on_purpose() -> None:
    assert _tags("/mnt/shared/tessera-x86", here=True) == [HOST]


def test_an_explicit_tag_wins_because_only_the_caller_knows_it() -> None:
    """A hardware class the work requires is the one thing the path cannot say."""

    assert _tags("/mnt/shared/tessera-x86", explicit=["x86"]) == ["x86"]
    assert _tags("/home/rob/tessera", explicit=["x86"]) == ["x86"]


def test_a_symlink_into_shared_storage_is_still_shared() -> None:
    """Placement follows the resolved path; a link must not change the answer."""

    assert _tags("/mnt/shared/./tessera-x86/../tessera-x86") == []


@pytest.mark.parametrize("cwd", ["/mnt/shared", "/mnt/shared-other/tree", "/mnt"])
def test_only_paths_under_the_shared_root_count(cwd: str) -> None:
    """``/mnt/shared`` itself is shared; a sibling that merely starts with the
    same characters is not.  ``relative_to`` compares path components, which is
    the reason to use it here rather than a string prefix."""

    assert _tags(cwd) == ([] if cwd == "/mnt/shared" else [HOST])


def test_the_result_and_stamp_names_move_with_the_commit(tmp_path, monkeypatch):
    """Two commits must not share one result file.

    ``pbrun_result.*.txt`` is the action's *declared* output: the runner
    refuses with "action succeeded without its declared result file" if it is
    not there when the action finishes.  Naming it from the command alone gave
    one checkout one result path forever, so a long run at one commit and its
    re-run at the next wrote the same file and the second destroyed the
    first's -- a green 1268-test suite, lost that way on 2026-09-04.  The
    closure stamp has the same shape of problem from the other end: its
    *content* is the commit, so a rewrite under a worker still verifying the
    previous action reads as a closure mismatch.
    """
    seen = []

    def identity(_cwd, _seen=seen):
        return {"commit": _seen.pop(0)}

    monkeypatch.setattr(pbrun, "_git_identity", identity)
    names = []
    for commit in ("aaaa", "bbbb", "aaaa"):
        seen.append(commit)
        names.append(pbrun.result_and_stamp_names(
            ["pytest", "-q"], tmp_path, {"cpu": 1}, {"LANG": "C.UTF-8"}))
    assert names[0] != names[1], "two commits shared one result path"
    assert names[0] == names[2], "the same commit must still dedup"


def test_portable_submission_identity_ignores_the_source_checkout_path() -> None:
    """Two clones of one tree must seal the same action, not merely run it."""

    identity = {"head": "a" * 40, "dirty_sha256": "b" * 64}
    command = ["python", "task.py"]
    demand = {"cpu": 1}
    variables = {"LANG": "C.UTF-8"}
    first = pbrun.result_and_stamp_names(
        command, Path("/home/rob/tmp/first"), demand, variables,
        identity=identity, logical_cwd=".",
    )
    second = pbrun.result_and_stamp_names(
        command, Path("/mnt/shared/second"), demand, variables,
        identity=identity, logical_cwd=".",
    )

    assert first == second
    assert pbrun.container_owner(
        command, Path("/home/rob/tmp/first"), demand, variables,
        determinism="stochastic",
        retry_policy={"max_attempts": 1, "retry_safe": False},
        marker_root="/mnt/shared/prismabuild-fleet/pb-queue/container-owners",
        identity=identity, logical_cwd=".",
    ) == pbrun.container_owner(
        command, Path("/mnt/shared/second"), demand, variables,
        determinism="stochastic",
        retry_policy={"max_attempts": 1, "retry_safe": False},
        marker_root="/mnt/shared/prismabuild-fleet/pb-queue/container-owners",
        identity=identity, logical_cwd=".",
    )


def test_effective_placement_is_normalized_into_action_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Different admissible worker populations cannot share a CAS result."""

    checkout = _git_checkout(tmp_path)
    fleet = tmp_path / "fleet"
    sealed: list[dict[str, object]] = []
    real_seal = core_module.seal_action

    class StopAfterSeal(Exception):
        pass

    def capture(body):
        sealed.append(real_seal(body))
        raise StopAfterSeal

    monkeypatch.setattr(pbrun.pb, "seal_action", capture)
    monkeypatch.setattr(pbrun, "SH", fleet)
    monkeypatch.setattr(pbrun, "RUNTIME_ROOT", PUBLISHED_RUNTIME)
    monkeypatch.setattr(
        pbrun, "CONTAINER_WRAPPER_DIR", fleet / "repo" / "tools"
    )

    populations = [
        ["x86", "dl380g10", "x86"],
        ["dl380g10", "x86"],
        ["sparky"],
    ]
    for tags in populations:
        argv = ["pbrun.py", "--cwd", str(checkout), "--wait-s", "0"]
        for tag in tags:
            argv.extend(["--tag", tag])
        argv.extend(["--", "true"])
        monkeypatch.setattr(sys, "argv", argv)
        with pytest.raises(StopAfterSeal):
            pbrun.main()

    assert sealed[0]["action_key"] != sealed[2]["action_key"], (
        "different effective placement populations shared one action key"
    )
    assert sealed[0]["action_key"] == sealed[1]["action_key"]
    placements = [action["params"]["placement"] for action in sealed]
    assert placements == [
        {"required_tags": ["dl380g10", "x86"]},
        {"required_tags": ["dl380g10", "x86"]},
        {"required_tags": ["sparky"]},
    ]
    owners = [
        action["environment"]["variables"]["PRISMABUILD_CONTAINER_OWNER"]
        for action in sealed
    ]
    assert owners[0] == owners[1]
    assert owners[0] != owners[2]
    results = [action["task"]["result_path"] for action in sealed]
    assert results[0] == results[1]
    assert results[0] != results[2]


def test_container_owner_tracks_every_pre_owner_semantic_distinction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Distinct actions cannot share one Docker cleanup namespace."""

    checkout = _git_checkout(tmp_path)
    fleet = tmp_path / "fleet"
    sealed: list[dict[str, object]] = []
    real_seal = core_module.seal_action

    class StopAfterSeal(Exception):
        pass

    def capture(body):
        sealed.append(real_seal(body))
        raise StopAfterSeal

    monkeypatch.setattr(pbrun.pb, "seal_action", capture)
    monkeypatch.setattr(pbrun, "SH", fleet)
    monkeypatch.setattr(pbrun, "RUNTIME_ROOT", PUBLISHED_RUNTIME)
    monkeypatch.setattr(
        pbrun, "CONTAINER_WRAPPER_DIR", fleet / "repo" / "tools"
    )

    policies = [
        [],
        [],  # exact repeat
        ["--deterministic"],
        ["--retry-safe"],
        ["--retry-safe", "--max-attempts", "2"],
        ["--retry-safe", "--max-attempts", "3"],
    ]
    for policy in policies:
        monkeypatch.setattr(
            sys,
            "argv",
            ["pbrun.py", "--cwd", str(checkout), "--wait-s", "0", *policy,
             "--", "true"],
        )
        with pytest.raises(StopAfterSeal):
            pbrun.main()

    # Runtime publication changes the wrapper path sealed in both argv and
    # PATH. A still-running action from the prior runtime must keep a distinct
    # cleanup namespace during that overlap window.
    monkeypatch.setattr(
        pbrun, "CONTAINER_WRAPPER_DIR", fleet / "next-runtime" / "tools"
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["pbrun.py", "--cwd", str(checkout), "--wait-s", "0", "--", "true"],
    )
    with pytest.raises(StopAfterSeal):
        pbrun.main()

    keys = [str(action["action_key"]) for action in sealed]
    owners = [
        str(action["environment"]["variables"]["PRISMABUILD_CONTAINER_OWNER"])
        for action in sealed
    ]
    assert keys[0] == keys[1] and owners[0] == owners[1]
    assert len(set(keys)) == len(sealed) - 1
    assert len(set(owners)) == len(sealed) - 1, (
        "an action-key semantic distinction shared a Docker cleanup owner"
    )


def test_pbrun_preflight_refuses_checkout_drift_after_sealing(tmp_path) -> None:
    """The worker must verify what the stamp says, not only the stamp bytes.

    A retry of Tessera action ``6c90ba1b`` kept its original stamp while the
    checkout advanced, then executed and published under the old action key.
    This is that race without a queue: seal, change a tracked source file, and
    ask the trusted worker preflight whether it may launch the argv.
    """

    checkout = _git_checkout(tmp_path)
    stamp_name = f"{pbrun.STAMP_PREFIX}test.json"
    (checkout / stamp_name).write_text(
        json.dumps({"cwd": str(checkout), **pbrun._git_identity(checkout)})
    )
    action = _sealed_pbrun_action(checkout, stamp_name)

    (checkout / "task.py").write_text("VALUE = 'changed after seal'\n")

    with pytest.raises(
        core_module.ActionContractError,
        match="live pbrun checkout identity differs from its sealed stamp",
    ):
        core_module.preflight_action(
            action,
            cas_root=tmp_path / "cas",
            checkout_root=checkout,
        )


def test_pbrun_identity_includes_bytes_below_an_untracked_directory(tmp_path) -> None:
    """Git abbreviates an untracked tree as ``?? directory/`` by default.

    The old identity skipped directory entries on the mistaken premise that
    porcelain expands them, so changing an untracked helper below one left the
    action key unchanged. The identity must move with the helper's bytes.
    """

    checkout = _git_checkout(tmp_path)
    helper = checkout / "experiments" / "campaign.py"
    helper.parent.mkdir()
    helper.write_text("print('first')\n")
    before = pbrun._git_identity(checkout)

    helper.write_text("print('second')\n")

    assert pbrun._git_identity(checkout) != before


def test_pbrun_identity_hashes_untracked_symlink_to_directory_text(tmp_path) -> None:
    """A directory-target symlink is a file whose payload is its link text.

    ``Path.is_dir()`` follows the link, so the old identity silently omitted
    this untracked member.  Retargeting it could therefore change which tree a
    command reads without moving the action key.
    """

    checkout = _git_checkout(tmp_path)
    (tmp_path / "outside-a").mkdir()
    (tmp_path / "outside-b").mkdir()
    link = checkout / "helper-tree"
    link.symlink_to("../outside-a", target_is_directory=True)
    before = pbrun._git_identity(checkout)

    link.unlink()
    link.symlink_to("../outside-b", target_is_directory=True)

    assert pbrun._git_identity(checkout) != before


def test_pbrun_identity_hashes_symlink_text_not_target_contents(tmp_path) -> None:
    """Equal target bytes do not make two different symlinks equivalent."""

    checkout = _git_checkout(tmp_path)
    (tmp_path / "outside-a.py").write_text("print('same')\n")
    (tmp_path / "outside-b.py").write_text("print('same')\n")
    link = checkout / "helper.py"
    link.symlink_to("../outside-a.py")
    before = pbrun._git_identity(checkout)

    link.unlink()
    link.symlink_to("../outside-b.py")

    assert pbrun._git_identity(checkout) != before


def test_pbrun_identity_refuses_an_untracked_fifo_without_opening_it(
    tmp_path: Path,
) -> None:
    """A special inode is neither stable payload bytes nor safe to open."""

    checkout = _git_checkout(tmp_path)
    os.mkfifo(checkout / "blocked.pipe")
    repository_root = Path(__file__).resolve().parents[1]
    program = """
from prismabuild import core
import sys
try:
    core.git_checkout_identity(sys.argv[1])
except core.ActionContractError as exc:
    print(exc, file=sys.stderr)
    raise SystemExit(2)
raise SystemExit('accepted an untracked FIFO')
"""
    completed = subprocess.run(
        [sys.executable, "-c", program, str(checkout)],
        capture_output=True,
        text=True,
        timeout=1,
        env={**os.environ, "PYTHONPATH": str(repository_root / "src")},
    )

    assert completed.returncode == 2
    assert "unsupported file type" in completed.stderr


def test_pbrun_reports_an_unsupported_identity_without_a_traceback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout = _git_checkout(tmp_path)

    def refuse(_root):
        raise core_module.ActionContractError("unsupported file type: 'socket'")

    monkeypatch.setattr(core_module, "git_checkout_identity", refuse)
    with pytest.raises(SystemExit, match="unsupported file type"):
        pbrun._git_identity(checkout)


def test_pbrun_identity_prunes_git_ignored_directories(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The special-file scan must not descend into excluded cache trees."""

    checkout = _git_checkout(tmp_path)
    (checkout / ".gitignore").write_text("ignored-cache/\n")
    assert _git(checkout, "add", ".gitignore").returncode == 0
    assert _git(checkout, "commit", "-qm", "ignore generated cache").returncode == 0
    ignored = checkout / "ignored-cache"
    ignored.mkdir()
    os.mkfifo(ignored / "worker.pipe")
    visited: list[Path] = []
    real_scandir = os.scandir

    def observed_scandir(path):
        if not isinstance(path, int):
            visited.append(Path(path))
        return real_scandir(path)

    monkeypatch.setattr(core_module.os, "scandir", observed_scandir)
    pbrun._git_identity(checkout)

    assert checkout in visited
    assert ignored not in visited


def test_pbrun_identity_hashes_untracked_nul_delimited_paths(tmp_path: Path) -> None:
    """Git owns pathname decoding; C-quoted porcelain is not a filesystem path."""

    checkout = _git_checkout(tmp_path)
    unusual = checkout / 'line\nbreak\\quote".txt'
    unusual.write_text("first bytes\n")
    before = pbrun._git_identity(checkout)

    unusual.write_text("second bytes\n")

    assert pbrun._git_identity(checkout) != before


def test_pbrun_identity_does_not_hide_legitimate_prefix_paths(tmp_path: Path) -> None:
    """Only generated basenames reserve pbrun's stamp/result namespaces."""

    checkout = _git_checkout(tmp_path)
    pbrun.keep_droppings_out_of_git(checkout)
    note = checkout / "notes" / "pbrun_result.notes.py"
    note.parent.mkdir()
    note.write_text("first bytes\n")
    before = pbrun._git_identity(checkout)

    note.write_text("second bytes\n")

    assert pbrun._git_identity(checkout) != before


def test_pbrun_identity_scans_the_repository_above_requested_cwd(
    tmp_path: Path,
) -> None:
    """A repo-sibling special inode must not disappear from subdir identity."""

    checkout = _git_checkout(tmp_path)
    requested = checkout / "package"
    requested.mkdir()
    os.mkfifo(checkout / "outside-requested-cwd.pipe")

    with pytest.raises(SystemExit, match="unsupported file type"):
        pbrun._git_identity(requested)


def test_pbrun_identity_refuses_an_unreadable_untracked_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient read failure is not a stable substitute for payload bytes."""

    checkout = _git_checkout(tmp_path)
    payload = checkout / "unreadable.bin"
    payload.write_bytes(b"bytes that identity must bind")
    real_open = Path.open

    def unreadable(candidate, *args, **kwargs):
        if candidate == payload and args and args[0] == "rb":
            raise PermissionError("simulated read refusal")
        return real_open(candidate, *args, **kwargs)

    monkeypatch.setattr(Path, "open", unreadable)
    with pytest.raises(SystemExit, match="cannot hash untracked path"):
        pbrun._git_identity(checkout)


@pytest.mark.parametrize("failed_git_verb", ["ls-files", "diff-index"])
def test_pbrun_identity_fails_closed_after_git_repository_detection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failed_git_verb: str,
) -> None:
    """A later Git error cannot collapse a repository delta to empty text."""

    checkout = _git_checkout(tmp_path)
    real_run = core_module.subprocess.run

    def fail_one_git_read(argv, *args, **kwargs):
        if (
            argv[:3] == ["git", "-C", str(checkout)]
            and failed_git_verb in argv[3:]
        ):
            return subprocess.CompletedProcess(
                argv, 1, stdout="", stderr="simulated Git read failure"
            )
        return real_run(argv, *args, **kwargs)

    monkeypatch.setattr(core_module.subprocess, "run", fail_one_git_read)
    with pytest.raises(SystemExit, match="cannot compute pbrun checkout identity"):
        pbrun._git_identity(checkout)


def test_pbrun_identity_refuses_failed_initial_git_detection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A visible .git marker cannot be downgraded by transient rev-parse failure."""

    checkout = _git_checkout(tmp_path)
    real_run = core_module.subprocess.run

    def fail_toplevel(argv, *args, **kwargs):
        if (
            argv[:3] == ["git", "-C", str(checkout)]
            and "--show-toplevel" in argv
        ):
            return subprocess.CompletedProcess(
                argv, 1, stdout="", stderr="simulated initial Git failure"
            )
        return real_run(argv, *args, **kwargs)

    monkeypatch.setattr(core_module.subprocess, "run", fail_toplevel)
    with pytest.raises(SystemExit, match="cannot compute pbrun checkout identity"):
        pbrun._git_identity(checkout)
def test_git_snapshot_moves_with_nested_untracked_bytes(tmp_path) -> None:
    """The portable tree is the submitted dirty tree, not merely ``HEAD``."""

    checkout = _git_checkout(tmp_path)
    helper = checkout / "experiments" / "campaign.py"
    helper.parent.mkdir()
    helper.write_text("print('first')\n")
    stamp_name = f"{pbrun.STAMP_PREFIX}snapshot-test.json"
    (checkout / stamp_name).write_text(
        json.dumps({"cwd": str(checkout), **pbrun._git_identity(checkout)})
    )
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")
    first = pbrun.build_git_checkout_snapshot(
        checkout, stamp_name=stamp_name, cas=cas, max_bytes=16 * 1024 * 1024
    )

    helper.write_text("print('second')\n")
    (checkout / stamp_name).write_text(
        json.dumps({"cwd": str(checkout), **pbrun._git_identity(checkout)})
    )
    second = pbrun.build_git_checkout_snapshot(
        checkout, stamp_name=stamp_name, cas=cas, max_bytes=16 * 1024 * 1024
    )

    assert first["commit"] != second["commit"]
    assert first["input"] != second["input"]


def test_git_snapshot_keeps_a_tracked_file_that_now_matches_ignore(
    tmp_path: Path,
) -> None:
    """The synthetic index starts from HEAD before overlaying live bytes."""

    checkout = _git_checkout(tmp_path)
    ignored = checkout / "tracked.cache"
    ignored.write_text("still part of the checkout\n")
    (checkout / ".gitignore").write_text("*.cache\n")
    assert _git(checkout, "add", ".gitignore").returncode == 0
    assert _git(checkout, "add", "-f", "tracked.cache").returncode == 0
    assert _git(checkout, "commit", "-qm", "track ignored input").returncode == 0
    stamp_name = f"{pbrun.STAMP_PREFIX}ignored-test.json"
    (checkout / stamp_name).write_text(
        json.dumps({"cwd": str(checkout), **pbrun._git_identity(checkout)})
    )
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")

    snapshot = pbrun.build_git_checkout_snapshot(
        checkout, stamp_name=stamp_name, cas=cas, max_bytes=16 * 1024 * 1024
    )
    materialized = tmp_path / "materialized"
    bundle = cas.input_path(snapshot["input"])
    materialized.mkdir()
    assert _git(materialized, "init", "-q").returncode == 0
    assert _git(
        materialized,
        "fetch",
        "-q",
        str(bundle),
        "refs/heads/prismabuild-snapshot",
    ).returncode == 0
    assert _git(
        materialized, "checkout", "-q", "--detach", str(snapshot["commit"])
    ).returncode == 0

    assert (materialized / "tracked.cache").read_text() == (
        "still part of the checkout\n"
    )


def test_git_snapshot_from_a_linked_worktree_is_self_contained(
    tmp_path: Path,
) -> None:
    """A box-local Git common dir must not leak into worker materialization."""

    primary = _git_checkout(tmp_path)
    linked = tmp_path / "linked"
    assert _git(
        primary, "worktree", "add", "-q", "--detach", str(linked)
    ).returncode == 0
    assert (linked / ".git").is_file()
    common_dir = Path(_git(linked, "rev-parse", "--git-common-dir").stdout.strip())
    assert common_dir.is_absolute()
    assert linked not in common_dir.parents
    assert pbrun.placement_tags(
        linked,
        explicit=[],
        here=False,
        hostname=HOST,
        portable_checkout=True,
    ) == []

    (linked / "linked-only.txt").write_text("sealed linked worktree bytes\n")
    stamp_name = f"{pbrun.STAMP_PREFIX}linked-test.json"
    (linked / stamp_name).write_text(
        json.dumps({"cwd": ".", **pbrun._git_identity(linked)})
    )
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")
    snapshot = pbrun.build_git_checkout_snapshot(
        linked, stamp_name=stamp_name, cas=cas, max_bytes=16 * 1024 * 1024
    )

    materialized = tmp_path / "materialized-linked"
    materialized.mkdir()
    assert _git(materialized, "init", "-q").returncode == 0
    assert _git(
        materialized,
        "fetch",
        "-q",
        str(cas.input_path(snapshot["input"])),
        "refs/heads/prismabuild-snapshot",
    ).returncode == 0
    assert _git(
        materialized, "checkout", "-q", "--detach", str(snapshot["commit"])
    ).returncode == 0
    assert (materialized / "linked-only.txt").read_text() == (
        "sealed linked worktree bytes\n"
    )


def test_git_snapshot_limits_logical_tree_bytes_after_indexing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The authoritative written tree retains the worker expansion bound."""

    checkout = _git_checkout(tmp_path)
    (checkout / "mostly-zero.bin").write_bytes(b"\0" * (256 * 1024))
    stamp_name = f"{pbrun.STAMP_PREFIX}logical-size-test.json"
    (checkout / stamp_name).write_text(
        json.dumps({"cwd": str(checkout), **pbrun._git_identity(checkout)})
    )
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")

    monkeypatch.setattr(pbrun, "require_working_tree_size", lambda *_a, **_kw: 0)
    with pytest.raises(SystemExit, match="logical checkout tree"):
        pbrun.build_git_checkout_snapshot(
            checkout, stamp_name=stamp_name, cas=cas, max_bytes=32 * 1024
        )


def test_git_snapshot_refuses_oversize_before_git_add_hashes_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The submitter bound fires before Git hashes a sparse or huge file."""

    checkout = _git_checkout(tmp_path)
    oversized = checkout / "sparse-cache.bin"
    with oversized.open("wb") as handle:
        handle.truncate(256 * 1024)
    stamp_name = f"{pbrun.STAMP_PREFIX}early-size-test.json"
    (checkout / stamp_name).write_text(
        json.dumps({"cwd": str(checkout), **pbrun._git_identity(checkout)})
    )
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")
    real_snapshot_git = pbrun._snapshot_git

    def observed_snapshot_git(cwd, argv, **kwargs):
        if argv[:2] == ["add", "-A"]:
            pytest.fail("git add -A ran before the logical-size refusal")
        return real_snapshot_git(cwd, argv, **kwargs)

    monkeypatch.setattr(pbrun, "_snapshot_git", observed_snapshot_git)
    with pytest.raises(SystemExit, match="logical working tree"):
        pbrun.build_git_checkout_snapshot(
            checkout, stamp_name=stamp_name, cas=cas, max_bytes=32 * 1024
        )


def test_git_snapshot_limit_cannot_exceed_the_hard_fleet_ceiling(
    tmp_path: Path,
) -> None:
    """A caller cannot authorize unaccounted worker-local disk expansion."""

    checkout = _git_checkout(tmp_path)
    stamp_name = f"{pbrun.STAMP_PREFIX}hard-limit-test.json"
    (checkout / stamp_name).write_text(
        json.dumps({"cwd": ".", **pbrun._git_identity(checkout)})
    )
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")

    with pytest.raises(SystemExit, match="hard fleet ceiling"):
        pbrun.build_git_checkout_snapshot(
            checkout,
            stamp_name=stamp_name,
            cas=cas,
            max_bytes=pbrun.CHECKOUT_SNAPSHOT_MAX_BYTES + 1,
        )


@pytest.mark.parametrize("target", ["../large-data", "/home/rob/model-cache"])
def test_git_snapshot_refuses_a_symlink_that_escapes_the_repository(
    tmp_path: Path, target: str,
) -> None:
    """A worker-local checkout must not reinterpret a source-external link."""

    checkout = _git_checkout(tmp_path)
    (checkout / "data").symlink_to(target, target_is_directory=True)
    stamp_name = f"{pbrun.STAMP_PREFIX}escaping-link-test.json"
    (checkout / stamp_name).write_text(
        json.dumps({"cwd": ".", **pbrun._git_identity(checkout)})
    )
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")

    with pytest.raises(SystemExit, match="symlink.*outside"):
        pbrun.build_git_checkout_snapshot(
            checkout, stamp_name=stamp_name, cas=cas, max_bytes=16 * 1024 * 1024
        )


def test_git_snapshot_allows_an_internal_relative_symlink(tmp_path: Path) -> None:
    checkout = _git_checkout(tmp_path)
    target = checkout / "assets" / "payload.txt"
    target.parent.mkdir()
    target.write_text("sealed bytes\n")
    (checkout / "data").symlink_to("assets")
    stamp_name = f"{pbrun.STAMP_PREFIX}internal-link-test.json"
    (checkout / stamp_name).write_text(
        json.dumps({"cwd": ".", **pbrun._git_identity(checkout)})
    )
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")

    snapshot = pbrun.build_git_checkout_snapshot(
        checkout, stamp_name=stamp_name, cas=cas, max_bytes=16 * 1024 * 1024
    )

    # "Allows" has to mean "seals", not "does not raise": a snapshot that
    # dropped the link, or followed it into a copy, would also not raise.
    materialized = tmp_path / "materialized-internal-link"
    materialized.mkdir()
    assert _git(materialized, "init", "-q").returncode == 0
    assert _git(
        materialized,
        "fetch",
        "-q",
        str(cas.input_path(snapshot["input"])),
        "refs/heads/prismabuild-snapshot",
    ).returncode == 0
    assert _git(
        materialized, "checkout", "-q", "--detach", str(snapshot["commit"])
    ).returncode == 0

    assert (materialized / "data").is_symlink()
    assert os.readlink(materialized / "data") == "assets"
    assert (materialized / "data" / "payload.txt").read_text() == "sealed bytes\n"


def test_git_snapshot_refuses_a_gitlink_whose_working_bytes_are_not_bundled(
    tmp_path: Path,
) -> None:
    """A parent bundle cannot claim the separately owned submodule checkout."""

    dependency = tmp_path / "dependency"
    dependency.mkdir()
    assert _git(dependency, "init", "-q").returncode == 0
    assert _git(dependency, "config", "user.email", "test@example.invalid").returncode == 0
    assert _git(dependency, "config", "user.name", "PrismaBuild test").returncode == 0
    (dependency / "dependency.py").write_text("VALUE = 'external object store'\n")
    assert _git(dependency, "add", "dependency.py").returncode == 0
    assert _git(dependency, "commit", "-qm", "dependency").returncode == 0

    checkout = _git_checkout(tmp_path)
    assert _git(
        checkout,
        "-c",
        "protocol.file.allow=always",
        "submodule",
        "add",
        "-q",
        str(dependency),
        "vendor/dependency",
    ).returncode == 0
    assert _git(checkout, "commit", "-qam", "add submodule").returncode == 0
    stamp_name = f"{pbrun.STAMP_PREFIX}gitlink-test.json"
    (checkout / stamp_name).write_text(
        json.dumps({"cwd": str(checkout), **pbrun._git_identity(checkout)})
    )
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")

    with pytest.raises(SystemExit, match="gitlink"):
        pbrun.build_git_checkout_snapshot(
            checkout, stamp_name=stamp_name, cas=cas, max_bytes=16 * 1024 * 1024
        )


def test_git_snapshot_refuses_an_active_clean_filter(
    tmp_path: Path,
) -> None:
    """Git filters would snapshot canonical blobs, not exact worktree bytes."""

    checkout = _git_checkout(tmp_path)
    (checkout / ".gitattributes").write_text("filtered.txt filter=rewrite\n")
    (checkout / "filtered.txt").write_text("WORKTREE\n")
    assert _git(
        checkout, "config", "filter.rewrite.clean", "sed s/WORKTREE/CANONICAL/"
    ).returncode == 0
    assert _git(checkout, "config", "filter.rewrite.smudge", "cat").returncode == 0
    assert _git(checkout, "config", "filter.rewrite.required", "true").returncode == 0
    stamp_name = f"{pbrun.STAMP_PREFIX}filter-test.json"
    (checkout / stamp_name).write_text(
        json.dumps({"cwd": str(checkout), **pbrun._git_identity(checkout)})
    )
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")

    with pytest.raises(SystemExit, match="content transform"):
        pbrun.build_git_checkout_snapshot(
            checkout, stamp_name=stamp_name, cas=cas, max_bytes=16 * 1024 * 1024
        )


def _stamped(checkout: Path, stamp_name: str) -> None:
    (checkout / stamp_name).write_text(
        json.dumps({"cwd": str(checkout), **pbrun._git_identity(checkout)})
    )


def _materialized(snapshot: dict[str, object], cas_root: Path):
    """Materialize a sealed snapshot exactly the way a worker does."""

    return pool_module._execution_checkout(
        {
            "action_key": "a" * 64,
            "cas_root": str(cas_root),
            "checkout_snapshot": snapshot,
        }
    )


def test_git_snapshot_keeps_the_source_commit_as_its_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A diff-derived gate needs ancestry, and a root commit has none.

    ``tools/impacted_tests.py --ref BASE...HEAD`` is a required pre-merge gate
    in the Tessera checkout.  Under the parentless snapshot every one of its
    revision arguments -- ``HEAD~1``, ``merge-base``, the symmetric difference
    -- was a ``fatal: ambiguous argument``, so the gate could not run under
    portable pbrun execution at all.  The sealed commit therefore keeps the
    source's HEAD as its parent, and the bundle carries the ancestry that
    makes those spellings resolve.
    """

    checkout = _git_checkout(tmp_path)
    source_head = _git(checkout, "rev-parse", "HEAD").stdout.strip()
    (checkout / "task.py").write_text("VALUE = 'edited after the commit'\n")
    stamp_name = f"{pbrun.STAMP_PREFIX}ancestry-test.json"
    _stamped(checkout, stamp_name)
    cas_root = tmp_path / "cas"
    cas = core_module.PrismaBuildCAS(cas_root)

    snapshot = pbrun.build_git_checkout_snapshot(
        checkout, stamp_name=stamp_name, cas=cas, max_bytes=16 * 1024 * 1024
    )
    assert snapshot["parent"] == source_head

    monkeypatch.setattr(
        pool_module, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized",
        raising=False,
    )
    with _materialized(snapshot, cas_root) as root:
        assert _git(root, "rev-parse", "HEAD~1").stdout.strip() == source_head
        diff = _git(root, "diff", "--name-only", f"{source_head}...HEAD")
        assert diff.returncode == 0, diff.stderr
        assert "task.py" in diff.stdout.split()


def test_git_snapshot_advertises_a_requested_branch_by_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``BASE...HEAD`` is usually spelled with a branch name, not a hash."""

    checkout = _git_checkout(tmp_path)
    branch = _git(checkout, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    source_head = _git(checkout, "rev-parse", "HEAD").stdout.strip()
    (checkout / "task.py").write_text("VALUE = 'edited after the commit'\n")
    stamp_name = f"{pbrun.STAMP_PREFIX}named-ref-test.json"
    _stamped(checkout, stamp_name)
    cas_root = tmp_path / "cas"
    cas = core_module.PrismaBuildCAS(cas_root)

    snapshot = pbrun.build_git_checkout_snapshot(
        checkout,
        stamp_name=stamp_name,
        cas=cas,
        max_bytes=16 * 1024 * 1024,
        snapshot_refs=(branch,),
    )
    assert snapshot["refs"] == {branch: source_head}

    monkeypatch.setattr(
        pool_module, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized",
        raising=False,
    )
    with _materialized(snapshot, cas_root) as root:
        assert _git(root, "rev-parse", branch).stdout.strip() == source_head
        diff = _git(root, "diff", "--name-only", f"{branch}...HEAD")
        assert diff.returncode == 0, diff.stderr
        assert "task.py" in diff.stdout.split()


def test_an_unknown_snapshot_ref_refuses_before_anything_is_sealed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A name the source cannot resolve is a typo, not a queue item."""

    checkout = _git_checkout(tmp_path)

    def unreachable(*_args: object, **_kwargs: object) -> None:
        pytest.fail("a refused --snapshot-ref reached the CAS")

    monkeypatch.setattr(pbrun, "build_git_checkout_snapshot", unreachable)
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    monkeypatch.setattr(
        sys, "argv",
        ["pbrun.py", "--cwd", str(checkout), "--snapshot-ref", "no-such-branch",
         "--", "true"],
    )
    with pytest.raises(SystemExit) as raised:
        pbrun.main()

    message = str(raised.value)
    assert message.startswith("pbrun: ")
    assert "no-such-branch" in message
    assert not list(checkout.glob(f"{pbrun.STAMP_PREFIX}*"))
    assert not (tmp_path / "fleet").exists()


def test_a_malformed_snapshot_ref_refuses_by_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The name becomes a refspec on a worker; Git's own check owns it."""

    checkout = _git_checkout(tmp_path)
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    monkeypatch.setattr(
        sys, "argv",
        ["pbrun.py", "--cwd", str(checkout), "--snapshot-ref", "bad name",
         "--", "true"],
    )
    with pytest.raises(SystemExit, match="pbrun: .*bad name"):
        pbrun.main()


def test_git_snapshot_bounds_a_bundle_its_history_made_large(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The compressed-bundle ceiling now measures ancestry, not just the tree.

    Before the snapshot carried a parent, the bundle was roughly the
    compressed working tree, so the two tree-side bounds covered it by
    proxy.  A small tree over a heavy history is a new way to exceed the
    limit -- and the limit, not the design, is what has to keep saying no.
    """

    checkout = _git_checkout(tmp_path)
    heavy = checkout / "deleted-later.bin"
    heavy.write_bytes(os.urandom(512 * 1024))
    assert _git(checkout, "add", "deleted-later.bin").returncode == 0
    assert _git(checkout, "commit", "-qm", "heavy history").returncode == 0
    assert _git(checkout, "rm", "-q", "deleted-later.bin").returncode == 0
    assert _git(checkout, "commit", "-qm", "small tree again").returncode == 0
    stamp_name = f"{pbrun.STAMP_PREFIX}bundle-size-test.json"
    _stamped(checkout, stamp_name)
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")

    # Both tree-side bounds pass on this checkout; the bundle bound is the
    # only thing between a heavy history and CAS ingestion.
    monkeypatch.setattr(pbrun, "require_working_tree_size", lambda *_a, **_k: 0)
    monkeypatch.setattr(
        pbrun, "require_supported_snapshot_tree", lambda *_a, **_k: None
    )
    with pytest.raises(SystemExit, match="above the .* safety limit"):
        pbrun.build_git_checkout_snapshot(
            checkout, stamp_name=stamp_name, cas=cas, max_bytes=64 * 1024
        )


def test_git_snapshot_refuses_a_shallow_source_by_name(tmp_path: Path) -> None:
    """Ancestry a source does not have cannot be sealed into a bundle.

    ``bundle create`` walks parents now, so a shallow clone dies inside
    pack-objects with ``Failed to traverse parents of commit`` -- a message
    about Git's internals, arriving after the tree has been hashed.  Refuse
    it up front, in a sentence that names the fix.
    """

    origin = _git_checkout(tmp_path)
    (origin / "second.txt").write_text("later history\n")
    assert _git(origin, "add", "second.txt").returncode == 0
    assert _git(origin, "commit", "-qm", "second").returncode == 0
    shallow = tmp_path / "shallow"
    assert subprocess.run(
        ["git", "clone", "-q", "--depth", "1", f"file://{origin}", str(shallow)],
        capture_output=True, text=True,
    ).returncode == 0
    assert _git(
        shallow, "config", "user.email", "test@example.invalid"
    ).returncode == 0
    assert _git(
        shallow, "config", "user.name", "PrismaBuild test"
    ).returncode == 0
    stamp_name = f"{pbrun.STAMP_PREFIX}shallow-test.json"
    _stamped(shallow, stamp_name)
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")

    with pytest.raises(SystemExit, match="shallow clone"):
        pbrun.build_git_checkout_snapshot(
            shallow, stamp_name=stamp_name, cas=cas, max_bytes=16 * 1024 * 1024
        )


def test_git_snapshot_refuses_a_repository_with_no_commits(
    tmp_path: Path,
) -> None:
    """An unborn HEAD refuses, exactly as it did before ancestry was sealed.

    ``parent: null`` exists in the v2 contract so the shape is total, but the
    submitter never produces it: identity is taken before the snapshot and
    ``rev-parse HEAD`` fails on a repository with no commits.  Recorded here
    so the refusal stays a decision rather than an accident.
    """

    checkout = tmp_path / "unborn"
    checkout.mkdir()
    assert _git(checkout, "init", "-q").returncode == 0
    assert _git(
        checkout, "config", "user.email", "test@example.invalid"
    ).returncode == 0
    assert _git(
        checkout, "config", "user.name", "PrismaBuild test"
    ).returncode == 0
    stamp_name = f"{pbrun.STAMP_PREFIX}unborn-test.json"
    (checkout / stamp_name).write_text("{}")
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")

    with pytest.raises(SystemExit, match="cannot identify checkout"):
        pbrun.build_git_checkout_snapshot(
            checkout, stamp_name=stamp_name, cas=cas, max_bytes=16 * 1024 * 1024
        )


def test_portable_checkout_refuses_a_submitter_local_path_in_argv(tmp_path) -> None:
    """Relocation must not leave an argv escape back into the live checkout."""

    checkout = _git_checkout(tmp_path)

    with pytest.raises(SystemExit, match="submitter checkout path"):
        pbrun.require_relocatable_checkout(
            ["/bin/bash", "-lc", f"python {checkout}/task.py"],
            {"PYTHONPATH": str(checkout / "src")},
            checkout,
        )


@pytest.mark.parametrize("field", ["argv", "environment"])
@pytest.mark.parametrize("form", [
    "{path}", "--out={path}", "'{path}'", '"{path}"',
    'open("{path}")', r'open(\"{path}\")', "/external:{path}:/other",
])
@pytest.mark.parametrize("suffix", ["-results", "_results", ".results", "2"])
def test_portable_checkout_allows_sibling_path_prefixes(tmp_path, field, form, suffix):
    checkout = tmp_path / "repo"
    value = form.format(path=str(checkout) + suffix + "/result.json")
    pbrun.require_relocatable_checkout(
        ["true", value] if field == "argv" else ["true"],
        {"OUTPUT": value} if field == "environment" else {},
        checkout, repository_root=checkout,
    )


@pytest.mark.parametrize("field", ["argv", "environment"])
@pytest.mark.parametrize("form", [
    "{path}", "--out={path}", "'{path}'", '"{path}"',
    'open("{path}")', r'open(\"{path}\")', "/external:{path}:/other",
])
@pytest.mark.parametrize("suffix", ["", "/", "/src/task.py"])
def test_portable_checkout_refuses_embedded_exact_and_descendant_paths(
    tmp_path, field, form, suffix,
):
    checkout = tmp_path / "repo"
    value = form.format(path=str(checkout) + suffix)
    with pytest.raises(SystemExit, match="submitter repository path"):
        pbrun.require_relocatable_checkout(
            ["true", value] if field == "argv" else ["true"],
            {"OUTPUT": value} if field == "environment" else {},
            checkout, repository_root=checkout,
        )


def test_portable_checkout_checks_every_path_list_component(tmp_path):
    checkout = tmp_path / "repo"
    with pytest.raises(SystemExit, match="environment PYTHONPATH"):
        pbrun.require_relocatable_checkout(
            ["true"], {"PYTHONPATH": f"{checkout}-results:{checkout}/src"},
            checkout, repository_root=checkout,
        )


def test_portable_subdirectory_refuses_an_absolute_repository_sibling(
    tmp_path: Path,
) -> None:
    """Relocation closes over the repository root, not only requested cwd."""

    checkout = _git_checkout(tmp_path)
    requested = checkout / "package"
    requested.mkdir()
    sibling = checkout / "tools" / "helper.py"
    sibling.parent.mkdir()
    sibling.write_text("print('helper')\n")

    with pytest.raises(SystemExit, match="submitter repository path"):
        pbrun.require_relocatable_checkout(
            ["python", str(sibling)], {}, requested, repository_root=checkout
        )


def test_portable_subdirectory_allows_a_relative_repository_sibling_script(
    tmp_path: Path,
) -> None:
    """The repository snapshot, not requested cwd, owns relative helpers."""

    checkout = _git_checkout(tmp_path)
    requested = checkout / "package"
    requested.mkdir()
    sibling = checkout / "tools" / "helper.py"
    sibling.parent.mkdir()
    sibling.write_text("print('helper')\n")

    pbrun.require_checkout_owned_scripts(
        ["python", "../tools/helper.py"],
        requested,
        repository_root=checkout,
    )

    # The control the acceptance needs.  Nothing else refuses a *relative*
    # token, so a gate that resolved only absolute ones read exactly like this
    # acceptance: measured, by skipping relative tokens and running the whole
    # suite, which stayed green.
    outside = tmp_path / "outside" / "helper.py"
    outside.parent.mkdir()
    outside.write_text("print('helper')\n")
    with pytest.raises(SystemExit, match="outside the snapshotted repository"):
        pbrun.require_checkout_owned_scripts(
            ["python", "../../outside/helper.py"],
            requested,
            repository_root=checkout,
        )


def test_a_non_git_checkout_cannot_fall_back_to_mutable_execution(
    tmp_path, monkeypatch,
) -> None:
    """Every new pbrun action must execute bytes carried through the CAS."""

    from unittest import mock

    work = tmp_path / "plain-directory"
    work.mkdir()
    fleet = tmp_path / "fleet"
    with mock.patch.object(pbrun, "SH", fleet), \
         mock.patch.object(sys, "argv", [
             "pbrun.py", "--cwd", str(work), "--wait-s", "0", "--", "true",
        ]):
        with pytest.raises(SystemExit, match="Git checkout"):
            pbrun.main()


def test_an_external_script_argument_is_refused_before_submission(tmp_path) -> None:
    """A path in argv is not part of the checkout closure by magic.

    Tessera action ``6c90ba1b`` invoked a helper beside its checkout.  The
    action bound the checkout commit and the literal helper path, but not the
    helper's bytes, so editing it after submission changed what ran without
    changing the action key.  Refuse that shape while the submitter is still
    present to put the script under the checkout.
    """

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    helper = tmp_path / "run-campaign.sh"
    helper.write_text("#!/bin/sh\nexit 0\n")

    with pytest.raises(SystemExit) as caught:
        pbrun.require_checkout_owned_scripts([str(helper)], checkout)

    message = str(caught.value)
    assert str(helper) in message
    assert "outside the snapshotted repository" in message


def test_a_script_inside_the_checkout_is_bound_by_its_identity(tmp_path) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    helper = checkout / "run-campaign.sh"
    helper.write_text("#!/bin/sh\nexit 0\n")

    # No assertion: acceptance is the absence of the refusal, and the gate is
    # proved to look at this argv shape by the refusal test directly above.
    pbrun.require_checkout_owned_scripts([str(helper)], checkout)


def test_exclusive_demands_what_a_box_actually_offers(tmp_path):
    """``--exclusive`` must not guess the size of a box.

    It used to demand ``--gpu-capacity``'s default of 4 while sparky declares
    2 and sparklina 1, so every exclusive submission asked for twice the slots
    that exist on any box in the fleet.  That does not fail loudly: it
    publishes an action no worker can ever claim, and the caller sees a queued
    item rather than a refusal.
    """
    queue = pool_module.PoolQueue(tmp_path / "q")
    queue.announce(host="sparky", tags=["gb10", "sparky"], has_gpu=True,
                   capacity={"gpu": 2, "mem_gb": 48})
    queue.announce(host="gx10-6b77", tags=["gb10", "sparklina"], has_gpu=True,
                   capacity={"gpu": 1, "mem_gb": 40})
    queue.announce(host="dl380g10", tags=["cpu", "x86"], has_gpu=False,
                   capacity={"gpu": 0, "mem_gb": 60})

    assert pbrun.exclusive_gpu_demand(queue, []) == 2
    assert pbrun.exclusive_gpu_demand(queue, ["sparky"]) == 2
    assert pbrun.exclusive_gpu_demand(queue, ["sparklina"]) == 1


def test_exclusive_refuses_rather_than_guesses_when_nothing_offers(tmp_path):
    """A CPU-only fleet has no answer to "the whole GPU", and says so."""
    queue = pool_module.PoolQueue(tmp_path / "q")
    queue.announce(host="dl380g10", tags=["cpu", "x86"], has_gpu=False,
                   capacity={"gpu": 0, "mem_gb": 60})
    with pytest.raises(SystemExit) as caught:
        pbrun.exclusive_gpu_demand(queue, [])
    assert "--gpu-capacity" in str(caught.value)


def _submitted_environment(root: Path, *extra_argv: str) -> dict[str, str]:
    """The environment a real submission carries, read off the sealed action.

    Driven through ``main()`` against a private pool root, because the
    environment the child gets is the one in the action body -- past the
    ``--env`` loop, the GPU rule and the container variables that all edit the
    dict after the defaults are written.
    """

    import socket
    from unittest import mock

    root.mkdir(parents=True, exist_ok=True)
    work = _git_checkout(root)
    queue = pool_module.PoolQueue(root / "pb-queue")
    queue.announce(host=HOST, tags=["gb10", HOST], has_gpu=True,
                   capacity={"gpu": 2, "mem_gb": 48, "cpu": 10})

    with mock.patch.object(pbrun, "SH", root), \
         mock.patch.object(pbrun, "POLL_S", 0.001), \
         mock.patch.object(socket, "gethostname", return_value=HOST), \
         mock.patch.object(sys, "argv",
                           ["pbrun.py", "--cwd", str(work), "--here",
                            "--wait-s", "0.01", *extra_argv,
                            "--", "echo", "hi"]):
        assert pbrun.main() == 75          # accepted; nothing here claims it

    requests = sorted((root / "cas" / "requests").rglob("*.json"))
    assert len(requests) == 1, requests
    body = json.loads(requests[0].read_text(encoding="utf-8"))
    return body["environment"]["variables"]


def test_the_default_environment_bounds_the_thread_pools(tmp_path):
    """A fleet's parallelism is many actions, not one action per box.

    Torch, numpy and OpenBLAS each size their pool from the machine's core
    count, and the pool admits many actions per box, so the default
    multiplies.  dl380g10 ran a 24-worker pytest under 16 worker loops and
    reached a load average of **927** on 80 cores -- every process fighting
    for a scheduler slot it did not need.

    Asserted on the submitted action rather than on pbrun's source: a default
    that is overwritten further down ``main`` is still spelled in the dict
    literal, and a source grep reads it as present.
    """

    variables = _submitted_environment(tmp_path / "defaults")

    for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS"):
        assert variables[name] == "1", name

    # And it must stay overridable: --env is applied after the defaults.
    overridden = _submitted_environment(
        tmp_path / "override", "--env", "OMP_NUM_THREADS=16")
    assert overridden["OMP_NUM_THREADS"] == "16"
    assert overridden["MKL_NUM_THREADS"] == "1"


def _fleet(tmp_path: Path):
    """The live fleet's shape, from ``tools/fleet/fleet_boxes.json``."""

    queue = pool_module.PoolQueue(tmp_path / "q")
    queue.announce(host="sparky", tags=["gb10", "sparky"], has_gpu=True,
                   capacity={"gpu": 2, "mem_gb": 48, "cpu": 10})
    queue.announce(host="gx10-6b77", tags=["gb10", "gx10-6b77", "sparklina"],
                   has_gpu=True, capacity={"gpu": 1, "mem_gb": 40, "cpu": 10})
    queue.announce(host="dl380g10", tags=["cpu", "dl380g10", "x86"],
                   has_gpu=False, capacity={"gpu": 0, "mem_gb": 60, "cpu": 80})
    return queue


def _notice(queue, *, cwd: str, tags: list[str], demand: dict, here=False,
            needs_gpu=False) -> str:
    intent = {"tags": tags, "needs_gpu": needs_gpu, "resources": demand}
    return pbrun.pin_notice(queue, intent, cwd=Path(cwd), hostname=HOST,
                            here=here)


def test_a_box_local_checkout_says_it_pinned_the_action(tmp_path) -> None:
    """The pin was a silent consequence of a path.

    ``pbrun`` printed ``tags=['sparky']`` and stopped there, so an agent that
    had just made itself a worktree under ``/home/rob/tmp`` had no way to know
    it had narrowed the fleet to one box.  131 of 391 items in the live queue
    on 2026-09-04 carried a hostname tag, 129 of them as a consequence of a
    path -- 114 pinned to sparky by a ``/home/rob/tmp/ts*`` worktree -- while
    the other two boxes idled.
    """

    notice = _notice(_fleet(tmp_path), cwd="/home/rob/tmp/ts101",
                     tags=[HOST], demand={"cpu": 1, "mem_gb": 4})

    assert "PINNED to sparky" in notice
    assert "/home/rob/tmp/ts101 is box-local" in notice
    assert "2 other live boxes fit this demand: dl380g10, gx10-6b77" in notice
    assert "/mnt/shared" in notice                     # and what to do about it


def test_the_width_quoted_is_the_width_the_pin_cost(tmp_path) -> None:
    """Not "how many boxes match the pinned tags" -- that is always one.

    The question worth answering is how many boxes would have been eligible
    without it, so the host tag comes off before the fleet is asked.  A demand
    only this box can meet costs nothing to pin, and saying so keeps the
    warning from crying wolf on every GPU-heavy submission.
    """

    queue = _fleet(tmp_path)

    one_slot = _notice(queue, cwd="/home/rob/tmp/ts101", tags=[HOST],
                       needs_gpu=True, demand={"gpu": 1, "mem_gb": 16})
    both_slots = _notice(queue, cwd="/home/rob/tmp/ts101", tags=[HOST],
                         needs_gpu=True, demand={"gpu": 2, "mem_gb": 16})

    assert "1 other live box fits this demand: gx10-6b77" in one_slot
    assert "No other live box fits this demand" in both_slots


def test_a_shared_checkout_has_nothing_to_report(tmp_path) -> None:
    """Silence is the correct output for an action that is already free."""

    assert _notice(_fleet(tmp_path), cwd="/mnt/shared/tessera-x86", tags=[],
                   demand={"cpu": 1}) == ""


def test_here_is_still_reported_as_the_pin_it_is(tmp_path) -> None:
    """Asked for on purpose, and still worth pricing."""

    notice = _notice(_fleet(tmp_path), cwd="/mnt/shared/tessera-x86",
                     tags=[HOST], demand={"cpu": 1}, here=True)

    assert notice.startswith("pbrun: PINNED to sparky by --here")
    assert "2 other live boxes fit this demand" in notice


def test_an_explicit_tag_over_a_box_local_checkout_is_a_warning(tmp_path) -> None:
    """``--tag`` REPLACES the pin, so the tree can be invisible where it lands.

    That failure is loud rather than silent -- the worker refuses on an
    unavailable checkout root, or on ``core.verify_code_closure`` when a
    same-named tree exists there with other bytes -- but it is loud after a
    claim and two retries, on another box, in a log nobody is watching.  The
    submitter is here now.
    """

    notice = _notice(_fleet(tmp_path), cwd="/home/rob/tmp/ts101", tags=["x86"],
                     demand={"cpu": 1})

    assert "WARNING" in notice
    assert "exists only on sparky" in notice
    assert "--tag sparky" in notice


def test_naming_this_box_is_the_correct_submission_not_a_warning(tmp_path) -> None:
    """The issue's own remedy must not be scolded for being applied.

    ``--tag sparky`` from a sparky worktree is exactly what the issue says
    submitters do, and it is right: the action cannot land where its tree is
    absent.  A first draft warned on the presence of any ``--tag`` and so told
    this submitter to add the tag they had just passed.

    A one-box alias (``--tag sparklina``) is right too, and is a weaker
    statement: it is exclusive because of who is announcing, not because of
    what the tag means.  So it is reported without a WARNING and without the
    word PINNED -- naming the contingency instead, which is the difference
    ``test_a_tag_no_other_box_offers_today_is_not_called_exclusive`` exists
    to hold.
    """

    queue = _fleet(tmp_path)

    own = _notice(queue, cwd="/home/rob/tmp/ts101", tags=[HOST],
                  demand={"cpu": 1})
    alias = pbrun.pin_notice(
        queue, {"tags": ["sparklina"], "needs_gpu": False, "resources": {"cpu": 1}},
        cwd=Path("/home/rob/tmp/ts91"), hostname="gx10-6b77", here=False)

    assert "WARNING" not in own and "WARNING" not in alias
    assert notice_host(own) == "sparky"
    assert "PINNED" not in alias
    assert "exists only on gx10-6b77" in alias
    assert "no other live box offers tags ['sparklina']" in alias
    assert "--tag gx10-6b77" in alias


def test_a_host_tag_another_box_also_offers_is_not_exclusive(tmp_path) -> None:
    """The one thing a host tag is trusted for, checked rather than assumed.

    ``sparky`` is this box's alone by construction of ``worker_loop``'s
    offered tags -- until a loop is started elsewhere with ``--tag sparky``,
    which is a thing a person can do.  The notice asks the placer instead of
    reasoning from the construction, so the day that happens it says so.
    """

    queue = _fleet(tmp_path)
    queue.announce(host="dl380g10", tags=["cpu", "dl380g10", "x86", HOST],
                   has_gpu=False, capacity={"gpu": 0, "mem_gb": 60, "cpu": 80})

    notice = _notice(queue, cwd="/home/rob/tmp/ts101", tags=[HOST],
                     demand={"cpu": 1})

    assert "WARNING" in notice and "dl380g10" in notice
    assert "not exclusive to this box" in notice


def notice_host(notice: str) -> str:
    return notice.split("PINNED to ", 1)[1].split(" ", 1)[0]


def test_with_nobody_announced_only_this_boxs_own_name_is_trusted(tmp_path) -> None:
    """The placer cannot answer, so fall back to the one tag that is provable.

    A tag naming this host cannot be claimed elsewhere whatever the fleet turns
    out to be; any other tag might be, and an unanswerable question is not a
    reason to go quiet about a tree that exists on one box.
    """

    empty = pool_module.PoolQueue(tmp_path / "q")

    own = _notice(empty, cwd="/home/rob/tmp/ts101", tags=[HOST],
                  demand={"cpu": 1})
    other = _notice(empty, cwd="/home/rob/tmp/ts101", tags=["x86"],
                    demand={"cpu": 1})

    assert "WARNING" not in own
    assert "WARNING" in other and "let another box claim" in other


def test_an_unannounced_fleet_reports_unknown_rather_than_zero(tmp_path) -> None:
    """A missing diagnostic must not be printed as a measurement."""

    empty = pool_module.PoolQueue(tmp_path / "q")

    notice = _notice(empty, cwd="/home/rob/tmp/ts101", tags=[HOST],
                     demand={"cpu": 1})

    assert "PINNED to sparky" in notice
    assert "Fleet width unknown" in notice


def test_a_real_submission_says_it_before_it_says_queued(tmp_path, capsys) -> None:
    """An intentional pin learned after the fact is a receipt, not a warning.

    Driven through ``main()`` against a private pool root rather than asserted
    on the source, because what matters is that the sentence reaches the
    person's terminal on a real submit -- past the placement, the demand
    defaults and the CAS publication that come between.
    """

    import socket
    from unittest import mock

    work = _git_checkout(tmp_path)
    queue = pool_module.PoolQueue(tmp_path / "pb-queue")
    queue.announce(host=HOST, tags=["gb10", HOST], has_gpu=True,
                   capacity={"gpu": 2, "mem_gb": 48, "cpu": 10})
    queue.announce(host="dl380g10", tags=["cpu", "x86"], has_gpu=False,
                   capacity={"gpu": 0, "mem_gb": 60, "cpu": 80})

    with mock.patch.object(pbrun, "SH", tmp_path), \
         mock.patch.object(pbrun, "RUNTIME_ROOT", PUBLISHED_RUNTIME), \
         mock.patch.object(pbrun, "POLL_S", 0.001), \
             mock.patch.object(socket, "gethostname", return_value=HOST), \
             mock.patch.object(sys, "argv",
                               ["pbrun.py", "--cwd", str(work), "--here",
                                "--wait-s", "0.01",
                                "--", "echo", "hi"]):
        assert pbrun.main() == 75          # nothing is running to claim it

    err = capsys.readouterr().err
    assert err.index("PINNED to sparky") < err.index("pbrun: queued")
    assert "1 other live box fits this demand: dl380g10" in err


@pytest.mark.parametrize("offset_s", [-121.0, 7.0], ids=["old-offer", "clock-skew"])
def test_a_matching_recorded_offer_outvotes_a_fresh_nonmatch_at_submit(
    tmp_path, capsys, monkeypatch, offset_s,
) -> None:
    """Capability is not whichever boxes happened to announce this instant.

    dl380g10 is the fleet's only ``x86`` box.  Its real worker can spend longer
    than the offer TTL inside an action, while another box keeps announcing;
    that made the old precheck answer ``False`` and refuse a two-hour wait even
    though dl380g10 had explicitly advertised enough capacity.  The latest
    record per host is the fleet's capability evidence.  Freshness decides who
    may claim now, not whether the submission may wait.
    """

    import socket
    from unittest import mock

    work = _git_checkout(tmp_path)
    now = [1_000.0 + offset_s]
    monkeypatch.setattr(pool_module, "_now", lambda: now[0])
    queue = pool_module.PoolQueue(tmp_path / "pb-queue")
    queue.announce(host="dl380g10", tags=["cpu", "x86"], has_gpu=False,
                   capacity={"gpu": 0, "mem_gb": 60, "cpu": 80})
    now[0] = 1_000.0
    queue.announce(host=HOST, tags=["gb10", HOST], has_gpu=True,
                   capacity={"gpu": 2, "mem_gb": 48, "cpu": 10})

    with mock.patch.object(pbrun, "SH", tmp_path), \
         mock.patch.object(pbrun, "RUNTIME_ROOT", PUBLISHED_RUNTIME), \
         mock.patch.object(pbrun, "POLL_S", 0.001), \
         mock.patch.object(socket, "gethostname", return_value=HOST), \
         mock.patch.object(sys, "argv",
                           ["pbrun.py", "--cwd", str(work), "--tag", "x86",
                            "--wait-s", "0.01", "--", "echo", "hi"]):
        assert pbrun.main() == 75          # accepted; no worker is polling here

    err = capsys.readouterr().err
    if offset_s < 0:
        assert "recorded capable worker is between announcements" in err
    else:
        assert "clock skew" in err and "dl380g10" in err
    assert "pbrun: queued" in err


def test_a_tag_and_here_are_both_constraints_the_submitter_asked_for(
    tmp_path,
) -> None:
    """``--here --tag x86`` asks for this box AND for an x86 box.

    ``placement_tags`` returned ``list(explicit)`` the moment any ``--tag``
    was given, so the host pin was discarded without a word.  From a shared
    checkout on sparky, ``pbrun --here --tag x86`` then printed: "pbrun:
    PINNED to sparky by --here, so no other box can claim this action.  1
    other live box fits this demand: dl380g10." -- because the notice read
    the flag, while the tags that landed were ``['x86']`` alone.  Both halves
    are fixed here: the pin lands, and the notice reads the tags.
    """

    tags = pbrun.placement_tags(Path("/mnt/shared/tessera-x86"),
                                explicit=["x86"], here=True, hostname=HOST)
    assert tags == ["x86", HOST]               # both, hostname last

    notice = _notice(_fleet(tmp_path), cwd="/mnt/shared/tessera-x86", tags=tags,
                     demand={"cpu": 1}, here=True)

    assert "PINNED to sparky by --here" in notice
    # And what the pin costs, measured against the tags without the hostname:
    # dl380g10 offers x86 and would have been eligible without --here.
    assert "1 other live box fits this demand: dl380g10" in notice


def test_here_beside_the_host_tag_does_not_repeat_the_hostname(tmp_path) -> None:
    """The two spellings of one pin are one tag, and the tag matcher is exact."""

    assert pbrun.placement_tags(
        Path("/mnt/shared/tessera-x86"),
        explicit=[HOST, "x86"], here=True, hostname=HOST,
    ) == ["x86", HOST]


def test_a_pinning_tag_over_a_box_local_tree_says_both_things(tmp_path) -> None:
    """A class tag that is not this box's leaves the local tree unreachable."""

    notice = _notice(_fleet(tmp_path), cwd="/home/rob/tmp/ts101", tags=["x86"],
                     demand={"cpu": 1}, here=False)

    assert "PINNED" not in notice
    assert "WARNING" in notice and "exists only on sparky" in notice


def test_anywhere_and_a_tag_are_refused_together(tmp_path, monkeypatch) -> None:
    """Portable, but only on x86, is the submitter contradicting itself.

    ``--anywhere`` reached the SLURM lane's ``partition_for`` as well as the
    tags, so the pairing did not merely pick one: the action carried an
    ``x86`` constraint into the partition chosen for portable work.
    """

    import socket
    from unittest import mock

    work = _git_checkout(tmp_path)
    with mock.patch.object(pbrun, "SH", tmp_path), \
         mock.patch.object(pbrun, "RUNTIME_ROOT", PUBLISHED_RUNTIME), \
         mock.patch.object(socket, "gethostname", return_value=HOST), \
         mock.patch.object(pbrun, "POLL_S", 0.001), \
         mock.patch.object(sys, "argv",
                           ["pbrun.py", "--cwd", str(work), "--anywhere",
                            "--tag", "x86", "--wait-s", "0.01",
                            "--", "echo", "hi"]):
        with pytest.raises(SystemExit) as raised:
            pbrun.main()

    assert "--anywhere and --tag contradict each other" in str(raised.value)


def test_a_tag_no_other_box_offers_today_is_not_called_exclusive(tmp_path) -> None:
    """"match only this box" was true of the fleet as ANNOUNCED, not of the fleet.

    With only sparky's offer live, a box-local checkout submitted ``--tag
    gb10`` printed "PINNED to sparky -- the checkout /home/rob/tmp/ts101 is
    box-local and tags ['gb10'] match only this box."  gx10-6b77 offers
    ``gb10`` too; the moment its offer refreshes it can claim an action whose
    tree it does not have, which is exactly the case the WARNING branch
    exists to catch.  Only a tag naming this host is provably exclusive.
    """

    lonely = pool_module.PoolQueue(tmp_path / "q")
    lonely.announce(host=HOST, tags=["gb10", HOST], has_gpu=True,
                    capacity={"gpu": 2, "mem_gb": 48, "cpu": 10})

    notice = _notice(lonely, cwd="/home/rob/tmp/ts101", tags=["gb10"],
                     demand={"cpu": 1})

    assert "match only this box" not in notice
    assert "gb10" in notice
    assert f"--tag {HOST}" in notice           # the submission that IS exclusive

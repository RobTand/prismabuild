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

# Imported BEFORE ``pbrun`` is exec'd, on purpose.  ``pbrun`` puts the source
# beside its own entry point at the front of ``sys.path`` so a live submitter
# binds one published generation.  Binding the checkout's package first keeps
# this direct module-load test self-contained too.
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
    assert "outside the stamped checkout" in message


def test_a_script_inside_the_checkout_is_bound_by_its_identity(tmp_path) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    helper = checkout / "run-campaign.sh"
    helper.write_text("#!/bin/sh\nexit 0\n")

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


def test_the_default_environment_bounds_the_thread_pools():
    """A fleet's parallelism is many actions, not one action per box.

    Torch, numpy and OpenBLAS each size their pool from the machine's core
    count, and the pool admits many actions per box, so the default
    multiplies.  dl380g10 ran a 24-worker pytest under 16 worker loops and
    reached a load average of **927** on 80 cores -- every process fighting
    for a scheduler slot it did not need.
    """
    import subprocess
    import sys

    out = subprocess.run(
        [sys.executable, str(pbrun.__file__), "--help"],
        capture_output=True, text=True, check=False)
    assert out.returncode == 0
    source = Path(pbrun.__file__).read_text()
    for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS"):
        assert f'"{name}": "4"' in source, name
    # And it must stay overridable: --env is applied after the defaults.
    assert source.index('"OMP_NUM_THREADS"') < source.index("for entry in args.env")


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
    """A pin the submitter learns about after the fact is a receipt, not a warning.

    Driven through ``main()`` against a private pool root rather than asserted
    on the source, because what matters is that the sentence reaches the
    person's terminal on a real submit -- past the placement, the demand
    defaults and the CAS publication that come between.
    """

    import socket
    from unittest import mock

    work = tmp_path / "tree"
    work.mkdir()
    (work / "hello.txt").write_text("hi\n")
    queue = pool_module.PoolQueue(tmp_path / "pb-queue")
    queue.announce(host=HOST, tags=["gb10", HOST], has_gpu=True,
                   capacity={"gpu": 2, "mem_gb": 48, "cpu": 10})
    queue.announce(host="dl380g10", tags=["cpu", "x86"], has_gpu=False,
                   capacity={"gpu": 0, "mem_gb": 60, "cpu": 80})

    with mock.patch.object(pbrun, "SH", tmp_path), \
         mock.patch.object(pbrun, "POLL_S", 0.001), \
         mock.patch.object(socket, "gethostname", return_value=HOST), \
         mock.patch.object(sys, "argv",
                           ["pbrun.py", "--cwd", str(work), "--wait-s", "0.01",
                            "--", "echo", "hi"]):
        assert pbrun.main() == 75          # nothing is running to claim it

    err = capsys.readouterr().err
    assert err.index("PINNED to sparky") < err.index("pbrun: queued")
    assert "1 other live box fits this demand: dl380g10" in err


def test_a_matching_stale_offer_outvotes_a_fresh_nonmatch_at_submit(
    tmp_path, capsys, monkeypatch,
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

    work = tmp_path / "tree"
    work.mkdir()
    (work / "hello.txt").write_text("hi\n")
    now = [1_000.0]
    monkeypatch.setattr(pool_module, "_now", lambda: now[0])
    queue = pool_module.PoolQueue(tmp_path / "pb-queue")
    queue.announce(host="dl380g10", tags=["cpu", "x86"], has_gpu=False,
                   capacity={"gpu": 0, "mem_gb": 60, "cpu": 80})
    now[0] += pool_module.OFFER_TIMEOUT_S + 1
    queue.announce(host=HOST, tags=["gb10", HOST], has_gpu=True,
                   capacity={"gpu": 2, "mem_gb": 48, "cpu": 10})

    with mock.patch.object(pbrun, "SH", tmp_path), \
         mock.patch.object(pbrun, "POLL_S", 0.001), \
         mock.patch.object(socket, "gethostname", return_value=HOST), \
         mock.patch.object(sys, "argv",
                           ["pbrun.py", "--cwd", str(work), "--tag", "x86",
                            "--wait-s", "0.01", "--", "echo", "hi"]):
        assert pbrun.main() == 75          # accepted; no worker is polling here

    err = capsys.readouterr().err
    assert "recorded capable worker is between announcements" in err
    assert "pbrun: queued" in err


def test_here_overridden_by_a_tag_does_not_announce_a_pin_that_never_happened(
    tmp_path,
) -> None:
    """``placement_tags`` returns ``list(explicit)``, so ``--tag`` REPLACES ``--here``.

    The notice read the ``here`` FLAG rather than the tags that actually
    landed, so from a shared checkout on sparky ``pbrun --here --tag x86``
    printed, verbatim: "pbrun: PINNED to sparky by --here, so no other box can
    claim this action.  1 other live box fits this demand: dl380g10." --
    asserting an exclusivity that does not exist and then naming, as the
    "other" box, the only box that can actually run the action.
    """

    tags = pbrun.placement_tags(Path("/mnt/shared/tessera-x86"),
                                explicit=["x86"], here=True, hostname=HOST)
    assert tags == ["x86"]                     # the host tag never landed

    notice = _notice(_fleet(tmp_path), cwd="/mnt/shared/tessera-x86", tags=tags,
                     demand={"cpu": 1}, here=True)

    assert "PINNED" not in notice
    assert "--here" in notice                  # and that the flag did nothing
    assert "dl380g10" in notice                # the box that will really run it


def test_here_overridden_over_a_box_local_tree_says_both_things(tmp_path) -> None:
    """The override and the tree that cannot travel are two separate facts."""

    notice = _notice(_fleet(tmp_path), cwd="/home/rob/tmp/ts101", tags=["x86"],
                     demand={"cpu": 1}, here=True)

    assert "PINNED" not in notice
    assert "--here" in notice
    assert "WARNING" in notice and "exists only on sparky" in notice


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

"""An action must land on a box that can open the runtime ``pbrun`` sealed.

``pbrun`` transports the *checkout* through the CAS, so a box-local checkout
runs anywhere -- measured, not assumed: a worktree under ``/home/rob/tmp``
submitted through the published runtime executed on dl380g10 clean.  What
``pbrun`` does not transport is itself.  The worker launcher and ``core.py``
are sealed as absolute paths into the *submitting* runtime's tree, so a
``pbrun`` invoked out of a developer worktree can be executed only by the box
that worktree is on.

``placement_tags`` cannot see that and should not: it screens argv and the
caller's environment, which are the submitter's inputs, not ``pbrun``'s own
installation.  It does derive the right pin anyway, because the payload's
``env`` resolves outside the repository -- but an explicit ``--tag`` outranks
every derived pin by design, and ``pbtest`` supplied one unconditionally.

So ``pbtest`` run out of a worktree sent four suite shards to dl380g10, each
dying on ``can't open file '<worktree>/tools/prismabuild_worker.py'`` after a
claim, a checkout materialization and a wasted slot.  Two halves, because
either alone leaves the other trap set:

*   ``pbrun`` refuses at submit when the placement admits a box that cannot
    open this runtime.  A remote ``No such file or directory`` naming one of
    our own paths is not a diagnosis; the submitter knows.
*   ``pbtest`` stops fabricating the ``x86`` tag when its own runtime is
    box-local, so ``pbrun``'s host pin stands rather than being overridden.

Issue #292.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

import pbrun  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "pbtest_placement", REPOSITORY / "tools" / "fleet" / "pbtest.py"
)
pbtest = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbtest)                      # type: ignore[union-attr]

#: A runtime every box can open, and one only this box can.  Both are lexical:
#: ``pool.is_box_local_path`` is a string test on purpose, so neither has to
#: exist for the rule to answer.
PUBLISHED = Path("/mnt/shared/prismabuild-fleet/runtime-generations/g-1")
WORKTREE = Path("/home/rob/tmp/some-worktree")


# --------------------------------------------------------------------------
# pbrun: the refusal
# --------------------------------------------------------------------------

def test_a_worktree_runtime_refuses_a_placement_that_does_not_name_this_box():
    with pytest.raises(SystemExit) as refusal:
        pbrun.require_reachable_runtime(
            ["x86"], hostname="sparky", runtime_root=WORKTREE)
    said = str(refusal.value)
    # The reader has to be able to act on it without reading pbrun, so the
    # message carries all three facts: which runtime, which box has it, and
    # which placement was asked for.
    assert str(WORKTREE) in said
    assert "sparky" in said
    assert "x86" in said


def test_a_worktree_runtime_refuses_an_unpinned_placement():
    # ``--anywhere`` normalizes to no tags at all, which is the widest
    # placement there is and therefore the most certainly wrong one here.
    with pytest.raises(SystemExit) as refusal:
        pbrun.require_reachable_runtime(
            [], hostname="sparky", runtime_root=WORKTREE)
    assert "any eligible worker" in str(refusal.value)


def test_a_worktree_runtime_admits_a_placement_that_names_this_box():
    # ``--tag x86 --here`` on an x86 box is a real request -- a class AND this
    # machine -- and the refusal must not be what answers it.  Whether any box
    # satisfies the conjunction is the queue's question, asked later.
    assert pbrun.require_reachable_runtime(
        ["x86", "sparky"], hostname="sparky", runtime_root=WORKTREE) is None
    assert pbrun.require_reachable_runtime(
        ["sparky"], hostname="sparky", runtime_root=WORKTREE) is None


@pytest.mark.parametrize("tags", [[], ["x86"], ["gb10"], ["sparky"]])
def test_a_published_runtime_admits_every_placement(tags):
    # Nothing here narrows what the queue may choose.  The check states a fact
    # about this runtime, and a published runtime constrains nothing.
    assert pbrun.require_reachable_runtime(
        tags, hostname="sparky", runtime_root=PUBLISHED) is None


def test_the_rule_is_pools_and_not_a_second_copy_of_it():
    # ``pool.is_box_local_path`` documents itself as the one place the rule
    # lives, because the submitter's pin and the queue's census of that pin
    # must not describe different fleets.  A second spelling here is how they
    # would drift, so this asserts the delegation rather than the answer.
    seen: list[Path] = []
    real = pbrun.pool.is_box_local_path

    def _spy(path):
        seen.append(path)
        return real(path)

    pbrun.pool.is_box_local_path = _spy
    try:
        pbrun.require_reachable_runtime(
            ["sparky"], hostname="sparky", runtime_root=WORKTREE)
    finally:
        pbrun.pool.is_box_local_path = real
    assert seen == [WORKTREE]


# --------------------------------------------------------------------------
# pbtest: the tag it no longer fabricates
# --------------------------------------------------------------------------

class _FinishedProcess:
    returncode = 0

    def communicate(self):
        return "1 passed in 0.01s\n", None


def _shard_argv(tmp_path, monkeypatch, *, runtime_root, extra=()):
    """The pbrun argv one pbtest shard builds under ``runtime_root``."""

    checkout = tmp_path / "checkout"
    test_file = checkout / "tests" / "test_one.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("def test_one():\n    assert True\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    calls: list[list[str]] = []
    monkeypatch.setattr(
        pbtest.subprocess, "Popen",
        lambda command, **_kw: (calls.append(list(command)), _FinishedProcess())[1])
    monkeypatch.setattr(pbtest, "RUNTIME_ROOT", runtime_root)
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
         "--shards", "1", *extra, "tests"],
    )
    pbtest.main()
    assert len(calls) == 1
    return calls[0]


def _tags(command) -> list[str]:
    flags = command[:command.index("--")]
    return [flags[i + 1] for i, flag in enumerate(flags) if flag == "--tag"]


def test_a_worktree_pbtest_names_no_tag_and_lets_pbrun_pin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # No tag is not "run anywhere": pbrun derives the host pin from the
    # payload's own executable, and only an explicit tag could overrule it.
    argv = _shard_argv(tmp_path, monkeypatch, runtime_root=WORKTREE)
    assert _tags(argv) == []


def test_a_published_pbtest_still_defaults_to_x86(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # The default exists to say at the call site that a pass here is not a
    # measurement, and to keep the sparks' cores for GPU work.  Where the
    # runtime is reachable from every box, it is still both of those things.
    argv = _shard_argv(tmp_path, monkeypatch, runtime_root=PUBLISHED)
    assert _tags(argv) == ["x86"]


@pytest.mark.parametrize("runtime_root", [WORKTREE, PUBLISHED])
def test_an_explicit_tag_is_forwarded_from_either_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runtime_root
):
    # The operator's own tag is the one thing this must never rewrite; the
    # default is a default, and the conditional applies only to the default.
    argv = _shard_argv(tmp_path, monkeypatch, runtime_root=runtime_root,
                       extra=("--tag", "sparky"))
    assert _tags(argv) == ["sparky"]


# --------------------------------------------------------------------------
# The call site, not just the rule
# --------------------------------------------------------------------------

def test_pbrun_refuses_the_submission_and_not_only_the_predicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The check has to be reached, and reached before anything is queued.

    A rule nothing calls is the shape #292 already had once: ``pbrun`` knew
    the payload's executable was box-local and derived the right pin, and the
    explicit tag walked past it.  So this drives ``main`` rather than the
    predicate, and asserts on the refusal the operator would actually see.
    """

    from unittest import mock

    work = tmp_path / "work"
    (work / "tests").mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(work)], check=True)

    with mock.patch.object(pbrun, "SH", tmp_path / "fleet"), \
         mock.patch.object(pbrun, "RUNTIME_ROOT", WORKTREE), \
         mock.patch.object(pbrun.socket, "gethostname", return_value="sparky"), \
         mock.patch.object(
             sys, "argv",
             ["pbrun.py", "--cwd", str(work), "--tag", "x86",
              "--wait-s", "0.01", "--", "echo", "hi"]):
        with pytest.raises(SystemExit) as refusal:
            pbrun.main()

    said = str(refusal.value)
    assert "box-local" in said and str(WORKTREE) in said
    # Nothing may have been queued: the point of refusing at submit is that no
    # box claims an action it cannot run, so the queue must still be empty.
    queued = list((tmp_path / "fleet" / "pb-queue" / "ready").glob("*")) \
        if (tmp_path / "fleet" / "pb-queue" / "ready").exists() else []
    assert queued == []

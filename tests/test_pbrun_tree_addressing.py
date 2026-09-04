"""When ``pbrun`` names a tree instead of a path, and when it refuses to.

The decision is not a flag the submitter sets.  It is derived from three
things -- whether the checkout is a git repository, whether every live box has
ANNOUNCED it can build a tree, and whether the submission itself would survive
being moved -- and every path through it either widens the action or leaves it
exactly as pinned as it is today and says which step declined.  A widening that
can fail a submission would be worse than the pin it removes.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import socket
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import checkout as ck_module, pool as pool_module  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "pbrun_tree", Path(__file__).resolve().parents[1] / "tools" / "fleet" / "pbrun.py")
pbrun = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbrun)                       # type: ignore[union-attr]

HOST = socket.gethostname()


def _queue(tmp_path: Path, *, hosts: dict[str, bool]) -> pool_module.PoolQueue:
    """A queue where each named host does or does not announce the capability."""

    queue = pool_module.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    for host, capable in hosts.items():
        queue.announce(host=host, tags=[host, "x86"], has_gpu=False,
                       capacity={"cpu": 4, "mem_gb": 16},
                       capabilities=pool_module.WORKER_CAPABILITIES if capable
                       else ())
    return queue


@pytest.fixture()
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "home" / "rob" / "tmp" / "ts101"
    root.mkdir(parents=True)
    for args in (("init", "-q", "-b", "main"), ("config", "user.email", "t@e"),
                 ("config", "user.name", "t")):
        subprocess.run(["git", "-C", str(root), *args], check=True,
                       capture_output=True)
    (root / "run.sh").write_text("echo hi\n")
    subprocess.run(["git", "-C", str(root), "add", "-A"], check=True,
                   capture_output=True)
    subprocess.run(["git", "-C", str(root), "commit", "-qm", "first"], check=True,
                   capture_output=True)
    return root


def _plan(repo: Path, tmp_path: Path, monkeypatch, *, mode="auto",
          hosts=None, command=("bash", "run.sh"), variables=None,
          max_add_bytes=ck_module.DEFAULT_MAX_ADD_BYTES):
    monkeypatch.setattr(pbrun.ck, "SHARED_GIT_ROOT", tmp_path / "shared-git")
    queue = _queue(tmp_path, hosts=hosts if hosts is not None else {HOST: True})
    return pbrun.plan_tree_addressing(
        repo, command=list(command), variables=dict(variables or {}),
        queue=queue, mode=mode, max_add_bytes=max_add_bytes,
        scratch=tmp_path / "scratch")


# -- the gate ------------------------------------------------------------


def test_every_live_box_must_announce_it_before_an_action_is_widened(
    repo, tmp_path, monkeypatch
) -> None:
    """Attested, not asserted (principle 14).

    The decision reads a capability each box states about itself in its own
    offer.  Nothing here maps a runtime commit onto a feature, which is what
    makes the migration sequence itself: a box on older bytes announces
    nothing and that silence is a "no" that cannot be got wrong.
    """

    plan, why = _plan(repo, tmp_path, monkeypatch,
                      hosts={HOST: True, "sparklina": False})
    assert plan is None
    assert "sparklina announces no checkout_commit capability" == why


def test_a_fleet_with_every_box_announcing_it_is_widened(
    repo, tmp_path, monkeypatch
) -> None:
    plan, why = _plan(repo, tmp_path, monkeypatch,
                      hosts={HOST: True, "sparklina": True})
    assert why == "" and plan is not None
    assert len(plan["tree"]) == 40 and len(plan["commit"]) == 40


def test_no_worker_at_all_stays_unknown_and_therefore_pinned(
    repo, tmp_path, monkeypatch
) -> None:
    """Unknown is a "no" here, and the asymmetry is on purpose.

    ``placeable`` keeps unknown as unknown because a fleet whose loops predate
    the offer registry must still be able to submit.  This decision cannot
    afford the same generosity: addressing an action by a tree nothing can
    materialise would make it unrunnable everywhere, instead of runnable in
    one place.
    """

    assert pbrun.fleet_materialises(_queue(tmp_path, hosts={})) == \
        (None, "no worker has announced")
    plan, why = _plan(repo, tmp_path, monkeypatch, hosts={})
    assert plan is None and why == "no worker has announced"


def test_path_addressed_is_the_way_back(repo, tmp_path, monkeypatch) -> None:
    plan, why = _plan(repo, tmp_path, monkeypatch, mode="path")
    assert plan is None and why == "asked for with --path-addressed"


def test_commit_addressed_overrides_the_gate_but_not_reality(
    repo, tmp_path, monkeypatch
) -> None:
    """The flag can outvote a cautious fleet; it cannot invent a repository."""

    plan, _ = _plan(repo, tmp_path, monkeypatch, mode="commit",
                    hosts={HOST: False})
    assert plan is not None

    plain = tmp_path / "not-a-repo"
    plain.mkdir()
    plan, why = _plan(plain, tmp_path, monkeypatch, mode="commit")
    assert plan is None and "not a git checkout" in why


def test_a_publish_that_fails_falls_back_to_the_pin_rather_than_refusing(
    repo, tmp_path, monkeypatch
) -> None:
    """Publishing objects is the step that touches NFS, so it is the step that
    fails for reasons that have nothing to do with this submission.  Falling
    back keeps the work moving and says what it cost; refusing would turn a
    widening into an outage."""

    def boom(*a, **kw):
        raise ck_module.CheckoutError("mount is gone")

    monkeypatch.setattr(pbrun.ck, "publish_tree_commit", boom)
    plan, why = _plan(repo, tmp_path, monkeypatch)
    assert plan is None
    assert "could not publish the tree" in why and "mount is gone" in why


def test_the_same_failure_under_an_explicit_flag_is_refused(
    repo, tmp_path, monkeypatch
) -> None:
    """A flag that silently does nothing is worse than one that says it cannot."""

    def boom(*a, **kw):
        raise ck_module.CheckoutError("mount is gone")

    monkeypatch.setattr(pbrun.ck, "publish_tree_commit", boom)
    with pytest.raises(SystemExit, match="--commit-addressed asked for"):
        _plan(repo, tmp_path, monkeypatch, mode="commit")


# -- the refusals --------------------------------------------------------


def test_an_argv_naming_the_submitters_own_checkout_is_refused(repo) -> None:
    """The one failure mode of this addressing that is not loud.

    A relocated tree breaks an absolute path in silence: the command runs,
    against the wrong file or none, and reports whatever that produced.  So it
    is refused at the one moment the caller is watching -- the same place an
    unplaceable tag already is.
    """

    with pytest.raises(SystemExit, match="names the submitter's own checkout"):
        pbrun.refuse_paths_into_the_checkout(
            ["pytest", f"{repo}/tests"], {}, str(repo))


def test_the_environment_is_scanned_too(repo) -> None:
    """``--env PYTHONPATH=<checkout>/src`` is the same hole wearing a hat."""

    with pytest.raises(SystemExit, match="PYTHONPATH"):
        pbrun.refuse_paths_into_the_checkout(
            ["pytest"], {"PYTHONPATH": f"{repo}/src"}, str(repo))


def test_a_sibling_that_merely_shares_a_prefix_is_not_dragged_in(repo) -> None:
    """``/home/rob/tessera-results`` beside ``/home/rob/tessera`` is a different tree."""

    pbrun.refuse_paths_into_the_checkout(
        [f"{repo}-results/out.json"], {"OUT": f"{repo}x"}, str(repo))


def test_a_relative_command_is_what_survives_being_moved(repo) -> None:
    pbrun.refuse_paths_into_the_checkout(
        ["python3", "-m", "pytest", "tests/", "-q"], {"TMPDIR": "/home/rob/tmp"},
        str(repo))


def test_a_working_tree_too_big_to_be_a_submission_is_refused(
    repo, tmp_path, monkeypatch
) -> None:
    (repo / "accident.bin").write_bytes(b"z" * 4096)
    with pytest.raises(SystemExit, match="shared object store"):
        _plan(repo, tmp_path, monkeypatch, max_add_bytes=1024)


def test_the_flag_default_and_the_module_bound_are_the_same_number() -> None:
    """``pbrun`` cannot read the bound off ``checkout`` when it builds its parser
    -- the module may not be importable there at all -- so the copy is checked
    here rather than trusted."""

    assert pbrun.MAX_ADD_GB_DEFAULT == \
        ck_module.DEFAULT_MAX_ADD_BYTES / 1024 ** 3


# -- what the widening does to placement ---------------------------------


def test_a_portable_action_drops_the_pin_a_path_would_have_forced(repo) -> None:
    assert pbrun.placement_tags(repo, explicit=[], here=False, hostname=HOST) \
        == [HOST]
    assert pbrun.placement_tags(repo, explicit=[], here=False, hostname=HOST,
                                portable=True) == []


def test_here_and_an_explicit_tag_still_mean_what_they_said(repo) -> None:
    """A hardware class the work requires, and a deliberate statement about one
    machine, are the two things the path never knew and this must not touch."""

    assert pbrun.placement_tags(repo, explicit=[], here=True, hostname=HOST,
                                portable=True) == [HOST]
    assert pbrun.placement_tags(repo, explicit=["gb10"], here=False,
                                hostname=HOST, portable=True) == ["gb10"]

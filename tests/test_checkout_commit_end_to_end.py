"""A box-local checkout stops being a one-box sentence.

This is the issue, end to end.  An agent makes itself a worktree under
``/home/rob/tmp``, submits from it, and today that action can only ever run on
the box holding the path -- measured on the live queue at 2026-09-04 07:15:
``ready 22, 22 on exactly one box (sparky 22), 22 by a box-local checkout, 0 on
more than one``, with sparklina holding a free GPU and dl380g10 eighty free
cores.

The test submits from exactly such a checkout, with the tree dirty in both ways
that matter (a tracked edit and an untracked file), and asserts three things:
the action carries no host tag, the worker runs it somewhere that is **not**
the submitter's checkout, and the real worker's code-closure verification
passes there -- which is the only thing that makes running elsewhere safe
rather than merely possible.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import sys
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

# Bound BEFORE ``pbrun`` is exec'd: ``pbrun`` puts the published mirror at the
# front of ``sys.path``, so without this the test would exercise the fleet's
# bytes instead of this checkout's.  See ``test_pbrun_placement``.
from prismabuild import pool as pool_module  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "pbrun_e2e", REPO / "tools" / "fleet" / "pbrun.py")
pbrun = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbrun)                       # type: ignore[union-attr]

HOST = socket.gethostname()


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", "-C", str(cwd), *args], capture_output=True,
                          text=True, check=True).stdout


@pytest.fixture()
def agents_worktree(tmp_path: Path) -> Path:
    """A checkout shaped like the ones the issue is about: box-local and dirty."""

    repo = tmp_path / "home" / "rob" / "tmp" / "ts101"
    repo.mkdir(parents=True)
    _git("init", "-q", "-b", "main", cwd=repo)
    _git("config", "user.email", "t@example", cwd=repo)
    _git("config", "user.name", "t", cwd=repo)
    (repo / "say.sh").write_text("echo committed\n")
    _git("add", "-A", cwd=repo)
    _git("commit", "-qm", "first", cwd=repo)
    # The two dirty shapes that must both travel.  ``git stash create`` carries
    # only the first, which is why it is not what the synthesiser uses.
    (repo / "say.sh").write_text("echo edited\n")
    (repo / "extra.sh").write_text("echo untracked\n")
    return repo


def _fleet(tmp_path: Path, monkeypatch, *, capable: bool) -> pool_module.PoolQueue:
    """A queue with one live box, which does or does not announce the capability."""

    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    monkeypatch.setattr(pbrun.ck, "SHARED_GIT_ROOT", tmp_path / "fleet" / "git")
    monkeypatch.setattr(pbrun.ck, "MIRROR_ROOT", tmp_path / "box" / "mirror")
    monkeypatch.setattr(pbrun.ck, "TREES_ROOT", tmp_path / "box" / "trees")
    # ``pbrun`` names ``SH/repo/tools/prismabuild_worker.py`` as the worker,
    # so the fleet's repo has to be a real one: this test runs the ACTUAL
    # worker, not a stub, because the property being proved is that the real
    # code-closure verification passes against a tree this box built.
    (tmp_path / "fleet").mkdir(parents=True, exist_ok=True)
    (tmp_path / "fleet" / "repo").symlink_to(REPO)
    queue = pool_module.PoolQueue(tmp_path / "fleet" / "pb-queue")
    queue.ensure_layout()
    # TWO boxes, because the issue is about the second one idling.  With one
    # announced box every item is "one box wide" whatever its addressing, and
    # the census would report the fix as no change -- which is what a first
    # version of this test asserted, wrongly.
    for host in (HOST, "other-box"):
        queue.announce(
            host=host, tags=[host, "x86", "cpu"], has_gpu=False,
            capacity={"cpu": 4, "mem_gb": 16, "gpu": 0},
            capabilities=pool_module.WORKER_CAPABILITIES if capable else (),
        )
    return queue


def _submit(repo: Path, monkeypatch, *extra: str) -> int:
    """Run the real submit path, without waiting for an outcome."""

    argv = ["pbrun", "--cwd", str(repo), "--wait-s", "1", *extra,
            "--", "bash", "say.sh"]
    with mock.patch.object(sys, "argv", argv), \
         mock.patch.object(pbrun, "await_outcome", return_value=0):
        return pbrun.main()


def _only_ready(queue: pool_module.PoolQueue) -> dict:
    items = queue.ready_items()
    assert len(items) == 1, f"expected one ready item, got {len(items)}"
    return items[0]


def test_a_box_local_checkout_is_claimable_by_any_box_and_runs_off_its_own_tree(
    agents_worktree: Path, tmp_path: Path, monkeypatch, capsys
) -> None:
    queue = _fleet(tmp_path, monkeypatch, capable=True)

    assert _submit(agents_worktree, monkeypatch) == 0
    item = _only_ready(queue)

    # 1. The pin is gone.  This is the issue's whole complaint: the tags used
    #    to be [HOST] purely because the path was under /home/rob/tmp.
    assert item["tags"] == [], (
        "a tree-addressed action must carry no host tag; it is not about a box")
    assert item["checkout_tree"] and item["checkout_commit"]

    # 2. And the queue's own measurement agrees -- ``one_box_by_path`` is the
    #    number the migration is judged on.
    census = queue.placement_census()
    assert census["wide"] == 1 and census["one_box_by_path"] == 0, census
    assert queue.placeable_hosts(item) == sorted([HOST, "other-box"]), (
        "the box that was idling must now be a box that could claim this")

    # 3. The worker runs it somewhere else entirely, off a tree it built.
    claimed = queue.claim(tags=[HOST, "x86", "cpu"], has_gpu=False,
                          capacity={"cpu": 4, "mem_gb": 16})
    assert claimed is not None
    outcome = queue.execute(claimed, python=sys.executable, timeout_s=300)
    assert outcome["status"] == "executed", outcome
    assert outcome["returncode"] == 0, outcome

    ran_in = Path(json.loads(json.dumps(outcome))["argv"][
        outcome["argv"].index("--checkout-root") + 1])
    assert ran_in != agents_worktree, (
        "the action ran in the submitter's own checkout, so nothing was widened")
    assert ran_in.is_relative_to(tmp_path / "box" / "trees")

    # 4. The dirty tree travelled -- both halves of it.  The action prints what
    #    the EDITED file says, and the untracked sibling is there too.
    assert "edited" in str(outcome["stdout"]), outcome["stdout"]
    assert (ran_in / "extra.sh").read_text() == "echo untracked\n"

    # 5. And the real worker verified the code closure against the stamp the
    #    materialiser derived from the tree it built.  That is what makes
    #    running elsewhere safe rather than merely possible: had the worktree
    #    landed on another tree, the bytes would differ and this would have
    #    been an ActionContractError instead of an execution.
    assert (ran_in / item["checkout_stamp"]).read_bytes() == \
        pbrun.ck.stamp_bytes(item["checkout_tree"])


def test_the_same_work_from_two_checkouts_is_one_action(
    agents_worktree: Path, tmp_path: Path, monkeypatch
) -> None:
    """The action key stops binding the submitter's path.

    ``params.cwd`` was in the sealed body, so the same command against the same
    content submitted from two boxes was two actions with two keys -- and the
    coordinator's "the CAS makes re-execution on another box a correctness
    non-event" was true of the CAS and not of these actions.  A second checkout
    of the same repository, at the same content, is the same request.
    """

    queue = _fleet(tmp_path, monkeypatch, capable=True)
    assert _submit(agents_worktree, monkeypatch) == 0
    first = _only_ready(queue)

    twin = tmp_path / "home" / "rob" / "tmp" / "ts102"
    subprocess.run(["cp", "-a", str(agents_worktree), str(twin)], check=True)
    queue.item_path(pool_module.READY, str(first["action_key"])).unlink()

    assert _submit(twin, monkeypatch) == 0
    second = _only_ready(queue)
    assert second["action_key"] == first["action_key"], (
        "the same content from a different path must be the same action")
    assert second["checkout_tree"] == first["checkout_tree"]


def test_a_box_that_cannot_materialise_keeps_the_action_pinned_and_says_so(
    agents_worktree: Path, tmp_path: Path, monkeypatch, capsys
) -> None:
    """The migration sequences itself, and the notice names who is holding it.

    Attested, not asserted: the decision reads a capability each box states in
    its own offer, so a box running older bytes keeps the fleet path-addressed
    by saying nothing.  Nothing anywhere maps a runtime commit onto a feature.
    """

    queue = _fleet(tmp_path, monkeypatch, capable=False)
    assert _submit(agents_worktree, monkeypatch) == 0
    item = _only_ready(queue)

    assert item["tags"] == [HOST], "an unmaterialisable tree must keep its pin"
    assert "checkout_commit" not in item
    notice = capsys.readouterr().err
    assert "PINNED" in notice and HOST in notice
    assert "announce no checkout_commit capability" in notice, notice
    assert "other-box" in notice and HOST in notice, notice

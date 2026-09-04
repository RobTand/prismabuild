"""Naming a tree, publishing it, and building it again on another box.

Each test here is a property the addressing rests on rather than a line of
code: that an unchanged tree names itself the same way twice (or every resubmit
is a CAS miss), that the whole working tree travels including the parts git
would not normally carry, that the stamp the worker verifies is DERIVED from
the tree that was built rather than copied from the request, and that the
sweep can only ever remove what it made.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import checkout as ck  # noqa: E402


def git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", "-C", str(cwd), *args], capture_output=True,
                          text=True, check=True).stdout


@pytest.fixture()
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    git("init", "-q", "-b", "main", cwd=root)
    git("config", "user.email", "t@example", cwd=root)
    git("config", "user.name", "t", cwd=root)
    (root / "a.txt").write_text("one\n")
    (root / "pkg").mkdir()
    (root / "pkg" / "b.txt").write_text("two\n")
    git("add", "-A", cwd=root)
    git("commit", "-qm", "first", cwd=root)
    return root


def _plan(repo: Path, tmp_path: Path, *, cwd: Path | None = None) -> dict[str, str]:
    identity = ck.repo_identity(cwd or repo)
    assert identity is not None
    tree, commit = ck.synthesise_tree_commit(
        identity["toplevel"], scratch=tmp_path / "scratch")
    origin = ck.ensure_shared_bare(identity["repo"], name=identity["name"],
                                   root=tmp_path / "shared")
    ck.publish_tree_commit(identity["toplevel"], commit, origin)
    return {
        "action_key": "k" * 64,
        "checkout_root": str(cwd or repo),
        "checkout_commit": commit, "checkout_tree": tree,
        "checkout_repo": identity["repo"], "checkout_origin": str(origin),
        "checkout_prefix": identity["prefix"],
        "checkout_stamp": ".pbrun-closure.test.json",
    }


def _materialise(item, tmp_path: Path, **kw) -> Path:
    return Path(ck.materialise(item, mirror_root=tmp_path / "mirror",
                               trees_root=tmp_path / "trees", **kw))


# -- naming --------------------------------------------------------------


def test_an_unchanged_tree_names_itself_the_same_way_twice(repo, tmp_path) -> None:
    """The property the action key is built on.

    ``git commit-tree`` bakes the committer and the clock into its object, so a
    synthesiser that let those vary would give an unchanged checkout a new
    commit on every submit -- and, if the commit were the identity, a new
    action key: every resubmit a CAS miss, which is the one thing the key
    exists to prevent.
    """

    (repo / "a.txt").write_text("edited\n")
    first = ck.synthesise_tree_commit(repo, scratch=tmp_path / "s")
    second = ck.synthesise_tree_commit(repo, scratch=tmp_path / "s")
    assert first == second


def test_the_identity_is_the_repository_not_the_checkout(repo, tmp_path) -> None:
    """Two worktrees of one repository are one repository.

    The identity carried the checkout's basename in a first version, so
    ``ts101`` and ``ts102`` of one repository produced two identities, two bare
    repositories and two action keys for one request -- which is the defect
    this addressing exists to remove, reintroduced by its own naming.
    """

    twin = tmp_path / "elsewhere" / "ts102"
    twin.parent.mkdir()
    subprocess.run(["cp", "-a", str(repo), str(twin)], check=True)
    assert ck.repo_identity(repo)["repo"] == ck.repo_identity(twin)["repo"]
    assert ck.repo_identity(repo)["repo"] != ck.repo_identity(twin)["toplevel"]


def test_a_directory_that_is_not_a_git_checkout_has_no_identity(tmp_path) -> None:
    plain = tmp_path / "plain"
    plain.mkdir()
    assert ck.repo_identity(plain) is None


def test_a_repository_with_no_commit_has_no_identity(tmp_path) -> None:
    """An unborn HEAD cannot parent a synthesised commit or name a repository."""

    fresh = tmp_path / "fresh"
    fresh.mkdir()
    git("init", "-q", "-b", "main", cwd=fresh)
    assert ck.repo_identity(fresh) is None


# -- what travels --------------------------------------------------------


def test_a_dirty_tree_travels_whole(repo, tmp_path) -> None:
    """Agents submit from dirty trees constantly; both dirty shapes must carry.

    ``git stash create`` is the tempting shortcut and drops the second of
    these, and ``pbrun`` learned the hard way that an untracked edit must move
    the action key -- a stale CAS result is indistinguishable from a fresh one
    from the outside.
    """

    (repo / "a.txt").write_text("edited\n")
    (repo / "new.txt").write_text("untracked\n")
    (repo / "pkg" / "b.txt").unlink()

    built = _materialise(_plan(repo, tmp_path), tmp_path)
    assert (built / "a.txt").read_text() == "edited\n"
    assert (built / "new.txt").read_text() == "untracked\n"
    assert not (built / "pkg" / "b.txt").exists(), "a deletion is a change too"


def test_a_tracked_file_that_is_also_ignored_still_travels(repo, tmp_path) -> None:
    """The reason the scratch index is seeded from HEAD instead of started empty.

    ``git add -A`` skips a path an ignore rule matches, tracked or not.  From
    an empty index that means the file is absent from the tree -- a WRONG tree,
    and one no later check can catch, because every later check compares
    against this one.
    """

    (repo / ".gitignore").write_text("a.txt\n")
    git("add", "-A", cwd=repo)
    git("commit", "-qm", "ignore it", cwd=repo)

    built = _materialise(_plan(repo, tmp_path), tmp_path)
    assert (built / "a.txt").read_text() == "one\n"


def test_the_submitters_branch_and_index_are_untouched(repo, tmp_path) -> None:
    """A submit must be invisible to whatever the agent is doing in that tree."""

    (repo / "a.txt").write_text("edited\n")
    before_head = git("rev-parse", "HEAD", cwd=repo).strip()
    before_status = git("status", "--porcelain", cwd=repo)
    before_stash = git("stash", "list", cwd=repo)

    ck.synthesise_tree_commit(repo, scratch=tmp_path / "s")

    assert git("rev-parse", "HEAD", cwd=repo).strip() == before_head
    assert git("status", "--porcelain", cwd=repo) == before_status
    assert git("stash", "list", cwd=repo) == before_stash


def test_a_submit_from_a_subdirectory_lands_in_that_subdirectory(repo, tmp_path) -> None:
    """The tree is always written from the toplevel, so the prefix must travel.

    Without it the action would run one or more directories above where it was
    asked for, which is a wrong-cwd failure that mostly looks like a missing
    file.
    """

    item = _plan(repo, tmp_path, cwd=repo / "pkg")
    assert item["checkout_prefix"] == "pkg"
    built = _materialise(item, tmp_path)
    assert built.name == "pkg"
    assert (built / "b.txt").read_text() == "two\n"
    assert (built / ck.TREE_MARKER).exists() is False, "the marker is at the top"
    assert (built.parent / ck.TREE_MARKER).exists()


# -- the stamp is derived, not copied ------------------------------------


def test_the_stamp_is_read_out_of_the_tree_that_was_built(repo, tmp_path) -> None:
    """The difference between a check and a receipt.

    A materialiser that copied the request's tree sha into the stamp would make
    ``verify_code_closure`` compare the action against itself.  Reading it back
    from the worktree is what makes a wrong tree produce wrong bytes -- so the
    closure refuses rather than running the wrong code.
    """

    item = _plan(repo, tmp_path)
    built = _materialise(item, tmp_path)
    assert (built / item["checkout_stamp"]).read_bytes() == \
        ck.stamp_bytes(item["checkout_tree"])


def test_a_commit_that_does_not_carry_the_pinned_tree_is_refused(repo, tmp_path) -> None:
    """The item's own two fields must agree, and the built tree is the arbiter."""

    item = dict(_plan(repo, tmp_path))
    item["checkout_tree"] = "0" * 40
    with pytest.raises(ck.CheckoutError, match="not the 000000000000"):
        _materialise(item, tmp_path)


def test_a_half_addressed_item_is_refused(repo, tmp_path) -> None:
    item = dict(_plan(repo, tmp_path))
    del item["checkout_origin"]
    with pytest.raises(ck.CheckoutError, match="checkout_origin"):
        _materialise(item, tmp_path)


def test_stamp_bytes_refuses_anything_that_is_not_a_tree(tmp_path) -> None:
    with pytest.raises(ck.CheckoutError):
        ck.stamp_bytes("not-a-sha")


# -- reuse and the sweep -------------------------------------------------


def test_a_second_action_at_one_tree_reuses_the_worktree(repo, tmp_path) -> None:
    item = _plan(repo, tmp_path)
    first = _materialise(item, tmp_path)
    (first / "left-behind.log").write_text("output from the first action\n")
    second = _materialise(item, tmp_path)
    assert first == second
    assert (second / "left-behind.log").exists(), (
        "an action's untracked output must not force a rebuild")


def test_the_sweep_never_removes_a_directory_it_did_not_create(repo, tmp_path) -> None:
    """The 2026-09-03 lesson, as a rule rather than as care.

    That sweep removed sixteen experiment checkouts.  The rule that came out of
    it is absolute: a sweep may only remove what it can prove it made, and the
    proof is a marker file it wrote itself.
    """

    item = _plan(repo, tmp_path)
    _materialise(item, tmp_path)
    someone_elses = tmp_path / "trees" / item["checkout_repo"] / "not-ours"
    someone_elses.mkdir(parents=True)
    (someone_elses / "precious.txt").write_text("an experiment pin\n")

    result = ck.sweep(trees_root=tmp_path / "trees",
                      mirror_root=tmp_path / "mirror", keep=0)
    assert str(someone_elses) in result["skipped"]
    assert (someone_elses / "precious.txt").exists()


def test_the_sweep_keeps_a_tree_with_a_live_claim_however_old(repo, tmp_path) -> None:
    item = _plan(repo, tmp_path)
    built = _materialise(item, tmp_path)
    result = ck.sweep(trees_root=tmp_path / "trees", mirror_root=tmp_path / "mirror",
                      keep=0, live_commits={item["checkout_commit"]})
    assert result["removed"] == []
    assert built.exists()


def test_the_sweep_removes_the_least_recently_used_tree(repo, tmp_path) -> None:
    """Least recently USED, which is why ``materialise`` touches the marker.

    A shard fan-out reuses one tree for hours without ever recreating it, so a
    sweep ordering by creation time would evict the tree everything is running
    in and keep the ones nothing has touched since.
    """

    older = _plan(repo, tmp_path)
    kept_path = _materialise(older, tmp_path)
    (repo / "a.txt").write_text("second tree\n")
    newer = _plan(repo, tmp_path)
    doomed_path = _materialise(newer, tmp_path)
    assert kept_path != doomed_path

    # Make the first look older, then USE it: the sweep must follow the use.
    os.utime(doomed_path / ck.TREE_MARKER, (1, 1))
    _materialise(older, tmp_path)

    result = ck.sweep(trees_root=tmp_path / "trees",
                      mirror_root=tmp_path / "mirror", keep=1)
    assert result["removed"] == [str(doomed_path)]
    assert kept_path.exists() and not doomed_path.exists()


def test_a_swept_tree_can_be_built_again(repo, tmp_path) -> None:
    """``git worktree`` keeps administrative state, and a stale entry refuses the path."""

    item = _plan(repo, tmp_path)
    built = _materialise(item, tmp_path)
    ck.sweep(trees_root=tmp_path / "trees", mirror_root=tmp_path / "mirror", keep=0)
    assert not built.exists()
    assert _materialise(item, tmp_path) == built


# -- what the queue is allowed to read -----------------------------------


def test_a_tree_addressed_item_is_not_box_local_whatever_its_path_says(tmp_path) -> None:
    """One rule, because the pin and the measurement of the pin must agree.

    ``checkout_root`` stays the submitter's own path on a tree-addressed item
    -- it is the honest record of where the work came from, and an empty one
    would be filed as an unexecutable stub -- so every reader has to ask this
    rather than the path.
    """

    pinned = {"checkout_root": "/home/rob/tmp/ts101"}
    portable = {"checkout_root": "/home/rob/tmp/ts101",
                "checkout_commit": "a" * 40}
    assert ck.item_is_box_local(pinned) is True
    assert ck.item_is_box_local(portable) is False
    assert ck.item_is_box_local({}) is False


def test_the_size_bound_measures_what_would_be_staged(repo, tmp_path) -> None:
    """A ``du`` would refuse the submissions that are fine and miss the one that is not.

    The 90 GB cache this bound exists to catch is normally ``.gitignore``d and
    never enters a tree at all.
    """

    (repo / ".gitignore").write_text("cache/\n")
    (repo / "cache").mkdir()
    (repo / "cache" / "big.bin").write_bytes(b"x" * 200_000)
    git("add", "-A", cwd=repo)
    git("commit", "-qm", "ignore the cache", cwd=repo)
    assert ck.pending_add_bytes(repo) == 0

    (repo / "real.txt").write_bytes(b"y" * 1_000)
    assert ck.pending_add_bytes(repo) == 1_000


# -- the shape the issue actually has ------------------------------------


def test_a_linked_worktree_travels_like_any_other_checkout(repo, tmp_path) -> None:
    """The literal shape in the issue: ``/home/rob/tmp/ts101`` is a worktree.

    Every other test here builds a plain ``git init`` repository, and a linked
    worktree is not one: its ``.git`` is a *file* pointing into the parent's
    ``worktrees/`` directory, its objects live in the parent's object store,
    and ``rev-parse --show-toplevel`` answers the linked path rather than the
    parent's.  The agents whose actions queued behind sparky were all
    submitting from exactly this, so it is worth one test that the whole path
    -- identity, synthesis, publish, materialise -- works on the case that
    caused the issue, not only on the case that was convenient to write.
    """

    linked = tmp_path / "ts101"
    git("worktree", "add", "-q", "--detach", str(linked), cwd=repo)
    (linked / "a.txt").write_text("worktree edit\n")
    (linked / "untracked.txt").write_text("came along\n")

    identity = ck.repo_identity(linked)
    assert identity is not None
    # Same repository as the parent: one bare, one identity, one action key.
    assert identity["repo"] == ck.repo_identity(repo)["repo"]

    item = _plan(repo, tmp_path, cwd=linked)
    built = _materialise(item, tmp_path)
    assert Path(built) != linked
    assert (Path(built) / "a.txt").read_text() == "worktree edit\n"
    assert (Path(built) / "untracked.txt").read_text() == "came along\n"


def test_the_marker_carries_the_readable_name_the_item_sent(repo, tmp_path) -> None:
    """Decoration, but not a field that is always empty.

    The marker's ``name`` was written from ``item["checkout_name"]`` while
    ``publish`` accepted no such field, so it was the empty string on every
    tree ever built.  A field that cannot hold anything is worse than no field:
    it reads, to whoever opens the marker, as a repository with no name.
    """

    item = dict(_plan(repo, tmp_path), checkout_name="repo")
    built = _materialise(item, tmp_path)
    marker = json.loads(
        (Path(built) / ck.TREE_MARKER).read_text())
    assert marker["name"] == "repo"


# -- the lease under a slow step -----------------------------------------


def test_a_slow_step_beats_the_lease_while_it_runs(tmp_path) -> None:
    """The bound the doc claims, tested on the mechanism that provides it.

    A first fetch into an empty mirror is the one step with no local upper
    bound, and ``pool`` reaps a lease that has been silent for 300 s -- which
    requeues an action that is running, so the same tree materialises on a
    second box and the work runs twice.  Beating around the call bounds
    nothing; the beats have to land during it.
    """

    beats: list[float] = []
    ck._run_while_beating(["sh", "-c", "sleep 0.5"],
                          beat=lambda: beats.append(time.monotonic()),
                          timeout=60.0, every=0.05)
    assert len(beats) >= 4, beats


def test_a_failing_slow_step_still_reports_what_failed(tmp_path) -> None:
    beats: list[float] = []
    with pytest.raises(ck.CheckoutError, match="exited 3"):
        ck._run_while_beating(["sh", "-c", "echo boom >&2; exit 3"],
                              beat=lambda: beats.append(time.monotonic()),
                              timeout=60.0, every=0.05)


def test_a_slow_step_that_never_ends_is_bounded(tmp_path) -> None:
    """A timeout that can hang is not a timeout -- so this test is timed.

    The first version asserted only the message and passed while taking 30.0 s
    to raise a 0.2 s timeout: the child was killed within microseconds and the
    cleanup then read the pipes, which stayed open because the transport
    grandchild still held them.  The elapsed assertion is the test; the
    message was the part that was already true.
    """

    started = time.monotonic()
    with pytest.raises(ck.CheckoutError, match="did not finish within"):
        ck._run_while_beating(["sh", "-c", "sleep 30"], beat=lambda: None,
                              timeout=0.2, every=0.05)
    assert time.monotonic() - started < 5.0

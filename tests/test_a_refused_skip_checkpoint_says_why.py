"""A stale-mention receipt says why its skip checkpoint was refused (#1069).

An owner with nothing to act on installs a skip checkpoint (#1056), and the
installation can refuse: a directory stamp the trusted rule refuses (#1062),
a full cache, an owner the latest sweep did not discover, or a document
version that is unknown.  Each refusal reads ``cacheable: false`` and sends
the owner back to the uncached census on the next pass.  Before #1069 the
receipt did not say which one, so a cycle that kept re-censusing an owner
could not be told apart from one that re-censused for a good reason without
re-deriving it.

The receipt now carries ``cache_refused`` whenever ``cacheable`` is false for
an owner that was otherwise idle, and the tier-cycle line counts each reason
(``census_stale_cache_refused.<reason>``).  An owner that caches, and one
that was not idle, carry an empty ``cache_refused``.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

import test_a_same_tick_rename_is_not_cached as tick  # noqa: E402
import test_stale_material_done_owner_retires as red  # noqa: E402
from test_stale_material_done_owner_retires import fleet  # noqa: E402,F401
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

NAMES = red.NAMES

pytestmark = pytest.mark.skipif(
    tick.COARSE is None, reason="the trusted stamp needs Linux's coarse clock")


@pytest.fixture(autouse=True)
def _fresh_checkpoints():
    stage_release.reset_skip_checkpoints()
    yield
    stage_release.reset_skip_checkpoints()


def _idle_owner(fleet, monkeypatch, *, filesystem: str = "zfs",
                trusted: bool = True):
    """A dead owner with nothing to act on, its stage stamps pinned."""

    queue, stage, _ = fleet
    _consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                       replace=False)
    state = tick._pin(monkeypatch, stage, filesystem)
    if trusted:
        state["clock"] = state["stamp"] + tick.TICK_NS
    return queue, stage, mover


def _refused(cache) -> dict[str, int]:
    prefix = "census_stale_cache_refused."
    return {name[len(prefix):]: value
            for name, value in tier_loop._read_counts(cache).items()
            if name.startswith(prefix)}


def _assert_refused(queue, stage, mover, reason: str) -> None:
    cache = tier_loop.ReceiptCache()
    before = _refused(cache)
    receipt = tick._one(tick._pass(queue, stage, index=cache.census), mover)
    assert receipt["cacheable"] is False, receipt
    assert receipt.get("cache_refused") == reason, (
        f"an idle owner whose checkpoint was refused must say why: {receipt}")
    after = _refused(cache)
    assert after.get(reason, 0) - before.get(reason, 0) == 1, (
        f"the tier-cycle line counts each refusal by reason: {after}")
    assert sum(after.values()) - sum(before.values()) == 1, after


def test_an_untrusted_directory_stamp_says_so(fleet, monkeypatch):
    queue, stage, mover = _idle_owner(fleet, monkeypatch, trusted=False)
    _assert_refused(queue, stage, mover, "directory-stamp-untrusted")


def test_a_filesystem_the_rule_does_not_list_says_so(fleet, monkeypatch):
    # Since #1070 the owner's own fragment and material are fenced by the
    # same trusted rule, and documents are tested before directories, so an
    # owner on a filesystem the rule does not list is refused on its
    # documents first. `directory-stamp-untrusted` keeps its own case in
    # `test_an_untrusted_directory_stamp_says_so`.
    queue, stage, mover = _idle_owner(fleet, monkeypatch, filesystem="nfs4")
    _assert_refused(queue, stage, mover, "document-version-unknown")


def test_a_full_cache_says_so(fleet, monkeypatch):
    queue, stage, mover = _idle_owner(fleet, monkeypatch)
    monkeypatch.setattr(stage_release, "_skip_checkpoint_capacity",
                        lambda discovered: 0)
    _assert_refused(queue, stage, mover, "cache-full")


def test_an_owner_outside_the_sweep_scope_says_so(fleet, monkeypatch):
    queue, stage, mover = _idle_owner(fleet, monkeypatch)
    real = stage_release._retain_skip_checkpoints
    monkeypatch.setattr(stage_release, "_retain_skip_checkpoints",
                        lambda discovered: real([]))
    _assert_refused(queue, stage, mover, "outside-sweep-scope")


def test_an_unknown_co_owner_version_says_so(fleet, monkeypatch):
    queue, stage, mover = _idle_owner(fleet, monkeypatch)
    monkeypatch.setattr(stage_release, "_co_owner_fences",
                        lambda memo, documents: None)
    _assert_refused(queue, stage, mover, "document-version-unknown")


def test_an_unknown_own_document_version_says_so(fleet, monkeypatch):
    queue, stage, mover = _idle_owner(fleet, monkeypatch)
    real = stage_release._install_skip_checkpoint

    def install(key, fragment_version, material_version, *args, **kwargs):
        # The version sampled after the scan read nothing: the file vanished
        # or is not regular.  Only the installation sees it here.
        return real(key, fragment_version, None, *args, **kwargs)

    monkeypatch.setattr(stage_release, "_install_skip_checkpoint", install)
    _assert_refused(queue, stage, mover, "document-version-unknown")


def test_a_cached_owner_carries_no_refusal(fleet, monkeypatch):
    queue, stage, mover = _idle_owner(fleet, monkeypatch)
    cache = tier_loop.ReceiptCache()
    receipt = tick._one(tick._pass(queue, stage, index=cache.census), mover)
    assert receipt["cacheable"] is True, receipt
    assert receipt["cache_refused"] == "", receipt
    assert _refused(cache) == {}


def test_an_owner_that_was_not_idle_carries_no_refusal(fleet, monkeypatch):
    """A stale path is pruned, not cached: nothing was refused."""

    queue, stage, _ = fleet
    _consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    cache = tier_loop.ReceiptCache()
    receipt = tick._one(tick._pass(queue, stage, index=cache.census), mover)
    assert receipt["cacheable"] is False, receipt
    assert receipt["cache_refused"] == "", receipt
    assert _refused(cache) == {}


def test_the_refusal_is_a_verdict_that_is_falsy(tmp_path):
    """The installer's answer is still usable as the bool it always was."""

    fragment = tmp_path / "fragment.json"
    fragment.write_text("{}")
    version = stage_release._path_version(fragment)
    stamps = {str(tmp_path): stage_release._directory_version(tmp_path)}
    key = ("tier", "consumer", "mover")

    refused = stage_release._install_skip_checkpoint(
        key, version, version, stamps, {})
    assert not refused and refused.refused == "outside-sweep-scope"
    stage_release._retain_skip_checkpoints([key])
    installed = stage_release._install_skip_checkpoint(
        key, version, version, stamps, {})
    assert installed and installed.refused == ""

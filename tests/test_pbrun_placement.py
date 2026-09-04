"""Where an action may run is derived from the checkout, not asked of the caller."""

from __future__ import annotations

import importlib.util
from pathlib import Path
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
from prismabuild import pool as pool_module  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "pbrun", Path(__file__).resolve().parents[1] / "tools" / "fleet" / "pbrun.py"
)
pbrun = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbrun)                       # type: ignore[union-attr]

HOST = "sparky"


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
            explicit=(), needs_gpu=False) -> str:
    intent = {"tags": tags, "needs_gpu": needs_gpu, "resources": demand}
    return pbrun.pin_notice(queue, intent, cwd=Path(cwd), hostname=HOST,
                            here=here, explicit=list(explicit))


def test_a_box_local_checkout_says_it_pinned_the_action(tmp_path) -> None:
    """The pin was a silent consequence of a path.

    ``pbrun`` printed ``tags=['sparky']`` and stopped there, so an agent that
    had just made itself a worktree under ``/home/rob/tmp`` had no way to know
    it had narrowed the fleet to one box.  129 of 394 items in the live queue
    on 2026-09-04 carried a hostname tag; 114 of them were pinned to sparky by
    a ``/home/rob/tmp/ts*`` worktree, while the other two boxes idled.
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
                     demand={"cpu": 1}, explicit=["x86"])

    assert "WARNING" in notice
    assert "exists only on sparky" in notice
    assert "--tag sparky" in notice


def test_an_unannounced_fleet_reports_unknown_rather_than_zero(tmp_path) -> None:
    """A missing diagnostic must not be printed as a measurement."""

    empty = pool_module.PoolQueue(tmp_path / "q")

    notice = _notice(empty, cwd="/home/rob/tmp/ts101", tags=[HOST],
                     demand={"cpu": 1})

    assert "PINNED to sparky" in notice
    assert "Fleet width unknown" in notice


def test_the_notice_is_printed_before_the_queue_is_told(tmp_path) -> None:
    """A pin the submitter learns about after the fact is a receipt, not a warning."""

    source = Path(pbrun.__file__).read_text()
    assert source.index("pin_notice(q, intent") < source.index("q.publish(")

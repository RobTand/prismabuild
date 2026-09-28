"""A canary leg waits on the image-holding box's generation, not refuses (#1239).

The 2026-09-28 publish refused leg-2 at submission: only sparky holds the
canary GPU image, and sparky had not yet re-offered on the new generation,
so the generation tag -- a tag the rollout itself is still turning -- was
the only blocker.  The submission now waits within the leg's own budget;
an image no box reports at all stays an immediate refusal.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import prismabuild.core as pb
import prismabuild.pool as pool
from prismabuild import publication_canary

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbcanary  # noqa: E402

IMAGE = "ghcr.io/example/glm@sha256:" + "c0" * 32
NEW_GEN = "f4887717df6f-1790565050-627c46a0ce98"
OLD_GEN = "b50d7f0126a6-1790563481-376b6a2d3928"
HOST = "sparky"
SPEC = {"name": "leg-2", "container_image": IMAGE}

CAPACITY = {"gpu": 1, "cpu": 4, "mem_gb": 8}


def _paths(tmp_path: Path) -> dict:
    return {"published_src": str(Path(pb.__file__).resolve().parent.parent),
            "queue_root": tmp_path / "pb-queue"}


def _offer(queue, host, *, generation, image=True, gpu=True):
    queue.announce(
        host=host, tags=[HOST, publication_canary.CAPABILITY,
                         f"runtime-generation:{generation}",
                         *( [pb.CONTAINER_IMAGE_TAG] if image else [] )],
        has_gpu=gpu, capacity=dict(CAPACITY),
        observed_images=[IMAGE] if image else [])


def test_an_image_with_the_old_generation_tag_waits(tmp_path):
    """The rollout's own shape: image present, generation not yet turned."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _offer(queue, HOST, generation=OLD_GEN)

    assert pbcanary.generation_pending(
        _paths(tmp_path), SPEC, NEW_GEN, HOST) is True


def test_the_same_offer_on_the_new_generation_does_not_wait(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _offer(queue, HOST, generation=NEW_GEN)

    assert pbcanary.generation_pending(
        _paths(tmp_path), SPEC, NEW_GEN, HOST) is False


def test_an_image_no_box_reports_does_not_wait(tmp_path):
    """Nowhere at all is the immediate refusal it always was."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _offer(queue, HOST, generation=OLD_GEN, image=False)

    assert pbcanary.generation_pending(
        _paths(tmp_path), SPEC, NEW_GEN, HOST) is False


def test_a_leg_without_an_image_never_waits(tmp_path):
    assert pbcanary.generation_pending(
        _paths(tmp_path), {"name": "leg-1"}, NEW_GEN, HOST) is False


def test_the_submission_retries_within_its_budget(tmp_path, monkeypatch):
    """Refused while pending, submitted once the box rolls."""

    calls: list[str] = []

    def submit_leg(paths, spec, checkout, run_id, priority, generation, argv,
                   *, extra_flags=None, extra_env=None, manifest=None,
                   submit_label=None):
        calls.append(submit_label)
        if len(calls) < 2:
            raise pbcanary.PreconditionRefused(
                "precondition refused (leg-2): pbrun submission refused")
        return "9" * 64, {"action_key": "9" * 64}

    monkeypatch.setattr(pbcanary, "submit_leg", submit_leg)
    monkeypatch.setattr(pbcanary, "generation_pending",
                        lambda *args, **kwargs: True)
    monkeypatch.setattr(pbcanary.time, "sleep", lambda s: None)
    monkeypatch.setattr(pbcanary.time, "monotonic",
                        lambda: 0.0)  # never past the deadline

    def unobserved(paths, leg, action_key, wait_s):
        # The retry is what this test proves; the wait beyond it is another
        # leg of the flow, and a timeout there is the clean place to stop.
        raise pbcanary.subprocess.TimeoutExpired(cmd="pbwait", timeout=wait_s)

    monkeypatch.setattr(pbcanary, "wait_leg", unobserved)

    import pytest
    with pytest.raises(pbcanary._SideUnverified):
        pbcanary._execute_side(
            {"published_src": "", "queue_root": tmp_path},
            leg="leg-2", spec=SPEC, argv=["/bin/true"],
            checkout=tmp_path, run_id="r", generation=NEW_GEN, priority=-10,
            fleet_root=tmp_path, leg_dir=tmp_path, side=None,
            extra_flags=[], extra_env={}, manifest=None, wait_s=600)

    # The submission was refused once, waited on the generation, and the
    # resubmit succeeded -- the wait that follows is where this test stops.
    assert len(calls) == 2


def test_a_refusal_that_is_not_generation_pending_stands(tmp_path, monkeypatch):
    from pathlib import Path as _P
    leg_dir = tmp_path
    (leg_dir / "detach.json").unlink(missing_ok=True)

    def submit_leg(*args, **kwargs):
        raise pbcanary.PreconditionRefused(
            "precondition refused (leg-2): pbrun submission refused")

    monkeypatch.setattr(pbcanary, "submit_leg", submit_leg)
    monkeypatch.setattr(pbcanary, "generation_pending",
                        lambda *args, **kwargs: False)

    import pytest
    with pytest.raises(pbcanary.PreconditionRefused):
        pbcanary._execute_side(
            {"published_src": "", "queue_root": tmp_path},
            leg="leg-2", spec=SPEC, argv=["/bin/true"],
            checkout=tmp_path, run_id="r", generation=NEW_GEN, priority=-10,
            fleet_root=tmp_path, leg_dir=tmp_path, side=None,
            extra_flags=[], extra_env={}, manifest=None, wait_s=600)


# A leg that mints the generation's one publication-canary slot cannot
# resubmit: pbrun mints the slot before its placement check, so a refused
# submission has already spent it, and the retry collides with its own slot
# ("output already exists").  The 2026-09-28 08:54Z publish of 9098f84c872b
# refused leg-2 exactly that way.

MINTING_PATHS = {"published_src": "", "publication_canary_authorizer": object()}


def _run_minting_side(tmp_path, monkeypatch, *, pending, submit_leg, clock):
    monkeypatch.setattr(pbcanary, "submit_leg", submit_leg)
    monkeypatch.setattr(pbcanary, "generation_pending", pending)
    monkeypatch.setattr(pbcanary.time, "sleep", lambda s: None)
    monkeypatch.setattr(pbcanary.time, "monotonic", clock)

    def unobserved(paths, leg, action_key, wait_s):
        raise pbcanary.subprocess.TimeoutExpired(cmd="pbwait", timeout=wait_s)

    monkeypatch.setattr(pbcanary, "wait_leg", unobserved)
    return pbcanary._execute_side(
        {**MINTING_PATHS, "queue_root": tmp_path},
        leg="leg-2", spec=SPEC, argv=["/bin/true"],
        checkout=tmp_path, run_id="r", generation=NEW_GEN, priority=-10,
        fleet_root=tmp_path, leg_dir=tmp_path, side=None,
        extra_flags=[], extra_env={}, manifest=None, wait_s=600)


def test_a_minting_leg_waits_for_the_generation_before_submitting(
        tmp_path, monkeypatch):
    import pytest
    state = {"pending": 3}
    submitted_while_pending: list[bool] = []

    def pending(*args, **kwargs):
        state["pending"] -= 1
        return state["pending"] >= 0

    def submit_leg(*args, **kwargs):
        submitted_while_pending.append(state["pending"] >= 0)
        return "9" * 64, {"action_key": "9" * 64}

    with pytest.raises(pbcanary._SideUnverified):
        _run_minting_side(tmp_path, monkeypatch, pending=pending,
                          submit_leg=submit_leg, clock=lambda: 0.0)
    assert submitted_while_pending == [False]


def test_a_minting_leg_never_resubmits_after_a_refusal(tmp_path, monkeypatch):
    import pytest
    calls: list[int] = []
    ticks = iter(range(0, 10_000, 100))

    def submit_leg(*args, **kwargs):
        calls.append(1)
        raise pbcanary.PreconditionRefused(
            "precondition refused (leg-2): pbrun submission refused")

    # The box looked rolled when the leg submitted; the refusal then reads
    # as generation-pending again (a race, or another blocker).  The slot
    # is spent either way, so the refusal stands.
    with pytest.raises(pbcanary.PreconditionRefused):
        _run_minting_side(tmp_path, monkeypatch,
                          pending=lambda *a, **k: bool(calls),
                          submit_leg=submit_leg,
                          clock=lambda: float(next(ticks)))
    assert calls == [1]


def test_a_minting_leg_still_pending_at_its_deadline_does_not_submit(
        tmp_path, monkeypatch):
    import pytest
    calls: list[int] = []
    ticks = iter(range(0, 10_000, 100))

    def submit_leg(*args, **kwargs):
        calls.append(1)
        return "9" * 64, {"action_key": "9" * 64}

    with pytest.raises(pbcanary.PreconditionRefused, match="did not test"):
        _run_minting_side(tmp_path, monkeypatch,
                          pending=lambda *a, **k: True,
                          submit_leg=submit_leg,
                          clock=lambda: float(next(ticks)))
    assert calls == []

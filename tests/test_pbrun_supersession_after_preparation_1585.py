"""A supersession is filed after everything that can refuse, not before (#1585).

The record ``pbrun --supersedes OLD`` files is immutable and followed unconditionally.  It used to be
filed before ``publish_consumer_row`` did the window's preparation (the stage tier, the phase table,
the seal), so a refusal there left ``OLD`` superseded by a key that never published: the corrected
submission is another key and conflicts with the record, and following the record finds an absent
action (the D44 recovery of 2026-10-06).  Now ``pbrun`` checks everything it can refuse first, writes
nothing, and files the record immediately before the row goes in.

Real ``pbrun.main`` against a local queue, as in ``test_pbrun_residency_stage_submission``.
"""
from __future__ import annotations

import json
from pathlib import Path
import socket
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import action_edges as ae, pool, residency_plan  # noqa: E402
import pbrun  # noqa: E402
from test_pbrun_detach import _checkout  # noqa: E402
from test_pbrun_residency_stage_submission import _announce_tier, _manifest  # noqa: E402

OLD = "1" * 64


def _setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, tier: bool = True):
    work = _checkout(tmp_path)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(_manifest()))
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(host="sparky", tags=["sparky", "gb10"], has_gpu=True,
                   capacity={"cpu": 4, "mem_gb": 16, "gpu": 1})
    if tier:
        _announce_tier(queue, mountpoint=tmp_path / "stage")
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    return queue, work, manifest


def _publish_old(queue: pool.PoolQueue, *, ended: bool = True) -> None:
    """The key to be superseded: a published row, withdrawn when ``ended``."""
    queue.publish(action_key=OLD, cas_root=str(queue.root / "cas"), checkout_root=str(queue.root / "co"),
                  worker_script=str(queue.root / "worker.py"), resources={"cpu": 1},
                  max_attempts=1, retry_safe=True, tags=["sparky"])
    if ended:
        queue.withdraw(OLD, reason="the producer failed", by="test")


def _argv(monkeypatch, work: Path, manifest: Path, *options: str, word: str) -> None:
    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--cwd", str(work), "--wait-s", "0.01", "--detach",
        "--data-manifest", str(manifest), *options, "--", "/bin/bash", "-lc", word])


def _refused(monkeypatch, work, manifest, *options: str, word: str) -> str:
    _argv(monkeypatch, work, manifest, *options, word=word)
    with pytest.raises(SystemExit) as caught:
        pbrun.main()
    return str(caught.value)


def _submitted(monkeypatch, capsys, work, manifest, *options: str, word: str) -> str:
    _argv(monkeypatch, work, manifest, *options, word=word)
    capsys.readouterr()
    assert pbrun.main() == 0
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return str(json.loads(lines[-1])["action_key"])


def test_a_staged_submission_refused_in_preparation_files_no_supersession_and_the_corrected_one_supersedes(
        tmp_path, monkeypatch, capsys):
    """The incident: refused in the window's preparation, then resubmitted corrected.

    No stage tier is announced, so preparing the window refuses.  Nothing is superseded.  The
    corrected submission (another key: different work) then supersedes ``OLD`` and publishes.
    """
    queue, work, manifest = _setup(tmp_path, monkeypatch, tier=False)
    _publish_old(queue)
    message = _refused(monkeypatch, work, manifest, "--residency", "stage", "--supersedes", OLD,
                       word="printf first")
    assert "stage tier" in message or "tier" in message, message
    assert ae.read_supersession(queue.root, OLD) is None, "a refused submission superseded the old key"

    _announce_tier(queue, mountpoint=tmp_path / "stage")
    corrected = _submitted(monkeypatch, capsys, work, manifest, "--residency", "stage",
                           "--supersedes", OLD, word="printf corrected")
    assert ae.read_supersession(queue.root, OLD)["new"] == corrected
    assert pool._read_json(queue.item_path(pool.READY, corrected)) is not None, "the replacement never published"


def test_a_refused_seal_after_the_phase_table_files_no_supersession(tmp_path, monkeypatch):
    """A refusal late in the window's preparation (the seal), after everything else succeeded."""
    queue, work, manifest = _setup(tmp_path, monkeypatch)
    _publish_old(queue)

    def refuse(*args, **kwargs):
        raise residency_plan.ResidencyPlanError("the window cannot be sealed")

    monkeypatch.setattr(pbrun.residency_plan, "seal_window", refuse)
    message = _refused(monkeypatch, work, manifest, "--residency", "stage", "--supersedes", OLD,
                       word="printf sealed")
    assert "cannot be sealed" in message, message
    assert ae.read_supersession(queue.root, OLD) is None


def test_a_refused_row_on_the_plain_path_files_no_supersession(tmp_path, monkeypatch):
    queue, work, manifest = _setup(tmp_path, monkeypatch)
    _publish_old(queue)

    def refuse(*args, **kwargs):
        raise SystemExit("pbrun: the row cannot be built")

    monkeypatch.setattr(pbrun, "publication_row", refuse)
    assert "row cannot be built" in _refused(monkeypatch, work, manifest, "--supersedes", OLD, word="printf plain")
    assert ae.read_supersession(queue.root, OLD) is None


def test_what_can_be_refused_early_is_refused_before_any_preparation(tmp_path, monkeypatch):
    """``OLD`` still runs: the refusal is the cheap one, before the window is prepared at all."""
    queue, work, manifest = _setup(tmp_path, monkeypatch)
    _publish_old(queue, ended=False)

    def prepared(*args, **kwargs):
        raise AssertionError("the window was prepared for a submission that could never supersede")

    monkeypatch.setattr(pbrun, "residency_stage_rows", prepared)
    message = _refused(monkeypatch, work, manifest, "--residency", "stage", "--supersedes", OLD,
                       word="printf early")
    assert "cannot be superseded" in message, message
    assert ae.read_supersession(queue.root, OLD) is None


def test_the_record_is_filed_at_the_commit_point_and_a_publish_refusal_is_recovered_by_the_same_submission(
        tmp_path, monkeypatch, capsys):
    """The one place a record can precede a missing row: the publish itself refuses.

    The same submission (same content, same key) finds its own record when retried, so the
    replacement the record names does publish: it is not stranded.
    """
    queue, work, manifest = _setup(tmp_path, monkeypatch)
    _publish_old(queue)
    real = pbrun.publish_or_refuse
    calls = []

    def flaky(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise SystemExit("pbrun: the queue refused the row once")
        return real(*args, **kwargs)

    monkeypatch.setattr(pbrun, "publish_or_refuse", flaky)
    assert "refused the row once" in _refused(monkeypatch, work, manifest, "--residency", "stage",
                                              "--supersedes", OLD, word="printf retry")
    filed = ae.read_supersession(queue.root, OLD)
    assert filed is not None, "the record is filed immediately before the publish"
    key = _submitted(monkeypatch, capsys, work, manifest, "--residency", "stage", "--supersedes", OLD,
                     word="printf retry")
    assert key == filed["new"], "the retry is the same work, so it is the key the record names"
    assert pool._read_json(queue.item_path(pool.READY, key)) is not None


def test_check_supersession_writes_nothing_and_refuses_what_filing_refuses(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _publish_old(queue)
    new, other = "2" * 64, "3" * 64
    record = ae.check_supersession(queue, OLD, new=new, new_kind=ae.PRODUCER_KEY)
    assert record["old"] == OLD and record["new"] == new
    assert ae.read_supersession(queue.root, OLD) is None, "a check wrote a record"

    with pytest.raises(ae.ActionEdgeError, match="cannot supersede itself"):
        ae.check_supersession(queue, OLD, new=OLD, new_kind=ae.PRODUCER_KEY)
    ae.file_supersession(queue, OLD, new=new, new_kind=ae.PRODUCER_KEY)
    assert ae.check_supersession(queue, OLD, new=new, new_kind=ae.PRODUCER_KEY) == record, "the same successor"
    with pytest.raises(ae.ActionEdgeError, match="conflicts with the immutable record"):
        ae.check_supersession(queue, OLD, new=other, new_kind=ae.PRODUCER_KEY)
    # the conflict is the same refusal filing gives
    with pytest.raises(ae.ActionEdgeError, match="conflicts with the immutable record|already filed|differ"):
        ae.file_supersession(queue, OLD, new=other, new_kind=ae.PRODUCER_KEY)


def test_check_supersession_refuses_a_key_that_has_not_ended(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _publish_old(queue, ended=False)
    with pytest.raises(ae.ActionEdgeError, match="cannot be superseded"):
        ae.check_supersession(queue, OLD, new="2" * 64, new_kind=ae.PRODUCER_KEY)
    assert ae.read_supersession(queue.root, OLD) is None

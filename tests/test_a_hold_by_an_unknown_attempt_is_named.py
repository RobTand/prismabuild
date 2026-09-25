"""A hold by an attempt whose state is unknown is named, never silent (#1065).

A path another attempt that can still commit has prewritten holds a
consumed batch's retirement, and an ended attempt's prewrite, until that
attempt commits or ends (#1053).  "Can still commit" is ``live`` or
``unknown``, and ``unknown`` covers an attempt with no queue row at all: a
row that was lost, or never filed.  Nothing ends such an attempt, so before
#1065 its prewrite held every overlapping batch forever, and the tick said
nothing -- the retirement's ``quiet()`` and the sweep's hold both dropped
the report.

Now a hold by an unknown attempt files ``output-origin-held-by-unknown-
attempt`` once per change, naming the holder and why it is unknown, and
``pbstatus --blocked-origins`` lists it under ``held_by_unknown``.  An
attempt with no queue row whose records are older than the lease timeout is
reported as orphaned, with a remedy.  The hold itself stays: the report is
the change, never a delete.  A live writer's hold stays quiet.
"""
from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

from prismabuild import pool  # noqa: E402
import prismabuild.produced_output as po  # noqa: E402
import pbstatus  # noqa: E402

import test_prepaid_writer_integration as fx  # noqa: E402
from test_consumed_origin_retirement import _bind_owner  # noqa: E402
from test_a_dead_producers_origin_files_survive_every_sweep import (  # noqa: E402
    _dead_consumed_batch,
)
import test_write_only_produced_output as wo  # noqa: E402

HELD = po.ORIGIN_HELD_BY_UNKNOWN_EVENT


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT", "PRISMABUILD_ACTION_KEY"):
        monkeypatch.delenv(name, raising=False)
    po._UNFILED_REPORTS.clear()
    po._KEPT_PREWRITES.clear()
    yield
    po._UNFILED_REPORTS.clear()
    po._KEPT_PREWRITES.clear()


def _events(events, name):
    return [event for event in events if event.get("event") == name]


def _drop_row(queue: pool.PoolQueue, key: str) -> None:
    """The holder's queue row is lost: no record of its key anywhere."""

    queue.item_path(pool.CLAIMED, key).unlink()
    assert po._key_generation(queue, key) == ("absent", None)


def _age(queue: pool.PoolQueue, instance: dict, seconds: float) -> None:
    """Every record of the attempt was last written ``seconds`` ago."""

    scope = po.instance_dir(queue.root, instance)
    stamp = os.stat(scope).st_mtime - seconds
    for directory, _dirs, files in os.walk(scope):
        for name in files:
            os.utime(Path(directory) / name, (stamp, stamp))


def _foreign_holder(tmp_path: Path, queue, template, path: Path) -> dict:
    """Another action, of an overlapping template, prewrote ``path``."""

    overlapping = po.validate_template({
        **template, "template_id": "write-only-overlapping-v1",
        "output_prefix": str(tmp_path)})
    holder = _bind_owner(queue, overlapping, fx._hexkey("foreign"))
    filed = wo._prewrite(queue, holder, overlapping, "f1", [path], 8)
    assert filed["ok"], filed
    return holder


def _entry(queue, instance) -> dict:
    return po._read_commitments(
        po._commitments_path(queue.root, instance))["batches"]["b1"]


def test_a_consumed_batch_held_by_an_attempt_with_no_row_is_named(
        tmp_path: Path) -> None:
    template, queue, instance, path = _dead_consumed_batch(tmp_path)
    holder = _foreign_holder(tmp_path, queue, template, path)
    owner = holder["owner_action_key"]
    nonce = holder["owner_attempt"]["nonce"]

    # While the holder is live, its hold is quiet, as before.
    assert po.origin_retirement_tick(queue) == []

    _drop_row(queue, owner)
    events = po.origin_retirement_tick(queue)

    held = _events(events, HELD)
    assert len(held) == 1, (
        f"a hold by an attempt with no queue row must be reported: {events}")
    assert held[0]["reason"] == "held-by-unknown-attempt"
    assert "remedy" not in held[0]
    [named] = held[0]["holders"]
    assert (named["path"], named["owner_action_key"], named["nonce"],
            named["batch_id"], named["foreign"], named["state"],
            named["why"], named["orphaned"]) == (
        str(path), owner, nonce, "f1", True, "unknown", "no-queue-row", False)
    assert Path(named["prewrite_record"]).exists()
    assert path.exists(), "the hold stays: the report is not a delete"
    entry = _entry(queue, instance)
    assert not entry.get("retiring") and not entry.get("origin_reclaimed")

    assert po.origin_retirement_tick(queue) == [], "once per change"

    listed = pbstatus.read_blocked_origins(queue.root)["held_by_unknown"]
    assert [(item["ref"]["batch_id"], item["reason"],
             [holder["why"] for holder in item["holders"]])
            for item in listed] == [
        ("b1", "held-by-unknown-attempt", ["no-queue-row"])], listed

    # Past the lease timeout with nothing written, it is orphaned: reported
    # once more, with the remedy, and still held.
    _age(queue, holder, pool.LEASE_TIMEOUT_S + 60)
    events = po.origin_retirement_tick(queue)
    held = _events(events, HELD)
    assert len(held) == 1, events
    assert held[0]["reason"] == "held-by-orphaned-attempt"
    assert held[0]["remedy"] == po.ORPHANED_HOLDER_REMEDY
    assert held[0]["holders"][0]["orphaned"] is True
    assert path.exists()
    assert po.origin_retirement_tick(queue) == [], "once per change"
    listed = pbstatus.read_blocked_origins(queue.root)["held_by_unknown"]
    assert [item["reason"] for item in listed] == ["held-by-orphaned-attempt"]

    # The remedy frees it: the prewrite record removed, the batch retires.
    Path(held[0]["holders"][0]["prewrite_record"]).unlink()
    retired = _events(po.origin_retirement_tick(queue), po.ORIGIN_RETIRED_EVENT)
    assert len(retired) == 1 and retired[0]["unlinked"] == [str(path)], retired
    assert pbstatus.read_blocked_origins(queue.root)["held_by_unknown"] == []


def test_a_queued_holder_is_named_with_why(tmp_path: Path) -> None:
    """A holder whose key waits in ``ready`` is unknown, and says so."""

    template, queue, instance, path = _dead_consumed_batch(tmp_path)
    holder = _foreign_holder(tmp_path, queue, template, path)
    owner = holder["owner_action_key"]
    row = queue.item_path(pool.CLAIMED, owner)
    os.replace(row, queue.item_path(pool.READY, owner))

    held = _events(po.origin_retirement_tick(queue), HELD)
    assert [holder["why"] for holder in held[0]["holders"]] == ["queued"], held
    assert held[0]["holders"][0]["orphaned"] is False


def test_a_live_holder_stays_quiet_in_the_listing(tmp_path: Path) -> None:
    template, queue, instance, path = _dead_consumed_batch(tmp_path)
    _foreign_holder(tmp_path, queue, template, path)
    assert po.origin_retirement_tick(queue) == []
    assert pbstatus.read_blocked_origins(queue.root)["held_by_unknown"] == []


def test_an_ended_prewrite_held_by_an_attempt_with_no_row_is_named(
        tmp_path: Path) -> None:
    """The sweep's hold (#949, #1053) names an unknown holder too."""

    template = wo._template(tmp_path / "canonical")
    queue = wo._queue(tmp_path)
    dead = _bind_owner(queue, template, fx._hexkey("dead"))
    path = Path(template["output_prefix"]) / "entries" / "at-43.pt"
    assert wo._prewrite(queue, dead, template, "b43", [path], 64)["ok"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"at-43, never committed")
    queue.finish(dead["owner_action_key"], status="failed")
    holder = _foreign_holder(tmp_path, queue, template, path)
    _drop_row(queue, holder["owner_action_key"])

    events = po.origin_retirement_tick(queue)

    assert not _events(events, po.ORIGIN_PREWRITE_ORPHANED_EVENT), events
    assert not _events(events, po.ORIGIN_PREWRITE_RECLAIMED_EVENT), events
    held = _events(events, HELD)
    assert len(held) == 1, (
        f"the ended prewrite's hold must name its unknown holder: {events}")
    assert held[0]["prewrite"] == po._batch_report_key(dead, "b43")
    assert [(item["why"], item["orphaned"]) for item in held[0]["holders"]] == [
        ("no-queue-row", False)]
    assert _events(po.origin_retirement_tick(queue), HELD) == [], (
        "once per change")

    listed = pbstatus.read_blocked_origins(queue.root)["held_by_unknown"]
    assert [item["prewrite"] for item in listed] == [
        po._batch_report_key(dead, "b43")], listed

    # Orphaned: the kept decision is taken again, and reported once more.
    _age(queue, holder, pool.LEASE_TIMEOUT_S + 60)
    held = _events(po.origin_retirement_tick(queue), HELD)
    assert len(held) == 1 and held[0]["reason"] == "held-by-orphaned-attempt"
    assert path.exists() and (po._prewrites_dir(queue.root, dead)
                              / "b43.prewrite.json").exists()

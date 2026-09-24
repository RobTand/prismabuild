"""A dead producer's origin files survive every sweep; a successor's are never lost (#1053).

The three hazards the #1053 fix must not open:

(a) A dead producer's unretired batch that a live action reads keeps its
    origin files through every sweep.  Retiring the batch closes its stage
    records; it never reclaims or deletes its origin.
(b) A present file at an ended attempt's prewrite path that a live successor
    has committed is ``superseded``: never reported ``orphaned``, never
    deleted.
(c) A retirement racing a successor's ``rename(tmp, path)`` never deletes the
    successor's file, wherever the rename lands.

Everything runs on a synthetic stage and origin prefix under ``tmp_path``.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

from prismabuild import pool, residency_map  # noqa: E402
import prismabuild.produced_output as po  # noqa: E402
import stage_release  # noqa: E402

import test_prepaid_writer_integration as fx  # noqa: E402
from test_a_dead_producers_batches_and_paths_are_released import (  # noqa: E402
    DEAD_BATCH_RETIRED, PREWRITE_ORPHANED, PREWRITE_RECLAIMED, TIER, _Fleet,
    _Producer, _events,
)
from test_consumed_origin_retirement import _bind_owner, _commit  # noqa: E402
import test_write_only_produced_output as wo  # noqa: E402


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT", "PRISMABUILD_ACTION_KEY"):
        monkeypatch.delenv(name, raising=False)
    stage_release.reset_holder_reports()
    po._UNFILED_REPORTS.clear()
    yield
    stage_release.reset_holder_reports()
    po._UNFILED_REPORTS.clear()


def _strand_holding(producer: _Producer, batch_id: str, res: dict, path: Path,
                    payload: bytes) -> None:
    """Mover done with its copy staged and its tokens held: #929's shape."""

    q = producer.fleet.q
    mover = str(res["mover_key"])
    namespace = str(res["batch_namespace"])
    claimed = fx._claim_mover(q, f"w-mover-{batch_id}")
    assert claimed["action_key"] == mover
    staged = producer.fleet.stage / "produced-output" / namespace / path.name
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(payload)
    residency_map.write_fragment(
        po.output_fragment_root(q.root / pool.RESIDENCY), {
            "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
            "consumer_action_key": namespace, "mover_action_key": mover,
            "tier_id": TIER, "stage_root": str(producer.fleet.stage),
            "manifest_sha256": "a" * 64,
            "entries": {residency_map.residency_map_key(str(path), 0): {
                "stage_path": str(staged), "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "offset": 0}}})
    q.record_move(mover, {
        "consumer_action_key": namespace, "tier_id": TIER,
        "stage_root": str(producer.fleet.stage), "manifest_sha256": "a" * 64,
        "range_start_bytes": 0, "range_end_bytes": len(payload),
        "bytes_staged": len(payload), "complete": True})
    q.finish(mover, status="executed")
    assert producer.fleet.ledger.holder_tokens(mover), "its tokens are held"


def _identity(path: Path) -> tuple[int, bytes]:
    return (os.lstat(path).st_ino, path.read_bytes())


# -- (a) ---------------------------------------------------------------------


def test_a_dead_producers_batches_a_live_action_reads_keep_their_origin(
        tmp_path: Path) -> None:
    """Both retirement routes close the records; no sweep touches the origin.

    One batch is R13's shape (tokens released, copy gone: the tick's route),
    the other still holds its tokens and its copy (the #929 sweep's route).
    A live relaunch holds both origin files open, as R13's resume reads 392
    of its predecessor's batches.
    """

    fleet = _Fleet(tmp_path)
    dead = fleet.producer("r13")
    stranded = fleet.prefix / "entries" / "boundary-448-43-at-43.pt"
    holding = fleet.prefix / "entries" / "boundary-384-1-at-1.pt"
    res = dead.publish("b43-g7", stranded, b"boundary at 43")
    dead.strand_like_r13("b43-g7", res, stranded, b"boundary at 43")
    res = dead.publish("b1-g6", holding, b"boundary at 1")
    _strand_holding(dead, "b1-g6", res, holding, b"boundary at 1")
    dead.fail()
    resume = fleet.producer("r13-resume")
    before = {path: _identity(path) for path in (stranded, holding)}
    with open(stranded, "rb") as first, open(holding, "rb") as second:
        events: list[dict] = []
        for _ in range(3):
            events += fleet.sweep() + fleet.tick()
            assert {path: _identity(path) for path in before} == before
        assert first.read() == b"boundary at 43"
        assert second.read() == b"boundary at 1"

    for batch_id in ("b43-g7", "b1-g6"):
        entry = dead.entry(batch_id)
        assert po._batch_stage_retired(entry), batch_id
        assert entry.get("origin_reclaimed") is False, batch_id
    assert [event["batch"] for event in _events(events, DEAD_BATCH_RETIRED)] == [
        dead.coordinates("b43-g7")]
    assert [event["batch_id"] for event in _events(
        events, stage_release.PRODUCED_ORPHAN_EVENT)] == ["b1-g6"]
    assert not _events(events, po.ORIGIN_RETIREMENT_REFUSED_EVENT), events
    # The relaunch may regenerate them: a dead action owns nothing live.
    assert resume.prewrite("b43-g7", [stranded, Path(f"{stranded}.tmp")],
                           64)["ok"] is True


# -- (b) ---------------------------------------------------------------------


def test_a_file_a_live_successor_committed_is_superseded_never_orphaned(
        tmp_path: Path) -> None:
    """Held while the successor writes, superseded once it commits, kept."""

    fleet = _Fleet(tmp_path)
    dead = fleet.producer("r13")
    path = fleet.prefix / "entries" / "cotangent-0-100-at-43.pt"
    assert dead.prewrite("b43p0-g1", [path, Path(f"{path}.tmp")],
                         64)["ok"] is True
    dead.write(path, b"at-43, never committed")
    dead.fail()

    successor = fleet.producer("r13-resume")
    regenerated = b"at-43, regenerated and committed"
    assert successor.prewrite("b43p0-g1", [path], len(regenerated))["ok"] is True
    successor.write(path, regenerated)
    held = fleet.sweep() + fleet.tick()
    assert not _events(held, PREWRITE_ORPHANED), (
        "a path a live successor prewrote is held, never orphaned")
    assert not _events(held, PREWRITE_RECLAIMED)
    assert dead.prewrite_record("b43p0-g1").exists()

    res = po.publish_prepaid_batch(
        fleet.q, successor.inst, fleet.template,
        [successor.descriptor(path, regenerated)], batch_id="b43p0-g1",
        tier=TIER, cas_root=fleet.cas_root,
        producer_action_key=successor.owner, command_extra=["--unpaced"])
    assert res.get("ok") is True, res
    assert path.read_bytes() == regenerated

    events = fleet.sweep() + fleet.tick()

    assert not _events(events, PREWRITE_ORPHANED), events
    reclaimed = _events(events, PREWRITE_RECLAIMED)
    assert [(event["prewrite"], event["reason"]) for event in reclaimed] == [
        (dead.coordinates("b43p0-g1"), "superseded")]
    assert reclaimed[0]["superseded"] == [{
        "path": str(path), "owner_action_key": successor.owner,
        "nonce": successor.inst["owner_attempt"]["nonce"],
        "batch_id": "b43p0-g1"}]
    assert not dead.prewrite_record("b43p0-g1").exists()
    assert path.read_bytes() == regenerated, "never deleted"
    assert not _events(fleet.sweep() + fleet.tick(), PREWRITE_ORPHANED)


# -- (c) ---------------------------------------------------------------------


def _successor_rename(path: Path, payload: bytes):
    """The successor's own write: a temporary, then ``rename`` onto the name."""

    real_replace = os.replace

    def write() -> None:
        temporary = path.with_name(path.name + ".successor.tmp")
        temporary.write_bytes(payload)
        real_replace(temporary, path)
    return write


def _interpose(monkeypatch, target: Path, action, *, after: bool) -> list[str]:
    """Run ``action`` at the first ``rename`` or ``unlink`` of ``target``.

    Before the call (the successor's rename lands first) or after it. Both
    calls are wrapped because the delete may be either: a plain ``unlink``
    on a tree without the fix, a rename to a private name with it. Only the
    exact path is interposed on; every other call passes straight through.
    """

    fired: list[str] = []
    real = {"rename": os.rename, "unlink": os.unlink}

    def wrap(name: str):
        def call(source, *args, **kwargs):
            first = os.fspath(source) == str(target) and not fired
            if first and not after:
                fired.append(name)
                action()
            result = real[name](source, *args, **kwargs)
            if first and after:
                fired.append(name)
                action()
            return result
        return call

    monkeypatch.setattr(os, "rename", wrap("rename"))
    monkeypatch.setattr(os, "unlink", wrap("unlink"))
    return fired


def _dead_consumed_batch(tmp_path: Path):
    """A dead producer's consumed origin-only batch, due for deletion."""

    template = wo._template(tmp_path / "canonical")
    queue = wo._queue(tmp_path)
    instance, path, committed = _commit(queue, template, "dead",
                                        payload=b"the dead attempt's bytes")
    queue.finish(instance["owner_action_key"], status="failed")
    assert po._producer_attempt_state(queue, instance) == "dead"
    return template, queue, instance, path


@pytest.mark.parametrize("after", [False, True],
                         ids=["rename-lands-first", "rename-lands-after"])
def test_a_retirement_racing_a_successors_rename_keeps_its_file(
        tmp_path: Path, monkeypatch, after: bool) -> None:
    """The delete never removes a file the successor renamed onto the name.

    The successor here is a writer the retirement could not see: its
    prewrite is under another template's lock (an overlapping prefix), or
    it landed after the owners were read. Its ``rename`` is interposed at
    the one moment that matters, the retirement's own call on the path.
    """

    template, queue, instance, path = _dead_consumed_batch(tmp_path)
    successor = b"the successor's bytes"
    fired = _interpose(monkeypatch, path, _successor_rename(path, successor),
                       after=after)

    events = po.origin_retirement_tick(queue)

    assert fired, "the retirement never reached the path"
    assert path.read_bytes() == successor, (
        "the retirement deleted the successor's file")
    retired = _events(events, po.ORIGIN_RETIRED_EVENT)
    assert len(retired) == 1, events
    if after:
        assert retired[0]["unlinked"] == [str(path)]
    else:
        assert retired[0]["unlinked"] == []
        assert retired[0]["superseded"] == [str(path)]
    leftovers = [name for name in os.listdir(path.parent)
                 if ".pb-retiring-" in name]
    assert leftovers == []


def test_a_retirement_holds_while_a_live_successors_prewrite_names_the_path(
        tmp_path: Path) -> None:
    """Under the lock: a prewrite filed first holds the delete, then supersedes."""

    template, queue, instance, path = _dead_consumed_batch(tmp_path)
    successor = _bind_owner(queue, template, fx._hexkey("successor"))
    payload = b"the successor's committed bytes"
    assert wo._prewrite(queue, successor, template, "b1", [path],
                        len(payload))["ok"] is True, (
        "a dead action's batch does not refuse a relaunch")

    assert po.origin_retirement_tick(queue) == [], (
        "a live prewrite of the path holds the retirement, quietly")
    entry = po._read_commitments(
        po._commitments_path(queue.root, instance))["batches"]["b1"]
    assert not entry.get("retiring") and not entry.get("origin_reclaimed")

    _successor_rename(path, payload)()
    committed = po.commit_origin_batch(
        queue, successor, template,
        [wo._descriptor(successor, template, path, payload)], batch_id="b1",
        lifetime=po.ORIGIN_LIFETIME_RETAIN)
    assert committed["ok"], committed

    events = po.origin_retirement_tick(queue)

    retired = _events(events, po.ORIGIN_RETIRED_EVENT)
    assert len(retired) == 1, events
    assert retired[0]["unlinked"] == []
    assert retired[0]["superseded"] == [str(path)]
    assert retired[0]["superseded_by"] == {str(path): {
        "owner_action_key": successor["owner_action_key"],
        "nonce": successor["owner_attempt"]["nonce"], "batch_id": "b1"}}
    assert path.read_bytes() == payload


def _private(instance: dict, path: Path) -> Path:
    tag = hashlib.sha256(po._batch_report_key(
        instance, "b1").encode()).hexdigest()[:16]
    return Path(po._retiring_name(str(path), tag))


def test_an_interrupted_delete_of_the_committed_file_is_finished(
        tmp_path: Path) -> None:
    """A crash after the move aside: the next tick deletes the moved file."""

    template, queue, instance, path = _dead_consumed_batch(tmp_path)
    private = _private(instance, path)
    os.rename(path, private)

    events = po.origin_retirement_tick(queue)

    retired = _events(events, po.ORIGIN_RETIRED_EVENT)
    assert len(retired) == 1 and retired[0]["unlinked"] == [str(path)], events
    assert not private.exists() and not path.exists()


def test_an_interrupted_delete_puts_a_writers_file_back(tmp_path: Path) -> None:
    """A crash after moving a writer's file aside: the next tick restores it."""

    template, queue, instance, path = _dead_consumed_batch(tmp_path)
    private = _private(instance, path)
    _successor_rename(path, b"the successor's bytes")()
    os.rename(path, private)

    events = po.origin_retirement_tick(queue)

    retired = _events(events, po.ORIGIN_RETIRED_EVENT)
    assert len(retired) == 1, events
    assert retired[0]["unlinked"] == [] and retired[0]["superseded"] == [
        str(path)]
    assert path.read_bytes() == b"the successor's bytes"
    assert not private.exists()


def test_a_writers_file_displaced_by_a_later_one_is_kept_and_named(
        tmp_path: Path) -> None:
    """Two writes since the move aside: nothing is deleted, an operator decides."""

    template, queue, instance, path = _dead_consumed_batch(tmp_path)
    private = _private(instance, path)
    # The committed inode stays allocated, so no later file can reuse its
    # number and read as the committed file changed in place.
    os.link(path, tmp_path / "committed-inode")
    _successor_rename(path, b"first")()
    os.rename(path, private)
    _successor_rename(path, b"second")()

    events = po.origin_retirement_tick(queue)

    refused = _events(events, po.ORIGIN_RETIREMENT_REFUSED_EVENT)
    assert len(refused) == 1, events
    assert refused[0]["reason"].startswith("origin-displaced")
    assert str(private) in refused[0]["reason"]
    assert private.read_bytes() == b"first" and path.read_bytes() == b"second"
    assert po.origin_retirement_tick(queue) == [], "reported once"


def test_a_file_that_names_no_prefix_does_not_stop_the_owner_lookup(
        tmp_path: Path) -> None:
    """Only a template's prefix is read; a file without one names no paths.

    The owner lookup reads every filed template's ``output_prefix``. A file
    beside them that is not a template PB filed -- no prefix, or not JSON
    -- has no attempts, so it is skipped rather than refusing every
    prewrite and every decision on the queue.
    """

    fleet = _Fleet(tmp_path)
    templates = fleet.q.root / "residency" / po.OUTPUT_TEMPLATES_SUBDIR
    (templates / "template-0.json").write_text(
        '{"schema": "prismaquant.prismabuild.produced_output_template.v1", '
        '"index": 0}\n')
    (templates / "torn.json").write_text("{")
    dead = fleet.producer("r13")
    path = fleet.prefix / "entries" / "cotangent-0-64-at-43.pt"
    assert dead.prewrite("b43p0-g1", [path], 64)["ok"] is True
    dead.write(path, b"at-43, never committed")
    dead.fail()
    live = fleet.producer("r13-resume")

    events = fleet.tick()

    assert [event["event"] for event in events] == [PREWRITE_ORPHANED], events
    other = fleet.prefix / "entries" / "cotangent-0-65-at-43.pt"
    assert live.prewrite("b43p1-g1", [other], 64)["ok"] is True

"""A consumed produced-output origin is retired once its consumers commit (#914).

An origin-only batch (#912) is written once by its producer and read by a
later action.  PrismaBuild never deleted one: `reclaim_origin` frees the
charge only after the producer has removed every file itself, and nothing
owned a handoff once its producer exited.  A batch committed with the
``consumed`` lifetime is now PrismaBuild's to retire:

*   each consumer that declares it is filed against it at submission
    (`declare_origin_consumer`, called by ``pbrun`` before the row exists);
*   `origin_retirement_tick`, once per tier-loop cycle, deletes the origin and
    frees the durable charge when every declared consumer has succeeded;
*   it sweeps a consumed batch that no consumer declared once its producer
    attempt is dead;
*   a ``retain`` batch is never touched.

The issue's acceptance, plus the coordinator's case: a consumer that failed
holds its batch (reported once as a stall), and after the resubmitted
consumer executes, the batch retires.

Fixture concessions: owners and consumers are published, claimed and finished
through the real ``PoolQueue``; a superseding retry is simulated by filing a
fresh broker control on the owner's claim (`fx._broker_control`), which is what
a new attempt's claim carries.
"""
from __future__ import annotations

import contextlib
import io
import json
import os
from pathlib import Path
import socket
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_prepaid_writer_integration as fx  # noqa: E402
from prismabuild import pool, produced_output as po, storage_tiers  # noqa: E402
import pbrun  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402
from test_pbrun_detach import _checkout  # noqa: E402
from test_write_only_produced_output import (  # noqa: E402
    KIND, TIER, _descriptor, _prewrite, _queue, _template,
)

_isolated_synthetic_launch_context = fx._isolated_synthetic_launch_context

WORKER = str(fx.REPO / "tools" / "prismabuild_worker.py")
CONSUMED = po.ORIGIN_LIFETIME_CONSUMED


def _bind_owner(queue: pool.PoolQueue, template: dict, owner: str) -> dict:
    """`fx._bind` with one attempt, so a failure is terminal, not a requeue."""

    queue.publish(action_key=owner, cas_root="/cas", worker_script=WORKER,
                  checkout_root=str(Path(queue.root).parent / "mover-checkout"),
                  resources={"cpu": 1, "mem_gb": 1}, max_attempts=1,
                  produced_output_template=template)
    claimed = queue.claim(owner="w-owner")
    assert claimed is not None and claimed["action_key"] == owner
    control = fx._broker_control(queue, owner)
    env = {"PRISMABUILD_ACTION_KEY": owner,
           "PRISMABUILD_ACTION_NONCE": control["nonce"],
           "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
    po.declare_template(queue.root, template)
    instance = po.bind_instance(queue, template, owner_action_key=owner,
                                claim_snapshot=claimed, env=env)
    po.declare_instance(queue.root, instance)
    assert po.admit_instance(queue, instance, template)["ok"] is True
    return instance


def _commit(queue, template, seed: str, *, lifetime: str = CONSUMED,
            batch_id: str = "b1", payload: bytes = b"band handoff bytes"):
    """One owner that wrote one origin file and committed it at origin."""

    instance = _bind_owner(queue, template, fx._hexkey(seed))
    path = Path(template["output_prefix"]) / f"{seed}-{batch_id}.bin"
    assert _prewrite(queue, instance, template, batch_id, [path],
                     len(payload))["ok"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    descriptor = _descriptor(instance, template, path, payload)
    committed = po.commit_origin_batch(queue, instance, template, [descriptor],
                                       batch_id=batch_id, lifetime=lifetime)
    assert committed["ok"], committed
    return instance, path, committed


def _publish_consumer(queue: pool.PoolQueue, key: str) -> None:
    queue.publish(action_key=key, cas_root="/cas", worker_script=WORKER,
                  checkout_root=str(Path(queue.root).parent / "consumer-checkout"),
                  resources={"cpu": 1, "mem_gb": 1}, max_attempts=1)


def _run_consumer(queue: pool.PoolQueue, key: str, status: str, *,
                  tags: tuple[str, ...] = ()) -> None:
    claimed = queue.claim(owner="w-consumer", tags=tags)
    assert claimed is not None and claimed["action_key"] == key
    queue.finish(key, status=status)


def _entry(queue, instance, batch_id: str = "b1") -> dict:
    return po._read_commitments(
        po._commitments_path(queue.root, instance))["batches"][batch_id]


def _charged(queue, instance) -> int:
    return po._class_sums(po._read_commitments(
        po._commitments_path(queue.root, instance))["batches"])["payload"]


# -- the consumed handoff ----------------------------------------------------


def test_a_consumed_handoff_is_retired_after_its_consumer_commits(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Declared through the real submission, retired by the tick."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    payload = b"band L handoff"
    instance, path, committed = _commit(queue, template, "handoff",
                                        payload=payload)
    assert committed["lifetime"] == CONSUMED
    record = json.loads((queue.root / "residency" / po.OUTPUT_BATCHES_SUBDIR
                         / po.instance_namespace(instance) / "b1.json"
                         ).read_text())
    assert record["lifetime"] == CONSUMED
    assert _entry(queue, instance)["lifetime"] == CONSUMED
    queue.finish(instance["owner_action_key"], status="executed")
    assert _charged(queue, instance) == len(payload)

    manifest = po.origin_batch_manifest(queue.root, [committed["ref"]])
    manifest_path = tmp_path / "consumer-manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    work = _checkout(tmp_path)
    queue.announce(host="sparky", tags=["sparky", "gb10"], has_gpu=True,
                   capacity={"cpu": 4, "mem_gb": 16, "gpu": 1})
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--cwd", str(work), "--wait-s", "0.01", "--detach",
        "--data-manifest", str(manifest_path),
        "--", "/bin/bash", "-lc", "true"])
    capsys.readouterr()
    assert pbrun.main() == 0
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    consumer = json.loads(lines[-1])["action_key"]
    declaration = (po._consumers_dir(queue.root, instance, "b1")
                   / f"{consumer}.json")
    assert json.loads(declaration.read_text()) == {
        "schema": po.ORIGIN_CONSUMER_SCHEMA_V1,
        "consumer_action_key": consumer, "ref": committed["ref"]}
    assert queue.item_path(pool.READY, consumer).exists()

    assert po.origin_retirement_tick(queue) == [], (
        "a queued consumer holds the batch, and holding it is not news")
    assert path.read_bytes() == payload

    identity = record["origin_identity"]
    _run_consumer(queue, consumer, "executed", tags=("sparky", "gb10"))
    events = po.origin_retirement_tick(queue)

    assert events == [{
        "event": po.ORIGIN_RETIRED_EVENT, "ref": committed["ref"],
        "bytes": len(payload), "reason": "consumed",
        "consumers": [{"action_key": consumer, "state": "succeeded"}],
        "origin_identity": identity, "unlinked": [str(path)],
        "superseded": [], "absent": []}]
    assert not path.exists(), "the origin is gone"
    assert _charged(queue, instance) == 0, "its durable charge is released"
    entry = _entry(queue, instance)
    assert entry["origin_reclaimed"] is True
    assert entry["retiring"] == {
        "reason": "consumed",
        "consumers": [{"action_key": consumer, "state": "succeeded"}]}
    with pytest.raises(po.ProducedOutputError, match="origin-batch-reclaimed"):
        po.origin_batch_manifest(queue.root, [committed["ref"]])
    with pytest.raises(po.ProducedOutputError, match="origin-batch-reclaimed"):
        po.declare_origin_consumer(queue, committed["ref"],
                                   consumer_action_key=fx._hexkey("late"))
    assert po.origin_retirement_tick(queue) == []


def test_a_failed_consumer_holds_the_batch_until_its_resubmission_executes(
        tmp_path: Path) -> None:
    """The coordinator's case: a retry needs the handoff, so it stays."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "band-l")
    queue.finish(instance["owner_action_key"], status="executed")
    consumer = fx._hexkey("band-l-minus-1")
    assert po.declare_origin_consumer(
        queue, committed["ref"], consumer_action_key=consumer) == {
            "ok": True, "declared": True}
    _publish_consumer(queue, consumer)
    _run_consumer(queue, consumer, "failed")
    assert queue.item_path(pool.FAILED, consumer).exists()

    stalled = po.origin_retirement_tick(queue)
    assert stalled == [{
        "event": po.ORIGIN_RETIREMENT_STALLED_EVENT, "ref": committed["ref"],
        "bytes": path.stat().st_size,
        "consumers": [{"action_key": consumer, "state": "failed"}]}]
    assert po.origin_retirement_tick(queue) == [], "once per change, not per cycle"
    assert path.exists() and not _entry(queue, instance).get("retiring")

    # The resubmission: same key, a new generation.  The failed record stays
    # where it was, and the newer generation answers for the key.
    assert po.declare_origin_consumer(
        queue, committed["ref"], consumer_action_key=consumer)["declared"]
    _publish_consumer(queue, consumer)
    assert queue.item_path(pool.FAILED, consumer).exists()
    assert po.origin_retirement_tick(queue) == []
    assert path.exists()
    _run_consumer(queue, consumer, "executed")
    assert queue.item_path(pool.FAILED, consumer).exists()

    events = po.origin_retirement_tick(queue)
    assert [event["event"] for event in events] == [po.ORIGIN_RETIRED_EVENT]
    assert events[0]["consumers"] == [{"action_key": consumer,
                                       "state": "succeeded"}]
    assert not path.exists() and _charged(queue, instance) == 0


def test_every_declared_consumer_must_succeed(tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "two-readers")
    first, second = fx._hexkey("reader-one"), fx._hexkey("reader-two")
    for key in (first, second):
        po.declare_origin_consumer(queue, committed["ref"],
                                   consumer_action_key=key)
    _publish_consumer(queue, first)
    _run_consumer(queue, first, "executed")

    # The second was declared and never published: a submitter that died
    # between the declaration and its row.  Held, and said so once.
    events = po.origin_retirement_tick(queue)
    assert events == [{
        "event": po.ORIGIN_RETIREMENT_STALLED_EVENT, "ref": committed["ref"],
        "bytes": path.stat().st_size,
        "consumers": [{"action_key": first, "state": "succeeded"},
                      {"action_key": second, "state": "unpublished"}]}]
    assert path.exists()
    _publish_consumer(queue, second)
    assert po.origin_retirement_tick(queue) == []
    assert "retirement_report" not in _entry(queue, instance), (
        "a stall that resolved is forgotten, so a new one reports again")
    _run_consumer(queue, second, "executed")
    assert [e["event"] for e in po.origin_retirement_tick(queue)] == [
        po.ORIGIN_RETIRED_EVENT]
    assert not path.exists()


def test_a_claim_in_transition_holds_and_a_widowed_lease_does_not(
        tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "in-transition")
    consumer = fx._hexkey("transition-reader")
    po.declare_origin_consumer(queue, committed["ref"],
                               consumer_action_key=consumer)
    _publish_consumer(queue, consumer)
    _run_consumer(queue, consumer, "executed")

    # A finisher that has moved a claim aside has not filed its ending yet.
    tombstone = queue.dir(pool.CLAIMED) / (
        f"{consumer}.1.sparky.1.0badf00d{pool.TOMBSTONE_SUFFIX}")
    tombstone.write_text("{}")
    assert po.origin_retirement_tick(queue) == []
    assert path.exists()

    # A lease with no claim is left over, not in flight: the ending answers.
    tombstone.unlink()
    queue.lease_path(consumer).write_text("{}")
    events = po.origin_retirement_tick(queue)
    assert [(e["event"], e["consumers"]) for e in events] == [
        (po.ORIGIN_RETIRED_EVENT,
         [{"action_key": consumer, "state": "succeeded"}])]
    assert not path.exists() and _charged(queue, instance) == 0


# -- the orphan sweep --------------------------------------------------------


def test_a_failed_attempts_undeclared_batch_is_swept(tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "failed-producer")
    owner = instance["owner_action_key"]

    assert po.origin_retirement_tick(queue) == [], "the live attempt keeps it"
    queue.finish(owner, status="failed")
    assert queue.item_path(pool.FAILED, owner).exists()

    events = po.origin_retirement_tick(queue)

    assert len(events) == 1 and events[0]["event"] == po.ORIGIN_RETIRED_EVENT
    assert events[0]["reason"] == "orphan" and events[0]["consumers"] == []
    assert events[0]["ref"] == committed["ref"]
    assert events[0]["unlinked"] == [str(path)]
    assert not path.exists() and _charged(queue, instance) == 0


def test_a_superseded_attempts_batch_is_swept_and_a_successful_ones_is_kept(
        tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    kept, kept_path, _ = _commit(queue, template, "succeeded-producer")
    queue.finish(kept["owner_action_key"], status="executed")
    swept, swept_path, _ = _commit(queue, template, "retried-producer")
    # A retry of the same owner now holds the claim under a new attempt.
    fx._broker_control(queue, swept["owner_action_key"])

    events = po.origin_retirement_tick(queue)

    assert [(e["event"], e["reason"]) for e in events] == [
        (po.ORIGIN_RETIRED_EVENT, "orphan")]
    assert events[0]["unlinked"] == [str(swept_path)]
    assert not swept_path.exists()
    assert kept_path.exists(), (
        "a producer that succeeded keeps its handoff for a consumer to come")
    assert _charged(queue, kept) == kept_path.stat().st_size


def test_a_batch_a_live_consumer_references_is_never_swept(tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "dead-producer")
    consumer = fx._hexkey("live-reader")
    po.declare_origin_consumer(queue, committed["ref"],
                               consumer_action_key=consumer)
    _publish_consumer(queue, consumer)
    queue.finish(instance["owner_action_key"], status="failed")

    assert po.origin_retirement_tick(queue) == []
    claimed = queue.claim(owner="w-consumer")
    assert claimed is not None and claimed["action_key"] == consumer
    assert po.origin_retirement_tick(queue) == []
    assert path.exists() and _charged(queue, instance) == path.stat().st_size
    entry = _entry(queue, instance)
    assert not entry.get("retiring") and not entry.get("origin_reclaimed")


def test_a_declaration_and_a_retirement_cannot_cross(tmp_path: Path) -> None:
    """Once the tick has decided, a new consumer is refused, never stranded."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, _path, committed = _commit(queue, template, "crossing")
    commitments_path = po._commitments_path(queue.root, instance)
    record = po._read_commitments(commitments_path)
    record["batches"]["b1"]["retiring"] = {"reason": "orphan", "consumers": []}
    po._write_commitments(commitments_path, {"batches": record["batches"]})

    with pytest.raises(po.ProducedOutputError, match="origin-batch-retiring"):
        po.declare_origin_consumer(queue, committed["ref"],
                                   consumer_action_key=fx._hexkey("too-late"))
    with pytest.raises(po.ProducedOutputError, match="origin-batch-retiring"):
        po.origin_batch_manifest(queue.root, [committed["ref"]])
    # A retiring batch resumes its delete even while its producer still runs.
    events = po.origin_retirement_tick(queue)
    assert [(e["event"], e["reason"]) for e in events] == [
        (po.ORIGIN_RETIRED_EVENT, "orphan")]


# -- identity ---------------------------------------------------------------


def test_an_origin_changed_in_place_is_refused_once_and_kept(
        tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "rewritten")
    queue.finish(instance["owner_action_key"], status="failed")
    with path.open("r+b") as handle:
        handle.write(b"X")

    events = po.origin_retirement_tick(queue)

    assert events == [{
        "event": po.ORIGIN_RETIREMENT_REFUSED_EVENT, "ref": committed["ref"],
        "bytes": path.stat().st_size, "reason": "origin-changed",
        "path": str(path)}]
    assert po.origin_retirement_tick(queue) == []
    assert path.exists(), "a file that is not the committed one is not deleted"
    entry = _entry(queue, instance)
    assert not entry.get("retiring") and not entry.get("origin_reclaimed")
    assert _charged(queue, instance) == path.stat().st_size


def test_a_path_a_retried_attempt_wrote_again_is_left_to_it(
        tmp_path: Path) -> None:
    """#912's per-attempt ownership: the earlier batch stops charging, the file stays."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, _committed = _commit(queue, template, "rewritten-by-retry")
    queue.finish(instance["owner_action_key"], status="failed")
    replacement = path.with_name(path.name + ".next")
    replacement.write_bytes(b"the retry's own bytes")
    os.replace(replacement, path)

    events = po.origin_retirement_tick(queue)

    assert len(events) == 1 and events[0]["event"] == po.ORIGIN_RETIRED_EVENT
    assert events[0]["unlinked"] == [] and events[0]["superseded"] == [str(path)]
    assert path.read_bytes() == b"the retry's own bytes"
    assert _charged(queue, instance) == 0


def test_an_unreachable_output_prefix_is_never_read_as_gone(
        tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "unmounted")
    queue.finish(instance["owner_action_key"], status="failed")
    prefix = Path(template["output_prefix"])
    parked = tmp_path / "parked"
    prefix.rename(parked)

    events = po.origin_retirement_tick(queue)

    assert [(e["event"], e["reason"]) for e in events] == [
        (po.ORIGIN_RETIREMENT_REFUSED_EVENT, "output-prefix-unreachable")]
    assert po.origin_retirement_tick(queue) == []
    entry = _entry(queue, instance)
    assert not entry.get("origin_reclaimed") and not entry.get("retiring")
    parked.rename(prefix)
    assert [e["event"] for e in po.origin_retirement_tick(queue)] == [
        po.ORIGIN_RETIRED_EVENT]
    assert not path.exists()


# -- the tier loop -----------------------------------------------------------


def _cycle_lines(queue: pool.PoolQueue, stage: Path) -> list[dict]:
    """One whole `tier_loop.cycle`, returning the JSON lines it printed."""

    queue.mint_tier_capacity(TIER, {KIND: 8})
    stage.mkdir(parents=True, exist_ok=True)
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        tier_loop.cycle(queue, host="sparky", source_pool="storage_pool",
                        receipts=tier_loop.ReceiptCache(),
                        discover=lambda **_kwargs: {
                            TIER: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                                   "tier_id": TIER, "host": "sparky",
                                   "tier": "stage", "mountpoint": str(stage),
                                   "capacity_bytes": 8 * storage_tiers.GIB}})
    return [json.loads(line) for line in out.getvalue().splitlines()
            if line.startswith("{")]


def test_the_tier_cycle_logs_one_line_per_retired_batch(tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "cycle")
    origin_events = {po.ORIGIN_RETIRED_EVENT, po.ORIGIN_RETIREMENT_STALLED_EVENT,
                     po.ORIGIN_RETIREMENT_REFUSED_EVENT}

    quiet = _cycle_lines(queue, tmp_path / "stage")
    assert [line for line in quiet if line.get("event") in origin_events] == []

    queue.finish(instance["owner_action_key"], status="failed")
    lines = [line for line in _cycle_lines(queue, tmp_path / "stage")
             if line.get("event") in origin_events]

    assert len(lines) == 1
    assert lines[0]["event"] == po.ORIGIN_RETIRED_EVENT
    assert isinstance(lines[0]["unix"], float)
    assert lines[0]["ref"] == committed["ref"]
    assert lines[0]["bytes"] == len(b"band handoff bytes")
    assert set(lines[0]["origin_identity"]) == {str(path)}
    assert not path.exists()
    again = [line for line in _cycle_lines(queue, tmp_path / "stage")
             if line.get("event") in origin_events]
    assert again == []


# -- off by default ----------------------------------------------------------


def test_a_retained_batch_survives_and_is_filed_as_912_filed_it(
        tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "retained",
                                        lifetime=po.ORIGIN_LIFETIME_RETAIN)
    assert "lifetime" not in committed
    record = json.loads((queue.root / "residency" / po.OUTPUT_BATCHES_SUBDIR
                         / po.instance_namespace(instance) / "b1.json"
                         ).read_text())
    assert "lifetime" not in record
    assert set(_entry(queue, instance)) == {
        "manifest_digest", "batch_namespace", "tier", "mover_key",
        "origin_only", "class_bytes", "paths", "retired", "origin_reclaimed"}
    consumer = fx._hexkey("retained-reader")
    assert po.declare_origin_consumer(
        queue, committed["ref"], consumer_action_key=consumer) == {
            "ok": True, "declared": False}
    assert not po._consumers_dir(queue.root, instance, "b1").exists()
    queue.finish(instance["owner_action_key"], status="failed")
    commitments = po._commitments_path(queue.root, instance)
    before = (commitments.read_bytes(), commitments.stat().st_mtime_ns)

    assert po.origin_retirement_tick(queue) == []

    assert path.exists(), "PB never deletes a retained batch, failed producer or not"
    assert (commitments.read_bytes(), commitments.stat().st_mtime_ns) == before
    assert _charged(queue, instance) == path.stat().st_size


def test_a_replay_with_another_lifetime_refuses(tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "replayed")
    descriptor = committed["entries"][0]
    assert po.commit_origin_batch(
        queue, instance, template, [descriptor], batch_id="b1",
        lifetime=CONSUMED)["duplicate"] is True
    assert po.commit_origin_batch(
        queue, instance, template, [descriptor], batch_id="b1",
    )["refusal"] == "batch-lifetime-mismatch"
    with pytest.raises(po.ProducedOutputError, match="lifetime"):
        po.commit_origin_batch(queue, instance, template, [descriptor],
                               batch_id="b1", lifetime="forever")


def test_a_submission_without_declared_batches_files_nothing(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected(*_args, **_kwargs):
        raise AssertionError("a consumer was declared for an ordinary submission")

    monkeypatch.setattr(po, "declare_origin_consumer", unexpected)
    manifest = {
        "schema": "prismaquant.prismabuild.data_manifest.v1", "produced_by": {},
        "annotations": {"phases": [{"name": "p", "bytes": 4,
                                    "cumulative_bytes": 4}]},
        "mount_prefix": str(tmp_path), "entries": [
            {"path": str(tmp_path / "input.bin"), "offset": 0, "bytes": 4,
             "sha256": None}],
        "entry_count": 1, "total_bytes": 4}
    (tmp_path / "input.bin").write_bytes(b"four")
    manifest_path = tmp_path / "plain-manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    work = _checkout(tmp_path)
    queue = _queue(tmp_path)
    queue.announce(host="sparky", tags=["sparky", "gb10"], has_gpu=True,
                   capacity={"cpu": 4, "mem_gb": 16, "gpu": 1})
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--cwd", str(work), "--wait-s", "0.01", "--detach",
        "--data-manifest", str(manifest_path),
        "--", "/bin/bash", "-lc", "true"])
    prepared: list[dict] = []
    original = pbrun.prepare_submission

    def spy(args):
        result = original(args)
        prepared.append(result)
        return result

    monkeypatch.setattr(pbrun, "prepare_submission", spy)
    assert pbrun.main() == 0
    assert len(prepared) == 1
    assert "produced_output_batches" not in prepared[0]["template"], (
        "the frozen template is the one it always was")
    assert not (queue.root / "residency" / po.OUTPUT_SCOPES_SUBDIR).exists()

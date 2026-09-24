"""A write-only produced output commits at its origin, and a later action stages it (#912).

A produced-output template used to describe one shape of work: an action that
writes outputs and reads them again itself, so every batch was staged through
a mover under a window the producer reserved at admission.  A producer whose
outputs only a later action reads paid for that window and that copy anyway.
A ``write_only`` template reserves no window, and its batches commit at their
origin (`produced_output.commit_origin_batch`); the later action declares
them in its data manifest (`origin_batch_manifest`) and stages them through
the ordinary input path.

The issue's acceptance, each driven through the real path:

*   The producer's admission charges no stage window: it publishes, claims
    and admits with no tier demand, and the tier ledger does not move.
*   A write-only producer commits a batch and exits with no retained
    prewrite: the batch goes through the spool's real export action, commits
    against the identities the export receipt recorded, and the owner
    finishes with its prewrite consumed.
*   A second action declares that batch, stages it and reads it: ``pbrun
    --data-manifest --residency stage`` seals the consumer, the tier cycle
    publishes its lead mover, the mover's own sealed command copies the
    origin, and the composed residency map resolves to the committed bytes.

Off by default: a template without ``write_only`` hashes exactly as it did
before #912 (the literal below was computed on main at b8e5f007ae3c), and a
data manifest that declares no batches is never checked against the queue.

Fixture concessions: the mover runs in-process through ``stage_move.main`` on
its sealed argv with ``--unpaced`` added (pacing needs a live ZFS pool's
member devices); the export runs through ``PoolQueue.execute`` as in
``test_produced_spool``.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import socket
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_prepaid_writer_integration as fx  # noqa: E402
from prismabuild import core, pool, produced_output as po  # noqa: E402
from prismabuild import produced_spool as ps, reader_lease  # noqa: E402
from prismabuild import residency_map as rm, residency_plan  # noqa: E402
import pbrun  # noqa: E402
import stage_move  # noqa: E402
from test_pbrun_detach import _checkout  # noqa: E402
from test_pbrun_residency_stage_submission import (  # noqa: E402
    TIER, _announce_tier, _detach_key, _tier_cycle,
)

_isolated_synthetic_launch_context = fx._isolated_synthetic_launch_context

KIND = "stage_gib"
#: `template_sha256` of `_read_back_body()` on main before #912 (b8e5f007ae3c).
READ_BACK_SHA256_BEFORE_912 = (
    "39fb93cc366eeb5d1201743a2a6ca45a8e5324247ece5adabd7229493492523d")


def _template(prefix: Path) -> dict:
    """A write-only template: every tier's minimum and window are zero."""

    return po.validate_template({
        "schema": po.TEMPLATE_SCHEMA_V1,
        "version": 1,
        "template_id": "write-only-integ-v1",
        "output_prefix": str(prefix),
        "slots": {"s0": {"class": "payload"}, "s1": {"class": "checkpoint"}},
        "durable_maxima": {"payload_max_bytes": 1 << 20,
                           "checkpoint_max_bytes": 1 << 20,
                           "temp_max_bytes": 1 << 20},
        "working_demands": {TIER: {"minimum_gib": 0, "window_gib": 0}},
        "permitted_tiers": [TIER],
        "write_only": True,
    })


def _read_back_body() -> dict:
    return {
        "schema": po.TEMPLATE_SCHEMA_V1, "version": 1,
        "template_id": "pinned-v1", "output_prefix": "/srv/pb-outputs",
        "slots": {"s0": {"class": "payload"}},
        "durable_maxima": {"payload_max_bytes": 1 << 20,
                           "checkpoint_max_bytes": 1 << 20,
                           "temp_max_bytes": 1 << 20},
        "working_demands": {"prismabuild-stage:dl380g10":
                            {"minimum_gib": 1, "window_gib": 2}},
        "permitted_tiers": ["prismabuild-stage:dl380g10"],
    }


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {KIND: 4})
    return queue


def _descriptor(instance: dict, template: dict, path: Path, payload: bytes,
                *, digest: bool = True) -> dict:
    return po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "s0",
        "artifact_class": "payload", "path": str(path),
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest() if digest else None,
        "producer_generation": po.mint_generation(),
        "owner_action_key": instance["owner_action_key"],
        "owner_attempt": dict(instance["owner_attempt"]),
    }, template, instance)


def _prewrite(queue, instance, template, batch_id: str, paths: list[Path],
              size: int) -> dict:
    return po.require_prewrite(
        queue, instance, template, batch_id=batch_id, tier=TIER,
        class_bytes={"payload": size, "checkpoint": 0, "temp": 0},
        paths=[str(path) for path in paths])


def _direct_batch(queue, template, seed: str, *, batch_id: str = "b1",
                  payload: bytes = b"direct origin bytes",
                  name: str | None = None):
    """Bind one write-only owner and commit one batch it wrote directly."""

    instance = fx._bind(queue, template, fx._hexkey(seed))
    path = Path(template["output_prefix"]) / (name or f"{batch_id}.bin")
    pre = _prewrite(queue, instance, template, batch_id, [path], len(payload))
    assert pre["ok"], pre
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_bytes(payload)
    descriptor = _descriptor(instance, template, path, path.read_bytes())
    committed = po.commit_origin_batch(queue, instance, template, [descriptor],
                                       batch_id=batch_id)
    assert committed["ok"], committed
    return instance, path, descriptor, committed


def _spool_producer(tmp_path: Path) -> ps.ProducedSpool:
    """`test_produced_spool.world`, with a write-only template and no tier."""

    cas_root = tmp_path / "cas"
    template = _template(tmp_path / "canonical")
    initial = fx._producer_request(tmp_path, cas_root, template)
    cas, request = po._read_producer_request(cas_root, initial)
    request.pop("action_key")
    request["environment"]["variables"].update({
        ps.ROOT_ENV: str(tmp_path / "local"), ps.MAX_ENV: "256"})
    action = core.seal_action(request)
    cas.publish_action_request(action)
    queue = _queue(tmp_path)
    instance = fx._bind(queue, template, action["action_key"], cas_root)
    return ps.ProducedSpool(queue, instance, template, cas_root=cas_root,
                            root=tmp_path / "local", max_bytes=256)


def _exported(spool: ps.ProducedSpool, payload: bytes) -> tuple[Path, dict]:
    """Write one group locally, export it through the real action, release it.

    Returns the canonical destination and the descriptor that names it; the
    group is acknowledged but not committed.
    """

    destination = Path(spool.template["output_prefix"]) / "b1.bin"
    pre = _prewrite(spool.queue, spool.instance, spool.template, "b1",
                    [destination, Path(str(destination) + ".tmp")], 64)
    assert pre["ok"], pre
    directory = spool.reserve_group("b1", 64)
    source = directory / "b1.bin"
    with source.open("x+b") as handle:
        os.posix_fallocate(handle.fileno(), 0, 64)
        handle.write(payload)
        handle.truncate(len(payload))
        handle.flush()
        os.fsync(handle.fileno())
    handle = spool.submit_group("b1", [{
        "source_path": str(source), "destination_path": str(destination),
        "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}])
    assert handle["ok"], handle
    descriptor = _descriptor(spool.instance, spool.template, destination, payload)
    early = spool.commit_origin_group("b1", [descriptor])
    assert early["ok"] is False and early["refusal"] == "export-incomplete-retain", (
        "nothing commits before the export's receipt is durable")
    claimed = spool.queue.claim(owner="spool-export-test", tags=[spool.host])
    assert claimed is not None and claimed["action_key"] == handle["export_key"]
    assert claimed["resources"] == {"cpu": 1, "mem_gb": 1}
    outcome = spool.queue.execute(claimed, timeout_s=120)
    assert outcome.get("returncode") == 0, outcome
    spool.queue.finish(handle["export_key"], status="executed")
    assert spool.poll_group("b1")["complete"]
    assert spool.release_group("b1")["ok"]
    return destination, descriptor


def _queued_rows(queue: pool.PoolQueue) -> set[str]:
    return {path.stem for state in (pool.READY, pool.CLAIMED)
            for path in queue.dir(state).glob("*.json")}


# -- admission ---------------------------------------------------------------


def test_a_write_only_template_declares_no_window_and_refuses_one(
        tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    assert po.is_write_only(template)
    assert template["working_demands"] == {
        TIER: {"minimum_gib": 0, "window_gib": 0}}
    assert po.owner_demand_terms(template) == {}
    body = {key: value for key, value in template.items()}
    body["working_demands"] = {TIER: {"minimum_gib": 0, "window_gib": 1}}
    with pytest.raises(po.ProducedOutputError, match="write-only template"):
        po.validate_template(body)
    body["working_demands"] = {TIER: {"minimum_gib": 0, "window_gib": 0}}
    body["write_only"] = "yes"
    with pytest.raises(po.ProducedOutputError):
        po.validate_template(body)


def test_the_producer_admission_charges_no_stage_window(tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    ledger = queue.tier_ledger(TIER)
    before = (ledger.capacity(), ledger.available())
    owner = fx._hexkey("wo-admission")
    instance = fx._bind(queue, template, owner)
    claimed = pool._read_json(queue.item_path(pool.CLAIMED, owner))
    assert claimed["resources"] == {"cpu": 1, "mem_gb": 1}
    assert (ledger.capacity(), ledger.available()) == before
    assert ledger.available() == ledger.capacity()
    assert ledger.holder_tokens(owner) == {}
    assert po.admit_funded_window(
        queue, instance, template, need_gib_per_tier={TIER: 1},
    )["refusal"] == "template-is-write-only"
    # A write-only template's producer that declares a window anyway is
    # refused at publish, the same rule every template's demand is held to.
    with pytest.raises(pool.PoolContractError, match="working window"):
        queue.publish(action_key=fx._hexkey("wo-windowed"), cas_root="/cas",
                      worker_script=str(fx.REPO / "tools" / "prismabuild_worker.py"),
                      checkout_root=str(tmp_path / "mover-checkout"),
                      resources={"cpu": 1, "mem_gb": 1, f"{KIND}@{TIER}": 1},
                      produced_output_template=template)


# -- the producer ------------------------------------------------------------


def test_the_write_only_producer_commits_at_its_origin_and_keeps_no_prewrite(
        tmp_path: Path) -> None:
    spool = _spool_producer(tmp_path)
    queue = spool.queue
    ledger = queue.tier_ledger(TIER)
    before = ledger.available()
    payload = b"write-only batch bytes"
    destination, descriptor = _exported(spool, payload)

    committed = spool.commit_origin_group("b1", [descriptor])

    assert committed["ok"] is True, committed
    assert committed["origin_only"] is True and committed["tier"] == TIER
    assert destination.read_bytes() == payload
    prewrites = po._prewrites_dir(queue.root, spool.instance)
    assert not list(prewrites.glob("*.prewrite.json")), (
        "the commit consumes the prewrite")
    record = json.loads((queue.root / "residency" / po.OUTPUT_BATCHES_SUBDIR
                         / po.instance_namespace(spool.instance)
                         / "b1.json").read_text())
    assert record["origin_only"] is True and record["mover_key"] is None
    assert ledger.available() == before, "no stage token moved"
    assert _queued_rows(queue) == {spool.owner}, "no mover was published"
    assert committed["ref"] == po.origin_batch_ref(
        spool.instance, batch_id="b1",
        manifest_digest=committed["manifest_digest"])

    again = spool.commit_origin_group("b1", [descriptor])
    assert again["ok"] is True and again["duplicate"] is True
    assert again["ref"] == committed["ref"]

    queue.finish(spool.owner, status="executed")
    assert _queued_rows(queue) == set()
    assert not list(prewrites.glob("*.prewrite.json"))


def test_the_staged_paths_refuse_a_write_only_template_and_the_origin_checks_bite(
        tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance = fx._bind(queue, template, fx._hexkey("wo-refusals"))
    path = Path(template["output_prefix"]) / "b1.bin"
    payload = b"x" * 32
    assert _prewrite(queue, instance, template, "b1", [path], 32)["ok"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    descriptor = _descriptor(instance, template, path, payload)

    assert po.commit_batch(queue, instance, template, [descriptor],
                           batch_id="b1", tier=TIER, mover_key="a" * 64
                           )["refusal"] == "template-is-write-only"
    assert po.publish_prepaid_batch(queue, instance, template, [descriptor],
                                    batch_id="b1", tier=TIER,
                                    cas_root=tmp_path / "cas",
                                    )["refusal"] == "template-is-write-only"
    assert po.refill_window(queue, instance, template, tier=TIER
                            )["refusal"] == "template-is-write-only"

    unhashed = _descriptor(instance, template, path, payload, digest=False)
    assert po.commit_origin_batch(queue, instance, template, [unhashed],
                                  batch_id="b1"
                                  )["refusal"] == "origin-batch-needs-sha256"
    identity = reader_lease.portable_identity(os.lstat(path))
    wrong = {str(path): dict(identity, ino=identity["ino"] + 1)}
    assert po.commit_origin_batch(queue, instance, template, [descriptor],
                                  batch_id="b1", landed=wrong
                                  )["refusal"] == "origin-is-not-the-landed-copy"
    right = {str(path): identity}
    assert po.commit_origin_batch(queue, instance, template, [descriptor],
                                  batch_id="b1", landed=right)["ok"]
    assert po.ensure_batch_materialized(
        queue, instance, template, batch_id="b1", cas_root=tmp_path / "cas",
    )["refusal"] == "template-is-write-only"

    # A read-back template's batches are staged, never committed at origin.
    other = tmp_path / "read-back"
    read_back = fx._template(str(other / "outputs"))
    other_queue = fx._queue(other)
    other_instance = fx._bind(other_queue, read_back, fx._hexkey("rb-owner"))
    assert po.commit_origin_batch(other_queue, other_instance, read_back, [],
                                  batch_id="b1"
                                  )["refusal"] == "template-reads-back"


def test_an_origin_only_batch_reads_as_never_staged_and_owns_its_path_until_reclaimed(
        tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, _descriptor_, committed = _direct_batch(
        queue, template, "wo-readers")

    assert po.retire_batch(queue, instance, template, "b1",
                           stage_root=str(tmp_path / "stage"),
                           residency_root=queue.root / pool.RESIDENCY) == {
        "ok": True, "batch_id": "b1", "origin_only": True, "staged": False}
    state = po.materialization_state(queue, instance, template, batch_id="b1")
    assert state["ok"] is True and state["stage_retired"] is True
    assert state["mover_key"] == "" and state["funding_state"] == "origin-only"
    events = po.recover_batches(queue, instance, template)
    assert [event["event"] for event in events] == ["output-batch-origin-only"]
    released = po.safe_release_instance(queue, instance, template,
                                        lease_sdk=reader_lease)
    assert released == {"ok": False, "refusal": "owner-active-retain"}, (
        "an origin-only batch holds no copy or mover, so only the live owner "
        "retains")

    # The origin stays owned: a consumer may have declared it.
    again = _prewrite(queue, instance, template, "b2", [path], 32)
    assert again["refusal"] == "prewrite-path-owned-by-live-batch"
    assert po.reclaim_origin(queue, instance, template, batch_id="b1"
                             )["refusal"] == "origin-present-retain"
    path.unlink()
    assert po.reclaim_origin(queue, instance, template, batch_id="b1")["ok"]
    assert _prewrite(queue, instance, template, "b2", [path], 32)["ok"]
    with pytest.raises(po.ProducedOutputError, match="origin-batch-reclaimed"):
        po.origin_batch_manifest(queue.root, [committed["ref"]])


# -- the consumer ------------------------------------------------------------


def test_a_later_action_declares_the_batch_stages_it_and_reads_it(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    spool = _spool_producer(tmp_path)
    queue = spool.queue
    payload = b"bytes a later action reads"
    destination, descriptor = _exported(spool, payload)
    committed = spool.commit_origin_group("b1", [descriptor])
    assert committed["ok"], committed
    queue.finish(spool.owner, status="executed")

    manifest = po.origin_batch_manifest(queue.root, [committed["ref"]])
    assert manifest["annotations"][po.ORIGIN_BATCHES_ANNOTATION] == [
        committed["ref"]]
    manifest_path = tmp_path / "consumer-manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    work = _checkout(tmp_path)
    queue.announce(host="sparky", tags=["sparky", "gb10"], has_gpu=True,
                   capacity={"cpu": 4, "mem_gb": 16, "gpu": 1})
    _announce_tier(queue, mountpoint=tmp_path / "stage")
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--cwd", str(work), "--wait-s", "0.01", "--detach",
        "--data-manifest", str(manifest_path), "--residency", "stage",
        "--", "/bin/bash", "-lc", "true"])
    capsys.readouterr()
    assert pbrun.main() == 0
    consumer = _detach_key(capsys)

    _tier_cycle(queue, tmp_path / "stage")
    plan = residency_plan.read(queue, consumer)
    assert plan is not None and len(plan["phases"]) == 1
    lead = str(plan["phases"][0]["mover_row"]["action_key"])
    assert queue.item_path(pool.READY, lead).exists(), "the cycle published the lead"
    request = json.loads((tmp_path / "cas" / "requests" / lead[:2]
                          / f"{lead}.json").read_text())
    command = request["params"]["command"]
    assert Path(command[1]).name == "stage_move.py", command
    # The mover's own entry point on its sealed argv; the worker launcher
    # would supply the action key through the environment instead.
    assert stage_move.main([*command[2:], "--action-key", lead, "--unpaced"]) == 0
    receipt = queue.move_record(lead)
    assert receipt["complete"] is True and receipt["errors"] == [], receipt

    composed = rm.compose(rm.read_fragments(queue.root / pool.RESIDENCY, consumer))
    staged = composed["entries"][rm.residency_map_key(str(destination), 0)]
    assert Path(staged["stage_path"]).read_bytes() == payload
    assert staged["sha256"] == hashlib.sha256(payload).hexdigest()


def test_submission_refuses_a_manifest_its_declared_batches_do_not_describe(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    _instance, path, _desc, committed = _direct_batch(queue, template, "wo-tamper")
    ref = committed["ref"]
    manifest = po.origin_batch_manifest(queue.root, [ref])
    check = pbrun.require_declared_origin_batches
    assert check(manifest, transport="pool", queue_root=queue.root) is None

    def tampered(**changes):
        body = json.loads(json.dumps(manifest))
        body.update(changes)
        return body

    entries = json.loads(json.dumps(manifest["entries"]))
    entries[0]["sha256"] = "0" * 64
    with pytest.raises(SystemExit, match="entries is not what"):
        check(tampered(entries=entries), transport="pool", queue_root=queue.root)
    with pytest.raises(SystemExit, match="mount_prefix is not what"):
        check(tampered(mount_prefix=str(tmp_path)), transport="pool",
              queue_root=queue.root)
    annotations = dict(manifest["annotations"])
    annotations[po.ORIGIN_BATCHES_ANNOTATION] = [dict(ref, manifest_digest="1" * 64)]
    with pytest.raises(SystemExit, match="origin-batch-mismatch"):
        check(tampered(annotations=annotations), transport="pool",
              queue_root=queue.root)
    annotations[po.ORIGIN_BATCHES_ANNOTATION] = [ref, ref]
    with pytest.raises(SystemExit, match="names a batch twice"):
        check(tampered(annotations=annotations), transport="pool",
              queue_root=queue.root)
    with pytest.raises(SystemExit, match="pull queue"):
        check(manifest, transport="slurm", queue_root=queue.root)

    # Through the real submission: freeze reads the declared batches.
    manifest_path = tmp_path / "tampered-manifest.json"
    manifest_path.write_text(json.dumps(tampered(entries=entries)))
    work = _checkout(tmp_path)
    queue.announce(host="sparky", tags=["sparky", "gb10"], has_gpu=True,
                   capacity={"cpu": 4, "mem_gb": 16, "gpu": 1})
    _announce_tier(queue, mountpoint=tmp_path / "stage")
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--cwd", str(work), "--wait-s", "0.01", "--detach",
        "--data-manifest", str(manifest_path), "--residency", "stage",
        "--", "/bin/bash", "-lc", "true"])
    with pytest.raises(SystemExit, match=po.ORIGIN_BATCHES_ANNOTATION):
        pbrun.main()

    # The origin rewritten after its commit, same length: the batch no longer
    # names the file that is there.
    path.write_bytes(b"X" * len(path.read_bytes()))
    with pytest.raises(SystemExit, match="origin-batch-changed"):
        check(manifest, transport="pool", queue_root=queue.root)


def test_two_batches_over_one_origin_path_cannot_be_declared_together(
        tmp_path: Path) -> None:
    """Two actions' batches may name one path; the manifest refuses the pair.

    A live action's committed path refuses another action's prewrite
    (#1053), so the second commits only once the first has ended: a
    relaunch writing its predecessor's paths again.
    """

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    i1, path, _d1, first = _direct_batch(queue, template, "wo-first")
    queue.finish(i1["owner_action_key"], status="executed")
    _i2, same, _d2, second = _direct_batch(queue, template, "wo-second",
                                           name=path.name)
    assert same == path
    with pytest.raises(po.ProducedOutputError, match="twice"):
        po.origin_batch_manifest(queue.root, [first["ref"], second["ref"]])


# -- off by default ----------------------------------------------------------


def test_a_template_without_write_only_is_the_template_it_was() -> None:
    validated = po.validate_template(_read_back_body())
    assert "write_only" not in validated
    assert not po.is_write_only(validated)
    assert po.template_sha256(validated) == READ_BACK_SHA256_BEFORE_912
    explicit = po.validate_template(dict(_read_back_body(), write_only=False))
    assert po.template_sha256(explicit) == READ_BACK_SHA256_BEFORE_912
    assert po.owner_demand_terms(validated) == {
        "stage_gib@prismabuild-stage:dl380g10": 2}


def test_a_manifest_that_declares_no_batches_is_never_checked_against_the_queue(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected(*_args, **_kwargs):
        raise AssertionError("a manifest without declared batches was checked")

    monkeypatch.setattr(po, "origin_batch_manifest", unexpected)
    for manifest in ({}, {"annotations": None},
                     {"annotations": {"phases": [{"name": "p", "bytes": 1,
                                                  "cumulative_bytes": 1}]}}):
        assert pbrun.require_declared_origin_batches(
            manifest, transport="slurm", queue_root=tmp_path / "absent") is None

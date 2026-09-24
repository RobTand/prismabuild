"""A failed producer's exported files do not block its successor (#1097).

Live shape, 2026-09-24: Stage A round-2 q0 (``85c3c57fdf75``) failed after
exporting its layer-39 group and part of layer 38.  It left 320 canonical
files under ``entries/``.  A resubmit, under the same key or a new one,
exports exactly those paths at its first layer, and ``_export_entry``
refused there: the destination existed, the new group had no copy proof for
it, and it raised ``unowned or changed canonical destination``.  The only
remedy PB named was removing the files by hand.

The successor's prewrite was already granted: a dead attempt owns nothing a
new write can disturb (#1053).  So the export decides, per file, from PB's
own ownership records (the #1053 attempt index and each attempt's
commitments and prewrites):

* a file only an ended attempt's outstanding prewrite names is that failed
  group's.  When its bytes are the successor's own (size and sha256), the
  successor adopts it without a copy.  Otherwise it is retired by the
  rename-aside delete with the inode check (#1053) and the successor copies;
* a file a commit names is adopted when its bytes are the successor's, and
  never deleted;
* a file no attempt names, or one a live attempt names, or one whose failed
  group's export may still be running, still refuses.

Everything runs on a synthetic origin prefix, spool and queue under
``tmp_path``.  Nothing reads or writes a real stage, origin or queue.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import test_prepaid_writer_integration as fx
from test_produced_spool import claim_export, prepare, world
from prismabuild import core, pool, produced_output as po, produced_spool as ps


# -- the two attempts -----------------------------------------------------------


def _export(spool, batch):
    group = spool._group(batch)
    record = ps._read(group / "export.json")
    return ps.export_group(spool.queue, group / "manifest.json",
                           record["manifest_sha256"], record["export_key"])


def _try_export(spool, batch):
    """The export's answer, or its refusal as an answer: the assertion is ours."""

    try:
        return _export(spool, batch)
    except ps.SpoolError as exc:
        return {"ok": False, "refusal": str(exc)}


def _landed(spool, batch, payload, *, finish=True):
    """Prewrite, write, submit and export one group of one file."""

    _source, destination, entries = prepare(spool, batch=batch, payload=payload)
    handle = spool.submit_group(batch, entries)
    claim_export(spool, handle)
    assert _export(spool, batch)["ok"]
    if finish:
        spool.queue.finish(handle["export_key"], status="executed")
        assert spool.poll_group(batch)["complete"]
    assert destination.read_bytes() == payload
    return destination, handle


def _retry(spool):
    """The same key's next attempt: a new nonce on its claim, a new instance.

    A resubmit under the same key.  The first attempt then reads ``dead``:
    the owner's claim names another attempt.
    """

    q = spool.queue
    control = fx._broker_control(q, spool.owner)
    claimed = pool._read_json(q.item_path(pool.CLAIMED, spool.owner))
    instance = po.bind_instance(q, spool.template, owner_action_key=spool.owner,
        claim_snapshot=claimed, env={"PRISMABUILD_ACTION_KEY": spool.owner,
                                     "PRISMABUILD_ACTION_NONCE": control["nonce"],
                                     "PRISMABUILD_ACTION_SCOPE": control["scope_id"]})
    po.declare_instance(q.root, instance)
    assert po.admit_instance(q, instance, spool.template)["ok"]
    assert po._producer_attempt_state(q, spool.instance) == "dead"
    return ps.ProducedSpool(q, instance, spool.template, cas_root=spool.cas_root,
                            root=spool.root, max_bytes=spool.max_bytes)


def _new_key(spool, label):
    """Another action key on the same template, spool root and queue.

    The first producer fails first: its latest generation is ``failed``.
    """

    q = spool.queue
    q.withdraw(spool.owner, reason="test: the producer failed")
    q.finish(spool.owner, status="failed")
    assert po._producer_attempt_state(q, spool.instance) == "dead"
    cas, request = po._read_producer_request(spool.cas_root, spool.owner)
    request.pop("action_key")
    request["environment"]["variables"]["PB_TEST_PRODUCER"] = label
    action = core.seal_action(request)
    cas.publish_action_request(action)
    owner = action["action_key"]
    assert owner != spool.owner
    instance = fx._bind(q, spool.template, owner, spool.cas_root)
    return ps.ProducedSpool(q, instance, spool.template, cas_root=spool.cas_root,
                            root=spool.root, max_bytes=spool.max_bytes)


def _fail_export(spool, key):
    """Fail an export row until no attempt of it is left to run."""

    while po._mover_live_state(spool.queue, key) == pool.READY:
        claimed = spool.queue.claim(owner="export-fails", tags=[spool.host])
        assert claimed["action_key"] == key
        spool.queue.finish(key, status="failed")
    assert po._mover_live_state(spool.queue, key) == pool.FAILED


def _proof(spool, batch, index=0):
    return ps._read(spool._group(batch) / f"copy-{index}.json")


# -- a failed group's file: adopted or retired, never refused -------------------


def test_a_retry_adopts_its_dead_attempts_identical_export(tmp_path):
    """The q0 resubmit under the same key: the same bytes, no copy."""

    first = world(tmp_path)
    payload = b"cotangent-0-0-at-39, exported before the attempt died"
    destination, _ = _landed(first, "b39", payload)
    inode = destination.stat().st_ino
    second = _retry(first)
    _source, again, entries = prepare(second, batch="b39", payload=payload)
    assert again == destination
    handle = second.submit_group("b39", entries)
    claim_export(second, handle)

    answer = _try_export(second, "b39")

    assert answer["ok"], (
        f"a retry cannot export over its dead attempt's identical file: {answer}")
    assert destination.stat().st_ino == inode, "adoption copies nothing"
    assert destination.read_bytes() == payload
    proof = _proof(second, "b39")
    assert proof["complete"] is True and proof["identity"]["ino"] == inode
    assert proof["adopted"]["owner_nonce"] == first.instance["owner_attempt"]["nonce"]
    assert proof["adopted"]["owner_kind"] == "prewrite"
    second.queue.finish(handle["export_key"], status="executed")
    assert second.poll_group("b39")["complete"]
    assert second.release_group("b39")["ok"]


def test_a_new_key_retires_a_failed_groups_other_bytes_and_copies(tmp_path):
    """The q0 resubmit under a new key, whose bytes differ: retired, then copied."""

    first = world(tmp_path)
    destination, _ = _landed(first, "b39", b"the dead attempt's bytes")
    # Pin the dead inode with a second name, so a new file cannot reuse its
    # number and read as the old one.
    pin = tmp_path / "dead-inode-pin"
    pin.hardlink_to(destination)
    second = _new_key(first, "resubmit")
    payload = b"the successor's own, different bytes"
    _source, _destination, entries = prepare(second, batch="b39", payload=payload)
    handle = second.submit_group("b39", entries)
    claim_export(second, handle)

    answer = _try_export(second, "b39")

    assert answer["ok"], (
        f"a new key cannot export over a failed group's file: {answer}")
    assert destination.read_bytes() == payload
    assert destination.stat().st_ino != pin.stat().st_ino
    assert pin.read_bytes() == b"the dead attempt's bytes"
    assert "adopted" not in _proof(second, "b39")
    assert answer["retired"] == [str(destination)]
    leftovers = sorted(path.name for path in destination.parent.iterdir()
                       if path.name != destination.name)
    assert leftovers == [], "nothing is left at a private or temporary name"
    second.queue.finish(handle["export_key"], status="executed")
    assert second.poll_group("b39")["complete"]
    assert second.release_group("b39")["ok"], "the retirement record does not hold release"


def test_a_dead_attempts_leftover_temporary_is_retired(tmp_path):
    """An export that died mid-copy left ``<path>.tmp``; the successor lands anyway."""

    first = world(tmp_path)
    _source, destination, entries = prepare(first, batch="b38", payload=b"never landed")
    handle = first.submit_group("b38", entries)
    _fail_export(first, handle["export_key"])
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(f"{destination}.tmp")
    temporary.write_bytes(b"never la")
    second = _retry(first)
    payload = b"landed by the successor"
    _source, _destination, entries = prepare(second, batch="b38", payload=payload)
    handle = second.submit_group("b38", entries)
    claim_export(second, handle)

    answer = _try_export(second, "b38")

    assert answer["ok"], (
        f"a dead attempt's leftover temporary blocks its successor: {answer}")
    assert destination.read_bytes() == payload and not temporary.exists()


def test_an_interrupted_retirement_is_finished_by_the_next_run(tmp_path):
    """A run stopped after moving the failed file aside: the next one deletes it."""

    first = world(tmp_path)
    destination, _ = _landed(first, "b39", b"the dead attempt's bytes")
    pin = tmp_path / "dead-inode-pin"
    pin.hardlink_to(destination)
    second = _new_key(first, "resubmit")
    payload = b"the successor's bytes"
    _source, _destination, entries = prepare(second, batch="b39", payload=payload)
    handle = second.submit_group("b39", entries)
    claim_export(second, handle)
    # What `_OtherClaims._retire` leaves when the run stops inside the
    # rename-aside delete: the record, and the file at its private name.
    key = handle["export_key"]
    tag = f"export-{key[:16]}"
    ps._write(second._group("b39") / ps.RETIRING_MARKER, {
        "schema": ps.SCHEMA, "export_key": key, "tag": tag,
        "paths": {str(destination): ps._identity(destination)}})
    private = Path(po._retiring_name(str(destination), tag))
    destination.rename(private)

    answer = _try_export(second, "b39")

    assert answer["ok"], f"an interrupted retirement blocks the next run: {answer}"
    assert destination.read_bytes() == payload
    assert not private.exists(), "the interrupted delete is finished"
    assert pin.read_bytes() == b"the dead attempt's bytes"
    assert sorted(path.name for path in destination.parent.iterdir()) == [destination.name]


def test_a_live_export_of_another_group_does_not_hold_the_destination(tmp_path):
    """The dead attempt's b40 export still runs; it never writes b39's path."""

    first = world(tmp_path)
    destination, _ = _landed(first, "b39", b"the dead attempt's b39")
    _source, _other, entries = prepare(first, batch="b40", payload=b"the dead attempt's b40")
    running = first.submit_group("b40", entries)
    claim_export(first, running)
    second = _retry(first)
    payload = b"the successor's b39"
    _source, _destination, entries = prepare(second, batch="b39", payload=payload)
    handle = second.submit_group("b39", entries)
    claim_export(second, handle)

    answer = _try_export(second, "b39")

    assert po._mover_live_state(first.queue, running["export_key"]) == pool.CLAIMED
    assert answer["ok"], f"another group's live export holds this path: {answer}"
    assert destination.read_bytes() == payload


# -- what still refuses -----------------------------------------------------------


def test_a_destination_no_failed_group_owns_still_refuses(tmp_path):
    """A dead attempt exists, but not for this path: the file is nobody's."""

    first = world(tmp_path)
    _landed(first, "b39", b"the dead attempt's own path")
    second = _retry(first)
    _source, destination, entries = prepare(second, batch="b40", payload=b"successor")
    destination.write_bytes(b"work nobody filed")
    handle = second.submit_group("b40", entries)
    claim_export(second, handle)

    answer = _try_export(second, "b40")

    assert not answer["ok"]
    assert "unowned or changed canonical destination" in answer["refusal"]
    assert destination.read_bytes() == b"work nobody filed"


def test_a_failed_groups_export_that_may_still_run_holds_the_destination(tmp_path):
    """The dead attempt's export is still claimed: it can still write the path."""

    first = world(tmp_path)
    destination, running = _landed(first, "b39", b"landed by a live export",
                                   finish=False)
    assert po._mover_live_state(first.queue, running["export_key"]) == pool.CLAIMED
    second = _retry(first)
    _source, _destination, entries = prepare(second, batch="b39",
                                             payload=b"the successor's bytes")
    handle = second.submit_group("b39", entries)
    claim_export(second, handle)

    answer = _try_export(second, "b39")

    assert not answer["ok"]
    assert running["export_key"] in answer["refusal"], answer
    assert destination.read_bytes() == b"landed by a live export"


def test_a_committed_file_is_adopted_with_its_bytes_and_never_replaced(tmp_path):
    """A dead attempt that committed its group: the same bytes adopt, others refuse."""

    first = world(tmp_path)
    payload = b"committed before the attempt died"
    destination, _ = _landed(first, "b39", payload)
    descriptor = po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "s0", "artifact_class": "payload",
        "path": str(destination), "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "producer_generation": po.mint_generation(),
        "owner_action_key": first.owner, "owner_attempt": first.instance["owner_attempt"]},
        first.template, first.instance)
    batch = po.publish_prepaid_batch(first.queue, first.instance, first.template,
        [descriptor], batch_id="b39", tier=fx.TIER, cas_root=first.cas_root,
        producer_action_key=first.owner, command_extra=["--unpaced"])
    assert batch["ok"], batch
    mover = fx._claim_mover(first.queue, "stage-never-ran")
    assert mover["action_key"] == batch["mover_key"]
    first.queue.finish(batch["mover_key"], status="failed")
    inode = destination.stat().st_ino

    second = _retry(first)
    _source, _destination, entries = prepare(second, batch="b39",
                                             payload=b"other bytes over a commit")
    handle = second.submit_group("b39", entries)
    claim_export(second, handle)
    refused = _try_export(second, "b39")
    assert not refused["ok"] and "committed" in refused["refusal"], refused
    assert destination.read_bytes() == payload and destination.stat().st_ino == inode
    second.queue.finish(handle["export_key"], status="failed")
    _fail_export(second, handle["export_key"])

    third = _retry(second)
    _source, _destination, entries = prepare(third, batch="b39", payload=payload)
    handle = third.submit_group("b39", entries)
    claim_export(third, handle)
    answer = _try_export(third, "b39")
    assert answer["ok"], f"a committed file with the successor's bytes is refused: {answer}"
    assert destination.stat().st_ino == inode
    assert _proof(third, "b39")["adopted"]["owner_kind"] == "batch"


def test_every_dead_attempt_in_a_retry_chain_is_asked_about_its_export(tmp_path, monkeypatch):
    """Keys A and B are dead and both prewrote b39; B's export is still claimed."""

    first = world(tmp_path)
    destination, _ = _landed(first, "b39", b"key A's bytes")
    second = _new_key(first, "resubmit-1")
    _source, _destination, entries = prepare(second, batch="b39", payload=b"key B's bytes")
    running = second.submit_group("b39", entries)
    claimed = second.queue.claim(owner="a-worker-that-is-still-copying", tags=[second.host])
    assert claimed["action_key"] == running["export_key"]
    third = _new_key(second, "resubmit-2")
    assert po._mover_live_state(third.queue, running["export_key"]) == pool.CLAIMED
    _source, _destination, entries = prepare(third, batch="b39", payload=b"key C's bytes")
    handle = third.submit_group("b39", entries)
    claim_export(third, handle)

    answer = _try_export(third, "b39")

    assert not answer["ok"]
    assert "failed-group-export-live" in answer["refusal"], answer
    assert running["export_key"] in answer["refusal"], answer
    assert destination.read_bytes() == b"key A's bytes"

    # Once B's export has failed for good, every dead claim is asked about,
    # whichever order the attempt index lists them in.
    asked = []
    original = ps._OtherClaims.require_ended_export

    def recording(self, claim, path):
        asked.append(claim["owner_action_key"])
        return original(self, claim, path)

    monkeypatch.setattr(ps._OtherClaims, "require_ended_export", recording)
    third.queue.finish(running["export_key"], status="failed")
    _fail_export(third, running["export_key"])

    answer = _try_export(third, "b39")

    assert answer["ok"], answer
    assert sorted(asked) == sorted([first.owner, second.owner]), asked
    assert destination.read_bytes() == b"key C's bytes"

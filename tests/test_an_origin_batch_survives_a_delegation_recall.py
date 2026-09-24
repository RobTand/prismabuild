"""A committed origin survives an NFS delegation recall that moves only its timestamps (#1111).

Part 2 of #1096.  Over NFSv4.2 a producer's client holds a write delegation
with delegated timestamps on each origin file it writes.  The first reader
on another client recalls it, and the server applies the delegated
timestamps: ``ctime_ns`` (and sometimes ``mtime_ns``) moves while the
inode, the size and the bytes stay the same.  A Stage A handoff is a
write-only origin batch, and its first consumer stage-in is that reader.
The origin checks compared the identity the commit recorded field by field,
so after the recall:

*   a second declaration of the batch (`origin_batch_manifest`, what
    ``pbrun`` derives again at submission) refused ``origin-batch-changed``;
*   the retirement of a consumed batch refused ``origin-changed`` on every
    tick, and the batch was never deleted;
*   a staged batch could not be restaged (``restage-origin-changed``);
*   an origin whose timestamps moved between the export receipt and the
    commit refused ``origin-is-not-the-landed-copy``.

Every origin batch that can meet this carries its sha256, so a mismatch in
the timestamps alone is settled by the content: the check reads the file,
and when its digest is the committed one it re-pins the identity on the
commitments entry (``origin_repins``), with the old and the new identity and
the reason, so later checks compare against it and read nothing.  The batch
record stays immutable.  A changed inode or size, different bytes, or a
batch with no digest still refuses.

The recall is simulated on a local file system: a same-mode ``chmod`` moves
ctime alone, and ``os.utime`` moves mtime and ctime.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_consumed_origin_retirement as retirement  # noqa: E402
import test_produced_output_restage as restage  # noqa: E402
from prismabuild import produced_output as po, reader_lease  # noqa: E402
from test_an_export_survives_a_delegation_recall import (  # noqa: E402
    _identity, _recall_ctime, _recall_mtime,
)
from test_write_only_produced_output import (  # noqa: E402
    _descriptor, _prewrite, _queue, _template,
)

_isolated_synthetic_launch_context = retirement.fx._isolated_synthetic_launch_context

RECALLS = pytest.mark.parametrize("recall", [_recall_ctime, _recall_mtime],
                                  ids=["ctime", "mtime-and-ctime"])


def _record_bytes(queue, instance, batch_id: str = "b1") -> bytes:
    return (Path(queue.root) / "residency" / po.OUTPUT_BATCHES_SUBDIR
            / po.instance_namespace(instance) / f"{batch_id}.json").read_bytes()


def _assert_repinned(repins, path, before, after, sha256):
    assert repins, "the check re-pinned nothing"
    last = repins[-1]
    assert last["path"] == str(path), last
    assert last["from"] == before and last["to"] == after, (
        f"the re-pin does not name the old and the new identity: {last}")
    assert last["sha256"] == sha256, last
    assert "timestamp" in last["reason"], last


def _no_reads(monkeypatch, why: str) -> None:
    def unexpected(*args, **kwargs):
        raise AssertionError(why)

    monkeypatch.setattr(reader_lease, "content_identity", unexpected,
                        raising=False)


def _rewrite_same_size(path: Path) -> None:
    """Different bytes, the same inode and size; the mtime moves."""

    info = os.stat(path)
    data = path.read_bytes()
    with path.open("r+b") as handle:
        handle.write(bytes([data[0] ^ 0xFF]))
    os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000_000))


# -- the Stage A handoff: declared again, then retired ----------------------


@RECALLS
def test_a_recalled_handoff_is_declared_again_and_then_retired(
        tmp_path: Path, monkeypatch, recall) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    payload = b"band handoff bytes"
    instance, path, committed = retirement._commit(
        queue, template, "recalled", payload=payload)
    record = _record_bytes(queue, instance)
    before, after = recall(path)

    try:
        manifest = po.origin_batch_manifest(queue.root, [committed["ref"]])
    except po.ProducedOutputError as exc:
        raise AssertionError(
            f"a batch whose timestamps alone moved could not be declared: {exc}"
        ) from None

    assert [entry["path"] for entry in manifest["entries"]] == [str(path)]
    entry = retirement._entry(queue, instance)
    _assert_repinned(entry.get("origin_repins"), path, before, after,
                     hashlib.sha256(payload).hexdigest())
    assert _record_bytes(queue, instance) == record, (
        "the re-pin rewrote the immutable batch record")

    # The re-pinned identity is what later checks compare against.
    _no_reads(monkeypatch, "a re-pinned identity was verified again")
    po.origin_batch_manifest(queue.root, [committed["ref"]])
    assert len(retirement._entry(queue, instance)["origin_repins"]) == 1

    queue.finish(instance["owner_action_key"], status="failed")
    events = po.origin_retirement_tick(queue)

    assert [(e["event"], e["reason"]) for e in events] == [
        (po.ORIGIN_RETIRED_EVENT, "orphan")], events
    assert events[0]["unlinked"] == [str(path)]
    assert events[0]["origin_identity"] == {str(path): after}
    assert not path.exists()
    assert retirement._entry(queue, instance)["origin_reclaimed"] is True
    assert retirement._charged(queue, instance) == 0


@RECALLS
def test_a_recalled_orphan_retires_on_its_first_tick(tmp_path: Path,
                                                     recall) -> None:
    """No declaration re-pinned it first: the tick verifies it itself."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    payload = b"never declared"
    instance, path, _committed = retirement._commit(
        queue, template, "orphan", payload=payload)
    queue.finish(instance["owner_action_key"], status="failed")
    before, after = recall(path)

    events = po.origin_retirement_tick(queue)

    assert [(e["event"], e["reason"]) for e in events] == [
        (po.ORIGIN_RETIRED_EVENT, "orphan")], (
        f"a batch whose timestamps alone moved was not retired: {events}")
    assert not path.exists()
    entry = retirement._entry(queue, instance)
    assert entry["origin_reclaimed"] is True
    _assert_repinned(entry.get("origin_repins"), path, before, after,
                     hashlib.sha256(payload).hexdigest())


def test_a_same_size_content_change_refuses_the_declaration_and_says_why(
        tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = retirement._commit(queue, template, "changed")
    recorded = _identity(path)
    _rewrite_same_size(path)
    observed = _identity(path)
    assert (observed["ino"], observed["size"]) == (recorded["ino"],
                                                   recorded["size"])

    with pytest.raises(po.ProducedOutputError, match="origin-batch-changed") as info:
        po.origin_batch_manifest(queue.root, [committed["ref"]])

    assert "sha256" in str(info.value), (
        f"the refusal does not say the content differs: {info.value}")
    assert "origin_repins" not in retirement._entry(queue, instance)


# -- the commit: the timestamps moved after the export receipt --------------


@RECALLS
def test_a_commit_after_a_recall_commits_the_verified_identity(
        tmp_path: Path, recall) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance = retirement._bind_owner(queue, template,
                                      retirement.fx._hexkey("landed"))
    path = Path(template["output_prefix"]) / "landed-b1.bin"
    payload = b"landed then recalled"
    assert _prewrite(queue, instance, template, "b1", [path], len(payload))["ok"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    descriptor = _descriptor(instance, template, path, payload)
    landed = {str(path): _identity(path)}
    before, after = recall(path)

    committed = po.commit_origin_batch(
        queue, instance, template, [descriptor], batch_id="b1", landed=landed,
        lifetime=retirement.CONSUMED)

    assert committed.get("ok"), (
        f"a landed copy whose timestamps alone moved could not commit: {committed}")
    sha256 = hashlib.sha256(payload).hexdigest()
    _assert_repinned(committed.get("landed_repins"), path, before, after, sha256)
    _assert_repinned(retirement._entry(queue, instance).get("landed_repins"),
                     path, before, after, sha256)
    record = json.loads(_record_bytes(queue, instance))
    assert record["origin_identity"] == {str(path): after}


def test_a_same_size_content_change_after_the_receipt_still_refuses_the_commit(
        tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance = retirement._bind_owner(queue, template,
                                      retirement.fx._hexkey("landed-changed"))
    path = Path(template["output_prefix"]) / "landed-b1.bin"
    payload = b"landed then rewritten"
    assert _prewrite(queue, instance, template, "b1", [path], len(payload))["ok"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    descriptor = _descriptor(instance, template, path, payload)
    landed = {str(path): _identity(path)}
    _rewrite_same_size(path)

    committed = po.commit_origin_batch(
        queue, instance, template, [descriptor], batch_id="b1", landed=landed)

    assert committed.get("refusal") == "origin-is-not-the-landed-copy", committed


# -- a staged batch's restage -------------------------------------------------


def _staged(tmp_path: Path, *, digest_mode: str, payload: bytes = b"S" * 600):
    world = restage._World(tmp_path)
    descs = restage._descriptors(world.template, world.inst, "p1", payload,
                                 digest_mode=digest_mode)
    restage._prewrite(world.q, world.inst, world.template, "b1", descs)
    first = world.first_publish("b1", descs)
    world.run_mover(str(first["mover_key"]), "w-setup")
    assert world.retire("b1").get("ok") is True
    assert po.refill_window(world.q, world.inst, world.template,
                            tier=restage.TIER).get("ok") is True
    return world, descs, Path(str(descs[0]["path"]))


def test_a_restage_after_a_recall_verifies_and_re_pins(tmp_path: Path) -> None:
    world, descs, origin = _staged(tmp_path, digest_mode="sha256")
    before, after = _recall_ctime(origin)

    ensured = world.ensure("b1")

    assert ensured.get("ok") is True, (
        f"a staged batch whose ctime alone moved could not be restaged: {ensured}")
    assert ensured["state"] == "materializing"
    _assert_repinned(world.entry("b1").get("origin_repins"), origin, before,
                     after, descs[0]["sha256"])


def test_a_same_size_content_change_still_refuses_the_restage_and_says_why(
        tmp_path: Path) -> None:
    world, _descs, origin = _staged(tmp_path, digest_mode="sha256")
    _rewrite_same_size(origin)

    refused = world.ensure("b1")

    assert refused.get("refusal") == "restage-origin-changed", refused
    assert "sha256" in str(refused.get("detail")), (
        f"the refusal does not say the content differs: {refused}")
    entry = world.entry("b1")
    assert "materializations" not in entry and "origin_repins" not in entry


def test_a_null_digest_batch_stays_strict_and_reads_nothing(
        tmp_path: Path, monkeypatch) -> None:
    """The DEV null digest: nothing can say the bytes are the committed ones."""

    world, _descs, origin = _staged(tmp_path, digest_mode="null")
    _recall_ctime(origin)
    _no_reads(monkeypatch, "a batch with no digest was verified by content")

    refused = world.ensure("b1")

    assert refused.get("refusal") == "restage-origin-changed", refused
    assert "materializations" not in world.entry("b1")

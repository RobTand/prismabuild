"""An export survives an NFS delegation recall that moves only its timestamps (#1096).

Live shape, 2026-09-24: Stage A round-2 q0 (``85c3c57fdf75``) failed on
``export-destination-changed`` about two minutes after the layer-39 group's
export was acknowledged.  Over NFSv4.2 the exporting client holds a write
delegation with delegated timestamps.  When another reader recalls it, the
server applies the delegated timestamps: ``ctime_ns`` (and sometimes
``mtime_ns``) moves while the inode, the size and the bytes stay the same.
The release check compared the recorded identity field by field and refused
a correct export.

The export records every entry's sha256, so a mismatch in timestamps only is
settled by the content: the check re-reads the file, and when its digest is
the recorded one it re-pins the identity in the record it checked, with the
old and new identities and the reason, so the next check compares against
the re-pinned identity and reads nothing.  A changed inode or size, or
different bytes, still refuses.

The recall is simulated on a local filesystem: a same-mode ``chmod`` moves
ctime alone, and ``os.utime`` moves mtime and ctime.  Everything runs on a
synthetic origin prefix, spool and queue under ``tmp_path``.
"""
from __future__ import annotations

import os
from pathlib import Path
import stat
import time

import pytest

from test_produced_spool import claim_export, prepare, world
from prismabuild import produced_spool as ps
from prismabuild import reader_lease


def _export(spool, batch="b1"):
    group = spool._group(batch)
    record = ps._read(group / "export.json")
    return ps.export_group(spool.queue, group / "manifest.json",
                           record["manifest_sha256"], record["export_key"])


def _exported(spool, payload=b"hello"):
    """Export one group; its export row stays claimed, so it can run again."""

    source, destination, entries = prepare(spool, payload=payload)
    handle = spool.submit_group("b1", entries)
    claim_export(spool, handle)
    assert _export(spool)["ok"]
    return source, destination, handle


def _landed(spool, payload=b"hello"):
    source, destination, handle = _exported(spool, payload)
    spool.queue.finish(handle["export_key"], status="executed")
    assert spool.poll_group("b1")["complete"]
    return source, destination, handle


def _identity(path):
    return reader_lease.portable_identity(os.stat(path))


def _until_moved(path, change):
    """Apply ``change`` until the file's ctime moves; the before and after identities."""

    before = _identity(path)
    for _ in range(1000):
        change(path)
        after = _identity(path)
        if after["ctime_ns"] != before["ctime_ns"]:
            return before, after
        time.sleep(0.002)
    raise AssertionError(f"ctime of {path} never moved")


def _recall_ctime(path):
    """A recall that applies a delegated ctime: a same-mode chmod moves ctime only."""

    mode = stat.S_IMODE(os.stat(path).st_mode)
    before, after = _until_moved(path, lambda target: os.chmod(target, mode))
    assert after["mtime_ns"] == before["mtime_ns"], "the fixture moved mtime too"
    return before, after


def _recall_mtime(path):
    """A recall that applies a delegated mtime: ``os.utime`` moves mtime and ctime."""

    info = os.stat(path)
    later = info.st_mtime_ns + 1_000_000_000
    before, after = _until_moved(
        path, lambda target: os.utime(target, ns=(info.st_atime_ns, later)))
    assert after["mtime_ns"] == later
    return before, after


def _proof(spool, name="receipt.json"):
    body = ps._read(spool._group("b1") / name)
    return body["entries"][0] if name == "receipt.json" else body


def _assert_repinned(proof, before, after):
    repins = proof.get("repinned")
    assert repins, f"the check re-pinned nothing: {proof}"
    last = repins[-1]
    assert last["from"] == before and last["to"] == after, (
        f"the re-pin does not name the old and the new identity: {last}")
    assert last["sha256"] == proof["sha256"], last
    assert "timestamp" in last["reason"], last
    assert proof["identity"] == after, (
        f"later checks compare against {proof['identity']}, not the re-pinned {after}")


@pytest.mark.parametrize("recall", [_recall_ctime, _recall_mtime],
                         ids=["ctime", "mtime-and-ctime"])
def test_a_release_after_a_timestamp_only_recall_verifies_and_re_pins(tmp_path, recall):
    spool = world(tmp_path)
    source, destination, _handle = _landed(spool)
    before, after = recall(destination)

    answer = spool.release_group("b1")

    assert answer.get("ok"), (
        f"a correct export refused after only its timestamps moved: {answer}")
    assert not source.exists(), "the release removed the local copy"
    assert destination.read_bytes() == b"hello"
    _assert_repinned(_proof(spool), before, after)
    assert spool.release_group("b1").get("duplicate")


def test_a_poll_re_pins_once_and_the_next_poll_reads_nothing(tmp_path, monkeypatch):
    spool = world(tmp_path)
    _source, destination, handle = _landed(spool)
    before, after = _recall_ctime(destination)

    first = spool.poll_group("b1")

    assert first.get("ok") and first.get("complete"), (
        f"a poll refused a correct export after only its ctime moved: {first}")
    assert first["export_key"] == handle["export_key"]
    _assert_repinned(_proof(spool), before, after)

    def unexpected(*args, **kwargs):
        raise AssertionError("a re-pinned identity was verified again")

    monkeypatch.setattr(reader_lease, "content_identity", unexpected)
    second = spool.poll_group("b1")
    assert second.get("ok") and second.get("complete"), second
    assert len(_proof(spool)["repinned"]) == 1, "the second poll re-pinned again"


def test_a_completed_copy_rerun_after_a_recall_re_pins_its_copy_proof(tmp_path, monkeypatch):
    """The export's crash window: its copy proof completed, its receipt never landed."""

    spool = world(tmp_path)
    _source, destination, entries = prepare(spool)
    handle = spool.submit_group("b1", entries)
    claim_export(spool, handle)
    original = ps._write

    def interrupted(path, body):
        if Path(path).name == "receipt.json":
            raise RuntimeError("interrupted before the group acknowledgement")
        return original(path, body)

    monkeypatch.setattr(ps, "_write", interrupted)
    with pytest.raises(RuntimeError, match="interrupted"):
        _export(spool)
    monkeypatch.setattr(ps, "_write", original)
    before, after = _recall_ctime(destination)

    try:
        answer = _export(spool)
    except ps.SpoolError as exc:
        raise AssertionError(
            f"the rerun refused a completed copy whose ctime alone moved: {exc}") from None

    assert answer["ok"], answer
    _assert_repinned(_proof(spool, "copy-0.json"), before, after)
    _assert_repinned(_proof(spool), before, after)
    spool.queue.finish(handle["export_key"], status="executed")
    assert spool.release_group("b1").get("ok")


def test_a_same_size_content_change_still_refuses_and_says_why(tmp_path):
    spool = world(tmp_path)
    source, destination, _handle = _landed(spool)
    recorded = _identity(destination)
    info = os.stat(destination)
    with open(destination, "r+b") as handle:
        handle.write(b"HELLO")
    os.utime(destination, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000_000))
    observed = _identity(destination)
    assert (observed["ino"], observed["size"]) == (recorded["ino"], recorded["size"])

    answer = spool.release_group("b1")

    assert answer["refusal"] == "export-destination-changed", answer
    assert source.exists(), "a refused release keeps the local copy"
    evidence = answer["evidence"]
    assert evidence["entry_index"] == 0 and evidence["path"] == str(destination)
    assert evidence["recorded"] == recorded and evidence["observed"] == observed, evidence
    assert "sha256" in str(evidence.get("detail")), (
        f"the refusal does not say the content differs: {evidence}")
    assert "repinned" not in _proof(spool), "a refused check re-pinned the identity"


def test_a_same_inode_size_change_refuses_without_reading(tmp_path, monkeypatch):
    spool = world(tmp_path)
    _source, destination, _handle = _landed(spool)
    with open(destination, "ab") as handle:
        handle.write(b", world")

    def unexpected(*args, **kwargs):
        raise AssertionError("a size mismatch was verified by content")

    monkeypatch.setattr(reader_lease, "content_identity", unexpected, raising=False)
    answer = spool.release_group("b1")

    assert answer["refusal"] == "export-destination-changed", answer
    assert answer["evidence"]["observed"]["size"] == len(b"hello, world")

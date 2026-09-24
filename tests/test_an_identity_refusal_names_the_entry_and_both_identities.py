"""An identity refusal names its entry and both identities, and outlives the spool (#1098).

Live shape, 2026-09-24: Stage A round-2 q0 (``85c3c57fdf75``) failed on
``export-destination-changed``.  The refusal said which check failed and
nothing else: not which of the group's files, not the identity the export
recorded, not the one the poll found.  The copy proofs that held the recorded
identities (``copy-N.json``) were on the producer's host, and the spool
retirement tick (#1001) deleted them with the dead attempt's group.  What
would have settled #1096 -- ctime moved, content unchanged -- was gone.

So every identity refusal in ``produced_spool`` names the entry, the path,
the identity it recorded and the one it observed, and a refused group's
receipt and copy proofs are filed under the queue when the refusal is made,
where no spool cleanup reaches them.  The refusal code a caller reads is
unchanged.

Everything runs on a synthetic origin prefix, spool and queue under
``tmp_path``.  Nothing reads or writes a real stage, origin or queue.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from test_produced_spool import claim_export, prepare, world
from prismabuild import produced_spool as ps


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


def _replace(path, payload, tmp_path):
    """Put a new inode at ``path``; the old one stays pinned, so its number is not reused."""

    pin = tmp_path / f"pin-{path.name}"
    pin.hardlink_to(path)
    replacement = path.with_name(path.name + ".replacement")
    replacement.write_bytes(payload)
    os.replace(replacement, path)
    return pin.stat().st_ino, path.stat().st_ino


def _records(spool):
    directory = Path(spool.queue.root) / "produced-spool-refusals" / spool.owner
    assert directory.is_dir(), f"no refusal was filed under {directory}"
    return [json.loads(path.read_text()) for path in sorted(directory.iterdir())
            if path.name.endswith(".json")]


# -- a poll's refusal ---------------------------------------------------------------


def test_a_changed_destination_refusal_names_the_entry_and_both_identities(tmp_path):
    spool = world(tmp_path)
    _source, destination, _handle = _landed(spool)
    recorded, observed = _replace(destination, b"other", tmp_path)

    answer = spool.poll_group("b1")

    assert answer["refusal"] == "export-destination-changed", "the code a caller reads"
    evidence = answer.get("evidence")
    assert evidence, f"the refusal names neither the entry nor an identity: {answer}"
    assert evidence["entry_index"] == 0 and evidence["path"] == str(destination)
    assert evidence["recorded"]["ino"] == recorded
    assert evidence["observed"]["ino"] == observed


def test_a_refused_release_names_the_local_file_and_both_identities(tmp_path):
    spool = world(tmp_path)
    source, _destination, _handle = _landed(spool)
    before = source.stat()
    os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns + 1_000_000_000))

    answer = spool.release_group("b1")

    assert answer["refusal"] == "local-spool-changed-retain"
    evidence = answer.get("evidence")
    assert evidence, f"the refusal names neither the file nor an identity: {answer}"
    assert evidence["path"] == str(source)
    assert evidence["recorded"]["mtime_ns"] == before.st_mtime_ns
    assert evidence["observed"]["mtime_ns"] == before.st_mtime_ns + 1_000_000_000


# -- an export's and a submit's refusal ------------------------------------------------


def _stray_destination(spool, destination, source, tmp_path):
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(b"nobody's")
    return None, destination.stat().st_ino


def _stray_temporary(spool, destination, source, tmp_path):
    temporary = Path(f"{destination}.tmp")
    temporary.parent.mkdir(parents=True, exist_ok=True)
    temporary.write_bytes(b"nobody's")
    return None, temporary.stat().st_ino


def _changed_source(spool, destination, source, tmp_path):
    before = source.stat()
    os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns + 1_000_000_000))
    return before.st_mtime_ns, before.st_mtime_ns + 1_000_000_000


@pytest.mark.parametrize("change,code", [
    (_stray_destination, "unowned or changed canonical destination"),
    (_stray_temporary, "unowned export temporary"),
    (_changed_source, "local source changed before export"),
])
def test_an_export_refusal_names_the_entry_and_both_identities(tmp_path, change, code):
    spool = world(tmp_path)
    source, destination, entries = prepare(spool)
    handle = spool.submit_group("b1", entries)
    claim_export(spool, handle)
    recorded, observed = change(spool, destination, source, tmp_path)

    with pytest.raises(ps.SpoolError) as refused:
        _export(spool)

    message = str(refused.value)
    assert message.startswith(code), message
    assert "entry 0" in message, f"the refusal does not name its entry: {message}"
    for value in (recorded, observed):
        if value is not None:
            assert str(value) in message, f"{value} is not in the refusal: {message}"


def test_a_completed_export_rerun_names_both_destination_identities(tmp_path):
    spool = world(tmp_path)
    _source, destination, _handle = _exported(spool)
    recorded, observed = _replace(destination, b"other", tmp_path)

    with pytest.raises(ps.SpoolError) as refused:
        _export(spool)

    message = str(refused.value)
    assert message.startswith("export-destination-changed"), message
    assert str(recorded) in message and str(observed) in message, (
        f"the refusal does not name both inodes: {message}")


def test_a_submit_refusal_names_the_declared_and_the_observed_size(tmp_path):
    spool = world(tmp_path)
    source, _destination, entries = prepare(spool, payload=b"hello")
    with source.open("ab") as handle:
        handle.write(b", world")

    with pytest.raises(ps.SpoolError) as refused:
        spool.submit_group("b1", entries)

    message = str(refused.value)
    assert message.startswith("local source size changed"), message
    assert "'size': 12" in message and "'bytes': 5" in message, (
        f"the refusal does not name both sizes: {message}")


# -- the evidence outlives the spool ------------------------------------------------------


def test_a_refused_groups_copy_proofs_outlive_the_retirement_tick(tmp_path):
    spool = world(tmp_path)
    _source, destination, handle = _landed(spool)
    group = spool._group("b1")
    proof = ps._read(group / "copy-0.json")
    recorded, observed = _replace(destination, b"other", tmp_path)
    assert spool.poll_group("b1")["refusal"] == "export-destination-changed"
    spool.queue.withdraw(spool.owner, reason="test: the producer failed on the refusal")
    spool.queue.finish(spool.owner, status="failed")
    ps.retirement_tick(spool.queue, spool.cas_root, host="spool-host")
    assert not group.exists(), "the tick retired the dead attempt's group"

    records = _records(spool)

    assert len(records) == 1, records
    record = records[0]
    assert record["code"] == "export-destination-changed"
    assert record["batch_id"] == "b1" and record["export_key"] == handle["export_key"]
    assert record["copy_proofs"] == [proof]
    assert record["receipt"]["entries"] == [proof]
    assert record["evidence"]["recorded"]["ino"] == recorded
    assert record["evidence"]["observed"]["ino"] == observed


def test_a_poll_repeated_on_one_refusal_files_it_once(tmp_path):
    spool = world(tmp_path)
    _source, destination, _handle = _landed(spool)
    _replace(destination, b"other", tmp_path)

    for _ in range(3):
        assert spool.poll_group("b1")["refusal"] == "export-destination-changed"

    assert len(_records(spool)) == 1


def test_an_export_refusal_is_filed_with_the_groups_copy_proofs(tmp_path):
    spool = world(tmp_path)
    _source, destination, _handle = _exported(spool)
    proof = ps._read(spool._group("b1") / "copy-0.json")
    _replace(destination, b"other", tmp_path)

    with pytest.raises(ps.SpoolError, match="export-destination-changed"):
        _export(spool)

    records = [record for record in _records(spool) if record["where"] == "export"]
    assert len(records) == 1, records
    assert records[0]["code"] == "export-destination-changed"
    assert records[0]["copy_proofs"] == [proof]

"""The published, read-only produced-spool receipt check (#1286).

A client outside PrismaBuild (PQ's forward-recovery loader) asks one question
of an exported group: does this receipt prove this manifest's export, with
every destination still the inode the export landed?  The internal
``_check_receipt`` answers it, but carries ``destinations=`` for the
retirement tick (#1001) and ``repin=`` for a caller that holds the group's
``.export.lock`` (#1096).  ``check_export_receipt`` is the public form: it
takes no lock, writes nothing and re-pins nothing, and a mismatch in
timestamps only is a public refusal, never the internal ``_RepinNeeded``.
"""
from __future__ import annotations

import copy
import hashlib
import inspect
import os
from pathlib import Path

import pytest

from test_produced_spool import world
from test_an_export_survives_a_delegation_recall import _landed, _recall_ctime, _recall_mtime
from prismabuild import produced_spool as ps


def _group_documents(spool, batch="b1"):
    group = spool._group(batch)
    record = ps._read(group / "export.json")
    manifest = ps._read(group / "manifest.json", expected_sha256=record["manifest_sha256"])
    receipt = ps._read(group / "receipt.json")
    return receipt, manifest, record


def _tree(*roots):
    """Every path under ``roots`` with its identity and, for a file, its digest."""

    seen = {}
    for root in roots:
        for path in sorted(Path(root).rglob("*")):
            info = os.lstat(path)
            entry = (info.st_mode, info.st_ino, info.st_size, info.st_mtime_ns,
                     info.st_ctime_ns)
            if path.is_file() and not path.is_symlink():
                entry += (hashlib.sha256(path.read_bytes()).hexdigest(),)
            seen[str(path)] = entry
    return seen


@pytest.fixture
def no_writes(monkeypatch):
    """Fail on any lock, spool write, re-pin or write-mode open during the check."""

    def refuse(name):
        def call(*args, **kwargs):
            raise AssertionError(f"check_export_receipt called {name}")
        return call

    monkeypatch.setattr(ps, "_lock", refuse("_lock"))
    monkeypatch.setattr(ps, "_write", refuse("_write"))
    monkeypatch.setattr(ps, "_repinned", refuse("_repinned"))
    original = os.open
    write_flags = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND

    def guarded(path, flags, *args, **kwargs):
        if flags & write_flags:
            raise AssertionError(f"check_export_receipt opened {path} for writing")
        return original(path, flags, *args, **kwargs)

    monkeypatch.setattr(ps.os, "open", guarded)
    for name in ("mkdir", "rename", "replace", "unlink", "remove", "utime", "chmod"):
        monkeypatch.setattr(ps.os, name, refuse(f"os.{name}"))


def test_the_check_is_public_and_takes_only_the_three_documents():
    assert "check_export_receipt" in dir(ps)
    parameters = inspect.signature(ps.check_export_receipt).parameters
    assert list(parameters) == ["receipt", "manifest", "record"]
    assert all(p.kind is p.POSITIONAL_OR_KEYWORD and p.default is p.empty
               for p in parameters.values())


def test_a_landed_export_is_accepted_and_nothing_moves(tmp_path, monkeypatch, request):
    spool = world(tmp_path)
    _, destination, _ = _landed(spool)
    receipt, manifest, record = _group_documents(spool)
    documents = copy.deepcopy((receipt, manifest, record))
    before = _tree(tmp_path)
    request.getfixturevalue("no_writes")
    assert ps.check_export_receipt(receipt, manifest, record) is None
    monkeypatch.undo()
    assert (receipt, manifest, record) == documents, "the check changed its arguments"
    assert _tree(tmp_path) == before, "the check changed the filesystem"


@pytest.mark.parametrize("recall", [_recall_ctime, _recall_mtime],
                         ids=["ctime", "mtime-and-ctime"])
def test_a_timestamp_only_mismatch_is_a_public_refusal_not_a_repin(tmp_path, monkeypatch,
                                                                    request, recall):
    spool = world(tmp_path)
    _, destination, _ = _landed(spool)
    recorded, observed = recall(destination)
    receipt, manifest, record = _group_documents(spool)
    documents = copy.deepcopy((receipt, manifest, record))
    before = _tree(tmp_path)
    request.getfixturevalue("no_writes")
    with pytest.raises(ps.SpoolIdentityRefusal) as refused:
        ps.check_export_receipt(receipt, manifest, record)
    monkeypatch.undo()
    assert not isinstance(refused.value, ps._RepinNeeded)
    assert refused.value.code == "export-destination-changed"
    assert refused.value.evidence["entry_index"] == 0
    assert refused.value.evidence["path"] == str(destination)
    assert refused.value.evidence["recorded"] == recorded
    assert refused.value.evidence["observed"] == observed
    assert "timestamps" in refused.value.evidence["detail"]
    assert (receipt, manifest, record) == documents, "the check changed its arguments"
    assert "repinned" not in receipt["entries"][0]
    assert _tree(tmp_path) == before, "the check re-pinned or wrote"


def test_a_replaced_destination_is_refused(tmp_path):
    spool = world(tmp_path)
    _, destination, _ = _landed(spool)
    receipt, manifest, record = _group_documents(spool)
    replacement = destination.with_name(destination.name + ".new")
    replacement.write_bytes(b"hello")
    os.replace(replacement, destination)
    with pytest.raises(ps.SpoolIdentityRefusal) as refused:
        ps.check_export_receipt(receipt, manifest, record)
    assert refused.value.code == "export-destination-changed"


def test_an_absent_destination_is_refused(tmp_path):
    spool = world(tmp_path)
    _, destination, _ = _landed(spool)
    receipt, manifest, record = _group_documents(spool)
    destination.unlink()
    with pytest.raises(ps.SpoolIdentityRefusal) as refused:
        ps.check_export_receipt(receipt, manifest, record)
    assert refused.value.evidence["observed"] == "absent"


@pytest.mark.parametrize("corrupt", ["export_key", "manifest_sha256", "entries"])
def test_a_receipt_not_bound_to_its_record_is_refused(tmp_path, corrupt):
    spool = world(tmp_path)
    _landed(spool)
    receipt, manifest, record = _group_documents(spool)
    if corrupt == "entries":
        receipt["entries"] = []
    else:
        receipt[corrupt] = "0" * 64
    with pytest.raises(ps.SpoolError, match="export receipt binding is corrupt"):
        ps.check_export_receipt(receipt, manifest, record)

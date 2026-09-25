"""A successor's committed identity survives a retirement's link-back (#1064).

#1053's delete (`produced_output._unlink_if_committed`) renames an origin to
a private name, and when the file it moved is not the committed one --
another writer renamed its own file onto the name first -- it puts that
file back.  The rename and the put-back each move the file's ctime, and
`reader_lease.file_id_matches`, which every strict reader and every restage
uses, compares ctime.  Two faults followed:

(a) Both commit paths, `commit_batch` and `commit_origin_batch`, took each
    origin's identity before they took the output-prefix lock.  A
    retirement holding that lock could move the file aside and back while
    the commit waited, so the identity the commit filed was stale before it
    was filed: a DEV null-digest staged batch could never be restaged
    (``restage-origin-changed``), and every other check paid a content
    read (#1111) to accept it.  An lstat that landed while the file was at
    the private name refused the commit as ``descriptor-unstatable``.  The
    identity is now taken under the lock, so a retirement step the commit
    waited for is behind it.

(b) The put-back was a hard link.  With ``fs.protected_hardlinks = 1``
    (dl380g10) a process may not link a file it does not own and cannot
    write, and the tier loop does not own the files producers write:
    ``os.link`` raised ``PermissionError``, the delete refused as
    ``origin-unlink``, and the other writer's file stayed at the private
    name, off its path.  A refused link now falls back to a rename that
    never replaces (``renameat2`` with ``RENAME_NOREPLACE``), which needs no
    ownership.

The retirement that holds the lock is modelled by running its delete step
when the commit asks for the output-prefix lock, before the lock is
granted: the one interleaving that lock orders.  A writer whose template's
prefix only overlaps takes that lock too since #1063, so the same
interleaving covers it.
The refused link is simulated by making ``os.link`` of a private retiring
name raise ``EPERM`` when the name is free, as the kernel does.
"""
from __future__ import annotations

import contextlib
import errno
import json
import os
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

from prismabuild import produced_output as po, reader_lease  # noqa: E402

import test_prepaid_writer_integration as fx  # noqa: E402
import test_produced_output_lifecycle_r2 as r2  # noqa: E402
import test_write_only_produced_output as wo  # noqa: E402
from test_a_dead_producers_batches_and_paths_are_released import (  # noqa: E402
    _events,
)
from test_a_dead_producers_origin_files_survive_every_sweep import (  # noqa: E402
    _dead_consumed_batch, _interpose, _private, _successor_rename,
)
from test_consumed_origin_retirement import _bind_owner  # noqa: E402

_isolated_synthetic_launch_context = fx._isolated_synthetic_launch_context

#: A predecessor's recorded identity.  No file has inode 0, so the delete
#: finds another writer's file at the name and puts it back.
PREDECESSOR = {"ino": 0, "size": 0, "mtime_ns": 0, "ctime_ns": 0}
#: The private-name tag of the modelled retirement.
TAG = "1064" * 4


def _identity(path: Path) -> dict[str, int]:
    return reader_lease.portable_identity(os.lstat(path))


def _same_file(before: dict, after: dict) -> bool:
    return all(before[key] == after[key] for key in ("ino", "size", "mtime_ns"))


def _moved_aside_and_back(path: Path, since: dict) -> None:
    """The retirement's delete meets another writer's file and puts it back.

    Repeated until the file's ctime differs from ``since``: a coarse clock
    can stamp a quick rename and link with the ctime the file already had,
    and then there would be nothing for the commit to get wrong.
    """

    for _ in range(1000):
        if _identity(path)["ctime_ns"] != since["ctime_ns"]:
            break
        assert po._unlink_if_committed(str(path), PREDECESSOR, TAG) == (
            "superseded", ""), "the delete did not put the writer's file back"
        time.sleep(0.002)
    else:
        raise AssertionError(f"the ctime of {path} never moved")
    assert _same_file(since, _identity(path)), "not the writer's file"


def _while_a_retirement_holds_the_lock(monkeypatch, queue, prefix: str,
                                       step) -> list[bool]:
    """Run ``step`` when the commit asks for the output-prefix lock.

    ``step`` runs holding the lock and before the commit's block under it
    starts, as a retirement's delete that held the lock first runs before
    the commit is granted it.  Every other lock passes straight through.
    """

    real = queue.stage_ownership_lock
    ran: list[bool] = []

    @contextlib.contextmanager
    def lock(stage_root, *args, **kwargs):
        with real(stage_root, *args, **kwargs) as held:
            if str(stage_root) == prefix and not ran:
                ran.append(True)
                step()
            yield held

    monkeypatch.setattr(queue, "stage_ownership_lock", lock)
    return ran


def _filed(queue, instance: dict, batch_id: str) -> dict:
    return json.loads((Path(queue.root) / "residency"
                       / po.OUTPUT_BATCHES_SUBDIR
                       / po.instance_namespace(instance)
                       / f"{batch_id}.json").read_text())


def _no_reads(monkeypatch) -> None:
    def unexpected(*args, **kwargs):
        raise AssertionError("the committed identity needed a content read")

    monkeypatch.setattr(reader_lease, "content_identity", unexpected)


# -- (a) the commit takes its identity under the lock ------------------------


class _Successor:
    """One live producer that wrote one origin file and is about to commit it."""

    def __init__(self, tmp_path: Path, kind: str) -> None:
        self.kind = kind
        if kind == "staged-null-digest":
            origin = tmp_path / "outputs"
            origin.mkdir(parents=True)
            self.template = r2._template(str(origin))
            self.queue = r2._queue(tmp_path)
            self.instance = r2._bind(self.queue, self.template)["instance"]
            assert po.admit_instance(self.queue, self.instance,
                                     self.template)["ok"] is True
            self.batch_id = "w0"
            self.path = origin / "w0.pt"
            payload = b"S" * 64
            assert po.require_prewrite(
                self.queue, self.instance, self.template,
                batch_id=self.batch_id, tier=r2.STAGE_TIER,
                class_bytes={"payload": len(payload), "checkpoint": 0,
                             "temp": 0},
                paths=[str(self.path)])["ok"] is True
            r2._write(self.path, payload)
            # The DEV convention: no digest, so the identity is the proof.
            self.descriptor = po.validate_descriptor({
                "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "boundary-0",
                "artifact_class": "payload", "path": str(self.path),
                "bytes": len(payload), "sha256": None,
                "producer_generation": po.mint_generation(),
                "owner_action_key": self.instance["owner_action_key"],
                "owner_attempt": dict(self.instance["owner_attempt"]),
            }, self.template, self.instance)
            self.landed = None
            return
        self.template = wo._template(tmp_path / "canonical")
        self.queue = wo._queue(tmp_path)
        self.instance = _bind_owner(self.queue, self.template,
                                    fx._hexkey("successor"))
        self.batch_id = "b1"
        self.path = Path(self.template["output_prefix"]) / "successor-b1.bin"
        payload = b"the successor's bytes"
        assert wo._prewrite(self.queue, self.instance, self.template,
                            self.batch_id, [self.path], len(payload))["ok"]
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_bytes(payload)
        self.descriptor = wo._descriptor(self.instance, self.template,
                                         self.path, payload)
        # The spool passes the identity each file had when its export landed
        # it (`ProducedSpool.commit_origin_group`).
        self.landed = ({str(self.path): _identity(self.path)}
                       if kind == "origin-only-landed" else None)

    @property
    def prefix(self) -> str:
        return str(self.template["output_prefix"])

    def commit(self) -> dict:
        if self.kind == "staged-null-digest":
            return po.commit_batch(self.queue, self.instance, self.template,
                                   [self.descriptor], batch_id=self.batch_id,
                                   tier=r2.STAGE_TIER, mover_key=r2.MOVER0)
        return po.commit_origin_batch(self.queue, self.instance, self.template,
                                      [self.descriptor], batch_id=self.batch_id,
                                      landed=self.landed)


KINDS = pytest.mark.parametrize(
    "kind", ["origin-only", "origin-only-landed", "staged-null-digest"])


def _assert_the_identity_is_the_files(successor: _Successor, committed: dict,
                                      monkeypatch) -> None:
    assert committed["ok"] is True, committed
    filed = _filed(successor.queue, successor.instance, successor.batch_id)
    recorded = filed["origin_identity"][str(successor.path)]
    assert reader_lease.file_id_matches(recorded, _identity(successor.path)), (
        "the commit recorded an identity its file no longer has: "
        f"{recorded} against {_identity(successor.path)}")
    if successor.kind == "staged-null-digest":
        # The restage gate: with no digest, the identity is all it has.
        assert po._check_origin_identity(filed, filed["entries"]) == {
            "ok": True, "repins": []}
        return
    if successor.kind == "origin-only-landed":
        # The landed copy is accepted by one content read outside the lock,
        # and the identity it hashed is the one committed.
        repins = committed.get("landed_repins") or []
        assert [repin["path"] for repin in repins] == [str(successor.path)]
        assert repins[0]["from"] == successor.landed[str(successor.path)]
        assert repins[0]["to"] == recorded
    # A strict reader accepts the batch on its identity alone.
    _no_reads(monkeypatch)
    manifest = po.origin_batch_manifest(successor.queue.root,
                                        [committed["ref"]])
    assert [entry["path"] for entry in manifest["entries"]] == [
        str(successor.path)]
    entry = po._read_commitments(po._commitments_path(
        successor.queue.root, successor.instance))["batches"][successor.batch_id]
    assert "origin_repins" not in entry


@KINDS
def test_a_link_back_the_commit_waited_for_is_behind_its_identity(
        tmp_path: Path, monkeypatch, kind: str) -> None:
    """The retirement moves the file aside and back while the commit waits."""

    successor = _Successor(tmp_path, kind)
    written = _identity(successor.path)
    ran = _while_a_retirement_holds_the_lock(
        monkeypatch, successor.queue, successor.prefix,
        lambda: _moved_aside_and_back(successor.path, written))

    committed = successor.commit()

    assert committed["ok"] is True, committed
    assert ran, "the commit never asked for the output-prefix lock"
    _assert_the_identity_is_the_files(successor, committed, monkeypatch)


@KINDS
def test_a_file_at_the_private_name_when_the_commit_starts_is_waited_for(
        tmp_path: Path, monkeypatch, kind: str) -> None:
    """The retirement has moved the file aside; the commit waits for it back."""

    successor = _Successor(tmp_path, kind)
    written = _identity(successor.path)
    private = po._retiring_name(str(successor.path), TAG)
    os.rename(successor.path, private)

    def put_back() -> None:
        assert po._link_back(str(successor.path), private) == ("superseded", "")
        _moved_aside_and_back(successor.path, written)

    ran = _while_a_retirement_holds_the_lock(
        monkeypatch, successor.queue, successor.prefix, put_back)

    committed = successor.commit()

    assert committed["ok"] is True, committed
    assert ran, "the commit never asked for the output-prefix lock"
    _assert_the_identity_is_the_files(successor, committed, monkeypatch)


def test_an_origin_whose_timestamps_keep_moving_is_refused_not_committed_stale(
        tmp_path: Path, monkeypatch) -> None:
    """The landed check reads again once; a second move refuses the commit.

    Nothing is filed: an identity taken before the last move would be stale.
    """

    successor = _Successor(tmp_path, "origin-only-landed")
    moves: list[bool] = []
    real = successor.queue.stage_ownership_lock

    @contextlib.contextmanager
    def lock(stage_root, *args, **kwargs):
        with real(stage_root, *args, **kwargs) as held:
            if str(stage_root) == successor.prefix:
                moves.append(True)
                _moved_aside_and_back(successor.path, _identity(successor.path))
            yield held

    monkeypatch.setattr(successor.queue, "stage_ownership_lock", lock)

    committed = successor.commit()

    assert len(moves) == 2, moves
    assert committed["ok"] is False, committed
    assert committed["refusal"] == "origin-is-not-the-landed-copy", committed
    assert str(successor.path) in committed["detail"]
    batches = po._read_commitments(po._commitments_path(
        successor.queue.root, successor.instance))["batches"]
    assert successor.batch_id not in batches, "the refused commit filed a batch"

    # Nothing was consumed: once the file stops moving, the commit is taken.
    monkeypatch.setattr(successor.queue, "stage_ownership_lock", real)
    _assert_the_identity_is_the_files(successor, successor.commit(), monkeypatch)


# -- (b) a refused link still puts the writer's file back --------------------


def _hardlinks_protected(monkeypatch) -> list[str]:
    """``os.link`` of a private retiring name fails with ``EPERM``.

    What ``fs.protected_hardlinks = 1`` does to a process that does not own
    the file and cannot write it.  The kernel creates the new name first, so
    a name that is taken still answers ``EEXIST``.
    """

    real = os.link
    refused: list[str] = []

    def link(source, target, *, src_dir_fd=None, dst_dir_fd=None,
             follow_symlinks=True):
        if ".pb-retiring-" in os.fspath(source):
            try:
                os.stat(target, dir_fd=dst_dir_fd, follow_symlinks=False)
            except FileNotFoundError:
                refused.append(os.fspath(source))
                raise PermissionError(errno.EPERM, os.strerror(errno.EPERM),
                                      os.fspath(source)) from None
        return real(source, target, src_dir_fd=src_dir_fd,
                    dst_dir_fd=dst_dir_fd, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(os, "link", link)
    return refused


def _leftovers(path: Path) -> list[str]:
    return [name for name in os.listdir(path.parent) if ".pb-retiring-" in name]


def test_a_writers_file_the_retirement_may_not_link_goes_back_to_its_path(
        tmp_path: Path, monkeypatch) -> None:
    """The #1053 race, on a file the tier loop does not own."""

    template, queue, instance, path = _dead_consumed_batch(tmp_path)
    landed: dict[str, dict] = {}

    def write() -> None:
        _successor_rename(path, b"the successor's bytes")()
        landed["identity"] = _identity(path)

    fired = _interpose(monkeypatch, path, write, after=False)
    refused = _hardlinks_protected(monkeypatch)

    events = po.origin_retirement_tick(queue)

    assert fired and refused, "the put-back never met a refused link"
    assert path.read_bytes() == b"the successor's bytes", (
        "the successor's file is not at its path")
    assert _same_file(landed["identity"], _identity(path)), (
        "the file at the path is not the successor's")
    retired = _events(events, po.ORIGIN_RETIRED_EVENT)
    assert len(retired) == 1, events
    assert retired[0]["unlinked"] == []
    assert retired[0]["superseded"] == [str(path)]
    assert _leftovers(path) == []


def test_an_interrupted_delete_puts_a_writers_file_back_without_a_link(
        tmp_path: Path, monkeypatch) -> None:
    """The settle step after a crash, on a file the tier loop does not own."""

    template, queue, instance, path = _dead_consumed_batch(tmp_path)
    private = _private(instance, path)
    _successor_rename(path, b"the successor's bytes")()
    written = _identity(path)
    os.rename(path, private)
    refused = _hardlinks_protected(monkeypatch)

    events = po.origin_retirement_tick(queue)

    assert refused, "the put-back never met a refused link"
    retired = _events(events, po.ORIGIN_RETIRED_EVENT)
    assert len(retired) == 1, events
    assert retired[0]["unlinked"] == []
    assert retired[0]["superseded"] == [str(path)]
    assert path.read_bytes() == b"the successor's bytes"
    assert _same_file(written, _identity(path))
    assert not private.exists()


def test_a_file_nothing_can_put_back_is_kept_named_and_put_back_later(
        tmp_path: Path, monkeypatch) -> None:
    """No link and no no-replace rename (NFS answers EINVAL): refused, never lost."""

    template, queue, instance, path = _dead_consumed_batch(tmp_path)
    private = _private(instance, path)
    _successor_rename(path, b"the successor's bytes")()
    os.rename(path, private)
    real_link, real_rename = os.link, po._rename_noreplace
    _hardlinks_protected(monkeypatch)

    def unsupported(*args, **kwargs):
        raise OSError(errno.EINVAL, os.strerror(errno.EINVAL))

    monkeypatch.setattr(po, "_rename_noreplace", unsupported)

    events = po.origin_retirement_tick(queue)

    refused = _events(events, po.ORIGIN_RETIREMENT_REFUSED_EVENT)
    assert len(refused) == 1, events
    assert refused[0]["reason"].startswith("origin-displaced"), refused
    assert str(private) in refused[0]["reason"]
    assert "Operation not permitted" in refused[0]["reason"]
    assert "Invalid argument" in refused[0]["reason"]
    assert private.read_bytes() == b"the successor's bytes"
    assert not path.exists()
    assert po.origin_retirement_tick(queue) == [], "reported once"

    monkeypatch.setattr(os, "link", real_link)
    monkeypatch.setattr(po, "_rename_noreplace", real_rename)
    events = po.origin_retirement_tick(queue)

    retired = _events(events, po.ORIGIN_RETIRED_EVENT)
    assert len(retired) == 1 and retired[0]["superseded"] == [str(path)], events
    assert path.read_bytes() == b"the successor's bytes"
    assert not private.exists()


def test_a_rename_that_never_replaces_refuses_a_taken_name(
        tmp_path: Path) -> None:
    """The fallback itself: it moves a file to a free name and never over one."""

    source, target = tmp_path / "source", tmp_path / "target"
    source.write_bytes(b"moved")
    inode = os.lstat(source).st_ino
    po._rename_noreplace(str(source), str(target))
    assert not source.exists() and target.read_bytes() == b"moved"
    assert os.lstat(target).st_ino == inode

    source.write_bytes(b"not moved")
    with pytest.raises(FileExistsError):
        po._rename_noreplace(str(source), str(target))
    assert source.read_bytes() == b"not moved"
    assert target.read_bytes() == b"moved"

    directory = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        po._rename_noreplace("source", "other", dir_fd=directory)
    finally:
        os.close(directory)
    assert (tmp_path / "other").read_bytes() == b"not moved"
    assert target.read_bytes() == b"moved"

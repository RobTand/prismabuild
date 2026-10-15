"""Pair 1 of the logical-dedup phase (issue #1295): one owner for first-writer publish.

``immutable_files.publish_immutable`` owns the recipe; slurm's ``_atomic_publish_nofollow``
is deleted and its two callers pass their ``where`` through.  Every test below
is tmp_path-only: no SLURM binary, no mount, no network.
"""

from __future__ import annotations

import errno
import os
import stat
import tempfile

import pytest

from prismabuild import core as pb
from prismabuild import immutable_files


def _debris(directory):
    return [p.name for p in directory.iterdir() if p.suffix == ".tmp"]


def test_first_writer_wins_bytes_readonly_no_debris(tmp_path):
    target = tmp_path / "claim.json"
    assert immutable_files.publish_immutable(target, b'{"v":1}\n') is True
    assert immutable_files.publish_immutable(target, b'{"v":2}\n') is False
    assert target.read_bytes() == b'{"v":1}\n'
    assert stat.S_IMODE(target.stat().st_mode) == 0o444
    assert _debris(tmp_path) == []


def test_empty_payload_publishes(tmp_path):
    target = tmp_path / "empty.bin"
    assert immutable_files.publish_immutable(target, b"") is True
    assert target.read_bytes() == b""
    assert stat.S_IMODE(target.stat().st_mode) == 0o444


def _fail_mkstemp(monkeypatch):
    def failing(*args, **kwargs):
        raise OSError(errno.ENOSPC, "injected temp failure")

    monkeypatch.setattr(tempfile, "mkstemp", failing)


def test_without_where_raw_oserror_propagates(tmp_path, monkeypatch):
    _fail_mkstemp(monkeypatch)
    with pytest.raises(OSError) as caught:
        immutable_files.publish_immutable(tmp_path / "x.json", b"x")
    assert not isinstance(caught.value, pb.CASUnavailableError)


def test_where_maps_oserror_to_cas_unavailable(tmp_path, monkeypatch):
    _fail_mkstemp(monkeypatch)
    with pytest.raises(pb.CASUnavailableError, match="cannot publish probe") as caught:
        immutable_files.publish_immutable(tmp_path / "x.json", b"x", where="probe")
    assert pb.CASUnavailableError is immutable_files.CASUnavailableError
    assert pb.CASTamperError is immutable_files.CASTamperError
    assert isinstance(caught.value, pb.PrismaBuildError)
    assert caught.value.__cause__.errno == errno.ENOSPC
    assert pb.ReplacedRecordError.__bases__ == (immutable_files.CASTamperError,)
    assert pb._FileLinkCountError.__bases__ == (immutable_files.CASTamperError,)


def test_prelink_verify_fires_and_refusals_propagate(tmp_path):
    target = tmp_path / "hooked.json"
    fired: list[str] = []
    assert immutable_files.publish_immutable(
        target, b"{}", prelink_verify=lambda: fired.append("ran")
    ) is True
    assert fired == ["ran"]

    def refuse():
        raise ValueError("prelink refusal")

    with pytest.raises(ValueError, match="prelink refusal"):
        immutable_files.publish_immutable(tmp_path / "other.json", b"{}", prelink_verify=refuse)


def _lying_stat(real_stat, name, fake):
    def lying(*args, **kwargs):
        if args and args[0] == name:
            return fake
        return real_stat(*args, **kwargs)

    return lying


@pytest.mark.parametrize("where", [None, "tamper probe"])
def test_readback_tamper_is_never_mapped_to_unavailable(
    tmp_path, monkeypatch, where
):
    target = tmp_path / "tampered.json"
    if where is None:
        assert immutable_files.publish_immutable(target, b"first") is True
    else:
        assert immutable_files.publish_immutable(target, b"first", where=where) is True
    victim = tmp_path / "victim.json"
    real = target.stat()
    fake = os.stat_result(
        (
            real.st_mode,
            real.st_ino + 1000,
            real.st_dev,
            real.st_nlink,
            real.st_uid,
            real.st_gid,
            real.st_size,
            real.st_atime,
            real.st_mtime,
            real.st_ctime,
        )
    )
    monkeypatch.setattr(
        pb.os, "stat", _lying_stat(pb.os.stat, victim.name, fake)
    )
    with pytest.raises(pb.CASTamperError, match="changed before readback"):
        if where is None:
            immutable_files.publish_immutable(victim, b"second")
        else:
            immutable_files.publish_immutable(victim, b"second", where=where)

"""An unreadable RAM tier announcement is not an epoch move (#1146).

GLM-5.3 Stage B row 023 died 9 minutes in: ``open_pinned`` read the RAM
tier's announcement, the read failed under load, and the failure was
reported as "epoch moved during the hold".  The announced epoch had not
changed.  These tests pin the split: a transient fault is read through, a
readable announcement with another epoch still refuses as a move, and one
that stays unreadable raises :class:`TierAnnouncementUnreadable`.

The faults are injected at both ``builtins.open`` and ``os.open``, matched on
the announcement's own file name, so the red run on main (which read the
record with ``open``) and the green run (which reads it through the no-follow
helper's ``os.open``) cross the same fault.
"""
from __future__ import annotations

import builtins
import errno
import json
import os
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map, reader_lease  # noqa: E402
import stage_release  # noqa: E402

CONSUMER = "c" * 64
MOVER = "e" * 64
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:testhost"
ANNOUNCEMENT = f"{RAM_TIER}.json"
ATTEMPT = {"nonce": "n1", "scope_id": "s1"}
HOLDER = {"host": "test-host", "pid": 4242}
SOURCE = "/mnt/shared/model/p.safetensors"


@pytest.fixture()
def held(tmp_path: Path):
    """A RAM pin acquired under ``epoch-1``, and what opening it needs."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=STAGE_TIER,
                                      stage_root=stage)
    staged = stage / "model" / "p.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x16" * 4096)
    root = queue.root / pool.RESIDENCY
    key = residency_map.residency_map_key(SOURCE, 0)
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": RAM_TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64, "epoch": "epoch-1",
        "entries": {key: {"stage_path": str(staged), "bytes": 4096,
                          "sha256": "b" * 64, "offset": 0}}})
    identity = reader_lease.stat_identity(str(staged))
    assert identity is not None
    reader_lease.write_material(
        root, consumer_action_key=CONSUMER, mover_action_key=MOVER,
        tier_id=RAM_TIER, stage_root=str(stage), manifest_sha256="a" * 64,
        generation=reader_lease.mint_generation(),
        entries={key: {"stage_path": str(staged), "bytes": 4096,
                       "sha256": "b" * 64, "file_id": identity}},
        epoch="epoch-1")
    tiers = queue.root / "tiers"
    tiers.mkdir(parents=True, exist_ok=True)
    announcement = tiers / ANNOUNCEMENT
    announcement.write_text(json.dumps({"tier_id": RAM_TIER,
                                        "epoch": "epoch-1"}))
    acquired = _acquire(queue, root, "epoch-token")
    assert acquired["ok"], acquired
    return {"queue": queue, "root": root, "key": key,
            "announcement": announcement, "acquired": acquired}


def _acquire(queue, root: Path, token: str) -> dict:
    return reader_lease.acquire(
        queue, consumer_action_key=CONSUMER, attempt=ATTEMPT,
        tier_id=RAM_TIER, epoch="epoch-1",
        span={"start_bytes": 0, "end_bytes": 4096},
        holder=HOLDER, acquire_token=token,
        covers=[{"mover_action_key": MOVER, "manifest_sha256": "a" * 64}],
        residency_root=root)


def _open(held: dict):
    acquired = held["acquired"]
    return reader_lease.open_pinned(held["queue"], acquired["pin"],
                                    acquired["ref_id"], held["key"],
                                    residency_root=held["root"])


def _fail_announcement_opens(monkeypatch, code: int, times: int | None):
    """Fail the announcement's next ``times`` opens with ``code`` (``None``: all).

    Both seams are wrapped; the returned list counts the faults raised.
    """

    fired: list[str] = []
    real_open, real_os_open = builtins.open, os.open

    def armed(path) -> bool:
        if os.path.basename(os.fsdecode(path)) != ANNOUNCEMENT:
            return False
        return times is None or len(fired) < times

    def fake_open(file, *args, **kwargs):
        if isinstance(file, (str, bytes, os.PathLike)) and armed(file):
            fired.append("open")
            raise OSError(code, os.strerror(code), os.fsdecode(file))
        return real_open(file, *args, **kwargs)

    def fake_os_open(path, *args, **kwargs):
        if armed(path):
            fired.append("os.open")
            raise OSError(code, os.strerror(code), os.fsdecode(path))
        return real_os_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)
    monkeypatch.setattr(os, "open", fake_os_open)
    return fired


def _record_sleeps(monkeypatch) -> list[float]:
    slept: list[float] = []
    monkeypatch.setattr(time, "sleep", slept.append)
    return slept


def test_a_stale_handle_once_then_the_same_epoch_opens(held, monkeypatch):
    """The #1146 regression: one ESTALE, then epoch-1 again, opens the pin."""

    fired = _fail_announcement_opens(monkeypatch, errno.ESTALE, times=1)
    fd, serving = _open(held)
    try:
        assert fired, "the fault was never injected"
        assert len(fired) == 1
        assert serving["tier_id"] == RAM_TIER
        assert serving["epoch"] == "epoch-1"
        assert os.read(fd, 4) == b"\x16" * 4
    finally:
        os.close(fd)


def test_torn_bytes_once_then_the_same_epoch_opens(held, monkeypatch):
    """A read that returns a torn record is read again, not called a move."""

    from prismabuild import core

    real_read = core._read_regular_file_nofollow
    torn: list[Path] = []

    def tearing(path, **kwargs):
        if Path(path).name == ANNOUNCEMENT and not torn:
            torn.append(Path(path))
            return b'{"tier_id": "ram:testhost", "epo'
        return real_read(path, **kwargs)

    monkeypatch.setattr(core, "_read_regular_file_nofollow", tearing)
    fd, serving = _open(held)
    try:
        assert torn, "the torn read was never served"
        assert serving["epoch"] == "epoch-1"
    finally:
        os.close(fd)


def test_a_readable_different_epoch_still_refuses_as_a_move(held):
    """A real re-announcement keeps the existing refusal and message."""

    held["announcement"].write_text(json.dumps({"tier_id": RAM_TIER,
                                                "epoch": "epoch-2"}))
    with pytest.raises(reader_lease.ReaderLeaseError,
                       match="epoch moved during the hold") as caught:
        _open(held)
    assert not isinstance(caught.value,
                          reader_lease.TierAnnouncementUnreadable)


def test_an_announcement_that_stays_unreadable_is_named_not_moved(
        held, monkeypatch):
    """Past the bounded retry: the new error, never "epoch moved"."""

    slept = _record_sleeps(monkeypatch)
    fired = _fail_announcement_opens(monkeypatch, errno.ESTALE, times=None)
    with pytest.raises(reader_lease.TierAnnouncementUnreadable) as caught:
        _open(held)
    error = caught.value
    assert isinstance(error, reader_lease.ReaderLeaseError)
    message = str(error)
    assert "epoch moved" not in message
    assert str(held["announcement"]) in message
    assert "ESTALE" in message
    assert " s " in message or " s (" in message
    assert error.path == str(held["announcement"])
    assert error.errno == errno.ESTALE
    assert error.attempts == len(reader_lease.RELEASE_RETRY_DELAYS_S) + 1
    assert len(fired) == error.attempts
    # The bound is the module's existing transient-fault schedule.
    assert slept == list(reader_lease.RELEASE_RETRY_DELAYS_S)
    assert error.seconds >= 0

    # Nothing was released: once the announcement reads, the same ref opens.
    monkeypatch.undo()
    fd, serving = _open(held)
    os.close(fd)
    assert serving["epoch"] == "epoch-1"


def test_a_non_transient_fault_is_not_retried(held, monkeypatch):
    """An errno that does not clear by itself is named at once."""

    slept = _record_sleeps(monkeypatch)
    _fail_announcement_opens(monkeypatch, errno.EACCES, times=None)
    with pytest.raises(reader_lease.TierAnnouncementUnreadable) as caught:
        _open(held)
    assert caught.value.attempts == 1
    assert caught.value.errno == errno.EACCES
    assert slept == []


def test_acquire_names_an_unreadable_announcement(held, monkeypatch):
    """Acquire keeps the ``stale-epoch`` head and says the record did not read."""

    _record_sleeps(monkeypatch)
    _fail_announcement_opens(monkeypatch, errno.ESTALE, times=None)
    refused = _acquire(held["queue"], held["root"], "second-token")
    assert refused["ok"] is False
    refusal = str(refused["refusal"])
    assert refusal.split(":", 1)[0] == "stale-epoch"
    assert "unreadable" in refusal and "not moved" in refusal

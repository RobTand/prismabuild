"""#1451: the host-local state policy accepts tmpfs; the disk scratch does not.

The measurement census qualifies its ``BOX_STATE_ROOT`` rendezvous with the
same exact open-descriptor mount observer the local-scratch profile uses, and
applied that observer's disk *I/O* filesystem policy to it.  ``/tmp`` is tmpfs
on the affected worker, so every ordinary CPU action waiting there was denied
``measurement_census_unavailable`` with ``scratch filesystem type not
supported: tmpfs``.

These are the causal controls.  One observer, two closed purpose policies that
differ by tmpfs alone; the disk profile still refuses tmpfs on a real tmpfs
mount; shared storage still refuses; and the census reader runs on a real
tmpfs state directory with its same-directory guard lock.
"""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import shutil
import tempfile

import pytest

from prismabuild import (
    _measurement_reservation as reservation, adaptive_cpu, local_scratch,
)

from test_measurement_drains_gpu_backfill import fleet as fleet_fixture

fleet = fleet_fixture

#: Every mount type either closed policy is compared against in the tables.
SHARED_AND_UNKNOWN_TYPES = ["nfs", "nfs4", "cifs", "9p", "overlay", "squashfs"]


def _mount_points(wanted) -> list[Path]:
    """The mount points of real mounts whose type ``wanted`` accepts."""

    points: list[Path] = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        before, separator, after = line.partition(" - ")
        fields = before.split()
        if not separator or len(fields) < 5:
            continue
        if wanted(after.split()[0]):
            points.append(Path(fields[4].replace("\\040", " ")))
    return points


def _an_accessible_directory(points, *, writable: bool) -> Path | None:
    mode = os.W_OK if writable else os.R_OK
    for point in points:
        try:
            if point.is_dir() and os.access(point, mode | os.X_OK):
                return point
        except OSError:
            continue
    return None


@pytest.fixture()
def tmpfs_mount() -> Path:
    """A real tmpfs mount this box holds, or a recorded skip."""

    points = _mount_points(lambda kind: kind == "tmpfs")
    # /dev/shm is not the production mount -- the affected worker's
    # BOX_STATE_ROOT is /tmp, which is tmpfs there.  Prefer /dev/shm as a
    # representative writable tmpfs of the same filesystem type so the
    # control never mounts or repoints anything.
    points.sort(key=lambda point: 0 if point == Path("/dev/shm") else 1)
    mount = _an_accessible_directory(points, writable=True)
    if mount is None:
        pytest.skip("this host mounts no writable tmpfs")
    return mount


@pytest.fixture()
def tmpfs_state(tmpfs_mount: Path):
    made = Path(tempfile.mkdtemp(prefix="pb-1451-state-", dir=tmpfs_mount))
    try:
        yield made
    finally:
        shutil.rmtree(made, ignore_errors=True)


def test_the_state_policy_is_the_disk_policy_plus_tmpfs_only():
    assert (local_scratch.LOCAL_STATE_FILESYSTEM_TYPES
            - local_scratch.LOCAL_FILESYSTEM_TYPES) == {"tmpfs"}
    assert (local_scratch.LOCAL_FILESYSTEM_TYPES
            - local_scratch.LOCAL_STATE_FILESYSTEM_TYPES) == set()


@pytest.mark.parametrize(
    "filesystem",
    ["ext4", "xfs", "btrfs", "zfs", "tmpfs"] + SHARED_AND_UNKNOWN_TYPES)
def test_the_two_purpose_policies_are_closed(filesystem, monkeypatch):
    identity = {"device": "1", "filesystem": "2",
                "filesystem_type": filesystem, "root_inode": "3"}
    monkeypatch.setattr(local_scratch, "_descriptor_mount",
                        lambda fd: dict(identity))
    # The disk scratch profile keeps exactly its qualified local set: this is
    # the policy that must NOT change.
    if filesystem in local_scratch.LOCAL_FILESYSTEM_TYPES:
        assert local_scratch._descriptor_identity(0) == identity
    else:
        with pytest.raises(local_scratch.LocalScratchError,
                           match="scratch filesystem type"):
            local_scratch._descriptor_identity(0)
    # The host-local state rendezvous adds tmpfs and nothing else.
    if filesystem in local_scratch.LOCAL_STATE_FILESYSTEM_TYPES:
        assert local_scratch._descriptor_state_identity(0) == identity
    else:
        with pytest.raises(local_scratch.LocalScratchError,
                           match="state filesystem type"):
            local_scratch._descriptor_state_identity(0)


def test_a_real_tmpfs_state_directory_is_accepted_but_never_a_scratch_profile(
    tmpfs_state: Path,
):
    state = tmpfs_state / "box-state"
    state.mkdir(mode=0o700)
    descriptor = os.open(state, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        identity = local_scratch._descriptor_state_identity(descriptor)
    finally:
        os.close(descriptor)
    assert identity["filesystem_type"] == "tmpfs"
    # Same descriptor, same exact type: the I/O-profile qualification is
    # unchanged and still refuses it.
    with pytest.raises(local_scratch.LocalScratchError,
                       match="scratch filesystem type not supported: tmpfs"):
        local_scratch.root_identity(str(state))


def test_a_real_shared_state_directory_still_refuses():
    shared = _an_accessible_directory(
        _mount_points(lambda kind: kind.startswith("nfs")
                      or kind in {"cifs", "9p", "smb3"}),
        writable=False)
    if shared is None:
        pytest.skip("this host mounts no shared storage")
    descriptor = os.open(shared, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        with pytest.raises(local_scratch.LocalScratchError,
                           match="state filesystem type"):
            local_scratch._descriptor_state_identity(descriptor)
    finally:
        os.close(descriptor)


def test_a_non_directory_descriptor_is_a_malformed_observation_refusal(
    tmpfs_state: Path,
):
    regular = tmpfs_state / "not-a-directory"
    regular.write_bytes(b"x")
    descriptor = os.open(regular, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        with pytest.raises(local_scratch.LocalScratchError,
                           match="not a directory"):
            local_scratch._descriptor_state_identity(descriptor)
    finally:
        os.close(descriptor)


def test_the_census_reader_runs_on_a_real_tmpfs_state_directory(
    fleet, tmpfs_state: Path, monkeypatch
):
    queue, *_ = fleet
    state = tmpfs_state / "box-state"
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", state)
    census = reservation.CensusReader(queue, queue.ledger())
    assert census.directory == state
    value = census.capture()
    assert set(value) == {"measurements", "elections", "selections",
                          "opportunities", "keys", "gang_elections"}
    assert value["keys"] == []
    # The reader ran from this exact directory: its guard lives here and stays.
    # A permanent guard is the design, and the state root must never be cleared
    # or repointed while census readers or admission claimants can survive: a
    # cleared /tmp only lapses that mutual exclusion.
    guard = state / (census.name + ".guard")
    assert guard.exists()
    # Reuse control: a settled reader fence leaves the guard reusable.
    again = reservation.CensusReader(queue, queue.ledger()).capture()
    assert again["keys"] == []
    assert guard.exists()


def test_a_malformed_reader_fence_refuses_instead_of_being_ignored(
    fleet, tmpfs_state: Path, monkeypatch
):
    queue, *_ = fleet
    state = tmpfs_state / "box-state"
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", state)
    census = reservation.CensusReader(queue, queue.ledger())
    (state / census.name).write_text("{", encoding="utf-8")
    with pytest.raises(reservation.CensusUnavailable):
        reservation.CensusReader(queue, queue.ledger()).capture()


def test_a_live_retained_census_fence_refuses_the_next_reader(
    fleet, tmpfs_state: Path, monkeypatch
):
    queue, *_ = fleet
    state = tmpfs_state / "box-state"
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", state)
    census = reservation.CensusReader(queue, queue.ledger())
    census.capture()  # settles the reader and writes its ownership fence
    marker = state / census.name
    record = json.loads(marker.read_text(encoding="utf-8"))
    # Point the fence at a live process on this boot, with its real start time:
    # the next reader must refuse the retained fence, never read past it.
    fields = Path("/proc/1/stat").read_text(encoding="utf-8").rsplit(") ", 1)[1].split()
    record["pid"] = 1
    record["starttime_ticks"] = int(fields[19])
    marker.write_text(json.dumps(record), encoding="utf-8")
    with pytest.raises(reservation.CensusUnavailable, match="retained"):
        reservation.CensusReader(queue, queue.ledger()).capture()


def test_the_census_guard_is_the_same_tmpfs_directory(
    fleet, tmpfs_state: Path, monkeypatch
):
    queue, *_ = fleet
    state = tmpfs_state / "box-state"
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", state)
    census = reservation.CensusReader(queue, queue.ledger())
    directory = os.open(state, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    guard = os.open(census.name + ".guard",
                    os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC,
                    0o600, dir_fd=directory)
    try:
        fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(reservation.CensusUnavailable, match="busy"):
            reservation.CensusReader(queue, queue.ledger()).capture()
    finally:
        os.close(guard)
        os.close(directory)

"""#1358: a valid unrelated nsfs root must not poison local record trust.

The literal net namespace row is parent-observed kernel data. All record
observations remain synthetic and exact-object bound; this is neither an
attributed worker RED yet nor evidence explaining every integrated failure.
Linux v6.14 nsfs_show_path and the parent-provided proc_ns_operations names
bound the closed vocabulary below. Mountpoints and ordinary filesystem roots
remain escaped absolute paths; opaque namespace roots apply only to nsfs.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

import stage_move  # noqa: E402
import stage_release  # noqa: E402
import test_btrfs_subvolume_records_are_retained as observations  # noqa: E402

kernel = observations.kernel
pytestmark = observations.pytestmark

NSFS_DEVICE = os.makedev(0, 5)
# Linux v6.14 fs/nsfs.c:350–357 prints ns_ops->name:[inode]. Names come
# from the pinned net/ipc/fs/kernel proc_ns_operations evidence in the report.
NSFS_KINDS = (
    "net", "pid", "pid_for_children", "time", "time_for_children", "uts",
    "ipc", "mnt", "user", "cgroup",
)
NSFS_ROW = (
    "679 34 0:5 net:[4026531833] /run/docker/netns/default rw shared:450 "
    "- nsfs nsfs rw\n")


def _with_namespace_row(kernel, row=NSFS_ROW):
    return kernel.table() + row


@pytest.mark.parametrize("direct_match", [True, False])
def test_valid_nsfs_row_does_not_disable_local_record_retention(kernel, direct_match):
    if direct_match:
        kernel.table_device = observations.OBSERVED_DEVICE
    kernel.mountinfo_text = _with_namespace_row(kernel)
    with open(observations.MOUNTINFO, "rb") as stream:
        table = stream.read()
    assert NSFS_ROW.encode() in table
    fields = NSFS_ROW.split()
    assert fields[2] == "0:5" and fields[-3:] == ["nsfs", "nsfs", "rw"]
    # The root is an opaque namespace dentry, NOT a mountpoint/path. The
    # existing path-only classifier must continue distinguishing the fields.
    assert not stage_move._mount_path_field(fields[3])
    assert stage_move._mount_path_field(fields[4])
    assert os.stat(kernel.record).st_dev == observations.OBSERVED_DEVICE
    reader = stage_release.DirectoryRecords()
    first = observations._read(reader, kernel)
    assert first == [(kernel.record, {"v": 1})]
    assert observations._counts(reader) == (1, 0, 1)
    generation = reader.generation(kernel.directory)
    second = observations._read(reader, kernel)
    assert second == first
    assert observations._counts(reader) == (1, 1, 1), (
        "a valid unrelated nsfs opaque root must not reject the complete "
        f"table or force direct/inferred local relists: {observations._counts(reader)}")
    assert second[0][1] is first[0][1]
    assert reader.generation(kernel.directory) == generation


@pytest.mark.parametrize("kind", [kind for kind in NSFS_KINDS if kind != "net"])
def test_other_proven_nsfs_kinds_preserve_local_record_trust(kernel, kind):
    row = NSFS_ROW.replace("net:[4026531833]", f"{kind}:[4026531833]")
    kernel.mountinfo_text = _with_namespace_row(kernel, row)
    reader = stage_release.DirectoryRecords()
    first = observations._read(reader, kernel)
    second = observations._read(reader, kernel)
    assert first == second == [(kernel.record, {"v": 1})]
    assert observations._counts(reader) == (1, 1, 1)
    assert stage_move._filesystem_type(NSFS_DEVICE) == "nsfs"
    assert "nsfs" not in stage_move._LOCAL_CLOCK_FILESYSTEMS


def test_nsfs_row_is_classified_but_never_local_clock_evidence(kernel):
    kernel.mountinfo_text = _with_namespace_row(kernel)
    assert stage_move._filesystem_type(NSFS_DEVICE) == "nsfs"
    assert "nsfs" not in stage_move._LOCAL_CLOCK_FILESYSTEMS
    # Exact descriptor evidence selecting that row must still refuse reuse,
    # even with settled stat fields and a separate valid btrfs row present.
    kernel.mount_id = 679
    reader = stage_release.DirectoryRecords()
    observations._read(reader, kernel)
    observations._read(reader, kernel)
    assert observations._counts(reader) == (2, 0, 2)
    assert stage_move._trusted_directory_stamp(kernel.directory) is None
    assert stage_move._current_directory_version(kernel.directory) is None
    assert stage_move._keepable_version(
        os.stat(kernel.record), kernel.now, path=kernel.record) is None


@pytest.mark.parametrize("invalid", [
    "bad-net-inode", "non-ascii-inode", "bad-net-suffix", "unknown-root",
    "unknown-namespace-kind", "non-nsfs-opaque-root",
    "opaque-mountpoint", "invalid-escaped-mountpoint", "bad-ordinary-root",
    "duplicate-mount-id",
])
def test_nsfs_exception_cannot_accept_malformed_or_ambiguous_table(kernel, invalid):
    fields = NSFS_ROW.split()
    local = kernel.table()
    if invalid == "bad-net-inode":
        fields[3] = "net:[not-a-number]"
    elif invalid == "non-ascii-inode":
        fields[3] = "net:[٤٠٢٦٥٣١٨٣٣]"
    elif invalid == "bad-net-suffix":
        fields[3] = "net:[4026531833]/extra"
    elif invalid == "unknown-root":
        fields[3] = "not-a-root"
    elif invalid == "unknown-namespace-kind":
        fields[3] = "unproven:[4026531833]"
    elif invalid == "non-nsfs-opaque-root":
        fields[-3] = "btrfs"
    elif invalid == "opaque-mountpoint":
        fields[4] = "net:[4026531833]"
    elif invalid == "invalid-escaped-mountpoint":
        fields[4] = r"/run/docker/\999/default"
    elif invalid == "bad-ordinary-root":
        local = local.replace(" /subvol ", " relative-root ", 1)
    row = " ".join(fields) + "\n"
    if invalid == "duplicate-mount-id":
        row += NSFS_ROW
    kernel.mountinfo_text = local + row
    reader = stage_release.DirectoryRecords()
    first = observations._read(reader, kernel)
    second = observations._read(reader, kernel)
    assert first == second == [(kernel.record, {"v": 1})]
    assert observations._counts(reader) == (2, 0, 2)
    assert stage_move._trusted_directory_stamp(kernel.directory) is None
    assert stage_move._current_directory_version(kernel.directory) is None


@pytest.mark.parametrize("loss", ["nfs4", "unknown", "watch-error", "namespace-change"])
def test_valid_nsfs_row_does_not_bypass_later_trust_loss(kernel, loss):
    kernel.mountinfo_text = _with_namespace_row(kernel)
    reader = stage_release.DirectoryRecords()
    observations._read(reader, kernel)
    observations._read(reader, kernel)
    assert observations._counts(reader) == (1, 1, 1)
    if loss in ("nfs4", "unknown"):
        fstype = "nfs4" if loss == "nfs4" else "fuse.fixture"
        kernel.mountinfo_text = kernel.mountinfo_text.replace(
            " - btrfs /dev/fixture ", f" - {fstype} /dev/fixture ")
        kernel.generation += 1
    elif loss == "watch-error":
        kernel.poll_error = True
    else:
        kernel.namespace_generation += 1
        kernel.mountinfo_text = kernel.mountinfo_text.replace(
            " - btrfs /dev/fixture ", " - nfs4 /dev/fixture ")
        # No POLLPRI event on the old namespace's watch.
    assert observations._read(reader, kernel) == [(kernel.record, {"v": 1})]
    assert observations._read(reader, kernel) == [(kernel.record, {"v": 1})]
    assert observations._counts(reader) == (3, 1, 3)

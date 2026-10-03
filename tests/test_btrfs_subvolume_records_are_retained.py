"""#1358: real cache consumers with synthetic Linux descriptor/mount facts.

Only exact fixture objects report st_dev 0:31; mountinfo names btrfs 0:30,
while each object's live descriptor names mount 36 in fdinfo. No resolver,
cache decision, or retention counter is replaced. These are kernel-observation
fixtures, not a dl380g10 measurement, timing claim, or local-mount ABA proof.
"""
from __future__ import annotations

import builtins
import io
import json
import os
import select
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

import pbmetrics  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

OBSERVED_DEVICE = os.makedev(0, 31)
TABLE_DEVICE = os.makedev(0, 30)
MOUNT_ID = 36
MOUNTINFO = "/proc/self/mountinfo"
NAMESPACE = "/proc/self/ns/mnt"
STAMP_NS = 1_000_000_000
FENCE_NS = STAMP_NS + 1_000_000

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Linux procfs mount identity")


class _ObservedStat:
    """Preserve actual object attributes except the declared kernel facts."""

    def __init__(self, real, *, device=None, stamp=None, inode=None):
        self._real = real
        self.st_dev = real.st_dev if device is None else device
        self.st_ino = real.st_ino if inode is None else inode
        self.st_mtime_ns = real.st_mtime_ns if stamp is None else stamp
        self.st_ctime_ns = real.st_ctime_ns if stamp is None else stamp
        self.st_mtime = self.st_mtime_ns / 10**9
        self.st_ctime = self.st_ctime_ns / 10**9

    def __getattr__(self, name):
        return getattr(self._real, name)


class _KernelFacts:
    """Serve procfs facts only for descriptors bound to exact real objects.

    Real pathname operations preserve follow/no-follow arguments. An fd must
    resolve to the exact fixture pathname AND have its real device/inode/mode
    identity before its stat or fdinfo is changed. No ancestor/prefix/sole-mount
    inference is used. Procfs byte reads use actual memfd descriptors; the
    mount-watch poll reports only declared generation changes.
    """

    def __init__(self, directory, record, monkeypatch):
        self.directory = directory
        self.record = record
        self.device = OBSERVED_DEVICE
        self.table_device = TABLE_DEVICE
        self.fstype = "btrfs"
        self.mount_id = MOUNT_ID
        self.fdinfo_text = None
        self.mountinfo_text = None
        self.mountinfo_unreadable = False
        self.fdinfo_unreadable = False
        self.descriptor_changed = False
        self.change_during_fdinfo = False
        self.directory_stamp = STAMP_NS
        self.record_stamp = STAMP_NS
        self.now = FENCE_NS
        self.generation = 0
        self.namespace_generation = 0
        self.namespace_unreadable = False
        self.poll_error = False
        self.poll_invalid = False
        self.proc_descriptors = {}
        self.real_open = os.open
        self.real_stat = os.stat
        self.real_lstat = os.lstat
        self.real_fstat = os.fstat
        self.real_readlink = os.readlink
        self.real_builtin_open = builtins.open
        self.real_io_open = io.open
        self.objects = {
            str(path): self.identity(self.real_lstat(path))
            for path in (directory, record)
        }
        self.namespace_stat = self.real_stat(NAMESPACE)
        self.namespace_link = self.real_readlink(NAMESPACE)

        monkeypatch.setattr(os, "stat", self.stat)
        monkeypatch.setattr(os, "lstat", self.lstat)
        monkeypatch.setattr(os, "fstat", self.fstat)
        monkeypatch.setattr(os, "readlink", self.readlink)
        monkeypatch.setattr(os, "open", self.open_descriptor)
        monkeypatch.setattr(builtins, "open", self.open_stream)
        monkeypatch.setattr(io, "open", self.open_io_stream)
        monkeypatch.setattr(select, "poll", self.poller)
        real_clock = time.clock_gettime_ns

        def clock(clock_id):
            if clock_id == stage_move._COARSE_REALTIME:
                return self.now
            return real_clock(clock_id)

        monkeypatch.setattr(time, "clock_gettime_ns", clock)
        # Reset existing process-local state, not its implementation/verdict.
        monkeypatch.setattr(stage_move, "_filesystem_types", {})
        monkeypatch.setattr(stage_move, "_mount_watch", None)

    @staticmethod
    def identity(info):
        return info.st_dev, info.st_ino, info.st_mode

    def object_for_descriptor(self, descriptor):
        try:
            path = self.real_readlink(f"/proc/self/fd/{descriptor}")
            info = self.real_fstat(descriptor)
        except OSError:
            return None
        if self.objects.get(path) == self.identity(info):
            return path
        return None

    def observe(self, path, info):
        if path == NAMESPACE:
            return _ObservedStat(
                info, inode=self.namespace_stat.st_ino + self.namespace_generation)
        if self.objects.get(path) != self.identity(info):
            return info
        stamp = (self.directory_stamp if path == str(self.directory)
                 else self.record_stamp)
        return _ObservedStat(info, device=self.device, stamp=stamp)

    def stat(self, target, *args, **kwargs):
        if (not isinstance(target, int) and os.fsdecode(target) == NAMESPACE
                and self.namespace_unreadable):
            raise PermissionError("fixture namespace is unreadable")
        info = self.real_stat(target, *args, **kwargs)
        if isinstance(target, int):
            return self.fstat(target)
        return self.observe(os.fsdecode(target), info)

    def lstat(self, target, *args, **kwargs):
        info = self.real_lstat(target, *args, **kwargs)
        return self.observe(os.fsdecode(target), info)

    def fstat(self, descriptor):
        info = self.real_fstat(descriptor)
        path = self.object_for_descriptor(descriptor)
        if path is not None:
            stamp = (self.directory_stamp if path == str(self.directory)
                     else self.record_stamp)
            return _ObservedStat(
                info, device=self.device, stamp=stamp,
                inode=info.st_ino + int(self.descriptor_changed))
        if (self.real_readlink(f"/proc/self/fd/{descriptor}") == self.namespace_link
                and self.identity(info) == self.identity(self.namespace_stat)):
            return self.observe(NAMESPACE, info)
        return info

    def readlink(self, target, *args, **kwargs):
        if os.fsdecode(target) == NAMESPACE:
            if self.namespace_generation:
                return f"mnt:[{self.namespace_stat.st_ino + self.namespace_generation}]"
            return self.namespace_link
        return self.real_readlink(target, *args, **kwargs)

    def table(self):
        if self.mountinfo_text is not None:
            return self.mountinfo_text
        device = self.table_device
        # A second, nonlocal row prevents trust from a sole btrfs mount or
        # inode. mnt_id, not a device-number adjustment, selects the row.
        return (
            f"36 25 {os.major(device)}:{os.minor(device)} /subvol "
            f"{self.directory} rw,relatime - {self.fstype} /dev/fixture rw\n"
            "77 25 0:80 / /unrelated rw - nfs4 server:/export rw\n")

    def proc_text(self, target):
        if isinstance(target, int):
            return None
        path = os.fsdecode(target)
        if path == MOUNTINFO:
            if self.mountinfo_unreadable:
                raise PermissionError("fixture mountinfo is unreadable")
            return self.table()
        prefix = "/proc/self/fdinfo/"
        if path.startswith(prefix):
            try:
                descriptor = int(path[len(prefix):])
            except ValueError:
                return None
            obj = self.object_for_descriptor(descriptor)
            if obj is None:
                return None
            if self.fdinfo_unreadable:
                raise PermissionError("fixture fdinfo is unreadable")
            if self.fdinfo_text is not None:
                return self.fdinfo_text
            # Preserve this actual descriptor's flags, position and inode;
            # only the declared mount-id observation differs from the host.
            with self.real_builtin_open(path) as stream:
                rows = stream.read().splitlines()
            text = "".join(
                f"mnt_id:\t{self.mount_id}\n" if row.startswith("mnt_id:")
                else row + "\n" for row in rows)
            if self.change_during_fdinfo:
                self.change_during_fdinfo = False
                self.fstype = "nfs4"
                self.generation += 1
            return text
        return None

    def open_stream(self, target, mode="r", *args, **kwargs):
        text = self.proc_text(target)
        if text is not None:
            return io.BytesIO(text.encode()) if "b" in mode else io.StringIO(text)
        return self.real_builtin_open(target, mode, *args, **kwargs)

    def open_io_stream(self, target, mode="r", *args, **kwargs):
        text = self.proc_text(target)
        if text is not None:
            return io.BytesIO(text.encode()) if "b" in mode else io.StringIO(text)
        return self.real_io_open(target, mode, *args, **kwargs)

    def open_descriptor(self, target, flags, *args, **kwargs):
        text = self.proc_text(target)
        if text is None:
            return self.real_open(target, flags, *args, **kwargs)
        descriptor = os.memfd_create("pb1358-proc-observation", os.MFD_CLOEXEC)
        os.write(descriptor, text.encode())
        os.lseek(descriptor, 0, os.SEEK_SET)
        self.proc_descriptors[descriptor] = self.identity(self.real_fstat(descriptor))
        return descriptor

    def poller(self):
        facts = self

        class MountPoll:
            def __init__(self):
                self.descriptor = None
                self.seen = facts.generation

            def register(self, descriptor, mask):
                assert descriptor in facts.proc_descriptors
                assert mask == select.POLLPRI | select.POLLERR
                self.descriptor = descriptor

            def poll(self, timeout):
                assert timeout == 0
                if facts.poll_error:
                    raise OSError("fixture mount watch is unreadable")
                if facts.poll_invalid:
                    return [(self.descriptor, select.POLLNVAL)]
                if self.seen != facts.generation:
                    self.seen = facts.generation
                    return [(self.descriptor, select.POLLPRI)]
                return []

        return MountPoll()

    def close(self):
        for descriptor, identity in self.proc_descriptors.items():
            try:
                if self.identity(self.real_fstat(descriptor)) == identity:
                    os.close(descriptor)
            except OSError:
                pass  # A bounded procfs reader may already have closed it.


@pytest.fixture
def kernel(tmp_path, monkeypatch):
    directory = tmp_path / "ready"
    directory.mkdir()
    record = directory / "r.json"
    record.write_bytes(b'{"v": 1}')
    facts = _KernelFacts(directory, record, monkeypatch)
    try:
        yield facts
    finally:
        facts.close()


def _select(entry):
    return entry.name.endswith(".json")


def _parse(path):
    return json.loads(path.read_bytes())


def _read(reader, kernel, **kwargs):
    return reader.read(kernel.directory, select=_select, parse=_parse, **kwargs)


def _counts(reader):
    return reader.listed, reader.kept, reader.parsed


def test_directory_records_retains_unlisted_btrfs_subvolume(kernel):
    # Witness the fixture facts on an actual no-follow directory descriptor,
    # independently of how the production resolver obtains its evidence.
    descriptor = os.open(kernel.directory,
                         os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        assert os.fstat(descriptor).st_dev == OBSERVED_DEVICE
        assert os.fstat(descriptor).st_ino == os.lstat(kernel.directory).st_ino
        with open(f"/proc/self/fdinfo/{descriptor}") as stream:
            assert "mnt_id:\t36\n" in stream.read()
        with open(MOUNTINFO) as stream:
            rows = stream.read().splitlines()
        assert rows[0].split()[2] == "0:30"
        assert all(row.split()[2] != "0:31" for row in rows)
    finally:
        os.close(descriptor)
    reader = stage_release.DirectoryRecords()
    first = _read(reader, kernel)
    assert first == [(kernel.record, {"v": 1})]
    assert _counts(reader) == (1, 0, 1)
    generation = reader.generation(kernel.directory)
    second = _read(reader, kernel)
    assert second == first
    assert _counts(reader) == (1, 1, 1), (
        "same settled object on descriptor-bound btrfs must retain its parse "
        f"and listing, not zero-kept/relist: {_counts(reader)}")
    assert second[0][1] is first[0][1]
    assert reader.generation(kernel.directory) == generation


def test_census_level_compares_unlisted_btrfs_directory(kernel):
    """Real census acquisition and post-listing/reuse comparison, no seeding."""
    index = stage_release.CensusIndex()
    for _pass in range(2):
        fragments, tainted = [], []
        stage_release._census_level(
            kernel.directory, direct=True, allow_nested=False,
            fragments=fragments, tainted=tainted, memo=index)
        assert fragments == []  # r.json is not a fragment namespace.
        assert tainted == []
    assert (index.listed, index.kept) == (1, 1), (
        "census must compare the same trusted stamp it acquired; "
        f"got listed={index.listed}, kept={index.kept}")


def test_record_version_is_retained_when_directory_stamp_is_in_its_tick(kernel):
    """Isolate file keepability: neither pass may keep the directory listing."""
    kernel.directory_stamp = kernel.now
    reader = stage_release.DirectoryRecords()
    first = _read(reader, kernel)
    second = _read(reader, kernel)
    assert first == second == [(kernel.record, {"v": 1})]
    assert _counts(reader) == (2, 0, 1), (
        "tick-refused directory must relist but retain the older file parse; "
        f"got {_counts(reader)}")
    assert second[0][1] is first[0][1]


def test_direct_device_match_is_a_positive_fixture_control(kernel):
    kernel.table_device = OBSERVED_DEVICE
    reader = stage_release.DirectoryRecords()
    _read(reader, kernel)
    _read(reader, kernel)
    assert _counts(reader) == (1, 1, 1)


@pytest.mark.parametrize("refusal", [
    "nfs4", "unknown", "wrong-mount", "missing-mount", "missing-id",
    "malformed-id", "duplicate-id", "malformed-table", "duplicate-mount",
    "unreadable-table",
    "unreadable-fdinfo", "changed-descriptor", "mount-change-during-resolution",
])
def test_inference_refusals_do_not_keep_records(kernel, refusal):
    """Old-compatible negatives; each challenges a guessed inferred positive."""
    if refusal == "nfs4":
        kernel.fstype = "nfs4"
    elif refusal == "unknown":
        kernel.fstype = "fuse.fixture"
    elif refusal == "wrong-mount":
        kernel.mount_id = 77  # Exact descriptor selects NFS despite btrfs row.
    elif refusal == "missing-mount":
        kernel.mount_id = 999
    elif refusal == "missing-id":
        kernel.fdinfo_text = "pos:\t0\nflags:\t010000000\n"
    elif refusal == "malformed-id":
        kernel.fdinfo_text = "mnt_id:\tnot-a-number\n"
    elif refusal == "duplicate-id":
        kernel.fdinfo_text = "mnt_id:\t36\nmnt_id:\t77\n"
    elif refusal == "malformed-table":
        kernel.mountinfo_text = "36 25 0:30 /subvol /fixture rw\n"
    elif refusal == "duplicate-mount":
        kernel.mountinfo_text = (
            kernel.table() + "36 25 0:81 / /other rw - nfs4 server:/other rw\n")
    elif refusal == "unreadable-table":
        kernel.mountinfo_unreadable = True
    elif refusal == "unreadable-fdinfo":
        kernel.fdinfo_unreadable = True
    elif refusal == "changed-descriptor":
        kernel.descriptor_changed = True
    elif refusal == "mount-change-during-resolution":
        kernel.change_during_fdinfo = True
    reader = stage_release.DirectoryRecords()
    first = _read(reader, kernel)
    second = _read(reader, kernel)
    assert first == second == [(kernel.record, {"v": 1})]
    assert _counts(reader) == (2, 0, 2)
    assert stage_move._trusted_directory_stamp(kernel.directory) is None
    assert stage_move._current_directory_version(kernel.directory) is None


@pytest.mark.parametrize("fstype", ["nfs4", "fuse.fixture"])
def test_direct_nonlocal_device_never_keeps_records(kernel, fstype):
    kernel.table_device = OBSERVED_DEVICE
    kernel.fstype = fstype
    reader = stage_release.DirectoryRecords()
    _read(reader, kernel)
    _read(reader, kernel)
    assert _counts(reader) == (2, 0, 2)


@pytest.mark.parametrize("fstype", ["nfs4", "fuse.fixture"])
def test_mount_watch_trust_loss_drops_inferred_listing_and_parse(kernel, fstype):
    reader = stage_release.DirectoryRecords()
    _read(reader, kernel)
    _read(reader, kernel)
    assert _counts(reader) == (1, 1, 1)
    kernel.fstype = fstype
    kernel.generation += 1
    for _pass in range(2):
        assert _read(reader, kernel) == [(kernel.record, {"v": 1})]
    assert _counts(reader) == (3, 1, 3), (
        "a watch event and trust loss must void inferred parse evidence too")


def test_namespace_change_drops_inferred_listing_and_parse(kernel):
    reader = stage_release.DirectoryRecords()
    _read(reader, kernel)
    _read(reader, kernel)
    assert _counts(reader) == (1, 1, 1)
    kernel.namespace_generation += 1
    kernel.fstype = "nfs4"
    # No POLLPRI on the old namespace's watch: namespace identity must fence it.
    assert _read(reader, kernel) == [(kernel.record, {"v": 1})]
    assert _counts(reader) == (2, 1, 2)


@pytest.mark.parametrize("direct_match", [True, False])
def test_same_tick_record_is_not_kept(kernel, direct_match):
    if direct_match:
        kernel.table_device = OBSERVED_DEVICE
    kernel.record_stamp = kernel.now
    reader = stage_release.DirectoryRecords()
    _read(reader, kernel)
    # Model a same-size replacement reproducing every observed version field;
    # no pathname/object identity is silently substituted by the fixture.
    kernel.record.write_bytes(b'{"v": 2}')
    assert _read(reader, kernel) == [(kernel.record, {"v": 2})]
    assert _counts(reader) == (2, 0, 2)


def test_symlink_is_not_a_trusted_directory(kernel):
    link = kernel.directory.parent / "link"
    link.symlink_to(kernel.directory, target_is_directory=True)
    assert stage_move._trusted_directory_stamp(link) is None
    assert stage_move._current_directory_version(link) is None
    assert stage_move._trusted_directory_stamp(kernel.record) is None
    assert stage_move._current_directory_version(kernel.record) is None


def test_parse_refusal_never_becomes_a_kept_listing(kernel):
    kernel.table_device = OBSERVED_DEVICE
    reader = stage_release.DirectoryRecords()
    _read(reader, kernel, keep=lambda _record: False)
    _read(reader, kernel, keep=lambda _record: False)
    assert _counts(reader) == (2, 0, 2)


def test_parse_error_is_not_retained(kernel):
    kernel.table_device = OBSERVED_DEVICE
    reader = stage_release.DirectoryRecords()

    def unreadable(_path):
        raise PermissionError("record parse refused")

    for _pass in range(2):
        with pytest.raises(PermissionError, match="record parse refused"):
            reader.read(kernel.directory, select=_select, parse=unreadable)
    assert _counts(reader) == (2, 0, 0)


@pytest.mark.parametrize("record", [
    "oversized-table", "oversized-fdinfo", "truncated-table", "truncated-fdinfo",
    "bad-device", "bad-path-escape",
])
def test_bounded_complete_proc_records_are_required(kernel, record):
    if record == "oversized-table":
        kernel.mountinfo_text = kernel.table() + " " * (1024 * 1024) + "\n"
    elif record == "oversized-fdinfo":
        kernel.fdinfo_text = "mnt_id:\t36\npadding:\t" + "x" * 4096 + "\n"
    elif record == "truncated-table":
        kernel.mountinfo_text = kernel.table().rstrip("\n")
    elif record == "truncated-fdinfo":
        kernel.fdinfo_text = "mnt_id:\t36"
    elif record == "bad-device":
        kernel.mountinfo_text = kernel.table().replace("0:30", "0:not-a-number")
    elif record == "bad-path-escape":
        kernel.mountinfo_text = kernel.table().replace("/subvol", r"/bad\999")
    reader = stage_release.DirectoryRecords()
    _read(reader, kernel)
    _read(reader, kernel)
    assert _counts(reader) == (2, 0, 2)


@pytest.mark.parametrize("direct_match", [True, False])
def test_consistent_bind_rows_do_not_create_ambiguity(kernel, direct_match):
    if direct_match:
        kernel.table_device = OBSERVED_DEVICE
    kernel.mountinfo_text = (
        kernel.table() +
        f"78 25 0:{31 if direct_match else 30} /subvol /bind rw - btrfs /dev/fixture rw\n")
    reader = stage_release.DirectoryRecords()
    _read(reader, kernel)
    _read(reader, kernel)
    assert _counts(reader) == (1, 1, 1)


def test_contradictory_device_types_cannot_fall_back_to_local(kernel):
    kernel.table_device = OBSERVED_DEVICE
    kernel.mountinfo_text = (
        kernel.table() + "78 25 0:31 / /other rw - nfs4 server:/other rw\n")
    reader = stage_release.DirectoryRecords()
    _read(reader, kernel)
    _read(reader, kernel)
    assert _counts(reader) == (2, 0, 2)


def test_file_fallback_requires_matching_live_descriptor_or_path(kernel):
    info = os.stat(kernel.record)
    assert stage_move._keepable_version(info, kernel.now) is None
    assert stage_move._keepable_version(info, kernel.now, path=kernel.directory) is None
    descriptor = os.open(kernel.record, os.O_PATH | os.O_CLOEXEC)
    wrong = os.open(kernel.directory, os.O_PATH | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        version = stage_move._metadata_version(info)
        assert stage_move._keepable_version(
            info, kernel.now, descriptor=descriptor) == version
        assert stage_move._keepable_version(
            info, kernel.now, descriptor=wrong) is None
        assert stage_move._keepable_version(
            info, kernel.now, descriptor=descriptor, path=kernel.directory) is None
        assert os.fstat(descriptor).st_ino == info.st_ino  # caller still owns FD
    finally:
        os.close(descriptor)
        os.close(wrong)


def test_inferred_directory_type_is_not_an_alias_for_another_descriptor(kernel):
    assert stage_move._trusted_directory_stamp(kernel.directory) is not None
    # The next exact descriptor selects the nonlocal row of the same table.
    kernel.mount_id = 77
    info = os.stat(kernel.record)
    assert stage_move._keepable_version(info, kernel.now, path=kernel.record) is None


def test_listing_local_boolean_cannot_survive_a_watch_trust_loss(kernel):
    kernel.table_device = OBSERVED_DEVICE
    local = {OBSERVED_DEVICE: True}
    info = os.stat(kernel.record)
    assert stage_move._keepable_version(
        info, kernel.now, local=local, path=kernel.record) is not None
    kernel.fstype = "nfs4"
    kernel.generation += 1
    assert stage_move._keepable_version(
        info, kernel.now, local=local, path=kernel.record) is None


@pytest.mark.parametrize("direct_match", [True, False])
@pytest.mark.parametrize("failure", ["poll-error", "poll-invalid", "namespace-unreadable"])
def test_lost_watch_or_namespace_evidence_drops_warm_records(kernel, direct_match, failure):
    if direct_match:
        kernel.table_device = OBSERVED_DEVICE
    reader = stage_release.DirectoryRecords()
    _read(reader, kernel)
    _read(reader, kernel)
    assert _counts(reader) == (1, 1, 1)
    if failure == "poll-error":
        kernel.poll_error = True
    elif failure == "poll-invalid":
        kernel.poll_invalid = True
    else:
        kernel.namespace_unreadable = True
    _read(reader, kernel)
    _read(reader, kernel)
    assert _counts(reader) == (3, 1, 3)


def _fragment_document(kernel, digest):
    return {
        "schema": stage_release.residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": "a" * 64,
        "mover_action_key": "b" * 64,
        "tier_id": "fixture-stage",
        "stage_root": str(kernel.directory),
        "manifest_sha256": digest * 64,
        "entries": {},
    }


def test_fragment_memo_keeps_live_fd_evidence_and_rechecks_trust(kernel):
    # Real schema validator, real record reader, real memo; no validator mock.
    kernel.record.write_text(json.dumps(_fragment_document(kernel, "c")))
    memo = stage_release._CensusMemo()
    first = stage_release._read_fragment(kernel.record, memo)
    second = stage_release._read_fragment(kernel.record, memo)
    assert isinstance(first, dict)
    assert second is first
    assert memo.counts() == (1, 1)
    assert memo.version_of(first) is not None
    kernel.fstype = "nfs4"
    kernel.generation += 1
    # Changed bytes behind the same synthetic observed version must be read
    # after loss of trust, rather than served by the old fstat memo hit.
    kernel.record.write_text(json.dumps(_fragment_document(kernel, "d")))
    third = stage_release._read_fragment(kernel.record, memo)
    assert isinstance(third, dict)
    assert third["manifest_sha256"] == "d" * 64
    assert memo.counts() == (2, 1)
    assert memo.version_of(third) is None


def test_metrics_history_retains_exact_file_evidence_after_relisting(kernel):
    reader = pbmetrics.KeptReads()

    def scrape():
        reader.begin()
        out = list(reader.history(kernel.directory, select=_select))
        reader.end()
        return out

    first = scrape()
    assert len(first) == 1
    assert first[0].version is not None
    assert reader.scrape_counts()["listed"] == 1
    kernel.directory_stamp += 1  # still settled, but the listing must move
    second = scrape()
    assert reader.scrape_counts()["listed"] == 1
    assert reader.scrape_counts()["kept"] == 0
    assert second[0] is first[0], "relisted unchanged file must retain its history entry"

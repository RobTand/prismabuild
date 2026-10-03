"""#1358 review regressions against the current shared resolver repair.

Real checkpoint installation/coherency consumers and real resolver threads;
only exact-object kernel observations are controlled. No production changes,
cache verdict mocks, performance claims or affected-host evidence live here.
"""
from __future__ import annotations

import json
import os
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

import stage_move  # noqa: E402
import stage_release  # noqa: E402
import test_btrfs_subvolume_records_are_retained as observations  # noqa: E402
import test_stale_material_done_owner_retires as owner  # noqa: E402

# Re-export the existing supported queue fixture, without calling its body.
fleet = owner.fleet
pytestmark = observations.pytestmark

DIRECT_DEVICE = os.makedev(0, 90)
DIRECTORY_DEVICE = os.makedev(0, 41)
WAIT_S = 5
LOSSES = ("file-nfs4", "file-unknown", "dir-nfs4", "dir-unknown",
          "watch-error", "namespace-change")


class _CheckpointKernelFacts(observations._KernelFacts):
    """Distinct file/dir mounts, each bound to explicitly registered objects.

    Payload files are NOT registered: their actual sidecar-qualified identities
    and bytes remain unchanged. No prefix/ancestor classification occurs.
    """

    def __init__(self, directory, document_root, fragment, material, parents, monkeypatch):
        self.document_root = document_root
        self.devices = {}
        self.mount_ids = {}
        self.file_type = "btrfs"
        self.directory_type = "btrfs"
        super().__init__(directory, fragment, monkeypatch)
        for path in (fragment, material):
            self.register(path, observations.OBSERVED_DEVICE, 36)
        for path in {directory, *parents}:
            self.register(path, DIRECTORY_DEVICE, 46)

    def register(self, path, device, mount_id):
        name = str(path)
        self.objects[name] = self.identity(self.real_lstat(path))
        self.devices[name] = device
        self.mount_ids[name] = mount_id

    def observe(self, path, info):
        observed = super().observe(path, info)
        if (path in self.devices
                and self.objects.get(path) == self.identity(info)):
            return observations._ObservedStat(observed, device=self.devices[path])
        return observed

    def stat(self, target, *args, **kwargs):
        if (isinstance(target, int) or os.path.isabs(os.fsdecode(target))
                or kwargs.get("dir_fd") is None):
            return super().stat(target, *args, **kwargs)
        # core's no-follow reader stats the leaf relative to its held parent.
        # Keep the real syscall/follow policy, then bind ONLY an exact object.
        info = self.real_stat(target, *args, **kwargs)
        try:
            parent = self.real_readlink(f"/proc/self/fd/{kwargs['dir_fd']}")
        except OSError:
            return info
        if not os.path.isabs(parent) or parent.endswith(" (deleted)"):
            return info
        path = os.path.normpath(os.path.join(parent, os.fsdecode(target)))
        if self.objects.get(path) != self.identity(info):
            return info
        return self.observe(path, info)

    def fstat(self, descriptor):
        observed = super().fstat(descriptor)
        path = self.object_for_descriptor(descriptor)
        if path in self.devices:
            return observations._ObservedStat(observed, device=self.devices[path])
        return observed

    def table(self):
        return (
            f"36 25 0:30 /subvol {self.document_root} rw - {self.file_type} /dev/files rw\n"
            f"46 25 0:40 /subvol {self.directory} rw - {self.directory_type} /dev/dirs rw\n"
            "77 25 0:80 / /unrelated rw - nfs4 server:/export rw\n")

    def proc_text(self, target):
        text = super().proc_text(target)
        if (text is not None and not isinstance(target, int)
                and os.fsdecode(target).startswith("/proc/self/fdinfo/")):
            descriptor = int(os.fsdecode(target).rsplit("/", 1)[1])
            path = self.object_for_descriptor(descriptor)
            if path in self.mount_ids:
                return "".join(
                    f"mnt_id:\t{self.mount_ids[path]}\n" if row.startswith("mnt_id:")
                    else row + "\n" for row in text.splitlines())
        return text

    def lose_trust(self, loss):
        if loss == "file-nfs4":
            self.file_type = "nfs4"
            self.generation += 1
        elif loss == "file-unknown":
            self.file_type = "fuse.fixture"
            self.generation += 1
        elif loss == "dir-nfs4":
            self.directory_type = "nfs4"
            self.generation += 1
        elif loss == "dir-unknown":
            self.directory_type = "fuse.fixture"
            self.generation += 1
        elif loss == "watch-error":
            self.poll_error = True
        elif loss == "namespace-change":
            # The old namespace's watch gets no event; the new namespace
            # exposes nonlocal rows despite identical object stat fields.
            self.namespace_generation += 1
            self.file_type = self.directory_type = "nfs4"
        else:
            raise AssertionError(loss)


@pytest.fixture
def warm_checkpoint(fleet, monkeypatch):
    queue, stage, _cas = fleet
    # Supported queue/fragment/material writers and real terminal history.
    consumer, mover = owner.stale_owner(
        fleet, coherent_names=set(owner.NAMES), replace=False)
    root = queue.residency_fragment_root()
    fragment = stage_release.residency_map.fragment_path(root, consumer, mover)
    material = stage_release.reader_lease.material_path(root, consumer, mover)
    parents = {str((stage / owner.staged_name(name)).parent) for name in owner.NAMES}
    facts = _CheckpointKernelFacts(
        stage, root, fragment, material, {Path(name) for name in parents}, monkeypatch)
    index = stage_release.CensusIndex()
    key = stage_release._skip_checkpoint_key(queue, root, stage, owner.TIER,
                                             consumer, mover)
    stage_release.reset_skip_checkpoints()

    def sweep():
        return stage_release.sweep_dead_owner_fragments(
            queue, stage_roots={owner.TIER: str(stage)},
            residency_root=root, index=index)

    try:
        first = [receipt for receipt in sweep()
                 if receipt.get("event") == stage_release.STALE_MENTION_EVENT
                 and receipt.get("action_key") == mover]
        assert len(first) == 1
        assert first[0]["cacheable"] is True, json.dumps(first[0], sort_keys=True)
        assert first[0]["entries_pruned"] == first[0]["entries_unlinked"] == 0
        assert stage_release._skip_checkpoint_hit(key, fragment, material)
        assert sweep() == [], "a real inferred checkpoint must warm before trust loss"
        assert (index.stale_skipped, index.stale_censused) == (1, 1)
        files = {str(path): stage_release._path_version(path)
                 for path in (fragment, material)}
        directories = {name: stage_release._directory_version(name) for name in parents}
        assert all(version is not None for version in (*files.values(), *directories.values()))
        yield {
            "kernel": facts, "key": key, "fragment": fragment, "material": material,
            "files": files, "directories": directories, "index": index,
            "sweep": sweep, "mover": mover, "stage": stage,
        }
    finally:
        stage_release.reset_skip_checkpoints()
        facts.close()


def _observe_trust_loss(warm, loss):
    warm["kernel"].lose_trust(loss)
    # Bare versions have not changed; the refusal must be about current trust.
    for path, version in warm["files"].items():
        assert stage_release._path_version(path) == version
        bare, trusted = stage_release._fenced_path_version(path, stage_move._version_fence())
        assert bare == version
        if loss.startswith("file-") or loss in ("watch-error", "namespace-change"):
            assert trusted is None
        else:
            assert trusted == version  # dir-only loss leaves file proof intact
    for path, version in warm["directories"].items():
        assert stage_release._directory_version(path) == version
        current = stage_move._current_directory_version(Path(path))
        if loss.startswith("dir-") or loss in ("watch-error", "namespace-change"):
            assert current is None
        else:
            assert current == version  # file-only loss leaves directory proof intact


@pytest.mark.parametrize("loss", LOSSES)
def test_inferred_skip_checkpoint_refuses_current_trust_loss(warm_checkpoint, loss):
    warm = warm_checkpoint
    _observe_trust_loss(warm, loss)
    assert not stage_release._skip_checkpoint_hit(
        warm["key"], warm["fragment"], warm["material"]), (
        "unchanged bare file/directory versions cannot preserve an inferred "
        f"checkpoint after {loss}")
    assert stage_release.skip_checkpoint_usage()["checkpoints"] == 0


@pytest.mark.parametrize("loss", LOSSES)
def test_dead_owner_coherency_rereads_after_inferred_checkpoint_trust_loss(
        warm_checkpoint, monkeypatch, loss):
    warm = warm_checkpoint
    real_prune = stage_release.prune_stale_mentions
    rescans = []

    def observed_prune(*args, **kwargs):
        rescans.append(args[1])
        return real_prune(*args, **kwargs)  # observe, never replace the verdict

    monkeypatch.setattr(stage_release, "prune_stale_mentions", observed_prune)
    _observe_trust_loss(warm, loss)
    receipts = warm["sweep"]()
    assert rescans == [warm["mover"]], (
        "real dead-owner coherency consumer skipped its prune/rescan after "
        f"{loss}; stale_skipped={warm['index'].stale_skipped}, "
        f"stale_censused={warm['index'].stale_censused}")
    assert (warm["index"].stale_skipped, warm["index"].stale_censused) == (1, 2)
    found = [receipt for receipt in receipts
             if receipt.get("event") == stage_release.STALE_MENTION_EVENT
             and receipt.get("action_key") == warm["mover"]]
    assert len(found) == 1
    assert found[0]["cacheable"] is False, found[0]
    assert found[0]["entries_pruned"] == found[0]["entries_unlinked"] == 0
    assert all((warm["stage"] / owner.staged_name(name)).exists() for name in owner.NAMES)


@pytest.fixture
def kernel(tmp_path, monkeypatch):
    directory = tmp_path / "ready"
    directory.mkdir()
    record = directory / "r.json"
    record.write_bytes(b'{"v": 1}')
    facts = observations._KernelFacts(directory, record, monkeypatch)
    try:
        yield facts
    finally:
        facts.close()


def test_paused_exact_fdinfo_io_does_not_block_unrelated_direct_lookup(kernel, monkeypatch):
    kernel.mountinfo_text = (
        kernel.table() + "78 25 0:90 / /scratch rw - tmpfs fixture rw\n")
    # Warm validated direct evidence before any paused object IO.
    assert stage_move._filesystem_type(DIRECT_DEVICE) == "tmpfs"
    info = os.stat(kernel.record)
    entered = threading.Event()
    release = threading.Event()
    direct_started = threading.Event()
    direct_done = threading.Event()
    results = {}
    errors = []
    real_proc_text = kernel.proc_text

    def paused_fdinfo(target):
        if (not isinstance(target, int)
                and os.fsdecode(target).startswith("/proc/self/fdinfo/")):
            descriptor = int(os.fsdecode(target).rsplit("/", 1)[1])
            if kernel.object_for_descriptor(descriptor) == str(kernel.record):
                entered.set()
                if not release.wait(4 * WAIT_S):
                    raise AssertionError("fixture fdinfo release was not delivered")
        return real_proc_text(target)

    monkeypatch.setattr(kernel, "proc_text", paused_fdinfo)

    def fallback():
        try:
            results["fallback"] = stage_move._keepable_version(
                info, kernel.now, path=kernel.record)
        except BaseException as exc:
            errors.append(exc)

    def direct():
        direct_started.set()
        try:
            results["direct"] = stage_move._filesystem_type(DIRECT_DEVICE)
        except BaseException as exc:
            errors.append(exc)
        finally:
            direct_done.set()

    fallback_thread = threading.Thread(target=fallback, name="pb1358-paused-fdinfo")
    direct_thread = threading.Thread(target=direct, name="pb1358-direct-type")
    try:
        fallback_thread.start()
        assert entered.wait(WAIT_S), "exact-object fallback did not reach fdinfo IO"
        direct_thread.start()
        assert direct_started.wait(WAIT_S)
        assert direct_done.wait(WAIT_S), (
            "unrelated known-direct lookup waited for paused exact-object IO")
        assert not release.is_set(), "direct progress must precede fdinfo release"
        assert results["direct"] == "tmpfs"
    finally:
        release.set()
        if fallback_thread.ident is not None:
            fallback_thread.join(WAIT_S)
        if direct_thread.ident is not None:
            direct_thread.join(WAIT_S)
        assert not fallback_thread.is_alive(), "fallback thread survived released IO"
        assert not direct_thread.is_alive(), "direct thread survived released IO"
    assert not errors, errors
    assert results["fallback"] == stage_move._metadata_version(info)


@pytest.mark.parametrize("direct_match", [True, False])
def test_valid_unrelated_hyphen_mount_source_does_not_poison_local_retention(
        kernel, direct_match):
    if direct_match:
        kernel.table_device = observations.OBSERVED_DEVICE
    kernel.mountinfo_text = (
        kernel.table() + "78 25 0:90 / /scratch rw - tmpfs - rw\n")
    reader = stage_release.DirectoryRecords()
    first = observations._read(reader, kernel)
    second = observations._read(reader, kernel)
    assert first == second == [(kernel.record, {"v": 1})]
    assert observations._counts(reader) == (1, 1, 1), (
        "a valid unrelated source '-' must not poison direct/inferred local "
        f"retention: {observations._counts(reader)}")

"""Repeated seals reuse verified bytes without changing snapshot identity."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
import json
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import threading
import shutil

import pytest

from prismabuild import core, sealed_bundle_cache as cache_module, submission
from test_pbrun_bundle_determinism import _action_key, _repository


_AMPLE_VFS = (4096, 4096, 1 << 30, 1 << 29, 1 << 29, 1000000, 900000, 900000, 0, 255)


@pytest.fixture(autouse=True)
def ample_cache_space(monkeypatch):
    # Space policy has its own injected boundary cases below.
    monkeypatch.setattr(cache_module.os, "statvfs", lambda path: os.statvfs_result(_AMPLE_VFS))


@pytest.fixture
def sealing(tmp_path, monkeypatch):
    checkout = _repository(tmp_path / "checkout", files=24)
    cache = tmp_path / "bundle-cache"
    monkeypatch.setenv("PRISMABUILD_BUNDLE_CACHE_ROOT", str(cache))
    packs = []
    native = subprocess.run

    def observed(args, **kwargs):
        if "pack-objects" in args:
            packs.append(tuple(args))
        return native(args, **kwargs)

    monkeypatch.setattr(subprocess, "run", observed)
    cas = core.PrismaBuildCAS(tmp_path / "cas")

    def seal(**kwargs):
        snapshot = submission.snapshot_checkout(checkout, cas=cas, **kwargs)
        return snapshot, cas.input_path(snapshot["input"]).read_bytes()

    return checkout, cache, packs, seal


def test_repeated_submission_reuses_identical_bundle_and_action(sealing):
    checkout, cache, packs, seal = sealing
    first, first_bytes = seal()
    second, second_bytes = seal()
    assert first == second
    assert first_bytes == second_bytes
    assert _action_key(first, checkout) == _action_key(second, checkout)
    assert len(packs) == 1


def test_cached_bytes_equal_an_independent_fresh_pack(sealing, monkeypatch):
    _, _, packs, seal = sealing
    first, expected = seal(stamp_name="closure", stamp_payload="same input")
    assert seal(stamp_name="closure", stamp_payload="same input")[1] == expected
    monkeypatch.setenv("PRISMABUILD_BUNDLE_CACHE_ROOT", "off")
    fresh, actual = seal(stamp_name="closure", stamp_payload="same input")
    assert actual == expected
    assert fresh == first
    assert len(packs) == 2


@pytest.mark.parametrize("component", (
    "ref-name", "tip", "stamp-name", "stamp-payload", "pack-config", "pack-options", "git-version", "inherited-config", "git-environment",
))
def test_every_seal_input_invalidates_the_cache(sealing, monkeypatch, component):
    checkout, _, packs, seal = sealing
    kwargs = {"stamp_name": "closure", "stamp_payload": "first"}
    seal(**kwargs)
    if component == "ref-name":
        # pb-history-ok: Creates a ref at HEAD in the generated temporary repository.
        subprocess.run(["git", "-C", str(checkout), "branch", "extra", "HEAD"], check=True)
        kwargs["snapshot_refs"] = ["extra"]
    elif component == "tip":
        (checkout / "f00000.txt").write_text("different object closure\n")
    elif component == "stamp-name":
        kwargs["stamp_name"] = "other-closure"
    elif component == "stamp-payload":
        kwargs["stamp_payload"] = "second"
    elif component == "pack-config":
        monkeypatch.setattr(submission, "BUNDLE_PACK_CONFIGURATION", (
            *submission.BUNDLE_PACK_CONFIGURATION, ("pack.window", "0")))
    elif component == "pack-options":
        monkeypatch.setattr(submission, "BUNDLE_PACK_OPTIONS", (
            *submission.BUNDLE_PACK_OPTIONS, "--compression=5"))
    elif component == "inherited-config":
        monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
        monkeypatch.setenv("GIT_CONFIG_KEY_0", "pack.deltaCacheSize")
        monkeypatch.setenv("GIT_CONFIG_VALUE_0", "0")
    elif component == "git-environment":
        monkeypatch.setenv("GIT_TEST_NAME_HASH_VERSION", "2")
    else:
        native = submission._snapshot_git

        def version_changed(root, args, **kw):
            answer = native(root, args, **kw)
            return answer + "-different-build" if args == ["--version"] else answer

        monkeypatch.setattr(submission, "_snapshot_git", version_changed)
    seal(**kwargs)
    seal(**kwargs)
    assert len(packs) == 2


def test_external_graft_file_bypasses_cache_and_preserves_fresh_bytes(sealing, tmp_path, monkeypatch):
    root, _, packs, seal = sealing
    (root / "f00001.txt").write_text("second commit\n")
    subprocess.run(["git", "-C", str(root), "commit", "-qam", "second"], check=True)
    before = seal()[1]
    tip = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    grafts = tmp_path / "grafts"
    grafts.write_text(tip + "\n")
    monkeypatch.setenv("GIT_GRAFT_FILE", str(grafts))
    after = seal()[1]
    assert seal()[1] == after and len(packs) == 3
    monkeypatch.setenv("PRISMABUILD_BUNDLE_CACHE_ROOT", "off")
    assert seal()[1] == after
    assert before != after


def test_default_cache_prefers_qualified_tempdir_and_falls_back_to_home(tmp_path, monkeypatch):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(cache_module.tempfile, "gettempdir", lambda: str(scratch))
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    assert cache_module._default_bundle_cache_root() == scratch / "prismabuild-sealed-bundles"

    def unsupported(root):
        raise cache_module.local_scratch.LocalScratchError("unsupported filesystem")

    monkeypatch.setattr(cache_module.local_scratch, "root_identity", unsupported)
    assert cache_module._default_bundle_cache_root() == tmp_path / "home/tmp/prismabuild-sealed-bundles"
    assert not (tmp_path / "home").exists()


def test_ordered_refs_and_object_format_are_bound(tmp_path, monkeypatch):
    monkeypatch.setenv("PRISMABUILD_BUNDLE_CACHE_ROOT", str(tmp_path / "cache"))
    identities = []
    native = cache_module.seal_bundle_cached

    def captured(destination, identity, seal, **kwargs):
        identities.append(identity)
        return native(destination, identity, seal, **kwargs)

    monkeypatch.setattr(cache_module, "seal_bundle_cached", captured)
    for algorithm in ("sha1", "sha256"):
        root = tmp_path / algorithm
        root.mkdir()
        subprocess.run(["git", "init", "-q", f"--object-format={algorithm}", str(root)], check=True)
        (root / "file").write_text("same content")
        subprocess.run(["git", "-C", str(root), "add", "file"], check=True)
        subprocess.run(["git", "-C", str(root), "-c", "user.name=Test",
                        "-c", "user.email=test@example.invalid", "commit", "-qm", "initial"], check=True)
        oid = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
        bare = tmp_path / f"{algorithm}.git"
        # pb-history-ok: Clones only objects created in this test's temporary repository.
        subprocess.run(["git", "clone", "-q", "--bare", str(root), str(bare)], check=True)
        environment = dict(os.environ, GIT_INDEX_FILE=str(root / ".git" / "index"))
        refs = [("refs/heads/a", oid), ("refs/heads/b", oid)]
        for ordered in (refs, list(reversed(refs))):
            submission.write_deterministic_bundle(root, root / "out.bundle", ordered,
                git_dir=bare, environment=environment)
    assert {identity["object_format"] for identity in identities} == {"sha1", "sha256"}
    assert len({core.canonical_sha256(identity) for identity in identities}) == 4
    assert len(list((tmp_path / "cache").glob("*/bundle"))) == 4


@pytest.mark.parametrize("source", ("global", "repository", "index"))
def test_external_attribute_contents_invalidate_cached_bytes(sealing, tmp_path, monkeypatch, source):
    root, _, packs, _ = sealing
    bare = tmp_path / "bare.git"
    # pb-history-ok: Clones only objects created by the generated temporary repository fixture.
    subprocess.run(["git", "clone", "-q", "--bare", str(root), str(bare)], check=True)
    attrs = (tmp_path / "global-attributes" if source == "global" else
             root / ".gitattributes" if source == "index" else bare / "info" / "attributes")
    attrs.write_text("")
    if source == "index":
        subprocess.run(["git", "-C", str(root), "add", ".gitattributes"], check=True)
    environment = dict(os.environ, GIT_INDEX_FILE=str(root / ".git" / "index"))
    if source == "global":
        environment.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="core.attributesFile",
                           GIT_CONFIG_VALUE_0=str(attrs))
    tip = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()

    def seal():
        target = tmp_path / "out.bundle"
        submission.write_deterministic_bundle(root, target, [("refs/heads/test", tip)],
                                             git_dir=bare, environment=environment)
        return target.read_bytes()

    before = seal()
    assert seal() == before and len(packs) == 1
    attrs.write_text("* -delta\n")
    if source == "index":
        subprocess.run(["git", "-C", str(root), "add", ".gitattributes"], check=True)
    after = seal()
    assert seal() == after and len(packs) == 2
    monkeypatch.setenv("PRISMABUILD_BUNDLE_CACHE_ROOT", "off")
    assert seal() == after
    if source != "index":
        assert before != after


@pytest.mark.parametrize("doubt", ("unsupported-git", "attribute-source", "symlink"))
def test_uncertain_attribute_context_seals_fresh(sealing, tmp_path, monkeypatch, doubt):
    root, _, packs, seal = sealing
    if doubt == "unsupported-git":
        native = submission._snapshot_git

        def without_attribute_discovery(root, args, **kwargs):
            result = native(root, args, **kwargs)
            return ("\n".join(line for line in result.splitlines() if not line.startswith("GIT_ATTR_"))
                    if args[-2:] == ["var", "-l"] else result)

        monkeypatch.setattr(submission, "_snapshot_git", without_attribute_discovery)
    elif doubt == "attribute-source":
        tip = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
        monkeypatch.setenv("GIT_ATTR_SOURCE", tip)
    else:
        target = tmp_path / "attrs"
        target.write_text("")
        link = tmp_path / "attribute-link"
        link.symlink_to(target)
        monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
        monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.attributesFile")
        monkeypatch.setenv("GIT_CONFIG_VALUE_0", str(link))
    seal()
    seal()
    assert len(packs) == 2


@pytest.mark.parametrize("planted", ("info/grafts", "shallow"))
def test_git_init_template_cannot_poison_cached_history(sealing, tmp_path, monkeypatch, planted):
    checkout, cache, _, seal = sealing
    (checkout / "f00001.txt").write_text("second commit\n")
    subprocess.run(["git", "-C", str(checkout), "commit", "-qam", "second"], check=True)
    head = subprocess.check_output(
        ["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip()
    template = tmp_path / "template"
    (template / "info").mkdir(parents=True)
    monkeypatch.setenv("GIT_TEMPLATE_DIR", str(template))
    (template / planted).write_text(head + "\n")
    seal()
    (template / planted).unlink()
    monkeypatch.setenv("PRISMABUILD_BUNDLE_CACHE_ROOT", "off")
    fresh = seal()[1]
    monkeypatch.setenv("PRISMABUILD_BUNDLE_CACHE_ROOT", str(cache))
    assert seal()[1] == fresh


def test_attribute_aba_inside_pack_cannot_publish_the_old_address(sealing, tmp_path, monkeypatch):
    _, cache, _, seal = sealing
    attrs = tmp_path / "external-attributes"
    attrs.write_text("")
    before = attrs.stat()
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.attributesFile")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", str(attrs))
    entered, pack, packed, recheck = (threading.Event() for _ in range(4))
    native = submission._write_bundle_pack

    def gated_pack(*args, **kwargs):
        entered.set()
        pack.wait()
        try:
            return native(*args, **kwargs)
        finally:
            packed.set()
            recheck.wait()

    monkeypatch.setattr(submission, "_write_bundle_pack", gated_pack)
    # Keep the old inode alive so replacement cannot reuse its number.
    with attrs.open("rb"), ThreadPoolExecutor(max_workers=1) as workers:
        result = workers.submit(seal)
        try:
            assert entered.wait(120)
            attrs.write_text("* -delta\n")
            pack.set()
            assert packed.wait(120)
            replacement = tmp_path / "restored-attributes"
            replacement.write_text("")
            os.utime(replacement, ns=(before.st_atime_ns, before.st_mtime_ns))
            os.replace(replacement, attrs)
        finally:
            pack.set()
            recheck.set()
        during = result.result(timeout=120)[1]
    monkeypatch.setattr(submission, "_write_bundle_pack", native)
    assert not list(cache.glob("*/bundle"))
    assert seal()[1] != during


@pytest.mark.parametrize("filesystem_bytes", (4000, 20000))
def test_free_space_floor_declines_cache_fills(tmp_path, monkeypatch, filesystem_bytes):
    cache = tmp_path / "cache"
    monkeypatch.setenv("PRISMABUILD_BUNDLE_CACHE_ROOT", str(cache))
    monkeypatch.setattr(cache_module, "MAX_BYTES", 1000)
    payload = b"ordinary fresh seal"
    floor = max(1000, filesystem_bytes // 10)
    available = floor + len(payload) - 1
    monkeypatch.setattr(cache_module.os, "statvfs", lambda path: os.statvfs_result(
        (1, 1, filesystem_bytes, available, available, 100, 99, 99, 0, 255)))
    calls = []
    destination = tmp_path / "bundle"

    def fresh():
        calls.append(None)
        destination.write_bytes(payload)

    cache_module.seal_bundle_cached(destination, {"space": "low"}, fresh)
    cache_module.seal_bundle_cached(destination, {"space": "low"}, fresh)
    assert len(calls) == 2
    assert not list(cache.glob("*/bundle"))


def test_attribute_change_during_pack_does_not_publish_under_the_old_key(sealing, tmp_path, monkeypatch):
    _, cache, packs, seal = sealing
    attrs = tmp_path / "external-attributes"
    attrs.write_text("")
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.attributesFile")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", str(attrs))
    native = subprocess.run
    changed = False

    def change_at_pack(args, **kwargs):
        nonlocal changed
        if "pack-objects" in args and not changed:
            changed = True
            attrs.write_text("* -delta\n")
        return native(args, **kwargs)

    monkeypatch.setattr(subprocess, "run", change_at_pack)
    during = seal()[1]
    assert not list(cache.glob("*/bundle"))
    attrs.write_text("")
    after = seal()[1]
    assert after != during
    assert seal()[1] == after and len(packs) == 2


@pytest.mark.parametrize("damage", (
    "payload", "digest", "key", "size", "partial", "missing-payload", "invalid-json", "symlink", "fifo",
))
def test_corrupt_or_partial_entry_is_resealed(sealing, damage):
    _, cache, packs, seal = sealing
    first, expected = seal()
    entry = next(cache.glob("*/bundle")).parent
    metadata = entry / "digest.json"
    record = json.loads(metadata.read_text())
    if damage == "payload":
        payload = bytearray((entry / "bundle").read_bytes())
        payload[-1] ^= 1
        (entry / "bundle").write_bytes(payload)
    elif damage in {"digest", "key", "size"}:
        field = {"digest": "sha256", "key": "key", "size": "bytes"}[damage]
        record[field] = 1 if damage == "size" else "0" * 64
        metadata.write_text(json.dumps(record))
    elif damage == "partial":
        metadata.unlink()
        (cache / ".pending-abandoned").mkdir()
        (cache / ".pending-abandoned" / "bundle").write_bytes(b"partial")
    elif damage == "missing-payload":
        (entry / "bundle").unlink()
    elif damage == "invalid-json":
        metadata.write_text("{")
    elif damage == "symlink":
        metadata.unlink()
        metadata.symlink_to(entry / "bundle")
    else:
        (entry / "bundle").unlink()
        os.mkfifo(entry / "bundle")
    actual, actual_bytes = seal()
    assert actual == first
    assert actual_bytes == expected
    assert len(packs) == 2
    assert seal()[1] == expected
    assert len(packs) == 2
    assert not list(cache.glob(".pending-*"))


def test_cache_bounds_entries_bytes_and_oversize_bypass(sealing, monkeypatch):
    _, cache, packs, seal = sealing
    monkeypatch.setattr(cache_module, "MAX_ENTRIES", 2)
    known = set()
    for tick, payload in enumerate(("one", "two", "three"), 1):
        seal(stamp_name="closure", stamp_payload=payload)
        current = set(cache.glob("*/bundle"))
        created, = current - known
        os.utime(created.parent, ns=(tick * 1000000000, tick * 1000000000))
        known = current
    assert len(list(cache.glob("*/bundle"))) == 2
    seal(stamp_name="closure", stamp_payload="one")
    assert len(packs) == 4
    size = max(sum(path.stat().st_size for path in bundle.parent.iterdir())
               for bundle in cache.glob("*/bundle"))
    monkeypatch.setattr(cache_module, "MAX_BYTES", size + 1)
    seal(stamp_name="closure", stamp_payload="four")
    assert sum(path.stat().st_size for bundle in cache.glob("*/bundle")
               for path in bundle.parent.iterdir()) <= size + 1
    monkeypatch.setattr(cache_module, "MAX_BYTES", 1)
    seal(stamp_name="closure", stamp_payload="too big")
    seal(stamp_name="closure", stamp_payload="too big")
    assert len(packs) == 7
    assert not list(cache.glob("*/bundle"))


@pytest.mark.parametrize("filesystem", ("tmpfs", "nfs", "unknown"))
def test_cache_refuses_ram_shared_and_unknown_filesystems(sealing, monkeypatch, filesystem):
    _, cache, packs, seal = sealing
    observer = cache_module.local_scratch._descriptor_mount

    def mount(fd):
        return {**observer(fd), "filesystem_type": filesystem}

    monkeypatch.setattr(cache_module.local_scratch, "_descriptor_mount", mount)
    seal()
    seal()
    assert len(packs) == 2
    assert not cache.exists()


def test_cache_io_error_and_git_version_doubt_fall_back(sealing, monkeypatch):
    _, cache, packs, seal = sealing
    cache.write_text("not a directory")
    seal()
    seal()
    cache.unlink()
    native = submission._snapshot_git

    def unknown_version(root, args, **kwargs):
        if args == ["--version"]:
            raise submission.SubmissionRefused("version unavailable")
        return native(root, args, **kwargs)

    monkeypatch.setattr(submission, "_snapshot_git", unknown_version)
    seal()
    assert len(packs) == 3
    assert not cache.exists()


def test_concurrent_processes_share_one_atomic_fill(sealing, tmp_path):
    checkout, cache, _, seal = sealing
    marker = tmp_path / "packs"
    read_gate, write_gate = os.pipe()
    script = '''
import json, os, subprocess, sys
from pathlib import Path
from prismabuild import core, submission
space = tuple(json.loads(sys.argv[5]))
os.statvfs = lambda path: os.statvfs_result(space)
native = subprocess.run
def observed(args, **kwargs):
    if "pack-objects" in args:
        fd = os.open(sys.argv[3], os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
        os.write(fd, b"pack\\n")
        os.close(fd)
    return native(args, **kwargs)
subprocess.run = observed
os.read(int(sys.argv[4]), 1)
cas = core.PrismaBuildCAS(Path(sys.argv[2]))
snapshot = submission.snapshot_checkout(Path(sys.argv[1]), cas=cas)
print(snapshot["input"]["sha256"])
'''
    children = [subprocess.Popen([sys.executable, "-c", script, str(checkout),
        str(tmp_path / f"cas-{index}"), str(marker), str(read_gate), json.dumps(_AMPLE_VFS)],
        pass_fds=(read_gate,), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env={**os.environ, "PYTHONPATH": str(Path(submission.__file__).resolve().parents[1])})
        for index in range(3)]
    os.close(read_gate)
    try:
        os.write(write_gate, b"xxx")
        digests = []
        for child in children:
            stdout, stderr = child.communicate(timeout=90)
            assert child.returncode == 0, stderr
            digests.append(stdout.strip())
        assert len(set(digests)) == 1
        assert marker.read_text().splitlines() == ["pack"]
        assert seal()[0]["input"]["sha256"] == digests[0]
        assert len(list(cache.glob("*/bundle"))) == 1
    finally:
        os.close(write_gate)
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait()


@pytest.mark.parametrize("stage", ("read", "reserve", "publish"))
def test_inventory_contention_does_not_duplicate_a_waiting_fill(sealing, tmp_path, stage):
    checkout, cache, packs, seal = sealing
    if stage == "read":
        seal()
    else:
        cache.mkdir(mode=0o700)
    marker = tmp_path / "packs"
    script = r'''
import json, os, subprocess, sys
from contextlib import contextmanager
from multiprocessing.connection import Connection
from pathlib import Path
from prismabuild import core, sealed_bundle_cache as cache, submission
checkout, store, marker, stage, role = sys.argv[1:6]
control = Connection(int(sys.argv[6]))
os.statvfs = lambda path: os.statvfs_result(tuple(json.loads(sys.argv[7])))
run, lock = subprocess.run, cache._bundle_cache_lock
def observed(args, **kwargs):
    if "pack-objects" in args:
        fd = os.open(marker, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
        os.write(fd, b"pack\n")
        os.close(fd)
    return run(args, **kwargs)
subprocess.run = observed
counts = {False: 0, True: 0}
@contextmanager
def gated(path, *, nonblocking=False, shared=False):
    if role == "first" and path.name == ".inventory":
        counts[shared] += 1
        selected = ((stage == "read" and shared and counts[shared] == 2)
                    or (not shared and counts[shared] == {"reserve": 2, "publish": 3}.get(stage)))
        if selected:
            control.send("ready")
            assert control.recv() == "held"
            # Capture an actual contended probe before the parent releases it.
            # A nonblocking call must retain that miss; a blocking call retries.
            with lock(path, nonblocking=True, shared=shared) as probe:
                assert probe is None
                control.send("contended")
                assert control.recv() == "release"
                if nonblocking:
                    yield None
                    return
    if role == "waiter" and path.name.startswith(".lock-"):
        with lock(path, nonblocking=True, shared=shared) as probe:
            assert probe is None, "first filler must still own the address"
        control.send("waiting")
    with lock(path, nonblocking=nonblocking, shared=shared) as locked:
        yield locked
cache._bundle_cache_lock = gated
snapshot = submission.snapshot_checkout(Path(checkout), cas=core.PrismaBuildCAS(Path(store)))
print(snapshot["input"]["sha256"])
'''
    children, controls = [], []

    def start(role):
        parent, child = multiprocessing.Pipe()
        controls.append(parent)
        try:
            process = subprocess.Popen([sys.executable, "-c", script, str(checkout),
                str(tmp_path / ("cas-" + role)), str(marker), stage, role,
                str(child.fileno()), json.dumps(_AMPLE_VFS)], pass_fds=(child.fileno(),),
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                env={**os.environ, "PYTHONPATH": str(Path(submission.__file__).resolve().parents[1])})
            children.append(process)
        finally:
            child.close()
        return parent

    def expect(control, message):
        assert control.poll(90), "cache child did not reach " + message
        assert control.recv() == message

    inventory = ExitStack()
    try:
        if stage == "read":
            inventory.enter_context(cache_module._bundle_cache_lock(cache / ".inventory"))
        first = start("first")
        expect(first, "ready")
        if stage != "read":
            inventory.enter_context(cache_module._bundle_cache_lock(cache / ".inventory"))
        first.send("held")
        expect(first, "contended")
        waiter = start("waiter")
        expect(waiter, "waiting")
        inventory.close()
        first.send("release")
        digests = []
        for child in children:
            stdout, stderr = child.communicate(timeout=90)
            assert child.returncode == 0, stderr
            digests.append(stdout.strip())
        assert len(set(digests)) == 1
        assert ["pack"] * len(packs) + marker.read_text().splitlines() == ["pack"]
        assert seal()[0]["input"]["sha256"] == digests[0]
        assert len(list(cache.glob("*/bundle"))) == 1
    finally:
        inventory.close()
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait()
        for control in controls:
            control.close()


def test_distinct_addresses_with_one_prefix_never_serialize(tmp_path, monkeypatch):
    cache = tmp_path / "cache"
    monkeypatch.setenv("PRISMABUILD_BUNDLE_CACHE_ROOT", str(cache))
    first = {"case": 0}
    prefix = core.canonical_sha256(first)[0]
    second = next({"case": i} for i in range(1, 1000)
                  if core.canonical_sha256({"case": i})[0] == prefix)
    entered, release = threading.Event(), threading.Event()
    a, b = tmp_path / "a", tmp_path / "b"

    def seal_first():
        entered.set()
        release.wait()
        a.write_bytes(b"first")

    def seal_second():
        b.write_bytes(b"second")

    with ThreadPoolExecutor(max_workers=2) as workers:
        first_result = workers.submit(cache_module.seal_bundle_cached, a, first, seal_first)
        try:
            assert entered.wait(120)
            second_result = workers.submit(cache_module.seal_bundle_cached, b, second, seal_second)
            second_result.result(timeout=120)
        finally:
            release.set()
        first_result.result(timeout=120)
    assert not list(cache.glob(".pin-*")) and not list(cache.glob(".lock-*"))


def test_warm_deadline_callers_all_reuse_the_bundle(sealing, monkeypatch):
    _, _, packs, seal = sealing
    expected = seal()[1]
    ready = threading.Barrier(4)
    monkeypatch.setattr(core, "git_deadline_timeout", lambda deadline: None)

    def warm():
        ready.wait(timeout=120)
        return seal(deadline=1.0)[1]

    with ThreadPoolExecutor(max_workers=4) as workers:
        results = [future.result(timeout=120) for future in [workers.submit(warm) for _ in range(4)]]
    assert results == [expected] * 4
    assert len(packs) == 1


def test_hit_copy_releases_inventory_and_survives_eviction(sealing, monkeypatch):
    _, cache, packs, seal = sealing
    expected = seal()[1]
    entry = next(cache.glob("*/bundle")).parent
    copying, release = threading.Event(), threading.Event()
    native_opened = cache_module._opened_bundle_cache_entry

    @contextmanager
    def held_entry(*args):
        with native_opened(*args) as opened:
            copying.set()
            release.wait()
            yield opened

    monkeypatch.setattr(cache_module, "_opened_bundle_cache_entry", held_entry)
    with ThreadPoolExecutor(max_workers=1) as workers:
        result = workers.submit(seal)
        try:
            assert copying.wait(120)
            with cache_module._bundle_cache_lock(cache / ".inventory", nonblocking=True) as locked:
                assert locked is not None, "copy holds the inventory lock"
                shutil.rmtree(entry)
        finally:
            release.set()
        assert result.result(timeout=60)[1] == expected
    assert len(packs) == 1


def test_fallback_does_not_rewrite_a_valid_entry(sealing, monkeypatch):
    _, cache, packs, seal = sealing
    expected = seal()[1]
    bundle = next(cache.glob("*/bundle"))
    with ExitStack() as held:
        held.enter_context(bundle.open("rb"))
        directory = os.open(bundle.parent, os.O_RDONLY | os.O_DIRECTORY)
        held.callback(os.close, directory)
        identity = (bundle.parent.stat().st_ino, bundle.stat().st_ino)
        monkeypatch.setattr(cache_module, "_reuse_bundle_cache", lambda *args, **kwargs: False)
        assert seal()[1] == expected and len(packs) == 2
        assert (bundle.parent.stat().st_ino, bundle.stat().st_ino) == identity
    assert not list(cache.glob(".pending-*"))


def test_dead_address_lock_is_reaped_without_process_inspection(tmp_path, monkeypatch):
    cache = tmp_path / "cache"
    monkeypatch.setenv("PRISMABUILD_BUNDLE_CACHE_ROOT", str(cache))
    script = '''
import os, sys
from pathlib import Path
from prismabuild.sealed_bundle_cache import seal_bundle_cached
seal_bundle_cached(Path(sys.argv[1]), {"dead": True}, lambda: os._exit(0))
'''
    result = subprocess.run([sys.executable, "-c", script, str(tmp_path / "dead")], check=False)
    assert result.returncode == 0
    assert list(cache.glob(".pin-*")) and list(cache.glob(".lock-*"))
    target = tmp_path / "live"
    cache_module.seal_bundle_cached(target, {"dead": False}, lambda: target.write_bytes(b"live"))
    assert target.read_bytes() == b"live"
    assert not list(cache.glob(".pin-*")) and not list(cache.glob(".lock-*"))


def test_dead_filler_is_reaped_and_the_same_address_can_recover(sealing, tmp_path):
    checkout, cache, packs, seal = sealing
    parent, control = multiprocessing.Pipe()
    script = r'''
import json, os, shutil, sys
from multiprocessing.connection import Connection
from pathlib import Path
from prismabuild import core, submission
control = Connection(int(sys.argv[3]))
os.statvfs = lambda path: os.statvfs_result(tuple(json.loads(sys.argv[4])))
native = shutil.copyfileobj
def stalled(source, destination, length=0):
    name = getattr(destination, "name", None)
    if isinstance(name, (str, Path)) and Path(name).name == "bundle":
        control.send("staged")
        control.recv()
    return native(source, destination, length)
shutil.copyfileobj = stalled
submission.snapshot_checkout(Path(sys.argv[1]), cas=core.PrismaBuildCAS(Path(sys.argv[2])))
'''
    child = subprocess.Popen([sys.executable, "-c", script, str(checkout),
        str(tmp_path / "dead-cas"), str(control.fileno()), json.dumps(_AMPLE_VFS)],
        pass_fds=(control.fileno(),), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env={**os.environ, "PYTHONPATH": str(Path(submission.__file__).resolve().parents[1])})
    control.close()
    try:
        assert parent.poll(90), "filler did not publish its reservation"
        assert parent.recv() == "staged"
        assert len(list(cache.glob(".pending-*"))) == 1
    finally:
        child.kill()
        child.communicate(timeout=90)
        parent.close()
    expected = seal()
    assert seal() == expected and len(packs) == 1
    assert not list(cache.glob(".pending-*"))
    assert not list(cache.glob(".pin-*")) and not list(cache.glob(".lock-*"))


def test_deadline_caller_bypasses_a_stuck_cold_filler(tmp_path, monkeypatch):
    monkeypatch.setenv("PRISMABUILD_BUNDLE_CACHE_ROOT", str(tmp_path / "cache"))
    entered, release = threading.Event(), threading.Event()
    first, bounded, later = (tmp_path / name for name in ("first", "bounded", "later"))
    calls = []

    def stalled():
        calls.append("first")
        entered.set()
        release.wait()
        first.write_bytes(b"same bundle")

    def fresh():
        calls.append("bounded")
        bounded.write_bytes(b"same bundle")

    with ThreadPoolExecutor(max_workers=2) as workers:
        original = workers.submit(cache_module.seal_bundle_cached, first, {"stuck": True}, stalled)
        try:
            assert entered.wait(90)
            deadline = workers.submit(cache_module.seal_bundle_cached, bounded, {"stuck": True},
                                      fresh, bounded_wait=True)
            deadline.result(timeout=90)
            assert bounded.read_bytes() == b"same bundle"
        finally:
            release.set()
        original.result(timeout=90)
    cache_module.seal_bundle_cached(later, {"stuck": True}, lambda: pytest.fail("completed fill was lost"))
    assert later.read_bytes() == first.read_bytes() == b"same bundle"
    assert calls == ["first", "bounded"]


def test_failed_fill_is_removed_under_inventory_and_can_recover(sealing, monkeypatch):
    _, cache, packs, seal = sealing
    native_copy, native_remove = shutil.copyfileobj, shutil.rmtree
    removed = []

    def full(source, destination, length=0):
        name = getattr(destination, "name", None)
        if isinstance(name, (str, Path)) and Path(name).name == "bundle":
            raise OSError("fixture disk full")
        return native_copy(source, destination, length)

    def checked_remove(path, *args, **kwargs):
        if Path(path).name.startswith(".pending-"):
            with cache_module._bundle_cache_lock(cache / ".inventory", nonblocking=True) as locked:
                assert locked is None, "pending removal escaped inventory protection"
            removed.append(Path(path).name)
        return native_remove(path, *args, **kwargs)

    monkeypatch.setattr(shutil, "copyfileobj", full)
    monkeypatch.setattr(shutil, "rmtree", checked_remove)
    expected = seal()[1]
    assert removed and not list(cache.glob(".pending-*"))
    monkeypatch.setattr(shutil, "copyfileobj", native_copy)
    assert seal()[1] == expected
    assert seal()[1] == expected and len(packs) == 2


@pytest.mark.parametrize("which_lock", ("address", "inventory"))
def test_caller_deadline_bypasses_a_contended_cache_lock(sealing, monkeypatch, which_lock):
    _, cache, packs, seal = sealing
    expected = seal()[1]
    address = next(cache.glob("*/bundle")).parent.name
    lock = (cache_module._bundle_cache_flight(cache, address, bounded_wait=False) if which_lock == "address"
            else cache_module._bundle_cache_lock(cache / ".inventory"))
    monkeypatch.setattr(core, "git_deadline_timeout", lambda deadline: None)
    with lock:
        assert seal(deadline=1.0)[1] == expected
    assert len(packs) == (1 if which_lock == "address" else 2)


def test_non_utf8_configuration_cannot_disable_sealing(tmp_path):
    destination = tmp_path / "bundle"
    cache_module.seal_bundle_cached(destination, {"effective_config": "\udcff"},
                                     lambda: destination.write_bytes(b"fresh seal"))
    assert destination.read_bytes() == b"fresh seal"

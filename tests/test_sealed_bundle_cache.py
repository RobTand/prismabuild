"""Repeated seals reuse verified bytes without changing snapshot identity."""
import ast
import copy
import json
import os
import pickle
from pathlib import Path
import subprocess
import sys
import types

import pytest

from prismabuild import core, sealed_bundle_cache as cache_module, submission
from test_pbrun_bundle_determinism import _action_key, _repository


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
    "ref-name", "tip", "stamp-name", "stamp-payload", "pack-config", "pack-options", "git-version", "inherited-config",
))
def test_every_seal_input_invalidates_the_cache(sealing, monkeypatch, component):
    checkout, _, packs, seal = sealing
    kwargs = {"stamp_name": "closure", "stamp_payload": "first"}
    seal(**kwargs)
    if component == "ref-name":
        subprocess.run(["git", "-C", str(checkout), "branch", "extra"], check=True)
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
    else:
        native = submission._snapshot_git

        def version_changed(root, args, **kw):
            answer = native(root, args, **kw)
            return answer + "-different-build" if args == ["--version"] else answer

        monkeypatch.setattr(submission, "_snapshot_git", version_changed)
    seal(**kwargs)
    seal(**kwargs)
    assert len(packs) == 2


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
    assert seal()[1] == seal()[1]
    assert len(packs) == 2


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
    for payload in ("one", "two", "three"):
        seal(stamp_name="closure", stamp_payload=payload)
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
import os, subprocess, sys
from pathlib import Path
from prismabuild import core, submission
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
        str(tmp_path / f"cas-{index}"), str(marker), str(read_gate)],
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


def test_only_copy_verification_constructs_the_capability():
    tree = ast.parse(Path(cache_module.__file__).read_text())
    constructors = []
    for function in tree.body:
        if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for node in ast.walk(function):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                    if node.func.id == "_VerifiedBundle":
                        constructors.append(function.name)
    assert constructors == ["_verify_bundle_copy"]


def test_installer_requires_an_unforgeable_single_use_proof(tmp_path):
    staged = tmp_path / "staged"
    destination = tmp_path / "destination"
    staged.write_bytes(b"verified bytes")
    proof = cache_module._verify_bundle_copy(staged, staged.stat().st_size,
                                             core.stream_sha256(staged))
    assert not hasattr(proof, "__dict__")
    for operation in (copy.copy, copy.deepcopy, pickle.dumps):
        with pytest.raises(TypeError):
            operation(proof)
    with pytest.raises(TypeError):
        type("ForgedBundle", (cache_module._VerifiedBundle,), {})
    for invalid in (None, True, object(), types.SimpleNamespace(),
                    object.__new__(cache_module._VerifiedBundle)):
        with pytest.raises(ValueError):
            cache_module._install_bundle_copy(invalid, destination)
    cache_module._install_bundle_copy(proof, destination)
    assert destination.read_bytes() == b"verified bytes"
    with pytest.raises(ValueError):
        cache_module._install_bundle_copy(proof, destination)


@pytest.mark.parametrize("through_cache", (False, True))
def test_changed_verified_copy_is_rejected_at_installation(sealing, tmp_path, monkeypatch, through_cache):
    if through_cache:
        _, _, packs, seal = sealing
        expected = seal()[1]
        verifier = cache_module._verify_bundle_copy

        def changed(path, *args):
            proof = verifier(path, *args)
            path.write_bytes(b"changed after verification")
            return proof

        monkeypatch.setattr(cache_module, "_verify_bundle_copy", changed)
        assert seal()[1] == expected
        assert len(packs) == 2
    else:
        staged, destination = tmp_path / "staged", tmp_path / "destination"
        staged.write_bytes(b"original")
        proof = cache_module._verify_bundle_copy(staged, 8, core.stream_sha256(staged))
        staged.write_bytes(b"tampered")
        with pytest.raises(ValueError, match="changed"):
            cache_module._install_bundle_copy(proof, destination)
        assert not destination.exists()


def test_digest_check_mutation_fails_the_corruption_invariant(sealing, tmp_path):
    _, cache, _, seal = sealing
    seal()
    bundle = next(cache.glob("*/bundle"))
    payload = bytearray(bundle.read_bytes())
    payload[-1] ^= 1
    bundle.write_bytes(payload)
    source = Path(cache_module.__file__).read_text()
    check = "digests.stream_sha256(path) != expected_digest"
    assert source.count(check) == 1
    mutant = types.ModuleType("prismabuild._bundle_cache_mutant")
    exec(compile(source.replace(check, "False"), cache_module.__file__, "exec"), mutant.__dict__)

    def corruption_invariant(module):
        assert not module._reuse_bundle_cache(cache, bundle.parent.name,
                                              tmp_path / "out", bounded_wait=False)

    corruption_invariant(cache_module)
    with pytest.raises(AssertionError):
        corruption_invariant(mutant)


@pytest.mark.parametrize("which_lock", ("stripe", "inventory"))
def test_caller_deadline_bypasses_a_contended_cache_lock(sealing, which_lock):
    _, cache, packs, seal = sealing
    expected = seal()[1]
    address = next(cache.glob("*/bundle")).parent.name
    name = f".lock-{address[0]}" if which_lock == "stripe" else ".inventory"
    with cache_module._bundle_cache_lock(cache / name):
        assert seal(deadline=core.time.monotonic() + 60)[1] == expected
    assert len(packs) == 2


def test_ordinary_cache_path_refuses_a_forged_verification(sealing, monkeypatch):
    _, _, packs, seal = sealing
    expected = seal()[1]
    monkeypatch.setattr(cache_module, "_verify_bundle_copy",
                        lambda *args: object.__new__(cache_module._VerifiedBundle))
    assert seal()[1] == expected
    assert len(packs) == 2


def test_non_utf8_configuration_cannot_disable_sealing(tmp_path):
    destination = tmp_path / "bundle"
    cache_module.seal_bundle_cached(destination, {"effective_config": "\udcff"},
                                     lambda: destination.write_bytes(b"fresh seal"))
    assert destination.read_bytes() == b"fresh seal"

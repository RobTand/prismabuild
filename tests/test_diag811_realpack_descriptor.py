"""Real Git objects qualify experimental fallback, not a production lease.

The ioctl outcome is injected. Git writes/verifies the actual pack and index,
and the copied objects must materialize the original commit and worktree.
"""
from __future__ import annotations

import errno
import hashlib
import importlib.util
import os
from pathlib import Path
import subprocess

import pytest


@pytest.fixture
def harness():
    source = (Path(__file__).resolve().parents[1] / "tools" / "maintenance"
              / "diag_811_e1_checkout_cache.py")
    spec = importlib.util.spec_from_file_location("diag811_realpack_test", source)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def git(*arguments):
    # This fixture runs only inside its admitted PB pytest action.
    result = subprocess.run(
        ["git", "-c", "pack.threads=1", "-c", "gc.auto=0", *map(str, arguments)],
        check=True, capture_output=True, text=True,
        env={**os.environ, "GIT_CONFIG_NOSYSTEM": "1",
             "GIT_CONFIG_GLOBAL": os.devnull,
             "GIT_AUTHOR_NAME": "PB fixture", "GIT_AUTHOR_EMAIL": "test@example.invalid",
             "GIT_COMMITTER_NAME": "PB fixture", "GIT_COMMITTER_EMAIL": "test@example.invalid",
             "GIT_AUTHOR_DATE": "2000-01-01T00:00:00+00:00",
             "GIT_COMMITTER_DATE": "2000-01-01T00:00:00+00:00"},
    )
    return result.stdout.strip()


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize("selected,mutation", [
    (None, None),
    ("pack", "replace"), ("pack", "unlink"),
    ("idx", "replace"), ("idx", "unlink"),
])
def test_real_pack_fallback_materializes_the_verified_commit(
        harness, tmp_path, monkeypatch, selected, mutation):
    original = tmp_path / "original"
    git("init", "-q", "--object-format=sha1", original)
    payload = b"original worktree payload\n"
    (original / "payload.txt").write_bytes(payload)
    git("-C", original, "add", "payload.txt")
    git("-C", original, "commit", "-q", "-m", "pack fixture")
    commit = git("-C", original, "rev-parse", "HEAD")
    bundle = tmp_path / "source.bundle"
    git("-C", original, "bundle", "create", bundle, "HEAD")
    framing = harness.parse_bundle(bundle)
    entry = tmp_path / "entry"
    entry.mkdir()
    pack, idx = harness.entry_paths(entry, framing)
    raw = bundle.read_bytes()
    pack.write_bytes(raw[int(framing["pack_offset"]):])
    git("index-pack", "-o", idx, pack)
    git("index-pack", "--verify", pack)
    expected = {"pack": sha256(pack), "idx": sha256(idx)}
    assert expected["pack"] == framing["pack_sha256"]

    private = tmp_path / "private"
    git("init", "-q", "--object-format=sha1", private)
    objects = private / ".git" / "objects" / "pack"
    mutated = []
    descriptors = []

    def unsupported_reflink(destination_fd, operation, source_fd):
        assert operation == harness.FICLONE
        source = Path(os.readlink(f"/proc/self/fd/{source_fd}"))
        label = "pack" if source == pack else "idx"
        assert source in (pack, idx)
        descriptors.extend((destination_fd, source_fd))
        if label == selected:
            size = os.fstat(source_fd).st_size
            if mutation == "replace":
                source.replace(entry / f"retired.{label}")
                source.write_bytes(b"\x00" * size)  # Same length, new inode.
            else:
                source.unlink()
            mutated.append(label)
        raise OSError(errno.EOPNOTSUPP, "injected unsupported reflink")

    monkeypatch.setattr(harness.fcntl, "ioctl", unsupported_reflink)
    for label, source in (("pack", pack), ("idx", idx)):
        destination = objects / source.name
        assert harness.copy_or_reflink(source, destination) == "copy"
        assert sha256(destination) == expected[label], (
            f"copied {label} differs from the originally verified Git object")
        for descriptor in descriptors:
            with pytest.raises(OSError) as failure:
                os.fstat(descriptor)
            assert failure.value.errno == errno.EBADF
    assert mutated == ([] if selected is None else [selected])
    git("index-pack", "--verify", objects / pack.name)
    git("-C", private, "checkout", "-q", "--detach", commit)
    assert git("-C", private, "rev-parse", "HEAD") == commit
    assert (private / "payload.txt").read_bytes() == payload
    assert git("-C", private, "status", "--porcelain") == ""

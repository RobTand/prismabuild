"""Synthetic arm-B witnesses for its verification-to-open identity fence.

The copy and descriptor operations are real. Git/CAS/preflight/cleanup control
plumbing is scaffolded; this does not qualify a production lease or Git bytes.
"""
from __future__ import annotations

import errno
import hashlib
import importlib.util
from pathlib import Path
import shutil
from types import SimpleNamespace

import pytest

from prismabuild import reader_lease


@pytest.fixture
def harness():
    source = (Path(__file__).resolve().parents[1] / "tools" / "maintenance"
              / "diag_811_e1_checkout_cache.py")
    spec = importlib.util.spec_from_file_location("diag811_acquisition_test", source)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("selected", [None, "pack", "idx"])
def test_arm_b_refuses_a_new_inode_before_copy(harness, tmp_path, monkeypatch,
                                              selected):
    entry = tmp_path / "entry"
    entry.mkdir()
    framing = {"pack_trailer_hex": hashlib.sha1(b"synthetic fixture").hexdigest()}
    pack, idx = harness.entry_paths(entry, framing)
    pack.write_bytes(b"synthetic pack bytes")
    idx.write_bytes(b"synthetic index bytes")
    (entry / "manifest.json").write_text("{}\n")
    identity = harness.entry_identity(entry, framing, reader_lease)
    memo = {str(entry): {
        "identity": identity,
        "pack_sha256": harness.sha256_stream(pack),
        "idx_sha256": harness.sha256_stream(idx),
    }}
    commit = hashlib.sha1(b"synthetic commit").hexdigest()
    mutations, copies, cleaned = [], [], []

    class Git:
        spans = []
        arm = None

        def run(self, arguments, *, where, environment=None):
            self.spans.append({"where": where})
            if where == "initialize materialized checkout":
                (Path(arguments[-1]) / ".git" / "objects" / "pack").mkdir(parents=True)
                if selected is not None:
                    source = pack if selected == "pack" else idx
                    data = source.read_bytes()
                    source.replace(entry / f"retired.{selected}")
                    source.write_bytes(data)  # Same bytes/length, different inode.
                    assert source.stat().st_ino != identity[selected]["ino"]
                    mutations.append(selected)
            if where == "read checkout snapshot bundle":
                return f"{commit} HEAD\n"
            return ""

    def unsupported_reflink(destination_fd, operation, source_fd):
        copies.append(source_fd)
        raise OSError(errno.EOPNOTSUPP, "injected unsupported reflink")

    def cleanup(root, temporary, item):
        cleaned.append(temporary)
        shutil.rmtree(temporary)

    monkeypatch.setattr(harness.fcntl, "ioctl", unsupported_reflink)
    monkeypatch.setattr(harness, "preflight", lambda *args: {"ok": True})
    monkeypatch.setattr(harness, "repo_state", lambda *args: {"head": commit})
    core = SimpleNamespace(PrismaBuildCAS=lambda root: SimpleNamespace(
        input_path=lambda item: tmp_path / "scaffold.bundle"))
    materialize = SimpleNamespace(
        _cleanup_execution_checkout=cleanup,
        _require_contained_materialized_links=lambda root: None,
    )
    checkout_root = tmp_path / "checkouts"
    checkout_root.mkdir()
    arguments = dict(
        item={"cas_root": str(tmp_path), "action_key": hashlib.sha256(b"fixture").hexdigest(),
              "request": {}},
        snapshot={"input": {}, "commit": commit}, framing=framing,
        bundle_path=tmp_path / "scaffold.bundle", entry=entry,
        checkout_root=checkout_root, core=core, materialize=materialize, git=Git(),
        reader_lease=reader_lease, memo=memo, pair=0, order="test",
    )
    if selected is None:
        result = harness.arm_b_rep(**arguments)
        assert result["copy_methods"] == ["copy", "copy"]
        assert len(copies) == 2
    else:
        with pytest.raises(harness.E1Error, match="identity.*changed|changed.*identity"):
            harness.arm_b_rep(**arguments)
        # Refuse before the selected object's ioctl or byte-copy path starts.
        assert len(copies) == (0 if selected == "pack" else 1)
    assert mutations == ([] if selected is None else [selected])
    assert len(cleaned) == 1
    assert list(checkout_root.iterdir()) == []

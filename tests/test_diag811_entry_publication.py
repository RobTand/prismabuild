"""CPU publication-record witnesses for the experimental checkout cache.

The Git double isolates manifest admission; these tests do not qualify a Git
pack, run a queue action, or measure checkout performance.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pytest

from prismabuild import core, reader_lease


@pytest.fixture
def entry_case(tmp_path):
    source = (Path(__file__).resolve().parents[1] / "tools" / "maintenance"
              / "diag_811_e1_checkout_cache.py")
    spec = importlib.util.spec_from_file_location("diag811_publication_test", source)
    assert spec is not None and spec.loader is not None
    harness = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(harness)
    pack_bytes, idx_bytes = b"synthetic-pack", b"synthetic-index"
    bundle_bytes = b"header\n" + pack_bytes
    bundle = tmp_path / "source.bundle"
    bundle.write_bytes(bundle_bytes)
    digest = lambda value: hashlib.sha256(value).hexdigest()
    framing = {
        "sha256": digest(bundle_bytes), "bytes": len(bundle_bytes),
        "pack_offset": len(b"header\n"), "pack_len": len(pack_bytes),
        "pack_sha256": digest(pack_bytes), "pack_trailer_hex": "a" * 40,
        "advertised": [{"oid": "b" * 40, "ref": "HEAD"}],
        "object_format": "sha1", "runtime_generation": "test-generation",
    }
    entry = tmp_path / "entry"
    entry.mkdir()
    pack, idx = harness.entry_paths(entry, framing)
    pack.write_bytes(pack_bytes)
    idx.write_bytes(idx_bytes)
    manifest = {
        "schema": "prismabuild.diag811.e1_entry.v1",
        "generation": framing["runtime_generation"],
        "bundle": {key: framing[key] for key in
                   ("sha256", "bytes", "pack_offset", "pack_len")},
        "pack": {"name": pack.name, "sha256": digest(pack_bytes),
                 "bytes": len(pack_bytes)},
        "idx": {"name": idx.name, "sha256": digest(idx_bytes),
                "bytes": len(idx_bytes)},
        "advertised": framing["advertised"],
        "object_format": framing["object_format"], "created_unix": 1.0,
    }
    publication = entry / "manifest.json"
    publication.write_text(json.dumps(manifest))

    class GitDouble:
        error = RuntimeError

        def run(self, argv, *, where):
            assert argv[:3] == ["git", "index-pack", "--verify"]
            return ""

    memo = {}

    def verify():
        return harness.verify_entry(entry=entry, bundle_path=bundle,
                                    framing=framing, git=GitDouble(), core=core,
                                    reader_lease=reader_lease, memo=memo)

    return manifest, publication, verify, memo


def test_valid_published_entry_can_be_memoized(entry_case):
    _, _, verify, _ = entry_case
    assert verify()["ok"] is True
    assert verify()["cached"] is True


@pytest.mark.parametrize("fault", [
    "missing", "malformed", "schema", "generation", "bundle", "pack",
    "index", "advertised", "object_format",
])
def test_unpublished_or_misbound_entry_is_refused(entry_case, fault):
    manifest, publication, verify, memo = entry_case
    if fault == "missing":
        publication.unlink()
    elif fault == "malformed":
        publication.write_text("{broken")
    else:
        if fault == "schema":
            manifest["schema"] = "unknown"
        elif fault == "generation":
            manifest["generation"] = "another-generation"
        elif fault == "bundle":
            manifest["bundle"]["sha256"] = "0" * 64
        elif fault == "pack":
            manifest["pack"]["sha256"] = "0" * 64
        elif fault == "index":
            manifest["idx"]["sha256"] = "0" * 64
        elif fault == "advertised":
            manifest["advertised"] = []
        else:
            manifest["object_format"] = "sha256"
        publication.write_text(json.dumps(manifest))
    verdict = verify()
    assert verdict["ok"] is False, "unpublished or misbound cache entry accepted"
    assert "manifest" in verdict["reason"]
    assert not memo


@pytest.mark.parametrize("sign", [-1, 1])
@pytest.mark.parametrize("primed", [False, True])
def test_extreme_creation_time_is_a_named_refusal(entry_case, sign, primed):
    """Binary64 conversion overflow must not escape the refusal boundary."""
    manifest, publication, verify, memo = entry_case
    if primed:
        assert verify()["ok"] is True
    # Derived from the timestamp check's floating-point exponent range.
    manifest["created_unix"] = sign * (1 << sys.float_info.max_exp)
    publication.write_text(json.dumps(manifest))
    verdict = verify()
    assert verdict["ok"] is False
    assert "created_unix" in verdict["reason"]
    assert not memo


@pytest.mark.parametrize("fault", ["missing", "generation"])
def test_manifest_change_cannot_reuse_a_positive_memo(entry_case, fault):
    manifest, publication, verify, _ = entry_case
    assert verify()["ok"] is True
    if fault == "missing":
        publication.unlink()
    else:
        manifest["generation"] = "another-generation"
        publication.write_text(json.dumps(manifest))
    assert verify()["ok"] is False, "manifest change reused a positive memo"

"""Shared deterministic fixtures for the full-stack integration harness.

Pure data, stdlib only. Fixture payloads use sorted keys and sha256 naming
so the future deterministic join is byte-stable; these shapes are the
stable portion the PQ join will consume once root's API bindings land.
Small by construction: MiB-scale payloads generated in-test, never
committed binaries, never huge-payload hashes in the repo.
"""

from __future__ import annotations

import gzip
import hashlib
import json

#: Whole-file shard: one entry, offset zero.
WHOLE_BYTES = 1 << 20
#: Split range: nonzero offset into a larger declared file.
SPLIT_OFFSET = 1 << 20
SPLIT_BYTES = 3 << 20
SPLIT_FILE_BYTES = 8 << 20


def corpus() -> dict[str, bytes]:
    """Deterministic HDD-source corpus: {declared_path: bytes}."""
    whole = hashlib.sha256(b"fullstack-whole").digest() * (WHOLE_BYTES // 32)
    split_file = hashlib.sha256(b"fullstack-split").digest() * (SPLIT_FILE_BYTES // 32)
    head = hashlib.sha256(b"fullstack-head").digest() * ((1 << 18) // 32)
    return {
        "/pool/model/shard-0.bin": whole,
        "/pool/model/shard-1.bin": split_file,
        "/pool/model/head.bin": head,
    }


def manifest_entry(path: str, offset: int, payload: bytes) -> dict[str, object]:
    return {"path": path, "offset": offset, "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest()}


def gzip_member(payload: dict) -> tuple[bytes, str]:
    """Deterministic gzip seal (mtime=0) plus wire digest."""
    raw = gzip.compress(json.dumps(payload, sort_keys=True).encode(), mtime=0)
    return raw, hashlib.sha256(raw).hexdigest()

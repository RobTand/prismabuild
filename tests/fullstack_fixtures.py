"""Small deterministic source bytes for PB mover and map fixtures."""

from __future__ import annotations

import hashlib

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


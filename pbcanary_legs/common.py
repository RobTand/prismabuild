"""Shared deterministic bytes + digest helpers for pbcanary legs 3-4.

This module is deliberately tiny: the deterministic byte generator and the
digest helpers, nothing else. No PrismaBuild imports, stdlib only, so both
the canary driver (submit side) and the sealed worker actions (execute side,
``python3 pbcanary_legs/legN.py --run-action``) can use it without a
configured environment. See PB #688.

Contract:
  * ``deterministic_bytes(seed, size)`` is a pure function: the same
    ``(seed, size)`` always yields the same bytes on any box, any
    interpreter. It is SHA-256 in counter mode, domain-separated by
    ``PBCANARY_DOMAIN``.
  * ``canonical_json(obj)`` is the single envelope encoding: ``sort_keys``
    plus compact separators. Envelopes compared bitwise (leg 4) must be
    produced by this function and must not embed hostnames, timestamps,
    paths, or anything else that varies per box or per run.
"""

from __future__ import annotations

import hashlib
import json
from typing import Iterable, Iterator

#: Domain separation for every canary byte stream. Changing this value
#: changes every expected digest pinned in leg3/leg4; that is intentional:
#: digests are fixed expectations, and ``build()`` in each leg refuses
#: unless recomputation matches the pinned constants.
PBCANARY_DOMAIN = b"prismabuild-pbcanary-v1"

#: Chunk size for streaming generation/hashing helpers. Not part of any
#: sealed identity; callers may use any granularity.
_STREAM_CHUNK = 1 << 20


def deterministic_bytes(seed: bytes, size: int) -> bytes:
    """Return exactly ``size`` deterministic bytes derived from ``seed``.

    SHA-256 counter mode: block ``i`` is
    ``sha256(PBCANARY_DOMAIN || seed || i_be64)``. Raises ``ValueError``
    on a negative size or an empty seed.
    """
    if not isinstance(seed, bytes) or not seed:
        raise ValueError("seed must be non-empty bytes")
    if not isinstance(size, int) or size < 0:
        raise ValueError("size must be a non-negative int")
    out = bytearray()
    counter = 0
    while len(out) < size:
        block = hashlib.sha256(
            PBCANARY_DOMAIN + seed + counter.to_bytes(8, "big")
        ).digest()
        out += block
        counter += 1
    del block
    return bytes(out[:size])


def deterministic_stream(
    seed: bytes, size: int, *, chunk: int = _STREAM_CHUNK
) -> Iterator[bytes]:
    """Yield ``size`` deterministic bytes in ``chunk``-sized pieces.

    Same bytes as ``deterministic_bytes(seed, size)``, without holding them
    all at once. Used by the driver when staging multi-MiB chunk files.
    """
    if not isinstance(chunk, int) or chunk <= 0:
        raise ValueError("chunk must be a positive int")
    remaining = size
    offset = 0
    while remaining > 0:
        take = min(chunk, remaining)
        # Counter mode is random-access: block index = offset // 32.
        start_block = offset // 32
        skip = offset % 32
        buf = bytearray()
        block_index = start_block
        while len(buf) < skip + take:
            buf += hashlib.sha256(
                PBCANARY_DOMAIN + seed + block_index.to_bytes(8, "big")
            ).digest()
            block_index += 1
        yield bytes(buf[skip : skip + take])
        offset += take
        remaining -= take


def sha256_hex(data: bytes) -> str:
    """Hex SHA-256 of ``data``."""
    return hashlib.sha256(data).hexdigest()


def sha256_stream_hex(chunks: Iterable[bytes]) -> str:
    """Hex SHA-256 over concatenated ``chunks``, hashed incrementally."""
    digest = hashlib.sha256()
    for piece in chunks:
        digest.update(piece)
    return digest.hexdigest()


def sha256_file_hex(path: str, *, offset: int = 0, size: int | None = None) -> str:
    """Hex SHA-256 of the byte range ``[offset, offset+size)`` of ``path``.

    ``size=None`` hashes to end-of-file. Reads in 1 MiB windows so the
    worker action never holds a whole staged chunk in memory.
    """
    if offset < 0:
        raise ValueError("offset must be non-negative")
    if size is not None and size < 0:
        raise ValueError("size must be non-negative or None")
    digest = hashlib.sha256()
    remaining = size
    with open(path, "rb") as handle:
        handle.seek(offset)
        while remaining is None or remaining > 0:
            want = _STREAM_CHUNK if remaining is None else min(_STREAM_CHUNK, remaining)
            piece = handle.read(want)
            if not piece:
                break
            digest.update(piece)
            if remaining is not None:
                remaining -= len(piece)
    if size is not None and remaining:
        raise ValueError(f"short read: {path} has fewer than offset+size bytes")
    return digest.hexdigest()


def canonical_json(obj: object) -> str:
    """Encode ``obj`` as canonical JSON: sorted keys, compact separators."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))

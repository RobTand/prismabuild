#!/usr/bin/env python3
"""One PB-admitted native filesystem witness, not an admission bypass.

The existing publisher/supervisor authority must authorize each actual capture.
This helper creates no registration, offer, pin, mount or cleanup operation.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import socket
import sys

SOURCE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE_ROOT / "src"))
from prismabuild import core, filesystem_capacity as fs  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, action="append")
    parser.add_argument("--mapping", required=True)
    args = parser.parse_args()
    mapping = core._decode_strict_json(args.mapping.encode(), where="native owner mapping")
    if not isinstance(mapping, dict) or set(mapping) != {"owner", "constraints"}:
        raise fs.LocalScratchError("closed native owner mapping required")
    if [entry["root"] for entry in mapping["constraints"]] != args.root:
        raise fs.LocalScratchError("capture roots differ from admitted owner mapping")
    frames = [fs.capture_filesystem_capacity(root) for root in args.root]
    witness = {"schema": fs.WITNESS_SCHEMA, "host": socket.gethostname(), "frames": frames}
    payload = core._canonical_bytes(witness)
    if len(payload) > fs.FILESYSTEM_FRAME_MAX_BYTES:
        raise fs.LocalScratchError("native namespace witness exceeds its result bound")
    # The existing standard captured-log recipe owns the only result write.
    # No special pbrun wrapper, unbound extra_params or direct-argv fiction.
    sys.stdout.buffer.write(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

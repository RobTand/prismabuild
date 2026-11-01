#!/usr/bin/env python3
"""Measure real AF_UNIX binds while exercising pytest's external temp helper.

Run with pbrun, giving --output as its declared result. Failed test runs keep
this diagnostic file locally but publish no success receipt.
"""
import argparse
import json
import os
from pathlib import Path
import socket
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild.pytest_temp import pytest_basetemp


def main():
    import pytest

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("files", nargs="+")
    args = parser.parse_args()
    binds = []
    original = socket.socket.bind

    def measured(sock, address):
        result = original(sock, address)
        if sock.family == socket.AF_UNIX and isinstance(address, (str, bytes)):
            encoded = os.fsencode(address)
            if encoded and encoded[0] != 0:
                binds.append({"path": os.fsdecode(encoded), "bytes": len(encoded)})
        return result

    class ShardMeasurements:
        @pytest.hookimpl(hookwrapper=True)
        def pytest_runtest_call(self, item):
            yield
            # This regression writes the exact socket path only after its
            # child shard binds it. Read before tmp_path fixture teardown.
            if item.originalname != "test_real_shard_binds_socket_and_cleans_its_base_temp":
                return
            root = item.funcargs["tmp_path"]
            for record in root.rglob("socket-path.json"):
                path = json.loads(record.read_text())
                binds.append({"path": path, "bytes": len(os.fsencode(path)),
                              "source": "real child shard"})

    socket.socket.bind = measured
    try:
        with pytest_basetemp() as base:
            code = int(pytest.main(["-q", "-p", "no:cacheprovider", "--basetemp", str(base), *args.files], plugins=[ShardMeasurements()]))
        record = {"returncode": code, "base": str(base), "removed": not base.parent.exists(),
                  "binds": len(binds), "longest": max(binds, key=lambda row: row["bytes"], default=None)}
        args.output.write_text(json.dumps(record, indent=2) + "\n")
        print("pytest-socket-measurement: " + json.dumps(record), flush=True)
        return code
    finally:
        socket.socket.bind = original


if __name__ == "__main__":
    raise SystemExit(main())

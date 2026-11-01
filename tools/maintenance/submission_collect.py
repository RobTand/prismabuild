"""Collect the actual native roster in an admitted action, without truncation."""
import base64
import contextlib
import io
import json
import sys
import zlib
import pytest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild.digest_primitives import raw_sha256
from prismabuild.pytest_temp import pytest_basetemp


class Roster:
    nodes = []

    def pytest_collection_finish(self, session):
        self.nodes = [item.nodeid for item in session.items]


def main():
    roster = Roster()
    output = io.StringIO()
    with pytest_basetemp() as base, contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
        code = pytest.main(["--collect-only", "-q", f"--basetemp={base}", *(sys.argv[1:] or ["tests"])], plugins=[roster])
    raw = json.dumps(roster.nodes, separators=(",", ":")).encode()
    print(json.dumps({"returncode": int(code), "native": len(roster.nodes),
        "whole_files": len({node.split("::")[0] for node in roster.nodes}),
        "roster_sha256": raw_sha256(raw),
        "roster_zlib_base64": base64.b64encode(zlib.compress(raw)).decode()}))
    if code:
        print(output.getvalue())
    return int(code)


if __name__ == "__main__":
    raise SystemExit(main())

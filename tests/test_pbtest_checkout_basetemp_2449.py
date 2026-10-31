"""The default pytest temp belongs to the materialized checkout (#2449)."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from test_pbtest_sealed_basetemp_1469 import _dispatch


@pytest.mark.parametrize("action_identity", [True, False], ids=["pool", "no-identity"])
def test_default_basetemp_is_inside_checkout_even_with_shared_tmpdir(
    tmp_path, monkeypatch, action_identity,
):
    command = _dispatch(tmp_path, monkeypatch, [])
    program = command[command.index("-c") + 1].split("# A pbtest shard:", 1)[0]
    checkout = tmp_path / "checkout"
    shared = tmp_path / "shared-tmpdir"
    shared.mkdir()
    environment = dict(os.environ, TMPDIR=str(shared))
    for name in ("PRISMABUILD_ACTION_KEY", "PRISMABUILD_ACTION_NONCE"):
        environment.pop(name, None)
    if action_identity:
        environment["PRISMABUILD_ACTION_KEY"] = "a" * 64
        environment["PRISMABUILD_ACTION_NONCE"] = "b" * 32
    result = subprocess.run(
        [sys.executable, "-c", program + "\n"
         "import json, pathlib, sys\n"
         "base = (str(pathlib.Path(sys.argv[sys.argv.index('--basetemp') + 1]).resolve())\n"
         "        if '--basetemp' in sys.argv else None)\n"
         "print(json.dumps({'base': base, 'tmpdir': _pb_os.environ['TMPDIR']}))\n",
         "{}", "-q", "tests/test_one.py"],
        cwd=checkout, env=environment, capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    record = json.loads(result.stdout)
    assert record["base"] is not None, "shard sealed no per-action pytest basetemp"
    assert Path(record["base"]).is_relative_to(checkout), record
    assert record["tmpdir"] == str(shared)
    assert list(shared.iterdir()) == []

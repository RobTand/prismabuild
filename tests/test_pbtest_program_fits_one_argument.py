"""A shard's program is one exec argument, and Linux caps each argument.

``execve`` refuses any single argument longer than ``MAX_ARG_STRLEN`` (32
pages: 131,072 bytes with 4 KiB pages, its NUL included) with E2BIG.  pbtest
carries every helper inside the one ``python -c`` program, so the largest
program -- trace, capability digests, dependency pins, a sealed basetemp
and a requested tmpdir together -- must stay below it.  It stood at 123,863
bytes before the owner-lock module joined it, which alone would have pushed
it over; the largest helpers now travel compressed.
"""
from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pbtest_argument", ROOT / "tools" / "fleet" / "pbtest.py")
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)

MAX_ARG_STRLEN = 32 * 4096


def test_the_largest_shard_program_is_one_exec_argument(tmp_path):
    checkout = tmp_path / "co"
    (checkout / "tools").mkdir(parents=True)
    (checkout / "tools" / "resolve_pinned_dev_pin.py").write_text("")
    program = pbtest.shard_entry(
        sys.executable, checkout, tmpdir="/worker/scratch",
        basetemp="/qualified/scratch", trace=True, collection=True,
        capabilities=True)[2]
    assert "pbtest_pins" in program and "PACKED" in program
    assert len(os.fsencode(program)) + 1 <= MAX_ARG_STRLEN - 16 * 1024, (
        "the largest shard program has less than 16 KiB of headroom left")
    # The kernel agrees: the argument execs.
    result = subprocess.run([sys.executable, "-c", "pass", program],
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr


def test_a_packed_helper_unpacks_to_its_exact_bytes():
    source = ROOT / "src" / "prismabuild" / "resource_scope.py"
    import base64
    import zlib
    unpacked = zlib.decompress(base64.b85decode(pbtest.packed_source(source)))
    assert unpacked == source.read_bytes()

"""A passing test's tmp_path is removed when it ends; a failing test's is kept.

On 2026-10-05 the RAM tmpfs that holds the CPU suites' scratch ran out of
inodes, not bytes: six concurrent full suites left 750,000 inodes in pytest's
retained run directories and every action placed on the host failed in preflight
with OSError 28 while the filesystem reported free space. pytest keeps a passing
test's directory by default. ``tmp_path_retention_policy = "failed"`` in
``pyproject.toml`` removes it at once; ``pbtest`` refuses a command-line ``-o``
and seals an empty ``PYTEST_ADDOPTS``, so the setting has to live in the checkout.

The behaviour is proved by a real inner pytest run in a subprocess that loads this
repository's own ``pyproject.toml``, so it holds whatever order or worker the outer
test is scheduled on (a ``pbtest --workers-per-shard N`` shard runs ``pytest -n N``):
one inner test passes and one fails, each recording its ``tmp_path``.  Afterwards
the passing test's directory is gone and the failing test's is still there.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

INNER = '''
import os
from pathlib import Path

def _record(name, path):
    with open(os.environ["PB_TMP_RECORD"], "a") as handle:
        handle.write(f"{name}\\t{path}\\n")

def test_passes(tmp_path):
    (tmp_path / "marker").write_text("x")
    _record("passes", tmp_path)

def test_fails(tmp_path):
    (tmp_path / "marker").write_text("x")
    _record("fails", tmp_path)
    assert False, "kept for diagnosis"
'''


def test_the_configuration_asks_for_the_failed_only_policy(request):
    assert request.config.getini("tmp_path_retention_policy") == "failed"


def test_a_passing_tests_directory_is_removed_and_a_failing_ones_is_kept(tmp_path):
    inner = tmp_path / "inner"
    inner.mkdir()
    (inner / "test_inner.py").write_text(INNER)
    record = tmp_path / "record.tsv"
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("PYTEST_", "PBTEST_"))}
    env.update({"PB_TMP_RECORD": str(record), "PYTHONDONTWRITEBYTECODE": "1"})
    done = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
         "-c", str(ROOT / "pyproject.toml"), "--rootdir", str(inner),
         f"--basetemp={tmp_path / 'inner-base'}", str(inner / "test_inner.py")],
        cwd=inner, env=env, capture_output=True, text=True, timeout=180)
    # One inner test fails on purpose, so a clean exit would mean it was not run.
    assert done.returncode == 1, done.stdout + done.stderr
    paths = dict(line.split("\t") for line in record.read_text().splitlines())
    assert set(paths) == {"passes", "fails"}, paths
    assert not Path(paths["passes"]).exists(), f"{paths['passes']} survived its passing test"
    assert Path(paths["fails"]).exists(), f"{paths['fails']} was removed with its failing test"

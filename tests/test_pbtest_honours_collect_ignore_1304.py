"""A shard honours a conftest ``collect_ignore`` for the files pbtest names (#1304).

pbtest expands a directory into file names, and pytest never consults its
ignore hook for a path named on the command line.  A suite whose conftest
computes ``collect_ignore`` at run time (a torch or numpy import probe) then
reports a collection error for every module the interpreter cannot import.
The shard runs under the target interpreter, so it is the one place that can
apply the same rules the suite's own directory collection applies.
"""
from pathlib import Path
import json
import subprocess
import sys

import pytest

FLEET = Path(__file__).resolve().parents[1] / "tools/fleet"
sys.path.insert(0, str(FLEET))
import pbtest  # noqa: E402



def run_shard(root: Path, *named: str) -> tuple[int, dict]:
    argv = ["-q", "-p", "no:cacheprovider", *named]
    proc = subprocess.run(
        [*pbtest.shard_entry(sys.executable, root), *argv],
        cwd=root, capture_output=True, text=True, timeout=120)
    lines = [ln for ln in proc.stdout.splitlines()
             if ln.startswith("pbtest-outcomes: ")]
    assert lines, proc.stdout + proc.stderr
    return proc.returncode, json.loads(lines[-1][len("pbtest-outcomes: "):])


def make_suite(root: Path, conftest: str) -> None:
    tests = root / "tests"
    tests.mkdir()
    (tests / "conftest.py").write_text(conftest)
    (tests / "test_ok.py").write_text("def test_ok():\n    assert True\n")
    (tests / "test_needs_missing.py").write_text(
        "import a_module_that_does_not_exist_1304\n\n"
        "def test_never():\n    assert True\n")


def test_named_file_in_collect_ignore_is_not_collected(tmp_path):
    make_suite(tmp_path, 'collect_ignore = ["test_needs_missing.py"]\n')
    code, record = run_shard(
        tmp_path, "tests/test_ok.py", "tests/test_needs_missing.py")
    errors = [r for r in record["reports"] if r[2] == "error"]
    assert errors == []
    assert code == 0
    assert record["collected"] == ["tests/test_ok.py::test_ok"]


def test_named_file_in_collect_ignore_glob_is_not_collected(tmp_path):
    make_suite(tmp_path, 'collect_ignore_glob = ["*_missing.py"]\n')
    code, record = run_shard(
        tmp_path, "tests/test_ok.py", "tests/test_needs_missing.py")
    assert code == 0
    assert record["collected"] == ["tests/test_ok.py::test_ok"]


def test_runtime_computed_collect_ignore_applies(tmp_path):
    make_suite(tmp_path, (
        "def _probe():\n"
        "    try:\n"
        "        import a_module_that_does_not_exist_1304\n"
        "    except ImportError:\n"
        "        return ['test_needs_missing.py']\n"
        "    return []\n\n"
        "collect_ignore = _probe()\n"))
    code, record = run_shard(tmp_path, "tests/test_needs_missing.py",
                             "tests/test_ok.py")
    assert code == 0
    assert record["collected"] == ["tests/test_ok.py::test_ok"]


def test_a_shard_of_only_ignored_files_does_not_widen_to_the_suite(tmp_path):
    make_suite(tmp_path, 'collect_ignore = ["test_needs_missing.py"]\n')
    code, record = run_shard(tmp_path, "tests/test_needs_missing.py")
    assert record["collected"] == []
    assert code == 5  # pytest's "no tests collected", not a run of test_ok


def test_a_named_file_outside_collect_ignore_still_errors(tmp_path):
    make_suite(tmp_path, "collect_ignore = []\n")
    code, record = run_shard(tmp_path, "tests/test_needs_missing.py")
    assert [r[2] for r in record["reports"]] == ["error"]
    assert code != 0

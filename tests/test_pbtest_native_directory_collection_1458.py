"""PB file buckets preserve native directory collection, including in xdist.

Only submission is replaced: generated shard payloads execute directly inside
the enclosing admitted action. The oracle runs pytest's real directory path.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from pbtest_shard_output import admitted_child

import pytest


ROOT = Path(__file__).resolve().parents[1]
FLEET = ROOT / "tools" / "fleet"
SPEC = importlib.util.spec_from_file_location("pbtest_directory_1458", FLEET / "pbtest.py")
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)
REAL_POPEN = subprocess.Popen
MISSING = "pbtest_missing_dependency_1458"


def _checkout(tmp_path):
    root = tmp_path / "project"
    tests = root / "tests"
    nested = tests / "nested"
    nested.mkdir(parents=True)
    (root / "pytest.ini").write_text("[pytest]\n")
    (tests / "conftest.py").write_text('''\
collect_ignore = ["test_root_ignored.py"]
collect_ignore_glob = ["*_glob_ignored.py"]

def pytest_ignore_collect(collection_path, config):
    if collection_path.name == "test_hook_ignored.py":
        return True
''')
    (nested / "conftest.py").write_text('''\
collect_ignore = ["test_nested_ignored.py"]
collect_ignore_glob = ["*_glob_ignored.py"]
''')
    for relative in ["test_root_ignored.py", "test_root_glob_ignored.py",
                     "test_hook_ignored.py", "nested/test_nested_ignored.py",
                     "nested/test_nested_glob_ignored.py"]:
        (tests / relative).write_text(f"import {MISSING}\n")
    (tests / "test_a_visible.py").write_text('''\
import pytest
@pytest.mark.parametrize("value", [1, 2], ids=["one", "two"])
def test_visible(value):
    assert value > 0
''')
    (tests / "test_b_visible.py").write_text("def test_visible(): pass\n")
    for name in ["test_nested_a_visible.py", "test_nested_b_visible.py"]:
        (nested / name).write_text("def test_visible(): pass\n")
    (tests / "test_importorskip.py").write_text(
        f"import pytest\npytest.importorskip('{MISSING}')\n"
        "def test_never_collected(): pass\n")
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    return root


def _directory_oracle(root, workers, paths=("tests",)):
    argv = ["-q", "-p", "no:cacheprovider", *paths]
    if workers > 1:
        argv += ["-n", str(workers), "--dist", "worksteal"]
    program = (
        f"import sys; sys.path.insert(0, {str(FLEET)!r}); "
        f"import pbtest_outcomes; raise SystemExit(pbtest_outcomes.main({argv!r}))")
    completed = subprocess.run([sys.executable, "-c", program], cwd=root,
                               capture_output=True, text=True, timeout=120)
    record = pbtest.pbtest_outcomes.parse(completed.stdout)
    assert record is not None, completed.stdout + completed.stderr
    return completed.returncode, record


def _dispatch(root, monkeypatch, *, workers, shards=1, paths=("tests",)):
    calls = []

    def admitted_payload(command, **kwargs):
        if str(pbtest.PBRUN) not in [str(part) for part in command]:
            return REAL_POPEN(command, **kwargs)
        calls.append(command)
        argv, environment = admitted_child(command)
        return REAL_POPEN(argv, cwd=root, env=environment, **kwargs)

    monkeypatch.setattr(pbtest.subprocess, "Popen", admitted_payload)
    report = root.parent / "shards.json"
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(root), "--python", sys.executable,
        "--shards", str(shards), "--workers-per-shard", str(workers),
        "--threads-per-shard", "1", "--pytest-args", '["--dist","worksteal"]'
        if workers > 1 else "[]", "--json", str(report), *paths,
    ])
    code = pbtest.main()
    results = json.loads(report.read_text()) if report.exists() else []
    records = [pbtest.pbtest_outcomes.parse(row["output"]) for row in results]
    assert all(record is not None for record in records), results
    return code, calls, results, records


def _collected(records):
    return sorted(node for record in records for node in record["collected"])


def _collect_reports(records):
    return sorted(tuple(row[:3]) for record in records for row in record["reports"]
                  if row[1] == "collect")


@pytest.mark.parametrize("workers", [1, 2])
@pytest.mark.parametrize("shards", [1, 2])
def test_native_directory_population_survives_pb_file_fanout(
        tmp_path, monkeypatch, workers, shards):
    if workers > 1:
        pytest.importorskip("xdist")
    root = _checkout(tmp_path)
    oracle_code, oracle = _directory_oracle(root, workers)
    assert oracle_code == 0, oracle
    assert len(oracle["collected"]) == 5
    assert any("[one]" in node for node in oracle["collected"])
    assert _collect_reports([oracle]) == [
        ("tests/test_importorskip.py", "collect", "skipped")]

    code, calls, results, records = _dispatch(
        root, monkeypatch, workers=workers, shards=shards)
    assert calls, "the real generated shard program was not exercised"
    assert code == 0, results
    assert _collected(records) == _collected([oracle])
    assert _collect_reports(records) == _collect_reports([oracle])
    assert all(not row["reconciliation"]["problems"] for row in results)


@pytest.mark.parametrize("workers", [1, 2])
def test_an_explicit_user_file_preserves_pytest_ignore_override(
        tmp_path, monkeypatch, workers):
    if workers > 1:
        pytest.importorskip("xdist")
    root = _checkout(tmp_path)
    paths = ("tests/test_root_ignored.py",)
    native = subprocess.run([sys.executable, "-m", "pytest", "-q", *paths],
                            cwd=root, capture_output=True, text=True, timeout=120)
    assert native.returncode != 0 and MISSING in native.stdout
    code, _, _, records = _dispatch(root, monkeypatch, workers=workers, paths=paths)
    assert code != 0
    assert _collect_reports(records) == [
        ("tests/test_root_ignored.py", "collect", "error")]


@pytest.mark.parametrize("workers", [1, 2])
@pytest.mark.parametrize("broken", [f"import {MISSING}\n", "broken Python !!!\n"],
                         ids=["import-error", "syntax-error"])
def test_a_genuine_collection_error_is_not_filtered_from_the_bucket(
        tmp_path, monkeypatch, workers, broken):
    if workers > 1:
        pytest.importorskip("xdist")
    root = _checkout(tmp_path)
    (root / "tests" / "test_genuine_error.py").write_text(broken)
    oracle_code, oracle = _directory_oracle(root, workers)
    assert oracle_code != 0
    code, _, _, records = _dispatch(root, monkeypatch, workers=workers)
    assert code != 0
    assert _collected(records) == _collected([oracle])
    assert _collect_reports(records) == _collect_reports([oracle])


@pytest.mark.parametrize("workers", [1, 2])
def test_an_ignored_only_directory_never_widens_to_unrequested_tests(
        tmp_path, monkeypatch, workers):
    if workers > 1:
        pytest.importorskip("xdist")
    root = _checkout(tmp_path)
    directory = root / "tests" / "only_ignored"
    directory.mkdir()
    (directory / "conftest.py").write_text('collect_ignore = ["test_ignored.py"]\n')
    (directory / "test_ignored.py").write_text(f"import {MISSING}\n")
    paths = ("tests/only_ignored",)
    _, oracle = _directory_oracle(root, workers, paths)
    code, calls, _, records = _dispatch(root, monkeypatch, workers=workers, paths=paths)
    assert calls
    assert code != 0, "an empty population cannot certify a test pass"
    assert _collected(records) == _collected([oracle]) == []
    assert _collect_reports(records) == _collect_reports([oracle]) == []


def test_a_missing_requested_file_is_a_named_refusal(tmp_path, monkeypatch, capsys):
    root = _checkout(tmp_path)
    missing = "tests/test_missing_1458.py"
    code, calls, _, records = _dispatch(root, monkeypatch, workers=1, paths=(missing,))
    output = capsys.readouterr()
    assert code != 0 and not calls and not records
    assert missing in output.err + output.out

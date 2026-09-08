"""GPU fanout and population options retain PB's resource/file ownership."""
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from test_pbtest_reserves_its_threads import _dispatch, pbtest


def test_gpu_budget_and_population_options_reach_the_shard(tmp_path, monkeypatch):
    code, calls = _dispatch(tmp_path, monkeypatch, [
        "--gpu", "--gpu-memory-gb", "2.5", "--mem-gb", "8", "--tag", "gb10",
        "--workers-per-shard", "2", "--threads-per-shard", "1",
        "--pytest-args", json.dumps([
            "--strict-cuda", "--surface-json", "surface.json", "--dist", "worksteal",
            "-k", "native and not slow", "--durations=10"]),
    ])
    assert code == 0
    command, = calls
    flags, payload = command[:command.index("--")], command[command.index("--") + 1:]
    assert "--gpu" in flags
    assert flags[flags.index("--gpu-memory-gb") + 1] == "2.5"
    assert flags[flags.index("--demand") + 1] == "mem_gb=8"
    assert flags[flags.index("--cpus") + 1] == "2"
    assert "--strict-cuda" in payload
    assert payload[payload.index("--surface-json") + 1] == "surface.shard-0.json"
    assert payload[payload.index("--dist") + 1] == "worksteal"
    assert payload[payload.index("-k") + 1] == "native and not slow"
    assert payload[payload.index("-n") + 1] == "2"


@pytest.mark.parametrize("extra", [
    ["--gpu-memory-gb", "2"],
    ["--gpu", "--gpu-memory-gb", "nan"],
    ["--gpu", "--gpu-memory-gb", "0"],
    ["--gpu", "--gpu-memory-gb", "-1"],
    ["--gpu", "--gpu-memory-gb", "2", "--transport", "slurm"],
    ["--mem-gb", "0"],
    ["--pytest-args", '{"-n": 80}'],
    ["--pytest-args", '[1]'],
    ["--pytest-args", 'not json'],
    *[["--pytest-args", json.dumps(args)] for args in [
        ["-n", "auto"], ["-n8"], ["--numprocesses=8"],
        ["--tx", "8*popen"], ["-d"], ["--dist", "each"],
        ["-o", "addopts=-n80"], ["--override-ini=addopts=-n80"],
        ["-c", "alternate.ini"], ["--", "tests"], ["tests/extra.py"],
        ["--strict-cuda"], ["--surface-json"], ["--surface-json="],
        ["--surface-json", "/"], ["--surface-json", "."],
        ["--dist", "worksteal"], ["--unknown-plugin-option"],
    ]],
])
def test_invalid_options_submit_nothing(tmp_path, monkeypatch, extra):
    code, calls = _dispatch(tmp_path, monkeypatch, extra)
    assert code == 2
    assert not calls


def test_real_pytest_population_reports_are_separate_and_addopts_cannot_expand_workers(
    tmp_path, monkeypatch,
):
    # Capture the submission boundary, then run its actual pytest payload as
    # a child of this admitted test action. No recursive PB submissions.
    real_run = subprocess.run
    monkeypatch.setattr(pbtest, "discover", lambda *_: [
        "tests/test_one.py", "tests/test_two.py"])
    code, calls = _dispatch(tmp_path, monkeypatch, [
        "--shards", "2",
        "--workers-per-shard", "2", "--threads-per-shard", "1",
        "--pytest-args", json.dumps(["--surface-json", "surface.json",
                                     "--dist", "worksteal", "-k", "one"]),
    ])
    assert code == 0
    monkeypatch.undo()
    checkout = tmp_path / "checkout"
    (checkout / "tests/test_two.py").write_text("def test_another_one():\n    assert True\n")
    (checkout / "pytest.ini").write_text("[pytest]\naddopts = -n 99\n")
    (checkout / "conftest.py").write_text('''
import json
from pathlib import Path
def pytest_addoption(parser):
    parser.addoption("--surface-json")
def pytest_sessionfinish(session):
    if hasattr(session.config, "workerinput"):
        return
    Path(session.config.getoption("--surface-json")).write_text(json.dumps({
        "workers": session.config.getoption("numprocesses"),
        "collected": session.testscollected,
    }))
''')
    monkeypatch.setenv("PYTEST_ADDOPTS", "-n 88")
    assert len(calls) == 2
    def execute(command):
        payload = command[command.index("--") + 1:]
        payload[payload.index("/target/python")] = sys.executable
        return real_run(payload, cwd=checkout, capture_output=True, text=True)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(execute, calls))
    for index, result in enumerate(results):
        assert result.returncode == 0, result.stdout + result.stderr
        assert json.loads((checkout / f"surface.shard-{index}.json").read_text()) == {
            "workers": 2, "collected": 1,
        }


def test_gpu_default_placement_uses_the_fleets_gpu_class(tmp_path, monkeypatch):
    monkeypatch.setattr(pbtest, "RUNTIME_ROOT", Path("/mnt/shared/published-runtime"))
    code, calls = _dispatch(tmp_path, monkeypatch, ["--gpu"])
    assert code == 0
    assert calls[0][calls[0].index("--tag") + 1] == "gb10"


def test_cpu_class_tag_alone_does_not_request_gpu(tmp_path, monkeypatch):
    code, calls = _dispatch(tmp_path, monkeypatch, ["--tag", "gb10"])
    assert code == 0
    assert "--gpu" not in calls[0][:calls[0].index("--")]


def test_report_path_expansion_keeps_shards_distinct():
    assert pbtest.shard_pytest_args(["--surface-json", "surface.json"], 0) == [
        "--surface-json", "surface.shard-0.json"]
    assert pbtest.shard_pytest_args(["--surface-json", "surface.json"], 1) == [
        "--surface-json", "surface.shard-1.json"]
    assert pbtest.shard_pytest_args(["--surface-json", "out-{shard}.json"], 1) == [
        "--surface-json", "out-1.json"]

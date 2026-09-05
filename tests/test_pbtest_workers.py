"""Shard parallelism reserves every pytest worker's native thread ceiling."""
import pytest

from test_pbtest_reserves_its_threads import _dispatch, _demand


@pytest.mark.parametrize("workers,threads,cpus", [(2, 1, 2), (4, 2, 8), (5, 3, 15)])
def test_workers_reach_pytest_and_multiply_the_reservation(tmp_path, monkeypatch, workers, threads, cpus):
    code, calls = _dispatch(tmp_path, monkeypatch, [
        "--workers-per-shard", str(workers), "--threads-per-shard", str(threads)])
    assert code == 0
    command, = calls
    payload = command[command.index("--") + 1:]
    assert payload[payload.index("-n") + 1] == str(workers)
    assert _demand(command)["cpu"] == cpus
    assert f"OMP_NUM_THREADS={threads}" in payload
    assert f"OPENBLAS_NUM_THREADS={threads}" in payload


def test_default_does_not_require_xdist(tmp_path, monkeypatch):
    code, calls = _dispatch(tmp_path, monkeypatch, [])
    assert code == 0
    assert "-n" not in calls[0]


@pytest.mark.parametrize("extra", [
    ["--workers-per-shard", "0"],
    ["--workers-per-shard", "-1"],
    ["--workers-per-shard", "4", "--threads-per-shard", "2", "--cpus-per-shard", "7"],
    ["--workers-per-shard", "4", "--threads-per-shard", "0", "--cpus-per-shard", "3"],
])
def test_invalid_width_is_refused_before_submission(tmp_path, monkeypatch, extra):
    code, calls = _dispatch(tmp_path, monkeypatch, extra)
    assert code == 2
    assert calls == []


def test_explicit_larger_reservation_is_preserved(tmp_path, monkeypatch):
    code, calls = _dispatch(tmp_path, monkeypatch, [
        "--workers-per-shard", "3", "--threads-per-shard", "2", "--cpus-per-shard", "8"])
    assert code == 0
    assert _demand(calls[0])["cpu"] == 8

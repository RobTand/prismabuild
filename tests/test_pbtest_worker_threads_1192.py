"""Each pytest-xdist worker gets its share of the shard's cores (#1192).

Every xdist worker is its own process and inherits the row's native thread
count, which PrismaBuild sets to the whole row's cores. So a shard of N
workers with no ceiling of its own asked for N times its cores: on 2026-09-26
a 16-core shard of 16 workers ran 16 x ``OMP_NUM_THREADS=16`` and put
sparklina at load 136 on 20 CPUs. With several workers and no ceiling named,
each worker now gets ``max(1, cpus // workers)``.
"""
import pytest

from test_pbtest_reserves_its_threads import _dispatch, _demand

_KNOBS = ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
          "TORCH_NUM_THREADS")


def _payload(command):
    return command[command.index("--") + 1:]


@pytest.mark.parametrize("ceiling", [[], ["--threads-per-shard", "0"]],
                         ids=["unset", "zero"])
@pytest.mark.parametrize("workers,cpus,share", [(16, 16, 1), (4, 16, 4), (4, 17, 4),
                                                (3, 8, 2)])
def test_each_worker_gets_its_share_of_the_shard_s_cores(
        tmp_path, monkeypatch, ceiling, workers, cpus, share):
    code, calls = _dispatch(tmp_path, monkeypatch, [
        "--workers-per-shard", str(workers), "--cpus-per-shard", str(cpus), *ceiling])
    assert code == 0
    command, = calls
    payload = _payload(command)
    assert payload[payload.index("-n") + 1] == str(workers)
    for knob in _KNOBS:
        assert f"{knob}={share}" in payload
    # The reservation stays the one named: the share divides it, never grows it.
    assert _demand(command)["cpu"] == cpus


def test_a_named_ceiling_is_kept(tmp_path, monkeypatch):
    code, calls = _dispatch(tmp_path, monkeypatch, [
        "--workers-per-shard", "4", "--threads-per-shard", "2", "--cpus-per-shard", "16"])
    assert code == 0
    assert "OMP_NUM_THREADS=2" in _payload(calls[0])
    assert _demand(calls[0])["cpu"] == 16


def test_a_ceiling_the_reservation_cannot_hold_is_refused(tmp_path, monkeypatch, capsys):
    code, calls = _dispatch(tmp_path, monkeypatch, [
        "--workers-per-shard", "4", "--threads-per-shard", "5", "--cpus-per-shard", "16"])
    assert code == 2 and calls == []
    assert "at least 20" in capsys.readouterr().err


def test_fewer_cores_than_workers_is_refused(tmp_path, monkeypatch, capsys):
    code, calls = _dispatch(tmp_path, monkeypatch, [
        "--workers-per-shard", "4", "--cpus-per-shard", "3"])
    assert code == 2 and calls == []
    assert "at least 4" in capsys.readouterr().err


@pytest.mark.parametrize("extra,threads,cpus", [
    ([], 2, 2),
    (["--workers-per-shard", "4"], 2, 8),
    (["--cpus-per-shard", "6"], 2, 6),
])
def test_without_a_reservation_or_with_one_worker_the_default_stays_two(
        tmp_path, monkeypatch, extra, threads, cpus):
    code, calls = _dispatch(tmp_path, monkeypatch, extra)
    assert code == 0
    assert f"OMP_NUM_THREADS={threads}" in _payload(calls[0])
    assert _demand(calls[0])["cpu"] == cpus


def test_one_worker_with_no_ceiling_is_left_to_the_row(tmp_path, monkeypatch):
    code, calls = _dispatch(tmp_path, monkeypatch, [
        "--threads-per-shard", "0", "--cpus-per-shard", "16"])
    assert code == 0
    assert not [flag for flag in calls[0] if flag.startswith("OMP_NUM_THREADS=")]

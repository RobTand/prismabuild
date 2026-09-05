"""A shard's thread ceiling has to reach its resource demand.

``pbtest --threads-per-shard N`` sets each shard's BLAS and OMP ceiling to N
and used to forward no CPU demand at all, so ``pbrun`` filled the demand from
its own default of one core. Under SLURM the lane emits that as
``--cpus-per-task`` and ``cgroup.conf``'s ``ConstrainCores=yes`` turns it into
a cpuset, so the eight threads a shard was allowed took turns inside one core.
Under the pull queue the ledger admitted the shard as if it used one core and
oversubscribed the box.

``docs/resource_enforcement_2026-09-05.md`` states the same contract from the
other end: an under-declared CPU demand confines a parallel run to one core.

The ``0`` ceiling has no number to derive a reservation from, so it is refused
unless the operator names one.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

_SPEC = importlib.util.spec_from_file_location(
    "pbtest", REPOSITORY / "tools" / "fleet" / "pbtest.py"
)
pbtest = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbtest)                      # type: ignore[union-attr]

from prismabuild.slurm_lane import LaneResources  # noqa: E402


class _FinishedProcess:
    returncode = 0

    def communicate(self):
        return "1 passed in 0.01s\n", None


def _dispatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra):
    """One shard's dispatch, and the pbrun argv it built."""

    checkout = tmp_path / "checkout"
    test_file = checkout / "tests" / "test_one.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("def test_one():\n    assert True\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    calls: list[list[str]] = []

    def _popen(command, **_kwargs):
        calls.append(list(command))
        return _FinishedProcess()

    monkeypatch.setattr(pbtest.subprocess, "Popen", _popen)
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
         "--shards", "1", *extra, "tests"],
    )
    code = pbtest.main()
    return code, calls


def _demand(command) -> dict[str, int]:
    """The demand the lane would price, as ``pbrun`` seals it.

    ``pbrun`` fills the cpu demand from ``--cpus``, so the flag and the
    demand are read together here rather than trusting either alone.
    """

    flags = command[:command.index("--")]
    demand = dict(
        part.split("=", 1)
        for part in flags[flags.index("--demand") + 1].split(",")
    )
    priced = {name: int(count) for name, count in demand.items()}
    priced.setdefault("cpu", int(flags[flags.index("--cpus") + 1]))
    return priced


def test_a_shard_reserves_one_core_per_thread_it_is_allowed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ceiling and the reservation are the same number.

    main: ``--cpus`` on the generated argv equals ``--threads-per-shard``.
    branch: the demand the lane prices from it reserves that many cores, so
    the cpuset a shard runs in is as wide as its thread pool.
    """

    code, calls = _dispatch(
        tmp_path, monkeypatch,
        ["--transport", "slurm", "--threads-per-shard", "8"])

    assert code == 0
    command = calls[0]
    assert command[command.index("--cpus") + 1] == "8"
    assert command.index("--cpus") < command.index("--")
    assert "OMP_NUM_THREADS=8" in command
    assert _demand(command)["cpu"] == 8
    assert LaneResources.from_demand(_demand(command)).cpus == 8


def test_the_default_ceiling_reserves_itself_too(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The default has the same mismatch, so it gets the same pairing.

    branch: ``--threads-per-shard`` defaults to 2, and a suite fanned out
    without either flag reserves two cores per shard rather than one.
    """

    code, calls = _dispatch(tmp_path, monkeypatch, [])

    assert code == 0
    command = calls[0]
    assert command[command.index("--cpus") + 1] == "2"
    assert _demand(command)["cpu"] == 2


def test_an_explicit_reservation_overrides_the_pairing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A suite that wants more cores than threads can say so.

    branch: ``--cpus-per-shard`` is what makes the ``0`` ceiling usable, so
    it has to be honoured when a ceiling is named as well.
    """

    code, calls = _dispatch(
        tmp_path, monkeypatch,
        ["--threads-per-shard", "2", "--cpus-per-shard", "6"])

    assert code == 0
    command = calls[0]
    assert command[command.index("--cpus") + 1] == "6"
    assert "OMP_NUM_THREADS=2" in command


def test_an_unbounded_shard_must_name_its_reservation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    """``0`` sets no ceiling, so no reservation follows from it.

    main: the run is refused with exit 2 and nothing is started, naming the
    flag that makes it runnable.
    """

    code, calls = _dispatch(tmp_path, monkeypatch, ["--threads-per-shard", "0"])

    assert code == 2
    assert calls == []
    assert "--cpus-per-shard" in capsys.readouterr().err


def test_an_unbounded_shard_runs_once_it_names_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The refusal is a policy and not a ban.

    branch: with ``--cpus-per-shard`` beside it the same run is dispatched,
    the shard keeps its unbounded thread pool, and the reservation is the one
    the operator named.
    """

    code, calls = _dispatch(
        tmp_path, monkeypatch,
        ["--threads-per-shard", "0", "--cpus-per-shard", "16"])

    assert code == 0
    command = calls[0]
    assert command[command.index("--cpus") + 1] == "16"
    assert not [flag for flag in command if flag.startswith("OMP_NUM_THREADS=")]


def test_a_negative_ceiling_is_refused_rather_than_forwarded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    """A thread ceiling below zero has no reservation to pair with either.

    branch: forwarded instead, it would reach the target box as
    ``OMP_NUM_THREADS=-1`` and a cpu demand no lane can price.
    """

    code, calls = _dispatch(tmp_path, monkeypatch, ["--threads-per-shard", "-1"])

    assert code == 2
    assert calls == []
    assert "--threads-per-shard" in capsys.readouterr().err


def test_a_reservation_below_one_core_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    """``pbrun`` refuses ``--cpus 0``, so this refuses before submitting.

    branch: twenty shards refused one at a time by pbrun's own argparse would
    be twenty identical messages for one typo.
    """

    code, calls = _dispatch(tmp_path, monkeypatch, ["--cpus-per-shard", "0"])

    assert code == 2
    assert calls == []
    assert "at least 1" in capsys.readouterr().err

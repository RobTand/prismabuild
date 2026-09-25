"""A GPU shard's per-test bound comes from its submitter, never a campaign ceiling (#975).

Since #939 pbtest reads the timeout ceiling every box able to claim a shard
announces, and bounds each test one heartbeat inside the smallest.  Both Sparks
announce 86400 s, the ceiling they accept for campaign work, so a ``--gpu``
run with neither ``--timeout-s`` nor ``--test-timeout-s`` exported a per-test
bound of 86370 s: a hung test held its shard, and the GPU the shard reserved,
for a day before pytest named it.

A test's bound belongs to the suite and to whoever submits it.  So a shard that
reserves a GPU takes its bound from the submission -- ``--test-timeout-s``, or
``--timeout-s`` when only that is given -- and with neither, pbtest refuses
before submitting anything and names both flags.  CPU shards keep deriving
their bound from the ceilings they read, as before.
"""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

from test_pbtest_seals_its_shard_deadline import (  # noqa: E402
    _announce, _exported_bound, _FinishedProcess, _sealed, pbtest,
)
from prismabuild import pool  # noqa: E402

SPARK_CEILING_S = 86400.0


def _run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra, *, shards=2):
    """pbtest's exit code and every shard's ``pbrun`` argv."""

    checkout = tmp_path / "checkout"
    for index in range(shards):
        test_file = checkout / "tests" / f"test_{index}.py"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.write_text("def test_one():\n    assert True\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    calls: list[list[str]] = []

    def _popen(command, **_kwargs):
        calls.append(list(command))
        return _FinishedProcess(command)

    monkeypatch.setattr(pbtest.subprocess, "Popen", _popen)
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
         "--shards", str(shards), *extra, "tests"],
    )
    return pbtest.main(), calls


def test_a_gpu_shard_with_no_declared_bound_is_refused_before_anything_is_submitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """main: both shards submit with ``PRISMABUILD_TEST_TIMEOUT_S=86370``.

    branch: nothing is submitted, and the refusal names the flags that would
    have declared a bound.
    """

    _announce(sparky=(["gb10", "sparky"], SPARK_CEILING_S),
              sparklina=(["gb10", "sparklina"], SPARK_CEILING_S))

    code, calls = _run(tmp_path, monkeypatch, ["--tag", "gb10", "--gpu"])

    assert (code, calls) == (2, []), (
        "a GPU shard was submitted with per-test bound(s) "
        f"{[_exported_bound(command) for command in calls]} s, derived from "
        "the Sparks' campaign ceiling")
    err = capsys.readouterr().err
    assert "--timeout-s" in err and "--test-timeout-s" in err


def test_a_gpu_shard_with_no_declared_bound_is_refused_with_nothing_announced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The refusal is about the submission, not about what the fleet says.

    With no announcement the bound would fall back to the published loop
    default, which is also a box's ceiling and not the suite's.
    """

    code, calls = _run(tmp_path, monkeypatch, ["--tag", "gb10", "--gpu"])

    assert (code, calls) == (2, [])


def test_timeout_s_alone_bounds_a_gpu_shard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _announce(sparky=(["gb10", "sparky"], SPARK_CEILING_S))

    code, calls = _run(tmp_path, monkeypatch,
                       ["--tag", "gb10", "--gpu", "--timeout-s", "3600"])

    assert code == 0 and len(calls) == 2
    for command in calls:
        assert _sealed(command) == 3600.0
        assert _exported_bound(command) == pytest.approx(3600.0 - pool.HEARTBEAT_S)


def test_test_timeout_s_alone_bounds_a_gpu_shard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The per-test bound is the one declared; the shard's end stays the box's."""

    _announce(sparky=(["gb10", "sparky"], SPARK_CEILING_S))

    code, calls = _run(tmp_path, monkeypatch,
                       ["--tag", "gb10", "--gpu", "--test-timeout-s", "900"])

    assert code == 0 and len(calls) == 2
    for command in calls:
        assert _exported_bound(command) == 900.0
        assert _sealed(command) == SPARK_CEILING_S


def test_test_timeout_s_zero_is_a_declared_choice_for_a_gpu_shard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``0`` removes the bound on purpose; it is declared, so it is honoured."""

    _announce(sparky=(["gb10", "sparky"], SPARK_CEILING_S))

    code, calls = _run(tmp_path, monkeypatch,
                       ["--tag", "gb10", "--gpu", "--test-timeout-s", "0"])

    assert code == 0 and len(calls) == 2
    for command in calls:
        assert _exported_bound(command) is None

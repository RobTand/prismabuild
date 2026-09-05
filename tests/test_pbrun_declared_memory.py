"""``pbrun`` says what a declared demand was when the scheduler enforced it.

Under the pull queue, ``--cpus`` and ``--demand mem_gb=`` were admission
declarations that nothing enforced.  Under SLURM with ``ConstrainCores=yes``
and ``ConstrainRAMSpace=yes`` they become a cpuset and a ``memory.max``, so an
under-declared action is slow or killed for a reason that is in the
submission and nowhere else: a job the kernel kills writes nothing to its own
log, and ``mem_gb`` defaults to 4 whether or not the caller typed it.

The measurements behind this are rows 10a-10d of ``fleet/slurm/smoke``; what
the fleet should do about them is ``docs/resource_enforcement_2026-09-05.md``.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402

from test_slurm_lane import _paper_action, fleet  # noqa: E402

__all__ = ["fleet"]

REPOSITORY = Path(__file__).resolve().parents[1]


def _run(tmp_path: Path, name: str, demand: dict) -> int:
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, name)
    return pbrun.slurm_outcome(
        action, cas=cas, request_path=cas.publish_action_request(action),
        tags=[], demand=dict(demand), exclusive=False, timeout_s=600.0,
        wait_s=60.0, retry_safe=False, max_attempts=1,
        runtime_root=REPOSITORY, poll_s=0.0,
    )


def test_an_out_of_memory_job_names_the_memory_it_declared(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Smoke row 10c, ``PB_SMOKE_CONSTRAIN_SWAP=yes`` arm: a job declaring
    ``mem_gb=1`` and writing 3072 MiB ends ``OUT_OF_MEMORY``.  Before this the
    only thing said about it was the state, and the number that decided it was
    in the submission rather than in any log the operator was pointed at."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "OUT_OF_MEMORY")
    code = _run(tmp_path, "over-memory", {"cpu": 1, "mem_gb": 1})
    err = capsys.readouterr().err

    assert code == 1
    assert "exceeded the 1 GiB it declared" in err
    assert "--demand mem_gb=" in err
    assert "failed (OUT_OF_MEMORY)" in err


def test_the_default_four_gibibytes_is_named_as_plainly_as_an_explicit_one(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``mem_gb`` defaults to 4, so the submission that was killed may never
    have mentioned memory at all.  That is the caller who most needs the
    number said out loud."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "OUT_OF_MEMORY")
    _run(tmp_path, "default-memory", {"cpu": 1, "mem_gb": 4})

    assert "exceeded the 4 GiB it declared" in capsys.readouterr().err


def test_a_job_that_failed_for_another_reason_says_nothing_about_memory(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:7")
    _run(tmp_path, "ordinary-failure", {"cpu": 1, "mem_gb": 1})

    assert "declared" not in capsys.readouterr().err


def test_the_submit_line_carries_the_cores_the_action_declared(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Cores have no failure state.  An over-declared memory demand ends
    ``OUT_OF_MEMORY``; an under-declared ``--cpus`` ends ``COMPLETED``, slowly,
    and nothing at exit distinguishes a core-starved job from a slow one.  The
    submit line is therefore the only place the platform says what the cpuset
    will be, which is why ``cpu`` has to be in it."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "run")
    _run(tmp_path, "declared-cores", {"cpu": 24, "mem_gb": 8})
    submitted = [line for line in capsys.readouterr().err.splitlines()
                 if "pbrun: submitted" in line]

    assert submitted, "pbrun announced no submission"
    assert "'cpu': 24" in submitted[0]
    assert "'mem_gb': 8" in submitted[0]

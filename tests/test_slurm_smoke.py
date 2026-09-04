"""Run the SLURM lane against a real controller, when one can be had.

Opt-in, because it needs Docker, a privileged container and several minutes --
none of which belongs in the suite an agent runs on every change.  What it
buys is the one thing the rest of ``tests/test_slurm*.py`` cannot: those drive
fake ``sbatch``/``sacct``/``scontrol``/``squeue`` executables, so they check
that the lane speaks the scheduler's language and not that the scheduler
answers.  A defect that only a controller can show -- ``gres/shard`` refusing
a GRES with no ``File=``, a resubmission colliding with its own sealed
submission record -- is invisible to every one of them.

    PRISMABUILD_SLURM_SMOKE=1 PYTHONPATH=src pytest -q tests/test_slurm_smoke.py

``DEB_DIR`` selects the SLURM under test and is passed straight through:
unset installs Ubuntu 24.04's 23.11.4, and
``DEB_DIR=/home/rob/slurm-build/arm64-24.04`` installs the 25.11.2 the fleet
will actually run.
"""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

import pytest

RUN = Path(__file__).resolve().parents[1] / "fleet" / "slurm" / "smoke" / "run.sh"

#: Long enough for the whole table, including the row that waits for SLURM to
#: enforce a one-minute time limit, and short enough that a wedged controller
#: is a failure rather than a hang.
BUDGET_S = 900.0


def _reason() -> str | None:
    if os.environ.get("PRISMABUILD_SLURM_SMOKE") != "1":
        return "set PRISMABUILD_SLURM_SMOKE=1 to run the container smoke"
    if shutil.which("docker") is None:
        return "docker is not on PATH"
    probe = subprocess.run(
        ["docker", "info", "--format", "{{.ServerVersion}}"],
        capture_output=True, text=True, timeout=60,
    )
    if probe.returncode != 0:
        return f"docker is not usable: {(probe.stderr or '').strip()[:200]}"
    if not RUN.is_file():
        return f"the harness is missing at {RUN}"
    return None


def test_slurm_lane_against_a_real_controller() -> None:
    reason = _reason()
    if reason is not None:
        pytest.skip(reason)

    completed = subprocess.run(
        ["bash", str(RUN)],
        capture_output=True,
        text=True,
        timeout=BUDGET_S,
        # The harness reads DEB_DIR and PB_SMOKE_* out of the environment; it
        # is passed through rather than pinned so one test covers both the
        # archive's SLURM and the fleet's.
        env=dict(os.environ),
    )
    # Printed whatever happens: the PASS/FAIL table is the result, and a test
    # that hides it on success makes the operator run the harness again to see
    # what it said.
    print(completed.stdout)
    print(completed.stderr)
    assert "PrismaBuild SLURM smoke" in completed.stdout, (
        "the harness produced no table:\n"
        f"{completed.stdout[-4000:]}\n{completed.stderr[-4000:]}"
    )
    assert completed.returncode == 0, (
        "a smoke row failed:\n"
        f"{completed.stdout[-4000:]}\n{completed.stderr[-4000:]}"
    )

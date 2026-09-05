"""Run the SLURM lane against a three-node cluster, when one can be had.

Opt-in for the same reasons as ``tests/test_slurm_smoke.py``: it needs Docker,
three privileged containers and several minutes.  What it buys over that one
is the set of claims a single node cannot make at all -- a controller talking
to a remote ``slurmd`` over munge, an action landing on a box other than the
submitter's, the partition and weight routing rules against three real nodes,
a node leaving and returning, and a controller restarting under a running job.

    PRISMABUILD_SLURM_SMOKE=1 PYTHONPATH=src pytest -q \
        tests/test_slurm_smoke_multinode.py

``PB_SMOKE3_SLURM`` selects the SLURM under test and is passed straight
through: unset uses the fleet's 25.11.2 packages, and
``PB_SMOKE3_SLURM=24.04`` uses Ubuntu 24.04's 23.11.4.
"""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

import pytest

RUN = (
    Path(__file__).resolve().parents[1]
    / "fleet" / "slurm" / "smoke" / "multinode" / "run.sh"
)

#: Long enough for the whole table and short enough that a wedged cluster is a
#: failure rather than a hang.  Two rows dominate it and both are waits on a
#: real timer rather than on the harness: M6 waits ``SlurmdTimeout`` for the
#: controller to notice a dead node, and M7 takes the controller down for
#: sixty seconds, which is what it costs to outlast SLURM's own client-side
#: retry.  A passing run is about four and a half minutes.
BUDGET_S = 1800.0


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


def test_the_lane_against_three_nodes() -> None:
    reason = _reason()
    if reason is not None:
        pytest.skip(reason)

    completed = subprocess.run(
        ["bash", str(RUN)],
        capture_output=True,
        text=True,
        timeout=BUDGET_S,
        env=dict(os.environ),
    )
    # Printed whatever happens: the deviation list and the PASS/FAIL table are
    # the result, and a test that hides them on success makes the operator run
    # the harness again to see what it said.
    print(completed.stdout)
    print(completed.stderr)
    assert "PrismaBuild SLURM smoke, three nodes" in completed.stdout, (
        "the harness produced no table:\n"
        f"{completed.stdout[-4000:]}\n{completed.stderr[-4000:]}"
    )
    assert completed.returncode == 0, (
        "a smoke row failed:\n"
        f"{completed.stdout[-4000:]}\n{completed.stderr[-4000:]}"
    )

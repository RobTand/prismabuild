"""Launch-contract regression only; this does not qualify a fleet baseline."""
from __future__ import annotations

import json

import pytest

from record_compat_contract import HISTORY, generations
import record_compat


class LaunchInspected(Exception):
    """Stop before spawning; never synthesize successful runtime evidence."""


@pytest.mark.parametrize("scratch_name", ["admitted-scratch", "scratch with spaces"])
def test_archival_expectation_binds_the_selected_tmpdir(generations, tmp_path, monkeypatch, scratch_name):
    case = "shape-table/unhashed"
    _, old, manifest = next(row for row in generations if (row[0], case) in HISTORY)
    scratch = tmp_path / scratch_name
    scratch.mkdir()
    monkeypatch.setenv("TMPDIR", str(scratch))
    for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS"):
        monkeypatch.setenv(name, "1")

    def inspect_launch(command, **kwargs):
        expected = json.loads(command[command.index("-c") + 2])
        selected = kwargs["env"]
        assert selected["TMPDIR"] == str(scratch)
        assert expected["environment_before_driver"]["TMPDIR"] == selected["TMPDIR"], (
            "archival qualification must bind the admitted scratch directory, not a deployment constant"
        )
        # Keep the independent child observation and exact equality gate. The
        # launched child, not this test, supplies runtime qualification evidence.
        assert command[command.index("-c") + 1] == record_compat._ARCHIVAL_BOOTSTRAP
        assert expected["isolated"] and expected["no_site"]
        raise LaunchInspected

    monkeypatch.setattr(record_compat.subprocess, "run", inspect_launch)
    with pytest.raises(LaunchInspected):
        record_compat.invoke(old, case, tmp_path / "writer", manifest=manifest,
                             archival_runtime=record_compat.archival_runtime())

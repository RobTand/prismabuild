"""The canary workflow keeps workflow context out of its shell (issue #690).

``inputs.generation`` is operator-supplied text. Interpolated directly
into a ``run:`` block it would be shell-injectable, so it crosses into
the job through the step's ``env:`` and the shell reads an ordinary
variable. The structural rule is general: no ``${{ }}`` may appear in
any ``run:`` block of this workflow.
"""
from __future__ import annotations

from pathlib import Path

import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/canary.yml"


def _steps() -> list[dict]:
    return yaml.safe_load(WORKFLOW.read_text())["jobs"]["canary"]["steps"]


def test_no_step_interpolates_workflow_context_into_shell() -> None:
    for step in _steps():
        run = step.get("run")
        if isinstance(run, str):
            assert "${{" not in run, (step.get("name"), run)


def test_generation_reaches_the_driver_through_the_job_env() -> None:
    step = next(step for step in _steps() if isinstance(step.get("run"), str))
    assert step["env"]["PB_CANARY_GENERATION"] == "${{ inputs.generation }}"
    assert 'args+=(--generation "$PB_CANARY_GENERATION")' in step["run"]
    assert 'if [ -n "$PB_CANARY_GENERATION" ]; then' in step["run"]

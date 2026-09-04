"""The hook after SLURM: a bare ``sbatch`` is the new way around the pool.

The rule this hook enforces is "GPU work goes through PrismaBuild", and its
proxy for GPU work is the CUDA interpreter on the command line.  A submission
defeats that proxy by construction: ``sbatch job.sh`` names no interpreter at
all -- the venv is inside the script, on a node this box never sees -- so the
one shape that most needs routing through the lane is the one shape the hook
let through.

What must stay true at the same time is everything the hook already learned:
prose about the rule is not the rule, a read-only search for the word is not a
submission, and the machinery that *does* the submitting is never refused.
``scancel`` is deliberately left alone -- cancelling a job is not starting
work, and refusing it would strand the person cleaning up.
"""
from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import sys
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "require_pool.py"
CUDA = "/home/rob/dq-runs/venvs/prismaquant-cu130/bin/python"


@pytest.fixture()
def hook(tmp_path: Path):
    """The armed hook, reading a flag file this test owns."""

    spec = importlib.util.spec_from_file_location("require_pool", HOOK)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    flag = tmp_path / "on"
    flag.write_text("")
    module.FLAG = flag
    return module


def _verdict(module, command: str) -> int:
    stdin = io.StringIO(json.dumps({"tool_input": {"command": command}}))
    err = io.StringIO()
    old = sys.stdin
    sys.stdin = stdin
    try:
        with contextlib.redirect_stderr(err):
            code = module.main()
    finally:
        sys.stdin = old
    _verdict.stderr = err.getvalue()
    return code


@pytest.mark.parametrize("command", [
    "sbatch job.sh",
    "/usr/bin/sbatch --gres=gpu:1 job.sh",
    "srun --gres=gpu:1 --pty bash",
    "salloc -N1 --gres=gpu:1",
    "echo starting && sbatch job.sh",
    "cd /mnt/shared/prismabuild-fleet && srun python train.py",
])
def test_a_bare_submission_is_refused(hook, command: str) -> None:
    """Whichever of the three verbs, and behind an exempt leader too."""

    assert _verdict(hook, command) == 2


def test_the_refusal_has_the_shape_the_hook_already_uses(hook) -> None:
    """One message shape, so an agent reads the second refusal like the first."""

    assert _verdict(hook, "sbatch job.sh") == 2
    message = _verdict.stderr
    assert message.startswith("Refused:")
    assert "Run it as:" in message
    assert "--transport slurm" in message
    assert "Why:" in message


def test_cancelling_a_job_stays_allowed(hook) -> None:
    """Cancelling is not starting work, and a guard that refuses the cleanup
    strands the person complying with it."""

    assert _verdict(hook, "scancel 41234") == 0
    assert _verdict(hook, "squeue -u rob") == 0
    assert _verdict(hook, "sinfo -N -l") == 0


def test_prose_and_search_are_not_submissions(hook) -> None:
    """The fifth time this hook could have locked out its own repair."""

    assert _verdict(hook, "git commit -m 'refuse a bare sbatch'") == 0
    assert _verdict(hook, "grep -rn sbatch tools/fleet") == 0
    assert _verdict(hook, "cat fleet/slurm/epilog.sh") == 0


def test_a_word_that_merely_contains_a_verb_is_not_one(hook) -> None:
    assert _verdict(hook, "/usr/bin/python3 tools/my-sbatch-wrapper.py") == 0


def test_the_lane_itself_is_never_refused(hook) -> None:
    """``pbrun`` is what runs ``sbatch``, and ``slurm_job`` is what the job runs.

    A guard that refuses the alternative it names is worse than no guard, and
    the job entry names the CUDA venv by construction -- the action's own
    sealed interpreter.
    """

    assert _verdict(
        hook,
        f"/usr/bin/python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py "
        f"--transport slurm --gpu -- {CUDA} -m pytest tests",
    ) == 0
    assert _verdict(
        hook,
        f"/usr/bin/python3 /mnt/shared/prismabuild-fleet/repo/tools/slurm_job.py "
        f"--action /mnt/shared/prismabuild-fleet/cas/requests/ab/key.json "
        f"--worker-python {CUDA}",
    ) == 0


def test_the_cuda_interpreter_is_still_refused(hook) -> None:
    """The rule the hook was written for is unchanged."""

    assert _verdict(hook, f"{CUDA} -m pytest tests") == 2


@pytest.mark.parametrize("command", [
    "sbatch --help",
    "srun --version",
    "salloc -V",
    "/usr/bin/sbatch -h",
    "SLURM_CONF=/etc/slurm/slurm.conf sbatch --usage",
    "which sbatch",
    "type srun",
    "command -v sbatch",
    "man sbatch",
    "ls -l /usr/bin/sbatch",
    "dpkg -L slurm-client | grep sbatch",
])
def test_asking_about_a_verb_is_not_a_submission(hook, command: str) -> None:
    """The first two commands anyone runs at a new scheduler are ``which``
    and ``--help``; refusing them teaches the reader to route around the hook
    before they have read the rule."""

    assert _verdict(hook, command) == 0


def test_a_help_switch_does_not_excuse_the_rest_of_the_line(hook) -> None:
    """Only the verb and its own switches qualify.  A script after the
    switch, a bare verb reading its script from stdin, and a submission
    behind a permitted segment are all still submissions."""

    assert _verdict(hook, "sbatch --help job.sh") == 2
    assert _verdict(hook, "sbatch") == 2
    assert _verdict(hook, "sbatch --version && sbatch job.sh") == 2
    assert _verdict(hook, "srun -h --gres=gpu:1 nvidia-smi") == 2

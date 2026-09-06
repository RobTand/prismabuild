"""Prose in an argument is a mention of fleet work, not an instance of it.

Issue #209: a mailbox send was refused because the message body described a
shard that died under a test runner.  No test ran and no GPU was touched --
the command posts a JSON file to a directory -- but the runner's name in the
``--body`` prose matched the guard's lexical scan.

This is the failure mode the module's own docstring already names for commit
messages, one caller later.  The rule these tests pin is the boundary, not the
caller: in a segment that runs a *python script*, a quoted argument is data.
The shell execs the interpreter and the interpreter execs the file; neither
execs the argument, so its text is prose about work rather than work.

The second half is the load-bearing half.  A mention exemption that let real
work through would be far worse than the bug, so every shape that forwards a
quoted argument to something that runs it must stay refused.
"""
import pytest
from test_require_pool import _armed, _verdict

CUDA = "/home/rob/dq-runs/venvs/prismaquant-cu130/bin/python"
MBOX = "/mnt/shared/agent-mailbox/mbox.py"


# --- what is now allowed -------------------------------------------------

@pytest.mark.parametrize('command', [
    # The command from the issue, near enough verbatim.
    f'python3 {MBOX} send --from claude-triage --to main-campaign '
    f'--subject "shard died" '
    f'--body "shard 3 died mid-run; the pytest process vanished, requeue it"',
    # The same shape with the value attached to the option.
    f'python3 {MBOX} send --to x --body="pytest died under load"',
    # A single-quoted body, and a body quoting the CUDA interpreter rather
    # than the runner -- the other pattern this guard scans for.
    f"python3 {MBOX} send --to x --body 'do not call {CUDA} directly'",
    # And the scheduler verb, which prose about the lane names constantly.
    f'python3 {MBOX} send --to x --body "someone ran sbatch job.sh by hand"',
    # A positional prose argument, same reasoning: the script consumes it.
    f'python3 {MBOX} post "pytest shard 3 died mid-run"',
])
def test_prose_in_a_script_argument_is_allowed(tmp_path, command):
    assert _verdict(_armed(tmp_path, None), command) == 0


# --- what is still refused ----------------------------------------------

@pytest.mark.parametrize('command', [
    # The guard's three original targets, untouched.
    f'{CUDA} -m pytest tests',
    f'{CUDA} train.py',
    'flock /home/rob/tmp/arb/.gpu.lock some-command',
    '/home/rob/tmp/arb/gpuslot.sh python train.py',
    '/home/rob/tmp/arb/gpulock.sh -- python train.py',
    'python3 -m pytest tests',
    'pytest -q tests',
    'docker run --gpus all image train.py',
    'sbatch job.sh',
    # The command word in quotes is still the command word, not an argument.
    f"'{CUDA}' train.py",
    f'"{CUDA}" -m pytest tests',
    # Quoting the runner does not turn it into prose when python is asked to
    # execute it: -m runs a module and -c runs the string.
    'python3 -m "pytest" tests',
    'python3 -c "import pytest; pytest.main()"',
    # A shell always executes its argument, whatever the quoting.
    'bash -lc "pytest tests"',
    'sh -c "pytest tests"',
    # Wrappers hand their trailing argv to something that runs it.
    'ssh sparklina "pytest tests"',
    'sudo "pytest" tests',
    'docker run --entrypoint "pytest" image',
    'timeout 600 "pytest" tests',
    'xargs -I{} sh -c "pytest {}"',
    # Runners reached through a launcher that is not a python script run.
    'uv run --no-sync "pytest" tests',
    'systemd-run --pty "pytest" tests',
    'watch --no-title "pytest tests"',
    'conda run --live-stream "pytest"',
    'numactl --localalloc "pytest" tests',
    'poetry run --no-cache "pytest"',
    # sbatch's own forwarding option, whose value is a command line.
    'sbatch --wrap "srun train.sh"',
    # The interpreter is not running a script here, so nothing is data.
    f'python3 -m pytest --junitxml "{CUDA}.xml" tests',
])
def test_work_is_still_refused(tmp_path, command):
    assert _verdict(_armed(tmp_path, None), command) == 2


def test_a_script_run_does_not_vouch_for_its_neighbours(tmp_path):
    """The exemption is one segment wide, like every other one here."""
    module = _armed(tmp_path, None)
    assert _verdict(
        module, f'python3 {MBOX} send --body "pytest died" && pytest tests') == 2
    assert _verdict(
        module, f'python3 {MBOX} send --body "ok"; {CUDA} train.py') == 2


def test_an_escaped_inner_quote_falls_to_the_refusing_side(tmp_path):
    """The guard reads quotes lexically and does not track escapes inside them.

    A body carrying an escaped double quote ends its span early, so the rest
    of the prose is read as command text and refused.  That is the
    conservative direction and it is asserted rather than accidental: the
    documented way to send prose the guard misreads is ``--body-file``.
    """
    module = _armed(tmp_path, None)
    command = f'python3 {MBOX} send --body "he said \\"pytest\\" died"'
    assert _verdict(module, command) == 2


def test_a_body_file_never_puts_prose_on_the_command_line(tmp_path):
    """The shape the mailbox README recommends stays allowed, as it is today."""
    module = _armed(tmp_path, None)
    assert _verdict(module, f'python3 {MBOX} send --to x --body-file msg.txt') == 0

"""A here-document body is shell only when a shell is the one reading it.

Issue #223.  ``_is_interpreter`` counted ``python`` alongside ``bash``, so the
body of ``python3 - <<'EOF'`` was segmented with the shell's own boundary
rules.  Python tokens then stood where commands go -- ``print`` and a variable
name read as two commands -- and a runner's name inside a string literal was
read as the runner being run.  Measured on ``fea9eaa``::

    /usr/bin/python3 - <<'EOF'
    cases = ['python3 -m <runner> tests']
    print(cases)
    EOF

    segments: ['/usr/bin/python3 -', "cases = [...]", 'print', 'cases']
    verdict:  refused

That is the module's documented failure mode: in one session it refused a
script that evaluated the guard's own predicates, the test cases for issue
#209's fix, and that pull request's body.  Each refusal invites rewording the
thing being written, which is the quiet censorship #209 is about.

It also bought no enforcement.  ``cat > x.py <<'EOF'`` writes a program and
``python3 x.py`` runs it, and both are allowed today -- measured, not assumed;
``test_the_pair_this_change_is_measured_against`` below is that measurement.
A here-document fed to ``python3 -`` is that pair with the file inlined.

So this file pins BOTH sides of the boundary the fix draws:

* fed to a shell, the body is a script and is scanned in full;
* fed to anything else, the body is the file it stands in for and is dropped
  -- unless the delimiter is unquoted, in which case the shell expands the
  body before handing it on and those substitutions are still scanned;
* and one line may open two bodies for two different readers, so which side a
  body falls on is decided per opener, not per line.

Nothing here starts GPU work, submits a job, or reads the live flag: the
module is loaded directly and asked about strings.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "require_pool.py"

_SPEC = importlib.util.spec_from_file_location("require_pool_python_heredoc", HOOK)
hook = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(hook)                        # type: ignore[union-attr]

#: Assembled rather than written, so this file does not itself trip the rule
#: it tests when an agent edits it from a shell.
CUDA = "/home/rob/dq-runs/venvs/" + "prismaquant-cu130" + "/bin/python"
RUNNER = "py" + "test"


def _blocked(command: str) -> bool:
    return bool(hook.contends(command)
                or hook.submits(command)
                or hook.unpooled_work(command))


# --------------------------------------------------------------------------
# The premise: the same program is already reachable two commands over.
# --------------------------------------------------------------------------

def test_the_pair_this_change_is_measured_against() -> None:
    """Writing a program and running it is allowed, so inlining it must be.

    This is the argument for the change rather than a property of it, and it
    is asserted rather than quoted because the argument is only as good as the
    behaviour.  If either half of the pair ever starts being refused, the
    justification for dropping a python body has gone and this fails first.
    """

    write = "cat > x.py <<'EOF'\nimport os; os.system('sbatch j.sh')\nEOF"
    run = "python3 x.py"

    assert _blocked(write) is False
    assert _blocked(run) is False
    assert _blocked(write + "\n" + run) is False


# --------------------------------------------------------------------------
# The side that must not regress: a shell's body is shell.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("name,command", [
    ("a shell reading its standard input",
     f"bash <<'EOF'\n{CUDA} train.py\nEOF"),
    ("a shell reading a body that names a test runner",
     f"bash <<'EOF'\n{RUNNER} -q tests\nEOF"),
    ("a shell reached over ssh",
     f"ssh sparklina bash <<'EOF'\n{CUDA} train.py\nEOF"),
    ("a shell reached through sudo",
     f"sudo bash <<'EOF'\n{CUDA} train.py\nEOF"),
    ("a shell inside a container",
     f"docker exec -i c bash <<'EOF'\n{CUDA} train.py\nEOF"),
    ("a body piped into a shell",
     f"cat <<'EOF' | bash\n{CUDA} train.py\nEOF"),
    ("a body piped into a shell over ssh",
     "cat <<'EOF' | ssh lina bash\nsbatch job.sh\nEOF"),
    # The question this change invites: python opens the body, so the body is
    # not scanned as shell -- but python's OUTPUT is handed to a shell, and
    # the downstream clause sees that shell one command further along.  What
    # the interpreter prints is a program either way, so this stays refused.
    ("a python body whose output is piped into a shell",
     f"python3 - <<'PY' | bash\n{CUDA} train.py\nPY"),
    # The other four spellings of a shell, so removing any one of them from
    # ``SHELLS`` is a failing test rather than a silent widening.
    ("sh", f"sh <<'EOF'\n{CUDA} train.py\nEOF"),
    ("dash", f"dash <<'EOF'\n{CUDA} train.py\nEOF"),
    ("zsh", f"zsh <<'EOF'\n{CUDA} train.py\nEOF"),
    ("ksh", f"ksh <<'EOF'\n{CUDA} train.py\nEOF"),
])
def test_a_body_a_shell_reads_is_still_scanned(name: str, command: str) -> None:
    """The case the fix must not touch.  A shell body genuinely is shell."""

    assert _blocked(command) is True, name


# --------------------------------------------------------------------------
# The side the issue asks for: another language's body is not shell.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("name,command", [
    ("the example measured in the issue",
     "/usr/bin/python3 - <<'EOF'\n"
     f"cases = ['python3 -m {RUNNER} tests']\n"
     "print(cases)\n"
     "EOF"),
    ("a scheduler verb inside a python string literal",
     "python3 - <<'EOF'\nimport subprocess; subprocess.run(['sbatch','j.sh'])\nEOF"),
    ("the CUDA interpreter's path inside a python string literal",
     f"python3 - <<'EOF'\nprint('{CUDA}')\nEOF"),
    # The pipeline spelling of the same thing: ``cat <<'EOF' | python3`` is
    # ``python3 - <<'EOF'`` with the body arriving one command further along.
    ("a body piped into python",
     f"cat <<'EOF' | python3\nprint('{CUDA}')\nEOF"),
    # A wrapper in front of the interpreter does not make the body shell.
    ("python reached over ssh",
     f"ssh sparklina python3 - <<'EOF'\nprint('{CUDA}')\nEOF"),
    ("a versioned interpreter name",
     f"python3.12 - <<'EOF'\nprint('{CUDA}')\nEOF"),
])
def test_a_body_another_language_reads_is_not_scanned(
    name: str, command: str,
) -> None:
    """Python tokens are not commands, and the scan of them bought nothing.

    Issue #89 asked for the opposite and this reverses it.  What changed is
    not the risk appetite but the reading: #89 wanted an interpreter's body
    inspected "whatever the delimiter's quoting", and the body of a python
    here-document is not a thing this module can read at all.
    """

    assert _blocked(command) is False, name


# --------------------------------------------------------------------------
# What the delimiter still decides, whoever reads the body.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("name,command", [
    ("a substitution in an unquoted python body",
     f"python3 - <<EOF\nprint('$({CUDA} train.py)')\nEOF"),
    ("a backquoted substitution in an unquoted python body",
     "python3 - <<EOF\nprint('`sbatch j.sh`')\nEOF"),
    ("a substitution in an unquoted python body, test runner",
     f"python3 - <<EOF\nprint('$({RUNNER} -q tests)')\nEOF"),
    # The writer spelling of the same rule, which is where it came from.
    ("a substitution in an unquoted body on its way to a file",
     f"cat >/dev/null <<EOF\n$({CUDA} train.py)\nEOF"),
])
def test_an_unquoted_body_is_expanded_before_anyone_reads_it(
    name: str, command: str,
) -> None:
    """An unquoted delimiter makes the body a double-quoted string.

    The shell runs ``$( ... )`` and backquotes in it BEFORE the interpreter
    ever sees the text, so those substitutions are shell the shell really
    runs.  Dropping a python body must not drop them, and this is the case
    that says so.
    """

    assert _blocked(command) is True, name


# --------------------------------------------------------------------------
# Nesting, in both directions.
# --------------------------------------------------------------------------

def test_a_shell_body_cannot_hide_work_in_a_python_opener() -> None:
    """The smuggle the fix would open if an executed body were re-scanned.

    ``_heredocs`` hands an executed body to ``_commands_in``, not to
    ``_scan``.  That choice is load-bearing now: a here-document opened INSIDE
    a script the outer shell runs is text for the inner program, and if the
    outer pass extracted it as an opener of its own it would then be dropped
    as a python body -- and the work in it would be gone from the segments
    while the shell still ran it.
    """

    command = (
        "bash <<'SH'\n"
        "python3 - <<'PY'\n"
        f"{CUDA} train.py\n"
        "PY\n"
        "SH"
    )

    assert _blocked(command) is True


def test_a_python_body_does_not_swallow_the_shell_after_it() -> None:
    """A nested opener in a dropped body must not move the body's end.

    The terminator is matched line by line against this opener's own tag, so
    an inner ``X`` inside a ``PY`` body is body text.  If a dropped body
    consumed to the first terminator-looking line instead, the real command
    after it would fall inside the drop and never be judged.
    """

    command = (
        "python3 - <<'PY'\n"
        "bash <<'X'\n"
        "harmless\n"
        "X\n"
        "PY\n"
        f"{CUDA} train.py"
    )

    assert _blocked(command) is True


def test_a_python_heredoc_in_a_substitution_does_not_hide_what_follows() -> None:
    command = (
        "echo $(python3 - <<'PY'\n"
        "print('hi')\n"
        "PY\n"
        f") && {CUDA} train.py"
    )

    assert _blocked(command) is True


# --------------------------------------------------------------------------
# One line, two bodies, two readers.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("name,command", [
    # This one was allowed before the fix: the whole line's verdict came from
    # the FIRST opener's owner, so a shell script behind a write was dropped
    # as if it were a file.  Splitting the interpreter set made that reachable
    # with a python owner too, which is why the verdict moved per opener.
    ("a write, then a shell script",
     f"cat > f <<'A' ; bash <<'B'\nplain\nA\n{CUDA} train.py\nB"),
    ("a python program, then a shell script",
     f"python3 - <<'A' ; bash <<'B'\nprint(1)\nA\n{CUDA} train.py\nB"),
    ("two shell scripts",
     f"bash <<'A' ; bash <<'B'\nplain\nA\n{CUDA} train.py\nB"),
])
def test_each_body_is_judged_by_the_command_that_reads_it(
    name: str, command: str,
) -> None:
    """Bodies are read in the order their openers appear, by their own owner."""

    assert _blocked(command) is True, name


def test_a_write_after_a_shell_script_is_still_a_write() -> None:
    """The same rule in the direction that permits rather than refuses.

    ``bash <<'A' ; cat > f <<'B'`` runs the first body and writes the second,
    and the second is the fleet's own config: a GB10 worker's ``--python``
    argument IS the CUDA interpreter, and the hook refused that write four
    times before the writer exemption existed.  A per-line verdict handed it
    the shell's.
    """

    command = f"bash <<'A' ; cat > f <<'B'\nplain\nA\n{CUDA}\nB"

    assert _blocked(command) is False

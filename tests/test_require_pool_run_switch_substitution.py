"""A ``-c`` switch runs what a command substitution produced.

Issue #247.  ``bash -c "$(cat <<'X'`` … ``X)"`` hands the here-document body to
the shell as the script it executes, and until this the body was dropped as
data: ``_heredocs`` asked who owned the opener, got ``cat``, and ``cat`` is a
writer.  It is not a writer here.  Its output is the program.

This is a different rule from the one ``_feeds_a_shell`` carries.  There the
body is shell because a shell reads its standard input; here it is shell
because the *enclosing* command hands its argument to an interpreter to run.
So the guard asks a separate question — does the text in front of this
``$( ... )`` name a shell with a pending run switch — and threads the answer
into the scan it already makes over the substitution.

This TIGHTENS the guard, so both halves are written out: the shapes that start
being refused, and the shapes that must not move.  Measured against
``origin/main`` at ``aba92a3`` with only ``tools/fleet/require_pool.py``
reverted, every command in the first list returned 0 and every command in the
other two returned what it returns here.

``RUNNER`` is assembled rather than written because this file is edited by
agents whose own Bash commands go through the hook it tests.
"""
import pytest
from test_require_pool import CUDA, _armed, _verdict

RUNNER = "py" + "test"

#: GPU work by the hook's own proxy, so a scanned body is a refusal and an
#: unscanned one is silence.
WORK = f"{CUDA} train.py"


# --- what starts being refused ---------------------------------------------
#
# All twelve returned 0 on origin/main.  The first is the spelling people
# actually type, and it had never been refused.

@pytest.mark.parametrize('command', [
    # The reported shape, in the spelling that has always been allowed.
    f"bash -c \"$(cat <<'X'\n{WORK}\nX\n)\"",
    # The same thing unquoted.  It was refused before #258 by the wrong-owner
    # accident that issue named, and allowed after it; both spellings run the
    # body, and now both are refused for the reason that is true of both.
    f"bash -c $(cat <<'X'\n{WORK}\nX\n)",
    # Another shell.  The rule is about shells, not about ``bash``.
    f"sh -c \"$(cat <<'X'\n{WORK}\nX\n)\"",
    # A clustered run switch.  A shell takes its options clustered and still
    # runs the next word.
    f"bash -lc \"$(cat <<'X'\n{WORK}\nX\n)\"",
    # Options before the switch, which is how a careful script spells it.
    f"bash -o pipefail -c \"$(cat <<'X'\n{WORK}\nX\n)\"",
    # The shell one hop away, through a wrapper.  This is how routine work
    # reaches the other boxes here.
    f"ssh lina bash -c \"$(cat <<'X'\n{WORK}\nX\n)\"",
    # Backquotes are the other spelling of the same substitution.
    f"bash -c \"`cat <<'X'\n{WORK}\nX\n`\"",
    # The substitution need not begin the value: after a ``;`` inside it, a
    # command may still begin, and this one does.
    f"bash -c \"cd x; $(cat <<'X'\n{WORK}\nX\n)\"",
    # ``tee`` copies its input to its output as readily as ``cat`` does.
    f"bash -c \"$(tee /dev/null <<'X'\n{WORK}\nX\n)\"",
    # A variable assignment in front of the writer is not the command.
    f"bash -c \"$(TMPDIR=/x cat <<'X'\n{WORK}\nX\n)\"",
    # An unquoted delimiter, which the outer shell expands first and the inner
    # one then runs.  Refused for both reasons rather than either.
    f"bash -c \"$(cat <<X\n{WORK}\nX\n)\"",
    # A wrapper reaching a shell with no shell of its own in front of it.
    f"sudo bash -c \"$(cat <<'X'\n{WORK}\nX\n)\"",
    # ``--`` ends option parsing; the word after it is still the code.
    f"bash -c -- \"$(cat <<'X'\n{WORK}\nX\n)\"",
])
def test_a_body_a_run_switch_executes_is_refused(tmp_path, command):
    assert _verdict(_armed(tmp_path, None), command) == 2


# --- what must keep being allowed ------------------------------------------
#
# The load-bearing half.  A guard that breaks a legitimate entrypoint is worse
# than the hole it closed, and every row here is a shape that runs no body.

@pytest.mark.parametrize('command', [
    # ``-c`` takes ``echo``; the substitution is an argument the printed
    # script never reads.  This is the false refusal #258 removed, and it
    # stays removed.
    f"bash -c echo $(cat <<'X'\n{WORK}\nX\n)",
    # Inside the value, but where a command cannot begin: ``echo`` prints the
    # body rather than running it.
    f"bash -c \"echo $(cat <<'X'\n{WORK}\nX\n)\"",
    # How this repo writes a commit message.  ``git`` is not a shell.
    "git commit -m \"$(cat <<'MSG'\nRoute the runner through PrismaBuild\nMSG\n)\"",
    # Issue #223: an interpreter of another language runs its own language.
    # ``cat > x.py <<'PY' … PY ; python3 x.py`` writes and runs the same
    # program and is allowed, so reading this body as commands would refuse
    # prose about the rule rather than the rule being broken.
    f"python3 -c \"$(cat <<'PY'\nprint({RUNNER!r})\nPY\n)\"",
    # The body goes to a file, so the substitution prints nothing and the
    # ``-c`` runs an empty string.  Both spellings of the redirection, because
    # one sits before the opener and one after it.
    f"bash -c \"$(cat > f <<'X'\n{WORK}\nX\n)\"",
    f"bash -c \"$(cat <<'X' > f\n{WORK}\nX\n)\"",
    # A pool submission's payload is argv another process execs under a
    # reservation, and the entrypoint in front of it is what vouches for it.
    f"python3 tools/fleet/pbrun.py --gpu -- bash -c \"$(cat <<'X'\n{WORK}\nX\n)\"",
    # The write the whole quoted-delimiter exemption exists for.
    f"cat > start.sh <<'EOF'\nworker_loop.py --python {CUDA}\nEOF",
    # A body fed to python, outside any substitution.
    f"python3 - <<'PY'\nprint({RUNNER!r})\nPY",
    # The submitter this repo's own briefs tell an agent to run.
    "python3 tools/fleet/pbtest.py --checkout . --python /usr/bin/python3",
    # A substitution with no here-document in it at all.
    "echo $(cat f)",
    # A body a shell really does run, carrying nothing this guard refuses.
    f"bash -c \"$(cat <<'X'\necho hello\nX\n)\"",
    # ``bash -- file`` names a script to read, not code to run: dropping the
    # separator must not leave a run switch behind that was never typed.
    f"bash -- \"$(cat <<'X'\n{WORK}\nX\n)\"",
    # The separator with no switch in front of it is the same shape again.
    f"bash \"$(cat <<'X'\n{WORK}\nX\n)\"",
])
def test_the_shapes_that_run_no_body_still_pass(tmp_path, command):
    assert _verdict(_armed(tmp_path, None), command) == 0


# --- what was already refused, and for its own reason -----------------------

def test_a_single_quoted_value_the_inner_shell_expands_is_still_refused(
    tmp_path,
):
    """Refused before this change and after it, by a different rule.

    The outer shell does not expand a single-quoted word, so the substitution
    is literal text handed to ``bash -c`` — which expands and runs it.  The
    guard reaches the same verdict without this change, because a quoted
    argument to a shell is not prose (``_prose_start``), and the interpreter
    path is in the text.  It is here so that a later edit cannot move it
    without saying so.
    """

    command = f"bash -c '$(cat <<X\n{WORK}\nX\n)'"
    assert _verdict(_armed(tmp_path, None), command) == 2


# --- the residue, named rather than implied --------------------------------

def test_a_transformed_body_is_not_reached(tmp_path):
    """A body a program rewrites before printing is out of this rule's reach.

    ``PASS_THROUGH`` enumerates programs that copy their standard input to
    their standard output, which is a fact about ``cat`` and ``tee``.  ``sed``
    also puts a body on the output, transformed, and reading it would mean
    proving what ``sed`` emits.  This module is a command-line guard, not that
    proof, and the standing agent policy covers the indirect shapes.  Recorded
    as the shape it is rather than left to be discovered as a surprise.
    """

    command = f"bash -c \"$(sed -n p <<'X'\n{WORK}\nX\n)\""
    assert _verdict(_armed(tmp_path, None), command) == 0

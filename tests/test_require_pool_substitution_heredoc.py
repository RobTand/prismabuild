"""A here-document opened inside ``$( ... )`` belongs to a command inside it.

Issue #239.  ``_heredocs`` asks who owns an opener by scanning the text in
front of it, and on ``echo $(bash <<'SH'`` that text ends inside the
substitution, so the owner came back ``echo``: a body a shell really runs was
read as data and never scanned.  The fix is to leave those openers to the
``_scan`` pass ``_commands_in`` already makes over the substitution's body,
where the owner is read from text with no substitution in front of it.

This narrows a guard, so the shapes it must still refuse are written out
below rather than assumed, and the one shape that stops being refused for the
wrong reason is pinned as a known gap (issue #247) rather than as a contract.

``RUNNER`` is assembled rather than written because this file is edited by
agents whose own Bash commands go through the hook it tests.
"""
import pytest
from test_require_pool import CUDA, _armed, _verdict

RUNNER = "py" + "test"

#: Bodies a shell executes.  Each is GPU work by the hook's own proxy, so a
#: scanned body is a refusal and an unscanned one is silence.
WORK = f"{CUDA} train.py"


# --- a body a shell runs inside a substitution ------------------------------
#
# All seven must refuse.  Four of them did not before this change, measured
# against origin/main with only tools/fleet/require_pool.py reverted: the two
# ``echo $(bash <<'SH'`` spellings, the nested-twice one, and the unterminated
# one.  The other three already refused, and are here because a narrowing has
# to say what it did NOT move as well as what it did.

@pytest.mark.parametrize('command', [
    # The reported shape.  ``echo`` takes the substitution's OUTPUT; ``bash``
    # inside it takes the body, and runs it.
    f"echo $(bash <<'SH'\n{WORK}\nSH\n)",
    # The same thing spelled with backquotes.
    f"echo `bash <<'SH'\n{WORK}\nSH\n`",
    # No outer command word at all: the substitution stands alone.
    f"$(bash <<'SH'\n{WORK}\nSH\n)",
    # The reader is one hop away, over ssh, inside the substitution.
    f"$(ssh lina bash <<'SH'\n{WORK}\nSH\n)",
    # Nested twice.  The owner walk has to survive both levels.
    f"echo $(echo $(bash <<'X'\n{WORK}\nX\n))",
    # An unterminated substitution.  Refusing on text a shell would reject is
    # the safe direction; admitting it is not.
    f"echo $(bash <<'X'\n{WORK}\nX",
    # A pipeline inside the substitution: ``cat`` opens it, ``bash`` runs it.
    f"echo $(cat <<'X' | bash\n{WORK}\nX\n)",
])
def test_a_body_run_inside_a_substitution_is_refused(tmp_path, command):
    assert _verdict(_armed(tmp_path, None), command) == 2


# --- what must still refuse: every shape that refused before the change -----

@pytest.mark.parametrize('command', [
    # The plain case the module was written for.
    f"bash <<'SH'\n{WORK}\nSH",
    # An unquoted delimiter to a non-shell: the body is data, but the shell
    # expands it first, so its substitutions are still scanned.
    f"cat > f <<EOF\n$({WORK})\nEOF",
    # A pipeline hands the body to a shell one command further along.  The
    # pipe sits on the opener's own line, which is where the shell reads it: a
    # newline ends the command, so ``SH`` followed by ``| bash`` is not a
    # pipeline at all.
    f"cat <<'SH' | bash\n{WORK}\nSH",
    # The owner is the last command before ITS OWN opener, not the first on
    # the line.
    f"cd x && bash <<'SH'\n{WORK}\nSH",
    # Two openers, two readers, in the order the shell reads them.
    f"cat > f <<'A' ; bash <<'B'\nprose\nA\n{WORK}\nB",
    # Only the opener tokens leave the line; what follows on it is judged.
    f"cat > f <<'EOF' && {WORK}\nprose\nEOF",
    # A shell one hop away.
    f"ssh lina bash <<'SH'\n{WORK}\nSH",
    # An executed body cannot hide work in a body it opens itself.
    f"bash <<'SH'\npython3 - <<'PY'\n{WORK}\nPY\nSH",
    # Nothing to do with substitutions, and it stays refused.
    f"{CUDA} train.py",
    f"python3 -m {RUNNER} tests",
])
def test_the_shapes_this_guard_refuses_still_refuse(tmp_path, command):
    assert _verdict(_armed(tmp_path, None), command) == 2


# --- what must still pass: the carve-outs this guard exists alongside ------

@pytest.mark.parametrize('command', [
    # The write the exemption was written for: the fleet's own worker command
    # names the CUDA interpreter as an argument, by construction.
    f"cat > start.sh <<'EOF'\nworker_loop.py --python {CUDA}\nEOF",
    # Issue #223: a quoted delimiter to python is Python, not commands.
    f"python3 - <<'PY'\nprint({RUNNER!r})\nPY",
    # Prose about the rule, handed to a program, inside a substitution.
    "git commit -m \"$(cat <<'MSG'\nRoute the runner through PrismaBuild\nMSG\n)\"",
    # The pool's own entrypoints.
    f"python3 tools/fleet/pbrun.py --gpu -- {CUDA} train.py",
    "python3 tools/fleet/pbtest.py --checkout . --python /usr/bin/python3",
    # A body nothing executes, inside a substitution, read by a writer.
    f"cat > f <<'X'\n{WORK}\nX",
])
def test_the_carve_outs_still_pass(tmp_path, command):
    assert _verdict(_armed(tmp_path, None), command) == 0


# --- a false refusal this change removes -----------------------------------

def test_a_body_the_substitution_only_prints_is_no_longer_refused(tmp_path):
    """``-c`` takes ``echo``; the body is printed, never run.

    Refusing this was the same wrong-owner bug landing on the other side: the
    text before the opener ended in ``bash -c``, whose first token is a shell.
    """

    command = f"bash -c echo $(cat <<'X'\n{WORK}\nX\n)"
    assert _verdict(_armed(tmp_path, None), command) == 0


# --- a gap, recorded as a gap ----------------------------------------------

@pytest.mark.xfail(strict=True, reason="issue #247: a -c switch runs what a "
                                       "substitution produced, and nothing "
                                       "scans the body that became it")
@pytest.mark.parametrize('command', [
    # Always allowed, before this change and after it.
    f"bash -c \"$(cat <<'X'\n{WORK}\nX\n)\"",
    # Refused before this change, by the accident above rather than by a rule
    # about ``-c``, and allowed after it.  Both spellings run the body.
    f"bash -c $(cat <<'X'\n{WORK}\nX\n)",
])
def test_a_substitution_that_c_runs_is_not_scanned(tmp_path, command):
    assert _verdict(_armed(tmp_path, None), command) == 2

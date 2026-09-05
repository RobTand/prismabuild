"""The hook must know where one command ends and the next begins (issue #89).

Every failure in that issue is one cause: the hook applied its exemptions to
raw shell text before it knew the shell's own execution boundaries.

*   ``_drop_pool_payload`` cut the whole command line at the first pool
    entrypoint's ``--``.  In ``pbrun.py -- true && <cuda python> train.py`` the
    second command belongs to the outer shell and never reaches pbrun, and it
    was discarded unread.  So was a following ``sbatch``.
*   Every here-document body was removed as data.  A body fed to ``bash`` is a
    script, and an unquoted body is expanded before it is written, so
    ``$( ... )`` inside one runs.
*   ``command`` sat in the list of commands that never start work.
    ``command -v sbatch`` is inspection; ``command sbatch job.sh`` submits.

What must stay true is everything the hook already learned, because its
documented failure mode is refusing the work of complying with it: a quoted
``&&`` inside a pbrun payload is not a boundary, a quoted here-document to a
writer is data, and prose about the rule is not the rule.

Nothing here starts GPU work, submits a job, or reads the live flag: the
module is loaded directly and asked about strings.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys

import pytest

HOOK = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "require_pool.py"

_SPEC = importlib.util.spec_from_file_location("require_pool_boundaries", HOOK)
hook = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(hook)                        # type: ignore[union-attr]

#: Assembled rather than written, so this file does not itself trip the rule
#: it tests when an agent edits it from a shell.
CUDA = "/home/rob/dq-runs/venvs/" + "prismaquant-cu130" + "/bin/python"
PBRUN = "/usr/bin/python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py"


def _blocked(command: str) -> bool:
    return bool(hook.contends(command) or hook.submits(command))


@pytest.mark.parametrize("name,command", [
    ("direct control",
     f"{CUDA} train.py"),
    ("after a pool payload, GPU",
     f"python3 pbrun.py --gpu -- true && {CUDA} train.py"),
    ("after a pool payload, scheduler",
     "python3 pbrun.py -- true && sbatch job.sh"),
    ("an executable here-document",
     f"bash <<'EOF'\n{CUDA} train.py\nEOF"),
    ("a substitution inside an unquoted here-document",
     f"cat >/dev/null <<EOF\n$({CUDA} train.py)\nEOF"),
    ("command executes its argument",
     "command sbatch job.sh"),
])
def test_every_case_the_issue_reported_is_refused(name: str, command: str) -> None:
    """The six shapes from the issue, which all returned False."""

    assert _blocked(command) is True, name


@pytest.mark.parametrize("command", [
    # The outer shell's own commands, in each of its boundaries.
    f"python3 pbrun.py --gpu -- true || {CUDA} train.py",
    f"python3 pbrun.py --gpu -- true; {CUDA} train.py",
    f"python3 pbrun.py --gpu -- true\n{CUDA} train.py",
    # A subshell is a boundary too, and its leader vouches for nothing.
    f"(cd /home/rob/tmp && {CUDA} train.py)",
    # A substitution is a command the shell runs.
    f"echo $({CUDA} train.py)",
    f'echo "$({CUDA} train.py)"',
    f"echo `{CUDA} train.py`",
    "echo $(sbatch job.sh)",
    # An interpreter's here-document is a script, quoted delimiter or not.
    f"sh <<EOF\n{CUDA} train.py\nEOF",
    "bash <<'EOF'\nsbatch job.sh\nEOF",
    # The owner is the command the here-document is attached to, which is the
    # last one before the opener rather than the first one on the line.
    f"cd /home/rob/tmp && bash <<'EOF'\n{CUDA} train.py\nEOF",
    # An unquoted body is expanded before it is written, backquotes included.
    f"cat > f <<EOF\n`{CUDA} train.py`\nEOF",
    # ``command`` with its PATH switch still executes.
    "command -p sbatch job.sh",
    f"command {CUDA} train.py",
    # A quoted opener is text, so it must not swallow what follows.
    f"echo '<<EOF' && {CUDA} train.py",
    # Only the opener token leaves the line, not the rest of the line it sits
    # on.  Dropping to the newline is the same "raw text is data" mistake one
    # token over, and the command after the write was never judged.
    f"cat > f <<'EOF' && {CUDA} train.py\nbody\nEOF",
    "cat > f <<'EOF' && sbatch job.sh\nbody\nEOF",
    f"cat > f <<'EOF'; {CUDA} train.py\nbody\nEOF",
])
def test_a_boundary_the_shell_honours_is_a_boundary_here(command: str) -> None:
    assert _blocked(command) is True, command


@pytest.mark.parametrize("command", [
    # A quoted ``&&`` inside a payload is argv for another process, not a
    # boundary.  This is the shape the whole-command cut existed for.
    f"{PBRUN} --gpu -- bash -lc 'cd /home/rob/tmp/ts50 && {CUDA} -m pytest -q'",
    f'{PBRUN} --gpu -- bash -lc "cd /home/rob/tmp && {CUDA} x.py"',
    # The lane is what runs ``sbatch`` on this fleet's behalf, so a payload
    # naming the verb behind a pool entrypoint is the sanctioned path.
    f"{PBRUN} --transport slurm --gpu -- sbatch job.sh",
    # A quoted here-document to a writer is bytes on their way to a file, and
    # the file that starts the fleet's workers names the CUDA interpreter by
    # construction.  The hook refused that write four times.
    "cat > tools/fleet/fleet_boxes.json <<'JSON'\n"
    '  {"args": ["--python", "' + CUDA + '"]}\n'
    "JSON",
    # A substitution beside an exempt leader leaves the leader exempt.
    f"git commit -m \"$(cat msg.txt)\" -m 'stop calling {CUDA}'",
    # The two inspection switches, which print a path and run nothing.
    "command -v sbatch",
    "command -V srun",
    "command -p -v sbatch",
    # And everything the hook already allowed, in a boundary-heavy shape.
    f"cd /mnt/shared && {PBRUN} --gpu -- {CUDA} -m pytest && echo done",
    # Two quoted here-documents on one line are two bodies the shell reads in
    # order, so the second is a body and not a command standing where one
    # should be.
    "cat > f <<'A' <<'B'\nfirst " + CUDA + "\nA\nsecond " + CUDA + "\nB",
])
def test_the_shapes_that_must_stay_allowed_stay_allowed(command: str) -> None:
    """Over-refusal is this hook's documented failure mode: five lockouts."""

    assert _blocked(command) is False, command


def test_the_payload_cut_no_longer_eats_the_outer_command() -> None:
    """The cause, stated as segments: two commands, and both are judged."""

    command = f"python3 pbrun.py --gpu -- true && {CUDA} train.py"
    segments = hook._segments(command)

    assert len(segments) == 2, segments
    assert segments[0] == "python3 pbrun.py --gpu", (
        "the payload is still cut off its own segment")
    assert segments[1] == f"{CUDA} train.py"


def test_a_quoted_separator_inside_a_payload_is_not_a_boundary() -> None:
    command = f"{PBRUN} --gpu -- bash -lc 'cd x && {CUDA} -m pytest'"

    assert len(hook._segments(command)) == 1, hook._segments(command)


def test_the_armed_hook_blocks_the_reported_shapes_with_exit_2(
    tmp_path: Path,
) -> None:
    """The verdict an agent actually sees, through ``main``."""

    flag = tmp_path / "on"
    flag.write_text("")
    hook.FLAG = flag
    try:
        for command, expected in (
            (f"python3 pbrun.py --gpu -- true && {CUDA} train.py", 2),
            ("python3 pbrun.py -- true && sbatch job.sh", 2),
            ("command sbatch job.sh", 2),
            ("command -v sbatch", 0),
            (f"{PBRUN} --gpu -- bash -lc 'cd x && {CUDA} -m pytest'", 0),
        ):
            stdin = io.StringIO(json.dumps({"tool_input": {"command": command}}))
            old = sys.stdin
            sys.stdin = stdin
            try:
                with contextlib.redirect_stderr(io.StringIO()):
                    assert hook.main() == expected, command
            finally:
                sys.stdin = old
    finally:
        hook.FLAG = Path("/home/rob/tmp/arb/require_pool.on")


@pytest.mark.parametrize("name,command", [
    # A commit message is written here as a here-document inside a command
    # substitution, and this repo's messages quote the rule they change.
    ("a commit message built from a here-document",
     f"git commit -m \"$(cat <<'EOF'\nRoute {CUDA} through the pool\nEOF\n)\""),
    ("the same message naming the scheduler",
     "git commit -m \"$(cat <<'EOF'\nStop calling sbatch by hand\nEOF\n)\""),
    ("a message whose prose has an apostrophe",
     "git commit -m \"$(cat <<'EOF'\nThe hook doesn't refuse sbatch\nEOF\n)\""),
    # A comment is not a command, and an apostrophe inside one used to open a
    # quote that ran on and swallowed the next line.
    ("a comment with an apostrophe above a real command",
     f"# don't run this locally\ngit commit -m \"route {CUDA} through pool\""),
    # Compound commands put a reserved word where the command word goes.
    ("a for loop grepping for the interpreter",
     f"for f in a b; do grep -l {CUDA} $f; done"),
    ("an if condition grepping for the verb",
     "if grep -q sbatch f; then echo y; fi"),
    ("a while loop reading a list",
     'while read -r l; do grep sbatch "$l"; done < list'),
    ("a brace group",
     "{ grep sbatch f; }"),
    ("a negated condition",
     "! grep -q sbatch f"),
    ("time in front of an inspection",
     "time grep sbatch f"),
    # POSIX has three spellings of a quoted delimiter, and all three stop
    # expansion, so all three bodies are bytes on their way to a file.
    ("a backslash-quoted delimiter writing the fleet's own config",
     f'cat > f <<\\EOF\n{{"python": "{CUDA}"}}\nEOF'),
    ("a pipeline that only merges its streams",
     "git log |& grep sbatch"),
    # The words around a here-document decide whether its body runs, and
    # these three name an interpreter without running one.
    ("a body counted rather than executed",
     f"cat <<'EOF' | grep -c python\n--python {CUDA}\nEOF"),
    ("a writer whose file is named after a shell",
     f"cat bash_notes <<'EOF'\n--python {CUDA}\nEOF"),
    ("a shell run after the write, not fed by it",
     f"cat > f <<'EOF' && bash other.sh\n--python {CUDA}\nEOF"),
])
def test_shapes_the_shell_never_runs_are_not_refused(
    name: str, command: str,
) -> None:
    """Over-refusals found by review, each one shell text read as a command.

    Three of these were introduced by the boundary rules themselves: reading a
    here-document body inside a command substitution, and letting an
    apostrophe in prose or in a comment open a quote. The hook's documented
    failure mode is refusing the work of complying with it, and refusing the
    commit that fixes it is that failure exactly.
    """

    assert _blocked(command) is False, name


@pytest.mark.parametrize("name,command", [
    # The interpreter reading the body is not always the command word.
    ("a shell reached over ssh",
     "ssh sparklina bash <<'EOF'\nsbatch job.sh\nEOF"),
    ("a shell reached through sudo",
     f"sudo bash <<'EOF'\n{CUDA} train.py\nEOF"),
    ("a shell inside a container",
     f"docker exec -i c bash <<'EOF'\n{CUDA} train.py\nEOF"),
    # A pipeline hands the body to the next command as its input, so the
    # interpreter is one command further along than the writer.
    ("a here-document piped into a shell",
     f"cat <<'EOF' | bash\n{CUDA} train.py\nEOF"),
    ("a here-document piped into a shell over ssh",
     "cat <<'EOF' | ssh lina bash\nsbatch job.sh\nEOF"),
    # A single ``&`` backgrounds what precedes it and starts a new command.
    ("a backgrounded command in front of GPU work",
     f"echo start & {CUDA} train.py"),
    ("a backgrounded command in front of a submission",
     "echo start & sbatch job.sh"),
    # An interpreter body is inspected whatever the delimiter's quoting, which
    # is what the issue asks for.
    ("python executing its standard input",
     "python3 - <<'EOF'\nimport subprocess; subprocess.run(['sbatch','j.sh'])\nEOF"),
    ("a substitution body that chains past the message",
     f"git commit -m \"$(cat <<'EOF'\nprose\nEOF\n)\" && {CUDA} t.py"),
])
def test_work_the_shell_does_run_is_still_refused(
    name: str, command: str,
) -> None:
    """Missed catches found by review, each a real shape on this fleet."""

    assert _blocked(command) is True, name


@pytest.mark.parametrize("name,command", [
    ("a hash inside a word is not a comment",
     f"grep foo#bar f && {CUDA} x"),
    ("a redirection that merges streams is not a boundary",
     f"{CUDA} train.py 2>&1 | tee log"),
    ("a redirection to a file is not a boundary",
     f"{CUDA} train.py &> log"),
])
def test_the_characters_that_only_look_like_boundaries(
    name: str, command: str,
) -> None:
    """``#`` mid-word and ``&`` in a redirection belong to their command.

    Each of these hides GPU work behind a character the boundary rules read.
    If ``#`` began a comment anywhere, or ``&`` ended a command anywhere, the
    interpreter would fall outside the text being judged.
    """

    assert _blocked(command) is True, name


def test_a_substitution_carries_its_own_here_document() -> None:
    """The segments of the commit idiom, so the reason is visible."""

    command = "git commit -m \"$(cat <<'EOF'\nprose about sbatch\nEOF\n)\""
    segments = hook._segments(command)

    assert [hook._first_token(segment) for segment in segments] == ["cat", "git"]
    assert not any("prose about sbatch" in segment for segment in segments), (
        "the body is bytes on their way to git, not a command")


def test_a_comment_ends_at_its_own_newline() -> None:
    segments = hook._segments("git status # don't\nsbatch job.sh")

    assert segments == ["git status", "sbatch job.sh"], segments


def test_a_backgrounded_command_is_its_own_segment() -> None:
    segments = hook._segments("echo start & sbatch job.sh")

    assert segments == ["echo start", "sbatch job.sh"], segments

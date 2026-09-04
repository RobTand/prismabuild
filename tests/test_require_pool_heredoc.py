"""A here-document body is data on its way to a file, not a command.

The hook refused, five times in one day, the work of repairing the hook and
the fleet it points at.  The last of those was structural rather than
accidental: `tools/fleet/fleet_boxes.json` declares each box's worker
arguments, and a GB10 worker's `--python` argument *is* the CUDA interpreter,
so the file that starts the fleet contains the refused pattern by
construction.  Writing it with a heredoc put that pattern into a command
string whose leading token was `cd`, and the hook refused the write.

Nothing inside a heredoc can start GPU work -- the shell is copying bytes to a
file -- so the body is not a command and is not judged.  What follows the
heredoc still is.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "require_pool",
    Path(__file__).resolve().parents[1] / "tools" / "fleet" / "require_pool.py",
)
hook = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(hook)                        # type: ignore[union-attr]

#: Assembled rather than written, so this file does not itself trip the rule
#: it tests when an agent edits it from a shell.
CUDA = "/home/rob/dq-runs/venvs/" + "prismaquant-cu130" + "/bin/python"


def test_a_heredoc_body_naming_the_cuda_interpreter_is_not_gpu_work() -> None:
    command = (
        "cd /home/rob/prismabuild && cat > tools/fleet/fleet_boxes.json <<'JSON'\n"
        '  {"args": ["--python", "' + CUDA + '"]}\n'
        "JSON"
    )
    assert hook.contends(command) is False


def test_a_command_after_a_heredoc_is_still_judged() -> None:
    """The exemption covers the body, not everything downstream of it."""

    command = (
        "cat > note.txt <<EOF\n"
        "harmless\n"
        "EOF\n"
        + CUDA + " train.py"
    )
    assert hook.contends(command) is True


def test_a_command_before_a_heredoc_is_still_judged() -> None:
    command = (
        CUDA + " train.py && cat > note.txt <<EOF\n"
        "harmless\n"
        "EOF"
    )
    assert hook.contends(command) is True


def test_an_unterminated_heredoc_does_not_swallow_the_whole_command() -> None:
    """A body that never closes is still body; the head of it is still judged."""

    command = CUDA + " train.py && cat > f <<EOF\n" + CUDA + " sneaky.py"
    assert hook.contends(command) is True


def test_two_heredocs_in_one_command_are_both_data() -> None:
    command = (
        "cat > a <<'A'\n" + CUDA + "\nA\n"
        "cat > b <<'B'\n" + CUDA + "\nB"
    )
    assert hook.contends(command) is False


def test_real_off_pool_gpu_work_is_still_refused() -> None:
    """The point of the hook, unchanged."""

    assert hook.contends("cd /home/rob/tessera && " + CUDA + " -m pytest") is True


def test_a_pool_submission_carrying_that_interpreter_is_still_allowed() -> None:
    assert hook.contends(
        "/usr/bin/python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py "
        '--gpu -- bash -lc "' + CUDA + ' x.py"'
    ) is False

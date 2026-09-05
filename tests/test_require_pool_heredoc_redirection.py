"""A redirection between a here-document opener and its pipe is not a boundary.

Issue #89's fix taught the hook that a here-document body piped into an
interpreter is a script rather than data, by looking at the rest of the
pipeline the opener sits in.  ``_pipeline_after`` stops that look at the first
``;``, ``&&``, ``||``, ``&`` or newline, and it stopped at every ``&``,
including the one inside a redirection.  So ``cat <<'EOF' 2>&1 | bash`` ended
the pipeline at the ``&`` of ``2>&1``, the ``| bash`` downstream was never
seen, and the body went back to being treated as data.  The body is a script
either way: redirecting ``cat``'s stderr says nothing about where its stdout
goes.

``_commands_in`` already draws this distinction, because a lone ``&``
backgrounds a command while ``2>&1``, ``>&2``, ``&>`` and ``<&3`` belong to
the command they sit in.  The two scanners now answer it the same way.

Nothing here starts GPU work, submits a job, or reads the live flag: the
module is loaded directly and asked about strings.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "require_pool.py"

_SPEC = importlib.util.spec_from_file_location("require_pool_heredoc_redir", HOOK)
hook = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(hook)                        # type: ignore[union-attr]

#: Assembled rather than written, so this file does not itself trip the rule
#: it tests when an agent edits it from a shell.
CUDA = "/home/rob/dq-runs/venvs/" + "prismaquant-cu130" + "/bin/python"


def _blocked(command: str) -> bool:
    return bool(hook.contends(command) or hook.submits(command))


@pytest.mark.parametrize("name,redirection", [
    ("stderr onto stdout", "2>&1"),
    ("stderr onto a descriptor", "2>&3"),
    ("both streams", "&>/dev/null"),
    ("stdin from a descriptor", "<&3"),
])
def test_a_redirection_before_the_pipe_still_reaches_the_interpreter(
    name: str, redirection: str,
) -> None:
    """The body is a script whatever the writer does with its other streams."""

    command = f"cat <<'EOF' {redirection} | bash\n{CUDA} train.py\nEOF"
    assert _blocked(command) is True, name


@pytest.mark.parametrize("name,redirection", [
    ("stderr onto stdout", "2>&1"),
    ("both streams", "&>/dev/null"),
])
def test_the_same_redirection_before_a_writer_leaves_the_body_data(
    name: str, redirection: str,
) -> None:
    """No interpreter downstream, so the body is still text being written."""

    command = f"cat <<'EOF' {redirection} > notes.txt\n{CUDA} train.py\nEOF"
    assert _blocked(command) is False, name


def test_a_real_background_operator_is_still_a_boundary() -> None:
    """The lone ``&`` the redirections only look like keeps ending a pipeline."""

    command = f"cat <<'EOF' & bash other.sh\nplain text\nEOF"
    assert hook._pipeline_after(command, command.index(" &")) == " "

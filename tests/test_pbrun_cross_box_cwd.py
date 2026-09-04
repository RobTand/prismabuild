"""A checkout on another box is not a typo, and must not be reported as one.

``pbrun`` stamps the code closure *inside* the checkout and computes the
closure from it, so it can only submit for a checkout that exists on the box
it is running on.  That is a real constraint and it is not going away here.
What was wrong was the report: ``--cwd is not a directory`` is exactly right
for a typo and exactly wrong for the cross-box case, where the path is correct
and simply belongs to a different filesystem.  Told "the directory does not
exist", the obvious next move is to go and create it -- which would produce an
empty checkout on the wrong box and a closure taken over nothing.

This pins the two things the message has to carry: which box could not see the
path, and that the fix is to submit from the box that holds it.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import socket
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

_SPEC = importlib.util.spec_from_file_location(
    "pbrun", Path(__file__).resolve().parents[1] / "tools" / "fleet" / "pbrun.py"
)
pbrun = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbrun)                       # type: ignore[union-attr]


def _refusal(argv: list[str], monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setattr(sys, "argv", ["pbrun.py", *argv])
    with pytest.raises(SystemExit) as caught:
        pbrun.main()
    return str(caught.value)


def test_the_refusal_names_the_box_that_could_not_see_the_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message = _refusal(
        ["--cwd", "/home/rob/tessera-verify-on-the-other-box", "--", "true"],
        monkeypatch,
    )
    assert socket.gethostname() in message, message
    assert "/home/rob/tessera-verify-on-the-other-box" in message, message


def test_the_refusal_says_to_submit_from_the_box_that_holds_the_checkout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The guidance is the whole point of the change; a future edit that
    shortens the message must not quietly drop it."""

    message = _refusal(
        ["--cwd", "/definitely/not/here", "--", "true"], monkeypatch,
    )
    assert "submit from there" in message, message
    # And say WHY, so the constraint is learnable rather than arbitrary.
    assert "closure" in message, message

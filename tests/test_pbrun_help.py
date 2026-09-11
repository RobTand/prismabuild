"""What ``pbrun --help`` says about the tool, checked as text.

Help output is the only documentation an agent at a terminal reads, so a claim
in it is as load-bearing as a claim in the guide -- and it goes stale the same
way.  ``--help`` is also cheap: the parser is built and printed without a
checkout, a queue, or a controller.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

# Bound before ``pbrun`` is exec'd, as ``test_pbrun_placement`` explains:
# ``pbrun`` puts the published mirror at the front of ``sys.path``, and this
# file is meant to test the checkout it lives in.
from prismabuild import pool as pool_module  # noqa: E402,F401

_SPEC = importlib.util.spec_from_file_location(
    "pbrun", Path(__file__).resolve().parents[1] / "tools" / "fleet" / "pbrun.py"
)
pbrun = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbrun)                       # type: ignore[union-attr]


def _help(monkeypatch: pytest.MonkeyPatch, capsys) -> str:
    """The whole of ``pbrun --help``, wrapped wide enough to read a phrase in."""

    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.setattr(sys, "argv", ["pbrun.py", "--help"])
    with pytest.raises(SystemExit) as raised:
        pbrun.main()
    assert raised.value.code == 0
    return capsys.readouterr().out


def test_the_description_names_no_transport_as_the_one_that_carries_work(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    """"Submit one command to the PrismaBuild pool" outlived the pool's monopoly.

    Two dispatchers carry work, the result does not depend on which, and which
    one a submission takes is ``--transport`` or the published generation's
    default.  A description naming the pull queue tells a reader on the lane
    that they are using the wrong tool.
    """

    text = _help(monkeypatch, capsys)

    assert "PrismaBuild pool" not in text
    assert "Submit one command to the PrismaBuild fleet and wait for it" in text


def test_priority_says_what_it_is_and_what_each_transport_does_with_it(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    """``--priority`` printed its name, its type and nothing else.

    Three facts a submitter needs and cannot read off the flag: it is a queue
    hint rather than part of the action's identity, so two submissions that
    differ only in priority are one action; the pull queue sorted its ready
    list on it ahead of age; and the lane spends it as a ``--nice``, scaled so
    one step outranks submission order rather than one later submission.
    """

    text = _help(monkeypatch, capsys)
    # The last mention is the option list's; the first is the usage line's.
    priority = text[text.rindex("--priority"):]
    priority = priority[:priority.index("--env")]

    assert "higher runs sooner" in priority
    assert "not part of the action" in priority
    assert "before age" in priority
    assert "--nice" in priority
    assert "outranks submission order" in priority


def test_progress_names_both_supported_spellings_and_the_required_allowance(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    text = _help(monkeypatch, capsys)
    option = next(line for line in text.splitlines()
                  if line.lstrip().startswith("--progress-phase"))
    assert "--progress " in option
    assert "NAME=SECONDS" in option

"""Every fleet tool flag carries help text.

An operator meets these tools through ``--help`` and nothing else. A flag
declared without ``help=`` is listed there with its name and no explanation,
which tells the reader that the flag exists and withholds the one thing they
came for. Six such flags accumulated before anyone noticed, because nothing
was looking.

Read statically, with ``ast``, and never by importing the files under test.
Importing a fleet tool does the tool's work: on 2026-09-05 an import sweep
served the live pool queue and overwrote a recorded result in the live store.
``tests/test_tools_do_not_run_on_import.py`` carries that story in full. A
help string is a property of the source, so reading the source is also the
honest way to check it.

``help=argparse.SUPPRESS`` counts as help. It is a deliberate statement that
a flag is not for operators, which is an answer rather than an omission.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

TOOLS = sorted((Path(__file__).resolve().parents[1] / "tools" / "fleet").glob("*.py"))


def _argument_name(call: ast.Call) -> str:
    """The flag or positional this ``add_argument`` call declares."""

    for argument in call.args:
        if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
            return argument.value
    for keyword in call.keywords:
        if keyword.arg == "dest" and isinstance(keyword.value, ast.Constant):
            return str(keyword.value.value)
    return "<unnamed>"


def _has_help(call: ast.Call) -> bool:
    """Whether the call declares help text that says something."""

    for keyword in call.keywords:
        if keyword.arg != "help":
            continue
        value = keyword.value
        if isinstance(value, ast.Constant):
            # An empty or absent string is the omission wearing a keyword.
            return bool(value.value)
        return True
    return False


def _add_argument_calls(source: str) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "add_argument"
    ]


@pytest.mark.parametrize("tool", TOOLS, ids=lambda path: path.name)
def test_every_declared_argument_explains_itself(tool: Path) -> None:
    undocumented = [
        f"{tool.name}:{call.lineno} {_argument_name(call)}"
        for call in _add_argument_calls(tool.read_text())
        if not _has_help(call)
    ]
    assert not undocumented, (
        "these arguments appear in --help with no explanation: "
        + ", ".join(undocumented)
    )

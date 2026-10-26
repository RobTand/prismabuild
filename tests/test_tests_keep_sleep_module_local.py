"""Test clocks must not replace the sleeper used by subprocess and pytest (#1782)."""
from __future__ import annotations

import ast
from pathlib import Path

import pytest


def _shared_sleep_patches(source):
    tree = ast.parse(source)
    aliases = {"time"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            aliases.update(alias.asname or alias.name for alias in node.names
                           if alias.name == "time")
    violations = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        first = node.args[0]
        if (isinstance(first, ast.Constant) and isinstance(first.value, str)
                and (first.value == "time.sleep" or first.value.endswith(".time.sleep"))):
            if isinstance(node.func, ast.Attribute) and node.func.attr in {"setattr", "patch"}:
                violations.append(node.lineno)
            elif isinstance(node.func, ast.Name) and node.func.id == "patch":
                violations.append(node.lineno)
        elif (isinstance(node.func, ast.Attribute)
              and node.func.attr in {"setattr", "object"}
              and len(node.args) >= 2
              and isinstance(node.args[1], ast.Constant)
              and node.args[1].value == "sleep"):
            target = ast.unparse(first)
            if target in aliases or target.endswith(".time"):
                violations.append(node.lineno)
    return violations


def test_tests_keep_sleep_module_local():
    violations = []
    for path in sorted(Path(__file__).parent.rglob("*.py")):
        violations.extend(f"{path.name}:{line}" for line in _shared_sleep_patches(path.read_text()))
    assert not violations, "Replace the owner's time binding instead:\n" + "\n".join(violations)


@pytest.mark.parametrize("source", [
    'monkeypatch.setattr(time, "sleep", fake)',
    'monkeypatch.setattr(owner.time, "sleep", fake)',
    'monkeypatch.setattr("time.sleep", fake)',
    'mock.patch("time.sleep", fake)',
    'patch("time.sleep", fake)',
    'mock.patch.object(time, "sleep", fake)',
    'import time as clock\nmonkeypatch.setattr(clock, "sleep", fake)',
])
def test_shared_sleeper_patch_spellings_are_rejected(source):
    assert _shared_sleep_patches(source)


def test_a_module_local_clock_is_allowed():
    assert not _shared_sleep_patches('monkeypatch.setattr(owner, "time", clock)')

"""Test clocks must not replace the sleeper used by subprocess and pytest (#1782)."""
from __future__ import annotations

import ast
from pathlib import Path


def test_tests_keep_sleep_module_local():
    violations = []
    for path in sorted(Path(__file__).parent.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
            if not isinstance(node, ast.Call):
                continue
            if (isinstance(node.func, ast.Attribute)
                    and node.func.attr in {"setattr", "object"}
                    and len(node.args) >= 2
                    and isinstance(node.args[1], ast.Constant)
                    and node.args[1].value == "sleep"):
                target = ast.unparse(node.args[0])
                if target == "time" or target.endswith(".time"):
                    violations.append(f"{path.name}:{node.lineno}: {target}.sleep")
    assert not violations, "Replace the owner's time binding instead:\n" + "\n".join(violations)

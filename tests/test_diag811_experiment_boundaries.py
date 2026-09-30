"""CPU witnesses for the research harness's timing and source boundaries."""
from __future__ import annotations

import ast
import importlib.util
import types
from pathlib import Path

import pytest


@pytest.fixture
def harness():
    path = (Path(__file__).resolve().parents[1] / "tools" / "maintenance"
            / "diag_811_e1_checkout_cache.py")
    spec = importlib.util.spec_from_file_location("diag811_test_harness", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _recorded_seconds(harness, arm, phases):
    """Execute the actual pure expression that populates an arm's record.

    No Git, CAS, queue, clock or performance measurement is simulated here.
    This isolates the arithmetic seam without materializing a snapshot.
    """
    tree = ast.parse(Path(harness.__file__).read_text())
    function = next(node for node in tree.body
                    if isinstance(node, ast.FunctionDef)
                    and node.name == f"arm_{arm.lower()}_rep")
    assignment = next(node for node in ast.walk(function)
                      if isinstance(node, ast.Assign)
                      and any(isinstance(target, ast.Name)
                              and target.id == "materialize_seconds"
                              for target in node.targets))
    expression = ast.fix_missing_locations(ast.Expression(assignment.value))
    return eval(compile(expression, str(harness.__file__), "eval"),
                vars(harness), {"phase": types.SimpleNamespace(seconds=phases)})


@pytest.mark.parametrize("verify_seconds", [0.0, 3.0])
def test_both_timed_arms_include_the_same_verification_boundary(harness, verify_seconds):
    a = {"materialize": 2.0, "verify": verify_seconds, "cleanup": 1.0}
    # B computes its headline before cleanup; A computes it afterward.
    b = {"git_init": 0.25, "copy_objects": 1.25, "checkout": 0.5,
         "verify": verify_seconds}
    expected = 2.0 + verify_seconds
    assert _recorded_seconds(harness, "A", a) == expected
    assert _recorded_seconds(harness, "B", b) == expected


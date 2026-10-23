"""Only the live clamp can supply capacity to a claim transition."""
import ast
import copy
from pathlib import Path
import pickle
import shutil
import subprocess
import sys

import pytest

from prismabuild import host_cap, pool

ROOT = Path(__file__).resolve().parents[1]


def test_only_registered_immutable_capacity_is_usable():
    source = {"cpu": 2, "mem_gb": 4}
    value = host_cap.enforce_host_cap(source, None, {})
    source["cpu"] = 90
    assert value == {"cpu": 2, "mem_gb": 4}
    assert host_cap.require_capped_capacity(value) is value
    with pytest.raises(TypeError):
        host_cap.CappedCapacity(source)
    with pytest.raises(TypeError):
        class Forged(host_cap.CappedCapacity):
            pass
    with pytest.raises(AttributeError):
        value._authority = object()
    with pytest.raises(TypeError):
        value["cpu"] = 90
    for duplicate in (copy.copy, copy.deepcopy, pickle.dumps):
        with pytest.raises(TypeError):
            duplicate(value)


@pytest.mark.parametrize("route", ["claim", "serve_once", "_claim", "_claim_pass", "_start_claim_scan"])
@pytest.mark.parametrize("forgery", ["dict", "new", "duck"])
def test_every_claim_route_refuses_unminted_capacity(tmp_path, route, forgery):
    genuine = host_cap.enforce_host_cap({"cpu": 2, "mem_gb": 4}, None, {})
    if forgery == "dict":
        value = dict(genuine)
    elif forgery == "new":
        value = object.__new__(host_cap.CappedCapacity)
    else:
        value = type("LooksCapped", (), {"get": lambda *args: 2})()
    queue = pool.PoolQueue(tmp_path / "queue")
    with pytest.raises(TypeError, match="enforce_host_cap"):
        getattr(queue, route)(capacity=value)
    assert not queue.root.exists(), "invalid capability reached queue I/O"


def test_only_the_clamp_constructs_capped_capacity():
    constructors = []
    for directory in (ROOT / "src", ROOT / "tools"):
        for path in directory.rglob("*.py"):
            tree = ast.parse(path.read_bytes())
            aliases = {"CappedCapacity"}
            aliases.update(alias.asname or alias.name for node in ast.walk(tree)
                           if isinstance(node, ast.ImportFrom) for alias in node.names
                           if alias.name == "CappedCapacity")

            def visit(node, owner=None):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    owner = node.name
                if isinstance(node, ast.Call):
                    function = node.func
                    name = function.id if isinstance(function, ast.Name) else (
                        function.attr if isinstance(function, ast.Attribute) else None)
                    if name in aliases:
                        constructors.append((str(path.relative_to(ROOT)), owner))
                for child in ast.iter_child_nodes(node):
                    visit(child, owner)

            visit(tree)
    assert constructors
    assert set(constructors) == {("src/prismabuild/host_cap.py", "enforce_host_cap")}


def test_builder_tests_kill_a_final_claim_cap_bypass(tmp_path):
    # Mutate a private copy. The action's source and sibling tests stay intact.
    mutant = tmp_path / "mutant"
    shutil.copytree(ROOT, mutant, ignore=shutil.ignore_patterns(
        ".git", "__pycache__", ".pytest_cache", ".pbrun-closure.*", "qualification_deps"))
    path = mutant / "tools/fleet/worker_loop.py"
    source = path.read_text()
    tree = ast.parse(source)
    fence = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                 and node.name == "claim_capacity_fence")
    assignments = [node for node in ast.walk(fence) if isinstance(node, ast.Assign)
                   and isinstance(node.value, ast.Call)
                   and isinstance(node.value.func, ast.Attribute)
                   and node.value.func.attr == "enforce_host_cap"]
    assert len(assignments) == 1
    expression = assignments[0].value
    lines = source.splitlines(keepends=True)
    assert expression.lineno == expression.end_lineno
    line = lines[expression.lineno - 1]
    lines[expression.lineno - 1] = (line[:expression.col_offset] + "dict(capacity)"
                                   + line[expression.end_col_offset:])
    path.write_text("".join(lines))
    result = subprocess.run([sys.executable, "-m", "pytest", "-o", "addopts=", "-ra",
        "tests/test_capped_partial_host.py", "tests/test_enrolling_worker.py",
        "tests/test_capped_host_fences.py"], cwd=mutant, capture_output=True,
        text=True, timeout=180)
    (tmp_path / "mutant.stdout").write_text(result.stdout)
    (tmp_path / "mutant.stderr").write_text(result.stderr)
    assert result.returncode == 1, "builder suite accepted the reviewer's dict(capacity) bypass"
    assert "test_worker_final_fence_observes_cap_change" in result.stdout
    assert "test_private_claim_rechecks_cap_after_sampling" in result.stdout
    assert "admission requires capacity from enforce_host_cap" in result.stdout

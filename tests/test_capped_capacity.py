"""Only the live clamp can supply capacity to a claim transition."""
import ast
import copy
from pathlib import Path
import pickle
import os
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


def _capability_references(source):
    tree = ast.parse(source)
    annotations = set()
    for node in ast.walk(tree):
        annotation = (node.annotation if isinstance(node, (ast.arg, ast.AnnAssign)) else
                      node.returns if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) else None)
        if annotation is not None:
            annotations.update(ast.walk(annotation))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "isinstance":
            if len(node.args) == 2:
                annotations.update(ast.walk(node.args[1]))
    aliases = {"CappedCapacity", "ClaimFence", "ClaimFenceProof"}
    private = {"_CAPACITY_MINT", "_CAPPED_VALUES", "_ADMISSION_REGISTRY", "_AdmissionRegistry"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name in aliases | private:
                    aliases.add(alias.asname or alias.name)
    violations = []
    for node in ast.walk(tree):
        name = (node.id if isinstance(node, ast.Name) else
                node.attr if isinstance(node, ast.Attribute) else
                node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else
                node.name if isinstance(node, ast.alias) else None)
        if name in private or (name in aliases and node not in annotations and not isinstance(node, ast.alias)):
            violations.append((node.lineno, name))
        if isinstance(node, ast.Attribute) and node.attr == "__globals__":
            if any(isinstance(child, ast.Attribute) and child.attr == "__new__" for child in ast.walk(node.value)):
                violations.append((node.lineno, "reflected constructor globals"))
    return violations


def test_only_the_clamp_constructs_capped_capacity():
    violations = []
    for directory in (ROOT / "src", ROOT / "tools"):
        for path in directory.rglob("*.py"):
            if path == ROOT / "src/prismabuild/host_cap.py":
                continue
            violations.extend((str(path.relative_to(ROOT)), *row) for row in _capability_references(path.read_text()))
    assert not violations, "admission capability escaped its owning module: " + repr(violations)
    tree = ast.parse((ROOT / "src/prismabuild/host_cap.py").read_text())
    mints = []
    for owner in tree.body:
        if not isinstance(owner, ast.FunctionDef):
            continue
        for node in ast.walk(owner):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name) and node.func.value.id == "_ADMISSION_REGISTRY"
                    and node.func.attr == "mint"):
                mints.append((owner.name, node.args[0].id))
    assert set(mints) == {("enforce_host_cap", "CappedCapacity"),
                          ("prepare_claim_fence", "ClaimFence"), ("check_claim_fence", "ClaimFenceProof")}


@pytest.mark.parametrize("source", [
    "cls = host_cap.CappedCapacity; cls({})",
    "from prismabuild.host_cap import CappedCapacity as CC\ncls = CC; cls({})",
    "getattr(host_cap, 'CappedCapacity')({})",
    "host_cap.__dict__['CappedCapacity']({})",
    "type(value).__new__.__globals__['_ADMISSION_REGISTRY']",
    "from prismabuild.host_cap import _ADMISSION_REGISTRY as registry",
    "host_cap._CAPACITY_MINT", "host_cap._CAPPED_VALUES",
    "cls = host_cap.ClaimFenceProof; cls()",
])
def test_the_capability_guard_catches_aliases_and_reflection(source):
    assert _capability_references(source)


def test_the_capability_guard_allows_annotations_and_isinstance():
    assert not _capability_references("def f(value: host_cap.CappedCapacity) -> host_cap.ClaimFenceProof: pass")
    assert not _capability_references("isinstance(value, host_cap.CappedCapacity)")


def test_builder_tests_kill_a_final_claim_cap_bypass(tmp_path):
    # Mutate a private copy. The action's source and sibling tests stay intact.
    mutant = tmp_path / "mutant"
    shutil.copytree(ROOT, mutant, ignore=shutil.ignore_patterns(
        ".git", "__pycache__", ".pytest_cache", ".pbrun-closure.*", "qualification_deps"))
    path = mutant / "src/prismabuild/host_cap.py"
    source = path.read_text()
    old = "fresh = recheck_host_cap(capacity, detail=detail)"
    assert source.count(old) == 1
    path.write_text(source.replace(old, "fresh = dict(capacity)"))
    paths = [str(mutant / relative) for relative in ("src", "tools/fleet", "tests")]
    (mutant / "tests/test_zz_mutant_origin.py").write_text(
        "from pathlib import Path\nimport prismabuild.host_cap, prismabuild.pool, worker_loop\n"
        "def test_mutated_modules_are_loaded():\n"
        "    root = Path(__file__).resolve().parents[1]\n"
        "    for module in (prismabuild.host_cap, prismabuild.pool, worker_loop):\n"
        "        assert Path(module.__file__).resolve().is_relative_to(root)\n")
    result = subprocess.run([sys.executable, "-m", "pytest", "-o", "addopts=", "-ra",
        "tests/test_capped_partial_host.py", "tests/test_enrolling_worker.py",
        "tests/test_capped_host_fences.py", "tests/test_zz_mutant_origin.py"], cwd=mutant, capture_output=True,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(paths)},
        text=True, timeout=180)
    (tmp_path / "mutant.stdout").write_text(result.stdout)
    (tmp_path / "mutant.stderr").write_text(result.stderr)
    assert result.returncode == 1, "builder suite accepted the reviewer's dict(capacity) bypass"
    assert "test_worker_final_fence_observes_cap_change" in result.stdout
    assert "test_private_claim_rechecks_cap_after_sampling" in result.stdout
    assert "admission requires capacity from enforce_host_cap" in result.stdout

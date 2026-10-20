"""Source guards for the host panic triggered by reading the global lock list."""
import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _path_literal(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp):
        left, right = _path_literal(node.left), _path_literal(node.right)
        if left is not None and right is not None:
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Div):
                return left.rstrip("/") + "/" + right.lstrip("/")
    if isinstance(node, ast.Call) and (
        isinstance(node.func, ast.Name) and node.func.id in {"Path", "PurePosixPath"}
        or isinstance(node.func, ast.Attribute) and node.func.attr in {"Path", "PurePosixPath"}
    ):
        parts = [_path_literal(arg) for arg in node.args]
        if parts and all(part is not None for part in parts):
            return "/".join(part.rstrip("/") for part in parts)
    return None


def _assert_safe_source(path):
    source = path.read_text(encoding="utf-8")
    assert "/proc/locks" not in source, f"unsafe global lock table reference: {path}"
    if path.suffix == ".py":
        assert not any(_path_literal(node) == "/proc/locks"
                       for node in ast.walk(ast.parse(source))), path


def test_runtime_sources_never_read_the_global_lock_table():
    for directory in (ROOT / "src", ROOT / "tools"):
        for path in directory.rglob("*"):
            if path.is_file() and path.suffix in {".py", ".sh"}:
                _assert_safe_source(path)


def _assert_no_fdinfo_source(path):
    tree = ast.parse(path.read_text())
    assert not any(isinstance(node, ast.Constant)
                   and isinstance(node.value, str) and "fdinfo" in node.value
                   for node in ast.walk(tree)), path


def test_holder_diagnostics_do_not_read_fdinfo():
    for path in (ROOT / "src/prismabuild/flock_owner.py",
                 ROOT / "tools/fleet/mount_latency.py"):
        _assert_no_fdinfo_source(path)


@pytest.mark.parametrize("source", [
    "open('/proc/locks').read()",
    "Path('/proc/locks').read_text()",
    "open('/proc/' + 'locks').read()",
    "(Path('/proc') / 'locks').read_bytes()",
    "Path('/proc', 'locks').read_text()",
])
def test_planted_global_lock_reads_fail_the_source_guard(tmp_path, source):
    planted = tmp_path / "planted.py"
    planted.write_text(source)
    with pytest.raises(AssertionError):
        _assert_safe_source(planted)


def test_planted_fdinfo_read_fails_the_diagnostic_guard(tmp_path):
    planted = tmp_path / "planted.py"
    planted.write_text("Path(f'/proc/{pid}/fdinfo/{fd}').read_text()")
    with pytest.raises(AssertionError):
        _assert_no_fdinfo_source(planted)

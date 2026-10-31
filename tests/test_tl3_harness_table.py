"""TL3 runs this repository's tests by node ID through ``[tool.tl3.harness]``.

TL3's harness reads the table from ``pyproject.toml``, runs the command with
pytest node IDs, and treats every Python file the ``test-files`` patterns match
as a test edit it may carry into a check. The patterns must therefore cover
every test file and match no production code.
"""
from __future__ import annotations

import re
import tomllib
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]


def harness() -> dict:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["tl3"]["harness"]


def full_match(name: str, pattern: str) -> bool:
    """``PurePosixPath.full_match`` (Python 3.13+), spelled out for 3.12 workers."""
    if hasattr(PurePosixPath, "full_match"):
        return PurePosixPath(name).full_match(pattern)
    assert not set("?[]") & set(pattern), f"the 3.12 matcher supports only * and **: {pattern}"
    segments = pattern.split("/")
    regex = ""
    for index, segment in enumerate(segments):
        last = index == len(segments) - 1
        if segment == "**":
            regex += ".*" if last else "(?:[^/]+/)*"
        else:
            regex += re.escape(segment).replace(r"\*", "[^/]*") + ("" if last else "/")
    return re.fullmatch(regex, name) is not None


def python_files(top: str) -> list[str]:
    return sorted(path.relative_to(ROOT).as_posix() for path in (ROOT / top).rglob("*.py"))


def test_command_is_the_pytest_module():
    assert harness()["command"] == ["python", "-m", "pytest"]


def test_patterns_have_the_shape_tl3_accepts():
    patterns = harness()["test-files"]
    assert patterns and len(set(patterns)) == len(patterns)
    for pattern in patterns:
        path = PurePosixPath(pattern)
        assert not path.is_absolute() and ".." not in path.parts and "\\" not in pattern
        assert path.suffix == ".py" and path.name != "conftest.py"


def test_every_test_file_is_a_test_edit():
    patterns = harness()["test-files"]
    tests = [name for name in python_files("tests") if PurePosixPath(name).name.startswith("test_")]
    assert len(tests) > 1000
    assert [name for name in tests if not any(full_match(name, p) for p in patterns)] == []


def test_no_production_file_is_a_test_edit():
    patterns = harness()["test-files"]
    production = python_files("src") + python_files("tools")
    assert "src/prismabuild/test_bound_contract.py" in production
    assert [name for name in production if any(full_match(name, p) for p in patterns)] == []

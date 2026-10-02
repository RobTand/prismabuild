"""Declarative interpreter selection; installed provenance stays a worker gate (#1427)."""

from __future__ import annotations

import ast
import json
import re
import string
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))
from prismabuild import core  # noqa: E402


class RuntimeSelectionError(ValueError):
    """A checkout cannot name its reviewed interpreter without executing code."""


def validate_policy(python: str, sources: dict) -> None:
    if not isinstance(python, str) or not python:
        raise RuntimeSelectionError("test_python must be a nonempty path")
    if not isinstance(sources, dict):
        raise RuntimeSelectionError("runtime_pins must be a mapping")
    for field, declaration in sources.items():
        if not isinstance(field, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", field):
            raise RuntimeSelectionError(f"invalid runtime pin field: {field!r}")
        if not isinstance(declaration, dict) or set(declaration) != {"source", "name"}:
            raise RuntimeSelectionError(f"runtime_pins.{field} needs source and name")
        source, name = declaration["source"], declaration["name"]
        if (not isinstance(source, str) or not source or Path(source).is_absolute()
                or ".." in Path(source).parts):
            raise RuntimeSelectionError(f"runtime_pins.{field}.source must stay relative to checkout")
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise RuntimeSelectionError(f"runtime_pins.{field}.name must be a literal assignment name")
    # With no policy, the legacy interpreter string remains byte-for-byte static.
    if not sources:
        return
    try:
        fields = set()
        for _, field, spec, conversion in string.Formatter().parse(python):
            if field is None:
                continue
            if field not in sources or conversion or (spec and not re.fullmatch(r"\.([1-9]|[1-3][0-9]|40)", spec)):
                raise RuntimeSelectionError(f"invalid test_python template field: {field!r}, {spec!r}")
            fields.add(field)
        if fields != set(sources):
            raise RuntimeSelectionError("test_python must use every runtime_pins field")
        if not Path(python).is_absolute():
            raise RuntimeSelectionError("templated test_python must be an absolute interpreter path")
    except ValueError as exc:
        raise RuntimeSelectionError(str(exc)) from exc


def select_runtime(checkout: Path, python: str, sources: dict) -> dict:
    """Read exactly one top-level literal full Git pin per declared source."""
    validate_policy(python, sources)
    root = checkout.resolve()
    pins, evidence = {}, {}
    for field, declaration in sorted(sources.items()):
        source = root / declaration["source"]
        name = declaration["name"]
        try:
            if not source.resolve().is_relative_to(root):
                raise RuntimeSelectionError(f"{source}: pin source escapes checkout")
            data = source.read_bytes()
            tree = ast.parse(data, filename=str(source))
            values = []
            for node in tree.body:
                if isinstance(node, (ast.Assign, ast.AnnAssign)):
                    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                    if any(isinstance(target, ast.Name) and target.id == name for target in targets):
                        if node.value is None:
                            raise RuntimeSelectionError(f"{source}: {name} has no literal value")
                        values.append(ast.literal_eval(node.value))
            if len(values) != 1:
                raise RuntimeSelectionError(f"{source}: expected exactly one literal {name} assignment")
            value = values[0]
            if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}", value):
                raise RuntimeSelectionError(f"{source}: {name} must be a full lowercase Git SHA")
        except (OSError, SyntaxError, ValueError, TypeError) as exc:
            raise RuntimeSelectionError(f"{source}: {exc}") from exc
        pins[field] = value
        evidence[field] = {**declaration, "sha256": core.raw_sha256(data)}
    return {"python": python.format_map(pins) if sources else python,
            "pins": pins, "pin_sources": evidence}


def baseline_identity(tree: str, runtime: dict, config: dict) -> str:
    """Only the same source tree, runtime policy and test invocation may reuse results."""
    identity = {"schema": "pbmergeq.baseline.v3", "tree": tree,
                "runtime": {key: runtime[key] for key in ("python", "pins", "pin_sources", "pbtest")},
                "config": config}
    return core.canonical_sha256(json.loads(json.dumps(identity, default=str)))


def runtime_refusal(output: str) -> str:
    """Recognize explicit pre-pytest pin/placement refusals, never missing summaries alone."""
    prefixes = ("pbtest: dependency pin refused before pytest:",
                "pbtest: every recorded worker that could take these shards names the interpreter absent:")
    return next((line.strip() for line in output.splitlines()
                 if line.strip().startswith(prefixes)), "")

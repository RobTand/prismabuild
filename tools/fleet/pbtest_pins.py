"""Worker-side reviewed dependency checks, embedded in pbtest's sealed argv.

This file uses only the target interpreter's standard library. It never
installs packages or executes a resolver on the submitting machine.
"""
from __future__ import annotations

import base64
import hashlib
import importlib.metadata as metadata
import importlib.util
import json
from pathlib import Path
import re
import runpy
import subprocess
import sys


def verify_install(module: str, expected: str) -> dict:
    """Require unambiguous Git provenance and intact installed package bytes."""
    owners = metadata.packages_distributions().get(module, [])
    if len(owners) != 1:
        raise ValueError(f"installed commit=<unknown>; expected one distribution "
                         f"owning {module}, found {owners}")
    dist = metadata.distribution(owners[0])
    direct = json.loads(dist.read_text("direct_url.json") or "{}")
    vcs = direct.get("vcs_info", {})
    observed = vcs.get("commit_id", "<unknown>")
    identity = f"distribution={owners[0]} installed commit={observed}"
    if (direct.get("dir_info", {}).get("editable") or
            vcs.get("vcs") != "git" or observed != expected):
        raise ValueError(f"{identity}; require a non-editable Git install at "
                         "the reviewed commit (local-directory installs do "
                         "not record a Git commit)")

    files = dist.files
    if not files:
        raise ValueError(f"{identity}; installed RECORD is missing")
    recorded = {}
    for entry in files:
        path = Path(dist.locate_file(entry)).resolve()
        if entry.hash is not None:
            if entry.hash.mode not in {"sha256", "sha384", "sha512"}:
                raise ValueError(f"{identity}; unsupported RECORD hash: {entry}")
            digest = hashlib.new(entry.hash.mode)
            with path.open("rb") as handle:
                for block in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(block)
            actual = base64.urlsafe_b64encode(digest.digest()).rstrip(b"=").decode()
            if actual != entry.hash.value:
                raise ValueError(f"{identity}; installed bytes differ from RECORD: {entry}")
            recorded[path] = actual

    # Distribution metadata alone does not say which module Python will load.
    # Refuse a checkout/PYTHONPATH shadow or an unrecorded package file.
    spec = importlib.util.find_spec(module)
    if spec is None or spec.origin is None or Path(spec.origin).resolve() not in recorded:
        raise ValueError(f"{identity}; imported {module} is not owned by its RECORD")
    for root in spec.submodule_search_locations or []:
        for path in Path(root).rglob("*"):
            if path.is_file() and path.suffix != ".pyc" and path.resolve() not in recorded:
                raise ValueError(f"{identity}; unrecorded package file: {path}")
    return {"module": module, "distribution": owners[0],
            "expected_commit": expected, "installed_commit": observed,
            "origin": spec.origin, "verified_files": len(recorded)}


def check_pins() -> None:
    root = Path.cwd().resolve()
    for resolver in sorted(Path("tools").glob("resolve_*_dev_pin.py")):
        module = resolver.name[len("resolve_"):-len("_dev_pin.py")]
        expected = "<unresolved>"
        try:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", module):
                raise ValueError("resolver name must identify one top-level Python module")
            if not resolver.resolve().is_relative_to(root):
                raise ValueError("resolver must stay inside the sealed checkout")
            resolved = subprocess.run([sys.executable, str(resolver)],
                                      capture_output=True, text=True, check=False)
            if resolved.returncode:
                raise ValueError(f"resolver exited {resolved.returncode}: "
                                 f"{resolved.stderr.strip()}")
            expected = resolved.stdout.strip()
            if not re.fullmatch(r"[0-9a-f]{40}", expected):
                raise ValueError("resolver must print one full lowercase Git commit")
            evidence = verify_install(module, expected)
        except (OSError, ValueError, TypeError, AttributeError, ImportError) as exc:
            raise ValueError(f"{resolver}: expected commit={expected}; {exc}") from exc
        print("pbtest dependency pin: " + json.dumps(evidence, sort_keys=True), flush=True)


def preflight() -> int:
    """Check every reviewed pin; ``1`` after saying why, ``0`` when all hold."""
    try:
        check_pins()
    except ValueError as exc:
        print(f"pbtest: dependency pin refused before pytest: {exc}",
              file=sys.stderr, flush=True)
        return 1
    return 0


def main() -> int:
    refused = preflight()
    if refused:
        return refused
    sys.argv[0] = "pytest"
    runpy.run_module("pytest", run_name="__main__", alter_sys=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

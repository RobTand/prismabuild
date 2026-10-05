"""Worker-side reviewed dependency checks, embedded in pbtest's sealed argv.

This file uses only the target interpreter's standard library. It never
installs packages or executes a resolver on the submitting machine.
"""
from __future__ import annotations

import base64
from prismabuild.digest_primitives import stream_digest
import importlib.metadata as metadata
import importlib.util
import json
from pathlib import Path
import re
import runpy
import subprocess
import sys


def verify_record_bytes(module: str) -> dict:
    """Require the imported module's installed bytes to match their RECORD.

    This is the integrity phase of :func:`verify_install`, exposed on its
    own: one distribution owning ``module``, a present RECORD, every hashed
    RECORD entry matching the bytes on disk, the module Python actually
    imports owned by that RECORD, and no unrecorded file inside the package.
    It never compares against the expected pin (it reads ``direct_url.json``
    only to label its messages and report ``installed_commit``). Known limit:
    ``importlib.metadata`` hides RECORD entries whose files are missing, so a
    deleted package file other than the imported module is not detected here.
    In particular the ownership refusal ("imported
    module is not owned by its RECORD") belongs to this phase, so an
    editable or shadowed import is refused here even when a caller has
    decided to tolerate recorded identity drift.
    """
    owners = metadata.packages_distributions().get(module, [])
    if len(owners) != 1:
        raise ValueError(f"installed commit=<unknown>; expected one distribution "
                         f"owning {module}, found {owners}")
    dist = metadata.distribution(owners[0])
    direct = json.loads(dist.read_text("direct_url.json") or "{}")
    vcs = direct.get("vcs_info", {})
    observed = vcs.get("commit_id", "<unknown>")
    identity = f"distribution={owners[0]} installed commit={observed}"

    files = dist.files
    if not files:
        raise ValueError(f"{identity}; installed RECORD is missing")
    recorded = {}
    for entry in files:
        path = Path(dist.locate_file(entry)).resolve()
        if entry.hash is not None:
            if entry.hash.mode not in {"sha256", "sha384", "sha512"}:
                raise ValueError(f"{identity}; unsupported RECORD hash: {entry}")
            digest = stream_digest(path, algorithm=entry.hash.mode)
            actual = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
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
            "installed_commit": observed,
            "origin": spec.origin, "verified_files": len(recorded)}


def verify_install(module: str, expected: str, *,
                   identity_policy=None) -> dict:
    """Require unambiguous Git provenance and intact installed package bytes.

    ``identity_policy`` decides nothing about bytes. The default ``None``
    keeps the historical behavior: any identity failure (editable install,
    non-Git origin, or an installed commit other than ``expected``) raises
    the same ValueError as before and the integrity phase never runs. A
    callable receives the identity-failure message and the observed facts;
    returning normally (any return value is ignored) records the drift as
    tolerated -- stamped in the returned evidence as
    ``identity_drift_tolerated`` -- and the full integrity phase still runs
    and can still refuse. A policy that raises propagates unchanged.
    """
    owners = metadata.packages_distributions().get(module, [])
    if len(owners) != 1:
        raise ValueError(f"installed commit=<unknown>; expected one distribution "
                         f"owning {module}, found {owners}")
    dist = metadata.distribution(owners[0])
    direct = json.loads(dist.read_text("direct_url.json") or "{}")
    vcs = direct.get("vcs_info", {})
    observed = vcs.get("commit_id", "<unknown>")
    identity = f"distribution={owners[0]} installed commit={observed}"
    tolerated = None
    if (direct.get("dir_info", {}).get("editable") or
            vcs.get("vcs") != "git" or observed != expected):
        message = (f"{identity}; require a non-editable Git install at "
                   "the reviewed commit (local-directory installs do "
                   "not record a Git commit)")
        if identity_policy is None:
            raise ValueError(message)
        identity_policy(message, {
            "module": module, "expected_commit": expected,
            "distribution": owners[0], "installed_commit": observed,
            "editable": bool(direct.get("dir_info", {}).get("editable")),
            "vcs": vcs.get("vcs"),
        })
        tolerated = message
    evidence = verify_record_bytes(module)
    evidence["expected_commit"] = expected
    if tolerated is not None:
        evidence["identity_drift_tolerated"] = tolerated
    return evidence


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
    """Check every reviewed pin; ``1`` after saying why, ``0`` when all hold.

    Distinct from the diagnostic harness's data-returning ``preflight``
    (same_name_distinct, #1386); this one is the pytest wrapper's gate.
    """
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

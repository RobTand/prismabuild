#!/usr/bin/python3
"""Fleet-executed end-to-end self-test driver (RobTand/prismabuild#688).

``pbcanary`` submits canary legs through the REAL interfaces (the existing
``pbrun`` submission path), waits within fixed budgets, verifies CAS
receipts against fixed expectations, and renders a mechanical verdict.
It submits, waits, and verifies. Nothing decides anything at runtime:
legs run in fixed order, waits are bounded, there are no retries, and any
refusal or mismatch is recorded with the leg and the refusing check named.

Exit semantics (per the issue):

* 0 -- every requested leg executed AND every receipt verified.
* 1 -- any leg failed its contract (stderr names the leg + check).
* 2 -- precondition refused: queue unreachable, generation root absent,
  runner misconfigured, unpinned GPU image, missing verdict module.
  "Did not test" is never "passed".

Namespaces: everything lives under
``/mnt/shared/prismabuild-fleet/pb-canary/<run-id>/``, sealed per run: a
fresh namespace is created for every run and a duplicate ``--run-id`` is
refused rather than reused, so no run ever shares state with another. CAS
receipts stay in the CAS (records pb_gc never removes); the namespace
carries a registration record (``run.json``) naming each leg's action key
so the audit trail resolves back to them.

Priority defaults to -10 (agent self-validation band): the canary never
contends with real work.

Crew split: this driver + legs 1-2 are crew A. Legs 3-4 arrive from crew B
under the ``pbcanary_legs`` contract; the final verdict module is crew C's
``tools/fleet/pbcanary_verdict.py`` (imported lazily at verdict time, with
a clear error when absent). ``--legs`` defaults to ``leg-1,leg-2`` until
crew B lands; the integrator flips the default to all four.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import importlib
import json
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
FLEET_DIR = HERE.parent

#: Published fleet client this driver submits through. Overridable with
#: --published-root; reading the live repo's tools keeps a stale checkout
#: from becoming a stale submission client.
DEFAULT_PUBLISHED_ROOT = Path("/mnt/shared/prismabuild-fleet/repo")

#: Shared fleet state. Overridable with --fleet-root for tests.
DEFAULT_FLEET_ROOT = Path("/mnt/shared/prismabuild-fleet")

CANARY_DIRNAME = "pb-canary"

#: Fixed leg order. Crew B appends leg-3/leg-4 modules here by adding rows;
#: nothing else in this driver keys on leg count.
LEG_MODULES = (
    ("leg-1", "pbcanary_legs.leg1"),
    ("leg-2", "pbcanary_legs.leg2"),
    ("leg-3", "pbcanary_legs.leg3"),
    ("leg-4", "pbcanary_legs.leg4"),
)

DEFAULT_LEGS = "leg-1,leg-2"

DEFAULT_PRIORITY = -10


class PreconditionRefused(Exception):
    """A precondition failed before a leg could become a test (exit 2)."""


def _fleet_paths(published_root: Path, fleet_root: Path) -> dict:
    return {
        "pbrun": published_root / "tools" / "pbrun.py",
        "pbwait": published_root / "tools" / "pbwait.py",
        "published_src": published_root / "src",
        "queue_root": fleet_root / "pb-queue",
        "cas_root": fleet_root / "cas",
        "canary_root": fleet_root / CANARY_DIRNAME,
    }


def check_preconditions(paths: dict, checkout: Path) -> None:
    """Refuse the run before anything is submitted (exit 2 on failure)."""
    for name in ("pbrun", "pbwait"):
        if not paths[name].is_file():
            raise PreconditionRefused(
                f"precondition refused (run): published {name} absent at "
                f"{paths[name]}: runner misconfigured: did not test"
            )
    if not paths["queue_root"].is_dir():
        raise PreconditionRefused(
            f"precondition refused (run): queue unreachable: "
            f"{paths['queue_root']} absent: did not test"
        )
    if not paths["cas_root"].is_dir():
        raise PreconditionRefused(
            f"precondition refused (run): CAS root absent at "
            f"{paths['cas_root']}: generation root absent: did not test"
        )
    if not (checkout / ".git").exists() and not (checkout / ".git").is_file():
        raise PreconditionRefused(
            f"precondition refused (run): checkout {checkout} is not a "
            "Git tree: runner misconfigured: did not test"
        )


def default_checkout() -> Path:
    """The Git toplevel holding this driver; ``pbrun --cwd`` snapshots it."""
    completed = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, check=False,
    )
    if completed.returncode != 0:
        raise PreconditionRefused(
            "precondition refused (run): no Git checkout found for the "
            "submission snapshot: runner misconfigured: did not test"
        )
    return Path(completed.stdout.strip())


def fresh_namespace(canary_root: Path, run_id: str) -> Path:
    """Create the sealed run namespace, refusing reuse (exit 2 on clash)."""
    namespace = canary_root / run_id
    if namespace.exists():
        raise PreconditionRefused(
            f"precondition refused (run): namespace {namespace} exists: "
            "re-runs create new namespaces, no state reuse: did not test"
        )
    namespace.mkdir(parents=True)
    return namespace


def run_process(argv: list[str], *, timeout_s: float) -> subprocess.CompletedProcess:
    return subprocess.run(
        argv, capture_output=True, text=True, check=False, timeout=timeout_s,
    )


def first_json_with_stdout_key(text: str, key: str) -> dict | None:
    """Scan output lines for the first JSON object carrying ``key``."""
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict) and key in value:
            return value
    return None


def last_json_object(text: str) -> dict | None:
    """Scan output lines backwards for the last parseable JSON object."""
    for line in reversed(text.splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict):
            return value
    return None


def submit_leg(
    paths: dict, spec: dict, checkout: Path, run_id: str, priority: int,
) -> tuple[str, dict]:
    """Submit one leg via ``pbrun --detach``; return (action_key, detach_json).

    Any refusal here means nothing executed: callers record it as a
    did-not-test leg entry (exit 2 class).
    """
    argv = [sys.executable, str(paths["pbrun"]), "--cwd", str(checkout)]
    for name, value in spec["demand"].items():
        argv += ["--demand", f"{name}={value}"]
    argv += [
        "--priority", str(priority),
        "--detach",
        "--env", f"PBCANARY_RUN_ID={run_id}",
        "--env", f"PBCANARY_LEG={spec['name']}",
        "--",
        *spec["argv"],
    ]
    try:
        completed = run_process(argv, timeout_s=120.0)
    except subprocess.TimeoutExpired as exc:
        raise PreconditionRefused(
            f"precondition refused ({spec['name']}): pbrun submission "
            f"timed out: queue unreachable: did not test ({exc})"
        ) from exc
    detach = first_json_with_stdout_key(completed.stdout, "action_key")
    if completed.returncode != 0 or detach is None:
        tail = (completed.stderr.strip().splitlines() or ["no stderr"]) [-1]
        raise PreconditionRefused(
            f"precondition refused ({spec['name']}): pbrun submission "
            f"refused (exit {completed.returncode}): {tail}: did not test"
        )
    return str(detach["action_key"]), detach


def wait_leg(paths: dict, leg: str, action_key: str, wait_s: int) -> dict:
    """Wait for one submitted leg within its fixed budget (no retries)."""
    completed = run_process(
        [sys.executable, str(paths["pbwait"]),
         "--wait-s", str(wait_s), action_key],
        timeout_s=float(wait_s) + 120.0,
    )
    record = last_json_object(completed.stdout) or {}
    return {"returncode": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
            "record": record}


def load_verified_receipt(
    paths: dict, action_key: str,
) -> tuple[dict, Path, bytes]:
    """Return (validated CAS receipt, receipt path, result-blob bytes).

    Reads the published request and validates through the published
    ``prismabuild.core`` -- the existing verification path, not a new one.
    Raises :class:`PreconditionRefused` when the toolchain itself is broken,
    and returns ``(None, ...)`` semantics via ``LookupError`` when the
    receipt is simply absent (a contract failure after an executed ending).
    """
    sys.path.insert(0, str(paths["published_src"]))
    try:
        from prismabuild import core as pb
    except ImportError as exc:
        raise PreconditionRefused(
            "precondition refused (run): published prismabuild.core "
            f"unimportable from {paths['published_src']}: runner "
            f"misconfigured: did not test ({exc})"
        ) from exc
    cas_root = Path(paths["cas_root"])
    try:
        action = json.loads(
            (cas_root / "requests" / action_key[:2]
             / f"{action_key}.json").read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise LookupError(f"published request unreadable: {exc}") from exc
    cas = pb.PrismaBuildCAS(cas_root)
    receipt = cas.lookup(action)
    if receipt is None:
        raise LookupError("CAS lookup empty")
    blob_path = Path(cas.result_path(receipt, action))
    blob = blob_path.read_bytes()
    matches = sorted((cas_root / "actions").glob(f"*/{action_key[:2]}/{action_key}.json"))
    receipt_path = matches[0] if matches else blob_path
    return receipt, receipt_path, blob


def receipt_ref_for(fleet_root: Path, receipt_path: Path) -> str:
    try:
        return str(receipt_path.relative_to(fleet_root))
    except ValueError:
        return str(receipt_path)


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8")


def run_canary(args: argparse.Namespace) -> int:
    published_root = Path(args.published_root or DEFAULT_PUBLISHED_ROOT)
    fleet_root = Path(args.fleet_root or DEFAULT_FLEET_ROOT)
    paths = _fleet_paths(published_root, fleet_root)
    requested = [leg.strip() for leg in args.legs.split(",") if leg.strip()]
    known = [name for name, _ in LEG_MODULES]
    for leg in requested:
        if leg not in known:
            print(f"pbcanary: unknown leg {leg!r} (known: {', '.join(known)})",
                  file=sys.stderr)
            return 2

    try:
        checkout = Path(args.checkout) if args.checkout else default_checkout()
        check_preconditions(paths, checkout)
        if args.gpu_image:
            os.environ["PBCANARY_GPU_IMAGE"] = args.gpu_image
        run_id = args.run_id or (
            datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        namespace = fresh_namespace(paths["canary_root"], run_id)
    except PreconditionRefused as exc:
        print(f"pbcanary: {exc}", file=sys.stderr)
        return 2

    sys.path.insert(0, str(FLEET_DIR))
    run_record = {
        "run_id": run_id,
        "legs_requested": requested,
        "priority": args.priority,
        "checkout": str(checkout),
        "published_root": str(published_root),
        "started_unix": datetime.datetime.now(
            datetime.timezone.utc).isoformat(),
    }
    write_json(namespace / "run.json", run_record)

    results: list[dict] = []
    for leg in requested:
        leg_dir = namespace / leg
        leg_dir.mkdir(exist_ok=True)
        entry: dict = {"leg": leg, "ok": False, "reason": "",
                       "receipt_ref": None}
        module_name = dict(LEG_MODULES)[leg]
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            entry["reason"] = (
                f"precondition refused ({leg}): module {module_name} "
                f"absent: did not test ({exc})"
            )
            write_json(leg_dir / "entry.json", entry)
            results.append({name: entry[name] for name in
                            ("leg", "ok", "reason", "receipt_ref")})
            print(f"pbcanary: {leg}: {entry['reason']}", file=sys.stderr)
            continue
        try:
            spec = module.build()
        except Exception as exc:  # LegBuildRefused or a bug: nothing executed.
            detail = str(exc)
            if "did not test" not in detail:
                detail += ": did not test"
            entry["reason"] = (
                f"precondition refused ({leg}): build refused: {detail}"
            )
            write_json(leg_dir / "entry.json", entry)
            results.append({name: entry[name] for name in
                            ("leg", "ok", "reason", "receipt_ref")})
            print(f"pbcanary: {leg}: {entry['reason']}", file=sys.stderr)
            continue
        write_json(leg_dir / "spec.json", spec)

        try:
            action_key, detach = submit_leg(
                paths, spec, checkout, run_id, args.priority)
        except PreconditionRefused as exc:
            entry["reason"] = str(exc)
            write_json(leg_dir / "entry.json", entry)
            results.append({name: entry[name] for name in
                            ("leg", "ok", "reason", "receipt_ref")})
            print(f"pbcanary: {leg}: {entry['reason']}", file=sys.stderr)
            continue
        write_json(leg_dir / "detach.json", detach)
        entry["action_key"] = action_key
        short = action_key[:12]

        waited = wait_leg(paths, leg, action_key, int(spec["wait_s"]))
        write_json(leg_dir / "pbwait.json",
                   {"returncode": waited["returncode"],
                    "record": waited["record"],
                    "stderr_tail": waited["stderr"].strip().splitlines()[-3:]})
        if waited["returncode"] != 0:
            entry["reason"] = (
                f"{leg} wait failed: pbwait exit {waited['returncode']} "
                f"after {spec['wait_s']}s for {short}: "
                f"{(waited['stderr'].strip().splitlines() or ['no stderr'])[-1]}"
            )
            write_json(leg_dir / "entry.json", entry)
            results.append({name: entry[name] for name in
                            ("leg", "ok", "reason", "receipt_ref")})
            print(f"pbcanary: {leg}: {entry['reason']}", file=sys.stderr)
            continue

        try:
            receipt, receipt_path, blob = load_verified_receipt(paths, action_key)
        except PreconditionRefused as exc:
            entry["reason"] = str(exc)
            write_json(leg_dir / "entry.json", entry)
            results.append({name: entry[name] for name in
                            ("leg", "ok", "reason", "receipt_ref")})
            print(f"pbcanary: {leg}: {entry['reason']}", file=sys.stderr)
            continue
        except Exception as exc:
            # Includes CASTamperError (a receipt that fails validation) and
            # absent requests/blobs: the ending claimed execution, so this
            # is a failed contract, not a did-not-test.
            entry["reason"] = (
                f"{leg} receipt-verified failed: executed ending but "
                f"CAS lookup failed for {short} ({exc})"
            )
            write_json(leg_dir / "entry.json", entry)
            results.append({name: entry[name] for name in
                            ("leg", "ok", "reason", "receipt_ref")})
            print(f"pbcanary: {leg}: {entry['reason']}", file=sys.stderr)
            continue
        try:
            artifact = blob.decode("utf-8")
        except UnicodeDecodeError as exc:
            entry["reason"] = (
                f"{leg} artifact-digest failed: result blob is not "
                f"text ({exc})"
            )
            write_json(leg_dir / "entry.json", entry)
            results.append({name: entry[name] for name in
                            ("leg", "ok", "reason", "receipt_ref")})
            print(f"pbcanary: {leg}: {entry['reason']}", file=sys.stderr)
            continue
        (leg_dir / "artifact.txt").write_text(artifact, encoding="utf-8")
        write_json(leg_dir / "receipt.json", receipt)

        ok, reason = module.verify(
            {"action_key": action_key, "receipt": receipt,
             "artifact": artifact},
            spec["expected"],
        )
        artifact_digest = hashlib.sha256(blob).hexdigest()
        entry.update(ok=ok, reason=reason,
                     receipt_ref=receipt_ref_for(fleet_root, receipt_path),
                     artifact_digest=artifact_digest)
        write_json(leg_dir / "verify.json",
                   {"ok": ok, "reason": reason,
                    "artifact_digest": artifact_digest})
        write_json(leg_dir / "entry.json", entry)
        print(f"pbcanary: {leg}: {'verified' if ok else 'FAILED'}: {reason}")
        results.append({name: entry[name] for name in
                        ("leg", "ok", "reason", "receipt_ref",
                         "artifact_digest") if name in entry})

    try:
        from pbcanary_verdict import verdict
    except ImportError as exc:
        write_json(namespace / "canary-result.json",
                   {"run_id": run_id, "results": results, "verdict": None,
                    "sealed": True,
                    "error": f"verdict module absent: {exc}"})
        print("pbcanary: precondition refused (run): "
              "tools/fleet/pbcanary_verdict.py absent (crew C pending): "
              f"did not test ({exc})", file=sys.stderr)
        return 2

    verdict_entries = [
        {name: row[name] for name in ("leg", "ok", "reason", "receipt_ref")
         if name in row}
        for row in results
    ]
    exit_code, summary = verdict(verdict_entries)
    summary["sealed"] = True
    write_json(namespace / "canary-result.json",
               {"run_id": run_id, "results": results, "verdict": summary})
    for row in results:
        status = "ok" if row["ok"] else "FAIL"
        print(f"pbcanary: {row['leg']}: {status}: {row['reason']}")
    message = str(summary.get("stderr_message") or "")
    if exit_code == 0:
        print(f"pbcanary: verified ({namespace})"
              + (f": {message}" if message else ""))
    else:
        print(message or f"pbcanary: failed ({namespace})", file=sys.stderr)
    return exit_code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fleet-executed end-to-end self-test (issue #688).")
    parser.add_argument("--legs", default=DEFAULT_LEGS,
                        help="comma-separated legs in fixed order "
                             f"(default: {DEFAULT_LEGS}; full canary once "
                             "crew B lands: leg-1,leg-2,leg-3,leg-4)")
    parser.add_argument("--run-id", default=None,
                        help="run namespace; default UTC timestamp (must be fresh)")
    parser.add_argument("--priority", type=int, default=DEFAULT_PRIORITY,
                        help="queue hint (default: %(default)s, never contends)")
    parser.add_argument("--checkout", default=None,
                        help="tree pbrun snapshots (default: this driver's Git toplevel)")
    parser.add_argument("--fleet-root", default=None,
                        help="override the fleet root (tests)")
    parser.add_argument("--published-root", default=None,
                        help="published fleet client root (tests)")
    parser.add_argument("--gpu-image", default=None,
                        help="digest-pinned campaign image for leg 2 "
                             "(else PBCANARY_GPU_IMAGE)")
    args = parser.parse_args(argv)
    return run_canary(args)


if __name__ == "__main__":
    sys.exit(main())

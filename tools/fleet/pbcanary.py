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
so the audit trail resolves back to them. The namespace itself is owned by
``pb_gc --canary-root`` (issue #690): run.json stamps the retention rule,
and the reaper removes a namespace only when its record is valid, its
members are recognizable, and it is sealed or older than the age backstop.

Priority defaults to -10 (agent self-validation band): the canary never
contends with real work.

Crew split: this driver + legs 1-2 are crew A. Legs 3-4 arrive from crew B
under the ``pbcanary_legs`` contract; the final verdict module is crew C's
``tools/fleet/pbcanary_verdict.py`` (imported lazily at verdict time, with
a clear error when absent). ``--legs`` defaults to all four legs.

Leg-shape dispatch (the integrator's reconciliation of crews A and B):

* Legs 1-2: one submission from ``spec["argv"]`` + ``spec["demand"]``,
  two-arg ``verify(receipt, expected)``.
* Leg 3: ``build()`` carries a ``manifest`` skeleton plus ``action`` /
  ``pbrun_flags`` / ``progress_phases`` instead of top-level ``argv`` /
  ``demand``. The driver stages the chunk files, renders the manifest,
  submits with ``--data-manifest`` + ``--residency stage``, then verifies
  with the same two-arg call (the artifact text is also offered as the
  receipt's ``stdout`` so the leg's envelope scan finds it). The driver
  additionally binds the accepted-progress observation from the terminal
  attempt that produced the receipt (issue #784) so ``verify`` can refuse a
  leg whose progress never reached the worker's watchdog.
* Leg 4: ``build()`` carries an ``actions`` pair (sparky, sparklina) and a
  three-arg ``verify(receipt_a, receipt_b, expected)``. The driver submits
  both, waits for both, verifies the pair, and files ONE ``leg-4`` entry
  carrying ``digest_a`` / ``digest_b`` (the per-side envelope digests) so
  the verdict checks envelope equality here, not on trust.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import importlib
import json
import os
import math
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

DEFAULT_LEGS = "leg-1,leg-2,leg-3,leg-4"

DEFAULT_PRIORITY = -10

#: Schema of the registration record stamped into every run namespace.
#: ``pb_gc`` pins the same literal (issue #690) and refuses a namespace
#: whose record does not carry it, so the record is the namespace's
#: identity card as well as its audit trail.
RUN_RECORD_SCHEMA = "prismabuild.pbcanary.run.v1"

#: The namespace retention rule, stamped into run.json so every
#: namespace documents its own GC owner (issue #690): pb_gc surveys the
#: canary root when given ``--canary-root`` and removes a namespace only
#: during an acknowledged quiescent-store sweep, and only when its run
#: record is valid, every member is recognizable, and it is sealed
#: (``canary-result.json``) or older than the age backstop. The CAS
#: receipts a namespace refers to are records and are never removed.
RUN_GC_OWNER = "tools/fleet/pb_gc.py --canary-root"
RUN_GC_RULE = (
    "pb_gc removes this namespace only during an acknowledged "
    "quiescent-store sweep, and only when its run record is valid, every "
    "member is recognizable, and it is sealed (canary-result.json) or "
    "older than the age backstop; its CAS receipts are records and stay"
)


def build_run_record(
    *, run_id: str, generation: str | None, requested: list,
    priority: int, checkout: Path, published_root: Path,
) -> dict:
    """The registration record written to ``<namespace>/run.json``.

    Pure data; ``run_canary`` stamps it once, immediately after the fresh
    namespace is created, and nothing rewrites it afterwards.
    """
    return {
        "run_id": run_id,
        "schema": RUN_RECORD_SCHEMA,
        "generation": generation,
        "legs_requested": requested,
        "priority": priority,
        "checkout": str(checkout),
        "published_root": str(published_root),
        "gc": {"owner": RUN_GC_OWNER, "rule": RUN_GC_RULE},
        "started_unix": datetime.datetime.now(
            datetime.timezone.utc).isoformat(),
    }


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
    generation: str | None,
    argv: list[str] | None = None,
    *,
    extra_flags: list[str] | None = None,
    extra_env: dict[str, str] | None = None,
    manifest: Path | None = None,
    submit_label: str | None = None,
) -> tuple[str, dict]:
    """Submit one action via ``pbrun --detach``; return (action_key, detach_json).

    ``argv`` defaults to ``spec["argv"]`` (legs 1-2) or the spec's
    ``action.argv`` (leg 3); demand defaults to ``spec.get("demand", {})``
    and is forwarded as one comma-separated ``--demand`` aggregate -- the
    form pbrun's single-value flag parses -- with no flag at all when the
    demand is empty, so pbrun's own defaults stand.
    ``extra_flags`` (leg 3's ``pbrun_flags`` + progress phases, leg 4's
    per-side flags) extend the command line after the driver's own
    ``--priority``. Issue #690: the driver's ``--priority`` governs every
    leg, so a spec-side ``--priority`` in ``extra_flags`` is refused
    loudly instead of silently overriding it (argparse would let the
    later, spec-side value win). ``extra_env`` extends the action
    environment; ``manifest`` selects ``--data-manifest``.

    A spec that names ``container_image`` (leg 2's digest-pinned campaign
    image) has it forwarded as pbrun's ``--container-image``, sealed into
    the action so only a box whose local Docker positively holds it can
    claim the leg (#714). An absent or blank field adds no flag, so
    ordinary legs submit exactly as before.

    Any refusal here means nothing executed: callers record it as a
    did-not-test leg entry (exit 2 class).
    """
    label = submit_label or spec["name"]
    if argv is None:
        argv = spec.get("argv") or spec.get("action", {}).get("argv")
    if not argv:
        raise PreconditionRefused(
            f"precondition refused ({label}): spec carries no argv: "
            "did not test"
        )
    extra = list(extra_flags or [])
    for flag in extra:
        if flag == "--priority" or flag.startswith("--priority="):
            raise PreconditionRefused(
                f"precondition refused ({label}): spec-side flags carry "
                f"{flag!r}: the driver's --priority governs every leg, so "
                "a leg spec must not pin its own: did not test"
            )
    command = list(argv)
    submit_argv = [sys.executable, str(paths["pbrun"]), "--cwd", str(checkout)]
    demand = spec.get("demand", {})
    if demand:
        submit_argv += ["--demand", ",".join(
            f"{name}={value}" for name, value in sorted(demand.items()))]
    submit_argv += ["--priority", str(priority)]
    # The image a leg runs inside is operator configuration on the spec
    # (leg 2's ``container_image``: the digest-pinned campaign image it both
    # echoes and runs).  The driver declares it to pbrun -- never inferred
    # from argv and never a host pin -- so a box whose local Docker does not
    # positively hold it denies the claim instead of spending the attempt and
    # failing inside the container (#714).  Absent or blank is no flag at
    # all, leaving every ordinary leg's command line unchanged.
    container_image = str(spec.get("container_image") or "").strip()
    if container_image:
        submit_argv += ["--container-image", container_image]
    submit_argv += extra
    if manifest is not None:
        submit_argv += ["--data-manifest", str(manifest)]
    submit_argv += [
        "--detach",
        "--env", f"PBCANARY_RUN_ID={run_id}",
        "--env", f"PBCANARY_LEG={spec['name']}",
        *([ "--env", f"PBCANARY_GENERATION={generation}"]
          if generation else []),
    ]
    for name, value in (extra_env or {}).items():
        submit_argv += ["--env", f"{name}={value}"]
    submit_argv += ["--", *command]
    try:
        completed = run_process(submit_argv, timeout_s=120.0)
    except subprocess.TimeoutExpired as exc:
        raise PreconditionRefused(
            f"precondition refused ({label}): pbrun submission "
            f"timed out: queue unreachable: did not test ({exc})"
        ) from exc
    detach = first_json_with_stdout_key(completed.stdout, "action_key")
    if completed.returncode != 0 or detach is None:
        tail = (completed.stderr.strip().splitlines() or ["no stderr"]) [-1]
        raise PreconditionRefused(
            f"precondition refused ({label}): pbrun submission "
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


def terminal_progress_observation(
    paths: dict, action_key: str, artifact: str, receipt: dict,
    *, generation: float | None = None,
) -> dict | None:
    """Accepted progress from the terminal attempt that produced ``artifact``.

    Leg 3 declares progress phases, so qualification requires the worker's
    authenticated ``ProgressWatch`` observation, not the action's own
    self-report.  The binding is exact, and fails closed on anything less:

    * the CAS receipt's own result digest and length must name exactly the
      artifact bytes (the producer evidence);
    * the terminal must be the executed ending of the generation this driver
      submitted (``generation``, pbrun's ``published_unix``), read from that
      generation's immutable attempt archive.  ``PoolQueue.finish`` publishes
      the attempt, entombs the claim, releases capacity and only then files
      ``done/``, and a wait can answer inside that window: the 6d88c0b15b18
      canary's leg 3 read ``done/`` 1.23 s before it existed.  ``done/`` is
      read only when the archive has no ending yet, and only for the same
      generation; with no generation named it is the only source;
    * its immutable attempt history, read and verified by the queue's own
      ``attempt_outcomes`` (canonical links, content-addressed logs), must
      end at an executed ``done`` attempt -- the terminal's adopted attempt;
    * that adopted attempt's recorded stdout must begin with exactly the
      artifact bytes, and carry the observation.

    An older attempt's stdout is never searched: a superseding terminal does
    not lend its or an ancestor's observation to a receipt it did not
    produce.  ``None`` means the proof is absent and the leg must refuse.
    """
    result = receipt.get("result") if isinstance(receipt, dict) else None
    result_sha = result.get("sha256") if isinstance(result, dict) else None
    result_bytes = result.get("bytes") if isinstance(result, dict) else None
    artifact_bytes = artifact.encode("utf-8")
    if (not isinstance(result_sha, str) or len(result_sha) != 64
            or type(result_bytes) is not int
            or result_bytes != len(artifact_bytes)
            or hashlib.sha256(artifact_bytes).hexdigest() != result_sha):
        return None
    if generation is not None and (
            type(generation) not in (int, float)
            or not math.isfinite(float(generation))):
        return None
    source = str(paths.get("published_src") or "")
    if source and source not in sys.path:
        sys.path.insert(0, source)
    try:
        from prismabuild import pool as pool_mod
    except ImportError:
        return None
    try:
        queue = pool_mod.PoolQueue(Path(paths["queue_root"]))
    except (OSError, ValueError):
        return None
    record = None
    if generation is not None:
        try:
            archived = queue.archived_generation_outcomes(
                action_key, generation=float(generation))
        except (OSError, ValueError):
            return None
        if archived:
            record = archived[0][1]
    if record is None:
        record_path = Path(paths["queue_root"]) / "done" / f"{action_key}.json"
        try:
            record = json.loads(record_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
    if not isinstance(record, dict) or record.get("action_key") != action_key:
        return None
    if generation is not None:
        published = record.get("published_unix")
        if (type(published) not in (int, float)
                or float(published) != float(generation)):
            return None
    if record.get("status") != "executed":
        return None
    try:
        outcomes = queue.attempt_outcomes(record)
    except (OSError, ValueError):
        return None
    if not outcomes:
        return None
    adopted = outcomes[-1]
    if (adopted.get("status") != "executed"
            or adopted.get("disposition") != "done"):
        return None
    stdout = adopted.get("stdout")
    if not isinstance(stdout, str):
        return None
    try:
        stdout_bytes = stdout.encode("utf-8")
    except UnicodeEncodeError:
        return None
    if not stdout_bytes.startswith(artifact_bytes):
        return None
    detail = adopted.get("detail")
    observation = (
        detail.get("progress_observation") if isinstance(detail, dict) else None)
    if not isinstance(observation, dict):
        return None
    return {"observation": observation, "attempt": adopted.get("attempt")}


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8")


class _SideFailed(Exception):
    """One submitted action executed but broke its contract (exit 1 class)."""


#: Keys a per-leg ``results`` row may carry into the verdict. The verdict
#: reads only leg/ok/reason/receipt_ref plus leg-4 digest fields; anything
#: else stays in the per-leg files.
_ENTRY_KEYS = ("leg", "ok", "reason", "receipt_ref",
               "artifact_digest", "digest_a", "digest_b")


def _record_entry(leg_dir: Path, entry: dict, results: list,
                  *, stderr: bool = False, echo: bool = True) -> None:
    """File one leg entry and append its verdict row (no retries)."""
    write_json(leg_dir / "entry.json", entry)
    results.append({name: entry[name] for name in _ENTRY_KEYS
                    if name in entry})
    if echo:
        print(f"pbcanary: {entry['leg']}: {entry['reason']}",
              file=sys.stderr if stderr else sys.stdout)


def _prepare_leg3(module, spec: dict, leg_dir: Path, paths: dict
                  ) -> tuple[list, dict, Path]:
    """Stage leg-3 chunks + rendered manifest; return (flags, env, manifest).

    Raises with a did-not-test detail when staging setup itself refuses.
    """
    try:
        chunk_paths = module.write_chunk_files(str(leg_dir))
        manifest = module.manifest_for_run(
            chunk_paths, str(paths["canary_root"]))
    except Exception as exc:
        raise PreconditionRefused(
            f"leg-3 staging setup refused ({exc}): did not test"
        ) from exc
    manifest_path = leg_dir / "leg3.manifest.json"
    write_json(manifest_path, manifest)
    flags = list(spec.get("pbrun_flags", []))
    for phase in spec.get("progress_phases", []):
        flags += ["--progress-phase", phase]
    env = dict(spec.get("action", {}).get("env", {}))
    env["PBCANARY_LEG3_MANIFEST"] = str(manifest_path)
    return flags, env, manifest_path


def _execute_side(
    paths: dict, *, leg: str, spec: dict, argv: list[str] | None,
    checkout: Path, run_id: str, generation: str | None, priority: int,
    fleet_root: Path, leg_dir: Path, side: str | None,
    extra_flags: list, extra_env: dict, manifest: Path | None,
    wait_s: int,
) -> tuple[dict, str, bytes, str]:
    """Submit one action, wait within budget, CAS-load its receipt.

    Returns ``(envelope_for_verify, receipt_ref, result_blob,
    action_key)``. Raises :class:`PreconditionRefused` when nothing
    became a test (exit 2) and :class:`_SideFailed` when the action
    executed but broke its contract (exit 1). The envelope offers the
    artifact text both as ``artifact`` (legs 1-2 shape) and as
    ``stdout`` (legs 3-4 envelope-scan shape).
    """
    label = leg if side is None else f"{leg} ({side})"
    file_tag = "" if side is None else f"_{side}"
    action_key, detach = submit_leg(
        paths, spec, checkout, run_id, priority, generation, argv,
        extra_flags=extra_flags, extra_env=extra_env, manifest=manifest,
        submit_label=label,
    )
    write_json(leg_dir / f"detach{file_tag}.json", detach)
    short = action_key[:12]

    waited = wait_leg(paths, leg, action_key, int(wait_s))
    write_json(leg_dir / f"pbwait{file_tag}.json",
               {"returncode": waited["returncode"],
                "record": waited["record"],
                "stderr_tail": waited["stderr"].strip().splitlines()[-3:]})
    if waited["returncode"] != 0:
        tail = (waited["stderr"].strip().splitlines() or ["no stderr"])[-1]
        raise _SideFailed(
            f"{label} wait failed: pbwait exit {waited['returncode']} "
            f"after {wait_s}s for {short}: {tail}"
        )

    try:
        receipt, receipt_path, blob = load_verified_receipt(paths, action_key)
    except PreconditionRefused:
        raise
    except Exception as exc:
        # The ending claimed execution, so an unreadable receipt is a
        # failed contract, not a did-not-test.
        raise _SideFailed(
            f"{label} receipt-verified failed: executed ending but "
            f"CAS lookup failed for {short} ({exc})"
        ) from exc
    try:
        artifact = blob.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _SideFailed(
            f"{label} artifact-digest failed: result blob is not "
            f"text ({exc})"
        ) from exc
    (leg_dir / f"artifact{file_tag}.txt").write_text(artifact, encoding="utf-8")
    write_json(leg_dir / f"receipt{file_tag}.json", receipt)
    published = detach.get("published_unix") if isinstance(detach, dict) else None
    evidence = terminal_progress_observation(
        paths, action_key, artifact, receipt,
        generation=(float(published)
                    if type(published) in (int, float) else None))
    envelope = {"action_key": action_key, "receipt": receipt,
                "artifact": artifact, "stdout": artifact,
                "progress_observation": None, "progress_attempt": None}
    if evidence is not None:
        write_json(leg_dir / f"progress{file_tag}.json", evidence)
        envelope["progress_observation"] = evidence["observation"]
        envelope["progress_attempt"] = evidence["attempt"]
    return envelope, receipt_ref_for(fleet_root, receipt_path), blob, action_key


def _run_single_leg(
    paths: dict, module, spec: dict, leg: str, checkout: Path, run_id: str,
    generation: str | None, priority: int, fleet_root: Path,
    leg_dir: Path, entry: dict, results: list,
) -> None:
    """Run a one-action leg (legs 1-3) and file its entry."""
    extra_flags: list = []
    extra_env: dict = {}
    manifest: Path | None = None
    if "manifest" in spec:
        try:
            extra_flags, extra_env, manifest = _prepare_leg3(
                module, spec, leg_dir, paths)
        except PreconditionRefused as exc:
            _record_entry(leg_dir, {**entry,
                "reason": f"precondition refused ({leg}): {exc}"},
                results, stderr=True)
            return
    try:
        envelope, receipt_ref, blob, action_key = _execute_side(
            paths, leg=leg, spec=spec, argv=None, checkout=checkout,
            run_id=run_id, generation=generation, priority=priority,
            fleet_root=fleet_root, leg_dir=leg_dir, side=None,
            extra_flags=extra_flags, extra_env=extra_env,
            manifest=manifest, wait_s=int(spec.get("wait_s", 300)),
        )
        entry["action_key"] = action_key
    except PreconditionRefused as exc:
        _record_entry(leg_dir, {**entry, "reason": str(exc)},
                      results, stderr=True)
        return
    except _SideFailed as exc:
        _record_entry(leg_dir, {**entry, "reason": str(exc)},
                      results, stderr=True)
        return

    ok, reason = module.verify(envelope, spec["expected"])
    artifact_digest = hashlib.sha256(blob).hexdigest()
    entry.update(ok=ok, reason=reason, receipt_ref=receipt_ref,
                 artifact_digest=artifact_digest)
    write_json(leg_dir / "verify.json",
               {"ok": ok, "reason": reason,
                "artifact_digest": artifact_digest})
    _record_entry(leg_dir, entry, results, echo=False)
    print(f"pbcanary: {leg}: {'verified' if ok else 'FAILED'}: {reason}")


def _run_fanout_leg(
    paths: dict, module, spec: dict, leg: str, checkout: Path, run_id: str,
    generation: str | None, priority: int, fleet_root: Path,
    leg_dir: Path, entry: dict, results: list,
) -> None:
    """Run a two-action fanout leg (leg 4) and file its single entry.

    Both sides submit, wait, and load; the pair verifies through the leg's
    three-arg ``verify(receipt_a, receipt_b, expected)``. The entry carries
    ``digest_a`` / ``digest_b`` so the verdict checks envelope equality
    itself instead of trusting the ``ok`` flag.
    """
    actions = spec.get("actions", [])
    if len(actions) != 2:
        _record_entry(leg_dir, {**entry,
            "reason": f"precondition refused ({leg}): spec carries "
                      f"{len(actions)} actions, need 2: did not test"},
            results, stderr=True)
        return
    sides = []
    for index, action in enumerate(actions):
        side = str(action.get("tag") or f"side-{index}")
        try:
            envelope, receipt_ref, blob, action_key = _execute_side(
                paths, leg=leg, spec=spec, argv=action.get("argv"),
                checkout=checkout, run_id=run_id, generation=generation,
                priority=priority, fleet_root=fleet_root, leg_dir=leg_dir,
                side=side, extra_flags=list(action.get("pbrun_flags", [])),
                extra_env=dict(action.get("env", {})), manifest=None,
                wait_s=int(spec.get("wait_s", 300)),
            )
            entry.setdefault("action_keys", {})[side] = action_key
        except PreconditionRefused as exc:
            _record_entry(leg_dir, {**entry, "reason": str(exc)},
                          results, stderr=True)
            return
        except _SideFailed as exc:
            _record_entry(leg_dir, {**entry, "reason": str(exc)},
                          results, stderr=True)
            return
        sides.append((side, envelope, receipt_ref, blob))
    (side_a, env_a, ref_a, blob_a), (_side_b, env_b, _ref_b, blob_b) = sides

    ok, reason = module.verify(env_a, env_b, spec["expected"])
    digest_a = hashlib.sha256(blob_a).hexdigest()
    digest_b = hashlib.sha256(blob_b).hexdigest()
    entry.update(ok=ok, reason=reason, receipt_ref=ref_a,
                 digest_a=digest_a, digest_b=digest_b)
    write_json(leg_dir / "verify.json",
               {"ok": ok, "reason": reason,
                "digest_a": digest_a, "digest_b": digest_b})
    _record_entry(leg_dir, entry, results, echo=False)
    print(f"pbcanary: {leg}: {'verified' if ok else 'FAILED'}: {reason} "
          f"({side_a}+{_side_b})")


def write_summary_dir(summary_dir: str | Path, run_id: str, results: list,
                      verdict_summary: dict | None,
                      error: str | None = None) -> Path:
    """Drop the machine-readable verdict beside the CI workspace (not the fleet).

    The run namespace keeps the canonical copy; this is the GitHub-workflow
    artifact contract (``pbcanary-summary/``). Best-effort, never a pass.
    """
    out = Path(summary_dir)
    out.mkdir(parents=True, exist_ok=True)
    payload: dict = {"run_id": run_id, "results": results,
                     "verdict": verdict_summary}
    if error is not None:
        payload["error"] = error
    write_json(out / "canary-result.json", payload)
    lines = [f"pbcanary {run_id}"]
    for row in results:
        status = "ok" if row.get("ok") else "FAIL"
        lines.append(f"{row.get('leg')}: {status}: {row.get('reason')}")
    (out / "summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out


def run_canary(args: argparse.Namespace | None = None,
               *, generation: str | None = None) -> int:
    """Submit the requested legs through the real queue and verify them.

    ``args`` is the CLI namespace from :func:`main`; programmatic callers
    (the rollout gate) call ``run_canary(generation=<name>)`` and every
    other setting takes its CLI default. The ``generation`` keyword wins
    over ``args.generation`` when both are given. Returns the issue's
    verdict code: 0 every leg verified, 1 a leg failed, 2 did-not-test.
    """
    if args is None:
        args = argparse.Namespace(
            legs=DEFAULT_LEGS, run_id=None, priority=DEFAULT_PRIORITY,
            checkout=None, fleet_root=None, published_root=None,
            gpu_image=None, generation=None, summary_dir=None,
        )
    if generation is None:
        generation = getattr(args, "generation", None) or None
    summary_dir = getattr(args, "summary_dir", None)
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
    run_record = build_run_record(
        run_id=run_id, generation=generation, requested=requested,
        priority=args.priority, checkout=checkout,
        published_root=published_root)
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
            _record_entry(leg_dir, {**entry,
                "reason": f"precondition refused ({leg}): module "
                          f"{module_name} absent: did not test ({exc})"},
                results, stderr=True)
            continue
        try:
            spec = module.build()
        except Exception as exc:  # LegBuildRefused or a bug: nothing executed.
            detail = str(exc)
            if "did not test" not in detail:
                detail += ": did not test"
            _record_entry(leg_dir, {**entry,
                "reason": f"precondition refused ({leg}): build refused: "
                          f"{detail}"},
                results, stderr=True)
            continue
        write_json(leg_dir / "spec.json", spec)

        runner = _run_fanout_leg if "actions" in spec else _run_single_leg
        runner(paths, module, spec, leg, checkout, run_id, generation,
               args.priority, fleet_root, leg_dir, entry, results)

    try:
        from pbcanary_verdict import verdict
    except ImportError as exc:
        error = f"verdict module absent: {exc}"
        write_json(namespace / "canary-result.json",
                   {"run_id": run_id, "generation": generation,
                    "results": results, "verdict": None,
                    "sealed": True, "error": error})
        if summary_dir:
            write_summary_dir(summary_dir, run_id, results, None, error=error)
        print("pbcanary: precondition refused (run): "
              "tools/fleet/pbcanary_verdict.py absent (crew C pending): "
              f"did not test ({exc})", file=sys.stderr)
        return 2

    exit_code, summary = verdict(results)
    summary["sealed"] = True
    write_json(namespace / "canary-result.json",
               {"run_id": run_id, "generation": generation,
                "results": results, "verdict": summary})
    if summary_dir:
        write_summary_dir(summary_dir, run_id, results, summary)
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
                             f"(default: {DEFAULT_LEGS})")
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
    parser.add_argument("--generation", default=None,
                        help="runtime generation this run verifies against; "
                             "recorded in run.json and sealed into each "
                             "action's environment (the rollout gate passes "
                             "the just-activated generation)")
    parser.add_argument("--summary-dir", default=None,
                        help="also drop canary-result.json + summary.txt here "
                             "(the GitHub workflow's pbcanary-summary/ "
                             "artifact contract)")
    args = parser.parse_args(argv)
    return run_canary(args)


if __name__ == "__main__":
    sys.exit(main())

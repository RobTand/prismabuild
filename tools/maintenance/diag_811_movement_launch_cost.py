#!/usr/bin/python3
"""Issue #811 diagnostic: claim-to-terminal cost of a tier-host movement action.

Runs as one admitted PrismaBuild action on the tier host.  It replays the exact
production movement action's launch path -- the same sealed checkout snapshot
bundle, the same worker script, the same contained launch -- on a private queue
and a private CAS root, with a trivial payload, so the time it measures is the
launch path and not the bytes moved.

Arms, interleaved A/B, plus a path control and a direct launch:

  A   ``checkout_snapshot``  the current path: the claiming worker materialises
      the sealed Git bundle into a private tree for every action.
  B   ``checkout_root``      a materialised tree reused across actions, with a
      closure stamp derived from that tree: the path a fix would take for a
      movement node whose code closure is the published runtime.  (A stamp
      derived here is required because the sealed production stamp describes
      the producer's dirty tree and cannot match any clean materialised one.)
  A2  arm A under the production checkout root, to separate path effects from
      the box's load during this run.
  direct  ``run-local`` with no broker and no proxy, under ``-X importtime``,
      to attribute interpreter and import cost.

Nothing here writes to the live queue, the live CAS or a tier: the production
request and its bundle are read, and every queue/CAS/tree it creates is under
its own work root.  The profile and this report are the artifacts.
"""
from __future__ import annotations

import argparse
import contextlib
import copy
import functools
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid


_ACTION_BODY_KEYS = (
    "schema",
    "task",
    "inputs",
    "code_closure",
    "params",
    "environment",
    "execution_scope",
)


def log(message: str) -> None:
    print(f"[diag811 {time.strftime('%H:%M:%S')}] {message}", flush=True)


class Recorder:
    """Absolute-stamp event log for one repetition."""

    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []
        self.git_spans: list[dict[str, object]] = []

    def mark(self, name: str, when: float | None = None) -> None:
        self.events.append({"name": name, "unix": time.time() if when is None else when})

    def span(self, name: str, function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            started = time.time()
            self.mark(f"{name}:start", started)
            try:
                return function(*args, **kwargs)
            finally:
                self.mark(f"{name}:end")

        return wrapper

    def context(self, name: str, original):
        @contextlib.contextmanager
        def wrapper(*args, **kwargs):
            started = time.time()
            self.mark(f"{name}:start", started)
            try:
                with original(*args, **kwargs) as value:
                    yield value
            finally:
                self.mark(f"{name}:end")

        return wrapper

    def segments(self) -> dict[str, float]:
        out: dict[str, float] = {}
        stack: dict[str, float] = {}
        for event in self.events:
            name = str(event["name"])
            if name.endswith(":start"):
                stack[name[:-6]] = float(event["unix"])
            elif name.endswith(":end") and name[:-4] in stack:
                key = name[:-4]
                out[key] = round(float(event["unix"]) - stack.pop(key), 4)
        return out


_MISSING = object()


def install_recorder(rec: Recorder, pool, materialize):
    """Patch the launch path so each phase is stamped.  Returns a restore fn."""

    saved: list[tuple[object, str, object]] = []

    def patch(target, attribute: str, replacement) -> None:
        # The raw descriptor, not the bound value: restoring a plain function
        # where a staticmethod was would silently change its call signature.
        saved.append((target, attribute, target.__dict__.get(attribute, _MISSING)))
        setattr(target, attribute, replacement)

    original_git = materialize._run_materializer_git

    def timed_git(argv, *, where, environment=None):
        started = time.time()
        try:
            return original_git(argv, where=where, environment=environment)
        finally:
            rec.git_spans.append(
                {"where": where, "seconds": round(time.time() - started, 4)}
            )

    patch(materialize, "_run_materializer_git", timed_git)
    patch(
        materialize,
        "_require_contained_materialized_links",
        rec.context("materialize.symlink_scan", materialize._require_contained_materialized_links),
    )
    patch(
        materialize,
        "_cleanup_execution_checkout",
        rec.span("materialize.cleanup", materialize._cleanup_execution_checkout),
    )
    patch(pool.PoolQueue, "claim", rec.span("claim", pool.PoolQueue.claim))
    patch(pool.PoolQueue, "execute", rec.span("execute", pool.PoolQueue.execute))
    patch(
        pool.PoolQueue,
        "_execute_in_checkout",
        rec.span("launcher", pool.PoolQueue._execute_in_checkout),
    )
    patch(
        pool.PoolQueue,
        "_start_resource_scope",
        rec.span("scope.create", pool.PoolQueue._start_resource_scope),
    )
    patch(
        pool.PoolQueue,
        "_resource_profile",
        rec.span("resource_profile", pool.PoolQueue._resource_profile),
    )
    patch(
        pool.PoolQueue,
        "_box_window",
        staticmethod(rec.span("box_window", pool.PoolQueue._box_window)),
    )
    patch(
        pool.PoolQueue,
        "_sample_resource_scope",
        staticmethod(rec.span("sample_scope", pool.PoolQueue._sample_resource_scope)),
    )
    patch(pool.PoolQueue, "finish", rec.span("finish", pool.PoolQueue.finish))

    def restore() -> None:
        for target, attribute, value in reversed(saved):
            if value is _MISSING:
                delattr(target, attribute)
            else:
                setattr(target, attribute, value)

    return restore


def build_body(
    production: dict,
    *,
    result_name: str,
    checkout_root: Path | None,
    closure: dict | None = None,
    inputs: list | None = None,
):
    """The production movement action's body, with a trivial payload.

    ``checkout_root=None`` keeps the sealed snapshot and its input.  A path
    (the reuse arm) drops the snapshot, declares no inputs, and takes the
    closure the caller computed for that live tree.
    """

    body = {key: copy.deepcopy(production[key]) for key in _ACTION_BODY_KEYS}
    params = body["params"]
    for key in (
        "placement",
        "data_manifest",
        "produced_output_batch",
        "produced_output_template",
        "retry_policy",
    ):
        params.pop(key, None)
    params["cwd"] = "."
    params["demand"] = {"cpu": 1, "mem_gb": 1}
    if checkout_root is None:
        params["checkout_snapshot"] = copy.deepcopy(production["params"]["checkout_snapshot"])
        body["inputs"] = [dict(production["params"]["checkout_snapshot"]["input"])]
    else:
        params.pop("checkout_snapshot", None)
        params["checkout_root"] = str(checkout_root)
        body["inputs"] = []
    if closure is not None:
        body["code_closure"] = copy.deepcopy(closure)
    if inputs is not None:
        body["inputs"] = copy.deepcopy(inputs)
    command = [
        "/bin/bash", "--noprofile", "--norc", "-c",
        f"printf 'ok {result_name}' > {result_name}",
    ]
    body["task"]["argv"] = command
    body["task"]["working_directory"] = "."
    body["task"]["result_path"] = result_name
    params["command"] = ["/bin/bash", "-c", f"printf 'ok {result_name}' > {result_name}"]
    variables = body["environment"]["variables"]
    variables.pop("PRISMABUILD_CONTAINER_OWNER", None)
    variables.pop("PRISMABUILD_CONTAINER_MARKER", None)
    return body


def result_name(tag: str, rep: int) -> str:
    return f"pbrun_result.{tag}{rep:015x}.txt"


def seed_cas(source_cas: Path, isolated_cas: Path, digest: str) -> None:
    """One production input blob, hardlinked (or copied) into a private CAS."""

    source = source_cas / "blobs" / digest[:2] / digest
    destination = isolated_cas / "blobs" / digest[:2] / digest
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.unlink(missing_ok=True)
    try:
        os.link(source, destination)
    except OSError:
        shutil.copyfile(source, destination)
        os.chmod(destination, 0o444)


def run_arm(
    *,
    label: str,
    rep: int,
    body: dict,
    queue,
    core,
    pool,
    materialize,
    cas_root: Path,
    worker_script: Path,
    worker_python: str,
    containment: bool,
    checkout_snapshot,
    checkout_root,
) -> dict:
    rec = Recorder()
    action = core.seal_action(body)
    key = str(action["action_key"])
    started = time.time()
    restore = None
    try:
        core.PrismaBuildCAS(cas_root).publish_action_request(action)
        queue.publish(
            action_key=key,
            cas_root=cas_root,
            worker_script=worker_script,
            checkout_snapshot=checkout_snapshot,
            checkout_root=checkout_root,
            tags=(),
            priority=-10,
            resources={"cpu": 1, "mem_gb": 1},
            max_attempts=1,
            retry_safe=True,
            recompute=True,
        )
        restore = install_recorder(rec, pool, materialize)
        outcome = queue.serve_once(
            tags=(),
            has_gpu=False,
            python=worker_python,
            containment=containment,
        )
    except Exception as exc:  # noqa: BLE001
        outcome = {"status": "harness_error", "error": f"{type(exc).__name__}: {exc}"}
    finally:
        if restore is not None:
            restore()
    wall = time.time() - started
    record = {
        "arm": label,
        "rep": rep,
        "action_key": key,
        "wall_seconds": round(wall, 4),
        "status": outcome.get("status") if isinstance(outcome, dict) else None,
        "returncode": outcome.get("returncode") if isinstance(outcome, dict) else None,
        "launcher_elapsed_s": outcome.get("elapsed_s") if isinstance(outcome, dict) else None,
        "execution_observation": outcome.get("execution_observation") if isinstance(outcome, dict) else None,
        "resource_telemetry": outcome.get("resource_telemetry") if isinstance(outcome, dict) else None,
        "resource_profile": outcome.get("resource_profile") if isinstance(outcome, dict) else None,
        "argv": outcome.get("argv") if isinstance(outcome, dict) else None,
        "segments": rec.segments(),
        "git_spans": rec.git_spans,
        "events": rec.events,
        "error": outcome.get("error") if isinstance(outcome, dict) else None,
    }
    done = queue.item_path("done", key)
    if not done.exists():
        done = queue.item_path("failed", key)
    if done.exists():
        terminal = json.loads(done.read_text())
        record["claimed_unix"] = terminal.get("claimed_unix")
        record["terminal_finished_unix"] = terminal.get("finished_unix")
        try:
            attempt = json.loads(queue.attempt_path(terminal, 1).read_text())
            record["attempt_claimed_unix"] = attempt.get("claimed_unix")
            record["attempt_finished_unix"] = attempt.get("finished_unix")
        except (OSError, ValueError):
            pass
    for stream in ("stdout", "stderr"):
        value = outcome.get(stream) if isinstance(outcome, dict) else None
        record[f"{stream}_tail"] = str(value)[-2000:] if value else ""
    log(
        f"{label} rep {rep}: status={record['status']} wall={record['wall_seconds']}s "
        f"launcher={record['launcher_elapsed_s']}"
    )
    if record["status"] != "executed":
        log(f"    stderr: {record['stderr_tail'][-500:]}")
    return record


def parse_importtime(stderr: str) -> dict[str, object]:
    """``python -X importtime`` rows: self us | cumulative us | module."""

    total = None
    top: list[tuple[int, str]] = []
    for line in stderr.splitlines():
        line = line.strip()
        if not line.startswith("import time:"):
            continue
        fields = [field.strip() for field in line[len("import time:"):].split("|")]
        if len(fields) < 3:
            continue
        try:
            cumulative_us = int(fields[1])
        except ValueError:
            continue
        module = fields[2]
        if module == "prismabuild.core" or module.startswith("prismabuild.core."):
            total = cumulative_us
        top.append((cumulative_us, module))
    top.sort(reverse=True)
    return {
        "prismabuild_core_cumulative_us": total,
        "slowest_imports_us": top[:15],
    }


def direct_run(
    *,
    worker_python: str,
    worker_script: Path,
    cas_root: Path,
    checkout_root: Path,
    request_path: Path,
) -> dict:
    argv = [
        worker_python, "-X", "importtime", str(worker_script), "run-local",
        "--action", str(request_path),
        "--cas-root", str(cas_root),
        "--checkout-root", str(checkout_root),
    ]
    # The outer action's broker identity is not this action's; a direct
    # launch must not forward it, so the three launcher-owned names are
    # cleared (resource_exec would have replaced them for the real launch).
    environment = {
        name: value for name, value in os.environ.items()
        if name not in (
            "PRISMABUILD_ACTION_NONCE",
            "PRISMABUILD_ACTION_SCOPE",
            "PRISMABUILD_READER_HELPER_ROOT",
        )
    }
    started = time.time()
    completed = subprocess.run(
        argv, capture_output=True, text=True, timeout=600, env=environment
    )
    wall = time.time() - started
    return {
        "argv": argv,
        "wall_seconds": round(wall, 4),
        "returncode": completed.returncode,
        "stdout_tail": completed.stdout[-400:],
        "stderr_tail": completed.stderr[-2000:],
        "importtime": parse_importtime(completed.stderr),
    }


def median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def summarise(records: list[dict]) -> dict:
    out: dict[str, object] = {}
    for arm in sorted({str(record["arm"]) for record in records}):
        rows = [record for record in records if record["arm"] == arm]
        wall = [float(row["wall_seconds"]) for row in rows]
        launcher = [
            float(row["launcher_elapsed_s"])
            for row in rows
            if isinstance(row.get("launcher_elapsed_s"), (int, float))
        ]
        prelaunch = [
            float(row["segments"].get("execute", 0.0))
            - float(row["segments"].get("launcher", 0.0))
            - float(row["segments"].get("resource_profile", 0.0))
            - float(row["segments"].get("sample_scope", 0.0))
            for row in rows
        ]
        git_total = [
            sum(float(span["seconds"]) for span in row.get("git_spans", []))
            for row in rows
        ]
        out[arm] = {
            "reps": len(rows),
            "claim_to_terminal_median_s": median(
                [
                    float(row["attempt_finished_unix"]) - float(row["attempt_claimed_unix"])
                    for row in rows
                    if isinstance(row.get("attempt_finished_unix"), (int, float))
                    and isinstance(row.get("attempt_claimed_unix"), (int, float))
                ]
            ),
            "harness_wall_median_s": median(wall),
            "launcher_elapsed_median_s": median(launcher),
            "prelaunch_median_s": median(prelaunch),
            "materialize_git_median_s": median(git_total),
            "segments_median": {
                name: median([float(row["segments"].get(name, 0.0)) for row in rows])
                for name in (
                    "claim",
                    "execute",
                    "launcher",
                    "scope.create",
                    "resource_profile",
                    "box_window",
                    "sample_scope",
                    "finish",
                    "materialize.cleanup",
                    "materialize.symlink_scan",
                )
            },
        }
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--production-request", required=True)
    parser.add_argument("--source-cas-root", default="/mnt/shared/prismabuild-fleet/cas")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--worker-python", default="/home/rob/venvs/pb-cpu/bin/python")
    parser.add_argument("--work-root", default="/home/rob/tmp/diag811-work")
    parser.add_argument("--direct-only", action="store_true")
    parser.add_argument("--report", default=None)
    args = parser.parse_args()

    generation_root = Path(
        os.environ.get(
            "PRISMABUILD_READER_HELPER_ROOT",
            "/mnt/shared/prismabuild-fleet/runtime-generations/d1c640e74d97-1790008584-459339399864",
        )
    )
    sys.path.insert(0, str(generation_root / "src"))
    import prismabuild.core as core  # noqa: E402
    import prismabuild.materialize as materialize  # noqa: E402
    import prismabuild.pool as pool  # noqa: E402

    assert str(core.__file__).startswith(str(generation_root)), core.__file__
    worker_script = generation_root / "tools" / "prismabuild_worker.py"
    assert worker_script.is_file(), worker_script

    production = json.loads(Path(args.production_request).read_text())
    snapshot = production["params"]["checkout_snapshot"]
    digest = snapshot["input"]["sha256"]

    work = Path(args.work_root) / f"run-{os.getpid()}-{int(time.time())}"
    work.mkdir(parents=True, exist_ok=True)
    cas_root = work / "cas"
    queue_root = work / "queue"
    checkout_root = work / "checkouts"
    checkout_root.mkdir(parents=True, exist_ok=True)
    seed_cas(Path(args.source_cas_root), cas_root, digest)
    pool.LOCAL_CHECKOUT_ROOT = checkout_root
    materialize.LOCAL_CHECKOUT_ROOT = checkout_root

    log(f"generation={generation_root.name} work={work} bundle={digest[:12]}")
    log(f"production request {production['action_key'][:12]} snapshot commit {snapshot['commit'][:12]}")

    queue = pool.PoolQueue(queue_root)
    records: list[dict] = []
    direct: list[dict] = []

    snapshot_body = build_body(production, result_name=result_name("a", 0), checkout_root=None)
    snapshot_action = core.seal_action(snapshot_body)
    core.PrismaBuildCAS(cas_root).publish_action_request(snapshot_action)

    reuse_item = {
        "action_key": str(snapshot_action["action_key"]),
        "cas_root": str(cas_root),
        "checkout_snapshot": snapshot,
    }
    with materialize._execution_checkout(
        reuse_item, local_checkout_root=checkout_root
    ) as reused:
        log(f"reused tree materialised at {reused}")
        # A path-addressed pbrun action must carry a stamp that names its own
        # live tree.  Deriving it here is what makes the reuse arm a legal
        # action under the current worker contract rather than a refusal; the
        # production stamp describes a dirty submission tree and cannot match
        # any materialised one.
        identity = core.git_checkout_identity(reused)
        stamp_name = ".pbrun-closure." + uuid.uuid4().hex[:16] + ".json"
        stamp = {
            "cwd": ".",
            "head": identity["head"],
            "dirty_sha256": identity["dirty_sha256"],
        }
        (reused / stamp_name).write_text(json.dumps(stamp, sort_keys=True) + "\n")
        reuse_closure = core.build_code_closure(reused, [stamp_name])
        log(f"reuse stamp {stamp_name} head={stamp['head'][:12]} dirty={stamp['dirty_sha256'][:12]}")

        for rep in range(0 if args.direct_only else args.reps):
            for label, use_snapshot in (("A", True), ("B", False)):
                body = build_body(
                    production,
                    result_name=result_name(label.lower(), rep),
                    checkout_root=None if use_snapshot else reused,
                    closure=None if use_snapshot else reuse_closure,
                )
                record = run_arm(
                    label=label,
                    rep=rep,
                    body=body,
                    queue=queue,
                    core=core,
                    pool=pool,
                    materialize=materialize,
                    cas_root=cas_root,
                    worker_script=worker_script,
                    worker_python=args.worker_python,
                    containment=True,
                    checkout_snapshot=snapshot if use_snapshot else None,
                    checkout_root=None if use_snapshot else reused,
                )
                records.append(record)

        # Path control: the identical snapshot arm, materialised under the
        # production checkout root instead of this action's own, so the
        # measured fetch cost can be attributed to the tree's location rather
        # than to the box's load during this run.
        if not args.direct_only:
            production_root = Path(materialize.DEFAULT_LOCAL_CHECKOUT_ROOT)
            saved_root = pool.LOCAL_CHECKOUT_ROOT
            try:
                pool.LOCAL_CHECKOUT_ROOT = production_root
                for rep in range(2):
                    body = build_body(
                        production,
                        result_name=result_name("c", rep),
                        checkout_root=None,
                    )
                    record = run_arm(
                        label="A2",
                        rep=rep,
                        body=body,
                        queue=queue,
                        core=core,
                        pool=pool,
                        materialize=materialize,
                        cas_root=cas_root,
                        worker_script=worker_script,
                        worker_python=args.worker_python,
                        containment=True,
                        checkout_snapshot=snapshot,
                        checkout_root=None,
                    )
                    records.append(record)
            finally:
                pool.LOCAL_CHECKOUT_ROOT = saved_root

        for rep in range(2):
            body = build_body(
                production,
                result_name=result_name("d", rep),
                checkout_root=reused,
                closure=reuse_closure,
            )
            action = core.seal_action(body)
            request_path = core.PrismaBuildCAS(cas_root).publish_action_request(action)
            outcome = direct_run(
                worker_python=args.worker_python,
                worker_script=worker_script,
                cas_root=cas_root,
                checkout_root=reused,
                request_path=request_path,
            )
            log(f"direct rep {rep}: wall={outcome['wall_seconds']}s rc={outcome['returncode']}")
            direct.append(outcome)

    report = {
        "schema": "prismabuild.diag811.movement_launch_cost.v1",
        "generation": generation_root.name,
        "worker_script": str(worker_script),
        "worker_python": args.worker_python,
        "production_request": production["action_key"],
        "production_snapshot": snapshot,
        "work_root": str(work),
        "checkout_root_used": str(checkout_root),
        "production_checkout_root": str(Path(materialize.DEFAULT_LOCAL_CHECKOUT_ROOT)),
        "statvfs": {
            str(path): {
                "device": os.stat(path).st_dev,
                "fsid": getattr(os.statvfs(path), "f_fsid", None),
                "bsize": os.statvfs(path).f_bsize,
            }
            for path in (work, checkout_root, Path(materialize.DEFAULT_LOCAL_CHECKOUT_ROOT))
        },
        "host": os.uname().nodename,
        "records": records,
        "summary": summarise(records),
        "direct": direct,
        "direct_median_wall_s": median([float(row["wall_seconds"]) for row in direct]),
    }
    text = json.dumps(report, indent=1, sort_keys=True)
    if args.report:
        Path(args.report).write_text(text + "\n")
    print("DIAG811-REPORT-BEGIN")
    print(text)
    print("DIAG811-REPORT-END")
    return 0 if (records or direct) else 1


if __name__ == "__main__":
    raise SystemExit(main())

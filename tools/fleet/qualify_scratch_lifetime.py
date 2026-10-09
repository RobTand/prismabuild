"""Qualify generation-bound scratch lifetime on admitted workers (#1360).

Run through the published pbrun on an x86 worker that announces measured
scratch capacity, with sealed scratch pairs and a versioned lifetime
selection. Do not place these CPU scenarios on a Spark. The pool registers the
ephemeral leaf before launch and cleans it only after exact stopped-attempt
proof. The harness asserts that lifecycle from inside the payload and from
the published terminal record.

Usage (one scenario per submission; roots must already exist on the worker
and lie on its supervisor-measured local disk, never on tmpfs):

  python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \\
    --cwd <checkout> --tag x86 --cpus 1 --demand mem_gb=2 \\
    --env IG1360_TEMP_ROOT=<disk-root>/ig1360-temp \\
    --env IG1360_TEMP_MAX=1073741824 \\
    --env IG1360_CACHE_ROOT=<disk-root>/ig1360-cache \\
    --env IG1360_CACHE_MAX=1073741824 \\
    --env PRISMABUILD_LOCAL_SCRATCH_PAIRS=IG1360_TEMP_ROOT:IG1360_TEMP_MAX,IG1360_CACHE_ROOT:IG1360_CACHE_MAX \\
    --env PRISMABUILD_EPHEMERAL_SCRATCH_DECLARATIONS='<selection>' \\
    -- /home/rob/venvs/pb-cpu/bin/python \\
    tools/fleet/qualify_scratch_lifetime.py --scenario <name> \\
    --temp-root-env IG1360_TEMP_ROOT --temp-name row-temp \\
    --cache-root-env IG1360_CACHE_ROOT --cache-name compile

The harness reads only its own admitted claim and the private leaf the pool
registered for this exact attempt. It mutates no queue record and no foreign
path. Unknown scenarios exit 2 without side effects.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from contextlib import ExitStack

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild import client, local_scratch, pool  # noqa: E402


def _harness(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True,
                        choices=["normal", "failed", "sigkill-child", "failing-cleanup-guard",
                                 "launcher-victim", "launcher-killer"],
                        help="lifecycle path this payload exercises")
    parser.add_argument("--temp-root-env", required=True,
                        help="environment variable that holds the sealed root of the "
                             "ephemeral row-temporary scratch pair")
    parser.add_argument("--temp-name", required=True,
                        help="declared name of the ephemeral leaf beneath that root")
    parser.add_argument("--cache-root-env", required=True,
                        help="environment variable that holds the persistent cache root; "
                             "the harness writes a marker there that cleanup must keep")
    parser.add_argument("--cache-name", required=True,
                        help="name prefix of the marker file written to the persistent "
                             "cache root")
    parser.add_argument("--queue-root", default="/mnt/shared/prismabuild-fleet/pb-queue",
                        help="pull-queue root that holds this action's claimed record; "
                             "the harness reads only its own claim")
    parser.add_argument("--rendezvous-root", default="/mnt/shared/pb-qualification",
                        help="shared directory for launcher kill rendezvous files")
    parser.add_argument("--rendezvous-id", default="",
                        help="rendezvous namespace; required for launcher kill roles")
    parser.add_argument("--victim-key", default="",
                        help="victim action key; required for the killer role")
    return parser.parse_args(argv)


def _live_claim(queue_root: str, key: str) -> dict:
    queue = pool.PoolQueue(Path(queue_root))
    record = pool._read_json(queue.item_path(pool.CLAIMED, key))
    if record is None:
        raise SystemExit("own claim is not in claimed state")
    return record


def _bound_leaf(record, *, root_env: str, name: str) -> tuple[Path, dict]:
    field = record.get(local_scratch.SCRATCH_LIFETIME_FIELD)
    if not isinstance(field, dict):
        raise SystemExit("own claim carries no lifetime record")
    for entry in field.get("entries", []):
        declaration = entry.get("declaration") or {}
        if declaration.get("root_env") == root_env and declaration.get("name") == name:
            if entry.get("lifetime") != "ephemeral":
                raise SystemExit("named entry is not ephemeral")
            return local_scratch.ephemeral_scratch_path(declaration), entry
    raise SystemExit("named ephemeral entry is absent from own record")


def _rendezvous_dir(args) -> Path:
    if not args.rendezvous_id or "/" in args.rendezvous_id or args.rendezvous_id in (".", ".."):
        raise SystemExit("launcher kill roles need --rendezvous-id")
    root = Path(args.rendezvous_root)
    allowed = Path("/mnt/shared/pb-qualification")
    if allowed not in root.resolve().parents and root.resolve() != allowed:
        raise SystemExit("--rendezvous-root must stay beneath /mnt/shared/pb-qualification")
    directory = root / args.rendezvous_id
    if allowed not in directory.resolve().parents and directory.resolve() != allowed:
        raise SystemExit("rendezvous id escapes the qualification root")
    return directory


def _launcher_victim(args, key: str, leaf: Path, result: dict) -> int:
    import time
    directory = _rendezvous_dir(args)
    directory.mkdir(parents=True, exist_ok=True)
    marker = leaf / "victim-marker.txt"
    marker.write_text(f"{key}\n", encoding="utf-8")
    launchers = pool.find_launcher_pids(key)
    if not launchers:
        raise SystemExit("victim found no live launcher for its own key")
    (directory / "victim-ready.json").write_text(json.dumps({
        "action_key": key, "host": result["host"], "leaf": str(leaf),
        "launcher_pids": launchers, "ready_unix": time.time(),
    }) + "\n", encoding="utf-8")
    print(json.dumps({**result, "launcher_pids": launchers}))
    deadline = time.monotonic() + 540
    while time.monotonic() < deadline:
        time.sleep(2)
    raise SystemExit("victim survived its kill window; killer never acted")


def _launcher_pidfd(pid: int, victim: str) -> int:
    """Pin one verified launcher incarnation, with no numeric signal fallback."""
    import signal
    if (not callable(getattr(os, "pidfd_open", None))
            or not callable(getattr(signal, "pidfd_send_signal", None))):
        raise SystemExit("launcher qualification requires pidfd support")
    descriptor = None
    pinned = False
    try:
        ticks = pool._contained_worker_start_ticks(pid)
        argv = pool._process_cmdline(pid)
        if (ticks is None or not argv or victim.encode() not in b"\0".join(argv)
                or b"run-local" not in argv):
            raise ValueError("process identity is unavailable or foreign")
        descriptor = os.pidfd_open(pid, 0)
        if (pool._contained_worker_start_ticks(pid) != ticks
                or pool._process_cmdline(pid) != argv):
            raise ValueError("process identity changed around pidfd open")
        pinned = True
        return descriptor
    except (OSError, ValueError) as exc:
        raise SystemExit(f"refusing launcher {pid}: {exc}") from exc
    finally:
        if descriptor is not None and not pinned:
            os.close(descriptor)


def _launcher_killer(args, key: str, result: dict) -> int:
    import signal
    import time
    victim = args.victim_key
    if not victim or len(victim) != 64:
        raise SystemExit("killer needs the victim action key")
    directory = _rendezvous_dir(args)
    ready_path = directory / "victim-ready.json"
    deadline = time.monotonic() + 420
    while not ready_path.exists():
        if time.monotonic() >= deadline:
            raise SystemExit("victim never published its rendezvous file")
        time.sleep(2)
    ready = json.loads(ready_path.read_text(encoding="utf-8"))
    if ready.get("action_key") != victim:
        raise SystemExit("rendezvous file names another action")
    if ready.get("host") != result["host"]:
        raise SystemExit("killer and victim landed on different hosts")
    launchers = pool.find_launcher_pids(victim)
    if not launchers:
        raise SystemExit("killer found no live victim launcher")
    with ExitStack() as handles:
        targets = []
        for pid in launchers:
            descriptor = _launcher_pidfd(pid, victim)
            handles.callback(os.close, descriptor)
            targets.append((pid, descriptor))
        (directory / "kill-intent.json").write_text(json.dumps({
            "victim": victim, "launcher_pids": launchers,
            "killer": key, "host": result["host"],
        }) + "\n", encoding="utf-8")
        time.sleep(5)
        killed = []
        for pid, descriptor in targets:
            try:
                signal.pidfd_send_signal(descriptor, signal.SIGKILL)
                killed.append(pid)
            except ProcessLookupError:
                pass
    (directory / "kill-done.json").write_text(json.dumps({
        "victim": victim, "killed": killed, "killer": key,
    }) + "\n", encoding="utf-8")
    print(json.dumps({**result, "victim": victim, "killed_launcher_pids": killed,
                      "victim_leaf": ready.get("leaf")}))
    return 0


def main(argv=None) -> int:
    args = _harness(argv)
    key = os.environ.get("PRISMABUILD_ACTION_KEY", "")
    if not key:
        raise SystemExit("PRISMABUILD_ACTION_KEY is absent; submit through pbrun")
    record = _live_claim(args.queue_root, key)
    leaf, entry = _bound_leaf(record, root_env=args.temp_root_env, name=args.temp_name)
    if not leaf.is_dir():
        raise SystemExit(f"registered leaf is absent: {leaf}")
    identity = entry.get("identity")
    if not identity:
        raise SystemExit("registered leaf has no committed identity")
    local_scratch._check_registered_scratch_directory(
        {k: entry["declaration"][k] for k in entry["declaration"]}, identity)
    payload = leaf / "payload-marker.txt"
    payload.write_text(f"{key}\n", encoding="utf-8")
    cache_root = os.environ.get(args.cache_root_env, "")
    if not cache_root:
        raise SystemExit("persistent cache root is absent from launch env")
    cache_file = Path(cache_root) / f"{args.cache_name}-marker.txt"
    cache_file.write_text(f"{key}\n", encoding="utf-8")
    result = {
        "schema": "prismabuild.scratch_lifetime_qualification.v1",
        "scenario": args.scenario,
        "action_key": key,
        "host": record.get("claimed_host"),
        "leaf": str(leaf),
        "leaf_existed_before_payload_exit": True,
        "persistent_marker": str(cache_file),
        "sdk_version": client.SDK_VERSION,
        "capability": client.SCRATCH_LIFETIME_TAG,
    }
    print(json.dumps(result))
    if args.scenario == "failed":
        return 3
    if args.scenario == "sigkill-child":
        import signal
        import subprocess
        child = subprocess.Popen([sys.executable, "-c",
                                  "import time; time.sleep(600)"])
        try:
            child.send_signal(signal.SIGKILL)
            child.wait(timeout=30)
        finally:
            if child.poll() is None:
                child.kill()
        result["sigkilled_descendant_pid"] = child.pid
        result["sigkilled_descendant_returncode"] = child.returncode
        print(json.dumps(result))
        return 0
    if args.scenario == "failing-cleanup-guard":
        # Plant a symlink inside the owned leaf. Production cleanup must
        # unlink it without following it; the foreign target must survive.
        foreign = Path(cache_root) / "guard-target.txt"
        foreign.write_text("guard target survives\n", encoding="utf-8")
        link = leaf / "guard-link"
        if link.is_symlink() or link.exists():
            link.unlink()
        link.symlink_to(foreign)
        result["guard_link"] = str(link)
        result["guard_target"] = str(foreign)
        print(json.dumps(result))
        return 0
    if args.scenario == "launcher-victim":
        return _launcher_victim(args, key, leaf, result)
    if args.scenario == "launcher-killer":
        return _launcher_killer(args, key, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

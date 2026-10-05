#!/usr/bin/env python3
"""Submit a gang: N actions admitted together or not at all (#1517).

One manifest names every member. This seals each member with a fresh gang
group through ``pbrun --detach --gang-*`` (one key per member), and only
after every member row is published files the group record -- no member is
claimable before it exists. Any refusal withdraws the members already
published and exits nonzero, so a half-submitted gang never waits forever.

Manifest (JSON)::

    {"priority": 10, "skew_s": 120, "timeout_s": 3600,
     "members": [
       {"tag": "sparklina", "demand": "gpu=1,mem_gb=100", "env": ["K=V"],
        "argv": ["/home/rob/venvs/x/bin/python", "head.py"]},
       {"tag": "sparky", "demand": "gpu=1,mem_gb=100",
        "argv": ["/home/rob/venvs/x/bin/python", "worker.py"]}]}

``tag`` is the host (or host class) a member is placed on; members land on
distinct hosts. ``timeout_s`` and ``priority`` apply to every member unless a
member overrides them. Prints one JSON line: the group and its member keys.
Gang admission must be enabled on the target boxes (worker ``--gang-admission``);
otherwise ``pbrun`` refuses because no box offers the capability.

Priority: a gang election fences its hosts only against strictly lower
priority work. Run windows (Goal 1/2) at priority 10 with routine work at 0
or below; equal or higher priority work can still take a fenced host.
"""
from __future__ import annotations

import argparse
import json
import secrets
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve(strict=True).parent
sys.path.insert(0, str(HERE))
from runtime_paths import generation_root  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import _gang, pool  # noqa: E402

SH = Path("/mnt/shared/prismabuild-fleet")
SCHEMA = "prismabuild.pbgang.v1"
MEMBER_FIELDS = {"tag", "argv", "demand", "env", "timeout_s", "priority", "cpus"}


def load(path: Path) -> dict:
    manifest = json.loads(path.read_text())
    members = manifest.get("members") if isinstance(manifest, dict) else None
    if not isinstance(members, list) or not 2 <= len(members) <= _gang.MAX_MEMBERS:
        raise SystemExit(f"pbgang: manifest needs 2..{_gang.MAX_MEMBERS} members")
    unknown = set(manifest) - {"members", "priority", "skew_s", "timeout_s"}
    if unknown:
        raise SystemExit(f"pbgang: unknown manifest fields {sorted(unknown)}")
    for index, member in enumerate(members):
        if (not isinstance(member, dict) or set(member) - MEMBER_FIELDS
                or not isinstance(member.get("tag"), str) or not member["tag"]
                or not isinstance(member.get("argv"), list) or not member["argv"]):
            raise SystemExit(f"pbgang: member {index} needs a tag and an argv; "
                             f"allowed fields {sorted(MEMBER_FIELDS)}")
    return manifest


def member_command(args, manifest: dict, member: dict, *, group: str, index: int) -> list[str]:
    size = len(manifest["members"])
    command = [sys.executable, str(HERE / "pbrun.py"), "--cwd", str(args.cwd), "--detach",
               "--tag", member["tag"], "--gang-group", group, "--gang-size", str(size),
               "--gang-index", str(index),
               "--priority", str(member.get("priority", manifest.get("priority", 0)))]
    timeout = member.get("timeout_s", manifest.get("timeout_s"))
    if timeout is not None:
        command += ["--timeout-s", str(timeout)]
    if member.get("demand"):
        command += ["--demand", str(member["demand"])]
    if member.get("cpus") is not None:
        command += ["--cpus", str(member["cpus"])]
    for entry in member.get("env", []):
        command += ["--env", str(entry)]
    return [*command, "--", *map(str, member["argv"])]


def withdraw(keys: list[str], reason: str) -> None:
    for key in keys:
        subprocess.run([sys.executable, str(HERE / "pbrun.py"), "--withdraw", key,
                        "--reason", reason], check=False)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--cwd", type=Path, required=True, help="checkout every member snapshots")
    ap.add_argument("--queue", type=Path, default=SH / "pb-queue")
    args = ap.parse_args(argv)
    manifest = load(args.manifest)
    skew_s = float(manifest.get("skew_s", _gang.DEFAULT_SKEW_S))
    group = secrets.token_hex(16)
    keys: list[str] = []
    for index, member in enumerate(manifest["members"]):
        result = subprocess.run(member_command(args, manifest, member, group=group, index=index),
                                capture_output=True, text=True, check=False)
        sys.stderr.write(result.stderr)
        line = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else ""
        try:
            reply = json.loads(line)
        except json.JSONDecodeError:
            reply = {}
        if result.returncode != 0 or reply.get("status") != "submitted":
            withdraw(keys, f"pbgang: member {index} was not submitted")
            print(f"pbgang: member {index} refused ({result.returncode}, {reply or line!r}); "
                  f"withdrew {len(keys)} published member(s)", file=sys.stderr)
            return 1
        keys.append(str(reply["action_key"]))
    queue = pool.PoolQueue(args.queue)
    rows = [pool._read_json(queue.item_path(pool.READY, key)) for key in keys]
    if any(row is None for row in rows):
        withdraw(keys, "pbgang: a member left READY before its group was filed")
        print("pbgang: a member row is no longer READY; withdrew the gang", file=sys.stderr)
        return 1
    try:
        record = _gang.publish_group(queue, group, rows, skew_s=skew_s)  # type: ignore[arg-type]
    except _gang.GangContractError as exc:
        withdraw(keys, f"pbgang: group record refused: {exc}")
        print(f"pbgang: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({"schema": SCHEMA, "group": group, "members": keys,
                      "skew_s": record["skew_s"], "priority": record["priority"]},
                     sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

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

The manifest may also be a bare list of members. A member names its host with
``tag`` or with ``tags`` (a list), may give ``demand`` as ``gpu=1,mem_gb=100`` or
as a mapping, ``env`` as ``K=V`` strings or as a mapping, and its own ``cwd``
(``--cwd`` is then optional).

A member carries the existing ``pbrun`` options a measurement window needs, each
as the one flag of that name: ``gpu_memory_gb``, ``exclusive`` and ``measurement``
(true or false), ``host_class``, ``container_images`` (a list, one flag per image),
``priority_reason`` and ``max_attempts`` (only 1). ``pbrun`` judges every value
exactly as it would for a plain submission. Any other key is refused by name:
the gang flags, ``tag``, ``priority`` and ``retry_safe`` are the driver's (a retry
ends the gang, so a member gets one attempt), and options a window does not
declare, such as a data manifest or residency, are not carried. ``--cwd`` is the
default checkout every member snapshots; a member's own ``cwd`` overrides it.
``priority_reason`` may also be set once in the manifest, like ``priority``.

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
MEMBER_FIELDS = {"tag", "tags", "cwd", "argv", "demand", "env", "timeout_s", "priority", "cpus"}

#: Member fields that are one ``pbrun`` flag each: exactly the existing options a
#: measurement window declares (#1517).  The kind says
#: how the JSON value becomes argv: ``switch`` is a boolean flag, ``value`` is
#: one scalar, ``repeat`` is a list with the flag once per entry.  ``pbrun``
#: stays the only judge of each value; this table forwards, it never reinterprets.
#: The gang flags, ``--tag``, ``--priority``, ``--detach`` and ``--cwd`` are
#: deliberately not here: the driver owns them.
FLAG_FIELDS: dict[str, tuple[str, str]] = {
    "gpu_memory_gb": ("--gpu-memory-gb", "value"),
    "exclusive": ("--exclusive", "switch"),
    "measurement": ("--measurement", "switch"),
    "host_class": ("--host-class", "value"),
    "container_images": ("--container-image", "repeat"),
    "priority_reason": ("--priority-reason", "value"),
    "max_attempts": ("--max-attempts", "value"),
}
MEMBER_FIELDS = MEMBER_FIELDS | set(FLAG_FIELDS)


def _field_problem(name: str, value: object) -> str | None:
    """Why ``value`` cannot be this member field, or ``None``."""

    kind = FLAG_FIELDS[name][1]
    if kind == "switch":
        return None if isinstance(value, bool) else "must be true or false"
    if kind == "repeat":
        if (isinstance(value, list) and value
                and all(isinstance(item, str) and item for item in value)):
            return None
        return "must be a nonempty list of nonempty strings"
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return "must be one string or number"
    if name == "max_attempts":
        # One attempt is what a gang member gets: an unsuccessful one ends the
        # whole gang.  Declaring it is allowed; asking for more is not.
        return None if value == 1 else "must be 1: a gang member gets one attempt"
    return "must not be empty" if value == "" else None


def _shape_problem(member: dict) -> str | None:
    """Why this member's tag, demand, env or cwd is malformed, or ``None``."""

    tags = member.get("tags", None)
    if ("tag" in member) == ("tags" in member):
        return "needs exactly one of tag or tags"
    if "tag" in member and not (isinstance(member["tag"], str) and member["tag"]):
        return "tag must be a nonempty string"
    if "tags" in member and not (isinstance(tags, list) and tags and all(
            isinstance(item, str) and item for item in tags)):
        return "tags must be a nonempty list of nonempty strings"
    if not isinstance(member.get("argv"), list) or not member["argv"]:
        return "needs a nonempty argv"
    demand = member.get("demand")
    if isinstance(demand, dict):
        if not demand or any(not isinstance(key, str) or not key
                             or isinstance(value, bool) or not isinstance(value, int)
                             for key, value in demand.items()):
            return "demand as a mapping needs string keys and whole-number values"
    elif demand is not None and not isinstance(demand, str):
        return "demand must be a string or a mapping"
    env = member.get("env", [])
    if isinstance(env, dict):
        if any(not isinstance(key, str) or not key or not isinstance(value, (str, int))
               or isinstance(value, bool) for key, value in env.items()):
            return "env as a mapping needs string keys and string or whole-number values"
    elif not isinstance(env, list) or any(not isinstance(item, str) for item in env):
        return "env must be a list of K=V strings or a mapping"
    if "cwd" in member and not (isinstance(member["cwd"], str) and member["cwd"]):
        return "cwd must be a nonempty string"
    return None


def load(path: Path) -> dict:
    manifest = json.loads(path.read_text())
    if isinstance(manifest, list):
        manifest = {"members": manifest}
    members = manifest.get("members") if isinstance(manifest, dict) else None
    if not isinstance(members, list) or not 2 <= len(members) <= _gang.MAX_MEMBERS:
        raise SystemExit(f"pbgang: manifest needs 2..{_gang.MAX_MEMBERS} members")
    unknown = set(manifest) - {"members", "priority", "priority_reason", "skew_s", "timeout_s"}
    if "priority_reason" in manifest and not isinstance(manifest["priority_reason"], str):
        raise SystemExit("pbgang: manifest priority_reason must be a string")
    if unknown:
        raise SystemExit(f"pbgang: unknown manifest fields {sorted(unknown)}")
    for index, member in enumerate(members):
        if not isinstance(member, dict):
            raise SystemExit(f"pbgang: member {index} must be an object")
        if set(member) - MEMBER_FIELDS:
            raise SystemExit(f"pbgang: member {index} has {sorted(set(member) - MEMBER_FIELDS)} "
                             f"outside the allowed fields {sorted(MEMBER_FIELDS)}")
        problem = _shape_problem(member)
        if problem:
            raise SystemExit(f"pbgang: member {index} {problem}; allowed fields "
                             f"{sorted(MEMBER_FIELDS)}")
        for name in sorted(set(member) & set(FLAG_FIELDS)):
            problem = _field_problem(name, member[name])
            if problem:
                raise SystemExit(f"pbgang: member {index} field {name!r} {problem}")
    return manifest


def member_command(args, manifest: dict, member: dict, *, group: str, index: int) -> list[str]:
    size = len(manifest["members"])
    command = [sys.executable, str(HERE / "pbrun.py"),
               "--cwd", str(member.get("cwd") or args.cwd), "--detach"]
    for tag in member.get("tags") or [member["tag"]]:
        command += ["--tag", tag]
    command += ["--gang-group", group, "--gang-size", str(size), "--gang-index", str(index),
                "--priority", str(member.get("priority", manifest.get("priority", 0)))]
    timeout = member.get("timeout_s", manifest.get("timeout_s"))
    if timeout is not None:
        command += ["--timeout-s", str(timeout)]
    if "priority_reason" not in member and manifest.get("priority_reason") is not None:
        command += ["--priority-reason", str(manifest["priority_reason"])]
    demand = member.get("demand")
    if isinstance(demand, dict):
        demand = ",".join(f"{key}={value}" for key, value in demand.items())
    if demand:
        command += ["--demand", str(demand)]
    if member.get("cpus") is not None:
        command += ["--cpus", str(member["cpus"])]
    env = member.get("env", [])
    entries = [f"{key}={value}" for key, value in env.items()] if isinstance(env, dict) else env
    for entry in entries:
        command += ["--env", str(entry)]
    for name, (flag, kind) in FLAG_FIELDS.items():
        if name not in member:
            continue
        value = member[name]
        if kind == "switch":
            if value:
                command.append(flag)
        elif kind == "repeat":
            for entry in value:
                command += [flag, str(entry)]
        else:
            command += [flag, str(value)]
    return [*command, "--", *map(str, member["argv"])]


def withdraw(keys: list[str], reason: str) -> None:
    for key in keys:
        subprocess.run([sys.executable, str(HERE / "pbrun.py"), "--withdraw", key,
                        "--reason", reason], check=False)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--manifest", type=Path, required=True,
                    help="JSON manifest naming every gang member (see the module docstring)")
    ap.add_argument("--cwd", type=Path, default=None,
                    help="checkout every member snapshots, unless the member names its own cwd")
    ap.add_argument("--queue", type=Path, default=SH / "pb-queue",
                    help="queue root where pbgang reads the published member rows and files the group record; must be the queue pbrun publishes to")
    args = ap.parse_args(argv)
    manifest = load(args.manifest)
    if args.cwd is None and any("cwd" not in member for member in manifest["members"]):
        ap.error("--cwd is required unless every member names its own cwd")
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

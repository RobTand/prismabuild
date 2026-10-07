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

A member may declare the bytes it reads and ask for them staged first::

    {"tag": "sparky", "demand": "gpu=1,mem_gb=100",
     "data_manifest": "/data/window4/manifest.json", "residency": "stage",
     "residency_ram": "auto", "residency_share": "auto",
     "argv": ["/home/rob/venvs/x/bin/python", "worker.py"]}

``tag`` is the host (or host class) a member is placed on; members land on
distinct hosts. ``timeout_s`` and ``priority`` apply to every member unless a
member overrides them. Prints one JSON line: the group and its member keys.

The manifest may also be a bare list of members. A member names its host with
``tag`` or with ``tags`` (a list), may give ``demand`` as ``gpu=1,mem_gb=100`` or
as a mapping, and ``env`` as ``K=V`` strings or as a mapping.

A member carries the existing ``pbrun`` options a measurement window needs, each
as the one flag of that name: ``gpu_memory_gb``, ``exclusive`` and ``measurement``
(true or false), ``host_class``, ``container_images`` (a list, one flag per image),
``priority_reason``, ``max_attempts`` (only 1), and the data-manifest and
residency options ``data_manifest``, ``residency``, ``residency_tier``,
``residency_ram``, ``residency_share``, ``residency_mover_mem_gb``,
``residency_mover_readers``, ``residency_prefetch_depth_gib``,
``residency_read_mb_s`` and ``residency_mover_max_attempts``. ``pbrun`` judges
every value exactly as it would for a plain submission: it refuses
``--residency stage`` without ``--data-manifest``, and it enforces the
enumerated values of ``--residency``, ``--residency-ram`` and
``--residency-share``. Two things ``pbgang`` itself refuses, because the
submission process, not ``pbrun``, decides them: a ``data_manifest`` must be
an absolute path (~ and $VAR are not expanded; ``pbrun`` reads it against
its own working directory -- the directory ``pbgang`` runs in, not the
member's ``cwd``, which is the checkout every member snapshots -- so a
relative name would ingest a different file of the same name), and a member
that declares ``data_manifest`` must also declare ``residency`` (``stage``):
the #1247 manifest planner files one row's plan per
tier-loop cycle, so a member left to it would hold its gang -- and its elected
siblings' hosts -- fenced while it reads the pool unplanned.  A third check
runs after every member is published: members with declared
prelaunch-resident prefixes (#1594) must jointly fit each stage tier's
minted capacity, with shared ranges counted once.  Each member submission
already refused an oversize prefix.  No member can see the joint sum
alone.  Any other key is
refused by name:
the gang flags, ``tag``, ``priority`` and ``retry_safe`` are the driver's (a
retry ends the gang, so a member gets one attempt). ``--cwd`` is the
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
import os
import secrets
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve(strict=True).parent
sys.path.insert(0, str(HERE))
from runtime_paths import generation_root  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import _gang, core, pool, residency_plan, storage_tiers  # noqa: E402

SH = Path("/mnt/shared/prismabuild-fleet")
SCHEMA = "prismabuild.pbgang.v1"
MEMBER_FIELDS = {"tag", "tags", "cwd", "argv", "demand", "env", "timeout_s", "priority", "cpus"}

#: Member fields that are one ``pbrun`` flag each: exactly the existing options a
#: measurement window declares (#1517), plus the data-manifest and residency
#: options (#583, #909, #1026) a gang member may declare like any other
#: submission.  The kind says
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
    "data_manifest": ("--data-manifest", "value"),
    "residency": ("--residency", "value"),
    "residency_tier": ("--residency-tier", "value"),
    "residency_ram": ("--residency-ram", "value"),
    "residency_share": ("--residency-share", "value"),
    "residency_mover_mem_gb": ("--residency-mover-mem-gb", "value"),
    "residency_mover_readers": ("--residency-mover-readers", "value"),
    "residency_prefetch_depth_gib": ("--residency-prefetch-depth-gib", "value"),
    "residency_read_mb_s": ("--residency-read-mb-s", "value"),
    "residency_mover_max_attempts": ("--residency-mover-max-attempts", "value"),
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
    if name == "data_manifest" and isinstance(value, str) and not os.path.isabs(value):
        # Not a style rule: pbrun reads the manifest against its own working
        # directory -- the directory pbgang runs in -- so a relative name
        # would ingest a different file of the same name.
        return "must be an absolute path (~ and $VAR are not expanded)"
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
    if "data_manifest" in member and member.get("residency") != "stage":
        # The #1247 planner files one row's plan per tier-loop cycle; a member
        # left to it -- anything but an explicit ``stage``, including pbrun's
        # default ``none`` -- holds the gang uncommitted, its elected siblings'
        # hosts fenced, while it would read the pool at full cost.
        return ("declares data_manifest without residency; set residency: stage")


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


def gang_prelaunch_refusal(queue, keys: list[str]) -> str | None:
    """Why this gang's declared prelaunch prefixes do not fit, or ``None``.

    Each stage tier (#1594) must fit the member distinct movers.  Shared
    ranges count once through ``share_namespace``.  Each member submission
    already refused an oversize prefix.  This refuses only the joint sum.
    Members with no filed plan add nothing.  Unknown capacity never
    refuses.
    """

    plans = []
    for key in keys:
        try:
            plan = residency_plan.read(queue, key)
        except (OSError, ValueError):
            plan = None
        if isinstance(plan, dict):
            plans.append(plan)
    if not any(residency_plan.prelaunch_phase_names(plan) for plan in plans):
        return None
    tiers: dict[str, int] = {}
    try:
        announced = queue.tiers()
    except (OSError, ValueError):
        announced = []
    for plan in plans:
        tier_id = plan.get("tier_id")
        if not isinstance(tier_id, str) or tier_id in tiers:
            continue
        records = [record for record in announced
                   if isinstance(record, dict)
                   and str(record.get("tier_id")) == tier_id
                   and not record.get("retired")]
        if len(records) != 1:
            continue
        try:
            minted = storage_tiers.minted_tokens(records[0]).get(
                storage_tiers.capacity_kind_of(tier_id))
        except (ValueError, KeyError, TypeError):
            continue
        if isinstance(minted, int) and not isinstance(minted, bool):
            tiers[tier_id] = minted
    demand = residency_plan.gang_prelaunch_demand(plans, tiers)
    over = {tier_id: entry for tier_id, entry in demand.items()
            if entry["over_capacity"]}
    if not over:
        return None
    rendered = []
    for tier_id in sorted(over):
        entry = over[tier_id]
        rendered.append(
            f"stage tier {tier_id}: members {entry['members']} need joint "
            f"peak {entry['peak_gib']} GiB (retained "
            f"{entry['retained_gib']} GiB + suffix "
            f"{entry['suffix_gib']} GiB), above the tier's minted "
            f"capacity of {entry['capacity_gib']} GiB")
    return "pbgang: declared prelaunch prefixes do not fit: " + "; ".join(rendered)



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
    # A class-scoped GPU measurement seals its class facts from a vetted
    # packet when the submitting box has no accelerator (#1598).  Only a
    # member that declares both a measurement and a host class can use it, so
    # only those members carry the flag; ``pbrun`` judges the packet.
    evidence = getattr(args, "target_evidence", None)
    if evidence is not None and member.get("measurement") and member.get("host_class"):
        command += ["--target-evidence", str(evidence)]
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
    ap.add_argument("--target-evidence", type=Path, default=None, metavar="PATH",
                    help="absolute path of an evidence packet (tools/fleet/pbevidence.py "
                         "prints one on a worker of the class); each member that declares "
                         "a measurement and a host_class seals its class facts from it "
                         "instead of probing this box (#1598). The manifest does not change")
    args = ap.parse_args(argv)
    manifest = load(args.manifest)
    if args.cwd is None and any("cwd" not in member for member in manifest["members"]):
        ap.error("--cwd is required unless every member names its own cwd")
    if args.target_evidence is not None:
        if not args.target_evidence.is_absolute():
            ap.error("--target-evidence must be an absolute path: pbrun reads a relative "
                     "one against its own working directory")
        if not any(member.get("measurement") and member.get("host_class")
                   for member in manifest["members"]):
            ap.error("--target-evidence needs a member that declares measurement and "
                     "host_class")
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
    refusal = gang_prelaunch_refusal(queue, keys)
    if refusal is not None:
        withdraw(keys, refusal)
        print(f"{refusal}; withdrew {len(keys)} published member(s)", file=sys.stderr)
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

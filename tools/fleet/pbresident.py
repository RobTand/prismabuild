#!/usr/bin/env python3
"""Publish, inspect and release whole-directory resident sets."""
import argparse
from datetime import datetime, timezone
import getpass
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from runtime_paths import generation_root
sys.path.insert(0, str(generation_root(__file__) / "src"))
from prismabuild import core, resident_sets


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("lease dates must include a timezone")
    return parsed.astimezone(timezone.utc).timestamp()


def movement_template(set_id, checkout):
    import pbrun
    return pbrun.freeze_action_template(
        command=["pbresident", "publish", set_id], cwd=Path(checkout).resolve(),
        logical_cwd=str(Path(checkout).resolve()), demand={"cpu": 1, "mem_gb": 1},
        placement={"required_tags": []}, variables={"PATH": "/usr/bin:/bin"},
        determinism="stochastic", retry_policy={"max_attempts": 3, "retry_safe": True},
        host_class=None, measurement=False, transport="pool", pool_measurement_class=False,
        data_manifest_path=None, checkout_snapshot_max_bytes=pbrun.CHECKOUT_SNAPSHOT_MAX_BYTES,
        snapshot_refs=(), exclusive=False, gpu_memory_gb=None, execution_timeout_s=None,
        progress=None, profile=None)


def submit_copies(store, set_id, *, policy_path, checkout):
    """Freeze through the existing submitter, then use its movement builder."""
    from prismabuild import local_resident, pool
    queue = pool.PoolQueue(store.queue_root)
    record = store.read(set_id)
    tiers = queue.tiers()
    by_host = {row["host"]: row for row in tiers if row.get("tier_id", "").startswith("local:")}
    for host in record["hosts"]:
        if host not in by_host:
            raise ValueError(f"local tier has not announced its movement tools on {host}")
    template = movement_template(set_id, checkout)
    return local_resident.publish_actions(template, store, set_id, tiers, policy_path=policy_path)


def submit_adoption(store, set_id, *, host, source, policy_path, checkout):
    from prismabuild import local_resident, pool
    queue = pool.PoolQueue(store.queue_root)
    tiers = {row["host"]: row for row in queue.tiers() if row.get("tier_id", "").startswith("local:")}
    if host not in store.read(set_id)["hosts"] or host not in tiers:
        raise ValueError("adoption host must be declared and announce its local tier")
    template = movement_template(set_id, checkout)
    action = local_resident.movement_action(template, store, set_id, tiers[host], policy_path=policy_path, operation="adopt", source=source)
    template["cas"].publish_action_request(action)
    row = local_resident.movement_row(action, template["cas"], tiers[host])
    with store.lock(set_id):
        current = store.read_copy(set_id, host)
        store.write_copy(set_id, host, {**current, "adoption_row": row})
    queue.publish(**row, recompute=True, refuse_if_live=True)
    return row




def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pool-root", required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    publish = commands.add_parser("publish")
    publish.add_argument("--manifest", required=True)
    publish.add_argument("--canonical-root", required=True)
    publish.add_argument("--hosts", required=True)
    lease = publish.add_mutually_exclusive_group(required=True)
    lease.add_argument("--lease-until", type=timestamp)
    lease.add_argument("--campaign")
    publish.add_argument("--hard-max", required=True, type=timestamp)
    publish.add_argument("--created-by", default=getpass.getuser())
    publish.add_argument("--checkout", default=str(Path.cwd()))
    publish.add_argument("--policy", default=str(Path(__file__).with_name("local_tier_policy.json")))
    status = commands.add_parser("status")
    status.add_argument("set_id", nargs="?")
    release = commands.add_parser("release")
    release.add_argument("set_id")
    release.add_argument("--by", default=getpass.getuser())
    adoption = commands.add_parser("adopt")
    adoption.add_argument("set_id")
    adoption.add_argument("--host", required=True)
    adoption.add_argument("--source", required=True)
    adoption.add_argument("--checkout", default=str(Path.cwd()))
    adoption.add_argument("--policy", default=str(Path(__file__).with_name("local_tier_policy.json")))
    args = parser.parse_args(argv)
    store = resident_sets.ResidentSets(args.pool_root)
    try:
        if args.command == "publish":
            manifest, _ = core.read_data_manifest(args.manifest)
            lease = {"hard_max": args.hard_max}
            lease.update({"campaign": args.campaign} if args.campaign else {"until": args.lease_until})
            result = store.publish(manifest=manifest, canonical_root=args.canonical_root,
                hosts=args.hosts.split(","), lease=lease, created_by=args.created_by)
            result["movements"] = submit_copies(store, result["set_id"], policy_path=args.policy, checkout=args.checkout)
        elif args.command == "status":
            result = store.status(args.set_id)
        elif args.command == "adopt":
            result = submit_adoption(store, args.set_id, host=args.host, source=args.source, policy_path=args.policy, checkout=args.checkout)
        else:
            result = store.release(args.set_id, by=args.by)
    except (ValueError, OSError, core.ActionContractError) as exc:
        print(f"pbresident: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

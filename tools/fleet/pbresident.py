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
    status = commands.add_parser("status")
    status.add_argument("set_id", nargs="?")
    release = commands.add_parser("release")
    release.add_argument("set_id")
    release.add_argument("--by", default=getpass.getuser())
    args = parser.parse_args(argv)
    store = resident_sets.ResidentSets(args.pool_root)
    try:
        if args.command == "publish":
            manifest, _ = core.read_data_manifest(args.manifest)
            lease = {"hard_max": args.hard_max}
            lease.update({"campaign": args.campaign} if args.campaign else {"until": args.lease_until})
            result = store.publish(manifest=manifest, canonical_root=args.canonical_root,
                hosts=args.hosts.split(","), lease=lease, created_by=args.created_by)
        elif args.command == "status":
            result = store.status(args.set_id)
        else:
            result = store.release(args.set_id, by=args.by)
    except (ValueError, OSError, core.ActionContractError) as exc:
        print(f"pbresident: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

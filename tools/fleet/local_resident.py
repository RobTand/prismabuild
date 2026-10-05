#!/usr/bin/env python3
"""The admitted host-local resident movement payload (copy or egress)."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from runtime_paths import generation_root
sys.path.insert(0, str(generation_root(__file__) / "src"))
from prismabuild import local_resident, reader_lease, resident_sets


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pool-root", required=True, help="PrismaBuild queue root (the pool directory) that holds the resident set records.")
    parser.add_argument("--set-id", required=True, help="Resident set id (the sha256 of its manifest) to act on.")
    parser.add_argument("--host", required=True, help="Host that owns the local copy; this action is pinned to it.")
    parser.add_argument("--policy", required=True, help="local_tier_policy.json: each host's local root, maximum GiB, floor fraction and docker allowance.")
    parser.add_argument("--operation", choices=("copy", "evict", "adopt"), required=True,
                        help="copy: fetch and verify the set from its canonical source; evict: remove the local copy once nothing pins it; adopt: take over an existing verified directory.")
    parser.add_argument("--source", help="Directory to adopt; required for --operation adopt, unused otherwise.")
    args = parser.parse_args(argv)
    store = resident_sets.ResidentSets(args.pool_root)
    spec = resident_sets.read_policy(args.policy)["hosts"][args.host]
    if args.operation == "copy":
        result = local_resident.copy(store, args.set_id, args.host, spec,
            reader_context=reader_lease.injected_context())
    elif args.operation == "adopt":
        if args.source is None:
            parser.error("adoption requires --source")
        result = local_resident.adopt(store, args.set_id, args.host, spec, args.source)
    else:
        result = local_resident.evict(store, args.set_id, args.host, spec)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

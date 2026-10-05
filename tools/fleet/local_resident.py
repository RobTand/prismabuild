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
    parser.add_argument("--pool-root", required=True)
    parser.add_argument("--set-id", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--operation", choices=("copy", "evict"), required=True)
    args = parser.parse_args(argv)
    store = resident_sets.ResidentSets(args.pool_root)
    spec = resident_sets.read_policy(args.policy)["hosts"][args.host]
    if args.operation == "copy":
        result = local_resident.copy(store, args.set_id, args.host, spec,
            reader_context=reader_lease.injected_context())
    else:
        result = local_resident.evict(store, args.set_id, args.host, spec)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

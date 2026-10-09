#!/usr/bin/env python3
"""The admitted host-local resident movement payload (copy or egress)."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from runtime_paths import generation_root
sys.path.insert(0, str(generation_root(__file__) / "src"))
from prismabuild import core, local_resident, reader_lease, resident_sets


def main(argv=None):
    parser = local_resident.build_parser(__doc__)
    args = parser.parse_args(argv)
    store = resident_sets.ResidentSets(args.pool_root)
    spec = resident_sets.read_policy(args.policy)["hosts"][args.host]
    if args.operation == "copy":
        result = local_resident.copy(store, args.set_id, args.host, spec,
            reader_context=reader_lease.injected_context())
    elif args.operation == "adopt":
        if args.source is None:
            parser.error("adoption requires --source")
        result = local_resident.adopt_resident_copy(store, args.set_id, args.host, spec, args.source)
    else:
        result = local_resident.evict_resident_copy(store, args.set_id, args.host, spec)
    print(core.sorted_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

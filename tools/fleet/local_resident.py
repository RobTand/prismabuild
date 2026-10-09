#!/usr/bin/env python3
"""The admitted host-local resident movement payload (copy or egress)."""
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from runtime_paths import generation_root
sys.path.insert(0, str(generation_root(__file__) / "src"))
from prismabuild import core, local_resident, reader_lease, resident_sets


OPERATIONS = ("copy", "evict", "adopt")


def effective_operation(argv):
    """The operation main runs for argv, or None when it refuses.

    The one parsing contract the tool and any reader of a sealed
    local_resident command share (#1579, review 3): exactly what argparse
    resolves, including a repeated --operation (last wins), the
    --operation=value form and unambiguous prefix spellings. A duplicate or
    ambiguous form is not its own verdict here; main runs the last resolved
    value or refuses, and a caller that needs a stricter shape checks that
    itself. Anything argparse refuses is None.
    """
    if not isinstance(argv, list) or not all(isinstance(part, str) for part in argv):
        return None
    import io
    import contextlib
    parser = _parser()
    try:
        with contextlib.redirect_stderr(io.StringIO()):
            args, _ = parser.parse_known_args(argv)
    except SystemExit:
        return None
    operation = getattr(args, "operation", None)
    return operation if operation in OPERATIONS else None


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pool-root", required=True, help="PrismaBuild queue root (the pool directory) that holds the resident set records.")
    parser.add_argument("--set-id", required=True, help="Resident set id (the sha256 of its manifest) to act on.")
    parser.add_argument("--host", required=True, help="Host that owns the local copy; this action is pinned to it.")
    parser.add_argument("--policy", required=True, help="local_tier_policy.json: each host's local root, maximum GiB, floor fraction and docker allowance.")
    parser.add_argument("--operation", choices=OPERATIONS, required=True,
                        help="copy: fetch and verify the set; evict: remove the local copy only after its lease ends and no pins remain; adopt: take over an existing verified directory.")
    parser.add_argument("--source", help="Directory to adopt; required for --operation adopt, unused otherwise.")
    return parser


def main(argv=None):
    parser = _parser()
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

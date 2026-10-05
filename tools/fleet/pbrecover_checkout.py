#!/usr/bin/env python3
"""Plan, or explicitly apply RootGO to, at most 32 banked execution checkouts.

This command does not bank trees, stop workers, acquire/release maintenance,
install privileged source, or discover candidates. Keep the host quiescent.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / 'src'))
from prismabuild import core as pb, pool, checkout_recovery as recovery  # noqa: E402


class _JSONParser(argparse.ArgumentParser):
    def error(self, message):
        print(pb._sorted_json_bytes(dict(schema=recovery.RESULT_SCHEMA, complete=False,
                                        status="refused", errors=[message], removed=[])).decode("utf-8"))
        raise SystemExit(2)


def _pbrecover_checkout_read(path: Path):
    return pb._decode_strict_json(pb._read_regular_file_nofollow(
        path.absolute(), where='checkout recovery CLI JSON',
        max_bytes=recovery.MAX_JSON_BYTES), where='checkout recovery CLI JSON')


def main() -> int:
    parser = _JSONParser(description=__doc__)
    parser.add_argument('--queue-root', required=True, type=Path,
                        help='existing absolute queue root (never created)')
    parser.add_argument('--bank-root', required=True, type=Path,
                        help='root of the bank holding the archived checkouts the plan names')
    parser.add_argument('--maintenance-owner', required=True,
                        help='explicit bounded name of the maintenance hold owner; apply must match the plan')
    parser.add_argument('--candidates', type=Path, help='explicit JSON selection list; plan only')
    parser.add_argument('--apply', action='store_true',
                        help='apply a saved plan as root (needs --plan and --plan-sha256); without it only plan')
    parser.add_argument('--plan', type=Path, help='exact JSON plan saved from plan output')
    parser.add_argument('--plan-sha256', help='Core canonical SHA256 authorized by RootGO')
    args = parser.parse_args()
    if args.apply:
        if args.candidates is not None or args.plan is None or args.plan_sha256 is None:
            parser.error('--apply requires --plan and --plan-sha256, without --candidates')
    elif args.candidates is None or args.plan is not None or args.plan_sha256 is not None:
        parser.error('planning requires --candidates, without apply/plan/SHA flags')
    try:
        # This no-follow check precedes PoolQueue construction and all locks.
        recovery._directory(recovery._checkout_recovery_path(args.queue_root, "queue root"), "existing queue root")
        queue = pool.PoolQueue(args.queue_root)
        if args.apply:
            plan = _pbrecover_checkout_read(args.plan)
            result = recovery.apply_checkout_recovery(queue, plan,
                expected_plan_sha256=args.plan_sha256, bank_root=args.bank_root,
                maintenance_owner=args.maintenance_owner)
        else:
            plan = recovery.prepare_checkout_recovery(queue, _pbrecover_checkout_read(args.candidates),
                bank_root=args.bank_root, maintenance_owner=args.maintenance_owner)
            if plan.get('complete') is True:
                result = dict(schema=recovery.RESULT_SCHEMA, complete=True, status='planned',
                              plan=plan, plan_sha256=pb.canonical_sha256(plan),
                              errors=[], removed=[])
            else:
                result = plan
    except Exception as exc:  # CLI emits refusal JSON even for unexpected read failures.
        result = recovery._failure(exc)
    print(pb._sorted_json_bytes(result, allow_nan=False).decode("utf-8"))
    return 0 if result.get('complete') is True else 2


if __name__ == '__main__':
    raise SystemExit(main())

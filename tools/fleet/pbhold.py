#!/usr/bin/env python3
"""Set, release or list operator holds on new claims. Running actions continue."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys

_ENTRY = Path(__file__).resolve(strict=True)
sys.path.insert(0, str(_ENTRY.parents[2 if _ENTRY.parent.name == "fleet" else 1] / "src"))
from prismabuild import host_hold
from prismabuild.digest_primitives import sorted_json


def hold_main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue-root", type=Path,
                        default=Path("/mnt/shared/prismabuild-fleet/pb-queue"),
                        help="shared PB queue root")
    parser.add_argument("--host", help="host whose new claims should stop")
    parser.add_argument("--owner", help="operator identity setting or releasing the hold")
    parser.add_argument("--reason", help="why claims are held, or why a forced override is needed")
    parser.add_argument("--until", help="expiry in ISO 8601 UTC, at most 12 hours ahead")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--release", action="store_true", help="release this owner's hold")
    mode.add_argument("--list", action="store_true", help="list active, expired and unreadable holds")
    parser.add_argument("--force", action="store_true", help="override ownership, requires --reason")
    args = parser.parse_args(argv)
    try:
        if args.list:
            if args.until or args.owner or args.reason or args.force:
                parser.error("--list accepts only --host and --queue-root")
            holds = host_hold.list_host_holds(args.queue_root)
            print(sorted_json({host: {**hold.fields(), "expired": hold.expired}
                               for host, hold in holds.items()
                               if args.host is None or args.host == host}))
            return 0
        if not args.host or not args.owner:
            parser.error("--host and --owner are required")
        expires = None
        if args.release:
            if args.until:
                parser.error("--release does not accept --until")
        else:
            if not args.until:
                parser.error("--until is required")
            until = datetime.fromisoformat(args.until.replace("Z", "+00:00"))
            if until.tzinfo is None or until.utcoffset() != timezone.utc.utcoffset(until):
                parser.error("--until must name UTC explicitly")
            expires = until.timestamp()
        hold = host_hold.change_host_hold(args.queue_root, args.host, owner=args.owner,
            reason=args.reason, expires_unix=expires, release=args.release, force=args.force)
        print(sorted_json({"host": args.host, "state": "released" if hold is None else "held",
                           "hold": hold.fields() if hold else None,
                           "override_reason": args.reason if args.force else None}))
        return 0
    except (OSError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(hold_main())

"""Compare this worker's selected host inventories with a sealed root profile."""
import argparse
import json
from pathlib import Path
import socket
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests'))
from proof_container_runtime import CAPTURE_BOUNDS, inventory, load_profile


def proof_runtime_host_drift(roots):
    changes, errors = [], []
    budget = dict(members=0, regular_bytes=0,
                  deadline=time.monotonic() + CAPTURE_BOUNDS['runtime_seconds'])
    for root in roots:
        if root['origin'] != 'host':
            continue
        try:
            actual = inventory(root['path'], budget=budget)
        except (OSError, ValueError, TimeoutError) as error:
            errors.append(dict(path=root['path'], error=str(error)))
            continue
        expected = root['entries']
        for relative in sorted(expected.keys() | actual.keys()):
            before, after = expected.get(relative), actual.get(relative)
            if before != after:
                changes.append(dict(path=str(Path(root['path']) / relative),
                                    before=before, after=after))
    return dict(host=socket.gethostname(), changes=changes, errors=errors,
                matches=not changes and not errors, qualification=False)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-profile', required=True, type=Path,
                        help='sealed native profile to compare with this worker')
    args = parser.parse_args(argv)
    try:
        profile = load_profile(args.runtime_profile, ROOT)
        report = proof_runtime_host_drift(profile.closure['roots'])
        report['profile_sha256'] = profile.sha256
    except (OSError, ValueError) as error:
        print(json.dumps(dict(host=socket.gethostname(), matches=False,
                              qualification=False, errors=[str(error)])), flush=True)
        return 2
    print(json.dumps(report), flush=True)
    return 2 if report['errors'] else 0 if report['matches'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

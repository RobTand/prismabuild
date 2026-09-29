#!/usr/bin/env python3
"""Run the fleet canary for any active generation that has no verdict (#978).

A systemd timer runs this.  It reads the active generation through the
``repo`` link, looks for the ``<generation>.canary.json`` sidecar the publisher
writes, and runs ``pbcanary.py`` when there is none.  That covers the case the
publisher's own hook cannot: a generation staged in PrismaBuild and activated
outside it never passes through ``publish_runtime``'s canary step.

``--nightly`` runs the canary whether or not a verdict exists, so a fleet
regression that lands without a new publication is still seen.

Exit status follows ``pbcanary.py``: 0 verified, 1 a leg failed, 2 the canary
could not run.  A generation that already has a verdict and no ``--nightly``
exits 0 without running anything.

This tool runs from a checkout under a timer.  It is not published to workers.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Callable, Sequence

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

DEFAULT_REPO_LINK = "/mnt/shared/prismabuild-fleet/repo"

Runner = Callable[[str, Sequence[str]], int]


def _default_runner(checkout: Path) -> Runner:
    driver = checkout / "tools" / "fleet" / "pbcanary.py"

    def run(generation: str, extra: Sequence[str]) -> int:
        return subprocess.run(
            [sys.executable, str(driver), "--generation", generation, *extra],
            check=False,
        ).returncode

    return run


def _generation_commit(generation_dir: Path) -> str | None:
    try:
        return json.loads((generation_dir / "RUNTIME_VERSION.json").read_text())["commit"]
    except (OSError, ValueError, KeyError, TypeError):
        return None


def main(
    argv: Sequence[str] | None = None,
    *,
    runner: Runner | None = None,
    now: float | None = None,
) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo-link", default=DEFAULT_REPO_LINK,
                        help="the live generation link (default: %(default)s)")
    parser.add_argument("--checkout", type=Path, default=HERE.parent.parent,
                        help="checkout holding tools/fleet/pbcanary.py")
    parser.add_argument("--nightly", action="store_true",
                        help="run even when a verdict exists")
    args, extra = parser.parse_known_args(argv)

    import pbstatus
    import publish_runtime

    summary = pbstatus.read_canary_summary(args.repo_link, now=now)
    generation = summary.get("generation")
    if generation is None:
        print(f"pbcanary_watch: {args.repo_link} does not resolve to a "
              f"generation: {summary.get('error')}", file=sys.stderr)
        return 2
    if summary["state"] not in pbstatus.CANARY_NEEDS_RUN and not args.nightly:
        print(f"pbcanary_watch: generation {generation} already has a "
              f"canary verdict ({summary['state']})")
        return 0

    reason = "nightly run" if summary["state"] not in pbstatus.CANARY_NEEDS_RUN \
        else f"no verdict ({summary['state']})"
    record = Path(summary["record"])
    generation_dir = Path(args.repo_link).resolve()
    commit = _generation_commit(generation_dir)
    shape_gate = {"source": "pbcanary_watch", "reason": reason}

    def write(status: str, code: int | None, detail: str) -> None:
        publish_runtime._write_canary_record(
            record, generation=generation, commit=commit, status=status,
            exit_code=code, detail=detail, shape_gate=shape_gate,
        )

    write("pending", None, f"pbcanary_watch started: {reason}")
    run = runner or _default_runner(args.checkout)
    try:
        code = run(generation, extra)
    except Exception as exc:  # the record must not stay pending for a crash
        write("failed", None, f"pbcanary_watch: {type(exc).__name__}: {exc}")
        raise
    if code == 0:
        write("verified", 0, "pbcanary_watch: every leg verified")
    elif code == 2:
        write("not_run", 2, "pbcanary_watch: the canary refused to run")
    else:
        write("failed", code, f"pbcanary_watch: pbcanary exited {code}")
    return code


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Print the live platform and accelerator facts of this box as one packet (#1598).

A class-scoped GPU measurement seals the platform and accelerator facts of its
class.  ``pbrun`` read them from the box that submits, so a box without an
accelerator could not submit one.  Run this tool as a normal PrismaBuild action
on a worker of the class.  It prints the packet that ``pbrun --target-evidence``
reads.

The packet is the evidence the worker preflight already collects, in the shape
the core already validates.  It names the host and the device UUIDs as
provenance.  The sealed identity does not contain them.  Each worker still
checks the declared facts against its own live facts before it runs, so a wrong
packet fails closed.

    pbevidence.py [--out PATH]

With ``--out`` the packet goes to that file in one atomic step.  Otherwise it
goes to stdout.  The tool exits 1 and writes nothing when this box cannot
attest an accelerator.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve(strict=True).parent
sys.path.insert(0, str(HERE))
_PB_ENTRYPOINT = Path(__file__).resolve(strict=True)
sys.path.insert(0, str(_PB_ENTRYPOINT.parents[
    2 if _PB_ENTRYPOINT.parent.name == "fleet" else 1] / "src"))
from prismabuild.runtime_paths import generation_root  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import core as pb, immutable_files  # noqa: E402
from prismabuild.movement_actions import SEALED_ARGV0  # noqa: E402


#: The packet also names the identity of the executable a class-scoped action
#: seals as argv[0], as the WORKER sees it.  A class spans boxes whose
#: ``/bin/bash`` differs (the sizes differ between an x86_64 submitter and an
#: aarch64 worker), and the worker refuses a declared size that is not its own,
#: so the submitter must not read this from its own file system (#1598).
from prismabuild.target_evidence import PacketError, collect_packet


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", type=Path, default=None,
                    help="write the packet to this file in one atomic step "
                         "instead of stdout")
    ap.add_argument("--recorder-python", default=None,
                    help="absolute path of the interpreter a scratch recorder "
                         "runs as argv[0] on this box; its identity joins the packet")
    args = ap.parse_args(argv)
    try:
        packet = collect_packet(args.recorder_python)
    except PacketError as exc:
        print(f"pbevidence: {exc}", file=sys.stderr)
        return 1
    if args.out is None:
        sys.stdout.write(pb._sorted_lf_bytes(packet).decode("utf-8"))
    else:
        # The repo's one owner of rename-atomic JSON records (#1330).
        immutable_files.write_json_atomic(args.out, packet, trailing_newline=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

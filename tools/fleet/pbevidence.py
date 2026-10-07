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
from runtime_paths import generation_root  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import core as pb, materialize  # noqa: E402


class PacketError(ValueError):
    """A packet that cannot seal the facts of an accelerator class."""


def vet(evidence: dict) -> dict:
    """The normalized packet, or a ``PacketError`` naming what is wrong.

    This is the class-independent half of the vetting.  ``pbrun`` adds the
    check that the packet agrees with the class it is used for.
    """

    # A SLURM packet is refused for what it is, whatever its job record holds.
    if isinstance(evidence, dict) and (
            evidence.get("source") == "slurm" or evidence.get("slurm") is not None):
        raise PacketError(
            "the packet must be local: a SLURM packet names a job, not a box")
    try:
        packet = pb._normalize_worker_evidence(evidence)
    except pb.ActionContractError as exc:
        raise PacketError(f"it is not a worker evidence packet: {exc}") from None
    if packet["source"] != "local" or packet["slurm"] is not None:
        raise PacketError(
            "the packet must be local: a SLURM packet names a job, not a box")
    accelerators = packet["accelerators"]
    assert isinstance(accelerators, list)
    if not accelerators:
        raise PacketError("the packet reports no accelerator")
    try:
        pb.accelerator_models_contract(packet)
    except pb.ActionContractError:
        raise PacketError(
            "the packet carries no device identity (name and uuid) for an "
            "accelerator") from None
    # Several devices of one model are one class.  Two models are not, even
    # when they share a compute capability and a driver: the sealed model hash
    # would cover a mixed set that no class has.
    if len({str(row["name"]) for row in accelerators}) != 1:
        raise PacketError(
            "the packet must report one accelerator model, one compute "
            "capability and one driver")
    toolchain = pb.live_platform_toolchain_contract(evidence=packet)
    if "cuda_compute_capability" not in toolchain:
        raise PacketError(
            "the packet must report one accelerator model, one compute "
            "capability and one driver")
    return packet


def collect_packet() -> dict:
    """This box's evidence packet, with the device identity attested."""

    try:
        evidence = pb._collect_worker_evidence(attest_accelerator_identity=True)
    except pb.ActionContractError as exc:
        raise PacketError(f"this box cannot attest its facts: {exc}") from None
    return vet(evidence)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", type=Path, default=None,
                    help="write the packet to this file in one atomic step "
                         "instead of stdout")
    args = ap.parse_args(argv)
    try:
        packet = collect_packet()
    except PacketError as exc:
        print(f"pbevidence: {exc}", file=sys.stderr)
        return 1
    if args.out is None:
        sys.stdout.write(pb._sorted_lf_bytes(packet).decode("utf-8"))
    else:
        # The repo's one owner of rename-atomic JSON records (#1330).
        materialize._write_json_atomic(args.out, packet, trailing_newline=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

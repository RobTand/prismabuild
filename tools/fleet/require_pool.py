#!/usr/bin/env python3
"""Refuse GPU work that does not go through the PrismaBuild pool.

Briefs are discipline, not enforcement, and discipline is what failed: the
same briefs that said "use the pool" produced fifteen agents recomputing one
baseline and a flock jam seven deep.  This hook moves the rule from prose an
agent may skim to a refusal it cannot.

It is deliberately narrow.  Read-only inspection (``nvidia-smi``) is allowed,
because refusing it would only teach agents to route around the hook.  What is
refused is the two things that actually contend for the GPU: the CUDA venv
interpreter, and the box-local flock wrappers the pool replaces.

Staged rather than flipped: enforcement requires the flag file to exist, so
turning it on is one ``touch`` and does not edit config under running agents.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

FLAG = Path("/home/rob/tmp/arb/require_pool.on")
PBRUN = "/mnt/shared/prismabuild-fleet/repo/tools/pbrun.py"

# The CUDA interpreter and the flock wrappers.  Anything else -- nvidia-smi,
# a CPU python, a git command -- is none of this hook's business.
CONTENDS = re.compile(
    r"venvs/prismaquant-cu130/bin/python|/gpuslot\.sh|/gpulock\.sh"
)


def main() -> int:
    if not FLAG.exists():
        return 0
    try:
        event = json.load(sys.stdin)
    except Exception:                                        # noqa: BLE001
        return 0
    command = str((event.get("tool_input") or {}).get("command") or "")
    if not CONTENDS.search(command):
        return 0
    if "pbrun.py" in command:
        return 0                      # already going through the pool
    sys.stderr.write(
        "Refused: GPU work goes through the PrismaBuild pool, not a local "
        "lock.\n\n"
        f"Run it as:\n  /usr/bin/python3 {PBRUN} --gpu -- <your command>\n\n"
        "Flags: --exclusive for a timing run that needs the whole box "
        "(expressed as a demand for its full GPU capacity, which the ledger "
        "turns into exclusion); --anywhere only if your checkout is on "
        "/mnt/shared, otherwise it is pinned to this box automatically.\n\n"
        "Why: a box-local flock cannot balance across sparky and sparklina, "
        "and it reproduced hold-while-gated, starvation and partial-hold "
        "waste that the pool's ledger solves structurally.\n"
    )
    return 2                          # exit 2 blocks and shows this to the agent


if __name__ == "__main__":
    raise SystemExit(main())

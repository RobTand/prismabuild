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

Two carve-outs, and both are the same lesson.  A guard that can refuse its own
repair, or refuse the alternative it names, is worse than no guard: it strands
the person trying to comply.  So commands that never start GPU work are let
through by name, and so is the pool's own machinery -- including starting a
worker, whose ``--python`` argument is the CUDA venv path by design.

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

# The CUDA interpreter, the flock wrappers, and a bare flock on the GPU lock.
# That last one is not hypothetical: the wrappers were only ever a convenience,
# and the jam this hook exists to prevent re-formed from direct `flock
# .../.gpu.lock ...` invocations that named no wrapper at all -- one pytest
# holding an exclusive GPU lock for 73 minutes with a `docker run --gpus all`
# waiting 53 minutes behind it.  Matching the wrappers alone would have left
# the observed path open.
#
# Anything else -- nvidia-smi, a CPU python, a git command -- is none of this
# hook's business.
CONTENDS = re.compile(
    r"venvs/prismaquant-cu130/bin/python"
    r"|/gpuslot\.sh"
    r"|/gpulock\.sh"
    r"|flock\s[^|;]*\.gpu\.lock"
)


#: Commands that never start GPU work, whatever their text contains.  A commit
#: message quoting the refused pattern is prose ABOUT the rule, not an instance
#: of it -- and this hook refused its own commit, then refused the edit that
#: would have fixed that, before this existed.  A guard that can lock out its
#: own repair is a worse failure than the one it guards against.
NEVER_GPU = ("git", "gh", "echo", "cat", "grep", "sed", "awk", "less", "diff")


#: The pool's own machinery, which must never be refused by the hook that
#: exists to route work *into* it.  ``pbrun`` submits; ``worker_loop`` and
#: ``worker`` are the things that consume the queue -- and a worker is
#: configured with ``--python <the CUDA venv>``, so it names the refused
#: pattern by construction.  Refusing that means the hook blocks the pool from
#: being started at all, which is the same class of failure as refusing its own
#: repair: the guard removing the alternative it is pointing at.
POOL_ENTRYPOINTS = ("pbrun.py", "worker_loop.py", "worker.py")


#: Shell operators that end one command and begin another.  A compound command
#: is judged segment by segment, because judging it by its first token is a
#: hole wide enough to drive the whole rule through: ``cat note.txt && <cuda
#: python> -m pytest`` leads with ``cat``, which is exempt, and the pytest
#: behind it ran unrefused.  Found by walking into it while fixing the hook.
SEPARATORS = re.compile(r"&&|\|\||;|\||\n")


def _first_token(command: str) -> str:
    """The command actually being run, past any leading env assignments."""

    stripped = command.lstrip()
    while stripped:
        head = stripped.split(" ", 1)[0]
        if "=" not in head or head.startswith("/"):
            break
        parts = stripped.split(" ", 1)
        if len(parts) == 1:
            return ""
        stripped = parts[1].lstrip()
    return stripped.split(" ", 1)[0].rsplit("/", 1)[-1]


def contends(command: str) -> bool:
    """True when any segment of this command starts GPU work off-pool.

    Each segment carries its own exemption: a leading ``cat`` does not vouch
    for what follows ``&&``, and a refused segment is not excused by a
    permitted neighbour.
    """

    for segment in SEPARATORS.split(command):
        if not CONTENDS.search(segment):
            continue
        if _first_token(segment) in NEVER_GPU:
            continue
        if any(entry in segment for entry in POOL_ENTRYPOINTS):
            continue
        return True
    return False


def main() -> int:
    if not FLAG.exists():
        return 0
    try:
        event = json.load(sys.stdin)
    except Exception:                                        # noqa: BLE001
        return 0
    command = str((event.get("tool_input") or {}).get("command") or "")
    if not contends(command):
        return 0
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

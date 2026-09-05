#!/usr/bin/env python3
"""Refuse GPU work that does not go through the PrismaBuild pool.

Briefs are discipline, not enforcement, and discipline is what failed: the
same briefs that said "use the pool" produced fifteen agents recomputing one
baseline and a flock jam seven deep.  This hook moves the rule from prose an
agent may skim to a refusal it cannot.

It is deliberately narrow.  Read-only inspection (``nvidia-smi``, ``squeue``,
``sinfo``) is allowed, because refusing it would only teach agents to route
around the hook.  What is refused is the things that actually contend for the
GPU: the CUDA venv interpreter, the box-local flock wrappers the pool replaces,
and -- since the fleet has a scheduler -- a bare ``sbatch``, ``srun`` or
``salloc``.

That last one is not a fourth rule, it is the same rule reaching the shape that
defeats its proxy.  The proxy for GPU work is the CUDA interpreter on the
command line, and a submission names no interpreter at all: the venv is inside
the job script, on a node this box never sees.  So the one shape that most
needs to go through the lane was the one shape the hook let through.
``scancel`` is deliberately left alone: cancelling a job is not starting work,
and a guard that refuses the cleanup strands the person complying with it.

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
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import fleet_tool, generation_root  # noqa: E402

_RUNTIME_ROOT = generation_root(__file__)
#: The submitter the refusals below tell an operator to run.  Resolved under
#: both layouts, because a hook running from a checkout would otherwise print
#: the published runtime's flat path, which a checkout does not have.
PBRUN = str(
    fleet_tool("pbrun.py", root=_RUNTIME_ROOT)
    or _RUNTIME_ROOT / "tools" / "pbrun.py"
)

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
NEVER_GPU = (
    "git", "gh", "echo", "cat", "grep", "sed", "awk", "less", "diff",
    # Asking where a program is, what it is, or what it says about itself.
    # ``which sbatch`` and ``man sbatch`` are the first two commands anyone
    # runs at a scheduler they have never used, and refusing them teaches the
    # reader to route around the hook before they have read the rule.
    "which", "whereis", "type", "command", "man", "ls", "stat", "file",
    "head", "tail", "wc", "dpkg", "apt", "apt-get", "apt-cache",
)


#: A segment that switches the GPU off for its own child cannot be GPU work,
#: whatever interpreter it names.  Two agents in one day were refused for
#: ``kl_tool.py --help`` and ``kl_tool.py compare --help`` -- argparse text
#: that starts nothing -- because the rule's proxy is the interpreter path and
#: nothing asked what the command does.  The interpreter stays the proxy: it
#: is the deliberate one, and asking "does this touch CUDA" of an arbitrary
#: command line is exactly the guess this hook refuses to make.  What this
#: adds is a way to *say so*, and the saying is enforced by the kernel rather
#: than believed: with ``CUDA_VISIBLE_DEVICES`` empty the child sees no
#: device, so this cannot become a way around the pool -- work smuggled
#: through it would simply fail.  ``--help`` under that prefix is the use.
NO_DEVICE = re.compile(r"""(?:^|\s)CUDA_VISIBLE_DEVICES=(?:''|""|)(?=\s)""")


#: The pool's own machinery, which must never be refused by the hook that
#: exists to route work *into* it.  ``pbrun`` submits; ``worker_loop`` and
#: ``worker`` are the things that consume the queue -- and a worker is
#: configured with ``--python <the CUDA venv>``, so it names the refused
#: pattern by construction.  Refusing that means the hook blocks the pool from
#: being started at all, which is the same class of failure as refusing its own
#: repair: the guard removing the alternative it is pointing at.
#:
#: ``slurm_job.py`` joins them for both reasons at once: it is what a SLURM job
#: execs on the node, so it *is* the sanctioned path, and it carries the
#: action's own sealed interpreter -- the CUDA venv -- on its command line.
POOL_ENTRYPOINTS = ("pbrun.py", "worker_loop.py", "worker.py", "slurm_job.py")


#: A submission to the scheduler, by any of its three verbs.  Bounded by
#: non-word characters on both sides so that ``/usr/bin/sbatch`` is one and
#: ``my-sbatch-wrapper`` and ``sbatch.sh`` are not -- and so that ``scancel``,
#: ``squeue`` and ``sinfo`` never match: reading the queue and cancelling a job
#: start no work.
SCHEDULER = re.compile(r"(?<![\w.-])(?:sbatch|srun|salloc)(?![\w.-])")


#: The verbs' own help and version switches.  ``sbatch --help`` submits
#: nothing, and it is how a reader of the runbook finds out whether the
#: scheduler is installed at all.  Only a segment that is the verb and these
#: switches and nothing else is let through: ``sbatch --help job.sh`` also
#: submits nothing in practice, but the hook does not parse the verbs' grammar
#: and does not guess.  A bare ``sbatch`` with no switch reads a script from
#: standard input, which is a submission.
DESCRIBES_ITSELF = frozenset({"-h", "--help", "--usage", "-V", "--version"})


def _describes_itself(segment: str) -> bool:
    """True when this segment only asks a scheduler verb about itself."""

    tokens = segment.split()
    while tokens and "=" in tokens[0] and not tokens[0].startswith("/"):
        tokens.pop(0)
    if len(tokens) < 2:
        return False
    if tokens[0].rsplit("/", 1)[-1] not in ("sbatch", "srun", "salloc"):
        return False
    return set(tokens[1:]) <= DESCRIBES_ITSELF


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


def _drop_pool_payload(command: str) -> str:
    """Cut a pool submission's payload off before the command is segmented.

    ``pbrun.py --gpu -- <cmd>`` hands everything past ``--`` to the pool, which
    execs it on a worker that already holds the reservation.  Segmenting that
    payload tears the interpreter away from the entrypoint that vouches for it,
    so a submission whose payload was ``bash -lc 'cd x && <cuda venv> -m
    pytest'`` was refused *as off-pool GPU work* -- the hook locking out the
    very submission it exists to require.

    The cut is deliberately narrow: it needs a pool entrypoint AND a ``--``
    after it, because that pair is what makes the rest argv for another
    process.  A chain like ``pbrun.py --help && <cuda venv> train.py`` has no
    ``--``, so its second segment is still scanned and still refused.
    """

    best = None
    for entry in POOL_ENTRYPOINTS:
        at = command.find(entry)
        if at < 0:
            continue
        sep = re.search(r"\s--\s", command[at:])
        if sep is None:
            continue
        cut = at + sep.start()
        if best is None or cut < best:
            best = cut
    return command if best is None else command[:best]


#: A here-document body is data on its way to a file, not a command.  Scanning
#: it is how this hook refused the very file that starts the fleet's workers:
#: that config names the CUDA interpreter as a worker's ``--python`` argument,
#: which is the pattern above by construction, and the writing command led with
#: ``cd``.  Nothing inside a heredoc can start GPU work -- the shell is copying
#: bytes to a file -- so the body is removed before anything is judged.  This
#: was the fourth time the hook locked out its own repair; the rule it enforces
#: is unchanged, only what counts as a command.
HEREDOC = re.compile(r"""<<-?\s*(['"]?)([A-Za-z_][A-Za-z0-9_]*)\1""")


def _drop_heredoc_bodies(command: str) -> str:
    """Remove every here-document body, keeping the commands around them."""

    out: list[str] = []
    rest = command
    while True:
        opener = HEREDOC.search(rest)
        if opener is None:
            out.append(rest)
            break
        tag = opener.group(2)
        newline = rest.find("\n", opener.end())
        if newline < 0:
            # An opener with no body yet: nothing has been fed in, so there is
            # nothing to strip and the text before it still stands as command.
            out.append(rest)
            break
        out.append(rest[: opener.start()])
        lines = rest[newline + 1:].split("\n")
        for index, line in enumerate(lines):
            if line.strip() == tag:
                rest = "\n".join(lines[index + 1:])
                break
        else:
            # Unterminated: the rest of the input is body all the way down.
            rest = ""
            break
    # Rejoin with a boundary, not a space.  Gluing the command before a
    # heredoc to the command after it makes one segment whose first token is
    # the writer -- ``cat``, which is exempt -- and the work behind the
    # heredoc inherits that exemption.  Same hole as judging a compound
    # command by its first token, reached from a different direction.
    return "\n".join(part for part in out if part)


def _segments(command: str) -> list[str]:
    """The commands inside one Bash invocation, each judged on its own.

    A leading ``cat`` does not vouch for what follows ``&&``, and a refused
    segment is not excused by a permitted neighbour.
    """

    # A backslash-newline is a line continuation, not a command boundary.
    # Splitting on the raw newline tears one command into pieces and strips
    # each piece of the context that exempts it -- which refused a worker
    # launch whose interpreter argument sat on its own continued line.
    joined = re.sub(r"\\\s*\n", " ", command)
    joined = _drop_heredoc_bodies(joined)
    joined = _drop_pool_payload(joined)
    return SEPARATORS.split(joined)


def contends(command: str) -> bool:
    """True when any segment of this command starts GPU work off-pool."""

    for segment in _segments(command):
        if not CONTENDS.search(segment):
            continue
        if _first_token(segment) in NEVER_GPU:
            continue
        if NO_DEVICE.search(segment):
            continue
        if any(entry in segment for entry in POOL_ENTRYPOINTS):
            continue
        return True
    return False


def submits(command: str) -> bool:
    """True when any segment submits to SLURM outside the fleet's lane.

    The exemptions are the three that cannot be anything else: a segment led
    by a command that never starts work (a commit message, a grep for the
    word, ``which sbatch``), a verb asked only about itself (``sbatch
    --help``), and a segment naming the lane's own entrypoints, which are
    what run ``sbatch`` on this fleet's behalf.

    ``CUDA_VISIBLE_DEVICES=`` is NOT an exemption here, deliberately.  It works
    for a local command because the kernel then denies the child a device; it
    says nothing about a job the scheduler will start on another node with a
    GRES allocation of its own.
    """

    for segment in _segments(command):
        if not SCHEDULER.search(segment):
            continue
        if _first_token(segment) in NEVER_GPU:
            continue
        if _describes_itself(segment):
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
    if contends(command):
        sys.stderr.write(
            "Refused: GPU work goes through the PrismaBuild pool, not a local "
            "lock.\n\n"
            f"Run it as:\n  /usr/bin/python3 {PBRUN} --gpu -- <your command>\n\n"
            "Flags: --exclusive for a timing run that needs the whole box "
            "(expressed as a demand for its full GPU capacity, which the ledger "
            "turns into exclusion); Git checkouts are sealed through the CAS and "
            "may be materialized on any matching box; --here pins one on purpose.\n\n"
            "Why: a box-local flock cannot balance across sparky and sparklina, "
            "and it reproduced hold-while-gated, starvation and partial-hold "
            "waste that the pool's ledger solves structurally.\n"
        )
        return 2                      # exit 2 blocks and shows this to the agent
    if submits(command):
        # The same shape, because it is the same rule: an agent who has read
        # one refusal should be able to read this one without stopping.
        sys.stderr.write(
            "Refused: SLURM work goes through the PrismaBuild lane, not a bare "
            "submission.\n\n"
            f"Run it as:\n  /usr/bin/python3 {PBRUN} --transport slurm "
            "--gpu -- <your command>\n\n"
            "Flags: the lane turns --demand and --tag into --gres and "
            "--constraint, --timeout-s into --time, and seals the checkout "
            "through the CAS so the job materializes it on whichever node the "
            "scheduler picks; scancel and squeue are not refused.\n\n"
            "Why: a bare sbatch names no interpreter, so nothing about it can "
            "be priced, placed against the fleet's own accounting, or looked "
            "up afterwards -- the job's CAS receipt is what makes an action "
            "mean the same thing under either transport.\n"
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

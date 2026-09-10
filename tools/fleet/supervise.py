#!/usr/bin/env python3
"""Keep this box's worker loops alive.

Nothing started the fleet's worker loops but a person at a keyboard, and
nothing restarted them.  A loop exits on ``--max-idle`` by design, and it
exits on an unhandled error by accident, and the counts were visibly
draining: sparky 5 -> 4, sparklina 3 -> 2, dl380g10 15 -> 14 over one
evening, every survivor reparented to init.  A fleet that only loses workers
reaches zero, and the failure at zero is indistinguishable from a busy queue:
items sit in ``ready``, counted and reported as pending, with nobody left to
claim them.  That is the same silence the placement bug had, arrived at from
the other end.

So: one supervisor per box, holding an exclusive advisory claim on a
box-local file so a second invocation is a no-op rather than a doubling; the
floor count and loop arguments read from ``fleet_boxes.json`` in the checkout,
so the shape of the fleet is a versioned file rather than three command lines
nobody wrote down; and elastic housekeeping width driven by the claims this
box is actually holding, which is the one signal about its own load the
supervisor can attribute.  ``--loops`` remains a fixed-count operator
override.

A long-lived supervisor also follows the immutable runtime generation at a
cycle boundary.  It validates the new generation and its own loaded bytes,
then preserves its exclusive local claim across ``exec``.  Worker loops are
not children to recycle during that handoff: active work finishes under the
generation that claimed it, and idle loops upgrade independently.

The claim is deliberately box-local.  It says "a supervisor owns THIS box",
which is a fact about one machine's processes; on shared storage it would let
one box's supervisor silence another's.  It is not a scheduling primitive and
admits no work -- every action still goes through the pool's ledger, which is
the thing that can actually balance two boxes.

``--ensure`` is the form a periodic job uses: become the supervisor if there
isn't one, exit quietly if there is.  Nothing supervises the supervisor, so
that idempotence is what makes a five-minute timer a sufficient answer.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import stat
import subprocess
import sys
import time
from typing import Collection, TextIO

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import pool  # noqa: E402

MIRROR = Path("/mnt/shared/prismabuild-fleet")
CONFIG = Path(__file__).resolve().parent / "fleet_boxes.json"
CLAIM = Path("/home/rob/tmp/prismabuild-supervisor.claim")
LOG_DIR = Path("/home/rob/tmp")
PROC = Path("/proc")

#: The mark this supervisor sets on every loop it launches, carrying the box
#: it launched the loop for.
#:
#: A basename is not ownership.  ``pgrep -f worker_loop.py`` matches any
#: command line containing that string, and the confirmation step only asked
#: whether an interpreter was running a script whose name ended that way -- so
#: an unrelated ``/another-project/worker_loop.py`` under the same user was
#: counted toward this box's fleet capacity, and on the first tick where its
#: arguments did not match ``fleet_boxes.json`` and it happened to have no
#: child process, it was SIGTERMed.  A long-running Python workload is busy
#: and childless at the same time all the time.
#:
#: The mark is read from ``/proc/<pid>/environ`` rather than believed from a
#: log or a pid file, and it is only half the answer: it says this supervisor
#: launched the process, and the resolved script path says the bytes came from
#: a runtime generation this fleet published.  Either alone is guessable.
OWNERSHIP_ENV = "PRISMABUILD_SUPERVISED_WORKER"

#: The script a fleet worker loop runs, checked after the path resolves rather
#: than as a suffix of whatever ``pgrep`` matched.
LOOP_SCRIPT = "worker_loop.py"

# Worker loops are queue pollers, not CPU reservations.  Keep a couple ready
# to claim without a process-start round trip, while the queue's admission
# controller remains the sole authority over whether an action may run.
#
# The reserve is also what absorbs the gap between two actions.  A loop that
# has just served one does not sleep: only ``worker_loop``'s idle and error
# branches wait ``--poll-s``, and the served branch falls straight back into
# ``serve_once``.  So a working loop is unclaimed for one claim round trip,
# not one poll interval, and two spares cover more of those than a 5 s
# supervision tick can land in.  That is why the sizing law below needs no
# smoothing window: there is no long transient to smooth.
IDLE_RESERVE = 2
BUSY_INTERVAL_S = 5.0
LOOPS_PER_VISIBLE_CPU = 4
BYTES_PER_HOUSEKEEPING_LOOP = 256 * 1024 * 1024
INHERITED_CLAIM_FD_ENV = "PRISMABUILD_SUPERVISOR_CLAIM_FD"
RUNTIME_VERSION_SCHEMA = "prismaquant.prismabuild.runtime_version.v1"


@dataclass(frozen=True)
class PublishedGeneration:
    root: Path
    name: str
    commit: str
    supervisor: Path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _published_generation(
    root: Path | None = None, *, entrypoint: Path | None = None,
) -> PublishedGeneration | None:
    """Return one fully validated published generation, or ``None``.

    A receipt-shaped directory is insufficient.  The root must be a direct,
    non-staging child of this fleet's generation store, its receipt must name
    that generation, and the supervisor bytes must match the sealed manifest.
    Transient or damaged publication state leaves the current supervisor in
    charge and is retried on the next boundary.
    """

    try:
        store = (MIRROR / "runtime-generations").resolve(strict=True)
        candidate = (_current_root() if root is None else root).resolve(strict=True)
        if (candidate.parent != store or candidate.name.startswith(".")
                or candidate.name in ("", ".", "..")):
            return None
        receipt = json.loads(
            (candidate / "RUNTIME_VERSION.json").read_text(encoding="utf-8"))
        if not isinstance(receipt, dict):
            return None
        commit = receipt.get("commit")
        files = receipt.get("files")
        if (receipt.get("schema") != RUNTIME_VERSION_SCHEMA
                or receipt.get("generation") != candidate.name
                or not isinstance(commit, str)
                or re.fullmatch(r"[0-9a-f]{40}", commit) is None
                or not isinstance(files, dict)):
            return None
        supervisor = (
            candidate / "tools" / "supervise.py" if entrypoint is None
            else entrypoint
        ).resolve(strict=True)
        relative = supervisor.relative_to(candidate).as_posix()
        if relative not in ("tools/supervise.py", "tools/fleet/supervise.py"):
            return None
        expected = files.get(relative)
        if (not isinstance(expected, str)
                or re.fullmatch(r"[0-9a-f]{64}", expected) is None
                or _sha256(supervisor) != expected):
            return None
        return PublishedGeneration(candidate, candidate.name, commit, supervisor)
    except (OSError, ValueError, TypeError):
        return None


def _loaded_published_generation() -> PublishedGeneration | None:
    """The receipt identity of the exact supervisor bytes now executing."""

    return _published_generation(RUNTIME_ROOT, entrypoint=Path(__file__))


def _inherited_claim_handle() -> TextIO | None:
    """Validate and adopt the flock description preserved across ``exec``."""

    raw = os.environ.pop(INHERITED_CLAIM_FD_ENV, None)
    if raw is None:
        return None
    if re.fullmatch(r"[0-9]+", raw) is None:
        raise SystemExit("invalid inherited PrismaBuild supervisor claim fd")
    try:
        descriptor = int(raw)
    except (ValueError, OverflowError) as exc:
        raise SystemExit(
            "invalid inherited PrismaBuild supervisor claim fd") from exc
    if descriptor <= 2:
        raise SystemExit("invalid inherited PrismaBuild supervisor claim fd")
    try:
        held = os.fstat(descriptor)
        named = CLAIM.stat()
        if (not stat.S_ISREG(held.st_mode) or held.st_uid != os.getuid()
                or (held.st_dev, held.st_ino) != (named.st_dev, named.st_ino)):
            raise OSError("descriptor does not name the supervisor claim")
        # This succeeds only when this open-file description owns the lock (or
        # can acquire an otherwise unowned one).  The fresh description below
        # must then be excluded, proving the lock survived the exec boundary.
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        contender = CLAIM.open("r+")
        try:
            try:
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pass
            else:
                raise OSError("inherited claim does not exclude a contender")
        finally:
            contender.close()
        os.set_inheritable(descriptor, False)
        return os.fdopen(descriptor, "r+", encoding="utf-8", closefd=True)
    except (OSError, OverflowError) as exc:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise SystemExit(
            f"invalid inherited PrismaBuild supervisor claim fd: {exc}") from exc


def _claim_handle(*, ensure: bool) -> TextIO | None:
    """Adopt an exec-preserved lock or acquire the box-local supervisor lock."""

    inherited = _inherited_claim_handle()
    if inherited is not None:
        return inherited
    CLAIM.parent.mkdir(parents=True, exist_ok=True)
    handle = CLAIM.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        if ensure:
            return None
        raise SystemExit(f"another supervisor owns {CLAIM}")
    handle.seek(0)
    handle.truncate()
    handle.write(f"{os.getpid()}\n")
    handle.flush()
    return handle


def _reexec_if_published(
    loaded: PublishedGeneration | None, claim_handle: TextIO,
) -> bool:
    """Exec a different trusted live generation at a cycle boundary.

    Worker loops are separate owned processes and are deliberately untouched.
    They finish active jobs under their immutable generation and perform their
    own idle upgrade; the replacement supervisor merely adopts their census.
    """

    if loaded is None:
        return False
    try:
        live_root = _current_root().resolve(strict=True)
    except OSError:
        return False
    if live_root == loaded.root:
        return False
    replacement = _published_generation(live_root)
    if replacement is None:
        return False
    descriptor = claim_handle.fileno()
    argv = [sys.executable, str(replacement.supervisor), *sys.argv[1:]]
    environment = {**os.environ, INHERITED_CLAIM_FD_ENV: str(descriptor)}
    print(f"[{socket.gethostname()}] supervisor generation {loaded.name} -> "
          f"{replacement.name}; preserving pid {os.getpid()} and worker loops",
          flush=True)
    os.set_inheritable(descriptor, True)
    try:
        os.execve(sys.executable, argv, environment)
    except OSError as exc:
        os.set_inheritable(descriptor, False)
        print(f"[{socket.gethostname()}] supervisor re-exec refused: {exc}",
              flush=True)
        return False
    # Test doubles can return even though a real successful exec never does.
    os.set_inheritable(descriptor, False)
    return True


def _current_root() -> Path:
    return MIRROR / "repo"


def _queue_root() -> Path:
    """The pull queue this box's loops serve, spelled as they spell it.

    Read from ``MIRROR`` at call time rather than bound at import, for the
    same reason ``_current_root`` is: it is the one name a test can repoint to
    keep the suite off the live store.
    """

    return MIRROR / "pb-queue"


def _claim_holders() -> frozenset[int] | None:
    """The pids on this box the queue says hold a claim, or ``None`` if unknown.

    ``None`` is not the empty set.  A queue root that is not there answers the
    question -- nobody holds a claim on a box with no queue, which is where the
    fleet ends up after the cutover -- while a listing that fails for any other
    reason answers nothing, and the caller must not read silence as idleness.
    """

    try:
        return frozenset(pool.PoolQueue(_queue_root()).claim_holder_pids())
    except (OSError, pool.PoolContractError):
        return None


def _config(host: str) -> dict:
    """Read this box's declared shape, from the checkout or the published copy."""

    # A supervisor intentionally outlives a generation.  Prefer the current
    # live generation; CONFIG is only the checkout/bootstrapping fallback.
    for path in (_current_root() / "tools" / "fleet_boxes.json", CONFIG):
        try:
            boxes = json.loads(path.read_text())["boxes"]
        except (OSError, ValueError, KeyError):
            continue
        if host in boxes:
            return boxes[host]
        # The second Spark was renamed from gx10-6b77 to sparklina.  Its
        # declared alias names the same machine, not a second capacity offer.
        # Keep one shape so the old and new hostname cannot drift apart.
        aliases = [shape for shape in boxes.values()
                   if shape.get("_alias") == host]
        if len(aliases) > 1:
            raise SystemExit(f"ambiguous fleet hostname alias {host}: {path}")
        if aliases:
            return aliases[0]
    raise SystemExit(
        f"no fleet_boxes.json entry for {host}; refusing to guess what this "
        f"box offers -- add it to tools/fleet/fleet_boxes.json and publish"
    )


def declared_shape(host: str, override_loops: int,
                   previous: tuple[int, list[str]] | None = None
                   ) -> tuple[int, list[str]]:
    """This box's target count and loop arguments, re-read every tick.

    The docstring above promises that the shape of the fleet is a versioned
    file rather than three command lines nobody wrote down, and reading it
    once at startup quietly broke that promise: a supervisor started before a
    publish kept its old arguments for its whole life, so an offer widened in
    the file reached the queue only if somebody remembered to restart the
    supervisor too.  That is the same failure as a worker holding stale
    modules -- ``cycle_stale`` exists for exactly that -- arriving one level
    up, where nothing was watching.  It cost a box two of three GPU slots for
    as long as nobody noticed.

    A bad read keeps the previous shape instead of raising.  Runtime
    publication is atomic now, but a transient NFS read failure or damaged
    generation must still not take the box's loops with it the next time one
    goes idle.  The first read has no previous to fall back on and still
    refuses, because guessing what a box offers is the one thing this must
    never do.
    """

    try:
        config = _config(host)
    except (SystemExit, OSError, ValueError):
        if previous is None:
            raise
        return previous
    return (override_loops or int(config.get("loops", 1)),
            [str(a) for a in config.get("args", [])])


def _proven_roots() -> list[Path]:
    """Every runtime tree a fleet worker loop may legitimately have started in.

    The live generation, and the published generations behind it.  A loop
    holds the bytes it imported for its whole life and publication never
    deletes a generation, so an old immutable generation may still be running
    an action; refusing to recognize it would make the supervisor spawn a
    replacement beside work in flight.  A tree that is neither is not this
    fleet's, whatever the script inside it is called.

    The generation store is filtered by the rule ``publish_runtime
    ._activate_existing`` already applies: a direct child, not a dot-name,
    carrying a ``RUNTIME_VERSION.json``.  A ``.<name>.staging`` survivor of an
    interrupted publish carries a receipt and passes every other check, and
    its bytes were never sealed or probed.
    """

    roots: list[Path] = []

    def add(candidate: Path) -> None:
        try:
            resolved = candidate.resolve(strict=True)
        except OSError:
            return
        if resolved not in roots:
            roots.append(resolved)

    add(_current_root())
    try:
        children = sorted((MIRROR / "runtime-generations").iterdir())
    except OSError:
        children = []
    for child in children:
        if child.name.startswith("."):
            continue
        if not (child / "RUNTIME_VERSION.json").is_file():
            continue
        add(child)
    return roots


def _proc_field(pid: int, name: str, proc_root: Path) -> bytes | None:
    try:
        return (proc_root / str(pid) / name).read_bytes()
    except OSError:
        return None                       # exited, or not ours to read


def _script_of(pid: int, argv: list[str], proc_root: Path) -> Path | None:
    """The loop script this process is running, resolved absolutely.

    dl380g10's loops carry a relative ``repo/tools/worker_loop.py``, so the
    path is resolved against the process's own working directory rather than
    the supervisor's -- reading ``/proc/<pid>/cwd`` asks the kernel instead of
    assuming the two agree.
    """

    script = Path(argv[1])
    if not script.is_absolute():
        try:
            script = (proc_root / str(pid) / "cwd").resolve(strict=True) / script
        except OSError:
            return None
    try:
        return script.resolve(strict=True)
    except OSError:
        return None


def _is_fleet_loop(pid: int, roots: list[Path],
                   proc_root: Path | None = None) -> bool:
    """True when this pid is a worker loop this box's supervisor launched.

    Three facts, none of them a name: an interpreter is running the script as
    an argument, the script resolves inside a proven runtime root, and the
    process carries this supervisor's ownership mark for this box.  A process
    that fails any of them is neither counted toward the box's target nor
    signalled, which is the whole of issue #87.
    """

    proc_root = PROC if proc_root is None else proc_root
    raw = _proc_field(pid, "cmdline", proc_root)
    if raw is None:
        return False
    argv = [part.decode("utf-8", "replace") for part in raw.split(b"\0") if part]
    if len(argv) < 2 or "python" not in argv[0].rsplit("/", 1)[-1]:
        return False
    script = _script_of(pid, argv, proc_root)
    if script is None or script.name != LOOP_SCRIPT:
        return False
    if not any(script.is_relative_to(root) for root in roots):
        return False
    environ = _proc_field(pid, "environ", proc_root)
    if environ is None:
        return False
    for entry in environ.split(b"\0"):
        name, sep, value = entry.partition(b"=")
        if sep and name.decode("utf-8", "replace") == OWNERSHIP_ENV:
            return value.decode("utf-8", "replace") == socket.gethostname()
    return False


def _live_loops(proc_root: Path | None = None) -> list[int]:
    """The worker loops this supervisor owns, not the processes that mention one.

    ``pgrep`` is a candidate generator and nothing more.  Over-counting is the
    dangerous direction for the respawn this exists for -- it makes a drained
    box look full -- and over-claiming is the dangerous direction for the
    SIGTERM: both are decided by ``_is_fleet_loop`` rather than by the string
    ``pgrep`` matched on.
    """

    proc = subprocess.run(["pgrep", "-f", LOOP_SCRIPT],
                          capture_output=True, text=True, check=False)
    mine = os.getpid()
    roots = _proven_roots()
    confirmed: list[int] = []
    for token in proc.stdout.split():
        pid = int(token)
        if pid == mine:
            continue
        if _is_fleet_loop(pid, roots, proc_root):
            confirmed.append(pid)
    return confirmed


def loop_args_of(pid: int) -> list[str] | None:
    """The arguments this loop is actually running with, or None if unreadable.

    Comparing the file against the previous *read* of the file is not enough:
    a supervisor restarted after a publish reads the new shape first thing,
    sees no change on any later tick, and leaves loops running the old one
    forever -- which is exactly how three loops kept announcing two GPU slots
    while their supervisor's own banner said three.  The authority is the
    file and the question is what the processes carry, so ask the processes.
    """

    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
    except OSError:
        return None
    args = [part.decode("utf-8", "replace") for part in raw if part]
    if len(args) < 2:
        return None
    return args[2:]                       # past the interpreter and the script


def _has_children(pid: int) -> bool | None:
    """Whether ``pid`` has a child, or ``None`` when its state is unreadable."""

    try:
        tasks = sorted(Path(f"/proc/{pid}/task").iterdir())
    except OSError:
        return None
    saw_task = False
    for task in tasks:
        try:
            children = (task / "children").read_text().split()
        except OSError:
            continue
        saw_task = True
        if children:
            return True
    return False if saw_task else None


def _is_idle(pid: int, holders: Collection[int] | None = None) -> bool:
    """True when this loop holds no action: no claim, and no child of its own.

    Childlessness alone was wrong in one direction, and the direction that
    costs work.  ``PoolQueue.claim`` returns as soon as its rename lands, and
    ``execute`` then materializes a sealed checkout -- a bundle of up to
    512 MiB out of the CAS -- before it reaches ``Popen``.  For all of that a
    loop holds an action and has no child, so it read as idle, was SIGTERMed,
    and left its claim in ``claimed/`` until ``reap_stale`` timed the lease out
    ``LEASE_TIMEOUT_S`` later.  The action then ran again from the start.

    So ask the queue first.  The loop already recorded the claim: ``claim``
    writes its own pid into the lease before it returns and ``finish`` unlinks
    it, so a lease naming this box and this pid IS the claim, where the process
    tree is an inference about it.  When that answer cannot be had at all the
    loop is treated as busy: a cycle deferred to the next tick costs a tick,
    and a claim dropped costs the action.

    The child check stays, and stays second.  It is what covers a loop running
    an action under a generation whose lease this one cannot parse, and it
    needs no shared filesystem.  Reading ``/proc/<pid>/task/*/children`` asks
    the kernel rather than guessing from a log line.
    """

    if holders is None:
        holders = _claim_holders()
    if holders is None or pid in holders:
        return False
    return _has_children(pid) is False


def _ready_backlog() -> bool | None:
    """Whether the queue has a ready record, or ``None`` when unknown.

    This is one directory scan per supervision cycle.  The loops parse and
    order records when they claim; the supervisor only needs the pressure
    signal and must not duplicate that NFS work.
    """

    ready = _queue_root() / pool.READY
    try:
        with os.scandir(ready) as entries:
            return any(entry.name.endswith(".json") for entry in entries)
    except FileNotFoundError:
        return False
    except OSError:
        return None


def _visible_memory_bytes() -> int:
    """Best-effort physical memory visible to this supervisor."""

    try:
        return int(os.sysconf("SC_PHYS_PAGES")) * int(
            os.sysconf("SC_PAGE_SIZE"))
    except (OSError, ValueError):
        return 0


def housekeeping_ceiling(floor: int) -> int:
    """Bound cheap queue pollers from visible CPU and memory.

    This is a process-count safety ceiling, not an action concurrency limit,
    and since the sizing law became evidence-driven it is not the operative
    bound either: reaching it now requires that many loops to be genuinely
    holding claims, which admission decides.  It was operative while growth
    was a fraction of it -- ``LOOPS_PER_VISIBLE_CPU = 4`` made it 80 on a
    20-CPU Spark and a growth batch 20 -- and these pollers are metadata-bound
    on a shared network filesystem rather than CPU-bound, so the number was
    never derived from the medium it actually loads.  Deriving it needs a
    measurement of that medium; until there is one, a different unmeasured
    constant would not be an improvement.
    """

    try:
        cpus = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        cpus = os.cpu_count() or 1
    by_cpu = max(1, cpus) * LOOPS_PER_VISIBLE_CPU
    memory = _visible_memory_bytes()
    by_memory = memory // BYTES_PER_HOUSEKEEPING_LOOP if memory else by_cpu
    return max(floor, min(by_cpu, max(1, by_memory)))


def _next_log_index() -> int:
    """Choose a log slot no running or earlier loop can already be appending."""

    highest = -1
    try:
        entries = list(LOG_DIR.iterdir())
    except OSError:
        return 0
    for path in entries:
        name = path.name
        if not (name.startswith("pb-worker-") and name.endswith(".log")):
            continue
        try:
            highest = max(highest, int(name[10:-4]))
        except ValueError:
            continue
    return highest + 1


def cycle_stale(published: str) -> list[int]:
    """Stop idle loops running bytes other than the published ones.

    A loop holds the modules it imported at start for its whole life, so a fix
    published under a running fleet reaches none of it -- and because the
    supervisor counts a stale-byte loop as a healthy one, the target is met
    and the fix is never loaded.  Loops carrying the reload check exit on
    their own; this is for the generation that predates it.

    Only idle loops are stopped, and only with SIGTERM: a loop mid-action
    keeps its claim and cycles when it next goes idle.  Nothing here kills
    work.
    """

    if not published:
        return []
    return _stop_idle_loops()


def _stop_idle_loops(pids: list[int] | None = None,
                     proc_root: Path | None = None,
                     holders: Collection[int] | None = None) -> list[int]:
    """SIGTERM every loop holding no action, and report which.

    The one rule both reasons to cycle a loop share -- stale bytes and a
    stale declared shape.  Only idle loops, only SIGTERM: a loop mid-action
    keeps its claim and cycles when it next goes idle.
    """

    stopped: list[int] = []
    roots = _proven_roots()
    if holders is None:
        holders = _claim_holders()
    if holders is None:
        return []                         # NFS silence never licenses a signal
    for pid in (_live_loops(proc_root) if pids is None else pids):
        # Proved again here, at the line that actually sends the signal.  The
        # caller has always passed pids that came from ``_live_loops``, but a
        # future caller that does not must not be able to turn this into the
        # basename kill it used to be.
        if not _is_fleet_loop(pid, roots, proc_root):
            continue
        if not _is_idle(pid, holders):
            continue
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            continue
        stopped.append(pid)
    return stopped


REAP_BUDGET = 4096


def _reap() -> int:
    """Collect every child that has exited, and answer how many that was.

    Nothing else in this file waits on a child.  ``_spawn`` drops the
    ``Popen`` it builds, so what reaped a finished loop until now was
    incidental: CPython files a dropped handle into the module-global
    ``subprocess._active`` and polls that list at the top of the *next*
    ``Popen.__init__``, and ``_live_loops`` builds one every tick for
    ``pgrep``.  Inside one process image that bounds the delay to a tick and
    hides the fact that the supervisor never owned the lifecycle at all.

    ``_reexec_if_published`` is where the omission becomes permanent.
    ``os.execve`` replaces the image to adopt a published generation: the
    kernel keeps the child list, ``_active`` goes with the old image, and
    every loop that was alive at that moment can no longer be reaped by the
    subprocess module.  It becomes a zombie when it exits and stays one for
    the life of the supervisor, so the count grows with publish cadence
    rather than with load and falls only when the supervisor itself dies.
    Measured on 2026-09-10: 1106 on dl380g10 across 69 re-execs of one
    supervisor process, 100% of them spawned before the last re-exec and
    none after it.

    ``waitpid`` asks the kernel rather than this process's memory, which is
    the property that matters: it is indifferent to the exec, so it collects
    what earlier generations stranded as well as what this one leaves.

    Taking every status here is safe because no caller in this file reads
    one.  The single place that does -- ``subprocess.run`` in ``_live_loops``
    -- waits on its own pid, and this process is single-threaded with no
    signal handler, so this loop never runs between that spawn and that wait.

    ``signal.signal(SIGCHLD, SIG_IGN)`` is the shorter spelling and is
    rejected.  A ``SIG_IGN`` disposition survives ``execve`` *and* is
    inherited by children, so it would not stop at this process: ``_spawn``
    execs a worker loop, the loop execs an action, and an ignored ``SIGCHLD``
    makes every ``waitpid`` in that chain answer ``ECHILD``, which
    ``subprocess`` turns into returncode 0.  Measured on sparky, python
    3.12.3: with the disposition set here, a grandchild running ``false``
    reports 0 rather than 1.  Every failing action in the pool would report
    success.  ``waitpid`` in this one process buys the same reap with none of
    that reach.
    """

    collected = 0
    while collected < REAP_BUDGET:
        try:
            pid, _status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            break                          # no children at all
        if pid == 0:
            break                          # children, but none have exited
        collected += 1
    return collected


def _spawn(args: list[str], index: int) -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    handle = (LOG_DIR / f"pb-worker-{index}.log").open("a", buffering=1)
    handle.write(f"\n=== spawned {time.strftime('%F %T')} ===\n")
    # Resolve the live generation once before spawning.  The child then loads
    # its script and every sibling import through one immutable absolute root,
    # even if a later publish moves the live ``repo`` symlink.
    loop = (_current_root() / "tools" / "worker_loop.py").resolve(strict=True)
    proc = subprocess.Popen(
        [sys.executable, str(loop), *args],
        cwd=str(MIRROR), stdout=handle, stderr=subprocess.STDOUT,
        start_new_session=True,
        # The mark that makes this loop attributable to this supervisor.  It
        # is what ``_is_fleet_loop`` reads back out of ``/proc``, and a loop
        # spawned without it is not counted and never signalled.
        env={**os.environ, OWNERSHIP_ENV: socket.gethostname()},
    )
    return proc.pid


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ensure", action="store_true",
                    help="exit 0 if a supervisor already owns this box")
    ap.add_argument("--loops", type=int, default=0,
                    help="fixed loop count (disables elastic scaling)")
    ap.add_argument("--interval-s", type=float, default=30.0,
                    help="seconds between supervision cycles; ignored with "
                         "--once")
    ap.add_argument("--once", action="store_true",
                    help="top the box up and exit, without supervising")
    ap.add_argument("--cycle-stale", action="store_true",
                    help="SIGTERM idle loops so they respawn on the published "
                         "runtime; a loop mid-action is left alone")
    args = ap.parse_args()

    host = socket.gethostname()
    target, loop_args = declared_shape(host, args.loops)

    if args.cycle_stale and args.once:
        # A one-shot cycle does not need to own the box: it stops only idle
        # loops, the running supervisor's next tick tops the count back up,
        # and taking the claim here would make the maintenance action fail
        # precisely when a supervisor is present -- which is always.
        published = ""
        try:
            published = str(json.loads(
                (_current_root() / "RUNTIME_VERSION.json").read_text()
            ).get("commit") or "")
        except (OSError, ValueError):
            pass
        stopped = cycle_stale(published)
        print(f"[{host}] cycled {len(stopped)} idle loop(s) onto "
              f"{published[:12] or '(unknown)'}: {stopped}", flush=True)
        return 0

    loaded_generation = _loaded_published_generation()
    handle = _claim_handle(ensure=args.ensure)
    if handle is None:
        return 0                           # somebody else already owns this box

    print(f"[{host}] supervising {target} loops: {' '.join(loop_args)}",
          flush=True)
    fixed_target = args.loops > 0
    next_log_index = _next_log_index()
    if args.cycle_stale:
        published = ""
        try:
            published = str(json.loads(
                (_current_root() / "RUNTIME_VERSION.json").read_text()
            ).get("commit") or "")
        except (OSError, ValueError):
            pass
        stopped = cycle_stale(published)
        print(f"[{host}] cycled {len(stopped)} idle loop(s) onto "
              f"{published[:12] or '(unknown)'}: {stopped}", flush=True)
    while True:
        # Before anything else, and before the exec below can strand them:
        # the supervisor owns its children's exit statuses, and this is the
        # one place it collects them.
        reaped = _reap()
        if reaped:
            print(f"[{host}] reaped {reaped} exited child process(es)",
                  flush=True)
        if not args.once and _reexec_if_published(loaded_generation, handle):
            return 0                       # reached only under an exec test double
        target, loop_args = declared_shape(
            host, args.loops, (target, loop_args))

        live = _live_loops()
        # One authoritative claim census per cycle.  Reusing it for stale
        # cycling, load feedback and scale-down avoids an NFS rescan per pid.
        holders = _claim_holders()
        # The file is the authority, so a loop running other arguments is
        # stale in the same sense a loop running other bytes is.  Stop the
        # idle ones and let the top-up below respawn them on the declared
        # shape; a loop mid-action keeps its claim and cycles when it next
        # goes idle, which is why this never kills work.
        wrong = [pid for pid in live
                 if loop_args_of(pid) not in (None, loop_args)]
        if wrong:
            stopped = _stop_idle_loops(wrong, holders=holders)
            live = [pid for pid in live if pid not in stopped]
            print(f"[{host}] {len(wrong)} loop(s) carry a shape the file no "
                  f"longer declares; stopped the {len(stopped)} idle one(s) "
                  f"{stopped} onto: {' '.join(loop_args)}", flush=True)
        backlog = _ready_backlog()
        busy = 0
        idle: list[int] = []
        unknown = 0
        if holders is None:
            unknown = len(live)
        else:
            for pid in live:
                if pid in holders:
                    busy += 1
                    continue
                children = _has_children(pid)
                if children is True:
                    busy += 1
                elif children is False:
                    idle.append(pid)
                else:
                    unknown += 1

        # The sizing law, and the one thing to keep in mind reading it: the
        # backlog is not in it.  A ready record says work is waiting; it does
        # not say why, and the supervisor cannot tell "no poller is free to
        # take it" from "no poller can take it" -- the second is what a slow
        # shared mount, a stuck client or a resource refusal all look like
        # from here.  Sizing on it made the response unbounded in exactly the
        # direction that hurts: growth was a fraction of the ceiling per busy
        # tick, shrink required the backlog to empty, and a backlog that is
        # not draining is at once the thing that forbids shrink and the thing
        # that authorises twenty more readers of the metadata path that is
        # not draining it.  Sparky reached twelve loops that way on
        # 2026-09-06 and had no path back down (issue #231).
        #
        # So size on the one signal that is attributable: a claim this box is
        # holding.  ``busy`` is loops with a lease or a running child, and
        # every poller above ``busy + IDLE_RESERVE`` is a reader the evidence
        # does not pay for.  Growth is then self-limiting without a batch --
        # each new claim earns the spare that lets the next one be taken
        # without a process start -- and shrink needs no permission from the
        # queue, because an idle poller is idle whether or not work is
        # waiting.  ``backlog`` survives only below, choosing the tick rate.
        desired = target
        if not fixed_target and not args.once:
            if holders is None:
                # Silence never licenses a signal, and it does not license a
                # spawn either: hold the count and ask again next tick.
                desired = max(target, len(live))
            else:
                desired = min(housekeeping_ceiling(target),
                              max(target, busy + unknown + IDLE_RESERVE))
                excess = max(0, len(live) - desired)
                if excess and idle:
                    stopped = _stop_idle_loops(
                        idle[-excess:], holders=holders)
                    live = [pid for pid in live if pid not in stopped]
                    print(f"[{host}] {busy} claim(s) held; sized to "
                          f"{desired} and stopped {len(stopped)} proven-idle "
                          f"loop(s) {stopped}", flush=True)

        missing = max(0, desired - len(live))
        for offset in range(missing):
            index = next_log_index
            next_log_index += 1
            pid = _spawn(loop_args, index)
            print(f"[{host}] spawned loop {index} pid {pid} "
                  f"({len(live) + offset + 1} of {desired})", flush=True)
        if missing:
            # One bounded delay lets a large batch spread its first polls
            # without charging half a second for every housekeeping process.
            time.sleep(min(0.5, 0.02 * missing))
        if args.once:
            return 0
        active = busy > 0 or backlog is True
        time.sleep(min(args.interval_s, BUSY_INTERVAL_S)
                   if active else args.interval_s)


if __name__ == "__main__":
    raise SystemExit(main())

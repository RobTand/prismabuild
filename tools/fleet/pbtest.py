"""Fan a test suite out across the pool instead of running it on one box.

The coordinator discovers files and submits independent shards; PB chooses
their placement and concurrency. CPU suites default to the x86 class, while
explicit GPU suites default to GB10. Each shard reserves its aggregate demand.

Four constraints shape this:

* **The checkout is transported by pbrun.** ``pbrun`` seals a Git snapshot in
  the CAS and each worker materializes it on local disk. The payload therefore
  uses only repository-relative paths; embedding the submitter's checkout path
  would escape that snapshot and is refused before publication.
* **The interpreter is named, not inherited.**  An action runs in a closed
  environment, so its interpreter is sealed into its command and hence its
  action key.  A shard therefore names an interpreter that exists on its target
  and carries the matching class tag; the two must agree.  The tag is also what
  owns that dependency claim, so a shard states it with ``--tag`` alone and
  never with ``--anywhere``, which would assert the opposite.
* **A pass/fail here is not a measurement.**  x86 against aarch64 is a
  different BLAS and a different FMA order, so this runs *tests*, never a
  timing or numeric arm.  ``--tag`` defaults to ``x86`` to make that explicit
  at the call site rather than in a comment -- but only when this runtime is
  the published one.  Out of a worktree ``pbrun`` seals that worktree's worker
  launcher into the action and only this box can open it, so the default is
  no tag at all and ``pbrun``'s own host pin stands.
* **A shard reserves what it is allowed to use.**  ``--threads-per-shard``
  sets each pytest worker's BLAS and OMP ceiling. Multiplying that by
  ``--workers-per-shard`` gives ``pbrun --cpus``, which the lane emits as ``--cpus-per-task``.  A ceiling
  without a reservation is threads taking turns inside one core, because
  ``ConstrainCores=yes`` makes the declared demand a cpuset.
  ``--cpus-per-shard`` overrides the pairing, and it is required when the
  ceiling is 0.

Shards are round-robin by file, which balances only if files cost roughly the
same.  They do not -- but the alternative is a duration model nobody has
measured, and an unbalanced shard costs wall-clock while a wrong one costs
trust.  The imbalance is reported so it can be seen rather than assumed.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import (  # noqa: E402
    fleet_tool, generation_root, tool_candidates,
)

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import adaptive_gpu, core, pool  # noqa: E402
from pbrun import require_gpu_memory_scope  # noqa: E402
#: Read for ``pbrun.SH`` when the queue is asked, so the conftest's repointing
#: of the live store reaches it: see :func:`announced_ceilings`.
import pbrun  # noqa: E402
#: The submitter each shard is started through, under whichever layout the
#: runtime containing this file uses.  ``None`` when neither layout has one.
PBRUN = fleet_tool("pbrun.py", root=RUNTIME_ROOT)
SHARED = Path("/mnt/shared")

#: pytest's terminal summary line -- the one line that says pytest reached the
#: end of a session.  Built from ``_pytest.terminal``'s own grammar:
#: ``", ".join(parts) + " in " + duration``, where a part is ``N <word>`` from
#: ``pluralize``, or the literal ``no tests ran``, or one of the
#: ``--collect-only`` forms; and the duration is ``S.SSs``, with a
#: ``(H:MM:SS)`` tail past a minute.
#:
#: The word-substring test this replaces matched any line containing
#: ``" passed"``, ``" failed"`` or ``" error"``.  Two live examples of what it
#: captured instead of a summary: pytest's own usage failure, whose second
#: line is ``python -m pytest: error: unrecognized arguments: ...``, and
#: pbrun's ``removed failed exchange probe /mnt/shared/pb-exchange-...``.
#: Either one set ``ran=True`` for a shard that never started a case, which is
#: exactly the "did not run" reading the ``ran`` flag exists to keep distinct.
#:
#: The ``=``-wrapped form is accepted too.  Shards run ``-q``, so
#: ``summary_stats`` takes its undecorated ``write_line`` branch and a real
#: summary looks like ``1 failed, 531 passed, 1 skipped in 17.82s``; the
#: decoration appears only above ``-q``.  Matching it costs nothing and keeps
#: a shard run at default verbosity from reading as "did not run".
#: pytest-subtests reports ``N subtests passed`` (and ``failed``/``skipped``):
#: the one two-word part.  Without it a shard that ran 16 cases and six
#: subtests read as "did not run" (#256), which is this grammar producing the
#: mirror of the defect it was tightened against.
_COUNTED = r"\d+ (?:subtests? )?[A-Za-z][\w-]*"
_COLLECT_ONLY = (r"no tests collected(?: \(\d+ deselected\))?"
                 r"|\d+/\d+ tests collected \(\d+ deselected\)"
                 r"|\d+ tests? collected")
_PARTS = rf"(?:no tests ran|{_COLLECT_ONLY}|{_COUNTED})(?:, {_COUNTED})*"
_DURATION = r"\d+\.\d+s(?: \([^)]*\))?"
PYTEST_SUMMARY = re.compile(rf"^(?:=+ )?{_PARTS} in {_DURATION}(?: =+)?$")
#: Colour is off down a pipe, but ``FORCE_COLOR`` in a shard's environment
#: would wrap the line in escapes and make it unmatchable.
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def pytest_summary(lines: list[str]) -> str:
    """The last line that is pytest's terminal summary, or ``""``."""

    for line in reversed(lines):
        if PYTEST_SUMMARY.match(ANSI.sub("", line).strip()):
            return line.strip()
    return ""


#: The line ``pbrun`` prints, with exit 74, when it could not read an action's
#: ending: at zero patience, when a reader could not be reaped, or when a
#: patient wait ended while the last read was unavailable.
UNOBSERVED_OUTCOME = re.compile(r"^pbrun: unavailable pool outcome for ([0-9a-f]{12})\b")


def unobserved_outcome(lines: list[str]) -> str | None:
    """The 12-character key whose ending ``pbrun`` could not read, or ``None``."""

    for line in reversed(lines):
        match = UNOBSERVED_OUTCOME.match(ANSI.sub("", line).strip())
        if match:
            return match.group(1)
    return None


#: The last line ``pbrun`` prints, with exit 1, when it refused a submission
#: because its worker-offer scan timed out and the reader was reaped
#: (``pbrun.OfferDiscoveryTimedOut``, raised by ``bounded_offer_snapshot``).
#: Nothing was published, so submitting the same shard again is safe, and it
#: is the one refusal a resubmission can clear.  The ``$`` matters: the same
#: timeout with a reader that survived cleanup appends `` retained reader=``,
#: and that one stays a plain refusal, because a retry would race the reader.
OFFER_DISCOVERY_TIMED_OUT = re.compile(
    r"^pbrun: worker-offer discovery timed out after [^;]*; refusing submission; "
    r"no runnable submission was published\.$")


def offer_discovery_timed_out(lines: list[str], returncode: int | None) -> bool:
    """Whether a shard's ``pbrun`` ended in the refusal a resubmission can clear.

    ``pbtest`` runs ``pbrun`` as a subprocess, so it cannot catch the class the
    way ``pbcampaign`` does (#560).  What it sees is the interpreter printing
    the ``SystemExit`` text last and exiting 1.  Only pbrun's LAST line counts:
    a shard whose tests quote the message, and fail, ran and is not retried.
    """

    if returncode != 1:
        return False
    for line in reversed(lines):
        text = ANSI.sub("", line).strip()
        if text:
            return bool(OFFER_DISCOVERY_TIMED_OUT.match(text))
    return False


#: The prefixes of the lines a shard's submitter prints about its own wait.
#: They are streamed as they arrive (#1048): a patient ``pbrun`` can wait out a
#: reader stuck in the kernel for all of ``--wait-s``, and says so every
#: minute, but a line that sits in a pipe until the shard ends says nothing.
STREAMED_PREFIXES = ("pbrun:", "pbstatus:")

#: The line ``pbrun`` prints when it publishes a shard to the pull queue, or
#: attaches to the run already carrying its key.  It is the one line naming
#: the full action key (#1012); SLURM's submission line names a prefix only.
SUBMITTED = re.compile(r"^pbrun: (queued|attached to) ([0-9a-f]{64})(?![0-9a-f])")

_PRINT_LOCK = threading.Lock()


def _say(text: str) -> None:
    """Print one whole line; shards' drain threads print beside the main one."""

    with _PRINT_LOCK:
        print(text, flush=True)


def drain_shard(index: int, stream, lines: list[str], *, submitted=None) -> None:
    """Read one shard's output as it arrives, keeping all of it in ``lines``.

    Every shard gets its own thread, started as soon as the shard is, so no
    pipe fills while ``pbtest`` is still reading an earlier shard: a full pipe
    blocks ``pbrun`` in ``write(2)``, in the middle of its own wait (#1048).
    Lines that start with a ``STREAMED_PREFIXES`` word are printed at once,
    prefixed with the shard; pytest's own output stays in the shard's result.
    ``submitted(key, verb)`` is called for ``pbrun``'s ``SUBMITTED`` line.
    """

    for line in iter(stream.readline, ""):
        lines.append(line)
        if line.startswith(STREAMED_PREFIXES):
            _say(f"shard {index:>3} {line.rstrip()}")
            match = SUBMITTED.match(line) if submitted is not None else None
            if match is not None:
                submitted(match.group(2), match.group(1))


def run_shard(index: int, command: list[str], first, *, wait_s: float,
              deadline: float, result: dict, files: list[str] = ()) -> None:
    """Drain one shard's ``pbrun`` and resubmit it after an offer-read timeout.

    ``first`` is the attempt the caller already launched, so shards start in
    order as before.  An attempt whose ``pbrun`` ended in
    ``OFFER_DISCOVERY_TIMED_OUT`` published nothing and holds nothing, so the
    same command is run again after ``pbrun.POLL_S``, while the shard's own
    ``--wait-s`` lasts -- the rule ``pbcampaign --max-inflight`` applies to a
    row (#560).  Any other ending is the shard's ending.  Every attempt's
    output stays in ``result["lines"]``, so the receipt shows each refusal.

    When ``pbrun`` says which key it queued or attached to, the key goes in
    ``result["action_key"]`` and is printed with the shard's ``files`` at
    once, so a run's keys can be cited while it is still running (#1012).
    """

    lines: list[str] = []
    result.update(lines=lines, attempts=1, returncode=None, action_key=None)

    def submitted(key: str, verb: str) -> None:
        # The first such line is this shard's: pbrun prints it before the
        # action runs and relays the action's own output only at the end,
        # where a failing test that drove pbrun can print one of its own.
        if result["action_key"] is not None:
            return
        result["action_key"] = key
        _say(f"shard {index:>3} action {key} "
             f"({'queued' if verb == 'queued' else 'attached'}): "
             + " ".join(files))

    proc = first
    while True:
        drain_shard(index, proc.stdout, lines, submitted=submitted)
        proc.wait()
        result["returncode"] = proc.returncode
        if not offer_discovery_timed_out(lines, proc.returncode):
            return
        remaining = deadline - time.monotonic()
        if remaining <= pbrun.POLL_S:
            # The pause would reach the deadline; pbcampaign would stop there
            # too.  The shard keeps pbrun's refusal as its ending.
            _say(f"shard {index:>3} pbtest: worker-offer discovery timed out on "
                 f"attempt {result['attempts']}; {max(remaining, 0.0):.0f}s of "
                 f"--wait-s {wait_s:g} left, not resubmitting")
            return
        result["attempts"] += 1
        _say(f"shard {index:>3} pbtest: worker-offer discovery timed out and "
             f"nothing was published; attempt {result['attempts']} in "
             f"{pbrun.POLL_S:g}s ({remaining:.0f}s of --wait-s {wait_s:g} left)")
        time.sleep(pbrun.POLL_S)
        try:
            proc = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, errors="replace")
        except OSError as exc:
            # The first attempt launched this same command, so this is the
            # box, not the shard.  The refusal already recorded stands.
            _say(f"shard {index:>3} pbtest: could not start attempt "
                 f"{result['attempts']}: {exc}")
            result["attempts"] -= 1
            return


def replayed_output(out: str) -> str:
    """The shard's own stdout, when ``pbrun`` printed a receipt instead of it.

    A submission whose action is already in the CAS comes back as
    ``"status": "cache_hit"``, and what ``pbrun`` prints then is the receipt --
    not the shard's stdout, which sits in the result payload the receipt names.
    Scanning the receipt for a pytest summary finds none, so a cached run of a
    clean suite read as four shards that never started a test.  A re-run of an
    unchanged control arm is precisely the case that hits the cache, which made
    the baseline half of every comparison the half most likely to report
    nothing.

    Returns "" when there is nothing to recover, so the caller keeps what it
    already had.
    """

    if '"status": "cache_hit"' not in out:
        return ""
    match = re.search(r'"payload_path": "([^"]+)"', out)
    if match is None:
        return ""
    try:
        return Path(match.group(1)).read_text(errors="replace")
    except OSError:
        # The blob is gone or unreadable.  Recovering nothing is the honest
        # answer: the caller then reports a shard whose result it cannot read,
        # which is true, rather than a green one.
        return ""


#: ``pbrun``'s own transport vocabulary, and its own reader for the default.
#: This tool builds pbrun's argv rather than importing it, so every flag a
#: shard needs has to be forwarded here -- a flag that is not forwarded is a
#: flag twenty shards never see.  ``fleet_submit`` is published beside this
#: file and holds the one reader of the default, so a shard's transport is the
#: fleet's transport rather than a second opinion about it.
from fleet_submit import TRANSPORTS, default_transport  # noqa: E402
#: The per-test bound's name and the ceiling the worker loops enforce, read
#: from the modules that own them rather than restated here: a bound derived
#: from a number this file copied would drift the moment either moved.
from prismabuild import pytest_test_bound  # noqa: E402
from worker_loop import DEFAULT_EXECUTION_CEILING_S  # noqa: E402
#: The recorder every shard's pytest runs under, and the reader of its record.
import pbtest_outcomes  # noqa: E402

#: The largest end a non-GPU shard may seal from an announcement it never
#: asked for, derived from admission's own rule rather than picked.
#: ``PoolQueue.holder_bound`` reads a bounded holder as ``transient`` -- the
#: only reading under which a starved action waits instead of overtaking it
#: -- while its age is inside ``pool.WITHHOLD_CEILING_S`` of its claim, or its
#: declared end is inside that same ceiling of *now*.  With a declared end
#: ``T``, those two conditions cover the holder's *entire* life (age 0 to
#: ``T``) exactly when ``T <= 2 * WITHHOLD_CEILING_S``: age alone covers the
#: first half, "ends soon" the second.  Past that, there is a window in the
#: middle of the holder's life -- from ``WITHHOLD_CEILING_S`` to
#: ``T - WITHHOLD_CEILING_S`` -- where neither condition holds and the holder
#: reads ``long``, however briefly it actually runs.  ``DEFAULT_EXECUTION_CEILING_S``
#: (7,200 s) still leaves that window open from 900 s to 6,300 s of a shard's
#: life -- the same shape of starvation the Sparks' 86,400 s campaign ceiling
#: gave, just shorter -- so it is not this cap; twice
#: ``pool.WITHHOLD_CEILING_S`` is (#1123).
NON_GPU_UNREQUESTED_CEILING_CAP_S = 2 * pool.WITHHOLD_CEILING_S


def announced_ceilings(tags: list[str]) -> dict[str, float | None]:
    """What each live worker able to take these shards says its ceiling is.

    ``None`` for a box that announced no ceiling, which is "did not say" and
    never "no limit" -- the same reading ``pool.placement_timeout_ceilings``
    insists on, and for the same reason.  An unreadable queue answers with an
    empty mapping: a bound is a convenience, and failing a submission because
    the shared store was slow would be a worse trade than deriving from the
    published default.

    The queue read is the one every shard is submitted to, ``pbrun.SH /
    "pb-queue"``, the root ``pbwait`` and ``pbstatus`` read too.  This used to
    ask a bare ``pool.PoolQueue()``, whose default root
    (``pool.DEFAULT_POOL_ROOT``, then ``/mnt/shared/pb-queue``) was a directory
    no box has, so it found no offers and every bound fell back to the
    published default: 7170 s on dl380g10, whose loop announces 3600 s (#939).
    That default now names this same queue and refuses a missing one (#976).
    """

    try:
        offers = pool.PoolQueue(pbrun.SH / "pb-queue").offers()
    except Exception:
        return {}
    required = set(tags)
    ceilings: dict[str, float | None] = {}
    for offer in offers:
        if not required <= set(offer.get("tags") or ()):
            continue
        announced = offer.get("timeout_ceiling_s")
        ceilings[str(offer.get("host") or "?")] = (
            float(announced)
            if isinstance(announced, (int, float)) and not isinstance(announced, bool)
            else None)
    return ceilings


def receipt_path(action_key: str | None) -> str | None:
    """The CAS receipt of a shard's key, when the fleet's CAS holds one.

    ``None`` when there is no key, no receipt at that path, or no answer from
    the mount: a path in a shard record is a file a reader can open.  One
    ``stat`` per shard, after it ended; the receipt is not verified here
    (``PrismaBuildCAS.lookup`` is the verifier).
    """

    if not action_key:
        return None
    path = core.PrismaBuildCAS(pbrun.SH / "cas").receipt_path(action_key)
    try:
        return str(path) if path.is_file() else None
    except OSError:
        return None


def per_test_bound(*, timeout_s: float | None, override_s: float | None,
                   gpu: bool = False,
                   ceilings: dict[str, float | None] | None = None) -> float:
    """The per-test bound a shard exports, in seconds; ``0`` means none.

    Derived, not chosen.  The bound exists so a hung test is *named*, and the
    only thing that names it is pytest itself, reporting the failure before
    the pool's deadline ends the lease.  So the largest useful bound is the
    one that still fires inside the lease, and the margin it needs is one
    heartbeat: ``pool.HEARTBEAT_S`` is the interval at which the lease's
    ``execution_observation`` is refreshed, so an alarm that fires a heartbeat
    early is one whose stderr bytes -- the node id the handler writes before
    raising -- are counted into the record while the action is still alive.

    With something announced, the ceiling is derived exactly as the shard's
    own sealed end is (:func:`shard_ceiling`, including its #1123 cap for a
    non-GPU shard with no ``--timeout-s``), so the two numbers cannot drift
    apart.  With nothing announced at all, ``DEFAULT_EXECUTION_CEILING_S``
    stands in for the box's ceiling *here, and only here*: dl380g10
    announces 3600 s, so a bound sized against the published default of
    7200 s would never have fired on the very box whose shard hung, but with
    no announcement to correct there is no borrowed campaign ceiling to cap
    either -- a bound too generous to fire is never worse than today, whereas
    a sealed guess would be (``shard_ceiling``).

    ``--test-timeout-s`` overrides it, because a shard whose duration has been
    measured can be bounded far tighter than its ceiling, and ``0`` disables
    the bound for a run that wants the old, unbounded behaviour.
    """

    if override_s is not None:
        return max(0.0, float(override_s))
    announced = {host: value for host, value in (ceilings or {}).items()
                 if value is not None}
    if announced:
        ceiling = shard_ceiling(timeout_s=timeout_s, gpu=gpu, ceilings=announced)
    else:
        ceiling = (DEFAULT_EXECUTION_CEILING_S if timeout_s is None
                  else min(float(timeout_s), DEFAULT_EXECUTION_CEILING_S))
    return max(0.0, ceiling - pool.HEARTBEAT_S)


def shard_ceiling(*, timeout_s: float | None, gpu: bool = False,
                  ceilings: dict[str, float | None] | None = None) -> float | None:
    """The deadline every shard seals as ``execution_timeout_s``; ``None`` seals none.

    The smaller of what the submitter asked for and the smallest ceiling a box
    that could claim the shard announces -- the ``min``
    ``pool._execution_timeout`` applies at claim.  So the sealed number is the
    deadline the shard runs under wherever it lands, and admission, which
    reads a holder's sealed request to judge whether it drains soon
    (``PoolQueue.holder_bound``), reads when the shard will actually end
    (#939).  Before #939 a shard sealed only an explicit ``--timeout-s``, and
    admission could read an unsealed shard by its age alone.

    It is the ceiling itself, not the per-test bound one heartbeat inside it.
    Sealing the bound would move the lease's deadline onto the instant the
    per-test alarm fires, and the node id the alarm writes would no longer
    reach the record while the action is alive.

    With no announcement and no ``--timeout-s`` it is ``None``, and the shard
    seals nothing, as before.  The published loop default is a guess about
    boxes that said nothing, and a sealed guess is a declared end nobody
    declared.

    A shard that does not reserve a GPU never runs campaign work, so an
    *unrequested* announcement is not this shard's own bound to inherit: it
    is capped at ``NON_GPU_UNREQUESTED_CEILING_CAP_S``, the largest end
    admission's own rule (``PoolQueue.holder_bound``) still reads as
    ``transient`` for a holder's *entire* life, whatever that end is (see the
    constant's own comment for the derivation).  A CPU-only shard tagged
    ``gb10`` sealed the bare 86,400 s campaign ceiling, so admission read it
    as ``long`` for the middle of its life and a starved GPU action lost its
    reservation to a same-band shard that in fact ran for minutes -- and
    capping at ``DEFAULT_EXECUTION_CEILING_S`` (7,200 s) alone does not fix
    that: it still reads ``long`` from 900 s to 6,300 s of a shard's life, the
    same shape of starvation, only shorter (#1123).  ``--timeout-s`` is a
    declared choice and is honoured uncapped; only the unrequested inheritance
    is corrected.
    """

    bounds = [value for value in (ceilings or {}).values() if value is not None]
    if timeout_s is not None:
        bounds.append(float(timeout_s))
    if not bounds:
        return None
    sealed = min(bounds)
    if not gpu and timeout_s is None:
        sealed = min(sealed, NON_GPU_UNREQUESTED_CEILING_CAP_S)
    return sealed


def discover(checkout: Path, paths: list[str]) -> list[str]:
    """Test files under the given paths, relative to the checkout."""

    found: list[Path] = []
    for raw in paths:
        target = checkout / raw
        if target.is_dir():
            found.extend(sorted(target.rglob("test_*.py")))
        elif target.is_file():
            found.append(target)
        else:
            raise ValueError(f"test path {raw!r} is not a file or directory in {checkout}")
    return [str(p.relative_to(checkout)) for p in dict.fromkeys(found)]


def shard(files: list[str], count: int) -> list[list[str]]:
    """Round-robin, so adjacent (and so similar) files land on different boxes."""

    count = max(1, min(count, len(files)))
    buckets: list[list[str]] = [[] for _ in range(count)]
    for index, name in enumerate(files):
        buckets[index % count].append(name)
    return buckets


#: The interpreter program every shard runs.  It carries the modules it runs
#: as text, so the action key names their bytes and no helper path has to
#: exist on the worker -- the rule the dependency guard already followed.
SHARD_PROGRAM = """\
# A pbtest shard: pytest under pbtest_outcomes' recorder.
import sys
import types

SOURCES = @SOURCES@


def load(name):
    module = types.ModuleType(name)
    module.__file__ = "<pbtest " + name + ">"
    sys.modules[name] = module
    exec(compile(SOURCES[name], module.__file__, "exec"), module.__dict__)
    return module


pins = load("pbtest_pins") if "pbtest_pins" in SOURCES else None
raise SystemExit(load("pbtest_outcomes").main(
    preflight=None if pins is None else pins.preflight))
"""


def shard_entry(python: str, checkout: Path) -> list[str]:
    """The argv that runs a shard's pytest under the outcome recorder.

    Every shard reports each counted outcome by node ID (#942), so every
    shard runs through ``pbtest_outcomes``.  A checkout that pins a reviewed
    dependency (``tools/resolve_<module>_dev_pin.py``) also carries the guard,
    which runs before pytest and refuses on drift.  Both travel as source in
    the argv, never as a path.  Reading either file can raise ``OSError``,
    which the caller reports.
    """

    here = Path(__file__)
    sources = {"pbtest_outcomes": here.with_name("pbtest_outcomes.py").read_text()}
    if any((checkout / "tools").glob("resolve_*_dev_pin.py")):
        sources["pbtest_pins"] = here.with_name("pbtest_pins.py").read_text()
    return [python, "-c", SHARD_PROGRAM.replace("@SOURCES@", repr(sources))]


def recorded_skips(record: dict | None) -> list[dict] | None:
    """Every skip a shard's record holds, with its reason; ``None`` without one."""

    if record is None:
        return None
    skips = []
    for row in record.get("reports") or ():
        entry = dict(zip(pbtest_outcomes.REPORT_FIELDS, row))
        if entry.get("category") == "skipped":
            skips.append({"nodeid": entry["nodeid"], "when": entry["when"],
                          "reason": entry.get("reason") or "",
                          "location": entry.get("location")})
    return skips


def summary_count(summary: str, word: str) -> int:
    """The ``N <word>`` count in a pytest summary line, ``0`` when absent."""

    match = re.search(rf"(?:^|[ =,])(\d+) {re.escape(word)}\b", summary)
    return int(match.group(1)) if match else 0


#: Summary parts that are not outcomes, though the same line prints them.
NOT_OUTCOMES = {"warning", "warnings", "deselected"}
_COLLECTED_COUNT = re.compile(r"^(?:no tests|(\d+)(?:/\d+)? tests?) collected\b")


def summary_outcomes(summary: str) -> tuple[dict[str, int] | None, int | None]:
    """A summary line's outcome counts by category, and its collected count.

    Outcomes are keyed the way the outcome record keys them: pytest prints
    ``N error``/``N errors`` for the ``error`` category, and every other
    counted word is the category itself.  A ``--collect-only`` summary
    counts items, not outcomes, and answers ``(None, N)``.
    """

    line = ANSI.sub("", summary).strip()
    line = re.sub(r"^=+ | =+$", "", line)
    body = line.rsplit(" in ", 1)[0]
    collected = _COLLECTED_COUNT.match(body)
    if collected:
        return None, int(collected.group(1) or 0)
    counts: dict[str, int] = {}
    for part in body.split(", "):
        match = re.fullmatch(r"(\d+) (.+)", part)
        if match is None or match.group(2) in NOT_OUTCOMES:
            continue
        word = "error" if match.group(2) == "errors" else match.group(2)
        counts[word] = counts.get(word, 0) + int(match.group(1))
    return counts, None


def reconcile_shards(results: list[dict]) -> None:
    """Reconcile each shard by node ID, and the shards with each other (#941).

    Each shard that reported a summary gets ``reconciliation``: its own
    collection matched against its outcomes (``pbtest_outcomes.reconcile``).
    A shard that reported a summary and printed no outcome record cannot be
    reconciled, and says so.  Then every node ID collected by two shards is a
    problem of each: files are disjoint across shards, so a test in two of
    them ran twice.
    """

    owners: dict[str, list[int]] = {}
    for result in results:
        if not result["ran"]:
            result["reconciliation"] = None
            continue
        record = pbtest_outcomes.parse(result["output"])
        if record is None:
            result["reconciliation"] = {"problems": [
                "no outcome record: the shard's tests cannot be matched "
                "against its collection"]}
            continue
        counts, collected = summary_outcomes(result["summary"])
        result["reconciliation"] = pbtest_outcomes.reconcile(record, counts, collected)
        for nodeid in dict.fromkeys(record.get("collected") or ()):
            owners.setdefault(nodeid, []).append(result["shard"])
    for nodeid, shards in owners.items():
        if len(shards) < 2:
            continue
        for result in results:
            if result["shard"] in shards:
                reconciliation = result["reconciliation"]
                reconciliation.setdefault("in_other_shards", []).append(nodeid)
                message = "node ID(s) also collected by another shard"
                if not any(message in problem for problem in reconciliation["problems"]):
                    reconciliation["problems"].append(message)


def _names(label: str, names, limit: int = 20) -> None:
    names = list(names)
    for name in names[:limit]:
        print(f"    {label} {name}")
    if len(names) > limit:
        print(f"    ... and {len(names) - limit} more {label}; the --json "
              "report lists them all")


def print_reconciliation(results: list[dict]) -> None:
    """The run's reconciliation: totals, how a summary exceeds its tests, and
    every shard that did not reconcile, by name."""

    reconciled = [r for r in results if r.get("reconciliation")
                  and "collected" in r["reconciliation"]]
    if reconciled:
        total = {key: sum(r["reconciliation"][key] for r in reconciled)
                 for key in ("collected", "ran", "outcomes")}
        at_collection = [(r["shard"], item) for r in reconciled
                         for item in r["reconciliation"]["at_collection"]]
        extra = [(r["shard"], nodeid, kinds) for r in reconciled
                 for nodeid, kinds in r["reconciliation"]["extra_phases"].items()]
        extra_count = sum(len(kinds) - 1 for _, _, kinds in extra)
        print(f"\nreconciliation: {total['collected']} collected, {total['ran']} "
              f"ran; the summaries count {total['outcomes']} outcome(s) = "
              f"{total['outcomes'] - len(at_collection) - extra_count} test(s) + "
              f"{len(at_collection)} at collection + {extra_count} extra phase(s)")
        for index, item in at_collection:
            print(f"  shard {index:>3} at collection: {item['nodeid']} "
                  f"{item['category']}"
                  + (f" - {item['reason']}" if item["reason"] else ""))
        for index, nodeid, kinds in extra:
            print(f"  shard {index:>3} counted {len(kinds)} times: {nodeid} "
                  f"({', '.join(kinds)})")
    for result in results:
        reconciliation = result.get("reconciliation")
        if not reconciliation or not reconciliation["problems"]:
            continue
        print(f"UNRECONCILED shard {result['shard']:>3}: "
              + "; ".join(reconciliation["problems"]))
        _names("never ran:", reconciliation.get("never_ran") or ())
        _names("not collected:", reconciliation.get("not_collected") or ())
        _names("collected twice:", reconciliation.get("collected_twice") or ())
        _names("also in another shard:", reconciliation.get("in_other_shards") or ())


def displayed(output: str) -> list[str]:
    """A shard's output lines for a human, without its outcome record."""

    return [line for line in (output or "").strip().splitlines()
            if not line.startswith(pbtest_outcomes.PREFIX)]


# A closed vocabulary prevents resource controls, config indirection, and
# extra file populations from hiding in forwarded arguments. Extend this list
# deliberately for new plugins, after checking their execution semantics.
PYTEST_SWITCHES = {"--strict-cuda", "--strict-markers", "--strict-config",
                   "--collect-only", "--co", "--disable-warnings", "-x"}
PYTEST_VALUES = {"-k", "-m", "--dist", "--surface-json", "--durations",
                 "--durations-min", "--maxfail", "--tb"}


def parse_pytest_args(raw: str, *, gpu: bool, workers: int) -> list[str]:
    """Validate and normalize a JSON argv without importing target plugins."""
    values = json.loads(raw)
    if not isinstance(values, list) or any(
        not isinstance(v, str) or not v or "\0" in v for v in values
    ):
        raise ValueError("--pytest-args must be a JSON array of nonempty strings")
    result: list[str] = []
    index = 0
    while index < len(values):
        option, equals, value = values[index].partition("=")
        index += 1
        if option in PYTEST_SWITCHES and not equals:
            if option == "--strict-cuda" and not gpu:
                raise ValueError("--strict-cuda requires --gpu")
            result.append(option)
            continue
        if option not in PYTEST_VALUES:
            raise ValueError(f"unsupported pytest option {option!r}; use "
                             "--workers-per-shard for parallelism and paths for files")
        if not equals:
            if index == len(values):
                raise ValueError(f"{option} requires a value")
            value = values[index]
            index += 1
        if not value or value.startswith("-"):
            raise ValueError(f"{option} requires a nonempty value, not another option")
        if option == "--dist":
            if workers == 1:
                raise ValueError("--dist requires --workers-per-shard greater than 1")
            if value not in {"load", "loadscope", "loadfile", "loadgroup", "worksteal"}:
                raise ValueError("--dist must partition tests; 'each' duplicates the population")
        if option == "--surface-json" and not Path(value).name:
            raise ValueError("--surface-json requires a filename")
        result += [option, value]
    return result


def shard_pytest_args(arguments: list[str], index: int) -> list[str]:
    """Give report outputs stable, distinct names even on shared storage."""
    result = list(arguments)
    for position, option in enumerate(arguments):
        if option != "--surface-json":
            continue
        raw = arguments[position + 1]
        path = Path(raw)
        result[position + 1] = (
            raw.replace("{shard}", str(index)) if "{shard}" in raw else
            str(path.with_name(f"{path.stem}.shard-{index}{path.suffix}"))
        )
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--checkout", required=True,
                    help="Git tree to snapshot and test on the pool")
    ap.add_argument("--python", required=True,
                    help="interpreter on the TARGET box, not this one")
    ap.add_argument("--tag", action="append", default=[],
                    help="placement tag; published default is x86, or gb10 with --gpu")
    ap.add_argument("--shards", type=int, default=20,
                    help="how many actions the suite is split into, "
                         "round-robin over the discovered files; more than "
                         "there are files is lowered to one shard per file")
    ap.add_argument("--workers-per-shard", type=int, default=1,
                    help="pytest workers in each action; above 1 uses pytest-xdist "
                         "(-n N), which must be installed in the target interpreter")
    ap.add_argument("--threads-per-shard", type=int, default=2,
                    help="BLAS/OMP threads each pytest worker may use; 0 leaves it "
                         "alone and then --cpus-per-shard is required")
    ap.add_argument("--cpus-per-shard", type=int, default=None,
                    help="cores each shard reserves; the default is "
                         "--workers-per-shard times --threads-per-shard, so the ceiling a shard is given "
                         "is the ceiling it can use. Required with "
                         "--threads-per-shard 0, which sets no ceiling at all")
    ap.add_argument("--mem-gb", type=int, default=3,
                    help="memory each shard demands of its box")
    ap.add_argument("--gpu", action="store_true",
                    help="request a GPU for every shard; a tag alone does not "
                         "request one. Requires --timeout-s or --test-timeout-s, "
                         "so each test's bound is the submitter's and never a "
                         "box's campaign ceiling (#975)")
    ap.add_argument("--gpu-memory-gb", type=float, default=None,
                    help="per-shard GPU memory budget, requires --gpu and pool transport")
    ap.add_argument("--pytest-args", default=None,
                    help="JSON array of population/report options; resource and config "
                         "overrides are refused. --surface-json paths get a shard suffix "
                         "or expand {shard}. Replaces pytest addopts when supplied")
    ap.add_argument("--timeout-s", type=float, default=None,
                    help="each shard's deadline. Every shard seals the smaller "
                         "of this and the smallest ceiling a box able to claim "
                         "it announces, which admission reads as its declared "
                         "end (#939); unset seals that ceiling, and nothing "
                         "if no box announces one -- except a non-GPU shard, "
                         "which caps an unrequested announcement at twice the "
                         "pool's own withhold ceiling, the largest end "
                         "admission still reads as draining soon for the "
                         "shard's whole life, never a box's campaign ceiling "
                         "(#1123)")
    ap.add_argument("--test-timeout-s", type=float, default=None,
                    help="per-test bound for every shard, in seconds; the "
                         "default is derived from the shard's own execution "
                         "ceiling and 0 disables the bound. A --gpu run with "
                         "no --timeout-s must pass it (#975). A test that "
                         "outlives it fails, named, instead of holding the "
                         "shard's slot to the ceiling (#600). Tighten it only "
                         "on a measured shard duration")
    ap.add_argument("--wait-s", type=float, default=10800.0,
                    help="how long each shard waits for the fleet to run it, "
                         "queueing included; forwarded to pbrun")
    ap.add_argument("--priority", type=int, default=0,
                    help="queue hint forwarded to every shard's pbrun; higher "
                         "runs sooner, negative yields to everything at 0 and "
                         "aging never lifts it past them (#362). Not part of "
                         "the action identity")
    ap.add_argument("--profile", default=None,
                    help="forwarded to every shard's pbrun --profile; a mode "
                         "runs a profiler around each shard's pytest and files "
                         "the profile as a CAS blob. It IS part of each "
                         "shard's action identity, so a profiled suite run is "
                         "a different set of actions from an unprofiled one "
                         "and never a cache hit for it")
    ap.add_argument("--json", default="", help="write the per-shard result here")
    ap.add_argument(
        "--transport", choices=TRANSPORTS, default=default_transport(),
        help="which dispatcher carries the shards (env PRISMABUILD_TRANSPORT, "
             "else the published runtime generation's default_transport); "
             "forwarded to pbrun unchanged")
    # pbrun does know --snapshot-ref, and this tool deliberately does not
    # forward it.  An advertised ref exists so an action can spell a branch
    # name, and a shard's command is pytest over repository-relative paths,
    # which spells none.  Forward it when a shard has something to spell.
    ap.add_argument("paths", nargs="*", default=["tests"],
                    help="test files or directories to shard, relative to "
                         "--checkout; a directory contributes every "
                         "test_*.py under it")
    args = ap.parse_args()

    try:
        if args.mem_gb < 1:
            raise ValueError("--mem-gb must be at least 1")
        require_gpu_memory_scope(gpu_memory_gb=args.gpu_memory_gb,
                                 gpu=args.gpu, transport=args.transport)
        if args.gpu_memory_gb is not None:
            adaptive_gpu.memory_budget_bytes(args.gpu_memory_gb)
        pytest_args = (parse_pytest_args(args.pytest_args, gpu=args.gpu,
                                        workers=args.workers_per_shard)
                       if args.pytest_args is not None else [])
    except ValueError as exc:
        sys.stderr.write(f"pbtest: {exc}\n")
        return 2
    # A test's bound belongs to the suite and its submitter, not to the
    # longest job a box accepts (#975).  Derived from the announced ceilings,
    # a GPU shard's bound was one heartbeat inside the Sparks' campaign
    # ceiling, 86370 s, so a hung test held its shard and its GPU for a day
    # before pytest named it.  So a shard that reserves a GPU takes its bound
    # from the submission, and with none there is nothing to derive it from.
    if args.gpu and args.timeout_s is None and args.test_timeout_s is None:
        sys.stderr.write(
            "pbtest: a --gpu shard needs a declared bound, because the "
            "ceiling a box announces may be the one it accepts for campaign "
            "work and a hung test would hold its GPU that long: pass "
            "--timeout-s (the shard's deadline; each test is bounded one "
            "heartbeat inside it) or --test-timeout-s (each test's own bound; "
            "0 removes it) (#975)\n")
        return 2

    if PBRUN is None:
        looked = " and ".join(
            str(candidate)
            for candidate in tool_candidates("pbrun.py", root=RUNTIME_ROOT)
        )
        sys.stderr.write(
            f"no pbrun.py to submit shards through; looked for {looked}\n")
        return 2

    # A thread ceiling is not a reservation.  Under SLURM the lane emits the
    # sealed cpu demand as --cpus-per-task, and cgroup.conf's
    # ConstrainCores=yes turns that into a cpuset, so eight threads inside a
    # one-core cpuset are eight threads taking turns on one core; under the
    # pull queue the ledger admits the shard as if it used one.  So the two
    # travel together, and 0 threads, which asks for no ceiling at all, has
    # no reservation to derive and must be told one.
    if args.workers_per_shard < 1:
        sys.stderr.write("--workers-per-shard must be at least 1\n")
        return 2
    if args.threads_per_shard < 0:
        sys.stderr.write("--threads-per-shard cannot be negative\n")
        return 2
    if args.cpus_per_shard is None:
        if args.threads_per_shard == 0:
            sys.stderr.write(
                "--threads-per-shard 0 leaves every shard's thread pool "
                "unbounded, so nothing here can say how many cores to "
                "reserve for it: pass --cpus-per-shard N as well, or name a "
                "thread ceiling and let it answer both\n")
            return 2
        cpus_per_shard = args.workers_per_shard * args.threads_per_shard
    else:
        cpus_per_shard = args.cpus_per_shard
    if cpus_per_shard < 1:
        sys.stderr.write("--cpus-per-shard must be at least 1\n")
        return 2

    minimum_cpus = args.workers_per_shard * max(1, args.threads_per_shard)
    if cpus_per_shard < minimum_cpus:
        sys.stderr.write(
            f"--cpus-per-shard must be at least {minimum_cpus} for "
            f"{args.workers_per_shard} workers and the declared thread ceiling\n")
        return 2

    checkout = Path(args.checkout).resolve()
    try:
        files = discover(checkout, args.paths or ["tests"])
    except (OSError, ValueError) as exc:
        sys.stderr.write(f"cannot discover tests: {exc}\n")
        return 2
    if not files:
        sys.stderr.write(f"no test files under {args.paths} in {checkout}\n")
        return 2
    buckets = shard(files, args.shards)
    # Resolve reviewed dependencies only inside each admitted worker action.
    # Embed the guard bytes in argv so neither a target-local helper path nor
    # an older receipt can silently omit the check. Unpinned projects retain
    # their existing commands and identities.
    try:
        python_entry = shard_entry(args.python, checkout)
    except OSError as exc:
        sys.stderr.write(f"pbtest: cannot load the shard program: {exc}\n")
        return 2
    # ``pbrun`` transports the checkout but never itself: it seals its own
    # ``prismabuild_worker.py`` as an absolute path into the action.  Out of
    # the published runtime that path is on every box; out of a worktree it is
    # on one -- and ``x86`` is an EXPLICIT tag, which by design outranks the
    # host pin ``pbrun`` would otherwise derive for exactly this reason.  So
    # the default was not "prefer the idle x86 cores", it was "override the
    # only correct placement", and the four shards died on dl380g10 with
    # ``can't open file '<worktree>/tools/prismabuild_worker.py'`` (#292).
    # Say nothing instead and let ``pbrun`` answer; it pins to this box.
    tags = args.tag or ([] if pool.is_box_local_path(RUNTIME_ROOT) else
                        ["gb10" if args.gpu else "x86"])
    sizes = [len(b) for b in buckets]
    print(f"{len(files)} files -> {len(buckets)} shards "
          f"(min {min(sizes)}, max {max(sizes)} files per shard), tags={tags}, "
          f"transport={args.transport}",
          flush=True)

    # torch sizes its thread pool from the affinity mask, so an unconstrained
    # shard on an 80-core box asks for 40 threads -- forty shards then ask for
    # 1,600 and the box spends its time context-switching.  Measured: 22 shards
    # at the default put dl380g10 at load 183 with 266 runnable processes and
    # 97% user, CPU-saturated while still holding 105 GB free.  Not an OOM, but
    # not work either.
    threads = []
    if args.threads_per_shard > 0:
        n = str(args.threads_per_shard)
        threads = [f"OMP_NUM_THREADS={n}", f"MKL_NUM_THREADS={n}",
                   f"OPENBLAS_NUM_THREADS={n}", f"TORCH_NUM_THREADS={n}"]

    pytest_workers = (["-n", str(args.workers_per_shard)]
                      if args.workers_per_shard > 1 else [])

    # One read of the announcements serves both numbers below, so the shard's
    # sealed deadline and the per-test bound inside it cannot disagree.
    ceilings = announced_ceilings(tags)
    sealed_s = shard_ceiling(timeout_s=args.timeout_s, gpu=args.gpu, ceilings=ceilings)
    announced = ", ".join(f"{host} {value:g}s" for host, value in sorted(ceilings.items())
                          if value is not None) or "none"
    if sealed_s is None:
        print("pbtest: no --timeout-s and no claimant announced a ceiling; the "
              "shards seal no deadline and run under their box's own", flush=True)
    else:
        print(f"pbtest: each shard seals execution_timeout_s={sealed_s:g} "
              f"(--timeout-s {'unset' if args.timeout_s is None else f'{args.timeout_s:g}'}; "
              f"ceilings announced: {announced}); admission reads it as the "
              "shard's declared end (#939)", flush=True)
        if args.timeout_s is not None and sealed_s < args.timeout_s:
            print(f"pbtest: --timeout-s {args.timeout_s:g} exceeds the ceiling a "
                  f"claimant announces, which would cut the shard at {sealed_s:g}s "
                  "anyway; that is the deadline sealed", flush=True)
    test_bound_s = per_test_bound(
        timeout_s=args.timeout_s, override_s=args.test_timeout_s, gpu=args.gpu,
        ceilings=ceilings)
    test_bound = ([f"{pytest_test_bound.TIMEOUT_ENV}={test_bound_s:g}"]
                  if test_bound_s > 0 else [])
    if test_bound:
        print(f"pbtest: per-test bound {test_bound_s:g}s "
              f"({pytest_test_bound.TIMEOUT_ENV}); a test that outlives it "
              "fails as itself instead of holding the shard to its ceiling",
              flush=True)
    procs = []
    for index, bucket in enumerate(buckets):
        # Built in order rather than spliced into.  The repeatable --tag used
        # to be inserted at a fixed index, which once landed between --demand
        # and its argument and killed every shard on "expected one argument";
        # appending each flag where it belongs cannot reach inside a pair.
        flags = [
            "/usr/bin/python3", str(PBRUN),
            "--cwd", str(checkout),
            "--transport", args.transport,
        ]
        # The class tag is the whole placement claim, and ``--anywhere``
        # beside it is the contradiction ``pbrun`` refuses: portable, but
        # only on x86.  It also bought nothing.  ``placement_tags`` returns
        # the explicit tags before it reads ``--anywhere``, and
        # ``partition_for`` answers the default partition for tagged work
        # either way, so the shards keep their placement and their keys.
        for tag in tags:
            flags += ["--tag", tag]
        flags += [
            "--demand", f"mem_gb={args.mem_gb}",
            # pbrun fills the cpu demand from --cpus, and its default is 1.
            # Naming it here is what makes the reservation match the thread
            # ceiling above; it is sealed into the action's params, so a suite
            # re-run at a different width is a different action rather than a
            # cache hit.
            "--cpus", str(cpus_per_shard),
        ]
        if args.gpu:
            flags += ["--gpu"]
        if args.gpu_memory_gb is not None:
            flags += ["--gpu-memory-gb", str(args.gpu_memory_gb)]
        if sealed_s is not None:
            # ``str`` of the float, the spelling an explicit ``--timeout-s``
            # was always forwarded in, so a request inside every announced
            # ceiling reaches ``pbrun`` byte for byte as before (#939).
            flags += ["--timeout-s", str(sealed_s)]
        flags += ["--wait-s", str(args.wait_s)]
        if args.priority != 0:
            # Zero is pbrun's own default; forwarding only a non-zero hint
            # leaves every existing shard argv byte-identical.
            flags += ["--priority", str(args.priority)]
        if args.profile is not None:
            # Same rule, and it matters more here: --profile enters the shard's
            # action key, so forwarding it unasked would re-key every suite run
            # on the fleet.  pbrun owns which modes are legal and refuses the
            # rest, so this passes the word through rather than listing them.
            flags += ["--profile", str(args.profile)]
        # Explicit forwarding replaces addopts from both environment and
        # project config: either can hide -n auto or --dist each. The original
        # no-forwarding command remains byte-identical for existing receipts.
        explicit_env = ["PYTEST_ADDOPTS="] if args.pytest_args is not None else []
        explicit_options = ["-o", "addopts="] if args.pytest_args is not None else []
        command = flags + [
            "--", "env", "TMPDIR=/home/rob/tmp",
            *threads, *test_bound, *explicit_env,
            "PYTHONPATH=src:experiments",
            *python_entry, "-q", "--no-header",
            "-p", "no:cacheprovider", *explicit_options,
            *shard_pytest_args(pytest_args, index), *pytest_workers, *bucket,
        ]
        # The shard's own wait budget, spanning every attempt (#1102).
        deadline = time.monotonic() + args.wait_s
        # ``errors="replace"``: a stray byte must not end the drain thread,
        # which would leave the pipe to fill and the shard's wait blocked.
        proc = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            errors="replace")
        shard_result: dict = {}
        drain = threading.Thread(
            target=run_shard, args=(index, command, proc),
            kwargs={"wait_s": args.wait_s, "deadline": deadline,
                    "result": shard_result, "files": bucket},
            name=f"pbtest-shard-{index}", daemon=True)
        drain.start()
        procs.append((index, bucket, drain, shard_result))

    results = []
    for index, bucket, drain, shard_result in procs:
        # EOF comes when the shard's last pbrun exits, so the join is the wait.
        drain.join()
        lines = shard_result["lines"]
        attempts = shard_result["attempts"]
        returncode = shard_result["returncode"]
        out = "".join(lines)
        # A cache hit prints the receipt where the shard's stdout would be, so
        # look through it to the payload before asking whether pytest reported.
        out = replayed_output(out or "") or (out or "")
        tail = [line for line in out.strip().splitlines() if line.strip()]
        summary = pytest_summary(tail)
        # A shard whose pytest never reported is a shard whose tests never
        # ran, and it is not the same event as a shard that ran clean -- but
        # with an empty summary it printed the same blank space, which is how
        # a submission killed before it queued anything (#208) cost 74 tests
        # silently.  Say the count that did not run; do not let the reader
        # infer it from an absence.
        # ``ran`` now means "pytest reported a terminal summary", which is the
        # question the flag is actually asked.  It used to mean "some line
        # mentioned passing, failing or an error", and those are not the same
        # claim: the second is true of a shard that died in argparse.
        ran = bool(summary)
        if not ran:
            # Name how it ended as well as that it did not run.  A shard killed
            # by a signal, one that timed out, and one whose pbrun refused to
            # submit all printed the same sentence, and the reader had to go
            # find the returncode elsewhere to tell them apart.
            rc = returncode
            how = f"signal {-rc}" if rc < 0 else f"rc={rc}"
            unobserved = unobserved_outcome(tail) if rc == 74 else None
            if unobserved is not None:
                # pbrun could not read the shard's ending. The action may have
                # run to completion or may still be running, so "did not run"
                # would be a claim nobody observed. It is still not green.
                summary = (f"OUTCOME UNOBSERVED -- {len(bucket)} file(s) have no "
                           f"observed result (rc=74; action {unobserved} may still "
                           "be running or may already have landed; read "
                           f"pb-queue/{{done,failed,withdrawn}}/{unobserved}*.json "
                           "before rerunning)")
            else:
                summary = (f"NO PYTEST SUMMARY -- {len(bucket)} file(s) did not run "
                           f"(the shard ended {how}, before or outside pytest)")
        # Each skip by node ID, with its reason (#942).  ``None`` is "this
        # shard printed no record", which is not "it skipped nothing".
        skipped = recorded_skips(pbtest_outcomes.parse(out))
        # The key as structured fields beside the returncode, so a report can
        # cite the action rather than a prefix dug out of ``output`` (#1012).
        action_key = shard_result.get("action_key")
        results.append({"shard": index, "files": bucket,
                        "returncode": returncode, "action_key": action_key,
                        "receipt_path": receipt_path(action_key),
                        "summary": summary,
                        "ran": ran, "skipped": skipped, "attempts": attempts,
                        "output": out})
        state = "ok" if returncode == 0 else f"rc={returncode}"
        # A resubmitted shard says so on its own line, so the receipt records
        # that its first submission was refused (#1102).
        retried = (f" [attempt {attempts}; {attempts - 1} earlier submission(s) "
                   "refused by a worker-offer discovery timeout]"
                   if attempts > 1 else "")
        keyed = (f" [action {action_key}]" if action_key else
                 " [no action key: pbrun printed no queued or attached line]")
        _say(f"shard {index:>3} {state:<8} {summary}{retried}{keyed}")
        counted = summary_count(summary, "skipped") if ran else 0
        if skipped is None and counted:
            print(f"shard {index:>3} {counted} skip(s) with NO RECORDED REASON: "
                  "the shard printed no outcome record", flush=True)
        elif skipped is not None and len(skipped) != counted:
            print(f"shard {index:>3} records {len(skipped)} skip(s) and its "
                  f"summary counts {counted}", flush=True)

    skips = [(r["shard"], skip) for r in results for skip in r["skipped"] or ()]
    if skips:
        print(f"\n{len(skips)} skipped, each with its reason:")
        for index, skip in skips:
            where = f" ({skip['location']})" if skip["location"] else ""
            when = "" if skip["when"] in ("setup", "call") else f" [{skip['when']}]"
            print(f"  shard {index:>3} {skip['nodeid']}{when} - "
                  f"{skip['reason'] or '(no reason given)'}{where}")

    reconcile_shards(results)
    print_reconciliation(results)

    # A shard is green when it exited 0 AND pytest reported a terminal summary.
    # ``ran`` has been computed, printed and written to the JSON since #213, and
    # the verdict never read it: ``returncode != 0`` alone called a shard that
    # started no test green, and returned 0 to whoever was deciding a merge on
    # it.  That is the #208 defect one level in -- the diagnostic improved and
    # the thing acting on it did not read the diagnostic.  And it reconciled:
    # a collected test with no outcome, or one two shards ran, is a missing or
    # doubled result that no exit code reports (#941).
    failed = [r for r in results if r["returncode"] != 0 or not r["ran"]
              or (r.get("reconciliation") or {}).get("problems")]
    print(f"\n{len(results) - len(failed)}/{len(results)} shards green")
    for r in failed:
        print(f"\n--- shard {r['shard']} ({', '.join(r['files'])})")
        print("\n".join(displayed(r["output"])[-25:]))
    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=1))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

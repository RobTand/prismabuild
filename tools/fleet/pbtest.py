"""Fan a test suite out across the pool instead of running it on one box.

The coordinator discovers files and submits independent shards; PB chooses
their placement and concurrency. CPU suites are portable across workers that
have their named interpreter; explicit GPU suites default to GB10. Each shard
reserves its aggregate demand.

Four constraints shape this:

* **The checkout is transported by pbrun.** ``pbrun`` seals a Git snapshot in
  the CAS and each worker materializes it on local disk. The payload therefore
  uses only repository-relative paths; embedding the submitter's checkout path
  would escape that snapshot and is refused before publication.
* **The interpreter is named, not inherited.** The absolute Python entry is
  first in the sealed command. Its exact path also enters the queue row and
  capability fence through pbrun's existing producer contract. A class tag is
  an additional explicit constraint, never the interpreter's dependency proof.
* **A pass/fail here is not a measurement.** x86 against aarch64 can use
  different BLAS and FMA orders. A suite requiring an architecture names it
  with ``--tag``; portable CPU suites allow every capable worker. Performance
  or numeric measurement arms still need their explicit workload contract.
* **A shard reserves what it is allowed to use.**  ``--threads-per-shard``
  sets each pytest worker's BLAS and OMP ceiling. Multiplying that by
  ``--workers-per-shard`` gives ``pbrun --cpus``, which the lane emits as ``--cpus-per-task``.  A ceiling
  without a reservation is threads taking turns inside one core, because
  ``ConstrainCores=yes`` makes the declared demand a cpuset.
  ``--cpus-per-shard`` overrides the pairing, and it is required when the
  ceiling is 0.  A shard of several pytest workers with no ceiling of its
  own gives each worker its share of those cores: each xdist worker would
  otherwise inherit the whole row's thread count (#1192).

Shards use receipt-backed duration history when available and round-robin by
file otherwise. The report records predicted and actual durations so a bad
estimate or an unbalanced shard remains visible.
"""
from __future__ import annotations

import argparse
import ast
import base64
from dataclasses import replace
import functools
import importlib.util
import io
import json
import math
import os
import re
import statistics
import subprocess
import sys
import threading
import time
import tempfile
import traceback
import uuid
import zlib
from pathlib import Path, PurePath

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
_PB_ENTRYPOINT = Path(__file__).resolve(strict=True)
sys.path.insert(0, str(_PB_ENTRYPOINT.parents[
    2 if _PB_ENTRYPOINT.parent.name == "fleet" else 1] / "src"))
from prismabuild.runtime_paths import (  # noqa: E402
    fleet_tool, generation_root, tool_candidates,
)

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import action_client, client_channel, client_recovery  # noqa: E402
from prismabuild.submission_frontend import store_for, render_submission, request_arguments, SUBMISSION_SCHEMA_V1
from prismabuild.submission_types import (
    OfferDiscoveryTimedOut, SubmissionRequest, SubmissionRefused,
)


from prismabuild import action_wait  # noqa: E402
from prismabuild import contracts
from prismabuild import adaptive_gpu, core, movement_actions, owned_tree, pbtest_scratch_roots, pool  # noqa: E402
from prismabuild import dependency_digest, fleet_roster, pbtest_collection  # noqa: E402
from prismabuild.submission import require_gpu_memory_scope  # noqa: E402
#: The capability declarations and cohort rules for this tool (#1495): one
#: module, so the coordinator's classification and the CLI's contract cannot
#: drift apart.
from prismabuild import pbtest_capabilities  # noqa: E402
#: Read for ``pbrun.SH`` when the queue is asked, so the conftest's repointing
#: of the live store reaches it: see :func:`announced_ceilings`.
from prismabuild import submission
#: The submitter each shard is started through, under whichever layout the
#: runtime containing this file uses.  ``None`` when neither layout has one.
PBRUN = fleet_tool("pbrun.py", root=RUNTIME_ROOT)
SHARED = Path("/mnt/shared")
#: Each shard holds one pbrun client on the submitting box for its whole wait,
#: about this much resident memory apiece (#1348).
CLIENT_RSS_MB = 80
#: Most clients kept resident at once; 24 shards would otherwise pin ~2 GB on a
#: box whose watchdog reads box-wide RAM.  ``--max-clients 0`` restores one per
#: shard.
DEFAULT_MAX_CLIENTS = 8
SHARD_THREAD_ENV = ("OMP_NUM_THREADS", "MKL_NUM_THREADS",
                    "OPENBLAS_NUM_THREADS", "TORCH_NUM_THREADS")

#: The prefixes of the lines a shard's submitter prints about its own wait.
#: They are streamed as they arrive (#1048): a patient ``pbrun`` can wait out a
#: reader stuck in the kernel for all of ``--wait-s``, and says so every
#: minute, but a line that sits in a pipe until the shard ends says nothing.
STREAMED_PREFIXES = ("pbrun:", "pbstatus:")

_PRINT_LOCK = threading.Lock()


def _say(text: str) -> None:
    """Print one whole line; shards' drain threads print beside the main one."""

    with _PRINT_LOCK:
        print(text, flush=True)


def drain_shard(index: int, stream, lines: list[str]) -> None:
    """Stream human diagnostics while retaining the shard's complete output."""
    for line in iter(stream.readline, ""):
        lines.append(line)
        if line.startswith(STREAMED_PREFIXES):
            _say(f"shard {index:>3} {line.rstrip()}")


def _shard_result(result: dict) -> list[str]:
    result.setdefault('lines', [])
    result.setdefault('attempts', 1)
    result.setdefault('returncode', None)
    result.setdefault('action_key', None)
    return result['lines']


def _announce_submission(index: int, identity, *, result: dict, files=(),
                         progress=None) -> None:
    """Record and print the action a shard's first submission names."""
    if result['action_key'] is not None:
        return
    result['action_key'] = identity.action_key
    if progress is not None:
        progress.observe(index, result)
    if isinstance(identity, action_client.CachedClientIdentity):
        verb = 'cached'
    else:
        verb = ('queued' if identity.arrival is action_client.ClientArrival.SUBMITTED
                else 'attached')
    _say(f"shard {index:>3} action {identity.action_key} ({verb}): " + " ".join(files))


def start_shard(index: int, command: list[str], *, result: dict,
                files=(), progress=None, expected_identity=None) -> client_channel.ClientProcess:
    _shard_result(result)

    def submitted(identity):
        _announce_submission(index, identity, result=result, files=files, progress=progress)

    environment = _client_environment()
    if expected_identity is not None:
        inherited = environment.get('PYTHONPATH')
        environment['PYTHONPATH'] = str(RUNTIME_ROOT / 'src') + (
            os.pathsep + inherited if inherited else '')
    return client_channel.ClientProcess.launch(command, queue_root=store_for(_PB_ENTRYPOINT).root / 'pb-queue',
        cas_root=store_for(_PB_ENTRYPOINT).root / 'cas', env=environment, expected_identity=expected_identity,
        on_identity=submitted)


def run_shard(index: int, first, *, again, wait_s: float,
              deadline: float, result: dict, files: list[str] = (), progress=None) -> None:
    """Retry only a typed unpublished offer timeout or recover one exact pool run.

    ``first`` is the shard's first attempt, already started; ``again`` starts
    another one -- a new publication, never a second copy of a published one.
    """
    lines = result['lines']
    running = first
    while True:
        drain_shard(index, running.process.stdout, lines)
        events = running.wait()
        result['returncode'] = running.process.returncode
        result['client_events'] = events
        recovery = client_recovery.command(events, deadline=deadline)
        if recovery is not None:
            _say(f"shard {index:>3} pbtest: recovering the same generation's "
                 "unobserved outcome inside the original deadline; not resubmitting")
            try:
                observer = start_shard(index, recovery, result=result, files=files,
                    progress=progress, expected_identity=events.identity)
            except OSError as exc:
                _say(f"shard {index:>3} pbtest: cannot start outcome recovery: {exc}")
                return
            drain_shard(index, observer.process.stdout, lines)
            result['client_events'] = observer.wait()
            result['returncode'] = observer.process.returncode
            return
        if (events.problem is not None or events.ending is None
                or not isinstance(events.ending.verdict, action_client.ClientOfferTimeout)):
            return
        remaining = deadline - time.monotonic()
        if remaining <= action_wait.POLL_S:
            _say(f"shard {index:>3} pbtest: worker-offer discovery timed out on "
                 f"attempt {result['attempts']}; {max(remaining, 0.0):.0f}s of "
                 f"--wait-s {wait_s:g} left, not resubmitting")
            return
        result['attempts'] += 1
        if progress is not None:
            progress.observe(index, result)
        _say(f"shard {index:>3} pbtest: worker-offer discovery timed out and "
             f"nothing was published; attempt {result['attempts']} in "
             f"{action_wait.POLL_S:g}s ({remaining:.0f}s of --wait-s {wait_s:g} left)")
        time.sleep(action_wait.POLL_S)
        try:
            running = again()
        except OSError as exc:
            _say(f"shard {index:>3} pbtest: could not start attempt "
                 f"{result['attempts']}: {exc}")
            result['attempts'] -= 1
            return



def _detach_record(text: str) -> bool:
    """Whether a display line is ``pbrun``'s JSON record for a detached caller.

    A shard is published detached and observed separately, so that record is
    this process's bookkeeping, never a line the shard's attached ``pbrun``
    printed; its stdout carried nothing else (``detached_submission_record``).
    """

    try:
        value = json.loads(text)
    except ValueError:
        return False
    return (isinstance(value, dict)
            and value.get("schema") in (action_client.DETACH_SCHEMA_V1, SUBMISSION_SCHEMA_V1))


class _ShardLines:
    """Submission display events, as the lines a shard's ``pbrun`` printed.

    ``pbrun`` wrote stdout and stderr to one text pipe that ``drain_shard``
    read line by line, with universal newlines, streaming its ``pbrun:``
    lines as they came (#1048).  This is that pipe without the process.
    """

    def __init__(self, index: int | None, lines: list[str]) -> None:
        self.index = index
        self.lines = lines
        self.pending = ""

    def __call__(self, event) -> None:
        if event.channel == "stdout" and _detach_record(event.text):
            return
        self.write(event.text + event.end)

    def write(self, text: str) -> None:
        text = (self.pending + text).replace("\r\n", "\n").replace("\r", "\n")
        *complete, self.pending = text.split("\n")
        for line in complete:
            self.line(line + "\n")

    def line(self, line: str) -> None:
        self.lines.append(line)
        if self.index is not None and line.startswith(STREAMED_PREFIXES):
            _say(f"shard {self.index:>3} {line.rstrip()}")

    def close(self) -> None:
        if self.pending:
            self.line(self.pending)
            self.pending = ""


class _Prepared:
    """One plan's preparation: its template and what preparing it printed.

    A plain class: this tool is loaded from a file under private module
    names, and ``dataclass`` resolves its annotations through
    ``sys.modules``, where such a module is not.
    """

    __slots__ = ("template", "cwd", "lines", "refusal")

    def __init__(self, template: dict | None, cwd: Path | None,
                 lines: tuple[str, ...], refusal: Exception | None) -> None:
        self.template = template
        self.cwd = cwd
        self.lines = lines
        self.refusal = refusal


class _Templates:
    """Each plan's checkout frozen once, and every shard sealed off it.

    ``submission.preparation`` identifies the checkout, identifies it again on
    both sides of its snapshot and writes one full-history bundle of it:
    three ``git diff-index -p --binary`` reads and one bundle.  One ``pbrun``
    per shard did all four per shard, so a 60-shard lane was 180 identity
    reads and 60 bundles of one commit, and on 2026-10-20 the reads outlived
    their 30 s on an HDD-backed checkout and TrustLadder's release gate
    answered 125.  So a plan -- the shards that share placement, deadline,
    pins and environment -- is prepared once, with its first shard's
    request, and each shard is sealed off that template with its own command,
    the way ``pbcampaign`` seals a plan's children.  The identity is the same
    function over the same tree, so it is exactly as strict; the action is
    still an ordinary ``pbrun`` action.

    A preparation that refuses is kept, so each shard of the plan reports
    that refusal instead of repeating it.  An offer-discovery timeout is not
    kept: the shard's retry asks again (#1102).
    """

    def __init__(self, store) -> None:
        self.store = store
        self._lock = threading.Lock()
        self._prepared: dict[object, _Prepared] = {}
        self._published: dict[int, _Prepared] = {}

    def prepare(self, key, request: SubmissionRequest, *, deadline=None) -> _Prepared:
        with self._lock:
            known = self._prepared.get(key)
            if known is not None:
                return known
            lines: list[str] = []
            shown = _ShardLines(None, lines)
            try:
                with submission.preparation(request, store=self.store,
                                            display=shown, deadline=deadline) as prepared:
                    template, cwd = prepared["template"], Path(prepared["cwd"])
                refusal = None
            except OfferDiscoveryTimedOut as exc:
                shown.close()
                return _Prepared(None, None, tuple(lines), exc)
            except Exception as exc:
                template, cwd, refusal = None, None, exc
            shown.close()
            record = self._prepared[key] = _Prepared(template, cwd, tuple(lines), refusal)
            return record

    def sealing(self, plan, owner: SubmissionRequest, index: int,
                request: SubmissionRequest, *, shared: bool, deadline=None):
        """The preparation a shard seals off, and the command it seals.

        Preparation may rewrite argv: it can relativize the interpreter and
        bind an argument naming a path outside the checkout.  The first is
        the same for every shard of a plan.  A plan whose first shard had an
        argument rebound, or whose shards each name their own path outside
        the checkout (``shared`` false), has nothing a sibling could reuse:
        each of its shards is prepared on its own, as before.
        """

        record = self.prepare(plan, owner, deadline=deadline)
        if record.template is None:
            return record, None
        sealed = list(record.template["params"]["command"])
        if request is owner:
            return record, sealed
        if shared and sealed[1:] == list(owner.command)[1:]:
            return record, [sealed[0], *list(request.command)[1:]]
        record = self.prepare((plan, index), request, deadline=deadline)
        return record, (list(record.template["params"]["command"])
                        if record.template is not None else None)

    def published(self, record: _Prepared) -> None:
        with self._lock:
            self._published[id(record)] = record

    def changed(self) -> str | None:
        """What moved in the checkout since its published shards were sealed.

        One identity read, after the last shard is published.  Each shard ran
        a snapshot taken before it was published, so a tree that still
        matches every published template here is the tree they all ran; one
        that does not is named, and fails the run.  ``None`` when nothing
        moved, or nothing was published from here.
        """

        with self._lock:
            records = list(self._published.values())
        if not records:
            return None
        cwd = records[0].cwd
        try:
            now = movement_actions.checkout_identity(cwd)
        except SubmissionRefused as exc:
            return (f"pbtest: the checkout {cwd} could not be identified again after "
                    f"its last shard was published ({exc.reason or exc}), so nothing "
                    "shows its shards ran the tree on disk; this run fails")
        moved: list[str] = []
        for record in records:
            for change in _identity_changes(record.template["checkout_identity"], now):
                if change not in moved:
                    moved.append(change)
        if not moved:
            return None
        return (f"pbtest: the checkout {cwd} changed after its shards were sealed: "
                + "; ".join(moved) + ". Every shard ran the snapshot sealed before it "
                "was published, not the tree on disk now, so this run fails; rerun "
                "pbtest on the tree you mean to test")


def _identity_changes(then, now) -> list[str]:
    """Each way two checkout identities differ, in words."""

    changes = []
    if then.get("head") != now.get("head"):
        changes.append(f"HEAD moved from {then.get('head')} to {now.get('head')}")
    if then.get("dirty_sha256") != now.get("dirty_sha256"):
        changes.append(
            "its uncommitted changes are not the sealed ones (working-tree digest "
            f"{str(then.get('dirty_sha256'))[:12]} is now "
            f"{str(now.get('dirty_sha256'))[:12]})")
    for key in sorted((set(then) | set(now)) - {"head", "dirty_sha256"}):
        if then.get(key) != now.get(key):
            changes.append(f"{key} {then.get(key)!r} is now {now.get(key)!r}")
    return changes


class _Settled:
    """A shard attempt that ended in this process, with no client to wait on.

    A refusal, a cache hit or an offer-discovery timeout ends at publication.
    It reads like a ``pbrun`` that printed its lines and exited.
    """

    def __init__(self, returncode: int, events: client_channel.ClientEventResult) -> None:
        self.process = self
        self.stdout = io.StringIO()
        self.returncode = returncode
        self._events = events

    def wait(self) -> client_channel.ClientEventResult:
        return self._events


def publish_shard(templates: _Templates, index: int, request: SubmissionRequest, *,
                  plan, owner: SubmissionRequest, shared: bool, result: dict,
                  files=(), progress=None, deadline: float):
    """Seal one shard off its plan's template, publish it, and start its observer.

    The shard's lines are what its ``pbrun`` printed: the preparation's
    notices, the publication's (``pbrun: queued ...``), then the observer's.
    The observer is ``client_recovery``, the program that already reads a
    shard's unobserved ending back: ``pbrun``'s own attached wait -- the same
    reader, notices and rendering -- on this one generation until
    ``deadline``.  Publication is detached only so this process does not wait
    in it; the detached record is not printed.
    """

    lines = _shard_result(result)
    shown = _ShardLines(index, lines)
    report = action_client.ClientReport()
    try:
        # The caller's shard --wait-s deadline also bounds preparation Git.
        record, command = templates.sealing(plan, owner, index, request,
                                            shared=shared, deadline=deadline)
        for line in record.lines:
            shown.line(line)
        refusal = record.refusal
        if refusal is None:
            action = submission.seal(record.template, command=command)
            outcome = submission.submit_action(
                action, template=record.template, request=replace(request, detach=True),
                store=templates.store, display=shown, report=report)
            shown.close()
            if isinstance(outcome, SubmissionRefused):
                refusal = outcome
            else:
                templates.published(record)
    except SubmissionRefused as exc:
        shown.close()
        refusal = exc
    except Exception as exc:
        shown.close()
        refusal = exc
    if refusal is not None and not isinstance(refusal, SubmissionRefused):
        # What a pbrun that crashed printed, and how pbtest read it: a
        # traceback and no ending (#1033).
        shown.write("".join(traceback.format_exception(refusal)))
        shown.close()
        result["published"] = True
        return _Settled(1, client_channel.ClientEventResult(None, None, "missing client ending"))
    if refusal is not None:
        # ``pbrun`` printed the reason and exited 2 for a usage refusal, 1
        # for any other with a reason; an offer-discovery timeout published
        # nothing and is the one refusal ``run_shard`` retries (#1102).
        if refusal.reason:
            shown.write(refusal.reason + "\n")
            shown.close()
        if isinstance(refusal, OfferDiscoveryTimedOut):
            return _Settled(1, client_channel.ClientEventResult(
                None, action_client.ClientEnding(action_client.ClientOfferTimeout()), None))
        result["published"] = True
        code = 2 if refusal.returncode == 2 else 1 if refusal.reason else refusal.returncode
        ending = (report.ending if report.ending is not None else
                  action_client.ClientEnding(action_client.ClientRefused(refusal.returncode)))
        problem = (None if ending.exit_code & 255 == code
                   else "client ending contradicts the process exit status")
        return _Settled(code, client_channel.ClientEventResult(report.identity, ending, problem))
    result["published"] = True
    identity = report.identity
    _announce_submission(index, identity, result=result, files=files, progress=progress)
    if (isinstance(identity, action_client.PoolClientIdentity)
            and identity.published_unix is not None
            and math.isfinite(identity.published_unix)):
        return start_shard(index, client_recovery.observe_command(identity, deadline=deadline),
                           result=result, files=files, progress=progress,
                           expected_identity=identity)
    # A cache hit: the receipt is the outcome, and nothing runs to wait on.
    return _Settled(report.ending.exit_code,
                    client_channel.ClientEventResult(identity, report.ending, None))


#: ``pbrun``'s own transport vocabulary, and its own reader for the default.
#: This tool builds pbrun's argv rather than importing it, so every flag a
#: shard needs has to be forwarded here -- a flag that is not forwarded is a
#: flag twenty shards never see.  ``fleet_submit`` is published beside this
#: file and holds the one reader of the default, so a shard's transport is the
#: fleet's transport rather than a second opinion about it.
from prismabuild.fleet_submit import TRANSPORTS, default_transport  # noqa: E402
#: The per-test bound's name and the ceiling the worker loops enforce, read
#: from the modules that own them rather than restated here: a bound derived
#: from a number this file copied would drift the moment either moved.
from prismabuild import test_bound_contract  # noqa: E402
from worker_loop import DEFAULT_EXECUTION_CEILING_S  # noqa: E402
#: Read the recorder from this tool's own generation. A pbtest test can itself
#: run under an older sealed recorder; its sys.modules entry is that outer
#: shard's recorder, not the sibling this source must seal and understand.
#: Loaded on first use, not at import (#1554): importing a tool must not run
#: code, and the import scanner (``tests/test_tools_do_not_run_on_import.py``)
#: says so.  The object stands where the module stood, so ``pbtest_outcomes.x``
#: reads, patches and deletes the same attribute of the same sibling module.
class _OutcomesOwner:
    _module = None
    _lock = threading.Lock()
    #: Bound at import, not at first use.  The published tools path is a link
    #: that a publication repoints to the next generation, so resolving it late
    #: would let generation A's pbtest read generation B's recorder.  The
    #: directory is resolved here, once, to the generation this file was
    #: imported from; retained generations are immutable.
    _path = Path(__file__).parent.resolve() / "pbtest_outcomes.py"

    def _load(self):
        with _OutcomesOwner._lock:
            if _OutcomesOwner._module is None:
                spec = importlib.util.spec_from_file_location(
                    __name__ + "_outcomes", _OutcomesOwner._path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                _OutcomesOwner._module = module
            return _OutcomesOwner._module

    def __getattr__(self, name):
        return getattr(self._load(), name)

    def __setattr__(self, name, value):
        setattr(self._load(), name, value)

    def __delattr__(self, name):
        delattr(self._load(), name)


pbtest_outcomes = _OutcomesOwner()

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


def fleet_queue():
    """The queue every shard is submitted to, read the way the tools do.

    One construction for the ceilings read and the interpreter pre-flight
    alike, so the two questions cannot drift onto different roots.
    """

    return pool.PoolQueue(store_for(_PB_ENTRYPOINT).root / "pb-queue")


def interpreter_refusal(queue, python: str, *, tags, resources,
                       needs_gpu: bool) -> tuple[str | None, str | None]:
    """The pre-flight's answer for one interpreter (#1263, #1266 review).

    ``(None, None)`` -- run.  ``("refusal", why)`` -- exit 2 before any shard
    is sealed: every capable eligible offer answered, and every answer named
    the path absent.  ``("notice", line)`` -- run, saying so: the path is
    unknown (a first submission is unknown by construction -- no READY row
    named it when the offers were written), the claim-time stat stays the
    guard everywhere, and the fleet's next poll answers for it.
    """

    probe = {
        "tags": list(tags),
        "interpreter": python,
        "resources": dict(resources),
        "needs_gpu": bool(needs_gpu),
    }
    verdict = queue.interpreter_placement_verdict(
        probe, python, max_age_s=submission.RECORDED_OFFER_MAX_AGE_S)
    if verdict == "absent":
        return ("refusal",
                "pbtest: every recorded worker that could take these shards "
                f"names the interpreter absent: {python}. Install it on a "
                "box that offers these tags and let its worker's next poll "
                "answer for the path; submitting now would queue shards "
                "that die with 127 on the first box to claim them.")
    if verdict == "unknown":
        return ("notice",
                f"pbtest: no worker has answered for the interpreter "
                f"{python} yet; the shards publish, the claim-time check "
                "guards every box, and the fleet's next poll places them "
                "where the path lives.")
    return (None, None)


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
        offers = fleet_queue().offers()
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
    path = core.PrismaBuildCAS(store_for(_PB_ENTRYPOINT).root / "cas").receipt_path(action_key)
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


def placement(tags: list[str], *, gpu: bool, timeout_s: float | None,
              test_timeout_s: float | None,
              ) -> tuple[dict[str, float | None], float | None, float]:
    """One read of the announcements for the three numbers a shard seals.

    The ceilings a claimant announces, the shard deadline derived from them
    (:func:`shard_ceiling`), and the per-test bound one heartbeat inside it
    (:func:`per_test_bound`).  Derived together, from one announcement read,
    so a shard's sealed end and the bound inside it cannot come from two
    different reads of the fleet and drift apart -- the rule
    :func:`default_duration` states for the packing, one level up.  A cohort
    of shards whose tags name a narrower fleet derives its numbers from
    exactly the boxes that can claim it by calling this per cohort.
    """

    ceilings = announced_ceilings(tags)
    sealed_s = shard_ceiling(timeout_s=timeout_s, gpu=gpu, ceilings=ceilings)
    bound_s = per_test_bound(timeout_s=timeout_s, override_s=test_timeout_s,
                             gpu=gpu, ceilings=ceilings)
    return ceilings, sealed_s, bound_s


def shard_ceiling(*, timeout_s: float | None, gpu: bool = False,
                  ceilings: dict[str, float | None] | None = None) -> float | None:
    """The payload budget each shard seals as ``execution_timeout_s``; ``None`` seals none.

    The smaller of what the submitter asked for and the smallest ceiling a box
    that could claim the shard announces -- the ``min``
    ``pool._execution_timeout`` applies at claim. The sealed number is a
    payload execution budget, not a wall-clock stop or resource-release fence.
    Admission uses a holder's sealed request only as advisory incumbent
    opportunity metadata (``PoolQueue.holder_bound``), not proof that checkout,
    credited waits or settlement finish by then (#1429). Before #939 a shard
    sealed only an explicit ``--timeout-s``, and admission could read an
    unsealed shard by its age alone.

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


#: The pytest marker a test file carries when it reads fleet data.
FLEET_DATA_MARKER = "fleet_data"
_FLEET_DATA_USE = re.compile(rf"\bmark\.{FLEET_DATA_MARKER}\b")


def fleet_data_files(checkout: Path, files: list[str]) -> list[str]:
    """The discovered files that declare they read fleet data (#915).

    A file declares it with ``@pytest.mark.fleet_data`` on a test, or
    ``pytestmark = pytest.mark.fleet_data`` for the module.  The scan is
    static, so it never imports the target's plugins. Strings and comments
    are not declarations; unparseable target syntax keeps the conservative
    text fallback and remains pytest's collection responsibility (#1460).
    """

    marked = []
    for name in files:
        try:
            text = (checkout / name).read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            declared = bool(_FLEET_DATA_USE.search(text))
        else:
            declared = any(
                isinstance(node, ast.Attribute) and node.attr == FLEET_DATA_MARKER
                and isinstance(node.value, ast.Attribute) and node.value.attr == "mark"
                for node in ast.walk(tree)
            )
        if declared:
            marked.append(name)
    return marked


def default_duration(known: dict[str, float]) -> float | None:
    """One predicted-duration default, shared by ``main()`` and ``shard()``.

    The median of the measured files -- a declared default derived from the
    same receipts, never a picked constant.  ``None`` with no measurement
    at all.  Computed here so the printed prediction and the actual packing
    cannot disagree (#1303 review).
    """

    return statistics.median(known.values()) if known else None


def shard(files: list[str], count: int, *, durations: dict[str, float] | None = None,
          ceiling: float | None = None,
          model_default: float | None = None) -> list[list[str]]:
    """Round-robin without measurements; measured packing with them (#1246).

    ``durations`` maps each measured file to its predicted seconds, and
    ``ceiling`` is the sealed shard deadline the predictions are judged
    against.  With neither, this is today's rotation: adjacent (and so
    similar) files land on different boxes.  With measurements:

    * up to ``count - 1`` files predicted at or past half the ceiling get
      their own shard, so a stall or timeout costs only that file --
      ``--shards`` is a ceiling the submitter sets on purpose, and the
      packer never exceeds it.  Half is not a picked share: two files each
      at or past half the ceiling can never share one shard within the
      ceiling, so giving each its own is never worse than pairing them;
    * the rest pack longest-processing-time first into the remaining
      shards, largest prediction onto the least-loaded bucket.

    Files with no measurement take the model's default: ``model_default``
    -- the median over ALL history that ``main()`` prints and reports
    (files outside this run included) -- falling back to
    ``default_duration`` of this run's measured files when no model
    default is passed.  With no measurement at all the answer is the
    rotation.  Deterministic throughout: ties break by bucket index, then
    file name.
    """

    count = max(1, min(count, len(files)))
    if not durations:
        return _round_robin(files, count)
    known = {name: durations[name] for name in files if name in durations}
    if not known:
        return _round_robin(files, count)
    default = (model_default if model_default is not None
               else default_duration(known))
    assert default is not None
    predicted = {name: known.get(name, default) for name in files}
    ordered = sorted(files, key=lambda name: (-predicted[name], name))
    singletons: list[str] = []
    if ceiling is not None:
        # At most count - 1, longest first: --shards is a ceiling, and any
        # further past-half file packs LPT with the rest (#1303 review).
        # The bar is half the ceiling itself, not a picked share: two files
        # each at or past it can never share one shard within the ceiling.
        singletons = [name for name in ordered
                      if 2 * predicted[name] >= ceiling][:max(0, count - 1)]
    alone = set(singletons)
    rest = [name for name in ordered if name not in alone]
    buckets = [[name] for name in singletons]
    pools = max(1, count - len(singletons))
    totals = [0.0] * pools
    packed: list[list[str]] = [[] for _ in range(pools)]
    for name in rest:
        index = min(range(pools), key=lambda i: (totals[i], i))
        packed[index].append(name)
        totals[index] += predicted[name]
    buckets.extend(packed)
    return [bucket for bucket in buckets if bucket]


def _round_robin(files: list[str], count: int) -> list[list[str]]:
    """Today's rotation: adjacent files land on different boxes."""

    buckets: list[list[str]] = [[] for _ in range(count)]
    for index, name in enumerate(files):
        buckets[index % count].append(name)
    return buckets


def allocate_shards(weights: dict, count: int) -> dict:
    """Split a shard budget across cohorts: one each, extras by weight.

    ``weights`` maps each cohort key to its predicted seconds -- file counts
    when nothing is measured.  Every cohort gets one shard first; the rest
    of ``count`` is handed out by largest remainder, ties breaking by key
    sort, so the same inputs always pack the same way.  The caller refuses
    more cohorts than ``count``, so each cohort's one shard is affordable
    and the total never exceeds the budget (#1495).
    """

    keys = sorted(weights)
    total = {key: 1 for key in keys}
    weight_sum = sum(weights[key] for key in keys)
    remaining = count - len(keys)
    if remaining <= 0 or weight_sum <= 0:
        return total
    exact = {key: remaining * weights[key] / weight_sum for key in keys}
    for key in keys:
        total[key] += int(exact[key])
    handed = sum(int(exact[key]) for key in keys)
    order = sorted(keys,
                   key=lambda key: (-(exact[key] - int(exact[key])), key))
    for key in order[:remaining - handed]:
        total[key] += 1
    return total


def load_history(paths: list[str] | None) -> tuple[dict[str, list[float]], list[str]]:
    """Per-file wall-time samples out of prior ``--json`` run reports (#1246).

    Each report holds every shard's files beside its full stdout, and the
    stdout holds the shard's outcome record with its ``file_durations`` --
    so a prior report is the receipts' content in one file, and loading it
    reads no live state.  Returns ``(history, problems)``: unreadable
    files, non-list shapes, and non-numeric samples are ignored with a
    stated reason per path, never silently and never fatally -- a history
    flag must neither fail a submission because one old report rotted nor
    pretend a foreign report packed the shards (#1303 review).  The caller
    prints the problems; tests assert on them.
    """

    history: dict[str, list[float]] = {}
    problems: list[str] = []
    for raw in paths or ():
        try:
            runs = json.loads(Path(raw).read_text())
        except OSError as exc:
            problems.append(f"{raw}: unreadable, skipped ({exc})")
            continue
        except ValueError as exc:
            problems.append(f"{raw}: not a JSON report, skipped ({exc})")
            continue
        if not isinstance(runs, list):
            problems.append(f"{raw}: top shape is "
                            f"{type(runs).__name__}, not a run list; skipped")
            continue
        skipped = 0
        for result in runs:
            if not isinstance(result, dict):
                skipped += 1
                continue
            record = pbtest_outcomes.parse(result.get("output") or "",
                record_binding=result.get("record_binding"))
            if record is None:
                skipped += 1
                continue
            durations = record.get("file_durations") or {}
            if not isinstance(durations, dict):
                skipped += 1
                continue
            for name, seconds in durations.items():
                if (not isinstance(name, str) or isinstance(seconds, bool)
                        or not isinstance(seconds, (int, float))
                        or seconds < 0):
                    skipped += 1
                    continue
                history.setdefault(name, []).append(float(seconds))
        if skipped:
            problems.append(f"{raw}: {skipped} entr" +
                            ("y" if skipped == 1 else "ies") +
                            " without usable file_durations, skipped")
    return history, problems


def predict_durations(history: dict[str, list[float]]) -> dict[str, float]:
    """Each measured file's prediction: the max of its samples (#1246).

    The most conservative high quantile, because a file's time moves with
    load: the slow file took 93 s alone on a quiet box and 1513 s in a
    batch of 38 concurrent shards, and packing for its mean strands the
    shard on its bad day.  The submitter bounds staleness by which reports
    it passes as history; the model itself never forgets inside one run.
    """

    return {name: max(samples) for name, samples in history.items() if samples}


def summary_seconds(summary: str) -> float | None:
    """pytest's own session seconds out of a terminal summary line.

    The grammar already ends every summary in ``in S.SSs``; this reads that
    number, so the run's predicted-against-actual report compares the model
    against what pytest measured, not against queueing and placement (#1246).
    ``None`` when the shard never reported -- a missing summary has no
    seconds to read.
    """

    if not summary:
        return None
    match = re.search(r" in (\d+\.\d+)s(?: \([^)]*\))?\s*$",
                      pbtest_outcomes.ANSI.sub("", summary).strip())
    return float(match.group(1)) if match else None

SHARD_DIGEST_MODULE = "_prismabuild_pbtest_digest_primitives"


#: The interpreter program every shard runs.  It carries the modules it runs
#: as text, so the action key names their bytes and no helper path has to
#: exist on the worker -- the rule the dependency guard already followed.
#: ``PACKED`` carries the largest optional helper the same way, compressed
#: (``packed_source``), and is unpacked into ``SOURCES`` before anything runs.
SHARD_PROGRAM = """\
# A pbtest shard: pytest under pbtest_outcomes' recorder.
import base64
import builtins
import sys
import types
import zlib

SOURCES = @SOURCES@
PACKED = @PACKED@
for _name, _blob in PACKED.items():
    SOURCES[_name] = zlib.decompress(base64.b85decode(_blob)).decode("utf-8")
DIGEST_MODULE = @DIGEST_MODULE@


def load(name):
    module = types.ModuleType(name)
    module.__file__ = "<pbtest " + name + ">"
    if name != DIGEST_MODULE:
        module.__dict__["__builtins__"] = HELPER_BUILTINS
    sys.modules[name] = module
    exec(compile(SOURCES[name], module.__file__, "exec"), module.__dict__)
    return module


# Only shipped helper imports bind to their captured owners. A later package
# import loads its real on-disk module, preserving that package's identity.
shipped_owner = load(DIGEST_MODULE)


def helper_import(name, globals=None, locals=None, fromlist=(), level=0):
    if name == "prismabuild.digest_primitives" and fromlist and level == 0:
        return shipped_owner
    if name == "prismabuild.pbtest_collection" and fromlist and level == 0:
        return shipped_collection
    return builtins.__import__(name, globals, locals, fromlist, level)


# Helpers snapshot builtins for isolation; the import fallback delegates live.
HELPER_BUILTINS = dict(vars(builtins), __import__=helper_import)
pins = load("pbtest_pins") if "pbtest_pins" in SOURCES else None
digest = load("pbtest_dependency_digest") if "pbtest_dependency_digest" in SOURCES else None
shipped_collection = load("pbtest_collection")
selection = None
if @COLLECTION@:
    import json
    selection = json.loads(sys.argv.pop(1))


def preflight():
    # The reviewed-dependency guard, then the sealed capability
    # requirements (#1495): each refuses the shard before pytest, which
    # makes a drifted dependency a failed shard, never a skipped test.
    if pins is not None:
        try:
            pins.check_pins()
        except ValueError as exc:
            message = f"pbtest: dependency pin refused before pytest: {exc}"
            print(message, file=sys.stderr, flush=True)
            return 1, {"code": "dependency_pin", "message": message}
    return (0 if digest is None else digest.verify_sealed(selection)), None


raise SystemExit(load("pbtest_outcomes").main(
    preflight=preflight,
    resource_source=SOURCES.get("pbtest_resource_scope"),
    collection_spec=selection,
    roster_parent=globals().get("_pb_attempt_directory"),
    collection_source=SOURCES["pbtest_collection"]))
"""

from prismabuild.pbtest_owned_temp import (
    OWNED_TREE_PROGRAM, ROOTS_PROGRAM, BASETEMP_PROGRAM, ATTEMPT_TEMP_PROGRAM,
)


def packed_source(path: Path) -> str:
    """A carried module's exact bytes, zlib-compressed and base85-encoded.

    The shard unpacks the same bytes, so the action key still names them;
    the encoding only keeps the program inside one exec argument.
    """

    return base64.b85encode(zlib.compress(path.read_bytes(), 9)).decode("ascii")


def shard_entry(python: str, checkout: Path, *, tmpdir: str | None = None,
                basetemp: str | None = None,
                trace: bool = False, collection: bool = False,
                capabilities: bool = False) -> list[str]:
    """The argv that runs a shard's pytest under the outcome recorder.

    Every shard reports each counted outcome by node ID (#942), so every
    shard runs through ``pbtest_outcomes``.  A checkout that pins a reviewed
    dependency (``tools/resolve_<module>_dev_pin.py``) also carries the guard,
    which runs before pytest and refuses on drift.  A run with capability
    declarations carries the digest verifier, which re-checks every sealed
    requirement before pytest for the same reason (#1495).  All travel as
    source in the argv, never as a path.  Reading any of them can raise
    ``OSError``, which the caller reports.
    """

    here = Path(__file__)
    sources = {SHARD_DIGEST_MODULE:
                   Path(core.digest_primitives.__file__).read_bytes().decode("utf-8"),
               "pbtest_collection": Path(pbtest_collection.__file__).read_text()}
    packed = {"pbtest_outcomes": packed_source(here.with_name("pbtest_outcomes.py"))}
    if trace:
        # The diagnostic plugin reuses the existing exact-process I/O reader.
        # Carry its standalone source into any target project/interpreter;
        # at 44 KB it is the largest helper, so it travels packed.
        packed["pbtest_resource_scope"] = packed_source(
            RUNTIME_ROOT / "src" / "prismabuild" / "resource_scope.py")
    if capabilities:
        sources["pbtest_dependency_digest"] = (
            RUNTIME_ROOT / "src" / "prismabuild" /
            "dependency_digest.py").read_text()
    if any((checkout / "tools").glob("resolve_*_dev_pin.py")):
        sources["pbtest_pins"] = here.with_name("pbtest_pins.py").read_text()
    # ``@PACKED@`` last: a base85 blob may spell any later placeholder.
    program = SHARD_PROGRAM.replace("@SOURCES@", repr(sources)).replace(
        "@DIGEST_MODULE@", repr(SHARD_DIGEST_MODULE)).replace(
        "@COLLECTION@", repr(collection)).replace("@PACKED@", repr(packed))
    # Either attempt program defines the cleanup beside the exit call, so the
    # derivation and the removal cannot drift apart.
    program = program.replace(
        'raise SystemExit(load("pbtest_outcomes").main(',
        'raise SystemExit(_pb_run_and_clean_attempt_base(lambda: load("pbtest_outcomes").main(',
        1).replace(
        'collection_source=SOURCES["pbtest_collection"]))',
        'collection_source=SOURCES["pbtest_collection"])))',
        1)
    if basetemp is not None:
        # The derivation runs before every other guard: it is the worker's
        # refusal point for an unusable or unowned sealed root (#1469), and
        # the sealed root travels inside this program, so a different root
        # is a different sealed request. An explicit root keeps its meaning;
        # the attempt default below never runs beside it.
        program = BASETEMP_PROGRAM.replace("@ROOT@", repr(basetemp)) + program
    else:
        # No sealed root: mkdtemp gives the attempt a short, exclusive
        # directory under TMPDIR.
        program = ATTEMPT_TEMP_PROGRAM + program
    # The loaded module's own bytes, as with the digest owner above.
    program = (OWNED_TREE_PROGRAM.replace("@SOURCE@", repr(packed_source(
        Path(owned_tree.__file__)))) + ROOTS_PROGRAM.replace("@ROOT_SOURCE@", repr(
            packed_source(Path(pbtest_scratch_roots.__file__))))
        + program)
    if tmpdir is not None:
        # Explicit scratch placement must refuse on the worker rather than let
        # tempfile silently choose another filesystem. The default entry stays
        # byte-identical, and a worker-only path needs no coordinator access.
        program = (
            "import tempfile\n"
            f"_pb_requested_tmpdir = {tmpdir!r}\n"
            "try:\n"
            "    _pb_check_capacity(_pb_requested_tmpdir)\n"
            "    with tempfile.TemporaryFile(dir=_pb_requested_tmpdir):\n"
            "        pass\n"
            "except (OSError, ValueError) as exc:\n"
            "    raise SystemExit('pbtest: cannot use requested --tmpdir: ' + "
            "str(exc)) from exc\n"
        ) + program
    if tmpdir is not None or basetemp is not None:
        capacity_source = (generation_root(__file__) / "src" / "prismabuild" /
                           "filesystem_capacity.py").read_text()
        program = (
            "_pb_capacity = {}\n"
            f"exec(compile({capacity_source!r}, '<pbtest capacity>', 'exec'), _pb_capacity)\n"
            "def _pb_check_capacity(path):\n"
            "    room = _pb_capacity['local_disk_room'](path, 0)\n"
            "    if room['inode_refusal'] is not None:\n"
            "        raise SystemExit('pbtest: scratch capacity refused: ' + "
            "room['inode_refusal'])\n"
        ) + program
    return [python, "-c", program]


def summary_count(summary: str, word: str) -> int:
    """The ``N <word>`` count in a pytest summary line, ``0`` when absent."""

    match = re.search(rf"(?:^|[ =,])(\d+) {re.escape(word)}\b", summary)
    return int(match.group(1)) if match else 0


def validate_basetemp(root: str) -> None:
    """The spelling checks a sealed basetemp must satisfy (#1469).

    A worker-visible scratch need not exist on the submitting box, so
    existence, kind and ownership are the worker's preflight; here only
    values that could never be a safe sealed root refuse, before any shard
    is sealed.  An absolute root may name a separately qualified mounted
    filesystem; a relative root is confined to the attempt's own
    materialized checkout, so both spellings are sealable.
    """

    if not root:
        raise ValueError("--basetemp must be a nonempty path")
    if "\0" in root:
        raise ValueError("--basetemp must not contain NUL")
    if any(part == ".." for part in PurePath(root).parts):
        raise ValueError("--basetemp must not traverse: no '..' in the root")


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
        _names("assigned file absent:", reconciliation.get("missing_files") or ())


def displayed(output: str) -> list[str]:
    """A shard's output lines for a human, without its outcome record."""

    return [line for line in (output or "").strip().splitlines()
            if not line.startswith((pbtest_outcomes.PREFIX,
                                    pbtest_outcomes.COMPLETION_PREFIX,
                                    pbtest_outcomes.PREFLIGHT_PREFIX))]


# A closed vocabulary prevents resource controls, config indirection, and
# extra file populations from hiding in forwarded arguments. Extend this list
# deliberately for new plugins, after checking their execution semantics.
#: The recorder's own trace switch joins this set where it is read, so naming it
#: does not load the recorder at import (#1554).
PYTEST_SWITCHES = {"--strict-cuda", "--strict-markers", "--strict-config",
                   "--collect-only", "--co", "--disable-warnings", "-x"}
PYTEST_VALUES = {"-k", "-m", "--dist", "--surface-json", "--durations",
                 "--durations-min", "--maxfail", "--tb"}
#: ``-o`` and its long spelling name one ini key and three values: pytest's own
#: tmp retention policy (#1535, D29).  A sealed checkout cannot take a config
#: edit, and a repository without the key keeps every passing test's directory
#: on an inode-limited scratch.  No other key passes: ``addopts``, ``testpaths``
#: and plugin keys would reopen resource control and config indirection.
PYTEST_OVERRIDES = {"-o", "--override-ini"}
RETENTION_KEY = "tmp_path_retention_policy"
RETENTION_POLICIES = {"all", "failed", "none"}


def parse_pytest_args(raw: str, *, gpu: bool, workers: int) -> list[str]:
    """Validate and normalize a JSON argv without importing target plugins."""
    values = json.loads(raw)
    if not isinstance(values, list) or any(
        not isinstance(v, str) or not v or "\0" in v for v in values
    ):
        raise ValueError("--pytest-args must be a JSON array of nonempty strings")
    result: list[str] = []
    retention_seen = False
    index = 0
    while index < len(values):
        option, equals, value = values[index].partition("=")
        index += 1
        if ((option in PYTEST_SWITCHES or option == pbtest_outcomes.TRACE_OPTION)
                and not equals):
            if option == "--strict-cuda" and not gpu:
                raise ValueError("--strict-cuda requires --gpu")
            result.append(option)
            continue
        if option not in PYTEST_VALUES | PYTEST_OVERRIDES:
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
        if option in PYTEST_OVERRIDES:
            key, _, policy = value.partition("=")
            if key != RETENTION_KEY or policy not in RETENTION_POLICIES:
                raise ValueError(
                    f"unsupported pytest option {option!r} {value!r}; the only "
                    f"override is {RETENTION_KEY}=" + "|".join(sorted(RETENTION_POLICIES)))
            if retention_seen:
                raise ValueError(f"{RETENTION_KEY} may be set once")
            retention_seen = True
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


def validate_shard_env(entries: list[str]) -> None:
    """Check caller additions before preparing or publishing any shard (#1891)."""
    owned = {"TMPDIR", *SHARD_THREAD_ENV, test_bound_contract.TIMEOUT_ENV,
             test_bound_contract.SHARD_BUDGET_ENV, "PYTHONPATH", "PYTEST_ADDOPTS",
             pbtest_outcomes.RECORD_BINDING_ENV}
    seen: set[str] = set()
    for entry in entries:
        if "\x00" in entry:
            raise ValueError(f"--env contains NUL: {entry!r}")
        if "=" not in entry:
            raise ValueError(f"--env expects KEY=VALUE, got {entry!r}")
        key = entry.split("=", 1)[0]
        if not key:
            raise ValueError(f"--env has an empty key: {entry!r}")
        if key in owned:
            raise ValueError(f"--env {key} is set by pbtest; callers may not override it")
        if key in seen:
            raise ValueError(f"--env repeats key {key}")
        seen.add(key)


def shard_request(request: SubmissionRequest, *, environment, python_entry,
                  arguments) -> SubmissionRequest:
    """One shard's whole request: the plan's, with its environment and command."""
    return replace(request, env=tuple(environment), command=(*python_entry, *arguments))


def shard_command(request: SubmissionRequest) -> list[str]:
    """Send a shard's typed request across the C06 client process boundary.

    The SLURM lane's shards still go this way: it has no detached observer
    to hand a publication to (``client_recovery`` reads the pull queue).
    """
    return ["/usr/bin/python3", str(PBRUN), *request_arguments(request)]


def _client_environment():
    return {key: value for key, value in os.environ.items()
            if key not in ("PRISMABUILD_PBTEST_RUN_ID", "PRISMABUILD_PBTEST_ENTRYPOINT")}


def _write_report_bytes(path, data, *, durable=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        if durable:
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class _Progress:
    def __init__(self, report):
        self.path = pbtest_outcomes.progress_path(Path(report)) if report else None
        self.lock = threading.Lock()
        self.failed = False
        self.value = {"schema": pbtest_outcomes.PROGRESS_SCHEMA,
            "run_id": os.environ.get("PRISMABUILD_PBTEST_RUN_ID") or uuid.uuid4().hex,
            "launch_entrypoint": os.environ.get("PRISMABUILD_PBTEST_ENTRYPOINT") or str(Path(__file__).resolve()),
            "producer_pid": os.getpid(), "producer_entrypoint": str(Path(__file__).resolve()),
            "producer_generation": str(RUNTIME_ROOT), "sequence": 0, "state": "running",
            "total_shards": 0, "completed_shards": 0, "shards": [], "runtime_refusal": None}
        self._write()

    def _write(self):
        if self.path is not None:
            self.value["sequence"] += 1
            _write_report_bytes(self.path, json.dumps(self.value, separators=(",", ":")).encode(),
                        durable=self.value["state"] != "running")

    def configure(self, count):
        with self.lock:
            self.value["total_shards"] = count
            self.value["shards"] = [{"shard": i, "status": "pending", "attempts": 0,
                "action_key": None, "returncode": None} for i in range(count)]
            self._write()

    def observe(self, index, result, *, completed=False):
        with self.lock:
            shard = self.value["shards"][index]
            shard.update(status="completed" if completed else "submitted",
                attempts=result["attempts"], action_key=result["action_key"],
                returncode=result["returncode"] if completed else None)
            self.value["completed_shards"] = sum(s["status"] == "completed" for s in self.value["shards"])
            try:
                self._write()
            except OSError as exc:
                self.failed = True
                sys.stderr.write(f"pbtest: cannot write progress: {exc}\n")

    def refuse(self, refusal):
        with self.lock:
            self.value.update(state="refused", runtime_refusal=refusal)
            self._write()

    def finish(self, report_bytes):
        with self.lock:
            self.value.update(state="finished", report_sha256=core.raw_sha256(report_bytes))
            self._write()


def _pbtest_population(checkout: Path, paths: list[str], capabilities: str | None):
    """Discover and classify once for execution and the read-only plan (#1900)."""
    plan = {"schema": "prismabuild.pbtest_plan.v1", "files": [],
            "capabilities": {}, "tags": {}, "left_out": {}, "refusals": []}
    config: dict = {}
    declared: dict = {}
    try:
        plan["files"] = discover(checkout, paths)
    except (OSError, ValueError) as exc:
        plan["refusals"].append({"code": "discovery_failed", "reason": str(exc)})
        return plan, config, declared
    if not plan["files"]:
        plan["refusals"].append({"code": "population_empty",
                                 "reason": f"no test files under {paths} in {checkout}"})
        return plan, config, declared
    capabilities_path = (checkout / capabilities if capabilities is not None
                         else checkout / "tests/pbtest_capabilities.json")
    try:
        if capabilities is not None and not capabilities_path.is_file():
            raise ValueError(f"--capabilities {capabilities!r} is not a file under {checkout}")
        if capabilities_path.is_file():
            resolved = capabilities_path.resolve()
            if not resolved.is_relative_to(checkout):
                raise ValueError(
                    f"--capabilities {capabilities!r} must name a file inside --checkout: "
                    "the sealed snapshot carries the declaration, so an outside file "
                    "is mutable state the run would only claim to declare")
            config = pbtest_capabilities.load_config(resolved)
    except (OSError, ValueError) as exc:
        plan["refusals"].append({"code": "capabilities_invalid",
                                 "reason": f"--capabilities {capabilities_path}: {exc}"})
        return plan, config, declared
    declared, problems = pbtest_capabilities.classify_files(checkout, plan["files"], config)
    plan["refusals"] = [{"code": "capability_unclassifiable", "reason": problem}
                        for problem in problems]
    plan["capabilities"] = {file: declared.get(file, []) for file in plan["files"]}
    return plan, config, declared


def _pbtest_tags(args) -> list[str]:
    return args.tag or (["gb10"] if args.gpu and
                        not pool.is_box_local_path(RUNTIME_ROOT) else [])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--checkout", required=True,
                    help="Git tree to snapshot and test on the pool")
    ap.add_argument("--python", default=None,
                    help="absolute python* executable on eligible TARGET workers; required except with --plan-only")
    ap.add_argument("--tmpdir", default=None,
                    help="absolute scratch directory on eligible TARGET workers; "
                         "default from fleet action_tmpdir, else /home/rob/tmp "
                         "(not checked on the coordinator)")
    ap.add_argument("--env", action="append", default=[], metavar="KEY=VALUE",
                    help="literal addition to every shard's sealed environment "
                         "(repeatable); duplicate and pbtest-owned keys are refused")
    ap.add_argument("--basetemp", default=None,
                    help="sealed scratch root for pytest's own temporary files "
                         "(#1469), separate from --tmpdir: the worker derives "
                         "<root>/<action-key>/<attempt>/pytest and hands that "
                         "to pytest --basetemp, never the sealed root itself "
                         "(pytest deletes its basetemp). Absolute roots may "
                         "name a separately qualified filesystem; relative "
                         "roots live in the attempt's materialized checkout. "
                         "Refused when empty, NUL-bearing or traversing; the "
                         "worker refuses a symlink, non-directory or unowned "
                         "root before pytest, with no fallback")
    ap.add_argument("--tag", action="append", default=[],
                    help="additional placement constraint; CPU default is portable, GPU is gb10")
    ap.add_argument("--shards", type=int, default=20,
                    help="how many actions the suite is split into, "
                         "round-robin over the discovered files; more than "
                         "there are files is lowered to one shard per file")
    ap.add_argument("--max-clients", type=int, default=DEFAULT_MAX_CLIENTS,
                    help="most pbrun clients this process keeps resident at "
                         f"once (default {DEFAULT_MAX_CLIENTS}; 0 = one per "
                         f"shard).  Each is about {CLIENT_RSS_MB} MB on the "
                         "submitting box for the shard's whole wait, so the "
                         "rest of the shards start as clients exit (#1348)")
    ap.add_argument("--workers-per-shard", type=int, default=1,
                    help="pytest workers in each action; above 1 uses pytest-xdist "
                         "(-n N), which must be installed in the target interpreter")
    ap.add_argument("--threads-per-shard", type=int, default=None,
                    help="BLAS/OMP threads each pytest worker may use (default 2, "
                         "or with --workers-per-shard above 1 and --cpus-per-shard, "
                         "each worker's share of the shard's cores); 0 leaves it "
                         "to the row and then --cpus-per-shard is required, and "
                         "with several workers each gets its share (#1192)")
    ap.add_argument("--cpus-per-shard", type=int, default=None,
                    help="cores each shard reserves; the default is "
                         "--workers-per-shard times --threads-per-shard, so the ceiling a shard is given "
                         "is the ceiling it can use. Required with "
                         "--threads-per-shard 0, which sets no ceiling at all")
    ap.add_argument("--mem-gb", type=int, default=3,
                    help="memory each shard demands of its box")
    ap.add_argument("--disk-metadata", action="store_true",
                    help="request disk_metadata=1 per shard (pool only); "
                         "workers must opt into capacity 1 (default 0). "
                         "Serializes declaring actions on the same host "
                         "ledger, not ordinary work, tier egress or external "
                         "I/O; no disk-throughput isolation (Refs #1008)")
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
    ap.add_argument("--json", default="", help="write the report or --plan-only object here")
    ap.add_argument("--plan-only", action="store_true",
                    help="discover and classify files without preparation or submission; "
                         "requires --json PATH. Uses --checkout, paths, --capabilities, "
                         "--tag and the --gpu default tag; ignores execution options, "
                         "including --pytest-args (no pytest collection)")
    ap.add_argument("--history", action="append", default=[],
                    help="prior --json run reports whose file_durations "
                         "pack this run's shards by measured duration "
                         "(#1246): repeatable, one report per flag "
                         "(--history takes exactly one path so it can never "
                         "swallow the test paths that follow it); each "
                         "measured file is predicted at the max of its "
                         "samples, a file past half the sealed deadline "
                         "shards alone, the rest pack longest-first, and "
                         "unmeasured files take the median. Without it the "
                         "shards stay round-robin")
    ap.add_argument(
        "--capabilities", default=None, metavar="PATH",
        help="capability configuration relative to --checkout (default "
             "tests/pbtest_capabilities.json when it exists); files declare "
             f"requirements with @pytest.mark.{pbtest_capabilities.MARKER}"
             "(\"name\"), fenced shards carry their cohort's tags and "
             "sealed dependency digests, and an unclassifiable or undefined "
             "declaration refuses the run (#1495)")
    ap.add_argument(
        "--data-manifest", default=None, metavar="PATH",
        help="a data manifest, as pbrun takes it, naming the shared-mount "
             "bytes the tests read; forwarded to every shard so the fleet can "
             "make them resident before a shard is claimed (#915).  It enters "
             "each shard's action key.  A test file marked "
             f"@pytest.mark.{FLEET_DATA_MARKER} reads fleet data, and the "
             "run is refused without this flag")
    ap.add_argument(
        "--residency", choices=("none", "stage"), default="none",
        help="forwarded to pbrun; 'stage' needs --data-manifest (#915)")
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
    if args.plan_only:
        if not args.json:
            ap.error("--plan-only requires --json PATH")
        plan, config, declared = _pbtest_population(
            Path(args.checkout).resolve(), args.paths or ["tests"], args.capabilities)
        tags = _pbtest_tags(args)
        plan["tags"] = {
            file: sorted(set(tags) | {tag for name in names for tag in config[name]["tags"]})
            for file, names in plan["capabilities"].items()}
        try:
            _write_report_bytes(Path(args.json), json.dumps(plan, indent=2).encode(), durable=True)
        except OSError as exc:
            sys.stderr.write(f"pbtest: cannot publish requested plan: {exc}\n")
            return 2
        for refusal in plan["refusals"]:
            sys.stderr.write(f"pbtest: {refusal['code']}: {refusal['reason']}\n")
        return 2 if plan["refusals"] else 0
    if args.python is None:
        ap.error("--python is required for execution")
    try:
        progress = _Progress(args.json)
    except OSError as exc:
        sys.stderr.write(f"pbtest: cannot initialize requested output: {exc}\n")
        return 2

    try:
        validate_shard_env(args.env)
        if submission.interpreter_of([args.python]) != args.python:
            raise ValueError("--python must name an absolute python* executable "
                             "so its path can be sealed into claim-time eligibility")
        if args.tmpdir is not None and not Path(args.tmpdir).is_absolute():
            raise ValueError("--tmpdir must be an absolute worker-visible path")
        shard_tmpdir = (args.tmpdir if args.tmpdir is not None else
                        fleet_roster.Roster.read(store_for(_PB_ENTRYPOINT).roster_path).action_tmpdir)
        if args.basetemp is not None:
            validate_basetemp(args.basetemp)
        if args.mem_gb < 1:
            raise ValueError("--mem-gb must be at least 1")
        require_gpu_memory_scope(gpu_memory_gb=args.gpu_memory_gb,
                                 gpu=args.gpu, transport=args.transport)
        if args.gpu_memory_gb is not None:
            adaptive_gpu.memory_budget_bytes(args.gpu_memory_gb)
        pytest_args = (parse_pytest_args(args.pytest_args, gpu=args.gpu,
                                        workers=args.workers_per_shard)
                       if args.pytest_args is not None else [])
    except (OSError, ValueError) as exc:
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
    if args.threads_per_shard is not None and args.threads_per_shard < 0:
        sys.stderr.write("--threads-per-shard cannot be negative\n")
        return 2
    # Every pytest-xdist worker is its own process and inherits the row's
    # native thread count, which PrismaBuild sets to the whole row's cores,
    # so N workers each asked for all of them: a 16-core shard of 16 workers
    # ran 16 x OMP_NUM_THREADS=16 and put sparklina at load 136 on 20 CPUs
    # (#1192).  A shard with several workers and no ceiling of its own
    # therefore gives each worker its share of the shard's cores.
    derived_threads = (args.workers_per_shard > 1 and args.cpus_per_shard is not None
                       and not args.threads_per_shard)
    if args.threads_per_shard is None and not derived_threads:
        args.threads_per_shard = 2
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

    if derived_threads:
        args.threads_per_shard = max(1, cpus_per_shard // args.workers_per_shard)
    minimum_cpus = args.workers_per_shard * max(1, args.threads_per_shard)
    if cpus_per_shard < minimum_cpus:
        sys.stderr.write(
            f"--cpus-per-shard must be at least {minimum_cpus} for "
            f"{args.workers_per_shard} workers and the declared thread ceiling\n")
        return 2

    checkout = Path(args.checkout).resolve()
    population, config, declared = _pbtest_population(
        checkout, args.paths or ["tests"], args.capabilities)
    for refusal in population["refusals"]:
        sys.stderr.write(f"pbtest: {refusal['reason']}\n")
    if population["refusals"]:
        return 2
    files = population["files"]
    history, history_problems = load_history(args.history)
    for problem in history_problems:
        sys.stderr.write(f"pbtest: --history {problem}\n")
    predicted = predict_durations(history)
    model_default = default_duration(predicted)
    # The packing model (#1246): prior ``--json`` reports are the receipts'
    # content in one file, so loading them reads no live state -- and a
    # submission never fails because one old report rotted.
    # Tests that read the shared mount undeclared cost the fleet a scan it
    # cannot see or stage (#915), so they run only with a manifest.
    if args.residency == "stage" and not args.data_manifest:
        sys.stderr.write("pbtest: --residency stage needs --data-manifest\n")
        return 2
    if args.data_manifest and not Path(args.data_manifest).is_file():
        sys.stderr.write(
            f"pbtest: --data-manifest {args.data_manifest!r} is not a file\n")
        return 2
    reading = fleet_data_files(checkout, files)
    if reading and not args.data_manifest:
        sys.stderr.write(
            "pbtest: these files are marked "
            f"@pytest.mark.{FLEET_DATA_MARKER}, so they read fleet data, and "
            "no --data-manifest declares it (#915); pass one, or leave them "
            "out of the run: " + ", ".join(reading) + "\n")
        return 2
    # The ceiling the model judges against is sealed later, beside the
    # deadline; the samples load and the predictions resolve above.  The
    # round-robin below is the no-history answer, not a first pass: the
    # history branch replaces it wholesale (#1303 review).
    # Resolve reviewed dependencies only inside each admitted worker action.
    # Embed the guard bytes in argv so neither a target-local helper path nor
    # an older receipt can silently omit the check. Unpinned projects retain
    # their existing commands and identities.
    try:
        checked_tmpdir = (shard_tmpdir if args.tmpdir is not None or
                         shard_tmpdir != fleet_roster.LEGACY_ACTION_TMPDIR else None)
        python_entry = shard_entry(args.python, checkout, tmpdir=checked_tmpdir,
                                   basetemp=args.basetemp,
                                   trace=pbtest_outcomes.TRACE_OPTION in pytest_args,
                                   collection=True,
                                   capabilities=bool(declared))
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
    tags = _pbtest_tags(args)
    shard_resources = {"cpu": cpus_per_shard, "mem_gb": args.mem_gb}
    if args.disk_metadata:
        shard_resources["disk_metadata"] = 1
    if args.gpu:
        shard_resources["gpu"] = 1
    # One placement question per distinct cohort (#1263, #1495): the
    # interpreter and dependency requirements every shard of a cohort names,
    # answered before anything is sealed -- a refusal fails the whole
    # submission fast, naming what no recorded worker can satisfy.
    cohort_list = pbtest_capabilities.cohorts(declared, files)
    if len(cohort_list) > args.shards:
        listing = "; ".join(
            f"{'portable' if not names else 'capabilities ' + json.dumps(list(names))}: "
            f"{len(members)} file(s)"
            for names, members in cohort_list)
        sys.stderr.write(
            f"pbtest: {len(cohort_list)} capability cohorts need at least "
            f"{len(cohort_list)} shards and --shards is {args.shards}; a "
            "cohort cannot share a shard with another and no file is "
            f"dropped: pass --shards >= {len(cohort_list)} (#1495). "
            f"Cohorts: {listing}\n")
        return 2
    plans = []
    weights = {}
    for names, members in cohort_list:
        weights[names] = (sum(predicted.get(name, model_default)
                              for name in members)
                          if predicted else float(len(members)))
    counts = allocate_shards(weights, args.shards)
    placement_by_tags: dict[tuple[str, ...], tuple] = {}
    for names, members in cohort_list:
        selection = (pbtest_capabilities.shard_selection(names, config)
                     if names else None)
        capability_tags = sorted({tag for name in names
                                  for tag in config[name]["tags"]})
        cohort_tags = sorted(set(tags) | set(capability_tags))
        key = tuple(cohort_tags)
        if key not in placement_by_tags:
            placement_by_tags[key] = placement(
                cohort_tags, gpu=args.gpu, timeout_s=args.timeout_s,
                test_timeout_s=args.test_timeout_s)
        cohort_ceilings, cohort_sealed_s, cohort_bound_s = (
            placement_by_tags[key])
        bound_env = ([f"{test_bound_contract.TIMEOUT_ENV}={cohort_bound_s:g}"]
                     if cohort_bound_s > 0 else [])
        if bound_env and cohort_sealed_s is not None:
            # A test that starts with less time left than its bound cannot
            # finish inside the shard; the plugin tightens its alarm to what
            # is left so the test is failed by name (#1309).
            bound_env.append(
                f"{test_bound_contract.SHARD_BUDGET_ENV}={cohort_sealed_s:g}")
        plans.append({"names": names, "members": members,
                      "tags": cohort_tags, "capability_tags": capability_tags,
                      "selection": selection, "shards": counts[names],
                      "ceilings": cohort_ceilings,
                      "sealed_s": cohort_sealed_s, "bound_s": cohort_bound_s,
                      "bound_env": bound_env})
    preflight_tags: set[tuple[str, ...]] = set()
    for plan in plans:
        key = tuple(plan["tags"])
        if key in preflight_tags:
            continue
        preflight_tags.add(key)
        kind, message = interpreter_refusal(
            fleet_queue(), args.python, tags=plan["tags"],
            resources=shard_resources, needs_gpu=bool(args.gpu))
        if message is not None:
            sys.stderr.write(message + "\n")
        if kind == "refusal":
            try:
                progress.refuse({"code": "interpreter_absent", "message": message})
            except OSError as exc:
                sys.stderr.write(f"pbtest: cannot write refusal: {exc}\n")
            return 2
        if plan["selection"] is None:
            continue
        intent = {"tags": plan["tags"], "resources": shard_resources,
                  "needs_gpu": bool(args.gpu),
                  "requires_files": [{"path": entry["path"]}
                                     for entry in plan["selection"]["dependencies"]
                                     if "path" in entry and "sha256" in entry]}
        if not intent["requires_files"]:
            # A tags-only fence seals no requirement (#1495): the digest
            # question below is only about bytes, and asking it here would
            # refuse a fleet whose workers offer no digest capability the
            # requirement-less row would never need.
            continue
        verdict = fleet_queue().dependency_placement_verdict(intent)
        paths = ", ".join(sorted(
            str(entry["path"]) for entry in intent["requires_files"]))
        if verdict == "unknown_capability":
            sys.stderr.write(
                f"pbtest: no recorded worker offers the "
                f"{contracts.DEPENDENCY_DIGEST_TAG} capability, so "
                f"the fenced shards ({paths}) would wait unclaimed for "
                "their whole --wait-s; run a worker generation that carries "
                "the contract first (#1495)\n")
            return 2
        if verdict == "absent":
            sys.stderr.write(
                f"pbtest: every recorded worker that could claim the fenced "
                f"shards names their dependencies absent: {paths} (#1495)\n")
            return 2
        if verdict == "unknown_paths":
            print(
                f"pbtest: no worker has answered for the fenced shards' "
                f"dependencies ({paths}) yet; the shards publish, the "
                "claim-time digest check guards every box, and the fleet's "
                "next poll places them where the bytes live", flush=True)
    # The sizes line prints beside the model below, after the ceiling the
    # model judges against is sealed -- printing it here would report the
    # round-robin the history then replaces.

    # torch sizes its thread pool from the affinity mask, so an unconstrained
    # shard on an 80-core box asks for 40 threads -- forty shards then ask for
    # 1,600 and the box spends its time context-switching.  Measured: 22 shards
    # at the default put dl380g10 at load 183 with 266 runnable processes and
    # 97% user, CPU-saturated while still holding 105 GB free.  Not an OOM, but
    # not work either.
    threads = []
    if args.threads_per_shard > 0:
        n = str(args.threads_per_shard)
        threads = [f"{key}={n}" for key in SHARD_THREAD_ENV]

    pytest_workers = (["-n", str(args.workers_per_shard)]
                      if args.workers_per_shard > 1 else [])

    # One read of the announcements per distinct tag set serves each plan's
    # sealed deadline and its per-test bound (:func:`placement`).  A run
    # with one portable cohort prints what it always printed; a fenced run
    # prints its cohorts, so the fences are visible beside the packing.
    if len(plans) == 1 and plans[0]["selection"] is None:
        ceilings = plans[0]["ceilings"]
        sealed_s = plans[0]["sealed_s"]
        test_bound_s = plans[0]["bound_s"]
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
        test_bound = ([f"{test_bound_contract.TIMEOUT_ENV}={test_bound_s:g}"]
                      if test_bound_s > 0 else [])
        if test_bound:
            print(f"pbtest: per-test bound {test_bound_s:g}s "
                  f"({test_bound_contract.TIMEOUT_ENV}); a test that outlives it "
                  "fails as itself instead of holding the shard to its ceiling",
                  flush=True)
            if sealed_s is not None:
                print(f"pbtest: each shard also seals its remaining budget "
                      f"({test_bound_contract.SHARD_BUDGET_ENV}={sealed_s:g}); a "
                      "test starting with less time left than its bound is failed "
                      "at what is left", flush=True)
    else:
        for plan in plans:
            label = ("portable" if not plan["names"]
                     else "capabilities " + json.dumps(list(plan["names"])))
            sealed_word = (
                f"seals execution_timeout_s={plan['sealed_s']:g}"
                if plan["sealed_s"] is not None else "seals no deadline")
            print(f"pbtest: cohort {label}: {len(plan['members'])} file(s) -> "
                  f"{plan['shards']} shard(s), tags={plan['tags']}, "
                  f"{sealed_word}, per-test bound {plan['bound_s']:g}s",
                  flush=True)
            for entry in (plan["selection"] or {}).get("dependencies") or ():
                if "kind" in entry:
                    print(f"pbtest:   pins {entry['module']} in "
                          f"{entry['distribution']!r} at payload "
                          f"{entry['sha256']}", flush=True)
                else:
                    print(f"pbtest:   pins {entry['path']} at "
                          f"{entry['sha256']}", flush=True)
    buckets: list[list[str]] = []
    plan_of: list[dict] = []
    if predicted:
        # The ceiling is sealed now, so the model can judge against it: the
        # slow file shards alone, the rest pack longest-first (#1246).
        assert model_default is not None
        default = model_default
        measured = sum(1 for name in files if name in predicted)
        print(f"pbtest: duration model from {len(args.history)} history "
              f"file(s): {measured}/{len(files)} files measured, the rest "
              f"default to the median {default:.1f}s, predictions are "
              "per-file maxima", flush=True)
    for plan in plans:
        if predicted:
            plan_buckets = shard(plan["members"], plan["shards"],
                                 durations=predicted,
                                 ceiling=plan["sealed_s"],
                                 model_default=model_default)
        else:
            plan_buckets = shard(plan["members"], plan["shards"])
        buckets.extend(plan_buckets)
        plan_of.extend([plan] * len(plan_buckets))
    if predicted:
        totals = [sum(predicted.get(name, default) for name in bucket)
                  for bucket in buckets]
        print("pbtest: predicted shard totals: " +
              ", ".join(f"shard {index} {total:.0f}s"
                           for index, total in enumerate(totals)), flush=True)
    sizes = [len(b) for b in buckets]
    print(f"{len(files)} files -> {len(buckets)} shards "
          f"(min {min(sizes)}, max {max(sizes)} files per shard), tags={tags}, "
          f"transport={args.transport}",
          flush=True)
    try:
        progress.configure(len(buckets))
    except OSError as exc:
        sys.stderr.write(f"pbtest: cannot initialize shard progress: {exc}\n")
        return 2
    procs = []
    def run_client(index, proc, **kwargs):
        try:
            run_shard(index, proc, progress=progress, **kwargs)
        finally:
            progress.observe(index, kwargs["result"], completed=True)
    # Pool shards are published from here, each plan's checkout frozen once
    # (``_Templates``); the SLURM lane keeps one ``pbrun`` client per shard.
    templates = _Templates(store_for(_PB_ENTRYPOINT)) if args.transport == "pool" else None
    owners: dict[int, SubmissionRequest] = {}
    # A shard that writes its own --surface-json outside the checkout names a
    # path only it has; its placement answer is its own, so it seals alone.
    surfaces = [pytest_args[position + 1] for position, option in enumerate(pytest_args[:-1])
                if option == "--surface-json"]
    shared = not any(os.path.isabs(path) for path in surfaces)
    requested_roots = [
        [os.path.normpath(os.path.relpath(checkout / raw, checkout)),
         "directory" if (checkout / raw).is_dir() else "file"]
        for raw in args.paths or ["tests"]]
    for index, bucket in enumerate(buckets):
        plan = plan_of[index]
        pins = ([entry for entry in plan["selection"]["dependencies"] if "kind" not in entry]
                if plan["selection"] is not None else [])
        request = SubmissionRequest(cwd=str(checkout), transport=args.transport,
            tag=(*tags, *plan["capability_tags"]),
            anywhere=not tags and not plan["capability_tags"] and not pool.is_box_local_path(RUNTIME_ROOT),
            demand=",".join(f"{kind}={value}" for kind, value in shard_resources.items()
                            if kind not in {"cpu", "gpu"}),
            cpus=shard_resources["cpu"], gpu=args.gpu, gpu_memory_gb=args.gpu_memory_gb,
            timeout_s=plan["sealed_s"], wait_s=args.wait_s, priority=args.priority,
            # CPU shards may restart interruptions, never a reported red test.
            # GPU shards retain their existing retry policy.
            retry_safe=args.transport == "pool" and not args.gpu,
            retry_scope="interruption" if args.transport == "pool" and not args.gpu else None,
            max_attempts=3 if args.transport == "pool" and not args.gpu else 1,
            profile=args.profile,
            data_manifest=str(Path(args.data_manifest).resolve()) if args.data_manifest else None,
            requires_files=json.dumps(pins) if pins else None, residency=args.residency)
        # Explicit forwarding replaces addopts from both environment and
        # project config: either can hide -n auto or --dist each. Only an
        # explicit forwarding request seals the empty addopts override.
        explicit_env = ["PYTEST_ADDOPTS="] if args.pytest_args is not None else []
        explicit_options = ["-o", "addopts="] if args.pytest_args is not None else []
        environment = [f"TMPDIR={shard_tmpdir}",
                       f"{pbtest_outcomes.RECORD_BINDING_ENV}={pbtest_outcomes.RECORD_BINDING_VERSION}",
                       *threads, *plan["bound_env"], *explicit_env,
                       "PYTHONPATH=src:experiments", *args.env]
        spec: dict = {"files": bucket, "roots": requested_roots}
        if plan["selection"] is not None:
            spec["capabilities"] = plan["selection"]
        arguments = [
            json.dumps(spec),
            "-q", "--no-header",
            "-p", "no:cacheprovider", *explicit_options,
            *shard_pytest_args(pytest_args, index), *pytest_workers, *bucket,
        ]
        request = shard_request(request, environment=environment,
                                python_entry=python_entry, arguments=arguments)
        owner = owners.setdefault(id(plan), request)
        # Hold at most ``--max-clients`` pbrun clients (#1348): the next shard
        # starts when one exits.  Its wait budget starts here, at its own
        # submission, not while it queued behind the others.
        if args.max_clients > 0:
            while sum(1 for entry in procs if entry[2].is_alive()) >= args.max_clients:
                next(entry[2] for entry in procs if entry[2].is_alive()).join(0.05)
        # The shard's own wait budget, spanning every attempt (#1102).
        deadline = time.monotonic() + args.wait_s
        # ``errors="replace"``: a stray byte must not end the drain thread,
        # which would leave the pipe to fill and the shard's wait blocked.
        shard_result: dict = {}
        if templates is None:
            attempt = functools.partial(start_shard, index, shard_command(request),
                                        result=shard_result, files=bucket, progress=progress)
        else:
            attempt = functools.partial(publish_shard, templates, index, request,
                                        plan=id(plan), owner=owner, shared=shared,
                                        result=shard_result, files=bucket,
                                        progress=progress, deadline=deadline)
        proc = attempt()
        drain = threading.Thread(
            target=run_client, args=(index, proc),
            kwargs={"again": attempt, "wait_s": args.wait_s, "deadline": deadline,
                    "result": shard_result, "files": bucket},
            name=f"pbtest-shard-{index}", daemon=True)
        drain.start()
        procs.append((index, bucket, drain, shard_result))
    # The tree every shard ran is the tree that was sealed; one more identity
    # read once the last shard is published says whether it is still the
    # tree on disk.  A shard still retrying an offer timeout may yet publish,
    # so this waits for each shard to publish or end.
    changed = None
    if templates is not None:
        while True:
            waiting = [drain for _, _, drain, shard_result in procs
                       if drain.is_alive() and not shard_result.get("published")]
            if not waiting:
                break
            waiting[0].join(0.05)
        changed = templates.changed()
        if changed is not None:
            sys.stderr.write(changed + "\n")
            sys.stderr.flush()

    observations = []
    events_by_shard = {}
    for index, bucket, drain, shard_result in procs:
        # EOF comes when the shard's last pbrun exits, so the join is the wait.
        drain.join()
        lines = shard_result["lines"]
        attempts = shard_result["attempts"]
        returncode = shard_result["returncode"]
        out = "".join(lines)
        events = shard_result['client_events']
        events_by_shard[index] = events
        if events.problem is not None:
            _say(f"shard {index:>3} pbtest: client report unusable: {events.problem}")
        ending = events.ending if events.problem is None else None
        if ending is not None:
            replay = action_client.read_cached_output(events.identity, ending)
            if isinstance(replay, action_client.CachedOutput):
                out = replay.text or out
            elif isinstance(replay, action_client.CachedOutputUnavailable):
                _say(f"shard {index:>3} pbtest: cached output unavailable: {replay.reason}")
        timed_out = (returncode != 0 and ending is not None
                     and isinstance(ending.verdict, action_client.ClientExecution)
                     and ending.verdict.execution_timeout)
        observations.append({"shard": index, "files": bucket, "returncode": returncode,
            "output": out, "timed_out": timed_out, "attempts": attempts,
            # The same owner/version was sealed into python_entry, independently
            # of stdout. Old report rows lack this declaration and stay legacy.
            "record_binding": pbtest_outcomes.RECORD_BINDING_VERSION,
            "action_key": shard_result.get("action_key")})
    verdict = pbtest_outcomes.judge_shards(observations, requested_files=files)
    results = []
    for observation, shard_verdict in zip(observations, verdict["shards"], strict=True):
        index, bucket = observation["shard"], observation["files"]
        returncode, attempts = observation["returncode"], observation["attempts"]
        out, timed_out = observation["output"], observation["timed_out"]
        tail = [line for line in out.strip().splitlines() if line.strip()]
        summary, ran = shard_verdict["summary"], shard_verdict["ran"]
        test_completion = shard_verdict["completion"]
        # Missing terminal output must stay visible: an empty summary once
        # hid 74 tests behind a killed submission (#208). But absence of the
        # summary cannot establish absence of execution (#1365): a deadline
        # can cut off a shard after some cases ran and before final counts.
        # ``ran`` now means "pytest reported a terminal summary", which is the
        # question the flag is actually asked.  It used to mean "some line
        # mentioned passing, failing or an error", and those are not the same
        # claim: the second is true of a shard that died in argparse.
        if not ran:
            # Preserve the observed ending without inventing coverage. A
            # signal, timeout, or refused submission may leave no summary;
            # their return codes distinguish endings, not executed cases.
            rc = returncode
            how = f"signal {-rc}" if rc < 0 else f"rc={rc}"
            events = events_by_shard[index]
            ending = events.ending if events.problem is None else None
            unobserved = (ending.verdict.action_key[:12]
                          if ending is not None and isinstance(ending.verdict, action_client.ClientUnavailable)
                          and ending.verdict.action_key is not None else None)
            if unobserved is not None:
                # pbrun could not read the shard's ending. The action may have
                # run to completion or may still be running, so "did not run"
                # would be a claim nobody observed. It is still not green.
                summary = (f"OUTCOME UNOBSERVED -- {len(bucket)} file(s) have no "
                           f"observed result (rc=74; action {unobserved} may still "
                           "be running or may already have landed; read "
                           f"pb-queue/{{done,failed,withdrawn}}/{unobserved}*.json "
                           "before rerunning)")
            elif any(re.search(r"\[Errno 28\]|\bENOSPC\b|No space left on device", line)
                     for line in tail):
                summary = (f"STORAGE EXHAUSTED (ENOSPC: No space left on device) -- "
                           f"{len(bucket)} file(s) have no verified final result "
                           f"(the shard ended {how}; execution/coverage unknown)")
            elif test_completion["status"] == "complete":
                label = ("TESTS COMPLETE; PROCESS DID NOT EXIT" if timed_out else
                         "TESTS COMPLETE; FINAL OUTCOME UNVERIFIED")
                summary = (f"{label} -- {len(test_completion['collected'])} "
                           f"collected test(s) finished teardown (the shard ended "
                           f"{how}; final outcomes unverified)")
            else:
                summary = (f"NO PYTEST SUMMARY -- {len(bucket)} file(s) have no "
                           f"verified final result (the shard ended {how}; "
                           "execution/coverage unknown)")
                if timed_out:
                    summary += "; TEST COMPLETION UNVERIFIED"
        # Each skip by node ID, with its reason (#942).  ``None`` is "this
        # shard printed no record", which is not "it skipped nothing".
        skipped = shard_verdict["skipped"]
        # The key as structured fields beside the returncode, so a report can
        # cite the action rather than a prefix dug out of ``output`` (#1012).
        action_key = observation["action_key"]
        predicted_s = (round(sum(predicted.get(name, model_default)
                                 for name in bucket), 1)
                       if predicted else None)
        actual_s = summary_seconds(summary) if ran else None
        results.append({"schema": "prismabuild.pbtest_report.v1",
                        "shard": index, "files": bucket,
                        "record_binding": observation["record_binding"],
                        "returncode": returncode, "action_key": action_key,
                        "receipt_path": receipt_path(action_key),
                        "summary": summary,
                        "capabilities": plan_of[index]["selection"],
                        # The model's prediction for this shard and pytest's
                        # own session seconds beside it, so the report can say
                        # whether the packing bought what it promised (#1246).
                        "predicted_s": predicted_s,
                        "pytest_s": actual_s,
                        "test_completion": test_completion,
                        "timed_out": timed_out,
                        "ran": ran, "skipped": skipped, "attempts": attempts,
                        "output": out, "reconciliation": shard_verdict["reconciliation"],
                        "verdict": shard_verdict})
        state = "ok" if returncode == 0 else f"rc={returncode}"
        # A resubmitted shard says so on its own line, so the receipt records
        # that its first submission was refused (#1102).
        retried = (f" [attempt {attempts}; {attempts - 1} earlier submission(s) "
                   "refused by a worker-offer discovery timeout]"
                   if attempts > 1 else "")
        keyed = (f" [action {action_key}]" if action_key else
                 " [no action key: the client reported no submission identity]")
        timed = ""
        if predicted:
            have = f"{actual_s:.1f}s" if actual_s is not None else "unknown"
            timed = f" [predicted {predicted_s:.1f}s, pytest {have}]"
        completion_note = ""
        if ran and timed_out:
            label = ("TESTS COMPLETE; PROCESS DID NOT EXIT"
                     if test_completion["status"] == "complete"
                     else "TEST COMPLETION UNVERIFIED")
            completion_note = f" [{label}]"
        _say(f"shard {index:>3} {state:<8} {summary}{completion_note}"
             f"{timed}{retried}{keyed}")
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

    print_reconciliation(results)
    if predicted:
        # Item 4 of #1246: the run reports what the model promised against
        # what pytest measured, per shard and in total, so the next history
        # flag is an informed choice and a stale model shows itself.
        print("\npredicted vs pytest seconds per shard:")
        total_want = 0.0
        total_have = 0.0
        unknown_shards = 0
        for r in results:
            want = r["predicted_s"]
            have = r["pytest_s"]
            total_want += want or 0.0
            if have is None:
                # An unobserved shard is not zero seconds of pytest: it
                # counts at the packing default, the same floor an
                # unmeasured file packs at (#1303 review).
                have = default
                unknown_shards += 1
            total_have += have
            shown = (f"{r['pytest_s']:.1f}s" if r["pytest_s"] is not None
                     else f"unknown (counts {default:.1f}s)")
            print(f"  shard {r['shard']:>3} predicted {want:.1f}s, "
                  f"pytest {shown}")
        total_note = (f" (includes {unknown_shards} unknown shard(s) at "
                      f"the {default:.1f}s default)" if unknown_shards else "")
        print(f"total predicted {total_want:.1f}s, total pytest "
              f"{total_have:.1f}s{total_note}")

    failed = [row for row, shard_verdict in zip(results, verdict["shards"], strict=True)
              if not shard_verdict["green"]]
    if any(p["code"] == "population_empty" for p in verdict["problems"]):
        print("pbtest: no tests collected in the requested population; no passing test coverage")
    print(f"\n{len(results) - len(failed)}/{len(results)} shards green")
    for r in failed:
        print(f"\n--- shard {r['shard']} ({', '.join(r['files'])})")
        # The full pytest failure section precedes the final 25 lines on a
        # busy shard. Keep the actual tracebacks visible without another run.
        print("\n".join(displayed(r["output"])))
    if args.json:
        try:
            report_bytes = json.dumps(results, indent=1).encode()
            _write_report_bytes(Path(args.json), report_bytes, durable=True)
            progress.finish(report_bytes)
        except OSError as exc:
            sys.stderr.write(f"pbtest: cannot publish requested output: {exc}\n")
            return 1
    if changed is not None:
        sys.stderr.write(changed + "\n")
    return 0 if verdict["green"] and not progress.failed and changed is None else 1


if __name__ == "__main__":
    raise SystemExit(main())

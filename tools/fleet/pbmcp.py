#!/usr/bin/env python3
"""Serve the pull queue to an agent as structured data, read-only and bounded.

An agent that wants to know what its own submission is doing has, without
this, exactly one route: run ``pbstatus`` and parse the table it printed for
a person.  The table is arranged for reading, its columns move when the
screen improves, and half of what an agent wants -- the sealed demand, the
attempt history, the receipt -- is not on it at all.  So the agent greps, and
a grep against a human table is a parser that breaks silently on the next
cosmetic change.  This answers the same questions in JSON, over the protocol
the agent's client already speaks.

Two properties, and they are the whole of the design.

**Read-only by construction.**  Nothing here renames, writes, mints, locks or
claims.  It never calls ``ensure_layout``, ``_transition_locked``,
``record_pass``, ``claim``, ``publish`` or ``withdraw``, and
``tests/test_pbmcp_read_only.py`` asserts both halves of that: an import
surface with no mutating name in it, and every tool run to completion against
a queue root whose every directory has had its write bits removed.  The
consequence an agent should trust is not "it happens not to write today": it
is that a write from this process would have to get past a test that fails on
the attempt.

**Bounded.**  Every read of the shared mount goes through ``pbstatus.bounded``
(#358), which runs it in a child this process can abandon, because a ``stat``
on a hard NFS mount need not return at any deadline and no in-process timeout
can make it.  A section that does not answer inside the caller's deadline is
named in ``timed_out`` and the response says ``complete: false``.  An agent
asking a wedged mount what its job is doing gets an answer that says the
mount is wedged, in five seconds, rather than a hung tool call and one more
``D``-state process on a box that already has too many (#350).

What it deliberately does not do:

*   Submit.  Submission goes through ``pbrun``, which is where the permission
    hooks that gate it live; an MCP tool that published to the queue would be
    a way around them.
*   Take the transition lock to resolve a preemption handoff.  ``pbrun``'s
    ``landed_outcome`` does, and it is right to: it is deciding what to tell a
    waiter.  This is reporting, so it re-derives the same successor rule
    unlocked and stamps ``unlocked_read`` on the answer -- a requeue published
    in the microseconds since the read reads as "not yet visible", never as
    "your work was abandoned".
*   Expand an attempt's logs.  ``pool.attempt_outcomes`` reads every stdout
    and stderr whole and hashes them, which is the right contract for a
    verifier and the wrong one for a status call on a run that printed a
    gigabyte.  The attempt records are read directly instead, and their log
    metadata is reported; ``pb_log`` seeks to the end of one log and reads a
    capped tail.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time
from collections.abc import Callable, Mapping, Sequence

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402
import pbstatus  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import core as pb, pool  # noqa: E402

#: The envelope every tool result carries, versioned so a client can tell.
SCHEMA_V1 = "prismaquant.prismabuild.pbmcp.v1"

#: Protocol revisions this server will speak.  The client's own revision is
#: echoed when it is one of these, per the MCP lifecycle: a server that always
#: answers with its favourite revision tells the client nothing about whether
#: they agree.
# 2025-03-26 requires receiving JSON-RPC batches; this stdio subset handles
# individual messages. Negotiate a supported revision instead of echoing it.
PROTOCOL_VERSIONS = ("2025-06-18", "2024-11-05")
DEFAULT_PROTOCOL_VERSION = "2024-11-05"

SERVER_NAME = "prismabuild"
SERVER_VERSION = "1"

#: How long one tool call may spend on the shared mount before it answers
#: with what it has.  Five seconds because the caller is an agent inside a
#: tool call and a wedged mount must cost it a sentence, not a session.
DEFAULT_DEADLINE_S = 5.0

#: How many terminal records ``pb_status`` reads, and the ceiling on the
#: window ``pb_actions`` filters inside.
DEFAULT_RECENT = 20
DEFAULT_ACTIONS_LIMIT = 50

#: The most bytes ``pb_log`` will read from the tail of one log.  A log is
#: never read whole: the reader seeks to ``size - cap`` and reads once.
DEFAULT_LOG_TAIL_BYTES = 65536
DEFAULT_TAIL_LINES = 40

#: The states one action key can be found in, in the order a reader should
#: prefer them: a live record outranks a terminal one, because a key that was
#: re-submitted after an ending has both.
STATES = (pool.READY, pool.CLAIMED, pool.DONE, pool.FAILED, pool.WITHDRAWN)

#: JSON-RPC codes, from the specification.
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603


class ToolError(Exception):
    """A tool ran and could not answer, which is a result, not a protocol fault.

    MCP draws this line deliberately: a transport-level error means the client
    asked wrongly, and a client cannot fix "no action starts with that
    prefix" by asking differently.  So an ambiguous prefix comes back as a
    tool result with ``isError`` and the candidates in it, where the model
    reading the result can act on it.
    """

    def __init__(self, message: str, **detail: object) -> None:
        super().__init__(message)
        self.detail = detail


class InvalidArguments(ToolError):
    """Arguments violate the advertised input schema before the tool runs."""


def _validate_arguments(value: object, schema: Mapping[str, object],
                        where: str = "arguments") -> None:
    """Enforce the JSON Schema vocabulary used by this server's tool inputs."""
    kind = schema["type"]
    number = type(value) is int or (type(value) is float and math.isfinite(value))
    valid = {
        "object": isinstance(value, Mapping),
        "array": isinstance(value, list),
        "string": isinstance(value, str),
        "boolean": type(value) is bool,
        "number": number,
        "integer": number and (type(value) is int or value.is_integer()),
    }
    if not valid.get(kind, False):
        raise InvalidArguments(f"{where} must be {kind}")
    if "minimum" in schema and value < schema["minimum"]:
        raise InvalidArguments(f"{where} must be at least {schema['minimum']}")
    if "enum" in schema and value not in schema["enum"]:
        raise InvalidArguments(f"{where} must be one of {schema['enum']}")
    if kind == "object":
        properties = schema["properties"]
        for key in schema.get("required", ()):
            if key not in value:
                raise InvalidArguments(f"{where} requires {key}")
        for key, item in value.items():
            if key not in properties:
                raise InvalidArguments(f"{where} has unknown property {key}")
            _validate_arguments(item, properties[key], f"{where}.{key}")
    elif kind == "array":
        for index, item in enumerate(value):
            _validate_arguments(item, schema["items"], f"{where}[{index}]")


# --------------------------------------------------------------------------
# Bounded reading
# --------------------------------------------------------------------------

class Call:
    """One tool call's deadline, its partial-read accounting, and its envelope.

    Every section of a call shares one budget, exactly as ``pbstatus`` does:
    a call that spends four of its five seconds on the census has one left
    for the endings, and a section with nothing left is reported as timed out
    without being started.  That is what keeps the whole call bounded rather
    than each of its parts.
    """

    def __init__(self, tool: str, *, deadline_s: float, startup_generation: str | None,
                 repo_link: Path, abandoned: list[dict] | None = None) -> None:
        self.tool = tool
        self.deadline_s = float(deadline_s)
        self.deadline = pbstatus.Deadline(deadline_s)
        self.startup_generation = startup_generation
        self.repo_link = Path(repo_link)
        self.timed_out: list[str] = []
        self.unavailable: list[dict] = []
        # The session owns retained children across calls, including startup.
        self.abandoned = abandoned if abandoned is not None else []

    def read(self, section: str, read: Callable[[], object], *, default=None):
        """Run one shared-mount read, or record why its answer is missing.

        The default is ``None`` and every caller here keeps it, so ``null`` in
        a payload means "this was not read" everywhere in this file.  An empty
        list or object is an answer -- no endings, no matching actions, no
        such action -- and handing one back for a read that never returned
        makes the payload contradict the ``timed_out`` beside it.
        """

        # Never grow a pile of blocked readers as a long-lived client polls.
        # waitpid is local, nonblocking, and scoped to our retained children;
        # no signal or shared-mount identity lookup is needed to collect exits.
        pending = []
        for child in self.abandoned:
            try:
                done, _status = os.waitpid(child["pid"], os.WNOHANG)
            except ChildProcessError:
                continue
            except OSError:
                done = 0
            if not done:
                pending.append(child)
        self.abandoned[:] = pending
        if self.abandoned:
            self.unavailable.append({
                "section": section, "type": "ReaderStillRunning",
                "error": "shared read suppressed until the retained reader exits",
            })
            return default
        outcome = pbstatus.bounded(section, read, deadline=self.deadline,
                                   abandoned=self.abandoned)
        if outcome["status"] == "ok":
            return outcome["value"]
        if outcome["status"] == "timed_out":
            self.timed_out.append(section)
        else:
            self.unavailable.append({"section": section,
                                     "type": outcome.get("type"),
                                     "error": outcome.get("error")})
        return default

    def generation(self) -> tuple[str | None, bool | None]:
        """What ``repo/`` points at now, and whether it has moved since startup.

        ``readlink`` on the shared mount is a shared-mount read like any
        other, so it is bounded like any other -- and when it does not answer,
        staleness is ``None``.  Never ``False``: "the link did not answer" and
        "the link is where it was" are opposite facts, and defaulting the
        first to the second would tell an agent it is on the published
        generation at exactly the moment nothing can say so.
        """

        target = self.read("repo-link", lambda: _link_target(self.repo_link),
                           default=_MISSING)
        if target is _MISSING:
            return None, None
        current = None if target is None else str(target)
        if current is None or self.startup_generation is None:
            return current, None
        return current, current != self.startup_generation

    def envelope(self, payload: Mapping[str, object]) -> dict:
        generation, stale = self.generation()
        return {
            "schema": SCHEMA_V1,
            "tool": self.tool,
            "generated_unix": time.time(),
            "deadline_s": self.deadline_s,
            "complete": not self.timed_out and not self.unavailable and not self.abandoned,
            "timed_out": list(self.timed_out),
            "unavailable": list(self.unavailable),
            "abandoned_readers": list(self.abandoned),
            "generation": generation,
            "generation_stale": stale,
            "started_from_generation": (
                None if self.startup_generation is None
                else RUNTIME_ROOT.name == self.startup_generation),
            **dict(payload),
        }


_MISSING = object()


def _link_target(repo_link: Path) -> str | None:
    """The name ``repo/`` resolves to, or ``None`` when there is no link.

    The name rather than the path: a generation is identified by its
    directory name (``<commit12>-<unix>-<hash>``), and that is what
    ``RUNTIME_VERSION.json`` calls the generation too.
    """

    try:
        return os.path.basename(os.readlink(repo_link)) or None
    except OSError:
        try:
            return repo_link.resolve(strict=True).name
        except OSError:
            return None


# --------------------------------------------------------------------------
# Queue reading -- every one of these opens for reading and nothing else
# --------------------------------------------------------------------------

def _absent(error: OSError) -> bool:
    """Whether this error says "nothing is there", not "the mount did not answer".

    ``ENOENT`` and ``ENOTDIR`` are absence: a key that was never published, a
    state directory a queue has not created yet.  Every other ``OSError`` --
    ``ESTALE`` and ``EIO`` are the two the fleet actually sees (#208) -- is a
    mount that failed to answer, and ``pool._read_json`` keeps those loud on
    purpose.  A reader that swallowed them would answer "no action starts with
    that prefix" during an NFS burst, with ``complete: true`` next to it, and
    those two sentences call for opposite responses.  Every call site below
    runs inside a ``call.read`` lambda, so a raised error becomes a named
    section with ``complete: false``, never an internal-error frame.
    """

    return isinstance(error, (FileNotFoundError, NotADirectoryError))


def _read_record(path: Path) -> dict | None:
    try:
        return pool._read_json(path)
    except OSError as error:
        if _absent(error):
            return None
        raise
    except (ValueError, pool.PoolContractError):
        return None


def _keys_in(directory: Path) -> list[str]:
    try:
        with os.scandir(directory) as entries:
            return sorted(entry.name[:-5] for entry in entries
                          if entry.name.endswith(".json") and entry.is_file())
    except OSError as error:
        if _absent(error):
            return []
        raise


def _valid_prefix(key_prefix: object) -> str:
    """The prefix, checked before any mount read so a bad one says so.

    Validated outside the bounded child on purpose: an exception raised inside
    ``bounded`` comes back as a section that failed, and "the queue did not
    answer" is the wrong sentence for "you passed an empty string".
    """

    prefix = str(key_prefix or "").strip().lower()
    if not prefix:
        raise ToolError("key_prefix must not be empty")
    if any(character not in "0123456789abcdef" for character in prefix):
        raise ToolError("an action key is hexadecimal; "
                        f"{key_prefix!r} cannot name one")
    return prefix


def _resolve_prefix(queue_root: Path, key_prefix: str) -> list[str]:
    """Every action key under this prefix, across live and terminal states.

    A prefix rather than a key because that is what an operator and an agent
    both hold: ``pbstatus`` prints twelve characters and ``pbrun`` says
    ``pb-<key12>``.  An ambiguous prefix is answered with its candidates
    rather than with the first match: picking one would be a guess, and the
    caller can disambiguate with two more characters.
    """

    prefix = _valid_prefix(key_prefix)
    found: set[str] = set()
    for state in STATES:
        found.update(key for key in _keys_in(Path(queue_root) / state)
                     if key.startswith(prefix))
    decisions = Path(queue_root) / pool.WITHDRAWN / "decisions"
    try:
        with os.scandir(decisions) as entries:
            found.update(entry.name for entry in entries
                         if entry.is_dir() and entry.name.startswith(prefix))
    except OSError as error:
        if not _absent(error):
            raise
    try:
        with os.scandir(Path(queue_root) / pool.ATTEMPTS) as entries:
            found.update(entry.name for entry in entries
                         if entry.is_dir() and entry.name.startswith(prefix))
    except OSError as error:
        if not _absent(error):
            raise
    return sorted(found)


def _records_for(queue_root: Path, key: str) -> dict[str, dict]:
    queue = pool.PoolQueue(Path(queue_root).absolute())
    records = {}
    for state in STATES:
        record = _read_record(queue.item_path(state, key))
        if record is not None:
            records[state] = record
    return records


def _attempt_records(queue_root: Path, record: Mapping[str, object]) -> list[dict]:
    """The immutable attempt outcomes this record links, without their logs.

    ``pool.attempt_outcomes`` is the verifying reader and reads every log
    whole to check its digest.  A status call must not: an action that printed
    a gigabyte would cost a gigabyte to ask about.  The link's canonical path
    is still checked against ``attempt_path``, so a record pointing somewhere
    else is reported rather than followed.
    """

    queue = pool.PoolQueue(Path(queue_root).absolute())
    history = record.get("attempt_history")
    if not isinstance(history, list):
        return []
    outcomes: list[dict] = []
    for link in history:
        if not isinstance(link, Mapping):
            continue
        number = link.get("attempt")
        if type(number) is not int:
            continue
        try:
            expected = queue.attempt_path(record, number)
        except pool.PoolContractError as exc:
            outcomes.append({"attempt": number, "unreadable": str(exc)})
            continue
        relative = str(expected.relative_to(queue.root))
        if link.get("outcome") != relative:
            outcomes.append({"attempt": number,
                             "unreadable": "outcome link is not its canonical path",
                             "link": link.get("outcome"), "canonical": relative})
            continue
        value = _read_record(expected)
        if value is None:
            outcomes.append({"attempt": number, "unreadable": "unreadable outcome",
                             "path": str(expected)})
            continue
        outcomes.append({**value, "path": str(expected)})
    outcomes.sort(key=lambda one: one.get("attempt") or 0)
    return outcomes


def _archived_preemption_records(queue_root: Path, key: str) -> list[dict]:
    """Preemption handoffs recovered from immutable attempts, without the logs.

    Needed because a mutable ``done``/``failed`` row is one slot per action
    key: a later generation overwrites it, and an intermediate handoff then
    survives nowhere but here.  ``pool.archived_preemption_outcomes`` is the
    verifying reader for this evidence and reaches ``adopted_attempt_summary``,
    which reads every linked log whole to check its digest -- so an action that
    printed a gigabyte would cost a gigabyte to ask about.  Only two fields
    decide a successor, the attempt's own ``published_unix`` and the
    ``supersedes_withdrawal`` its ``preemption_context`` preserved, and both
    are on the attempt record itself.  They are lifted directly and the whole
    answer is stamped ``unlocked_read``, so an unverified reading is never
    passed off as the verified one.
    """

    found: list[dict] = []
    base = Path(queue_root) / pool.ATTEMPTS / key
    try:
        with os.scandir(base) as entries:
            generations = sorted(entry.path for entry in entries if entry.is_dir())
    except OSError as error:
        if _absent(error):
            return found
        raise
    for generation in generations:
        try:
            with os.scandir(generation) as entries:
                attempts = sorted(entry.path for entry in entries
                                  if entry.is_file() and entry.name.endswith(".json"))
        except OSError as error:
            if not _absent(error):
                raise
            continue
        for path in attempts:
            value = _read_record(Path(path))
            if value is None:
                continue
            context = value.get("preemption_context")
            if not isinstance(context, Mapping):
                continue
            # An intermediate failed attempt is not an ending, which is the
            # same line ``archived_preemption_outcomes`` draws.
            if value.get("disposition") not in (pool.DONE, pool.FAILED):
                continue
            found.append({**value, **context, "path": path})
    return found


def _requeued_as(queue_root: Path, key: str, ending: Mapping[str, object]) -> float | None:
    """The generation a preemption requeued this one as, read without the lock.

    ``pbrun._preemption_requeue`` holds the transition lock while it looks,
    because it is deciding what to tell a waiter and must not answer in the
    middle of admission's withdraw-then-republish.  This is reporting, and
    taking that lock would make a read-only server a participant in the
    protocol it is describing.  So the same rule runs over the same three
    sources unlocked -- the immutable withdrawal decisions, the archived
    preemption handoffs, and the five mutable state rows -- and the answer is
    stamped ``unlocked_read``: a ``null`` here means "no successor is visible
    yet", which covers both "there is none" and "the handoff is in flight".
    """

    generation = ending.get("published_unix")
    if ending.get("preempted_by") is None or type(generation) not in (int, float):
        return None
    if str(ending.get("status") or "") != "withdrawn":
        return None
    queue = pool.PoolQueue(Path(queue_root).absolute())
    candidates: list[Mapping[str, object]] = []
    try:
        candidates.extend(record for _path, record in queue.withdrawal_decisions(key))
    except OSError as error:
        if not _absent(error):
            raise
    except pool.PoolContractError:
        pass
    candidates.extend(_archived_preemption_records(queue_root, key))
    candidates.extend(_records_for(queue_root, key).values())
    successors = []
    for record in candidates:
        theirs = record.get("published_unix")
        parent = record.get("supersedes_withdrawal")
        if (type(theirs) not in (int, float) or not isinstance(parent, Mapping)
                or parent.get("published_unix") != generation
                or parent.get("preempted_by") != ending.get("preempted_by")):
            continue
        if float(theirs) > float(generation):
            successors.append(float(theirs))
    return min(successors) if successors else None


def _sealed(record: Mapping[str, object]) -> dict:
    """What the submission asked for, as ``publish`` sealed it into the item."""

    return {
        field: record.get(field)
        for field in ("cas_root", "worker_script", "tags", "needs_gpu", "priority",
                      "resources", "max_attempts", "retry_safe", "container_owner",
                      "checkout_root", "checkout_snapshot", "published_unix",
                      "published_by", "supersedes_withdrawal", "preempted_by")
    }


def _derived_claim(record: Mapping[str, object]) -> dict:
    """The local-result claim digest for this action, derived the way it was made.

    The digest is not on the queue record.  It is returned by
    ``run_action_locally`` in the worker's own result and never travels into
    the item, so an agent holding a queue key and wanting to check its result
    could not get there from here.  It is derivable, though, and derivable is
    not the same as guessed: the claim is a hash of the action manifest, the
    resolved checkout root, and the working directory and result path the
    manifest declares.  The manifest is in the CAS at
    ``requests/<xx>/<key>.json``, the checkout root is on the record, and
    ``core.canonical_sha256`` is the producer's own hash.  So this recomputes
    the same body with the same function and reports whether the claim file
    it names is actually there.

    ``checkout_snapshot`` submissions get ``None`` and the reason: the
    checkout is materialised per box, its resolved path is what the digest
    binds, and that path is not on the record.
    """

    key = str(record.get("action_key") or "")
    cas_root = record.get("cas_root")
    checkout = record.get("checkout_root")
    if not cas_root or not key:
        return {"sha256": None, "reason": "the record names no CAS root"}
    if not checkout:
        return {"sha256": None,
                "reason": "a checkout_snapshot submission materialises its "
                          "checkout per box, and the resolved path the claim "
                          "binds is not on the record. The worker prints the "
                          "digest in its own result at the end of the "
                          "attempt's stdout, which pb_log returns."}
    request = Path(str(cas_root)) / "requests" / key[:2] / f"{key}.json"
    action = _read_record(request)
    if action is None:
        return {"sha256": None, "request": str(request),
                "reason": "the action manifest is not in the CAS"}
    task = action.get("task")
    if not isinstance(task, Mapping):
        return {"sha256": None, "request": str(request),
                "reason": "the action manifest declares no task"}
    body = {
        "schema": pb.LOCAL_RESULT_CLAIM_SCHEMA_V1,
        "action_key": action.get("action_key"),
        "action_manifest_sha256": pb.canonical_sha256(action),
        "checkout_root": str(checkout),
        "working_directory": task.get("working_directory"),
        "result_path": task.get("result_path"),
    }
    digest = pb.canonical_sha256(body)
    path = Path(str(cas_root)) / "local-results" / "v1" / digest[:2] / f"{digest}.json"
    # ``os.stat`` rather than ``path.is_file()``: a pathlib predicate swallows
    # OSError on Python 3.14, so a stale handle would read as "absent".
    try:
        os.stat(path)
        present = True
    except OSError as error:
        if not _absent(error):
            raise
        present = False
    return {"sha256": digest, "path": str(path), "present": present,
            "request": str(request), "derived_from": body}


def _receipt_summary(record: Mapping[str, object]) -> dict:
    """Whether the CAS holds a receipt for this action, and what it names."""

    key = str(record.get("action_key") or "")
    cas_root = record.get("cas_root")
    if not cas_root or not key:
        return {"present": None, "reason": "the record names no CAS root"}
    path = (Path(str(cas_root)) / "actions"
            / pb.CAS_RECEIPT_SCHEMA_V3.rsplit(".", 1)[-1] / key[:2] / f"{key}.json")
    receipt = _read_record(path)
    if receipt is None:
        return {"present": False, "path": str(path)}
    result = receipt.get("result")
    result = result if isinstance(result, Mapping) else {}
    return {
        "present": True,
        "path": str(path),
        "action_manifest_sha256": receipt.get("action_manifest_sha256"),
        "receipt_sha256": receipt.get("receipt_sha256"),
        "result": {"sha256": result.get("sha256"), "bytes": result.get("bytes")},
    }


def _log_tail(path: Path, *, tail_lines: int, max_bytes: int) -> dict:
    """The last lines of one log, without reading the log.

    ``lseek`` to ``size - max_bytes`` and one ``read``: the file is opened
    ``O_RDONLY|O_NOFOLLOW|O_CLOEXEC`` and the process never holds more than
    ``max_bytes`` of it.  ``truncated_head`` says the front was skipped, so a
    reader is never shown a partial log as if it were whole.
    """

    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError as exc:
        return {"path": str(path), "present": False, "error": str(exc)}
    start = 0
    try:
        total = os.fstat(descriptor).st_size
        start = max(0, total - max(0, int(max_bytes)))
        if start:
            os.lseek(descriptor, start, os.SEEK_SET)
        raw = os.read(descriptor, max(0, int(max_bytes)))
    finally:
        os.close(descriptor)
    text = raw.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if start and lines:
        # The first line of a mid-file read is half a line.
        lines = lines[1:]
    kept = lines[-max(0, int(tail_lines)):] if tail_lines else []
    return {
        "path": str(path),
        "present": True,
        "total_bytes": total,
        "bytes_read": len(raw),
        "truncated_head": bool(start),
        "lines": kept,
        "lines_returned": len(kept),
    }


def _reservations(queue_root: Path) -> dict:
    """Token files present per host, counted by kind.

    A count of files, named as a count of files.  Which tokens a live loop is
    actually using is decided by the loops themselves through the ledger, and
    this does not open a holder or take anything: ``held`` is one directory
    per holder, so the holders are counted and their tokens are counted, and
    neither number is presented as an admission decision.
    """

    root = Path(queue_root) / pool.RESERVATIONS
    hosts: dict[str, dict] = {}
    try:
        with os.scandir(root) as entries:
            names = sorted(entry.name for entry in entries if entry.is_dir())
    except OSError as error:
        if _absent(error):
            return hosts
        raise
    for host in names:
        free: dict[str, int] = {}
        for name in _names_in(root / host / "free"):
            free[name.rsplit("-", 1)[0]] = free.get(name.rsplit("-", 1)[0], 0) + 1
        holders = _names_in(root / host / "held")
        held: dict[str, int] = {}
        for holder in holders:
            for name in _names_in(root / host / "held" / holder):
                held[name.rsplit("-", 1)[0]] = held.get(name.rsplit("-", 1)[0], 0) + 1
        hosts[host] = {"free_tokens": free, "held_tokens": held,
                       "holders": len(holders)}
    return hosts


def _names_in(directory: Path) -> list[str]:
    try:
        with os.scandir(directory) as entries:
            return sorted(entry.name for entry in entries)
    except OSError as error:
        if _absent(error):
            return []
        raise


# --------------------------------------------------------------------------
# The tools
# --------------------------------------------------------------------------

class Session:
    """One client's view of one queue, on the generation it started from."""

    def __init__(self, *, queue_root: Path, cas_root: Path, repo_link: Path,
                 deadline_s: float = DEFAULT_DEADLINE_S,
                 recent: int = DEFAULT_RECENT,
                 log_tail_bytes: int = DEFAULT_LOG_TAIL_BYTES) -> None:
        self.queue_root = Path(queue_root)
        self.cas_root = Path(cas_root)
        self.repo_link = Path(repo_link)
        self.deadline_s = float(deadline_s)
        self.recent = int(recent)
        self.log_tail_bytes = int(log_tail_bytes)
        self._abandoned_readers: list[dict] = []
        self._startup_read = Call("startup", deadline_s=self.deadline_s,
                                  startup_generation=None,
                                  repo_link=self.repo_link,
                                  abandoned=self._abandoned_readers)
        self.startup_generation = self._startup_read.read(
            "startup-repo-link", lambda: _link_target(self.repo_link))

    # -- dispatch ----------------------------------------------------------

    def call(self, name: str, arguments: Mapping[str, object] | None = None) -> dict:
        arguments = dict(arguments or {})
        method = getattr(self, name, None)
        if name not in TOOL_NAMES or method is None:
            raise ToolError(f"no such tool: {name}", tools=list(TOOL_NAMES))
        schema = next(tool["inputSchema"] for tool in TOOLS if tool["name"] == name)
        _validate_arguments(arguments, schema)
        call = Call(name, deadline_s=self.deadline_s,
                    startup_generation=self.startup_generation,
                    repo_link=self.repo_link,
                    abandoned=self._abandoned_readers)
        # A later successful read cannot reconstruct the startup observation.
        # Retain its diagnostics, including ownership of any abandoned reader.
        call.timed_out.extend(self._startup_read.timed_out)
        call.unavailable.extend(self._startup_read.unavailable)
        try:
            payload = method(call, **arguments)
        except TypeError as exc:
            raise ToolError(f"{name}: {exc}") from exc
        return call.envelope(payload)

    # -- pb_status ---------------------------------------------------------

    def pb_status(self, call: Call, *, recent: int | None = None) -> dict:
        limit = self.recent if recent is None else int(recent)
        census = call.read("pool", lambda: pbstatus.read_pool(self.queue_root))
        endings = call.read("endings",
                            lambda: pbstatus.read_endings(self.queue_root, limit=limit))
        reservations = call.read("reservations",
                                 lambda: _reservations(self.queue_root))
        census = census or {}
        return {
            "queue_root": str(self.queue_root),
            "nodes": census.get("nodes"),
            "jobs": census.get("jobs"),
            "queue": census.get("queue"),
            "notes": census.get("notes"),
            "endings": endings,
            "endings_limit": limit,
            "reservations": reservations,
        }

    # -- pb_action ---------------------------------------------------------

    def pb_action(self, call: Call, *, key_prefix: str,
                  tail_lines: int = DEFAULT_TAIL_LINES) -> dict:
        key = self._one_key(call, key_prefix)
        records = call.read("records",
                            lambda: _records_for(self.queue_root, key))
        state = next((one for one in STATES if one in (records or {})), None)
        record = (records or {}).get(state) if state else None
        payload: dict[str, object] = {
            "key_prefix": str(key_prefix),
            "action_key": key,
            "state": state,
            "states": None if records is None else sorted(records),
        }
        if record is None:
            # ``null`` rather than ``False`` when the read never answered: an
            # unread queue has not told anyone this key is absent.
            payload["found"] = None if records is None else False
            return payload
        detail = record.get("detail")
        detail = detail if isinstance(detail, Mapping) else {}
        attempts = call.read("attempts",
                             lambda: _attempt_records(self.queue_root, record))
        adopted = attempts[-1] if attempts else None
        payload.update(
            found=True,
            sealed=_sealed(record),
            outcome={
                "status": record.get("status"),
                "claimed_host": record.get("claimed_host"),
                "claimed_by": record.get("claimed_by"),
                "claimed_unix": record.get("claimed_unix"),
                "finished_host": record.get("finished_host"),
                "finished_unix": record.get("finished_unix"),
                "attempts": record.get("attempts"),
                "elapsed_s": detail.get("elapsed_s"),
                "returncode": detail.get("returncode"),
                "action_returncode": detail.get("action_returncode"),
                "action_signal": detail.get("action_signal"),
                "receipt_published": detail.get("receipt_published"),
                "reason": record.get("reason"),
            },
            # #372 files a per-action resource profile on the outcome.  Read
            # optionally from both places it can appear, and passed through
            # rather than reshaped, so this reports whatever that branch lands
            # without this file having to name its fields.
            resource_profile=(record.get("resource_profile")
                              or detail.get("resource_profile")),
            # The attempt records verbatim.  They hold log *metadata* -- path,
            # length, digest -- and never a log body, so passing them through
            # keeps whatever a later branch files on them without this file
            # having to know the field names.
            attempts_detail=attempts,
            adopted_attempt=None if adopted is None else adopted.get("attempt"),
            preemption={
                "preempted_by": record.get("preempted_by"),
                "requeued_as": call.read(
                    "preemption",
                    lambda: _requeued_as(self.queue_root, key, record)),
                "unlocked_read": True,
            },
            receipt=call.read("receipt", lambda: _receipt_summary(record)),
            local_result_claim=call.read("claim", lambda: _derived_claim(record)),
        )
        if adopted is not None:
            payload["log_tail"] = call.read(
                "log",
                lambda: self._adopted_log(adopted, tail_lines=tail_lines))
        return payload

    def _adopted_log(self, attempt: Mapping[str, object], *, stream: str = "stdout",
                     tail_lines: int = DEFAULT_TAIL_LINES) -> dict:
        logs = attempt.get("logs")
        logs = logs if isinstance(logs, Mapping) else {}
        metadata = logs.get(stream)
        if not isinstance(metadata, Mapping) or not metadata.get("path"):
            return {"stream": stream, "present": False,
                    "reason": "the attempt records no log for this stream"}
        path = self.queue_root / str(metadata["path"])
        tail = _log_tail(path, tail_lines=tail_lines, max_bytes=self.log_tail_bytes)
        recorded = metadata.get("bytes")
        return {
            "stream": stream,
            "attempt": attempt.get("attempt"),
            "recorded_bytes": recorded,
            "recorded_sha256": metadata.get("sha256"),
            # Cheap honesty: the recorded length against the length on disk,
            # which catches a truncated or replaced log without hashing it.
            "bytes_match": (None if tail.get("total_bytes") is None
                            else recorded == tail.get("total_bytes")),
            **tail,
        }

    # -- pb_actions --------------------------------------------------------

    def pb_actions(self, call: Call, *, states: Sequence[str] | None = None,
                   tags: Sequence[str] | None = None,
                   priority_min: float | None = None,
                   priority_max: float | None = None,
                   checkout_root: str | None = None,
                   published_by: str | None = None,
                   max_age_s: float | None = None,
                   keys: Sequence[str] | None = None,
                   limit: int = DEFAULT_ACTIONS_LIMIT) -> dict:
        """List actions, which is how an agent asks for its own.

        There is no submitter identity on a queue record.  ``publish`` seals
        ``published_by`` (the hostname that submitted) and either
        ``checkout_root`` or ``checkout_snapshot``, and nothing that names the
        agent.  So "my jobs" is answered by the checkout an agent submitted
        from, the box it submitted on, or the keys it already holds -- and
        this says so rather than pretending to a filter it cannot honour.

        The window is bounded before anything is filtered.  Terminal records
        number in the thousands on the live queue, and filtering by a field
        inside the record means reading it, so the newest ``limit`` by
        modification time are selected by ``stat`` first.  ``scanned`` and
        ``truncated`` are returned for that reason: a key that is not in the
        answer may be a key that is not yours, or a key older than the window,
        and a caller must be able to tell those apart.
        """

        wanted_states = [str(one) for one in (states or STATES)]
        unknown = [one for one in wanted_states if one not in STATES]
        if unknown:
            raise ToolError(f"unknown state(s): {', '.join(unknown)}",
                            states=list(STATES))
        limit = max(0, int(limit))
        selected = [_valid_prefix(one) for one in (keys or ())]
        scan = call.read(
            "actions",
            lambda: _scan_actions(self.queue_root, states=wanted_states,
                                  keys=selected, limit=limit))
        returned = None
        if scan is not None:
            now = time.time()
            kept = [row for row in (scan.get("rows") or [])
                    if _matches(row, tags=tags, priority_min=priority_min,
                                priority_max=priority_max,
                                checkout_root=checkout_root,
                                published_by=published_by, max_age_s=max_age_s,
                                now=now)]
            kept.sort(key=lambda row: row.get("published_unix") or 0.0, reverse=True)
            returned = kept[:limit] if limit else kept
        scan = scan or {}
        return {
            "queue_root": str(self.queue_root),
            "actions": returned,
            "scanned": scan.get("scanned"),
            "returned": None if returned is None else len(returned),
            "limit": limit,
            "truncated": scan.get("truncated"),
            "filter": {
                "states": wanted_states, "tags": list(tags or []),
                "priority_min": priority_min, "priority_max": priority_max,
                "checkout_root": checkout_root, "published_by": published_by,
                "max_age_s": max_age_s, "keys": selected,
            },
            "identity": {
                "submitter_field": None,
                "note": "a queue record carries no submitter identity: publish "
                        "seals published_by (the submitting host) and either "
                        "checkout_root or checkout_snapshot, and nothing that "
                        "names an agent. Filter by checkout_root, "
                        "published_by, or the keys you already hold.",
            },
        }

    # -- pb_verify_claim ---------------------------------------------------

    def pb_verify_claim(self, call: Call, *, sha256: str,
                        cas_root: str | None = None,
                        hash_payload: bool = False) -> dict:
        """Resolve a local-result claim to its receipt and payload.

        Each check is reported separately and by name, because "verifies" is
        not one fact.  The claim's own digest is recomputed with the
        producer's ``canonical_sha256`` over the producer's own body keys; the
        receipt is checked for self-consistency the same way and for naming
        the same action manifest the claim does; the payload is checked for
        presence and length.  Its content digest is checked only when the
        caller asks, because a result blob is a rendered model often enough
        that hashing one by default would make this tool a way to spend an
        hour of NFS bandwidth by accident.

        What is *not* checked is said as plainly: the worker attestation
        inside the receipt binds to the full action manifest, and validating
        it is ``core.PrismaBuildCAS.lookup``'s job, which needs that manifest.
        That is why the verdict field is ``checks_passed`` and not
        ``verified``: every check this tool ran passed is a smaller claim than
        the claim is verified, and ``attestation_verified`` stays ``null`` to
        say which check is still owed.  A reader that wants the whole verdict
        goes to the verifier that holds the manifest.
        """

        digest = str(sha256).strip().lower()
        if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
            raise ToolError("sha256 must be a full hex SHA-256 digest")
        root = Path(cas_root) if cas_root else self.cas_root
        path = root / "local-results" / "v1" / digest[:2] / f"{digest}.json"
        checks: dict[str, object] = {
            "claim_present": None, "claim_addressed_correctly": None,
            "claim_digest_matches_body": None, "receipt_present": None,
            "receipt_self_consistent": None,
            "receipt_binds_the_claims_manifest": None,
            "payload_present": None, "payload_bytes_match": None,
            "payload_sha256_match": None,
        }
        not_checked = ["worker attestation (needs the full action manifest; "
                       "core.PrismaBuildCAS.lookup is the verifier)"]
        claim = call.read("claim", lambda: _read_record(path))
        checks["claim_present"] = claim is not None
        payload: dict[str, object] = {
            "sha256": digest, "cas_root": str(root), "claim_path": str(path),
            "claim": claim, "checks": checks, "not_checked": not_checked,
        }
        if claim is None:
            payload["checks_passed"] = False
            payload["attestation_verified"] = None
            return payload
        body = {key: claim.get(key) for key in pb._LOCAL_RESULT_CLAIM_BODY_KEYS}
        recomputed = pb.canonical_sha256(body)
        checks["claim_digest_matches_body"] = claim.get("claim_sha256") == recomputed
        checks["claim_addressed_correctly"] = recomputed == digest
        key = str(claim.get("action_key") or "")
        receipt_path = (root / "actions" / pb.CAS_RECEIPT_SCHEMA_V3.rsplit(".", 1)[-1]
                        / key[:2] / f"{key}.json")
        payload["receipt_path"] = str(receipt_path)
        receipt = call.read("receipt", lambda: _read_record(receipt_path))
        checks["receipt_present"] = receipt is not None
        if receipt is not None:
            receipt_body = {name: receipt.get(name) for name in pb._RECEIPT_BODY_KEYS}
            checks["receipt_self_consistent"] = (
                set(receipt) == set(pb._RECEIPT_KEYS)
                and receipt.get("receipt_sha256") == pb.canonical_sha256(receipt_body))
            checks["receipt_binds_the_claims_manifest"] = (
                receipt.get("action_manifest_sha256")
                == claim.get("action_manifest_sha256")
                and receipt.get("action_key") == claim.get("action_key"))
            result = receipt.get("result")
            result = result if isinstance(result, Mapping) else {}
            blob_digest = str(result.get("sha256") or "")
            if len(blob_digest) == 64:
                blob = root / "blobs" / blob_digest[:2] / blob_digest
                payload["payload_path"] = str(blob)
                observed = call.read("payload", lambda: _blob_identity(
                    blob, hash_payload=bool(hash_payload)))
                if isinstance(observed, dict):
                    checks["payload_present"] = observed.get("present")
                    if observed.get("present"):
                        checks["payload_bytes_match"] = (
                            observed.get("bytes") == result.get("bytes"))
                        if hash_payload:
                            checks["payload_sha256_match"] = (
                                observed.get("sha256") == blob_digest)
                    payload["payload"] = observed
        payload["checks_passed"] = all(
            value is True for name, value in checks.items()
            if not (name == "payload_sha256_match" and not hash_payload))
        payload["attestation_verified"] = None
        return payload

    # -- pb_log ------------------------------------------------------------

    def pb_log(self, call: Call, *, key_prefix: str,
               tail_lines: int = DEFAULT_TAIL_LINES,
               stream: str = "stdout", attempt: int | None = None) -> dict:
        if stream not in ("stdout", "stderr"):
            raise ToolError(f"unknown stream: {stream!r}",
                            streams=["stdout", "stderr"])
        key = self._one_key(call, key_prefix)
        records = call.read("records",
                            lambda: _records_for(self.queue_root, key))
        state = next((one for one in STATES if one in (records or {})), None)
        record = (records or {}).get(state) if state else None
        if record is None:
            return {"key_prefix": str(key_prefix), "action_key": key,
                    "found": None if records is None else False, "log": None}
        attempts = call.read("attempts",
                             lambda: _attempt_records(self.queue_root, record))
        if not attempts:
            return {
                "key_prefix": str(key_prefix), "action_key": key, "found": True,
                "state": state, "log": None,
                "reason": ("the attempt records did not answer within the "
                           "deadline" if attempts is None else
                           "an attempt publishes its log when it finishes; this "
                           "action has no published attempt yet"),
            }
        chosen = (attempts[-1] if attempt is None else
                  next((one for one in attempts if one.get("attempt") == attempt), None))
        if chosen is None:
            raise ToolError(f"no attempt {attempt} on {key[:12]}",
                            attempts=[one.get("attempt") for one in attempts])
        return {
            "key_prefix": str(key_prefix), "action_key": key, "found": True,
            "state": state,
            "log": call.read("log", lambda: self._adopted_log(
                chosen, stream=stream, tail_lines=int(tail_lines))),
        }

    # -- pb_runtime --------------------------------------------------------

    def pb_runtime(self, call: Call) -> dict:
        """The published generation, its manifest, and who is running on it."""

        manifest = call.read(
            "manifest",
            lambda: _read_record(self.repo_link / "RUNTIME_VERSION.json"))
        census = call.read("pool", lambda: pbstatus.read_pool(self.queue_root))
        nodes = (census or {}).get("nodes") or []
        return {
            "repo_link": str(self.repo_link),
            "server_generation": RUNTIME_ROOT.name,
            "server_entrypoint": str(Path(__file__).resolve()),
            "manifest": None if manifest is None else {
                "schema": manifest.get("schema"),
                "commit": manifest.get("commit"),
                "dirty": manifest.get("dirty"),
                "generation": manifest.get("generation"),
                "published_unix": manifest.get("published_unix"),
                "published_by": manifest.get("published_by"),
                "files": len(manifest.get("files") or {}),
            },
            "loops": [
                {"node": node.get("node"), "state": node.get("state"),
                 "runtime_commit": node.get("runtime_commit"),
                 "loops": node.get("loops"), "age_s": node.get("age_s")}
                for node in nodes
            ],
        }

    # -- helpers -----------------------------------------------------------

    def _one_key(self, call: Call, key_prefix: str) -> str:
        prefix = _valid_prefix(key_prefix)
        matches = call.read("keys",
                            lambda: _resolve_prefix(self.queue_root, prefix))
        if matches is None:
            raise ToolError(
                f"the queue did not answer within {call.deadline_s}s",
                timed_out=list(call.timed_out), unavailable=list(call.unavailable))
        if not matches:
            raise ToolError(f"no action starts with {key_prefix!r}")
        if len(matches) > 1:
            raise ToolError(f"{key_prefix!r} names {len(matches)} actions",
                            candidates=matches[:20])
        return matches[0]


def _blob_identity(path: Path, *, hash_payload: bool) -> dict:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError as exc:
        return {"present": False, "error": str(exc)}
    try:
        size = os.fstat(descriptor).st_size
        digest = None
        if hash_payload:
            hasher = hashlib.sha256()
            while True:
                chunk = os.read(descriptor, 1 << 20)
                if not chunk:
                    break
                hasher.update(chunk)
            digest = hasher.hexdigest()
    finally:
        os.close(descriptor)
    return {"present": True, "bytes": size, "sha256": digest}


def _scan_actions(queue_root: Path, *, states: Sequence[str],
                  keys: Sequence[str], limit: int) -> dict:
    """Rows for a bounded window of the queue, read before anything is filtered.

    The window is chosen by ``stat`` and only then read.  Terminal records
    number in the thousands on the live queue and every filter this offers
    lives *inside* a record, so filtering first would mean reading all of
    them and the deadline would expire on every call.  ``_ending_paths`` is
    ``pbstatus``'s own newest-first selection, reused rather than repeated.

    ``truncated`` is returned with the rows because a caller has to be able
    to tell "no such action of yours" from "older than the window".
    """

    root = Path(queue_root)
    now = time.time()
    rows: list[dict] = []
    scanned = 0
    truncated = False
    if keys:
        for prefix in keys:
            for key in _resolve_prefix(root, prefix):
                for state, record in _records_for(root, key).items():
                    if state in states:
                        scanned += 1
                        rows.append(_row(state, record, now))
        return {"rows": rows, "scanned": scanned, "truncated": False}
    for state in (pool.READY, pool.CLAIMED):
        if state not in states:
            continue
        for key in _keys_in(root / state):
            scanned += 1
            record = _read_record(root / state / f"{key}.json")
            if record is not None:
                rows.append(_row(state, record, now))
    terminal = [state for state in states
                if state in (pool.DONE, pool.FAILED, pool.WITHDRAWN)]
    if terminal and limit:
        entries = pbstatus._ending_paths(root, limit)
        truncated = len(entries) >= limit
        for entry in entries:
            path = Path(entry.path)
            if path.parent.name not in terminal:
                continue
            scanned += 1
            record = _read_record(path)
            if record is not None:
                rows.append(_row(path.parent.name, record, now))
    return {"rows": rows, "scanned": scanned, "truncated": truncated}


def _row(state: str, record: Mapping[str, object], now: float) -> dict:
    published = record.get("published_unix")
    key = str(record.get("action_key") or "")
    return {
        "action_key": key,
        "action_key_prefix": key[:12],
        "state": state,
        "status": record.get("status"),
        "tags": record.get("tags"),
        "priority": record.get("priority"),
        "resources": record.get("resources"),
        "attempts": record.get("attempts"),
        "checkout_root": record.get("checkout_root"),
        "checkout_snapshot": record.get("checkout_snapshot"),
        "published_by": record.get("published_by"),
        "published_unix": published,
        "age_s": (now - float(published)
                  if type(published) in (int, float) else None),
        "claimed_host": record.get("claimed_host"),
        "finished_unix": record.get("finished_unix"),
        "preempted_by": record.get("preempted_by"),
    }


def _matches(row: Mapping[str, object], *, tags, priority_min, priority_max,
             checkout_root, published_by, max_age_s, now: float) -> bool:
    if tags:
        have = {str(one) for one in (row.get("tags") or [])}
        if not have.issuperset({str(one) for one in tags}):
            return False
    priority = row.get("priority")
    if priority_min is not None:
        if type(priority) not in (int, float) or priority < float(priority_min):
            return False
    if priority_max is not None:
        if type(priority) not in (int, float) or priority > float(priority_max):
            return False
    if checkout_root is not None and str(row.get("checkout_root") or "") != str(checkout_root):
        return False
    if published_by is not None and str(row.get("published_by") or "") != str(published_by):
        return False
    if max_age_s is not None:
        published = row.get("published_unix")
        if type(published) not in (int, float):
            return False
        if now - float(published) > float(max_age_s):
            return False
    return True


# --------------------------------------------------------------------------
# The MCP surface
# --------------------------------------------------------------------------

_KEY_PREFIX = {"type": "string",
               "description": "An action key or any unique prefix of one, "
                              "such as the twelve characters pbstatus prints."}

TOOLS: tuple[dict, ...] = (
    {
        "name": "pb_status",
        "description": "The PrismaBuild fleet census: worker offers and their "
                       "ages, ready and claimed actions with why each is "
                       "waiting, per-host reservation tokens, and how the most "
                       "recent actions ended. Read-only and deadline-bounded; "
                       "check `complete` and `timed_out` before trusting a "
                       "quiet queue.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "recent": {"type": "integer", "minimum": 0,
                           "description": "How many terminal records to read, "
                                          "newest first."},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "pb_action",
        "description": "Everything the queue holds about one action: the "
                       "sealed submission (tags, demand, priority, checkout), "
                       "its state and host, every attempt with its log "
                       "metadata, the ending and its return codes, the CAS "
                       "receipt, the derived local-result claim, and a tail of "
                       "the last attempt's stdout.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "key_prefix": _KEY_PREFIX,
                "tail_lines": {"type": "integer", "minimum": 0,
                               "description": "Lines of the adopted attempt's "
                                              "stdout to return."},
            },
            "required": ["key_prefix"],
            "additionalProperties": False,
        },
    },
    {
        "name": "pb_actions",
        "description": "List actions by state, tag, priority band, checkout, "
                       "submitting host or age -- this is how an agent asks "
                       "for its own jobs. A queue record carries no submitter "
                       "identity, so filter by `checkout_root`, "
                       "`published_by`, or the keys you already hold.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "states": {"type": "array", "items": {"type": "string"},
                           "description": "Any of ready, claimed, done, "
                                          "failed, withdrawn."},
                "tags": {"type": "array", "items": {"type": "string"},
                         "description": "Keep actions carrying all of these "
                                        "placement tags."},
                "priority_min": {"type": "number",
                                 "description": "Lowest priority to keep."},
                "priority_max": {"type": "number",
                                 "description": "Highest priority to keep."},
                "checkout_root": {"type": "string",
                                  "description": "Exact checkout the action "
                                                 "was submitted from."},
                "published_by": {"type": "string",
                                 "description": "Hostname that submitted it."},
                "max_age_s": {"type": "number",
                              "description": "Keep actions published within "
                                             "this many seconds."},
                "keys": {"type": "array", "items": {"type": "string"},
                         "description": "Explicit action keys or prefixes; "
                                        "bypasses the newest-N window."},
                "limit": {"type": "integer", "minimum": 0,
                          "description": "Maximum rows, and the size of the "
                                         "newest-first window scanned."},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "pb_verify_claim",
        "description": "Resolve a local-result claim digest to its CAS receipt "
                       "and payload and report each check by name: claim "
                       "address and digest, receipt self-consistency and "
                       "binding, payload presence and length. Pass "
                       "hash_payload to also read and hash the blob. "
                       "checks_passed covers only the checks this tool ran; "
                       "attestation_verified is null because validating the "
                       "worker attestation needs the full action manifest.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sha256": {"type": "string",
                           "description": "The local_result_claim_sha256 a run "
                                          "reported."},
                "cas_root": {"type": "string",
                             "description": "CAS root to resolve against; "
                                            "defaults to the fleet's."},
                "hash_payload": {"type": "boolean",
                                 "description": "Read the payload blob and "
                                                "check its content digest."},
            },
            "required": ["sha256"],
            "additionalProperties": False,
        },
    },
    {
        "name": "pb_log",
        "description": "A bounded tail of one attempt's stdout or stderr. The "
                       "log is never read whole: the reader seeks to the end "
                       "and reads a capped number of bytes.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "key_prefix": _KEY_PREFIX,
                "tail_lines": {"type": "integer", "minimum": 0,
                               "description": "How many trailing lines to "
                                              "return."},
                "stream": {"type": "string", "enum": ["stdout", "stderr"],
                           "description": "Which stream to read."},
                "attempt": {"type": "integer", "minimum": 1,
                            "description": "Attempt number; the last attempt "
                                           "by default."},
            },
            "required": ["key_prefix"],
            "additionalProperties": False,
        },
    },
    {
        "name": "pb_runtime",
        "description": "The published runtime generation the fleet is on, its "
                       "manifest, which loops are announcing on it, and "
                       "whether this server was started from it.",
        "inputSchema": {"type": "object", "properties": {},
                        "additionalProperties": False},
    },
)

TOOL_NAMES = tuple(tool["name"] for tool in TOOLS)


class Server:
    """The MCP subset this needs, spoken over newline-delimited JSON-RPC.

    Stdlib only, and that is the point rather than an economy: the server is
    launched as ``/usr/bin/python3
    /mnt/shared/prismabuild-fleet/repo/tools/fleet/pbmcp.py`` from whichever
    box an agent is on, and every box has that interpreter and none of them
    reliably has the ``mcp`` SDK.  Adding the dependency would mean naming a
    venv per box, and a venv is the thing a published generation exists to
    stop mattering.

    The subset is ``initialize``, ``notifications/*``, ``ping``,
    ``tools/list``, ``tools/call``, ``resources/list`` and ``prompts/list``.
    The last two are answered empty because clients probe for them during the
    handshake and an error to a probe reads as a broken server.
    """

    def __init__(self, session: Session, *, stdin=None, stdout=None) -> None:
        self.session = session
        self.stdin = stdin if stdin is not None else sys.stdin.buffer
        self.stdout = stdout if stdout is not None else sys.stdout.buffer

    def serve(self) -> int:
        for line in self.stdin:
            text = line.strip()
            if not text:
                continue
            try:
                request = json.loads(text)
            except ValueError as exc:
                self._send({"jsonrpc": "2.0", "id": None,
                            "error": {"code": PARSE_ERROR,
                                      "message": f"invalid JSON: {exc}"}})
                continue
            if not isinstance(request, dict):
                self._send({"jsonrpc": "2.0", "id": None,
                            "error": {"code": INVALID_REQUEST,
                                      "message": "request must be an object"}})
                continue
            response = self.handle(request)
            if response is not None:
                self._send(response)
        return 0

    def _send(self, message: Mapping[str, object]) -> None:
        self.stdout.write(json.dumps(message).encode("utf-8") + b"\n")
        self.stdout.flush()

    def handle(self, request: Mapping[str, object]) -> dict | None:
        method = request.get("method")
        identifier = request.get("id")
        # A notification carries no id and takes no response, not even an
        # error one: answering ``notifications/initialized`` is enough to
        # break a client's handshake.
        if identifier is None:
            return None
        params = request.get("params")
        params = params if isinstance(params, Mapping) else {}
        try:
            if method == "initialize":
                return self._result(identifier, self._initialize(params))
            if method == "ping":
                return self._result(identifier, {})
            if method == "tools/list":
                return self._result(identifier, {"tools": [dict(t) for t in TOOLS]})
            if method == "resources/list":
                return self._result(identifier, {"resources": []})
            if method == "resources/templates/list":
                return self._result(identifier, {"resourceTemplates": []})
            if method == "prompts/list":
                return self._result(identifier, {"prompts": []})
            if method == "tools/call":
                return self._call(identifier, params)
        except Exception as exc:                   # noqa: BLE001 - diagnostic
            return {"jsonrpc": "2.0", "id": identifier,
                    "error": {"code": INTERNAL_ERROR, "message": str(exc)}}
        return {"jsonrpc": "2.0", "id": identifier,
                "error": {"code": METHOD_NOT_FOUND,
                          "message": f"unknown method: {method!r}"}}

    def _initialize(self, params: Mapping[str, object]) -> dict:
        asked = str(params.get("protocolVersion") or "")
        version = asked if asked in PROTOCOL_VERSIONS else DEFAULT_PROTOCOL_VERSION
        return {
            "protocolVersion": version,
            "capabilities": {"tools": {}, "resources": {}, "prompts": {}},
            "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            "instructions": (
                "Read-only view of the PrismaBuild pull queue. Use pb_actions "
                "to find your own submissions (filter by checkout_root or "
                "published_by; the queue records no submitter identity), "
                "pb_action and pb_log for one of them, and pb_status for the "
                "fleet. Every response carries complete/timed_out: a quiet "
                "queue and an unreachable mount look alike unless you read "
                "them. Submission is deliberately absent -- use pbrun."),
        }

    def _call(self, identifier: object, params: Mapping[str, object]) -> dict:
        name = params.get("name")
        arguments = params.get("arguments")
        if not isinstance(name, str):
            return {"jsonrpc": "2.0", "id": identifier,
                    "error": {"code": INVALID_PARAMS,
                              "message": "tools/call requires a tool name"}}
        if arguments is not None and not isinstance(arguments, Mapping):
            return {"jsonrpc": "2.0", "id": identifier,
                    "error": {"code": INVALID_PARAMS,
                              "message": "arguments must be an object"}}
        try:
            payload = self.session.call(name, arguments)
        except InvalidArguments as exc:
            return {"jsonrpc": "2.0", "id": identifier,
                    "error": {"code": INVALID_PARAMS, "message": str(exc)}}
        except ToolError as exc:
            return self._result(identifier, _content(
                {"error": str(exc), **exc.detail}), is_error=True)
        except Exception as exc:                   # noqa: BLE001 - diagnostic
            return self._result(identifier, _content(
                {"error": f"{type(exc).__name__}: {exc}"}), is_error=True)
        return self._result(identifier, _content(payload))

    @staticmethod
    def _result(identifier: object, result: Mapping[str, object],
                *, is_error: bool = False) -> dict:
        body = dict(result)
        if is_error:
            body["isError"] = True
        return {"jsonrpc": "2.0", "id": identifier, "result": body}


def _content(payload: Mapping[str, object]) -> dict:
    return {"content": [{"type": "text",
                         "text": json.dumps(payload, indent=1, default=str,
                                            sort_keys=True)}]}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Serve the PrismaBuild queue to an MCP client, read-only.")
    parser.add_argument(
        "--queue-root", default=None,
        help="The pull queue to read. Defaults to the fleet's shared queue.")
    parser.add_argument(
        "--cas-root", default=None,
        help="The content-addressed store pb_verify_claim resolves against. "
             "Defaults to the fleet's shared CAS.")
    parser.add_argument(
        "--repo-link", default=None,
        help="The published-runtime symlink whose target names the current "
             "generation. Defaults to the fleet's repo link.")
    parser.add_argument(
        "--deadline-s", type=float, default=DEFAULT_DEADLINE_S,
        help="How long one tool call may spend reading the shared mount "
             "before it answers with what it has (0 disables the bound).")
    parser.add_argument(
        "--recent", type=int, default=DEFAULT_RECENT,
        help="How many terminal records pb_status reads by default.")
    parser.add_argument(
        "--log-tail-bytes", type=int, default=DEFAULT_LOG_TAIL_BYTES,
        help="The most bytes read from the tail of any one log.")
    return parser


def session_from(args: argparse.Namespace) -> Session:
    """Build the session, reading the live roots when the caller named none.

    The defaults are read here rather than bound into the parser, because a
    default evaluated at definition time cannot be repointed and the suite
    repoints ``pbstatus``'s roots to keep tests off the live store.
    """

    shared = Path(pbstatus.SHARED_ROOT)
    return Session(
        queue_root=Path(args.queue_root) if args.queue_root
        else Path(pbstatus.DEFAULT_QUEUE_ROOT),
        cas_root=Path(args.cas_root) if args.cas_root else shared / "cas",
        repo_link=Path(args.repo_link) if args.repo_link else shared / "repo",
        deadline_s=args.deadline_s,
        recent=args.recent,
        log_tail_bytes=args.log_tail_bytes,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return Server(session_from(args)).serve()


if __name__ == "__main__":
    raise SystemExit(main())

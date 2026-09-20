"""Submit a list of actions to the fleet with one command, and wait for all.

A campaign is N independent commands, each of which should be a memoized
action: run it once, and a later run of the same manifest costs nothing.  Doing
that by hand meant N ``pbrun`` invocations held open in N shells, so this is
the one command that submits them all and the one table that reports them.

Rows are independent.  There is no DAG here and there is not meant to be: what
the fleet needs is fan-out, and a dependency is expressible as a later
manifest.

**Every row goes through ``pbrun``'s own seal path.**  ``pbrun`` is the only
sealer of shell-command actions, so this builds the command line ``pbrun``
would have been typed and calls ``pbrun.main`` with it.  The action key a row
produces is therefore the key a hand-typed ``pbrun`` produces for the same row,
byte for byte -- re-submitting is a CAS hit that runs nothing, and a row can be
reproduced at the terminal from the manifest alone.  (In process rather than as
a subprocess: it is the same ``main`` with the same argv either way, and the
in-process form does not pay N interpreter starts.)

Nothing here knows what the commands are for.  A row is argv, a working tree,
a demand and some tags; the tool that reads it must stay usable by any producer
and on any worker the fleet grows, so it names no project, no partition and no
host.

Runtime publication changes the Docker wrapper path sealed by an ordinary
row. For receipt collection across a publication, retain the first row keys
and add ``as_sealed_by`` to each row. The current submitter uses that key's
retained wrapper and refuses unless the complete current seal matches; a
receipted row is then a cache hit, including a manifest spanning generations.
This is not a receipts-only mode: a missing receipt can submit the same key.

Manifest schema
---------------

A manifest is a JSON list of rows.  Every field is optional except ``argv``,
and each one is exactly one ``pbrun`` flag:

===================  ====================================================
``argv``             the command, as a list; the part after ``pbrun --``
``cwd``              ``--cwd``: the Git checkout to seal (default: here)
``as_sealed_by``     ``--as-sealed-by``: require the original full action key
``demand``           ``--demand``: ``{"gpu": 1, "cpu": 8, "mem_gb": 32}``
``tags``             ``--tag``, once per entry
``env``              ``--env K=V``, once per pair
``timeout_s``        ``--timeout-s``
``deterministic``    ``--deterministic``
``anywhere``         ``--anywhere``
``here``             ``--here``
``no_default_env``   ``--no-default-env``
``snapshot_ref``     ``--snapshot-ref``, once per entry
``exclusive``        ``--exclusive``
``gpu_capacity``     ``--gpu-capacity``
``gpu_memory_gb``     ``--gpu-memory-gb``: pool GPU budget in GiB
``priority``         ``--priority``
``profile``          ``--profile``: a profiler mode, sealed into the row's key
``measurement``      ``--measurement``
``host_class``       ``--host-class``: worker class (pool measurement) or SLURM Feature
``retry_safe``       ``--retry-safe``
``progress_phases``  ``--progress-phase``, once per entry, ``"name=seconds"``
``progress_cycle``   ``--progress-cycle`` (boolean; requires phases)
``container_images`` ``--container-image``, once per entry: an exact local
                     image reference (``sha256:<64 hex>`` or
                     ``repository@sha256:<64 hex>``) the claiming box must
                     positively hold before the action runs. Mutable tags are
                     refused; PB never pulls or loads, so the image must
                     already be local. Pool transport only
``max_attempts``     ``--max-attempts``
``data_manifest``    ``--data-manifest``: file naming the shared-mount bytes
                     this row reads, so a storage-role loop can make them
                     resident first.  It is hashed into the action key
===================  ====================================================

Every field except ``argv`` is optional, and an omitted one is not passed to
``pbrun`` at all, so the row inherits whatever ``pbrun`` decides.  ``timeout_s``
is the one to be deliberate about: omitting it means no deadline, which is
what a long stage that is making progress wants, and setting it means the
scheduler kills the row at that many seconds whatever it was doing.

``progress_phases`` is the other half of being deliberate about time.  A row
that declares it -- ``["startup=1800", "encode=900", "publish=600"]``, in the
order the work does them -- is bounded by how long it goes without committing
work rather than by how long it runs: no total-duration limit while it keeps
advancing, and at most the sum of those allowances if it never advances at
all.  The action reports advancement with
``prismabuild.progress.commit(units, phase)``, or -- inside a pinned image or
any interpreter without PrismaBuild on its path -- by running the module the
worker names in ``PRISMABUILD_ACTION_PROGRESS_HELPER``; a row that declares
phases and reports nothing simply ends at that sum.  ``timeout_s`` and ``progress_phases``
compose rather than conflict: a row with both keeps the hard deadline AND
ends early on a stall.  Pool transport only, refused at load time on SLURM:
the watchdog is the pull-queue worker's, and a scheduler time limit is the
total duration this field exists to stop standing in for.

``progress_cycle: true`` lets the declared phases repeat. Each phase grants
its allowance once between increases in cumulative committed units, so a
publish can return to a longer encode step without permitting endless quiet
phase switching. This requires workers offering ``progress-cycle-v1``.

An unknown field is refused rather than ignored: a typo that is silently
dropped seals an action nobody asked for.

A row that names a path under the fleet's shared mount in ``argv`` or ``env``
and declares no ``data_manifest`` is warned about when it is actually
published: the storage prewarm can only warm the bytes a manifest names, so
those reads may arrive cold.  The scan is best-effort -- it sees the declared
``argv`` and ``env`` only, and a path the command only writes is a false
positive; ``cwd`` is not scanned because ``pbrun`` snapshots the checkout and
the action reads a box-local materialization of it.  ``--require-data-manifest``
is the opt-in for reads this scan cannot see: it refuses the whole manifest
before its first row is sealed unless every row carries a nonblank
``data_manifest``.

Unsupported rows are refused at load for the reason ``pbrun`` would refuse them at
submit, so a campaign of measurements is refused before it spends the fleet on
its first row rather than on its last:

* ``measurement`` without ``host_class`` under SLURM. Pool measurements
  instead default to the submitting platform/toolchain and host; an explicit
  class opts into matching workers while retaining platform-keyed numerics.
* ``measurement`` with ``anywhere`` under the pool. A locally sealed
  measurement cannot assert placement on every worker.
* ``host_class`` without ``measurement`` under ``--transport pool``. SLURM's
  host-class-keyed generation scope still requires controller attestation.
* ``max_attempts`` greater than 1.  Every row is submitted detached -- that is
  what lets one campaign hold N actions open -- and a retry needs somebody
  alive to see the attempt fail.  ``retry_safe`` is still worth spelling on a
  row without it: the retry policy is sealed into the action's identity, so a
  row that omits it is a different action from the hand-typed ``pbrun`` that
  passes it.
* ``gpu_memory_gb`` without GPU demand (explicit or implied by ``exclusive``),
  or under SLURM. The value must convert to a positive, bounded byte budget.

``--transport`` is a flag on the campaign and not a row field, because which
dispatcher carries the work is a fact about the fleet rather than about the
action.  One caveat that belongs to ``pbrun`` and travels here: ``exclusive``
is the one field whose demand ``pbrun`` derives differently per transport --
its ``--exclusive`` branch seals ``demand["gpu"] = gpu_capacity or 1`` under
SLURM and ``gpu_capacity or exclusive_gpu_demand(...)`` under the pull queue,
reading the pool's announced slot count -- and ``demand`` is sealed into the
action's params.  So an exclusive row keyed on one transport is a different
action on the other, and the two do not memoize each other.  Every other field
seals identically either way.

A logical request instead of a list
----------------------------------

A manifest may also be one JSON *object* whose ``schema`` is
``prismabuild.logical_request.v1``.  That is the work before anybody cut it,
and this tool cuts it: one ``common`` half that every child executes
identically, one ``roster`` of tasks with their residency keys and estimated
seconds, and one ``batch_policy`` saying how much setup a batch may amortize
and how long one may be estimated to run.  The partition is an exact cover --
every task in exactly one batch -- and it is published under the parent's key,
so a campaign interrupted halfway resumes into the same batches and publishes
only the children that are missing.

``common`` carries six base fields, spelled as the row fields above: ``argv``,
``cwd``, ``demand``, ``env``, ``gpu_memory_gb`` and ``data_manifest``. It also
accepts the ordinary progress, timeout, profile, retry, placement and snapshot
policies; the same row validation and pbrun sealing apply. ``priority`` stays
on the campaign CLI, and a single-action ``as_sealed_by`` key is refused. Its
``argv`` must contain ``{pb.task_batch}`` exactly once, as a whole argument.
That is where each child's own batch file lands -- substituted, never expanded
-- and the substituted value is a path into the CAS.

**A containerized producer has to mount the CAS root to open it.**  The fleet's
Docker shim adds no mounts of its own, so a child whose ``argv`` runs inside a
container sees the batch path but not the bytes unless the image is run with
the shared root mounted, exactly as a ``data_manifest``'s ``mount_prefix``
requires.  A child that cannot open its batch fails at its first read, which
is a poor way to learn this.

``--priority`` applies to the children; a row in a list manifest keeps its own
``priority`` field.  Decomposition is pull-queue work: ``--transport slurm``
is refused, because the SLURM lane submits one job per action and has no path
for publishing a plan's children.

Example
-------

Two rows, one wanting a GPU and one that must not have one::

    [
      {
        "argv": ["/home/rob/venv/bin/python", "-m", "mypkg.stage", "--shard", "3"],
        "cwd": "/home/rob/mypkg",
        "demand": {"gpu": 1, "mem_gb": 32},
        "timeout_s": 7200,
        "env": {"PYTHONPATH": "src"}
      },
      {
        "argv": ["/usr/bin/python3", "-m", "pytest", "-q", "tests"],
        "cwd": "/home/rob/mypkg",
        "demand": {"cpu": 8, "mem_gb": 16},
        "tags": ["x86"]
      }
    ]

Run it, and run it again::

    pbcampaign.py manifest.json
    pbcampaign.py manifest.json          # every row a cache hit, nothing runs
    pbcampaign.py --detach manifest.json # submit and walk away

Re-running is also how a campaign is resumed.  A row already finished is a
cache hit and costs nothing; a row still on a node is attached to by its
recorded job id rather than started a second time, and the table reports it
like any other row.  So a waiter that died, a laptop that closed or a
connection that dropped costs the wait, never the work.
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import math
from pathlib import Path
import posixpath
import re
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import (  # noqa: E402
    core as pb, decomposition as dc, pool,
)

import fleet_submit  # noqa: E402
import pbrun  # noqa: E402
import pbwait  # noqa: E402

#: Row field to ``pbrun`` flag, and how the value is spelled.  A table rather
#: than a chain of ifs, because the property that matters is that the mapping
#: is mechanical: a row is a ``pbrun`` command line and nothing else.
_VALUE_FIELDS = (
    ("cwd", "--cwd"),
    ("as_sealed_by", "--as-sealed-by"),
    ("timeout_s", "--timeout-s"),
    ("gpu_capacity", "--gpu-capacity"),
    ("gpu_memory_gb", "--gpu-memory-gb"),
    ("priority", "--priority"),
    ("profile", "--profile"),
    ("host_class", "--host-class"),
    ("max_attempts", "--max-attempts"),
    ("data_manifest", "--data-manifest"),
)
_SWITCH_FIELDS = (
    ("deterministic", "--deterministic"),
    ("anywhere", "--anywhere"),
    ("here", "--here"),
    ("no_default_env", "--no-default-env"),
    ("exclusive", "--exclusive"),
    ("measurement", "--measurement"),
    ("retry_safe", "--retry-safe"),
    ("progress_cycle", "--progress-cycle"),
)
_REPEATED_FIELDS = (
    ("tags", "--tag"),
    ("snapshot_ref", "--snapshot-ref"),
    ("progress_phases", "--progress-phase"),
    ("container_images", "--container-image"),
)
KNOWN_FIELDS = frozenset(
    {"argv", "demand", "env"}
    | {name for name, _ in _VALUE_FIELDS}
    | {name for name, _ in _SWITCH_FIELDS}
    | {name for name, _ in _REPEATED_FIELDS}
)


class ManifestError(Exception):
    """The manifest says something this cannot turn into a submission."""


#: Fields whose value reaches ``pbrun`` as a whole number, and the smallest
#: value each one can carry.  ``max_attempts`` is checked in
#: ``_require_submittable_row``, beside the refusal that reads it.
_INTEGER_FIELDS = (
    ("gpu_capacity", 0),
    ("priority", None),
)

#: Fields whose value reaches ``pbrun`` as text.
_TEXT_FIELDS = ("cwd", "host_class", "profile", "data_manifest", "as_sealed_by")

#: An absolute path mention inside a command word or an environment value.  A
#: mention starts where a path can start -- the beginning of the string, or a
#: separator a command line or an environment value uses -- and ends at the
#: next such separator, so a closing quote or bracket is not part of it.
_ABSOLUTE_PATH_MENTION = re.compile(
    r"(?<![^\s=,;:'\"(\[{|&])(/[^\s=,;:'\"()\[\]{}|&<>]+)"
)


def _names_a_shared_path(text: str) -> bool:
    """Whether one declared string mentions a path on the shared mount."""

    root = str(pbrun.SHARED_ROOT).rstrip("/")
    if not root:
        return False
    for mention in _ABSOLUTE_PATH_MENTION.findall(text):
        path = posixpath.normpath(mention)
        if path == root or path.startswith(root + "/"):
            return True
    return False


def shared_mount_read_evidence(row) -> list[str]:
    """Which declared fields of a row name a path on the shared mount.

    Best-effort by construction, and the reason the default policy is a
    warning rather than a refusal.  This sees the row's ``argv`` and ``env``
    values and nothing about what the command opens once it runs: a path built
    from a variable, read out of a configuration file, reached through a
    symlink or resolved from a relative name is invisible here.  ``cwd`` is
    not scanned, because ``pbrun`` snapshots the checkout and the action reads
    a box-local materialization of it.  A path the command only writes is a
    false positive, which is why the warning says the row *may* read cold.
    """

    evidence = []
    argv = row.get("argv")
    if isinstance(argv, list):
        for position, word in enumerate(argv):
            if isinstance(word, str) and _names_a_shared_path(word):
                evidence.append(f"argv[{position}]")
    environment = row.get("env")
    if isinstance(environment, dict):
        for name, value in sorted(environment.items()):
            if isinstance(value, str) and _names_a_shared_path(value):
                evidence.append(f"env[{name!r}]")
    return evidence


def _declares_data_manifest(row) -> bool:
    """Whether the row names a manifest file, ignoring blank spellings.

    A whitespace-only string is not a path any submitter meant, so it is
    treated as absent by both the warning and ``--require-data-manifest``.
    """

    value = row.get("data_manifest")
    return isinstance(value, str) and bool(value.strip())


def _require_data_manifest(row, *, where: str) -> None:
    """``--require-data-manifest``: every row declares the bytes it reads.

    Deliberately independent of ``shared_mount_read_evidence``.  A producer
    whose reads this tool cannot see -- a wrapper that builds paths at run
    time, a script that reads a dataset list -- has no visible declaration to
    match, so it opts into declaring every row's bytes instead.  Asked of the
    whole manifest before its first row is sealed, because publishing the rows
    before this one and then refusing cannot unwrite a read that already
    happened cold.
    """

    if _declares_data_manifest(row):
        return
    raise ManifestError(
        f"{where} has no data_manifest, and --require-data-manifest requires "
        f"one from every row before anything is submitted.  Give it a "
        f"data_manifest naming the shared-mount bytes it reads, or drop the "
        f"flag to accept cold reads."
    )


def _warn_about_a_cold_shared_read(row, *, where: str) -> None:
    """Warn about one row that declares shared-mount reads it does not name.

    Printed only for a row that is actually being published: a cache hit
    reads nothing and a refusal runs nothing, and the submission outcome
    already says which this was.  It says *may* read, because the declaration
    is all this sees.
    """

    if _declares_data_manifest(row):
        return
    evidence = shared_mount_read_evidence(row)
    if not evidence:
        return
    print(
        f"pbcampaign: WARNING {where} names the shared mount in "
        f"{', '.join(evidence)} but has no data_manifest: it may read those "
        f"paths cold, because the storage prewarm can only warm the bytes a "
        f"manifest names.  A path it only writes is a false positive; argv "
        f"and env are all this scan sees.  Add the row's data_manifest, or "
        f"pass --require-data-manifest to require one from every row.",
        file=sys.stderr, flush=True,
    )


def _refuse(index: int, field: str, wanted: str, value) -> ManifestError:
    """One refusal, naming the row, the field and the value that failed.

    A manifest is edited by hand, so the row index and the field name are the
    whole of what the author needs to find the typo.  The value is quoted
    because the common case is a number that arrived as text.
    """

    return ManifestError(f"row {index}: {field} {wanted}, got {value!r}")


def _require_integer(value, *, index: int, field: str, minimum=None) -> None:
    """Refuse a value ``pbrun`` cannot read as a whole number.

    ``pbrun`` parses its own ``--demand`` with ``int()``, so a string of
    digits is as good as an integer and is accepted here for the same reason.
    A boolean is refused: Python reads ``true`` as 1, so a field that was
    meant to be a count and arrived as a flag would seal a demand for one of
    something nobody asked for.
    """

    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise _refuse(index, field, "must be an integer", value)
    try:
        number = int(value)
    except ValueError:
        raise _refuse(index, field, "must be an integer", value) from None
    if minimum is not None and number < minimum:
        raise _refuse(index, field, f"must be at least {minimum}", value)


def _require_row_shape(row, *, index: int) -> None:
    """Refuse a field whose value cannot become the flag it stands for.

    Every check here is a conversion ``pbrun_argv`` performs later, moved to
    load time.  Performed later, the first bad row aborts a campaign that has
    already submitted the rows before it, and their keys are lost with the
    traceback: the work is queued and nobody holds its names.

    A truthiness field is held to a JSON boolean rather than to whatever is
    truthy.  ``"deterministic": "no"`` reads as true and seals the opposite of
    what it says, which is the same fault as a field that is silently ignored.
    """

    for field in _TEXT_FIELDS:
        value = row.get(field)
        if value is not None and not isinstance(value, str):
            raise _refuse(index, field, "must be a string", value)
    if "as_sealed_by" in row:
        try:
            pbrun.require_reseal_key(row["as_sealed_by"])
        except ValueError as exc:
            raise _refuse(index, "as_sealed_by", str(exc), row["as_sealed_by"]) from None
    for field, minimum in _INTEGER_FIELDS:
        if row.get(field) is not None:
            _require_integer(row[field], index=index, field=field,
                             minimum=minimum)
    timeout = row.get("timeout_s")
    if timeout is not None and (
        isinstance(timeout, bool) or not isinstance(timeout, (int, float))
    ):
        raise _refuse(index, "timeout_s", "must be a number of seconds",
                      timeout)
    budget = row.get("gpu_memory_gb")
    if budget is not None:
        try:
            if isinstance(budget, bool) or not isinstance(budget, (int, float, str)):
                raise ValueError("must be a number of GiB")
            pbrun.adaptive_gpu.memory_budget_bytes(float(budget))
        except (ValueError, OverflowError) as exc:
            raise _refuse(index, "gpu_memory_gb", str(exc), budget) from None
    for field, _flag in _SWITCH_FIELDS:
        value = row.get(field)
        if value is not None and not isinstance(value, bool):
            raise _refuse(index, field, "must be true or false", value)
    for field, _flag in _REPEATED_FIELDS:
        value = row.get(field)
        if value is None:
            continue
        if not isinstance(value, list):
            # A bare string is the shape to name: it iterates one character at
            # a time, so ``"tags": "x86"`` would seal three tags.
            raise _refuse(index, field, "must be a list of strings", value)
        for entry in value:
            if not isinstance(entry, str) or not entry:
                raise _refuse(index, field,
                              "must hold non-empty strings", entry)
    demand = row.get("demand")
    if demand is not None:
        if not isinstance(demand, dict):
            raise _refuse(index, "demand",
                          "must be an object of name to count", demand)
        for name, count in demand.items():
            if not isinstance(name, str) or not name:
                raise _refuse(index, "demand",
                              "must name each resource with a string", name)
            _require_integer(count, index=index, field=f"demand[{name!r}]",
                             minimum=0)
    environment = row.get("env")
    if environment is not None:
        if not isinstance(environment, dict):
            raise _refuse(index, "env",
                          "must be an object of name to value", environment)
        for name, value in environment.items():
            if not isinstance(name, str) or not name or "=" in name:
                raise _refuse(
                    index, "env",
                    "must name each variable with a string holding no '='",
                    name)
            if isinstance(value, bool) or not isinstance(
                value, (str, int, float)
            ):
                # ``--env K=V`` carries text.  A list or an object would be
                # sealed as its Python repr, which is not what the row says
                # and is not a value any shell would have produced.
                raise _refuse(index, f"env[{name!r}]",
                              "must be a string or a number", value)


def _require_submittable_row(row, *, index: int, transport: str) -> None:
    """Refuse a row ``pbrun`` would refuse, in ``pbrun``'s own words.

    The refusals are asked of ``pbrun`` rather than restated here.  A second
    copy of the measurement rule would be a second policy, and the row that
    told the operator something different from the flag it becomes is exactly
    the row this tool exists to make reproducible at the terminal.

    Pool class placement is an opt-in measurement contract; SLURM also supports
    controller-attested host-class-keyed generation.
    """

    try:
        # Preserve pbrun's whitespace normalization and ask its one closed
        # fleet-client vocabulary before any row can publish. The generic
        # PoolQueue ledger remains available to other producers through its
        # own API.
        demand = row.get("demand") or {}
        pbrun.validate_fleet_demand(
            {name.strip(): count for name, count in demand.items()}
        )
    except SystemExit as exc:
        raise ManifestError(f"row {index}: {exc}") from None
    try:
        pbrun.require_host_class_scope(
            measurement=bool(row.get("measurement")),
            host_class=row.get("host_class"),
            transport=transport, anywhere=bool(row.get("anywhere")),
        )
    except SystemExit as exc:
        raise ManifestError(f"row {index}: {exc}") from None
    try:
        demand = row.get("demand") or {}
        pbrun.require_gpu_memory_scope(
            gpu_memory_gb=row.get("gpu_memory_gb"),
            gpu=bool(row.get("exclusive") or int(demand.get("gpu", 0))),
            transport=transport,
        )
    except ValueError as exc:
        raise ManifestError(f"row {index}: {exc}") from None
    try:
        # Parsed here rather than trusted, so a row whose phases pbrun would
        # refuse is refused at load time with the rest of the manifest -- and
        # so the transport rule is asked of pbrun in pbrun's own words.
        pbrun.require_progress_scope(
            progress=pbrun.parse_progress_phases(
                row.get("progress_phases"), cycle=row.get("progress_cycle", False)),
            transport=transport,
        )
    except ValueError as exc:
        raise ManifestError(f"row {index}: {exc}") from None
    except SystemExit as exc:
        raise ManifestError(f"row {index}: {exc}") from None
    images = row.get("container_images") or []
    refusal = pbrun.container_image_refusal(images)
    if refusal is not None:
        raise _refuse(index, "container_images", refusal, images)
    try:
        # The same closed vocabulary pbrun applies at submit time: the image
        # check is a pull-queue claim decision, and a SLURM row would carry a
        # requirement nothing verifies.
        pbrun.require_container_image_scope(images=images, transport=transport)
    except ValueError as exc:
        raise ManifestError(f"row {index}: {exc}") from None
    attempts = row.get("max_attempts")
    if attempts is None:
        return
    if not isinstance(attempts, int) or isinstance(attempts, bool) or attempts < 1:
        raise ManifestError(
            f"row {index}: max_attempts must be an integer of at least 1"
        )
    if attempts > 1:
        # Every row goes out detached, so this is pbrun's own refusal reached
        # by a route the row's author cannot see.
        raise ManifestError(
            f"row {index}: {pbrun.detached_attempts_refusal(attempts)}.\n"
            f"A campaign submits every row detached, which is what lets one "
            f"command hold N actions open."
        )


def load_manifest(
    path, *, transport: str = "slurm", require_data_manifest: bool = False,
) -> list[dict] | dict:
    """Read the manifest, and refuse anything it cannot mean.

    Refused at load time, before a single row is sealed: a campaign that
    submits forty rows and then discovers the forty-first is malformed has
    already spent the fleet on a manifest its author has to edit.

    "Anything it cannot mean" includes every value conversion, not only the
    fields ``pbrun`` itself would refuse.  A count that arrived as a word used
    to convert while the row was being turned into a command line, which is
    after the rows before it had been submitted.

    Two shapes, and the JSON says which: a list is rows somebody already cut,
    a ``prismabuild.logical_request.v1`` object is one piece of work for this
    tool to cut.  The caller branches on what comes back.

    ``transport`` is the campaign's; scope and GPU-budget rules read it.
    The default is the lane, where a class is honoured, so a caller checking a
    manifest without a fleet in mind is told about the row and not about the
    transport.

    ``require_data_manifest`` is ``--require-data-manifest``: every row, and a
    logical request's common half, must carry a nonblank ``data_manifest``.
    It is asked here, beside every other whole-manifest refusal, so a campaign
    of forty rows cannot publish thirty-nine of them before the fortieth is
    found to be unnamed.
    """

    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise ManifestError(f"cannot read the manifest: {exc}") from None
    except ValueError as exc:
        raise ManifestError(f"the manifest is not JSON: {exc}") from None
    if isinstance(value, dict) and value.get("schema") == dc.LOGICAL_REQUEST_SCHEMA_V1:
        # The work before anybody cut it.  Validated here for the same reason
        # rows are: a request that names an impossible batch policy must be
        # refused at the operator's terminal, not after a plan is published
        # under a parent key that will outlive the mistake.
        try:
            request = dc.validate_logical_request(value)
            _require_row_shape(request["common"], index=0)
            _require_submittable_row(request["common"], index=0, transport=transport)
            if require_data_manifest:
                _require_data_manifest(
                    request["common"],
                    where="the common half of this logical request")
            return request
        except pb.ActionContractError as exc:
            raise ManifestError(f"this logical request is malformed: {exc}") from None
    if not isinstance(value, list):
        raise ManifestError(
            f"a manifest is a JSON list of rows, or one "
            f"{dc.LOGICAL_REQUEST_SCHEMA_V1} object -- not "
            f"{type(value).__name__}"
            + (f" with schema {value.get('schema')!r}"
               if isinstance(value, dict) else "")
        )
    rows = []
    for index, row in enumerate(value):
        if not isinstance(row, dict):
            raise ManifestError(f"row {index} is not an object")
        unknown = sorted(set(row) - KNOWN_FIELDS)
        if unknown:
            raise ManifestError(
                f"row {index} names fields this does not know: "
                f"{', '.join(unknown)}. A field that was ignored would seal an "
                f"action nobody asked for; the known ones are "
                f"{', '.join(sorted(KNOWN_FIELDS))}"
            )
        argv = row.get("argv")
        if not isinstance(argv, list) or not argv or not all(
            isinstance(item, str) for item in argv
        ):
            raise ManifestError(
                f"row {index} needs argv: a non-empty list of strings"
            )
        _require_row_shape(row, index=index)
        _require_submittable_row(row, index=index, transport=transport)
        if require_data_manifest:
            _require_data_manifest(row, where=f"row {index}")
        rows.append(row)
    return rows


def pbrun_argv(row) -> list[str]:
    """The ``pbrun`` command line this row means, flag for flag.

    Flag order is fixed but arbitrary; it cannot move the action key.  ``pbrun``
    normalizes and sorts the effective placement before sealing it, and every
    other flag here reaches identity as a value rather than as a position.
    """

    flags: list[str] = []
    for field, flag in _VALUE_FIELDS:
        if row.get(field) is not None:
            flags += [flag, str(row[field])]
    demand = row.get("demand") or {}
    if not isinstance(demand, dict):
        raise ManifestError("demand must be an object of name to count")
    if demand:
        flags += ["--demand", ",".join(
            f"{name}={int(count)}" for name, count in sorted(demand.items())
        )]
    for field, flag in _REPEATED_FIELDS:
        for value in row.get(field) or []:
            flags += [flag, str(value)]
    environment = row.get("env") or {}
    if not isinstance(environment, dict):
        raise ManifestError("env must be an object of name to value")
    for name, value in sorted(environment.items()):
        flags += ["--env", f"{name}={value}"]
    for field, flag in _SWITCH_FIELDS:
        if row.get(field):
            flags.append(flag)
    return flags + ["--", *[str(item) for item in row["argv"]]]


# The ``retryable`` value ``submit_row`` puts on a refused record when pbrun's
# worker-offer discovery timed out and its reader was reaped.
OFFER_DISCOVERY_TIMED_OUT = "offer_discovery_timed_out"


def submit_row(row, *, transport: str = "") -> dict:
    """Seal and submit one row through ``pbrun``, and return what it printed.

    A row that ``pbrun`` refuses is recorded and the campaign goes on.  Forty
    rows are not worth losing to the one that named a checkout that is not
    there, and the refusal reaches the operator on the table with the rest.
    """

    flags = ["--detach"]
    if transport:
        flags += ["--transport", transport]
    try:
        flags += pbrun_argv(row)
    except Exception as exc:                                     # noqa: BLE001
        # ``load_manifest`` refuses every shape this converts, so reaching
        # here means a conversion nothing validates yet.  It is still one
        # row's refusal: aborting would strand the keys of the rows already
        # submitted, which is the whole of what a detached campaign hands
        # back.
        return {"status": "refused",
                "error": f"this row cannot be turned into a pbrun command "
                         f"line: {type(exc).__name__}: {exc}",
                "flags": flags}
    saved = sys.argv
    captured = io.StringIO()
    sys.argv = ["pbrun.py", *flags]
    code, refusal = 0, ""
    try:
        with contextlib.redirect_stdout(captured):
            code = pbrun.main()
    except pbrun.OfferDiscoveryTimedOut as exc:
        # Still a refusal, so the submit-all and decomposed paths count it as
        # one.  The marker lets a windowed controller retry the row, because
        # pbrun published no runnable item before the offer scan (#560).
        return {"status": "refused", "retryable": OFFER_DISCOVERY_TIMED_OUT,
                "error": str(exc.code), "flags": flags}
    except SystemExit as exc:
        # ``pbrun`` refuses by raising ``SystemExit`` with the explanation as
        # its argument, so the text IS the diagnosis -- which tag no box
        # offers, which checkout is not there.  Reporting the exit status
        # instead would hand the operator a number for a message somebody
        # wrote for them.
        code = exc.code if isinstance(exc.code, int) else 2
        refusal = "" if isinstance(exc.code, (int, type(None))) else str(exc.code)
    except Exception as exc:                                     # noqa: BLE001
        return {"status": "refused", "error": f"{type(exc).__name__}: {exc}",
                "flags": flags}
    finally:
        sys.argv = saved
    lines = [line for line in captured.getvalue().splitlines() if line.strip()]
    if code != 0 or len(lines) != 1:
        return {"status": "refused",
                "error": refusal or f"pbrun exited {code}", "flags": flags}
    published = json.loads(lines[0])
    published["flags"] = flags
    return published


def _submit_record(row, *, index: int, transport: str) -> dict:
    """Keep earlier keys when one submission fails, and print each full key."""

    try:
        published = submit_row(row, transport=transport)
    except Exception as exc:                                     # noqa: BLE001
        published = {"status": "refused", "flags": [],
                     "error": f"row {index}: {type(exc).__name__}: {exc}"}
    key = str(published.get("action_key") or "")
    if published.get("status") in {"submitted", "attached"}:
        # Only a row that is actually going to run can read anything; the
        # submission outcome already says whether this one will.
        _warn_about_a_cold_shared_read(row, where=f"row {index}")
    label = str(published["status"])
    if published.get("retryable"):
        label += f" ({published['retryable']})"
    print(f"pbcampaign: row {index} {label} "
          f"{key or '-'}", file=sys.stderr, flush=True)
    return published


def submit(rows, *, transport: str = "") -> list[dict]:
    """Submit every row, in order, and return one submission record each."""

    return [_submit_record(row, index=index, transport=transport)
            for index, row in enumerate(rows)]


def _pool_slot_occupied(queue, key: str) -> bool:
    """A terminal or receipt can precede a withdrawn claim's cleanup.

    Missing leaves are absence; unreadable leaves are not capacity. Check
    ready before claimed so a ready-to-claimed move cannot open a slot.
    This is a controller observation, never permission to release tokens.
    """

    for state in (pool.READY, pool.CLAIMED):
        try:
            queue.item_path(state, key).lstat()
        except FileNotFoundError:
            continue
        return True
    return False


def run_windowed(rows, *, transport: str, max_inflight: int,
                 wait_s: float) -> tuple[list[dict], list[dict]]:
    """Publish a bounded window, refilling after any row drains.

    A single controller owns this window. It keeps only ordinary pbrun
    submissions; restarting the same manifest attaches to its published
    prefix. Unknown outcomes never authorize another submission. The first
    window is published even with wait_s=0; subsequent work shares one
    monotonic wait budget, including time spent submitting replacements.
    A worker-offer discovery timeout publishes nothing, so that row is retried
    within the same budget instead of stopping the window.
    """

    queue = pool.PoolQueue(pbrun.SH / "pb-queue")
    cas = pb.PrismaBuildCAS(pbrun.SH / "cas")
    submissions, pending, observed = [], {}, {}
    cached = set()
    deadline = None
    stopped = False
    while True:
        retrying = False
        while len(submissions) < len(rows) and len(pending) < max_inflight:
            if deadline is not None and time.monotonic() >= deadline:
                break
            index = len(submissions)
            # Keep the existing per-row exception boundary and print the full
            # digest immediately: a killed controller must leave its keys.
            published = _submit_record(rows[index], index=index, transport=transport)
            if published.get("retryable") == OFFER_DISCOVERY_TIMED_OUT:
                # pbrun refused before it published a runnable item, so no
                # outcome is uncertain and no slot is held.  The offer scan
                # was slow, which says nothing about the row.  Submit the same
                # row again at the next poll while the wait budget lasts; when
                # the budget runs out the row is reported not_submitted (#560).
                print(f"pbcampaign: row {index} not submitted yet: "
                      f"{published.get('error')}; retrying while --wait-s lasts",
                      file=sys.stderr, flush=True)
                retrying = True
                break
            submissions.append(published)
            key = str(published.get("action_key") or "")
            if published.get("status") not in {"submitted", "attached", "cache_hit"} or not key:
                # Even a refusal may follow an uncertain publication. Do not
                # spend the next slot while its fate is unknown.
                stopped = True
                break
            pending[key] = published.get("published_unix")
            if published["status"] == "cache_hit":
                cached.add(key)
            else:
                cached.discard(key)

        if deadline is None:
            deadline = time.monotonic() + wait_s
        if pending:
            waited = pbwait.wait_for_keys(
                queue, list(pending), cas=cas, wait_s=0.0,
                generations={key: stamp for key, stamp in pending.items()
                             if isinstance(stamp, (int, float))},
            )
            for row in waited:
                key = row["action_key"]
                if row["status"] in {"record_error", "unreadable"}:
                    # A reaped, timed-out read says nothing about the action.
                    # While the campaign deadline lasts, keep the key pending:
                    # it still holds its slot, so no new submission is
                    # authorized by it, and the next pass looks again. The
                    # row is still recorded, so a deadline that passes on it
                    # reports exit 74 rather than a verdict.
                    if not (row.get("observation_timed_out")
                            and time.monotonic() < deadline):
                        stopped = True
                elif row["status"] != "waiting":
                    try:
                        occupied = transport == "pool" and _pool_slot_occupied(queue, key)
                    except OSError as exc:
                        row = dict(row, status="record_error", succeeded=False,
                                   note=f"cannot establish a free campaign slot: {exc}")
                        stopped = True
                    else:
                        if occupied:
                            row = dict(row, status="waiting", succeeded=False,
                                       note="queue work or claim cleanup still present")
                        else:
                            pending.pop(key, None)
                            # A pbrun cache hit already verified reusable
                            # output; an older failed attempt is not its
                            # verdict. The slot still needed the check above.
                            if not row["succeeded"] and key not in cached:
                                # Keep a resumable prefix: continuing past a
                                # failure could let a restart republish that
                                # failed prefix before it discovers a later
                                # full window that is still running.
                                stopped = True
                observed[key] = row

        if stopped or (not pending and len(submissions) == len(rows)):
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        # A retry also waits a poll, so a slow offer scan is not asked again at
        # once, even when nothing else is pending.
        if retrying or (pending and (len(pending) >= max_inflight
                                     or len(submissions) == len(rows))):
            time.sleep(min(pbrun.POLL_S, remaining))

    for index in range(len(submissions), len(rows)):
        print(f"pbcampaign: row {index} not_submitted; "
              "resume the same manifest to continue", file=sys.stderr, flush=True)
        submissions.append({"status": "not_submitted", "row": index})
    return submissions, list(observed.values())


def rows_for(submissions, waited) -> list[dict]:
    """One table row per manifest row, in the manifest's order.

    A row that was a cache hit keeps that word rather than borrowing the status
    off an older terminal record for the same key.  The key is a content hash,
    so such a record is an account of a different run; what this campaign did
    with the row was find it already done.
    """

    by_key = {str(row["action_key"]): row for row in waited}
    table = []
    for published in submissions:
        key = str(published.get("action_key") or "")
        status = str(published.get("status") or "refused")
        if key in by_key and (status in {"submitted", "attached"} or
                              by_key[key]["status"] in {"waiting", "unreadable", "record_error"}):
            table.append(by_key[key])
        elif status == "cache_hit":
            table.append({
                "action_key": key, "status": "cache_hit", "transport": "cas",
                "host": "-", "elapsed_s": None, "returncode": None,
                "receipt_published": True, "succeeded": True,
            })
        else:
            table.append({
                "action_key": key, "status": status if status == "not_submitted" else "refused",
                "transport": str(published.get("transport") or "-"),
                "host": "-", "elapsed_s": None, "returncode": None,
                "receipt_published": None, "succeeded": False,
                "note": (f"row {published['row']}; resume the same manifest"
                         if status == "not_submitted" else None),
            })
    return table


# ---------------------------------------------------------------------------
# One logical request, decomposed
# ---------------------------------------------------------------------------
#
# A manifest is N rows somebody already cut.  A logical request is the work
# before anybody cut it: one command with one reserved slot, one roster of
# tasks and one policy saying how much setup a batch may amortize.  The cut
# happens here, before a single action is published, which is the whole point
# of #517 -- the fleet is handed independently retryable quanta rather than
# one long action that has to be watched.
#
# Nothing about the cut is negotiable after the fact.  The plan is published
# under the parent's key and reused verbatim by every later run, so a campaign
# interrupted halfway resumes into the same batches rather than asking the
# batcher for a second opinion about a roster that has not changed.

DECOMPOSITIONS = "decompositions"


def decomposition_dir(cas, parent_key: str) -> Path:
    """Where one parent's plan and publication index live.

    Beside the CAS's own shards rather than inside them: these are records
    about actions, like ``cas/requests``, and neither is an action input.
    ``pb_gc`` sweeps named subtrees -- claims, locks, namespaces, staging --
    and counts the rest, so a directory it does not know is never removed
    under a resuming campaign.  It is also never reclaimed: a plan is a few
    kilobytes per campaign and stays forever, which is the price of being
    able to resume one.
    """

    return Path(str(cas.root)) / DECOMPOSITIONS / parent_key[:2] / parent_key


def _stored_document(path: Path) -> object | None:
    """What is published at ``path``, or ``None`` if nothing is."""

    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ManifestError(f"cannot read {path}: {exc}") from None
    try:
        return json.loads(raw.decode("utf-8"))
    except ValueError as exc:
        raise ManifestError(
            f"{path} is published but is not JSON: {exc}.  It is immutable, "
            f"so this is damage rather than drift; move it aside deliberately "
            f"before re-running this campaign"
        ) from None


def frozen_plan(request, frozen_common, *, cas) -> dict:
    """The one partition of this parent, derived once and thereafter read.

    Read before build, on purpose.  A resumed campaign must not call the
    batcher again: the roster and the policy are the same bytes, so a second
    call would have to produce the same cut to be correct, and nothing but the
    published plan can prove that it did.  Reading it back instead makes the
    cut a fact about the parent rather than a property of whichever version of
    the batcher happened to run.

    The first writer wins the hard link; a loser reads the winner's bytes and
    validates them, including that the key they name is the parent asked for.
    """

    key = dc.parent_key(
        frozen_common, request["roster"], request["batch_policy"])
    path = decomposition_dir(cas, key) / "plan.json"
    stored = _stored_document(path)
    if stored is None:
        plan = dc.build_plan(request, frozen_common)
        if not pb._atomic_publish(path, dc.document_bytes(plan)):
            stored = _stored_document(path)
        else:
            return plan
    plan = dc.validate_plan(stored)
    if plan["parent_key"] != key:
        raise ManifestError(
            f"the plan published at {path} is keyed on parent "
            f"{plan['parent_key'][:12]}, not on {key[:12]}"
        )
    return plan


def publish_index(index, *, cas, parent_key: str) -> None:
    """Record which children this plan authorizes, before publishing any.

    Written first so that a crash in the middle of a campaign leaves behind a
    statement of which keys were meant to exist; the next run fills the gaps
    and publishes nothing else.  Everything upstream of it is pure or
    content-addressed, so this is the first byte that says these children are
    real.

    A differing index under one parent is refused rather than replaced.  Every
    input to it -- the plan, the envelopes, their digests, the sealed child
    keys -- is a deterministic function of bytes already fixed, so two runs
    that disagree here disagree about something that cannot vary, and the
    honest answer is to stop and say so.
    """

    path = decomposition_dir(cas, parent_key) / "publication.json"
    raw = dc.document_bytes(index)
    stored = _stored_document(path)
    if stored is None:
        if pb._atomic_publish(path, raw):
            return
        stored = _stored_document(path)
    if dc.document_bytes(stored) != raw:
        raise ManifestError(
            f"the publication index at {path} names different children than "
            f"this run sealed.  The index is a function of the plan and the "
            f"roster, so nothing that can legitimately vary produced this; "
            f"read both before touching either"
        )


def child_record(child, *, args, queue, cas) -> dict:
    """Publish one child, or attach to what is already answering for it.

    The same three answers a detached ``pbrun`` gives, in the same shape, so
    the campaign's table and ``pbwait`` read a decomposed child exactly as
    they read a hand-written row: already in the CAS, already running, or
    published now.
    """

    key = str(child["action_key"])
    cas.publish_action_request(child)
    if cas.lookup(child) is not None:
        return json.loads(pbrun.detach_line(
            key, transport=args.transport, status="cache_hit",
            queue_root=queue.root,
        ))
    try:
        live = pbrun.bounded_attachment(queue, key)
    except (pbrun.OutcomeReadUnavailable, OSError) as exc:
        raise SystemExit(f"pbrun: unavailable detached attachment for {key[:12]}: {exc}")
    if live is not None:
        return json.loads(pbrun.detach_line(
            key, transport=live["transport"], status="attached",
            queue_root=queue.root, published_unix=live["generation"],
            submission=live["submission"],
        ))
    queued = pbrun.publish_or_refuse(
        queue, pbrun.publication_row(child, args=args, queue=queue))
    return json.loads(pbrun.detach_line(
        key, transport="pool", status="submitted", queue_root=queue.root,
        published_unix=pbrun.published_generation(queue, key, queued),
        submission=queued,
    ))


def decompose(
    request, *, transport: str, priority: int
) -> tuple[list[dict], dict]:
    """Cut one logical request into children and publish every one of them.

    The order is the order of what can still refuse.  Validation, then
    ``pbrun``'s own argument parsing and submission resolution, then the
    freeze that cross-checks the declaration against what was sealed, then the
    plan, then every ingest and every child seal, then the placement verdict
    -- and only then the index and the children.  Everything that can say no
    says it before anything durable names a child.

    The children are sealed off one template, so they answer for one source
    tree.  Re-snapshotting a mutable checkout per child would give siblings
    different code closures and therefore different parents, which is the bug
    this whole two-stage shape exists to prevent.

    What comes back is one submission record per child, and beside it the
    three things the group receipt cannot be computed without: the request the
    cover is proved against, the plan that fixed the membership, and the
    sealed children whose receipts carry the answers.
    """

    if transport == "slurm":
        raise ManifestError(
            "a logical request is decomposed onto the pull queue; the SLURM "
            "lane submits one job per action and has no path for publishing "
            "a plan's children.  Re-run with --transport pool"
        )
    flags = ["--detach", "--transport", transport, "--priority", str(priority)]
    flags += pbrun_argv(request["common"])
    try:
        args = pbrun.parse_args(flags)
        prepared = pbrun.prepare_submission(args)
    except SystemExit as exc:
        # ``pbrun`` refuses with the explanation as the exception's argument,
        # and the text is the diagnosis.  Prefixed, because from here it is
        # the campaign that refused: there are no other rows to go on with.
        raise ManifestError(
            f"pbrun refused this request's common half: "
            f"{exc.code if not isinstance(exc.code, int) else f'exit {exc.code}'}"
            f"\n  pbrun {' '.join(flags)}"
        ) from None
    template = prepared["template"]
    cas = template["cas"]

    frozen = dc.freeze_common(
        request["common"],
        action_common=pbrun.template_action_common(template),
    )
    plan = frozen_plan(request, frozen, cas=cas)
    roster_input, _ = cas.ingest_bytes(
        dc.document_bytes(request["roster"]), input_id=dc.TASK_ROSTER_INPUT_ID)

    children, digests = [], []
    for ordinal in range(len(plan["partitions"])):
        batch_input, _ = cas.ingest_bytes(
            dc.document_bytes(
                dc.batch_envelope(request, plan, child_ordinal=ordinal)),
            input_id=dc.TASK_BATCH_INPUT_ID,
        )
        children.append(pbrun.seal_decomposed_child(
            template,
            request=request,
            plan=plan,
            child_ordinal=ordinal,
            roster_input=roster_input,
            batch_input=batch_input,
            cas=cas,
        ))
        digests.append(str(batch_input["sha256"]))

    queue = pool.PoolQueue(pbrun.SH / "pb-queue")
    # Once, not once per child.  Every child of one plan carries the same
    # placement and the same demand, so the census answers them all the same
    # way, and printing that answer N times would bury it.
    pbrun.announce_placement(
        prepared["offer_queue"](), children[0], args=args, cwd=prepared["cwd"],
        portable_checkout=prepared["portable_checkout"],
    )

    publish_index(
        dc.publication_index(
            plan,
            batch_input_digests=digests,
            child_action_keys=[str(child["action_key"]) for child in children],
        ),
        cas=cas,
        parent_key=plan["parent_key"],
    )
    print(f"pbcampaign: parent {plan['parent_key'][:pbwait.KEY_WIDTH]} "
          f"cut into {len(children)} children", file=sys.stderr, flush=True)

    records = []
    for ordinal, child in enumerate(children):
        try:
            published = child_record(child, args=args, queue=queue, cas=cas)
        except SystemExit as exc:
            # One child's refusal, reported like one row's.  The children
            # already published are in ``records`` and are the whole of what a
            # detached campaign hands back; raising out of here would strand
            # every one of their keys.
            published = {"status": "refused", "flags": [],
                         "action_key": str(child["action_key"]),
                         "error": str(exc.code)}
        print(f"pbcampaign: child {ordinal} {published['status']} "
              f"{str(published.get('action_key') or '')[:pbwait.KEY_WIDTH]}",
              file=sys.stderr, flush=True)
        records.append(published)
    if any(record.get("status") in {"submitted", "attached"}
           for record in records):
        _warn_about_a_cold_shared_read(
            request["common"],
            where="the common half of this logical request")
    return records, {"request": request, "plan": plan, "children": children}


# --------------------------------------------------------------------------
# Closing a decomposed campaign
# --------------------------------------------------------------------------

def child_result_manifest(child, *, cas, receipt=None) -> dict | None:
    """Read one finished child's result manifest, or ``None`` if it has none.

    Through the receipt rather than off the worker's filesystem: a child's
    manifest is its declared result, so ``result_path`` re-verifies the bytes
    against the digest the receipt fixed before anything reads them.  There is
    no second place to look, which is the point -- a child cannot pass while
    answering for nothing, because a missing manifest is a missing result and
    the action fails where it ran.
    """

    if receipt is None:
        receipt = cas.lookup(child)
    if receipt is None:
        return None
    raw = cas.result_path(receipt, child).read_bytes()
    try:
        return json.loads(raw)
    except ValueError as exc:
        raise ManifestError(
            f"child {str(child['action_key'])[:12]} published a result that is "
            f"not a JSON document: {exc}"
        ) from None


def publish_group_receipt(receipt, *, cas, parent_key: str) -> Path:
    """Record the one receipt that says the whole roster was answered.

    Written only after the cover is proved exact, and refused rather than
    replaced when it differs from one already there.  Every field in it is a
    function of the immutable plan and of child receipts that are themselves
    immutable, so two runs cannot honestly disagree about it; if they do, the
    disagreement is the finding and overwriting would hide it.
    """

    path = decomposition_dir(cas, parent_key) / "group.json"
    raw = dc.document_bytes(receipt)
    stored = _stored_document(path)
    if stored is None:
        if pb._atomic_publish(path, raw):
            return path
        stored = _stored_document(path)
    if dc.document_bytes(stored) != raw:
        raise ManifestError(
            f"the group receipt at {path} is not the one this run verified.  "
            f"It is a function of the plan and of immutable child receipts, so "
            f"nothing that can legitimately vary produced this; read both "
            f"before touching either"
        )
    return path


def close_group(group, *, cas) -> int:
    """Prove the children between them answered the roster, once each.

    The last gate of a decomposed campaign and the only one that can see the
    whole of it: every earlier refusal is about one child in isolation.  An
    incomplete, failed or withdrawn set is never a group success, so a child
    without a receipt ends the campaign here -- the table above has already
    named which one, and repeating that would be noise.

    It runs after the table rather than before it because the table is what an
    operator reads to find out what happened; this line says whether what
    happened answers the request.
    """

    request, plan, children = group["request"], group["plan"], group["children"]
    try:
        if len(children) != len(plan["partitions"]):
            raise ManifestError(
                f"the plan names {len(plan['partitions'])} children, but "
                f"the group carries {len(children)} sealed actions"
            )
        index_path = decomposition_dir(cas, plan["parent_key"]) / "publication.json"
        index = _stored_document(index_path)
        child_keys = [str(child["action_key"]) for child in children]
        if (not isinstance(index, dict)
                or index.get("parent_key") != plan["parent_key"]
                or index.get("plan_key") != plan["plan_key"]
                or index.get("child_action_keys") != child_keys):
            raise ManifestError(
                f"the group children do not match the frozen publication "
                f"index at {index_path}"
            )
        manifests = []
        child_evidence = []
        for ordinal, child in enumerate(children):
            expected_batch = dc.logical_batch_param(
                request, plan, child_ordinal=ordinal)
            params = child.get("params")
            if (not isinstance(params, dict)
                    or params.get(dc.LOGICAL_BATCH_PARAM) != expected_batch):
                raise ManifestError(
                    f"child {ordinal} does not seal the plan's exact batch "
                    f"membership and identity"
                )
            child_receipt = cas.lookup(child)
            if child_receipt is None:
                print(f"pbcampaign: no group receipt: child {ordinal} of "
                      f"{len(children)} has no receipt", file=sys.stderr)
                return 1
            manifest = child_result_manifest(child, cas=cas,
                                             receipt=child_receipt)
            if manifest is None:
                print(f"pbcampaign: no group receipt: child {ordinal} of "
                      f"{len(children)} has no receipt", file=sys.stderr)
                return 1
            checked = dc.validate_child_result_manifest(manifest)
            if checked["child_ordinal"] != ordinal:
                raise ManifestError(
                    f"child {ordinal}'s receipt carries a result manifest for "
                    f"ordinal {checked['child_ordinal']}"
                )
            manifests.append(manifest)
            child_evidence.append({
                "action_key": child_keys[ordinal],
                "receipt_sha256": dc.document_sha256(child_receipt),
            })
        receipt = {
            **dc.verify_exact_cover(request, plan, manifests),
            "children": child_evidence,
        }
        path = publish_group_receipt(
            receipt, cas=cas, parent_key=plan["parent_key"])
    except (ManifestError, pb.ActionContractError, pb.CASTamperError) as exc:
        print(f"pbcampaign: {exc}", file=sys.stderr)
        return 1
    print(f"pbcampaign: group receipt "
          f"{dc.document_sha256(receipt)[:pbwait.KEY_WIDTH]} at {path}: "
          f"{receipt['task_count']} tasks across {receipt['child_count']} "
          f"children, merged into "
          f"{receipt['merged_result_sha256'][:pbwait.KEY_WIDTH]}",
          file=sys.stderr, flush=True)
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Submit a manifest of actions to the fleet and wait."
    )
    ap.add_argument("--wait-s", type=float, default=86400.0,
                    help="how long to wait for ALL the rows, not for each")
    ap.add_argument(
        "--transport", choices=pbrun.TRANSPORTS,
        default=fleet_submit.default_transport(),
        help="which dispatcher carries every row (env PRISMABUILD_TRANSPORT, "
             "else the published runtime generation's default_transport); "
             "forwarded to pbrun unchanged. It is one flag and not a row "
             "field because the transport is a fact about the fleet, not "
             "about the work")
    ap.add_argument("--priority", type=int, default=0,
                    help="queue hint for a decomposed request's children, "
                         "higher runs sooner; not part of an action's "
                         "identity.  A row in a list manifest carries its own "
                         "priority field, and this does not override it")
    ap.add_argument("--detach", action="store_true",
                    help="print each row's submission line and return without "
                         "waiting; wait for them later with pbwait.py")
    ap.add_argument(
        "--require-data-manifest", action="store_true",
        help="refuse the whole manifest, before any row is submitted, unless "
             "every row (and a logical request's common half) carries a "
             "nonblank data_manifest.  Use it when the shared-mount reads this "
             "tool cannot see still need declaring; the default is only a "
             "warning about paths a row visibly names under the shared mount")
    ap.add_argument("--max-inflight", type=int,
                    help="publish at most N unfinished distinct actions from "
                         "this controller at once; requires waiting pool mode")
    ap.add_argument("manifest", help="JSON list of rows; see the module docstring")
    args = ap.parse_args(argv)
    if args.max_inflight is not None:
        if args.max_inflight < 1:
            ap.error("--max-inflight must be at least 1")
        if args.detach or args.transport != "pool":
            ap.error("--max-inflight requires waiting pool mode (no --detach)")
        if not math.isfinite(args.wait_s) or args.wait_s < 0:
            ap.error("--max-inflight requires a finite nonnegative --wait-s")

    try:
        rows = load_manifest(
            args.manifest, transport=args.transport,
            require_data_manifest=args.require_data_manifest)
    except ManifestError as exc:
        raise SystemExit(f"pbcampaign: {exc}")
    group = None
    if isinstance(rows, dict):
        if args.max_inflight is not None:
            ap.error("--max-inflight is not supported for logical requests")
        # One request, one refusal: unlike forty rows, there is nothing else
        # to go on with, so a refusal here is the campaign's.
        try:
            submissions, group = decompose(
                rows, transport=args.transport, priority=args.priority)
        except (ManifestError, pb.ActionContractError) as exc:
            raise SystemExit(f"pbcampaign: {exc}")
    else:
        if not rows:
            raise SystemExit("pbcampaign: the manifest has no rows")
        if args.max_inflight is not None:
            submissions, waited = run_windowed(
                rows, transport=args.transport, max_inflight=args.max_inflight,
                wait_s=args.wait_s)
        else:
            submissions = submit(rows, transport=args.transport)
    refused = [one for one in submissions if one.get("status") == "refused"]
    for one in refused:
        print(f"pbcampaign: {one.get('error')}\n"
              f"  pbrun {' '.join(one.get('flags') or [])}",
              file=sys.stderr)

    if args.detach:
        for published in submissions:
            if published.get("status") != "refused":
                payload = dict(published)
                payload.pop("flags", None)
                print(json.dumps(payload, sort_keys=True), flush=True)
        return 1 if refused else 0

    if args.max_inflight is not None:
        table = rows_for(submissions, waited)
        print(pbwait.render(table))
        # An unpublished suffix is unfinished work, not a failed action.
        verdict_rows = [dict(row, status="waiting")
                        if row["status"] == "not_submitted" else row for row in table]
        return 1 if refused else pbwait.verdict(verdict_rows)

    # ``attached`` is a row that was already running when the campaign was
    # re-run: there is a job to wait for, it is just not this run's job.
    submitted = [one for one in submissions
                 if one.get("status") in {"submitted", "attached"}]
    keys = [str(one["action_key"]) for one in submitted]
    # The generation each row was submitted under, taken from what pbrun
    # printed rather than read back off the queue.  Reading it back is a race
    # against a worker that claims and finishes the item first, and the cost of
    # losing it is reporting an older run's ending for this row.
    generations = {
        str(one["action_key"]): one.get("published_unix")
        for one in submitted
        if isinstance(one.get("published_unix"), (int, float))
    }
    queue = pool.PoolQueue(pbrun.SH / "pb-queue")
    cas = pb.PrismaBuildCAS(pbrun.SH / "cas")
    waited = pbwait.wait_for_keys(
        queue, keys, cas=cas, wait_s=args.wait_s, generations=generations)
    table = rows_for(submissions, waited)
    print(pbwait.render(table))
    verdict = 1 if refused else pbwait.verdict(table)
    if group is not None:
        # Every child passing is not yet the request being answered: the cover
        # is the claim a decomposed campaign actually makes, so it decides the
        # exit status too.  ``--detach`` never reaches here, and rightly: it
        # returns before anything has run, so there is no cover to prove.
        verdict = max(verdict, close_group(group, cas=cas))
    return verdict


if __name__ == "__main__":
    raise SystemExit(main())

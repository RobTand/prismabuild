"""Say that an action has committed work, from anywhere that action runs.

The worker side of this is #481: an action that declares phases is bounded by
whether it is still committing units, not by how long it has run.  This is the
side the *application* owes, and the reason it is a module of its own is that
the application is usually not a PrismaBuild process.  It is a pytest shard
under `/home/rob/venvs/pb-cpu/bin/python`, a pricing row inside a pinned
producer image, or a shell loop -- and `import prismabuild` fails in all
three, because the package is on the shared mount rather than installed.  The
first consumer of the contract wrote its own copy of the record against the
wire format for exactly that reason (prismaquant `prismabuild_progress.py`),
which is one copy of a versioned schema too many.

So this file is deliberately a leaf: standard library only, no intra-package
imports, no side effects at import.  That makes all four ways of reaching it
the same code the worker will read:

* installed or on `sys.path` --- `from prismabuild.progress import commit`;
* by path, any interpreter, no install ---
  `runpy.run_path(os.environ["PRISMABUILD_ACTION_PROGRESS_HELPER"])["commit"]`;
* from a shell or any other language ---
  `python3 "$PRISMABUILD_ACTION_PROGRESS_HELPER" --phase run --units 37`;
* and, where the fleet's mount is not visible at all (a container), the ten
  lines in `skills/prismabuild/SKILL.md`, which a test holds to this file's
  record byte for byte.

`core` imports the names below rather than defining its own, so there is one
definition of the schema and one writer behind every spelling.
"""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import sys
import time

#: The record schema the worker's ``ProgressWatch`` accepts.  Versioned with
#: the placement tag it travels under (``progress-v1``): a new record format
#: is a new tag, so an old worker never claims work whose reports it would
#: reject as foreign and then kill for the silence.
PROGRESS_RECORD_SCHEMA_V1 = "prismabuild.action_progress.v1"

#: A report is metadata, never a checkpoint payload.  Bound both accepted
#: bytes and parser work; the watcher uses the existing stable regular-file
#: reader.
MAX_ACTION_PROGRESS_BYTES = 64 * 1024

#: Where an action that declares the progress contract reports semantic
#: advancement, and the per-launch token it must echo back.
#:
#: These reach the action's own environment, which almost nothing else the
#: worker knows does: the whole point is a fact the *application* knows and
#: the launcher does not.  An action that seals either name itself is a
#: refusal rather than an overwrite, exactly as the profile contract already
#: refuses one.
#:
#: The token is minted per LAUNCH rather than per action key.  Unlinking the
#: file before the launch is not enough on its own: an action that outlived
#: SIGKILL on a previous attempt (a D-state GPU wedge does) keeps the same
#: path open and would replay its counter into the next attempt's watchdog.
#: A token it cannot know is what makes accepted advancement this attempt's.
ACTION_PROGRESS_PATH_ENV = "PRISMABUILD_ACTION_PROGRESS_PATH"
ACTION_PROGRESS_TOKEN_ENV = "PRISMABUILD_ACTION_PROGRESS_TOKEN"

#: The phases the submitter sealed, in the order they were declared, as a JSON
#: array of names.  Advisory to the worker -- it enforces the sealed policy it
#: holds, not this copy -- and load-bearing to the action: it is what lets
#: ``commit`` default the phase, and what turns a mistyped phase from a run
#: that quietly reports nothing acceptable and dies at its stall bound into a
#: ``ValueError`` on the first commit.
ACTION_PROGRESS_PHASES_ENV = "PRISMABUILD_ACTION_PROGRESS_PHASES"

#: The absolute path of this file inside the runtime generation that launched
#: the action.  Exported so an action that cannot import the package can still
#: run the same writer rather than reimplement the record: same generation,
#: same schema, same atomic write as the worker that will read it.
ACTION_PROGRESS_HELPER_ENV = "PRISMABUILD_ACTION_PROGRESS_HELPER"

#: Every variable the contract puts in an action's environment.  ``core``
#: forwards exactly these and refuses an action that seals any of them.
ACTION_PROGRESS_ENV = (
    ACTION_PROGRESS_PATH_ENV,
    ACTION_PROGRESS_TOKEN_ENV,
    ACTION_PROGRESS_PHASES_ENV,
    ACTION_PROGRESS_HELPER_ENV,
)


def channel() -> tuple[str, str] | None:
    """The path and token this launch reports on, or ``None`` if it has none.

    ``None`` is the ordinary case for every action that did not declare the
    contract, which is why every entry point here is a no-op rather than an
    error then: application code must not have to know how it was launched.
    """

    destination = os.environ.get(ACTION_PROGRESS_PATH_ENV) or ""
    token = os.environ.get(ACTION_PROGRESS_TOKEN_ENV) or ""
    if not destination or not token:
        return None
    return destination, token


def declared_phases() -> tuple[str, ...] | None:
    """The phase names the submitter sealed, or ``None`` if they are unknown.

    Unknown means either no contract or a worker generation that predates the
    export.  Both leave ``commit`` unable to default or check a phase name, and
    it says so rather than guessing one.
    """

    raw = os.environ.get(ACTION_PROGRESS_PHASES_ENV) or ""
    if not raw:
        return None
    try:
        names = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(names, list) or not names:
        return None
    if not all(isinstance(name, str) and name for name in names):
        return None
    return tuple(names)


def commit(
    units_completed: float, phase: str | None = None, *, unit: str | None = None
) -> bool:
    """Report that ``units_completed`` units are **durably** committed.

    Call it after the work is on disk -- a checkpoint shard written, an anchor
    journalled, a shard published -- never on entering a loop iteration.  The
    watchdog exists to tell a long run from a stuck one, and a counter that
    ticks on intent rather than on commitment cannot.

    ``units_completed`` is cumulative across the whole run and across phases: a
    resumed action continues from what its checkpoint already holds rather than
    restarting at zero.  A repeated or regressing count is not advancement and
    buys no time.  Whole numbers keep their precision, so a count past 2**53 is
    still exact.

    ``phase`` defaults to the first phase the submission declared.  A name the
    submission did not declare raises ``ValueError`` here, where the typo is,
    rather than being refused silently by the worker until the stall allowance
    runs out -- but only when the launching worker published the phase list;
    an older generation leaves it unknown and then ``phase`` is required.

    Returns whether a record was written: ``False`` when this action was not
    admitted under the contract, or when the queue directory could not be
    written.  A box that cannot write to its own queue reads as a stall, which
    is the honest verdict rather than a reason to fail the action.
    """

    open_channel = channel()
    if open_channel is None:
        return False
    destination, token = open_channel
    if (type(units_completed) not in (int, float)
            or (type(units_completed) is float
                and not math.isfinite(units_completed))
            or units_completed < 0):
        raise ValueError("units_completed must be a finite, non-negative number")
    phase = _resolve_phase(phase)
    record: dict[str, object] = {
        "schema": PROGRESS_RECORD_SCHEMA_V1,
        "token": token,
        "phase": phase,
        "units_completed": units_completed,
        "reported_unix": time.time(),
    }
    if unit is not None:
        record["unit"] = str(unit)
    return _write(Path(destination), record)


def _resolve_phase(phase: str | None) -> str:
    declared = declared_phases()
    if phase is None:
        if declared is None:
            raise ValueError(
                "phase is required: this action's launcher did not publish "
                f"{ACTION_PROGRESS_PHASES_ENV}, so there is no declared phase "
                "to default to"
            )
        return declared[0]
    phase = str(phase)
    if declared is not None and phase not in declared:
        # Loud here, because the alternative is quiet for the whole stall
        # allowance and then a termination that reads as a stall: the worker
        # refuses an undeclared phase, and an action reporting nothing it will
        # accept is indistinguishable from one reporting nothing at all.
        raise ValueError(
            f"phase {phase!r} is not one this action declared "
            f"({', '.join(declared)})"
        )
    return phase


def _write(path: Path, record: dict[str, object]) -> bool:
    """Replace the report with this one, atomically, or say it did not land.

    Whole-record replacement rather than a merge, and a file of its own rather
    than the launcher's status sidecar: that sidecar is an unlocked
    read-merge-write unlinked as it is read, which is fine for two facts
    written once at an ending and not for a writer ticking every few seconds.
    """

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.parent / f".{path.name}.{os.getpid()}.tmp"
        temporary.write_text(
            json.dumps(record, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, path)
    except OSError:
        return False
    return True


def main(argv: list[str] | None = None) -> int:
    """Commit from a shell, or from any language that can run a process.

        python3 "$PRISMABUILD_ACTION_PROGRESS_HELPER" --phase encode --units 37

    Exit 0 whether or not a record was written, for the same reason ``commit``
    returns ``False`` rather than raising: an action must not fail because it
    could not describe itself.  ``--require-channel`` inverts that for a
    caller testing its own wiring.
    """

    parser = argparse.ArgumentParser(description="Report committed units to PrismaBuild.")
    parser.add_argument("--units", type=float, required=True,
                        help="cumulative units durably committed so far")
    parser.add_argument("--phase", default=None,
                        help="which declared phase they belong to "
                             "(default: the first phase declared)")
    parser.add_argument("--unit", default=None,
                        help="what a unit is, for the receipt (e.g. anchors)")
    parser.add_argument("--require-channel", action="store_true",
                        help="exit 1 when this action has no progress channel")
    args = parser.parse_args(argv)
    units: float = args.units
    if units.is_integer():
        # ``--units 37`` is thirty-seven things, not 37.0 of them, and the
        # worker keeps integer counts exact past 2**53.
        units = int(units)
    try:
        written = commit(units, args.phase, unit=args.unit)
    except ValueError as exc:
        print(f"prismabuild.progress: {exc}", file=sys.stderr)
        return 2
    if not written and args.require_channel:
        print("prismabuild.progress: this action has no progress channel",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

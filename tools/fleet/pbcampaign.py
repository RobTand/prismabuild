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

Manifest schema
---------------

A manifest is a JSON list of rows.  Every field is optional except ``argv``,
and each one is exactly one ``pbrun`` flag:

===================  ====================================================
``argv``             the command, as a list; the part after ``pbrun --``
``cwd``              ``--cwd``: the Git checkout to seal (default: here)
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
``priority``         ``--priority``
===================  ====================================================

Every field except ``argv`` is optional, and an omitted one is not passed to
``pbrun`` at all, so the row inherits whatever ``pbrun`` decides.  ``timeout_s``
is the one to be deliberate about: omitting it means no deadline, which is
what a long stage that is making progress wants, and setting it means the
scheduler kills the row at that many seconds whatever it was doing.

An unknown field is refused rather than ignored: a typo that is silently
dropped seals an action nobody asked for.

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
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import core as pb, pool  # noqa: E402

import pbrun  # noqa: E402
import pbwait  # noqa: E402

#: Row field to ``pbrun`` flag, and how the value is spelled.  A table rather
#: than a chain of ifs, because the property that matters is that the mapping
#: is mechanical: a row is a ``pbrun`` command line and nothing else.
_VALUE_FIELDS = (
    ("cwd", "--cwd"),
    ("timeout_s", "--timeout-s"),
    ("gpu_capacity", "--gpu-capacity"),
    ("priority", "--priority"),
)
_SWITCH_FIELDS = (
    ("deterministic", "--deterministic"),
    ("anywhere", "--anywhere"),
    ("here", "--here"),
    ("no_default_env", "--no-default-env"),
    ("exclusive", "--exclusive"),
)
_REPEATED_FIELDS = (
    ("tags", "--tag"),
    ("snapshot_ref", "--snapshot-ref"),
)
KNOWN_FIELDS = frozenset(
    {"argv", "demand", "env"}
    | {name for name, _ in _VALUE_FIELDS}
    | {name for name, _ in _SWITCH_FIELDS}
    | {name for name, _ in _REPEATED_FIELDS}
)


class ManifestError(Exception):
    """The manifest says something this cannot turn into a submission."""


def load_manifest(path) -> list[dict]:
    """Read the manifest, and refuse anything it cannot mean.

    Refused at load time, before a single row is sealed: a campaign that
    submits forty rows and then discovers the forty-first is malformed has
    already spent the fleet on a manifest its author has to edit.
    """

    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise ManifestError(f"cannot read the manifest: {exc}") from None
    except ValueError as exc:
        raise ManifestError(f"the manifest is not JSON: {exc}") from None
    if not isinstance(value, list):
        raise ManifestError(
            f"a manifest is a JSON list of rows, not {type(value).__name__}"
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


def submit_row(row, *, transport: str = "") -> dict:
    """Seal and submit one row through ``pbrun``, and return what it printed.

    A row that ``pbrun`` refuses is recorded and the campaign goes on.  Forty
    rows are not worth losing to the one that named a checkout that is not
    there, and the refusal reaches the operator on the table with the rest.
    """

    flags = ["--detach"]
    if transport:
        flags += ["--transport", transport]
    flags += pbrun_argv(row)
    saved = sys.argv
    captured = io.StringIO()
    sys.argv = ["pbrun.py", *flags]
    code, refusal = 0, ""
    try:
        with contextlib.redirect_stdout(captured):
            code = pbrun.main()
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


def submit(rows, *, transport: str = "") -> list[dict]:
    """Submit every row, in order, and return one submission record each."""

    submissions = []
    for index, row in enumerate(rows):
        published = submit_row(row, transport=transport)
        key = str(published.get("action_key") or "")
        print(f"pbcampaign: row {index} {published['status']} "
              f"{key[:pbwait.KEY_WIDTH] or '-'}", file=sys.stderr, flush=True)
        submissions.append(published)
    return submissions


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
        if status in {"submitted", "attached"} and key in by_key:
            table.append(by_key[key])
        elif status == "cache_hit":
            table.append({
                "action_key": key, "status": "cache_hit", "transport": "cas",
                "host": "-", "elapsed_s": None, "returncode": None,
                "receipt_published": True, "succeeded": True,
            })
        else:
            table.append({
                "action_key": key, "status": "refused",
                "transport": str(published.get("transport") or "-"),
                "host": "-", "elapsed_s": None, "returncode": None,
                "receipt_published": None, "succeeded": False,
            })
    return table


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Submit a manifest of actions to the fleet and wait."
    )
    ap.add_argument("--wait-s", type=float, default=86400.0,
                    help="how long to wait for ALL the rows, not for each")
    ap.add_argument(
        "--transport", choices=pbrun.TRANSPORTS,
        default=os.environ.get(pbrun.DEFAULT_TRANSPORT_ENV) or "pool",
        help="which dispatcher carries every row (env PRISMABUILD_TRANSPORT); "
             "forwarded to pbrun unchanged. It is one flag and not a row "
             "field because the transport is a fact about the fleet, not "
             "about the work")
    ap.add_argument("--detach", action="store_true",
                    help="print each row's submission line and return without "
                         "waiting; wait for them later with pbwait.py")
    ap.add_argument("manifest", help="JSON list of rows; see the module docstring")
    args = ap.parse_args(argv)

    try:
        rows = load_manifest(args.manifest)
    except ManifestError as exc:
        raise SystemExit(f"pbcampaign: {exc}")
    if not rows:
        raise SystemExit("pbcampaign: the manifest has no rows")

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
    return 1 if refused else pbwait.verdict(table)


if __name__ == "__main__":
    raise SystemExit(main())

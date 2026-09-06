"""Fan a test suite out across the pool instead of running it on one box.

The full suite is the coordinator's to run, and running it serially on a GB10
is the single largest avoidable load on a box that should be doing GPU work.
dl380g10 has 80 x86 cores sitting idle next to the same shared storage, so the
suite goes there in shards and the sparks keep their cores.

Three constraints shape this, and none of them are negotiable:

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
  at the call site rather than in a comment.
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
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import (  # noqa: E402
    fleet_tool, generation_root, tool_candidates,
)

RUNTIME_ROOT = generation_root(__file__)
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
_COUNTED = r"\d+ [A-Za-z][\w-]*"
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


def discover(checkout: Path, paths: list[str]) -> list[str]:
    """Test files under the given paths, relative to the checkout."""

    found: list[Path] = []
    for raw in paths:
        target = checkout / raw
        if target.is_dir():
            found.extend(sorted(target.rglob("test_*.py")))
        elif target.is_file():
            found.append(target)
    return [str(p.relative_to(checkout)) for p in dict.fromkeys(found)]


def shard(files: list[str], count: int) -> list[list[str]]:
    """Round-robin, so adjacent (and so similar) files land on different boxes."""

    count = max(1, min(count, len(files)))
    buckets: list[list[str]] = [[] for _ in range(count)]
    for index, name in enumerate(files):
        buckets[index % count].append(name)
    return buckets


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--checkout", required=True,
                    help="Git tree to snapshot and test on the pool")
    ap.add_argument("--python", required=True,
                    help="interpreter on the TARGET box, not this one")
    ap.add_argument("--tag", action="append", default=[],
                    help="placement tag; defaults to x86")
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
    ap.add_argument("--timeout-s", type=float, default=None,
                    help="an explicit deadline for each shard; unset means none")
    ap.add_argument("--wait-s", type=float, default=10800.0,
                    help="how long each shard waits for the fleet to run it, "
                         "queueing included; forwarded to pbrun")
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
    files = discover(checkout, args.paths or ["tests"])
    if not files:
        sys.stderr.write(f"no test files under {args.paths} in {checkout}\n")
        return 2
    buckets = shard(files, args.shards)
    tags = args.tag or ["x86"]
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
        if args.timeout_s is not None:
            flags += ["--timeout-s", str(args.timeout_s)]
        flags += ["--wait-s", str(args.wait_s)]
        command = flags + [
            "--", "env", "TMPDIR=/home/rob/tmp",
            *threads,
            "PYTHONPATH=src:experiments",
            args.python, "-m", "pytest", "-q", "--no-header",
            "-p", "no:cacheprovider", *pytest_workers, *bucket,
        ]
        procs.append((index, bucket, subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)))

    results = []
    for index, bucket, proc in procs:
        out, _ = proc.communicate()
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
            rc = proc.returncode
            how = f"signal {-rc}" if rc < 0 else f"rc={rc}"
            summary = (f"NO PYTEST SUMMARY -- {len(bucket)} file(s) did not run "
                       f"(the shard ended {how}, before or outside pytest)")
        results.append({"shard": index, "files": bucket,
                        "returncode": proc.returncode, "summary": summary,
                        "ran": ran, "output": out})
        state = "ok" if proc.returncode == 0 else f"rc={proc.returncode}"
        print(f"shard {index:>3} {state:<8} {summary}", flush=True)

    # A shard is green when it exited 0 AND pytest reported a terminal summary.
    # ``ran`` has been computed, printed and written to the JSON since #213, and
    # the verdict never read it: ``returncode != 0`` alone called a shard that
    # started no test green, and returned 0 to whoever was deciding a merge on
    # it.  That is the #208 defect one level in -- the diagnostic improved and
    # the thing acting on it did not read the diagnostic.
    failed = [r for r in results if r["returncode"] != 0 or not r["ran"]]
    print(f"\n{len(results) - len(failed)}/{len(results)} shards green")
    for r in failed:
        print(f"\n--- shard {r['shard']} ({', '.join(r['files'])})")
        print("\n".join((r["output"] or "").strip().splitlines()[-25:]))
    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=1))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

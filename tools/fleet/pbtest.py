"""Fan a test suite out across the pool instead of running it on one box.

The full suite is the coordinator's to run, and running it serially on a GB10
is the single largest avoidable load on a box that should be doing GPU work.
dl380g10 has 80 x86 cores sitting idle next to the same shared storage, so the
suite goes there in shards and the sparks keep their cores.

Three constraints shape this, and none of them are negotiable:

* **The checkout must be on shared storage.**  ``pbrun`` pins an action to the
  submitting box unless ``--anywhere``, because an agent worktree exists on one
  box only.  A cross-box shard is correct exactly when its tree is visible from
  both ends, which is what ``--checkout`` is checked for.
* **The interpreter is named, not inherited.**  An action runs in a closed
  environment, so its interpreter is sealed into its command and hence its
  action key.  A shard therefore names an interpreter that exists on its target
  and carries the matching class tag; the two must agree.
* **A pass/fail here is not a measurement.**  x86 against aarch64 is a
  different BLAS and a different FMA order, so this runs *tests*, never a
  timing or numeric arm.  ``--tag`` defaults to ``x86`` to make that explicit
  at the call site rather than in a comment.

Shards are round-robin by file, which balances only if files cost roughly the
same.  They do not -- but the alternative is a duration model nobody has
measured, and an unbalanced shard costs wall-clock while a wrong one costs
trust.  The imbalance is reported so it can be seen rather than assumed.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
PBRUN = RUNTIME_ROOT / "tools" / "pbrun.py"
SHARED = Path("/mnt/shared")


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
                    help="tree to test; must be on /mnt/shared to run off-box")
    ap.add_argument("--python", required=True,
                    help="interpreter on the TARGET box, not this one")
    ap.add_argument("--tag", action="append", default=[],
                    help="placement tag; defaults to x86")
    ap.add_argument("--shards", type=int, default=20)
    ap.add_argument("--threads-per-shard", type=int, default=2,
                    help="BLAS/OMP threads each shard may use; 0 leaves it alone")
    ap.add_argument("--mem-gb", type=int, default=3,
                    help="memory each shard demands of its box")
    ap.add_argument("--timeout-s", type=float, default=3600.0)
    ap.add_argument("--wait-s", type=float, default=10800.0)
    ap.add_argument("--json", default="", help="write the per-shard result here")
    ap.add_argument("paths", nargs="*", default=["tests"])
    args = ap.parse_args()

    checkout = Path(args.checkout).resolve()
    if SHARED not in checkout.parents:
        sys.stderr.write(
            f"refusing: {checkout} is not under {SHARED}, so a worker on "
            "another box cannot see it.  Clone the tree to shared storage "
            "first -- a shard against a box-local path would run nowhere, or "
            "worse, run against a different tree of the same name.\n")
        return 2

    files = discover(checkout, args.paths or ["tests"])
    if not files:
        sys.stderr.write(f"no test files under {args.paths} in {checkout}\n")
        return 2
    buckets = shard(files, args.shards)
    tags = args.tag or ["x86"]
    sizes = [len(b) for b in buckets]
    print(f"{len(files)} files -> {len(buckets)} shards "
          f"(min {min(sizes)}, max {max(sizes)} files per shard), tags={tags}",
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

    procs = []
    for index, bucket in enumerate(buckets):
        command = [
            "/usr/bin/python3", str(PBRUN),
            "--cwd", str(checkout),
            "--anywhere",
            "--demand", f"mem_gb={args.mem_gb}",
            "--timeout-s", str(args.timeout_s),
            "--wait-s", str(args.wait_s),
            "--", "env", "TMPDIR=/home/rob/tmp",
            *threads,
            f"PYTHONPATH={checkout}/src:{checkout}/experiments",
            args.python, "-m", "pytest", "-q", "--no-header",
            "-p", "no:cacheprovider", *bucket,
        ]
        # Insert after "--anywhere" (index 4), never inside a flag/value pair:
        # index 6 sat between "--demand" and its argument and every shard died
        # on "expected one argument".
        for tag in tags:
            command[5:5] = ["--tag", tag]
        procs.append((index, bucket, subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)))

    results = []
    for index, bucket, proc in procs:
        out, _ = proc.communicate()
        tail = [line for line in (out or "").strip().splitlines() if line.strip()]
        summary = next((l for l in reversed(tail)
                        if " passed" in l or " failed" in l or " error" in l), "")
        results.append({"shard": index, "files": bucket,
                        "returncode": proc.returncode, "summary": summary,
                        "output": out})
        state = "ok" if proc.returncode == 0 else f"rc={proc.returncode}"
        print(f"shard {index:>3} {state:<8} {summary}", flush=True)

    failed = [r for r in results if r["returncode"] != 0]
    print(f"\n{len(results) - len(failed)}/{len(results)} shards green")
    for r in failed:
        print(f"\n--- shard {r['shard']} ({', '.join(r['files'])})")
        print("\n".join((r["output"] or "").strip().splitlines()[-25:]))
    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=1))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

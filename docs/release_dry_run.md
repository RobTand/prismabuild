# Release dry-run

This command proves local release packaging. It creates no remote tag, package
upload, artifact upload, deployment, or release. It needs no release credentials.
The source repository stays unchanged.

Run it through PrismaBuild from a clean, committed worktree. The worker needs
Python 3.12 or newer, setuptools 68 or newer, and wheel already installed. The
absolute interpreter names that prerequisite. The command installs no tools.
For development verification, submit both controls at priority -10:

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
  --cwd /path/to/clean/prismabuild --cpus 1 --demand mem_gb=4 \
  --priority -10 --timeout-s 600 \
  --env OMP_NUM_THREADS=1 --env OPENBLAS_NUM_THREADS=1 --env MKL_NUM_THREADS=1 \
  -- /home/rob/venvs/pb-cpu/bin/python tools/ci/release_dry_run.py v0.1.0

python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
  --cwd /path/to/clean/prismabuild --cpus 1 --demand mem_gb=4 \
  --priority -10 --timeout-s 600 \
  --env OMP_NUM_THREADS=1 --env OPENBLAS_NUM_THREADS=1 --env MKL_NUM_THREADS=1 \
  -- /home/rob/venvs/pb-cpu/bin/python tools/ci/release_dry_run.py v999999.0.0
```

The valid candidate exits 0 and prints a PASS report with the wheel hash and
file count. Verify its terminal record, log and CAS receipt. The invalid
candidate exits 1 and prints a FAIL report whose reason says the candidate does
not match the project version. It publishes no CAS receipt. Inspect its failed
terminal and log to confirm that intended refusal. A submission acknowledgement
alone proves neither result. For detached submissions, retain the action keys
and wait with the published `pbwait.py`.

Use the stable `vMAJOR.MINOR.PATCH` tag that matches `pyproject.toml`.
`--revision` selects a committed source revision and defaults to `HEAD`. Dirty
files are not included. Reports identify the resolved source SHA and candidate.
An optional `--report` path writes JSON on the executing worker; stdout also
retains the report in the PrismaBuild action log.

The command exports the commit into a temporary repository without remotes,
creates a local tag there, and builds a wheel with the installed setuptools
backend. Its subprocess environment excludes inherited credentials and Python
path overrides. It checks the wheel's package, CLI entry point, name, version,
and complete SHA-256 RECORD inventory. The temporary repository, tag and wheel
are deleted on exit.

PB's CI runs as GitHub Actions checks on GitHub-hosted runners, and Mergify is
its merge queue. This source unit supplies no release workflow.
The commands above are agent development checks submitted through PrismaBuild.
They establish no integrated CI, runner readiness, queue authentication, or
permission to release. Independent review and the separate queue and provenance
requirements of #2406 remain required.

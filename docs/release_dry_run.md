# Release dry-run

This is a local packaging proof, not permission to release. It does not publish,
upload packages or artifacts, deploy, or push tags. It needs no token or release
credentials. The source repository stays unchanged.

From an isolated worktree, with Python 3.12 or newer, setuptools 68 or newer and
wheel already installed, run:

```bash
python3 tools/ci/release_dry_run.py v0.1.0 --report /tmp/release-good.json
python3 tools/ci/release_dry_run.py v999999.0.0 --report /tmp/release-bad.json
```

The first command must exit 0. The second must exit 1 with a candidate-version
mismatch. Use the stable `vMAJOR.MINOR.PATCH` tag that matches `pyproject.toml`.
`--revision` selects a committed source revision and defaults to `HEAD`. Dirty
files are not included. Reports identify the resolved source SHA and candidate.

The command exports that commit into a temporary repository without remotes,
creates a local-only tag there, and builds a wheel with the local setuptools
backend. Its subprocess environment excludes inherited credentials and Python
path overrides. It checks the wheel's package, CLI entry point, name, version,
and complete SHA-256 RECORD inventory. The temporary repository, tag and wheel
are deleted on exit. Only the requested JSON report remains.

The manual `Release dry-run` workflow runs only from `main`, on
`[self-hosted, linux, x64, fleet]`. It fetches the exact public source SHA without
authentication and grants no GitHub token permissions. The runner must already
have the build tools. A missing prerequisite fails the job; the workflow does
not install tools, submit CI to PrismaBuild, or receive release credentials.
Both candidate results appear in the job summary. No artifact-upload step runs.

Local reports are development evidence. They do not prove that an integrated
SHA passed GitHub checks, that a fleet runner is ready, or that a queue can merge.
Manager approval, independent review, enforceable protections and the separate
queue design gate still apply. Source: RobTand/prismabuild#2406 and Paperclip
PRI-1057.

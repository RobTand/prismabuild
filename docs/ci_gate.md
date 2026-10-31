# CircleCI merge gate

Ordinary branch pushes run `pr-gate` on one `robtand/fast-arm64` slot. The
selector always includes FAST, changed tests, direct importers of changed
production modules, path references, and test helper import closure. Static path
resolution follows named directory bindings, __file__ parents, test-relative
paths, Path constructors, joinpath and os.path joins. A computed filename
retains its known directory as a conservative match; an unresolved base can
retain a literal suffix. These matches also form helper-closure edges.
Changing conftest, pyproject, verify, lane manifests or gate code/config selects
the full population. Documentation alone does not.

Mergify queue and `gate-canary/*` branches run the **full eligible population**.
`queue-gate-heavy` uses two ARM slots and `queue-gate-light` uses two x64 slots.
Files with summed inventory medians of at least 5 seconds, or unknown duration,
go to heavy. CircleCI splits each lane by filename timings. Coverage is never
truncated to meet a time budget. The threshold is provisional pending live trials.
These stable job names publish the GitHub statuses required by Mergify.

Each unprivileged slot has four CPUs and 6 GiB. Setup uses uv's private cache,
prefers Python 3.14, and falls back to image Python >=3.12. Python installation
and environment setup each have at most three attempts. Dependencies are
pytest 9.1.1, pytest-xdist 3.8.0 and PyYAML 6.0.3. Native threads are bounded to
one, xdist uses four workers within inherited affinity, and each test has the
repository's 60-second bound. No shared fleet mount, secrets or contexts are used.

## Plans, verdicts and timings

The archived `gate-artifacts/plan.json` records exact base/head SHAs, reasons,
population version, quarantine and duration provenance. Base is the merge base
of the fetched main and checkout. Queue plans include `shadow_selected`, the
selection that would have run. Queue gating remains full until batch evidence
qualifies selection. A single run with no missed failure proves only that run.

The runner is usable from any CI:

```sh
python tools/ci/gate_select.py --base "$BASE_SHA" --head "$HEAD_SHA" --mode full \
  --output plan.json --files-output files.txt
python tools/ci/gate_run.py --plan plan.json --files-from assigned.txt --workers 4 \
  --result result.json --junitxml results/gate.xml
```

Exit 0 means pass, 1 means test failure, and 75 means no verdict. Logs distinguish
`GATE_PASS`, `GATE_TEST_FAILURE` and `NO_VERDICT`. Setup and planning failures exit 75. The shell preserves the runner's explicit
0, 1 and 75 without treating a test failure as a planning error. Lost runners cannot self-retry
without credentials; Mergify owns the bounded check retry. Setup failure, lost
runner or absent status must never become a pass. JUnit uses xunit1 with `file`
attributes for CircleCI's `store_test_results` timing history. Plan, assignment,
result and JUnit are also artifacts on failed runs.

Download all four queue nodes' final result/JUnit artifacts, retain one identical
full plan, and compare shadow selection with complete full-run failures:

```sh
python tools/ci/gate_shadow.py --plan plan.json --results node*/result.json \
  --junitxml node*/gate.xml --output shadow-accuracy.json
```

The audit returns 0 for no missed observed failure, 1 for a missed failure, or
75 for incomplete evidence. It requires complete, unique assignments, actual
JUnit file coverage and matching population versions. Fully skipped or empty
files can make evidence inconclusive, which requires inspection rather than
an accuracy claim.

## Population and quarantine

The population excludes root, GPU and dedicated two-CPU lanes declared by
`tools/verify.py` and the proof/receipt manifests. It also excludes whole files
in `gate_quarantine.json` and deselects its node-prefix entries. Every quarantine
entry needs an open owning issue and a specific reason. Readiness tracker #2428
is not an owner. Historical entries are provisional under #2434; archival replay
stays under #1997. Inventory evidence and per-test action keys accompany new
observed flaky/broken entries. Filesystem-floor skip/pass variation is not a flake.

Add an entry only with evidence and an open owner. Remove it after root repair
or five fresh passing repeats on each architecture in the CI container, as the
owning issue requires. Quarantine is containment, not a root fix. Durations in
`gate_durations.json` are summed per-test medians from inventory 22e9d752.
Unknown new-file durations stay unknown; partly quarantined files retain their
whole-file estimate conservatively. Replay and AST function diagnostics are
available in `tools/ci/gate_replay.py`; function-level selection is follow-up work.

## Path-loading survey

```sh
python tools/ci/gate_paths.py --root . --output path-survey.json
```

The survey counts source AST idioms and distinct files, with examples. Counts
overlap and include temporary paths and generated fixture output; they do not
prove dependency completeness. Path-loaded modules through SourceFileLoader or
runpy use the same argument evidence as importlib loaders. with_suffix and
filesystem glob operations keep conservative directory matches. Runtime paths
received only through arguments, environment or arbitrary helper return values
can remain unresolved when there is no static repository directory or suffix.
Queue coverage therefore stays full while real-batch shadow evidence accumulates.

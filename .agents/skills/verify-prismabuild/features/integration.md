# CPU integration

## Sub-features

Worker execution, queue admission, retries, withdrawal, scratch ownership, broker adapters, and CLI compositions.

## How to get to it (user POV)

From a task checkout, run `python3 tools/verify.py integration`.

## Driving it with the CLI

Inspect `python3 tools/verify.py integration --plan`, then run the skill's integration command. Require `PASS`, `coverage_complete: true`, and `cas_verified: true`. Read the relevant test outcomes and skip reasons in `pbtest.json`.

## Gotchas

This is the remainder of the CPU suite, not the fast gate. Some files declare extra interpreter capabilities; PB retains those cohort fences. Scratch defaults to the worker's local disk because spool and durability tests cannot use tmpfs. Tests stay under the live-store audit guard and use private fixture queues; do not point them at live state.

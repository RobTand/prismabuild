# Fast CPU verdict

## Sub-features

Action-key compatibility, digest and CAS validation, data manifests, PB test selection, retry observations, and native thread declarations.

## How to get to it (user POV)

From a task checkout, run `python3 tools/verify.py fast`.

## Driving it with the CLI

Run the skill's doctor and fast command. Inspect `verdict.json` for `PASS`, `fast_under_600_s: true`, and `cas_verified: true`. Read the test counts and output in `pbtest.json`. Use `--plan` to inspect the exact file population without submitting.

## Gotchas

The 600-second budget includes queue and submission time. A slow queue can therefore fail the budget even when every test passes; retain that distinction in the review. This tier omits subprocess-heavy core and queue integration paths.

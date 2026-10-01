# Observer byte ownership validation

## Scope

Issue #1413 repairs the source guard regression introduced by #1408, separately
from #1411 and #1386 runtime adoption. It does not run an observer or change
capture, numerical, queue, profiling, networking or serving policy.

The observer uses existing Core `raw_sha256` for command, pool, profiler and
output-file bytes, and `_sorted_lf_bytes` for stdout. The latter preserves
Python's default ASCII escapes and NaN/Infinity spelling, spaces and one LF;
it is not compact canonical JSON. The same checkout supplies Core and CAS.
Insertion-ordered, indent-2 `capture.json`, archive construction and input ID
remain unchanged. No Core recipe or shrink-only baseline grows.

Exact name/path registry rows distinguish PID/source observation from canary
mapping validation, and kernel/control attribution from GPU PID/cgroup identity.
The helpers retain their original names and implementations.

## Evidence

- Clean main `2c792379d9a692ff71dc060fcb21f759d4cfb4c5` RED:
  `2b1ef3437b9a684205292673d598067d88380c3d10707ff68af3e66b8cbcb236`,
  dl380g10, 2 failed, 6 deselected, 0 skipped; 2 reconciled, 28.47 seconds.
- Guard and existing observer controls GREEN:
  `1b3e617ae197e4e3709a9018c0628309b23b54f0be8ca049b0f94ced8f382f14`,
  14 passed, 0 skipped; 57.65 seconds.
- Integrated GREEN:
  `4bdd268db4be581dcf0f8495184e2e3b81c7c7f030e30485bcb0b8946630492f`,
  109 passed, 0 failed, 0 skipped; 109 reconciled, 64.30 seconds.
  This includes 13 new exact-byte controls, 8 ownership guards, 45 existing
  Core recipe controls, 17 existing observer/analyzer controls and 26 canary
  identity/slot/deadline controls.

Terminal, immutable attempts, complete log hashes, successful CAS receipts and
results, and scope cleanup were independently verified. Source-only review
found no issues. Syntax and isolated cold `--help` bootstrap passed through PB:
`0a300d2b8f84108612f88bf4d4afce27fbe30011e47957abd50338c72c18f20e`,
`COMPILE_OK 2 files; COLD_HELP_OK`, with terminal/immutable/log/CAS verification.
The final committed-head gate remains pending at this record's creation.

## Limits

The private fixture runs actual old/new identity and main functions, the real
coverage analyzer and real CAS input publication. It compares complete report,
captured files and compressed archive bytes under identical paths and controlled
filesystem/gzip metadata. This does not make live archives deterministic or
establish an observation, execution receipt, benchmark, speedup or deployment.
The archival `.txt` fixture pins the original 8,381 bytes and SHA-256
`074564ba76e9e6f9a299f2c6174568bca3fc298f625ec1467cc35e4bfbab4f74`.

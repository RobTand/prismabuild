# Standards review — slice 3a #1942

Reviewed source head `7cec101154de517b7ded399ca2adaf8afc33f8de` against base `88b1a5108799040c4dbd87bfd628a82e44bf739b`, then reviewed the test-only delta through `6cef1e12efbb2610c8186fd2ab304bcfc1f55726`.

Standards: zero source or test findings. The refactor precedes behavior, namespace rules remain outside watcher traversal, and the helper rename migrates its callers. New tests use kernel events or fake clocks without sleeps or elapsed-time assertions.

The v2 fixture repair preserves refusal semantics. The partial case removes requested `done` coverage before capturing its cursor and coverage version, so it exercises incomplete coverage without relying on an unimplemented topic. The positive negotiation fixture explicitly covers only `done`, preserving the `tiers` coverage-refusal check after tier adapters become available. Existing reset, reason, no-events, wrong-queue and successful delivery assertions remain.

Before handoff, retain the required per-architecture touched-file qualification and terminal/log/CAS audit. This read-only review ran no tests, changed no implementation or test files, and grants no external approval.

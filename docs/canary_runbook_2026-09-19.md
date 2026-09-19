# pbcanary rollout gate runbook (issue #688, crew D)

The fleet canary (`pbcanary`, crew A) is a sealed self-verifying
mini-campaign submitted through the real interfaces with a fixed expected
outcome. This runbook covers only its **rollout gate**: how publication
invokes the driver, where the verdict is recorded, and what to do when it
fails. Leg design, verdict internals, CI scheduling, and the self-hosted
runner belong to the driver, workflow, and runner docs; the tracking issue
(#688) points at all of them.

## The gate

`tools/fleet/publish_runtime.py` accepts two flags:

*   `--canary` — after activation, submit the canary resolving against the
    new generation G and record `verified` or `failed` in G's rollout
    record. This is already the default, so the flag only forces a default
    in effect. A failed canary marks the record and exits nonzero. It never
    rolls back and never touches admission (campaign-primacy: the canary
    must not block or roll back real work).
*   `--no-canary` — skip the canary and record `not_run`. This is the
    escape hatch.

The gate is **default-ON** (`CANARY_DEFAULT_ENABLED = True` in
`publish_runtime.py`). A publication from a checkout carrying this default
runs the canary when neither flag is named, and `--no-canary` is the only
way to skip; a skip is still recorded as `not_run` so the absence of a
verdict is visible rather than silent. Phase 1 landed default-OFF with
`--canary` as the opt-in; the constant was flipped 2026-09-19 (phase 2)
after the first verified live 4-leg run: namespace
`pb-canary/20260919T173543Z`, exit 0, leg-4 envelopes bitwise-equal across
sparky+sparklina. The running fleet adopts the flipped default only when a
generation carrying it is published.

## The rollout record

Each generation's record is the sidecar
`/mnt/shared/prismabuild-fleet/runtime-generations/<generation>.canary.json`,
a sibling of the sealed generation directory (the generation itself is
read-only after publication, so the post-activation verdict cannot live
inside it). It carries:

```json
{
  "schema": "prismaquant.prismabuild.canary_status.v1",
  "generation": "<name>",
  "commit": "<40-hex>",
  "canary_status": "pending|verified|failed|not_run",
  "canary_exit": 0,
  "detail": "every leg executed and every receipt verified",
  "recorded_unix": 1788600000.0,
  "recorded_by": "<host>"
}
```

`pending` is written before the driver is invoked and rewritten to
`verified` or `failed` on its verdict. There is no separate error state:
a canary that could not verify G (missing driver, precondition refusal,
crash, return-shape drift) is `failed` with the reason in `detail`,
because "did not test" is never "passed".

Read it with:

```bash
cat /mnt/shared/prismabuild-fleet/runtime-generations/<generation>.canary.json
```

`supervise` ignores store children that carry no `RUNTIME_VERSION.json`,
so the sidecar is never mistaken for a generation.

## When the gate applies

*   Fresh publication, `--rollout rolling`: the canary runs after the
    inline activation. The canary hold extends the publication lock
    for the canary's duration (bounded ~15 min by the driver's wait
    budgets); plan publications accordingly.
*   Fresh publication, `--rollout barrier` (the default): the canary runs
    only after the barrier completes. A rolled-back or still-pending
    barrier writes no rollout record; the resume path owns that ending.
*   `--stage-only`: canary flags are refused (a staged generation is not
    live, so there is nothing to verify). Coordinator-activated
    generations are covered when the coordinator adopts the gate.
*   `--activate-generation` (rollback): canary flags are refused.
    Rollback restores bytes; it neither runs the canary nor rewrites the
    record. Re-verify by publishing, not by rolling back.
*   `--dry-run` reports the canary intent (`enabled`/`disabled`) and
    writes nothing.
*   An enabled canary with no driver present refuses **before** anything
    is published: the live runtime is untouched.

## On a failed canary

1.  Read the rollout record's `detail`, then the driver's per-leg summary
    in its run namespace (`/mnt/shared/prismabuild-fleet/pb-canary/<run-id>/`).
2.  The activation stands. Do not re-run publication to "clear" it; fix
    forward and publish a new generation (which gets its own canary).
3.  Rolling back to the previous generation with `--activate-generation`
    is a separate operator decision, never automatic. It leaves the
    failed record in place as history.

## Driver entry contract (confirmed at #688 integration; crew A owns it)

`publish_runtime` imports the driver from the publishing checkout at
`tools/fleet/pbcanary.py` — it never duplicates the submit-and-verify
logic — and calls `run_canary(generation=<name>) -> int` with the issue's
verdict codes (0 verified, 1 leg failed, 2 precondition refused). If that
shape drifts, the gate fails closed (`failed` + nonzero exit) naming the
drift; tell the integrator, who adapts the single call site in
`_load_canary_driver`.

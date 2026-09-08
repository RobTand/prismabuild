A box that has one loop stalled on the shared mount must keep claiming with its
other loops. It did not: `claim` moved `ready_items()` outside host admission in
#266, but still wrapped the whole of `_claim` in `controller.locked()`, and
everything `_claim` does after the decision is on the mount. The record rename
that IS the claim, the lease write, the token renames. A stall anywhere in that
region held the host-wide lock for its whole duration, every sibling loop
answered `None`, and the box went to zero claims with ready work waiting and
its tokens free. That is the signature #351 opened on: ready 14, claimed 0,
sparky idle at 111 GB free and 3% of the GPU envelope.

## The test comes first

`tests/test_claim_progresses_while_a_sibling_stalls.py` stalls the real shared
I/O rather than holding the flock: one `PoolQueue`'s `write_lease` blocks on an
event, and a second `PoolQueue` over the same root must still claim the other
ready item. Holding the flock directly is what
`test_admission_does_not_wait_on_a_peer.py` already covers, and it cannot show
this defect, because under the fix the flock is not held there at all. A
control test claims twice unstalled, so a red result cannot be the rig only
ever having had one admission in it. Every call into admission goes through a
`_bounded` helper, so a regression that made the acquisition blocking again is
a named failure instead of a hung worker.

RED at `10d10d46`, receipt `f18fb043a6f4`, host dl380g10, row failed rc=1,
parent `10d10d460f33`: `3 failed, 2 passed`. The headline assertion:

> the box claimed nothing while one loop was stalled on the shared mount: host
> admission is still held across the record rename, the lease write and the
> token renames, so every sibling loop is refused and ready work is left
> unclaimed (#351)

Two of the five were green at base (the control, and the lost-rename release),
which is the point of having them.

## What the lock covers now

The host flock is taken inside `_claim`: for the capacity prelude, and per
candidate for the adaptive decision, the GPU decision and probe reservation,
and `begin_acquire`. It is released before `_write_claim_intent`, the rename,
the lease write and `commit_acquire`. The one reacquire is `admitted`, which is
host-local borrow bookkeeping, after the claim is made.

Ownership is unaffected because the flock never arbitrated it. Two loops on one
box are excluded by the per-key `_transition_locked`; across boxes the atomic
`os.rename(ready -> claimed)` decides, and exactly one wins. Headroom cannot be
double-spent after the release either, because `begin_acquire` moves the tokens
out of `free/` into `held/<handle>/`, which every sibling's `decision` and
`available` already count, so a reservation is visible the moment the lock
drops.

A `try/except BaseException` now spans `begin_acquire` to `commit_acquire` and
calls `abandon_acquire(handle)` on any path that did not commit, then re-raises.
This closes a leak that predates the change: any raise between the two left the
whole demand under `held/claiming.<...>/` with no caller able to return it,
recoverable only by the stale sweep a `LEASE_TIMEOUT_S` later.

## Receipts

All GREEN rows: host dl380g10, `row=done rc=0`, parent `ff2391224577`,
`--priority -10`, runtime generation `332fa43447a2`.

- admission and adaptive set, 8 shards, 132 tests: `e071d4dd585f 5c2dab868435
  ed6acea9993e 50dcc6d9a34e 9aa6d96eab68 42930a6467a8 b8cb162ff444 65a505b792b7`
- all 55 `tests/test_pool_*.py`, 20 shards, 374 tests: `6614de7dc99f eccf02f3353a
  07ec28ef5398 11b27baba271 85bc59a818c8 18bb12e62f26 1b4e90285c37 a9075de71c74
  8201d11b2274 2a6ff0e1558d 1fd67bf4ea82 e83401fd8766 f4bc378185d9 194f35bf228e
  394fb7bc49a8 6ab73a0e9ea9 43f676820097 602889899dd8 47d139beb4ea 29c229e013e1`
- all 133 files importing `prismabuild.pool`, 20 shards, 1597 tests:
  `226eb5c19e95 26e243c31d9d 81857bf0fc98 2611b7edf5e1 2297e7ca6e94 17a9096f1e97
  9454dad41a6e deab094f454d ee27bb9be455 812a58026df7 7d50e6e444fb b3b8a664890a
  8045cd9af1d6 3b7944985061 76185c354eb4 e4592172a3fe 031e76318ed1 9acea317c368
  146de769ed0d 8f0bec9cc37f`

## What this does not establish

- **No fleet measurement.** These are unit tests under `tmp_path`. No induced
  NFS stall on a real mount, and no before/after telemetry of ready against
  claimed on a loaded box. That this fixes the observed idle-box signature is an
  argument from the code path, not a measurement.
- **The borrow window widened.** Between `begin_acquire` and `admitted` a
  sibling's `decision` reads a stale `last-borrow.json`, so one lightly used CPU
  can be lent twice for the length of one rename plus lease write. On a busy
  admission lock at that reacquire the borrow record is skipped for that host
  sample: it is the one swallowed `AdmissionBusy` in the method, because by then
  the item is claimed and there is no honest way to answer "nothing to run". At
  base this window was zero. Unmeasured.
- **Still under the lock on purpose:** the capacity prelude, and on the denial
  path `record_pass`, `_preempt_background_holder` and the starvation check. A
  stall there still holds the box. That is not this issue's signature, and
  preemption's one-holder-per-pass bound leans on the exclusivity.
- **New exception surface after the claim.** The reacquire swallows only
  `AdmissionBusy`; `locked()` can also raise `RuntimeError` or `OSError`, and
  those now propagate after the rename and strand a claimed item for the reaper.
  Same shape as `admitted()` raising today. Not proven absent.
- `_report_admission_busy` still prints "candidate evaluation not reached",
  which is no longer true for a refusal in the middle of the candidate loop.
  Left alone because `test_claim_reports_the_enclosing_admission_gate` pins the
  text; it wants its own change.
- x86 only, on `dl380g10`. Nothing ran on a GB10 box and nothing was GPU-tagged.
- The fleet is not running this. The current generation
  `332fa43447a2-1788897868-578846e2a9ea` is `origin/main` without this change.

Refs #351. Refs #266.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01WsJAfuLU26NWbYpSLrWFHJ

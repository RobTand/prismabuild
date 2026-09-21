# Issue #768: a promotion handoff defers its source's retirement

Component-level validation for the repair in
`tools/fleet/stage_release.py::_evict_owned`. This records what was
measured and what was not. Nothing here is a deployment, campaign or
Stage A claim: the deployed runtime generation still carries the
pre-repair behaviour, and no live queue, cache or role was touched.

## The defect

A staged path held by a live RAM promotion as its **copy source** was
skipped as *shared*: the file survived, but the pass then had no errors
and no deferred entries, so it unlinked this mover's map fragment and
material sidecar and settled its tokens.

Two consequences, both reproduced:

- **The bytes lost their proof.** A publisher adopts an existing staged
  file only through `stage_move._StagedPublisher._proof_search`, which
  wants a same-path fragment, its material sidecar, and current file
  identity. The promotion's RAM fragment names another tier and another
  path, so it can never stand for the surviving SSD incarnation.
- **The charge left early.** The shared branch added no `bytes_shared`,
  so the settle computed
  `shared_part = min(count - freed, _tokens_for_egressed_bytes(0)) = 0`
  and freed every token while every byte was still on the stage.

Field shape this matches (root-verified separately, not re-verified
here): head `a52860ef3084…` staged 36,439 entries / 10,895,318,814
logical bytes; egress `16f979eed929…` completed with `bytes_deleted 0`
and `entries_shared 36,439`, naming `promotion-handoff`; both fragment
and material for that head are absent; the next head over the same names
ran at roughly 0.5 files/s and timed out at 3600s.

## The repair

The handoff now defers, the way a reader pin already did: file,
fragment, sidecar and charge all retained, and `handoff_deferred` on the
receipt says how many entries made it so. Once the claim is gone the
ordinary retry deletes and releases once, or retains and decharges
against a real same-path co-owner.

A handoff-deferred pass deliberately files **no** retiring mark. A mark
closes one material generation to new acquires, and `ram_promote` takes
its proof-only cover through `reader_lease.acquire` *after* its claim
row exists, so the mark would refuse the handoff it protects. The
negative control in the test file shows that refusal directly. Marks are
per mover, so a reader-pinned entry of the same mover receives its mark
on the pass after the handoff ends; deleting stays safe meanwhile
because every pass re-reads claims and pins under the one ownership
lock.

## Evidence

Every promotion claim in the regression is a real `claimed/` row plus
its sealed CAS request and manifest blob, read through
`stage_release._claimed_source_paths` itself — no source-path set is
mocked. Files are 16 KiB. Executed through PrismaBuild at
`--priority -10`, one worker, one thread per shard.

| Run | Action key | Result |
|---|---|---|
| fail-before | `5ef95bc8ab826f4ef7c4c3f7d5c915bb74fd56ae8c069efcc76613ded7f773f9` | 7 failed, 1 passed; record in `pb-queue/failed/` |
| pass-after | `2878ec3f63f2e9c3451a1766f7a5923b8ae4ec8ff63f4a86159c351c2de8e076` | 8 passed; CAS receipt `cas/actions/v3/28/2878ec3f63f2….json` |

The fail-before run names the harm per axis: with the staged file still
present, `residency/<consumer>/<mover>.json` was gone ("the surviving
bytes lost their fragment: nothing can prove them"), and
`holder_tokens` was `{}` against an expected `{'stage_gib': 1}`
("tokens came back free for bytes that never left the stage"). The one
case that passed before the repair is the fixture check that
`_claimed_source_paths` recognizes the claim at all.

## What this does not establish

- No deployment. The published runtime generation is unchanged, and a
  source merge alone is not deployed support.
- No live-fleet or campaign evidence: no queue, role, cache or process
  operation was performed, and the field incident above was verified by
  root, not re-measured here.
- No claim about the staging-retry blocker or about GPU relaunch
  readiness; both are outside this repair.
- Throughput is not measured. The 30s publish grace per unproven entry
  is the mechanism the repair removes a cause of; this branch shows
  adoption succeeding without a recopy, not a files/s number.
- Concurrency between a handoff deferral and a simultaneous second
  egress is covered only by the existing ownership-lock cases, not by a
  new race case of its own.

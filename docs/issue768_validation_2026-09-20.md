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

### Targeted regressions, after the repair

Eight shards, 237 tests, no skips, all green (`dl380g10`, same PB
options). Ordinary reader pins, two-owner sharing and its decharge
accounting, dev/staged-only reuse and stale-donor refusal, adoption
without recopy, the orphan recovery's originals checks, and the design
doc checks are all covered by existing cases:

| Shard | Action key | Files |
|---|---|---|
| 0 | `d842c34aa8dd5b128389669f03462330a07873df805f01c9f1c296680bfe266f` | promotion handoff (this branch), orphaned-cache recovery by identity, two-tier relay design |
| 1 | `7704e106d5ad533e3c382d9ec9365d4b661b4ddecadd816e70304804aa9cfd0a` | shared staged path / two owners, shared-cache capacity credit |
| 2 | `3c66430e56ef9ed2f5c11b7115315464b7c8cead7ff147bf9293ec23cba02001` | reader lease lifetime, settled consumed pin |
| 3 | `b59bfafa2279c6d863ce19870bb7cf6caaddc0f906678625d59bcd29bb2c9e01` | promotion holds tokens until egress, stale donor declines |
| 4 | `1373089b30823f7c7e3cc3be2a0a2e7c99e6f3edd96cbb7dc05777b6ac6febf5` | ram shared source, resident range adopted not recopied |
| 5 | `7a20b360055493b44129536c90bb2e7b6fcfa6d203a9efd293d8bdbd6524093a` | ram own-consumer change, incomplete promotions release tokens |
| 6 | `464be6cf5a420fc7b287316257f757f5a41cb3e3cbf2f77380d35e65934dfc6b` | staged range holds tokens until an egress deletes it, full stage/ram chain |
| 7 | `2bf0bb8d7eb123ef1e87142a172abcf3104f91f09d5208e7ebf6db0896481827` | ram egress ordering, design doc line references |

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
- A promotion claim that lingers (a crashed promotion whose row has not
  yet been reaped) now holds the stage charge until PB's ordinary claim
  recovery clears it, where before the egress freed that charge while
  the bytes stayed. That is the intended direction -- occupancy follows
  the bytes -- but it does make stale-claim recovery the thing that
  releases those tokens.
- What re-drives a deferred egress is the pre-existing mechanism, read
  but not exercised end to end here: `stage_release.sweep` enumerates
  movers from the tier ledger's `held_keys()`, and a deferral retains
  those tokens, so the mover is revisited on a later cycle and its paths
  stay attributed through `wanted | owners | still_held` in the
  reconciliation. Retiring marks are never an enumeration key -- they
  are read only inside `reader_lease.acquire` -- so filing none does not
  hide a handoff deferral from the retry. A handoff-deferring egress
  action exits 1 exactly as a pin-deferring one already does.

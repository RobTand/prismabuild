# Slice 3a tier input path audit

Read-only source audit at main landing `88b1a5108799040c4dbd87bfd628a82e44bf739b`. No tests, live store reads or mutations.

## Authoritative names and bounds

| Topic | Existing store shape | ID | Watch depth below queue |
|---|---|---|---|
| `tiers` | `tiers/<tier>.json` | Exact tier | Directory depth 1; file depth 2 |
| `tier-reservations` | `tier-reservations/<tier>/{free,held,minted}`; `held/<holder>/<entry>`; `minted/dead/<entry>` | Exact tier | Directory depth 4; leaf depth 5 |
| `tier-funding` | `tier-funding/<mover>.<tier>.funding.json`, `<mover>.<tier>.output-funding.json`; retired output variant under `tier-funding/retired/` | Exact tier | Directory depth 2; leaf depth 3 |
| `tier-commitments` | `tier-commitments/<tier>.json`; claim order is a field in this record | Exact tier | Directory depth 1; file depth 2 |

Sources: `record_store.py:886-924`; `tier_holdings.py:46-64`; `pool.py:3635-3657`; `pool.py:4203-4220`. Tier IDs are not action hashes. `RecordStore.check_tier_id` rejects empty, leading dot, `/`, and `@`, but otherwise admits broader spelling than the current bell ID filter. It does not impose a length bound. `storage_tiers.tier_id`, lines 1202-1212, produces stage pool plus colon plus host, or kind plus colon plus host. Preserve dots and colons. A store-valid identifier outside the bell bounded grammar must withdraw coverage rather than silently disappear.

Funding parsing removes the exact suffix, then partitions once on the first dot. The mover is lowercase 64-hex as validated by `funding.py:240-246` and `funding.py:1015-1020`. The remainder is the complete tier ID, which may contain dots. Never split the whole filename on dots or take `Path.stem` as the tier. Retirement uses the identical output filename, atomically renamed to `retired/`, after a repeated terminal-and-empty proof under the mover transition lock, `funding.py:1177-1258`. The original active path and retirement path both matter.

## Mutation and census edges

`TierLedger` delegates token changes to `ResourceLedger` with `tier_census=True`, `tier_holdings.py:46-57`. Acquisition uses `held/claiming.<epoch_microseconds>.<action_key>.<host>.<pid>.<nonce8>/`, `pool.py:4378-4382`. Hostnames may contain dots; do not parse the holder as an action hash. It is counted as consumed throughout acquisition, commit and rollback. `begin_acquire` rolls back both insufficient demand and exceptions, `pool.py:4350-4530`. Commit transfers tokens individually from private holder to final holder, `pool.py:4525-4579`. Release and handover also operate through token renames.

Holder names also include advance grants, arbitrary legacy names and hidden directories. `HolderCensus.directory_inputs`, `holder_census.py:110-119`, includes all directory names and can retain all symlinks for coverage rejection. Tier observations also use raw held listings in `TierLedger.observation_fences`, `tier_holdings.py:98-113`. Do not require holder IDs to match an action pattern.

Token names are enumerated with `*-*` through `_glob_names` and `_census_glob`, `pool.py:3603-3633`. The retained-reader path uses `fnmatch.fnmatchcase` without excluding leading dots. Hidden matching token names therefore count. Mint history reads **all** immediate minted names except literal `dead`, and **all** immediate `minted/dead` names, `pool.py:4203-4220`. Hidden minted/dead names must invalidate too. `retire_held` renames a live token into `minted/dead/<token>` while preserving its mint marker, `pool.py:4036-4095`. This is a required real tier shape, unlike host reservation coverage where minted/dead is unsupported.

Tier root `census-unreadable.json`, `pool.py:4222-4260`, is diagnostic evidence. `.mutation.lock` is for direct ResourceLedger construction; TierLedger uses separately hashed `tier-mint-locks/<digest>.lock`, `tier_holdings.py:328-331`. Unlock has no inotify event. No adapter should scan under that lock or claim that watching its file supplies lock-release notifications.

## Claim order and actual time boundaries

There is no standalone claim-order record or namespace. `tier_cycle._commitment_report`, `tier_cycle.py:4092-4150`, embeds `claim_order` in the tier commitment record. `PoolQueue.file_tier_commitment`, `pool.py:12871-12889`, files that record atomically and adds `filed_unix`. `PoolQueue._tier_commitment_standing`, `pool.py:11531-11626`, reads it. One `tier-commitments` hint covers both commitment and order changes. A duplicate claim-order topic buys no new source coverage.

Actual deadline sources:

- Tier announcement liveness is `announced_unix + OFFER_TIMEOUT_S`, currently 120 seconds, `pool.py:848`, `pool.py:11771-11787`. Equality is still live.
- Private acquisition staleness uses name timestamp from `_acquisition_clock`, `pool.py:3369-3390`, plus grace, default `LEASE_TIMEOUT_S=300`. A known-dead local claimant uses the smaller `HEARTBEAT_S=30`, `pool.py:4914-4958`. Existing code sweeps only when age is greater than the bound.
- Claim-order short-relief maturity is `relief_since_unix + window_s`, where the caller default is `OFFER_TIMEOUT_S=120`, `pool.py:11531-11535`, `window_credit.py:411-431`, `window_credit.py:433-468`. It applies only to short relief with zero evicted GiB and valid recorded start. The helper reaches stuck at equality.
- Ahead-consumer evidence can be carried while `now - evidence_unix <= window_s`, `pool.py:11629-11699`. The verified lease can also establish progress/grace, bounded by lease heartbeat freshness of 300 seconds and its own `quiet_s`/`grace_s`, `pool.py:11750-11769`. Progress/lease watch coverage belongs to the later adapters, so tier topics alone cannot justify sleeping on every order dependency.
- Funding has no standalone TTL, expiry, lease-until or reservation horizon field. Its immutable binding includes generation and published_unix; states advance reserved to transferring to consumed/released, `funding.py:24-31`, `funding.py:33-50`, `funding.py:360-425`. Do not invent a funding timeout. Retirement requires authoritative absence/terminal/token proofs, not elapsed age.

## Consumer boundary

Keep the bell names-only and bounded by the table. The consumer owns confirmed record reads, semantic comparison, explicit unknown/absent states and deadlines from the authoritative predicates above. Preserve dirty tier IDs across unconfirmed NFS visibility and keep the original absolute read budget; use the existing named 50 ms to 1 s visibility retry, and 0.25 s to 5 s bell recovery. No callback, reconnect or duplicate hint may reset policy starts or extend the read budget. Consumer integration remains a later slice.

The four proposed topics are sufficient for this slice. Cover old-writer in-place write, replacement, removal, partial acquisition/rollback, populated moved-in tier/holder/dead/retired roots, permissions, symlinks and watch loss. Unknown or unreadable subtree shapes must not advertise complete coverage. Legacy per-batch `LegacyFunding` lives under residency attempt scopes rather than `tier-funding`, `legacy_funding.py:21-22`, `funding.py:4023`; it is a separate later residency/receipt dependency, not a reason to recurse into residency from this adapter.

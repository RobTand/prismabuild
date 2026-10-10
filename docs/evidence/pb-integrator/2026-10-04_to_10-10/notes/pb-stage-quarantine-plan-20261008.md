# Stage quarantine plan for issue 1636 (READY, NOT RUN)

Author pb-integrator, 2026-10-08. CEO decision on rep-1008-205017-ace3: step 1 (release of ended holders) was enough; keep this plan ready for the next stage shortage. Run it only on a new CEO word that names a shortage. Nothing here has been executed.

## Trigger
Free stage is below the 104 GiB a waiting PACT group needs, after the documented release of ended holders (`pb-stage-release-driver-20261008b.py`) has run.

## What may move
Only a `source_mark_only` copy with no consumer and no reference. "No consumer": no live or ended consumer's move record, plan or residency map names it. "No reference": `stage_release.census` reports it unowned, no `fragment` names it, no READY or CLAIMED row names its mover key, and no shared range interest. Anything the census cannot classify is left in place. The 10-08 pairing attempt found no mover that names these copies; the dry run below must report that again before anything moves.

## Steps (all on dl380g10, as PrismaBuild actions, since /stage is local to it)
1. **Dry run, read only.** List candidate copies with path, bytes, mtime and the proof of "no consumer, no reference" per entry. Write `~/fleet/inventory/pb-stage-quarantine-dryrun-<stamp>.json`. Stop if the candidate set is empty or any entry lacks a proof.
2. **Space check.** The quarantine directory must be on a disk outside the stage tier with room for the whole set. Name it in the dry run (candidate: a path under the shared scratch on dl380g10; confirm with `df` in the same action). Stop if it does not fit.
3. **Move, one entry at a time.** `rename(2)` when on the same filesystem, else copy, fsync, verify size and mtime, then unlink. Recheck "no consumer, no reference" immediately before each move. Stop on the first anomaly. Delete nothing else.
4. **Manifest.** `quarantine-manifest.json` beside the moved files: original path, quarantine path, bytes, mtime, the proof, the time. Written before the first move and appended after each.
5. **Restore command.** `python3 tools/fleet/<restore script>` or `mv` lines generated from the manifest, one per entry, written to `quarantine-restore.sh` before the first move. A restore test on one small entry before the bulk move.
6. **Retention.** Keep the quarantine at least 7 days. No deletion without a CEO decision after day 7.
7. **After.** Record stage occupancy before and after (ledger free GiB and the stage filesystem `df`), the count and bytes moved, and the manifest path. Report to the CEO within 30 minutes of the order.

## Risks stated
- The tier capacity swings (164, 198, 154, 215, 304 GiB on 10-08) and is likely tied to the unowned data. A quarantine may raise it, or may change nothing. Do not promise either.
- The earlier repair of issue 1636 could not pair copies to heads; a dry run that again cannot prove "no consumer" means this plan does not run.
- An action that moves 271 GiB competes for the stage disk with PACT movers. Run it when no PACT mover is copying.

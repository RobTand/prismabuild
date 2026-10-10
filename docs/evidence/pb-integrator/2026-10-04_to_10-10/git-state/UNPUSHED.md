# Commits that existed only in /home/rob/prismabuild on celestia (pb-integrator, 2026-10-10, D65.4)

Bundle: `/mnt/shared/fleet-ceo/pb-integrator-evidence-20261010/unpushed-commits.bundle` (on /mnt/shared), 81273 bytes, sha256 `c03418bf86388dbd46e95585f359c2d62ae58757e37f76d62c79cc874b79aca1`. It requires 15 commits from the history of origin/main (`git bundle verify` lists them), so restore it into a clone that has origin/main.
Local protection: each commit below also has a ref under `refs/pb-integrator/keep/` in /home/rob/prismabuild, so a worktree prune or gc cannot drop it.

Restore: `git fetch /mnt/shared/fleet-ceo/pb-integrator-evidence-20261010/unpushed-commits.bundle 'refs/pb-integrator/keep/*:refs/pb-integrator/keep/*'`

| keep ref | commit | subject | commits not in origin/main |
|---|---|---|---|
| `batch-1643-1644-merge` | `9e71ae01e9` | Merge commit '19db2298282c216536d0e3eb81f38d20d92a4714' into HEAD | 1 |
| `batch-20261004-1497` | `084f65d3ba` | merge 4/4: PR #1510 sealed pytest basetemp at 8b6fd01d | 4 |
| `batch-a-compose` | `dd4a3e9630` | Integrate batch A 6/6: PR #1509 range decision partials hint (96859b07) | 6 |
| `batch-a-integration` | `ffadf49f1f` | test: retire source-shape and wording ratchets | 10 |
| `carry-gen6` | `2399c0008f` | tier_loop: window_pressure reports each waiter and tier that asked for nothing, and why (n | 35 |
| `carry-gen7` | `54600c279f` | fix: reserve same-consumer prelaunch generations first (net of PR 1633, merge 5b843652ee,  | 36 |
| `carry-gen8` | `8f58b7f33e` | fix: a committed prelaunch group that lost its tokens tops up its deficit and keeps its pe | 37 |
| `d38-merged` | `865f5118f6` | Merge commit '85317884dc21e7e712a1a34fbc3e435d758b609d' into HEAD | 1 |
| `d38-merged2` | `289b3118c5` | Merge commit 'c5bf73ff66b8ec37d335cf24aa44e922e658e14c' into HEAD | 1 |
| `evict-gib-baseprobe` | `619bc40789` | base probe: the evict-gib tests alone, expected red on 04f6f00e | 15 |

## What they are
- `carry-gen6`, `carry-gen7`, `carry-gen8`: the generation commits of published runtime generations 6, 7 and 8 (gen8 `8f58b7f33e` was the live commit until gen9). They were built as stacks of carry commits over an older base and never went to origin.
- `d38-merged`, `d38-merged2`: scratch merges made while checking PR 1640 (the D38 gate).
- `batch-*`, `batch-a-*`: integration branches for the 10-04 merge batches (the PRs themselves merged on main).
- `evict-gib-baseprobe`: a base probe for the unmerged evict-gib feature (#1559, #1538).
- `batch-1643-1644-merge`: the commit of an unfinished merge attempt for #1530; #1530 landed through the train as `215cc6738b`.

## Uncommitted state, saved here
- `pb-batch-1643-1644.status.txt` and `.diff`: the working state of that unfinished merge, with one conflict marker in `docs/contributing.md`. It cannot be committed as it stands. Superseded by the train merge of #1530.
- `pb-gen-54600c.untracked.test_prelaunch_same_key_resubmission_after_withdrawal.py`: an untracked test file in the gen7 release worktree. It is not identical to the version on main (sha256 prefix `46fba2ac` against `24bbaa27`); it is probably the earlier text of PR 1638's test as carried into gen7. I did not diff them.

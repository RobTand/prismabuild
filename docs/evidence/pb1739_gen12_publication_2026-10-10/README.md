# prismabuild#1739: gen12 publication evidence (pb-integrator, 2026-10-10)

| item | value |
|---|---|
| live generation | `34c2228cb25e-1791602912-8161ce96d404`, published 2026-10-10T03:28:36Z, rolling |
| release head | `34c2228cb25e47e48d26781efc9fd6f1e378dd53` = main `561e3177be3c8e6f3e21e2f7ffa5abddf0c91663` + one commit that reverts PR 1640 (D38 gate). Branch `prismabuild-1739` holds exactly that head. This branch (`prismabuild-1739-evidence`) is that head plus these docs. |
| rollback target | gen11 `281006ef6374-1791578707-9515e3d96a7e` (not needed) |
| order | CEO under D64, `dec-1010-024316-b5c4`; the CEO kept the PR 1640 revert (report `rep-1010-030001-ec85`) |
| review | `rev-1010-025544-37e6`, APPROVE, https://github.com/RobTand/prismabuild/issues/1739#issuecomment-6093093124 |
| shape gate | action `6dfecfffd2de9a12b7a6cfabb48e2350ae9eb30c9e866f2c2dd4c6d2dfebdfbf`, 2 passed in 1002.6 s |
| full suite | 13,507 passed, 13 failing IDs: see `GATE-RESULT.md`. The raw result file was lost. |
| canary | run `20261010T032844Zg12`, driver exit 0, all four legs verified (`canary/`) |
| report | `rep-1010-033255-408c` |

## Directories
- `scripts/`: the scripts that built, gated, published and canaried gen12 (copied byte for byte from `~/fleet/inventory`). Note `pb-gate-gen12-20261010.sh` and `pb-shape-gen12-20261010.sh` were edited after the run to write to `/mnt/shared/fleet-ceo/pb-integrator-gates` and to use `/usr/bin/python3`; the run itself used `/home/rob/tmp/pbfix` and the venv Python, both deleted since.
- `logs/`: publish, dry-run, canary wrapper and shape-gate logs. `publish-...T032737Z.log` is the dry run; `...T032822Z.log` is the publication.
- `canary/`: the canary driver's result and run files.
- `gate-evidence/`: the isolated rerun of the three differing files on the candidate, and the control run on gen11.
- `notes/`: the comment posted on #1739, and `pr1584-enrollment-needs.md`.

## Lost
`gate-gen12-cand.json` and `.log` (the full-suite result) were in `/home/rob/tmp/pbfix` and were deleted with `/home/rob/tmp` at about 03:24Z. The counts and IDs in `GATE-RESULT.md` are what I read before that.

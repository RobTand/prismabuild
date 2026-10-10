# prismabuild#1738: same-key resubmission on a live band (pb-integrator, 2026-10-10)

CEO host task (D64). Issue closed 2026-10-10. Full account: `issue-comment-1738-6093081264.md` (the comment as posted).

## What happened, in one paragraph
The dl380g10 tier loop already ran gen11 (`281006ef6374-1791578707-9515e3d96a7e`), so it was not restarted. A CPU consumer
with one declared prelaunch phase (8 GiB) was submitted, committed its group, was withdrawn, and resubmitted under the same
key `7b06f3ad97c1933757afea77383629d89dc9ab95bc05bb85ba31921d8b2cf2c3`. The renewed row claimed at 02:56:14Z. The GPU admission path was not exercised.

## Files here
| file | what |
|---|---|
| `testband_driver.py` | the driver as it ran (copied byte for byte from `~/fleet/inventory/pb-1738-testband-driver-20261010.py`). It has lost paths in it: `CWD=/home/rob/tmp/pb-1738-job`, `MAN=/home/rob/tmp/pb-1738-manifest.json`, and `PB=/home/rob/tmp/pb-submit-celestia-20261003/bin/python`. To rerun, change `PB` to `/usr/bin/python3`, recreate the consumer repo from `consumer_README.txt`, and rebuild the manifest with `make_manifest.py`. |
| `receipts-20261010T024313Z.json`, `driver-run1.log` | run 1: refused at submit because a v2 read plan needs `--progress-phase`. Nothing was queued. |
| `receipts-20261010T024326Z.json`, `driver-run2.log` | run 2: the proof. Its own last line says "renewed row did NOT claim". That is wrong, because the driver's claim wait ended at 420 s and the claim came later. The `correction` key in the receipts file carries the queue records. |
| `make_manifest.py` | reconstruction of the manifest builder. NOT byte-identical to the original (see its header). |
| `consumer_README.txt` | the one-line content of the consumer repo (commit `ef6e3ad` in a repo that was deleted). Rebuilding it gives a different action key. |
| `issue-comment-1738-6093081264.md` | the comment posted on the issue |

## Not in git, and why
- `/mnt/shared/fleet-ceo/pb-1738-testband/f0.bin` to `f3.bin`: four random 2 GiB files, 8 GiB in all. Not hashed. They are **still on the mount on purpose**. `stage_release` refuses to evict a staged copy when its original is missing, so deleting them first would strand the 8 GiB copy in the stage. Delete them after the sweep evicts the stage copy.
- The row's manifest, as sealed, was lost with `/home/rob/tmp`. Its sha256 is above.

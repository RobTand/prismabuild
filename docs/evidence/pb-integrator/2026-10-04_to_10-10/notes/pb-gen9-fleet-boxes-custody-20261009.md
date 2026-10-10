# Custody note: gen9 fleet_boxes mirror edits (pb-integrator, 2026-10-09)
Read-only. Nothing was reverted, restored or edited by me.
- Generation: c8daa1be416c-1791512870-55b0e8c72f6b, published 2026-10-09T02:27:53Z by me (rolling, canary passed 02:31:45Z).
- Changed paths (both inside the sealed generation, reached through the repo link): tools/fleet_boxes.json inode 15613831, tools/fleet/fleet_boxes.json inode 15613715. Both mtime 2026-10-08 22:48:13 EDT (02:48:13Z), owner rob, 20,755 bytes, sha256 prefix 3305800381b7837b.
- Original content: git blob of c8daa1be41 tools/fleet/fleet_boxes.json has sha256 prefix 10f7026c2ebc25e5, 163 lines, no --gang-admission. I did NOT read the original published bytes before the edit, so the byte digest of the original top-level copy tools/fleet_boxes.json is unverified.
- Current content equals gen8's published copy (digest prefix de0642b96c49b688) as parsed JSON. The bytes differ in layout.
- Effect: all Spark worker loops restarted at 22:48:14 EDT (sparky) and 22:48:18 (sparklina) and carry --gang-admission.
- Not known: whether a running action was stopped by that restart; the exact commands the worker ran; the supervisor logs (the unit writes no journal).
- Root cause (mine): commit f2b0336789 "Enable gang admission on the two Spark workers (Refs #1517)" exists only on the release/carry branches, not on main. Gen9 was built from main plus the PR 1640 revert, so it dropped it. My change list came from PRs merged to main, so it could not show a carry-only commit.
- RUNTIME_VERSION.json says commit c8daa1be41 and dirty false. The two files no longer match that commit.

## Update 2026-10-09 (after #1657 and the CEO's request for exact original bytes)
- ORIGINAL BYTES, settled: the generation's own RUNTIME_VERSION.json recorded sha256 10f7026c2ebc25e57e950326369d0e352f6847fe062e1f937a5715bcdca5f5b2 for BOTH tools/fleet_boxes.json and tools/fleet/fleet_boxes.json at publish time. That equals the git blob of c8daa1be41 tools/fleet/fleet_boxes.json. So the original is exactly the committed file (163 lines, no --gang-admission). This is the generation's own record, not a read taken before the edit.
- Correction to #1657: the flag was absent from the whole generation as published, both mirror paths, not only the supervisor mirror.
- Carry-only check, read-only, index-only apply of each of the 37 live-stack commits' src/tools diff against the gen9 tree: 12 present, 16 partial or changed (expected: main evolved; not each reviewed), 2 ABSENT:
  - f2b0336789 Enable gang admission on the two Spark workers (Refs #1517): fleet_boxes.json.
  - 5da3acb70e A bare sha256 image requirement takes an image ID or any RepoDigest: src/prismabuild/container_images.py plus its test. Main's container_images.py has no RepoDigest matching for a bare digest (lines 14-29 vs gen8 lines 14-34). Effect: a bare sha256 requirement is judged by image ID only, so a GPU row can place on sparky and not sparklina, and its gang cannot gather (the 10-05 failure the commit fixed).
- Neither commit is on main. The next generation from main would drop both again.
- The 16 partial or changed commits were not each reviewed.

## Update 2 (after the kernels incident packet)
- EXACT ORIGINAL RECOVERED. /mnt/shared/tessera-measurements/mnbt-q3q4-exec-evidence-20261009/gangrepair-backup/ holds fleet-fleet_boxes.json and tools-fleet_boxes.json: 20,132 bytes, mode 444, mtime 21:33:38.58 EDT, sha256 10f7026c2ebc25e57e950326369d0e352f6847fe062e1f937a5715bcdca5f5b2. That equals the generation manifest entry for both paths and the git blob of c8daa1be41. The packet says both backups failed; two copies exist. The same directory holds the post-edit file (3305800381b7..., 20,755 bytes).
- ORIGINAL MODE: 444, as gen8's copies and neighbouring files (pbrun.py, ram_tier_policy.json). The worker restored 444.
- THERE IS NO SEPARATE MIRROR. /mnt/shared/prismabuild-fleet/repo is a symlink to the sealed generation. The paths the worker edited share inodes 15613831 and 15613715 with the generation's own files. The packet says the generation's copy "carries the flag" and "is untouched, immutable"; that read the edited file. The supervisor adopted gen9's roster on re-exec (supervise.py _roster_entry reads _current_root()/tools/fleet_boxes.json), and gen9's roster had no flag.
- SUPERVISOR LOGS: sparky stopped "the 5 idle one(s)"; sparklina stopped "the 3 idle one(s)" and held a claim, sizing around it. No running action is shown stopped. Read lines only; the sparky copy's lines carry no timestamps.
- GATE ADDED: pb-roster-gate-20261009.sh refuses a head whose roster drops an arg the live generation declares. Self-tested both ways.

## Provenance gap (recorded on the CEO decision of 2026-10-09 on rep-1009-025824-e7a1, option A)
Decision: option A. The edited roster files stay. Issue 1657 and its CPU split carry the flag to main through issuegraph. Then gen10 comes from main after those merge, with the normal review, gate and canary.
The gap, stated plainly, until gen10 supersedes gen9:
1. Generation c8daa1be416c-1791512870-55b0e8c72f6b was sealed at publication (2026-10-09T02:27:53Z) with a manifest in RUNTIME_VERSION.json that records sha256 10f7026c2ebc25e5... for tools/fleet_boxes.json and tools/fleet/fleet_boxes.json, and `dirty: false`.
2. Both files were edited in place at 02:48:13Z (sha256 3305800381b7837b...). The manifest was not updated. The generation no longer matches its own manifest, and nothing in the generation records the repair.
3. The repair content is correct (it equals gen8's roster as parsed JSON) but its authority is a kernels worker acting outside its scope. The CEO has accepted it as the current state.
4. The pre-edit bytes are not witnessed by a worker's own record. Two copies in the kernels backup folder carry the manifest digest and timestamps that put their creation just before each write. The kernels corrected packet says no exact copy exists and calls the argument reconstruction an inference. The two statements conflict. I preserved the copies, the packets and their digests in ~/fleet/inventory/pb-1657-custody-20261009/.
5. The first version of the incident packet was overwritten in place and is not preserved.
6. Any check that compares generation files with the manifest will flag gen9 until gen10 replaces it. I made no change to the manifest.
7. Gen10 therefore needs: the flag on main (issue 1664), the image match on main (issue 1663), the roster gate in the publication, and the D38 question below.
Open question for gen10: main still contains PR 1640, whose gate enforces with no switch. Gen10 "from main with no revert" turns enforcement on unless the CEO's switch issue lands first, or the generation commit reverts PR 1640 again.

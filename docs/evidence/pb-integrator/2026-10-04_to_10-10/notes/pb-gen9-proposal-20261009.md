# Proposal: one runtime generation from current main (pb-integrator, 2026-10-09)

Main: `6e5aaee1f4`, tree `ae940c60bd`. This tree is identical to the tree of my last full-suite gate (head 76b49dde3a): 11,538 passed, 11 failing IDs, none new against main ba540d991b, and the 11 are present on main too. Keys: `~/fleet/inventory/pr1619b-gate-action-keys-20261008.json`.

Live now: gen8 `8f58b7f33ef6-1791501736-15af5a3300de`. It is a stack of carry commits (nets of PRs) over base 54600c279f, not a commit on main. Its merge-base with main is 15455fa883 (2026-10-02). The rollback target is gen8.

## Size of the change (runtime-shipped paths, gen8 commit 8f58b7f33e to main)
57 files, +9,348 and -882 lines (src/prismabuild 26 files, tools 31 files). Tests (131 files) and docs (14 files) do not ship.

## Blocking finding: PR 1640 turns D38 enforcement on
PR 1640 (D38 preflight gate, #1639) is on main. `tools/fleet/d38_gate.py` sets `ENFORCE = True`. No flag or environment variable reaches it. `pbrun` calls it on every path that would publish new runnable work. `INVOCATIONS` is empty. The module says: "until one does no receipt can authorize a job and the scoped exception is the only path". `GPU_TAGS` is `gb10`, `sparky`, `sparklina`, and a submission tagged to a Spark counts as GPU intent.

So a generation built from main as it stands refuses every new GPU `pbrun` submission unless it carries `--d38-exception <CEO decision id>`. By the code, my own four-leg canary legs would be refused too (they are tagged `sparky`). I have not run that to confirm it. The review of PR 1640 said runtime publication needs separate authorization, and #1639 is open for that reason.

## Options
A. Publish main as it stands. D38 enforcement goes live with no producer and no reviewed descriptor. All GPU work, including PACT, then needs a per-job CEO exception. The canary probably cannot pass. I do not recommend it.
B. Publish main with PR 1640 reverted in the publication commit only (recommended). `git revert -m 1 ba540d991b` applies cleanly on main (9 files, all PR 1640's). The generation commit is then not on main, as gen8 is not. Enforcement stays for a later generation, with the producer and descriptors that make it usable.
C. Change source first: a PR that makes enforcement switchable by a reviewed route, then publish main. It needs a design decision on the switch, because the module says no caller may reach it.

## Plan (same as gen8)
Dry run with the emitter-reader gate and a shape-gate receipt for the exact commit, publish rolling (each idle loop upgrades; running actions are not interrupted), health check sparky and sparklina first, four-leg canary from sparky, automatic rollback to gen8 only on a definitive failure. I changed the script so a late-written result file is read after a short wait.

## Changes on main that gen8 lacks
Method: each PR's src/tools diff is tested against the gen8 tree. It applies cleanly forward = the PR is missing (17). It reverse-applies cleanly = already live (5). Neither = partly present or changed by a later PR (20). The 57-file tree diff above is the exact measure. The PR split is my best reading, not proof.

### Missing (17)
- #1468 `ab82e288f4` Check history before snapshot identity can traverse promised objects (#1467) (1 runtime files)
- #1482 `027103d9a8` Bind native launch evidence and finish I/O before final selection recheck (Refs #1481) (5 runtime files)
- #1488 `6a72154253` fix: honor tier maintenance at cooperative cycle boundaries (#1487) (1 runtime files)
- #1486 `4eb39668ae` fix: rename held's vouch kwarg to canonicalized (#1484, review r1) (2 runtime files)
- #1493 `c111602d83` Keep PB suite submission stdlib-only (#1491) (3 runtime files)
- #1505 `35c87d64d0` test: drive the in-progress cleanup through the real withdrawal (#1403) (1 runtime files)
- #1510 `3049858dae` Document who removes the derived basetemp namespaces (#1469) (1 runtime files)
- #1509 `661ea17e09` test: each range name sees refs added to a pin (#1028) (1 runtime files)
- #1518 `2bdd748b13` fix: rotate orphan pass sweeps through retained rows (#1503) (1 runtime files)
- #1497 `bd5a22fd4c` tests and docs: one bound-rate behaviour test, the pbrun flag test on the real parser, and the sample:HZ contract in des (2 runtime files)
- #1549 `59719c7db3` tests: name issue 1548 in the xfail reason (1 runtime files)
- #1568 `1030699e20` pbtest: the per-test resource tracer binds its procfs reader at plugin load (Closes #1550) (2 runtime files)
- #1596 `5283c73c2d` Ask the refusal counterfactuals of the probe intent (1 runtime files)
- #1605 `deef41208f` docker shim: a stop is proved only by exited, dead or absent (Refs #1599) (3 runtime files)
- #1606 `d5787521f1` Correct R1 guard isolation and design note (Refs #1542) (1 runtime files)
- #1624 `8fe5de4471` worker_loop: sweep dead offer temporaries after the stale-generation fence (Refs #1506) (1 runtime files)
- #1641 `4d26c976c5` docs: an unreadable published_unix still refuses; only passes is read as 0 (Refs #1506) (1 runtime files)

### Partly present or changed later (20)
- #1466 `26f06df929` Seal distinct action identities in the maximum-batch regression (#1465) (3 runtime files)
- #1502 `7fb7fa6b13` Create the lazily made passes directory in the #1498 census tests (2 runtime files)
- #1504 `d32f2f2ed9` test: a holder no claim record names does not elect the host (#1419) (2 runtime files)
- #1519 `43ea0cf6f7` test: interleave two gangs through a late group record (#1519 review) (6 runtime files)
- #1508 `7c24107299` Floor guard v2: registration, refresh, release, waiting locks; enforcement default-off (#1483) (7 runtime files)
- #1525 `6c6ec7bea1` pbgang --queue help names what pbgang does with the queue (Refs #1517) (2 runtime files)
- #1501 `ab8574f707` validate_sealed: refuse a malformed dependencies value instead of reading it as no fence (#1495) (7 runtime files)
- #1529 `a273d3c664` Publish pbtest_capabilities.py with the runtime (Refs #1495) (1 runtime files)
- #1531 `84a5650e55` pbgang: tidy wording after review: no whole-contract claim, one cwd note (Refs #1517) (1 runtime files)
- #1537 `6781c54905` File withdrawn late finishes with the withdrawn disposition (6 runtime files)
- #1540 `f690a6f4e1` Keep the device comparison out of ZFS capacity sampling (9 runtime files)
- #1552 `3f805b7548` tools: document every argument of pbresident, local_resident and local_tier_loop (test_fleet_tool_flags_have_help; found (12 runtime files)
- #1560 `278a2e4dda` docs: distinguish dispatch state reads from descriptor ownership (#1545) (8 runtime files)
- #1553 `cfc27807b3` Consolidate resident digest recipes without growing the ratchet (22 runtime files)
- #1567 `2d0e48a87b` Vary the template tag per seal so leakage would fail the test (Refs #1565) (3 runtime files)
- #1570 `e278a95d29` Read raw RECORD bytes before newline conversion (#1570) (1 runtime files)
- #1578 `abe046e072` pbtest: bind the recorder's directory at import so a generation switch cannot change it (review of PR 1578) (1 runtime files)
- #1613 `5e18eb94d1` tests: state the 12 post-merge expectations PR 1512's contract changes (resolved cat path, explicit class tag for absent (6 runtime files)
- #1642 `5e2fe1c05a` Refuse resident dispatch for inactive leases (1 runtime files)
- #1640 `ba540d991b` Merge branch 'main' into feat/d38-preflight-gate-20261008 (4 runtime files)

### Already live (5): #1514, #1523, #1527, #1556, #1558

## Named by the CEO
- #1571 d3e6d414: census: take a refusal once per pass and rescan a vanished publication (on main)
- #1628 5fcad83c: prelaunch: a second capture of one manifest rotates the first capture's spent funding records (on main)
- PR 1641, PR 1642, PR 1643: all on main. 1641 and 1642 touch runtime files and are in the lists above. 1643 changes tests only.

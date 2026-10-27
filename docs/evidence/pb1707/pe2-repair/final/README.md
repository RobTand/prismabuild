# PE2 final source qualification

Frozen source `abfb073bf1923ef6c05f5f450d75acb65306e011` passed 2,419 checks across 123 executed actions with 123 distinct success receipts. No failures or skips occurred. The frozen v4 plan completed all 14 rows on 2026-10-27 at 00:42:27 UTC.

| Scope | Checks | Actions |
|---|---:|---:|
| Retained focused checks and x86 queue lease | 40 | 4 |
| Five x86 rounds of eight files, including two xdist rounds | 655 | 40 |
| Five ARM rounds of eight files, including two xdist rounds | 655 | 40 |
| x86 integration, 23 files | 627 | 19 |
| ARM integration, 22 files | 442 | 20 |

The x86 integration includes all 185 tests in the unchanged native queue compatibility file. Action `66e7890113d357e039a6b52259c86235252b3b6b48a90f46fdf8e76fab73eb2b` has success receipt `a354732c04cbc3bb76a74b4ea9ace3adc494636e471b9e26a8b8a214e2a7c28a`. The original c874 queue action timed out at its sealed 1200-second deadline. It has no completion or receipt and receives no passing credit.

The archival-replay harness explicitly requires x86 and a pinned Python 3.14. The earlier ARM archival refusal published no action. ARM archival compatibility remains unsupported. Portable changed-file repeats and the portable integration passed on ARM with Python 3.12. This scope limit requires independent review.

## Evidence readers

`qualification.json` binds every successful key, receipt, source snapshot, request hash and test count. `qualification-plan.json` preserves the frozen commands and resource bounds. `qualification-state.json` records durable completed rows. `merge-acceptance.json` proposes scoped source acceptance. It does not record approval, merge, deployment or campaign completion.

The retained evidence root is `/mnt/shared/paperclip-runs/PRI-43/pe2-repair/qualification-v4/`. It contains full native logs, ending records and source bundles. `reaudit.py` rereads existing PB endings, full logs, canonical requests, native populations, cleanup, source and CAS receipt/payload bytes without submitting work. Run on the coordinator with the original shared CAS available:

```sh
python3 docs/evidence/pb1707/pe2-repair/final/reaudit.py
python3 docs/evidence/pb1707/pe2-repair/final/summarize.py
git diff --exit-code -- docs/evidence/pb1707/pe2-repair/final
```

`summarize.py` reproduces the committed manifest from the audited raw records. It also checks actual architecture, both sealed xdist rounds, complete file coverage and unchanged guard bytes. The qualified `src`, `tools` and `tests` tree IDs bind product/test source independently of this documentation commit.

Each shard declared two CPUs, four GiB and zero GPU. Native threads were bounded to one. Default eight-client fanout bounded the matrix to 16 CPUs and 32 GiB. The original queue action overlapped briefly, for at most 18 CPUs and 36 GiB. No idle-host measurements were submitted. PB owned decomposition and placement. No request, task membership or deadline changed after publication.

## Source and acceptance limits

The intentionally unbounded measurement fixture, queue compatibility file, compatibility adapter and filesystem guard are byte-identical to c874. Fixture finite-deadline corrections preserve every original assertion and timeout value. Earlier failing records remain failed, and all eight crash-era keys remain excluded. Historical c874 evidence retains its exact source scope. The initial error-translation regression and its unchanged-main control remain recorded in `../tamper-disposition.json`.

| Requirement | Target | Incoming implementation / validation / deployment / workload proof | Candidate |
|---|---|---|---|
| PRG-03 | Mid-phase grace and terminal reason on stall | partial / partial / yes / no | Component qualified; not deployed |
| SM-01 | Gated action lifecycle and named durable transitions | partial / partial / yes / partial | Component qualified; not deployed |
| LIVE-01 | Conditional liveness with finite-I/O and fairness premises | partial / partial / unknown / no | Component qualified; no live workload proof |
| SC-03 | Reviewed target/status ledger | no / unknown / unknown / unknown | Scoped evidence only; axes unchanged |

Worker signing keys were not checked. Receipts establish recorded bytes and rereadability, without proving power-loss durability. No staged-only waiver or whole-requirement promotion occurs. Independent changed-head Principal standards/spec review, current-main batch composition, required checks and merged closing proof remain due. PE2 cannot approve its own repair. #1707 remains open and PRI-69 remains downstream. No runtime publication or service restart occurred.

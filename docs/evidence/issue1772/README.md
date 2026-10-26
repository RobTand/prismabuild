# Canary observation under feasible priority waiting

This is component evidence for the proposed [#1772 acceptance amendment](https://github.com/RobTand/prismabuild/issues/1772#issuecomment-6405727852). Independent Principal approval remains required.

The deterministic reproduction uses the production tier fixture with capacity 20, held 5, leader 18 and canary 3. It proves the protected feasible wait before advancing the fake observer clock beyond 900 seconds. Releasing the fixture obligations permits the original canary action to proceed. No historical admission snapshot was recovered.

- RED action `19ba88324f0f44f1162ae32240784088db6c9cd848e1f3978c3c06099e60eff6` fails at the intended premature observer expiry. Verified stdout SHA-256 `da130fa814428bc7c7c02bf947d787d9cc09968ab5e6d65dae3ee27374446f28`.
- Five fresh runs per architecture cover all 16 regression cases, with two xdist runs each and no skips. The final integrated cohort adds another green regression run on each architecture. Production canary and regression bytes remain identical after rebase.
- Final integrated 26-file cohorts on base `954e14171565` each report 368 passed and one inherited wire-inventory failure, tracked by #2296. Earlier branch and clean-base runs also reproduce the raw-digest failure #2297. The new base includes its repair #2311; that guard now passes. Historical failures remain recorded.

[The acceptance record](merge-acceptance.json) scopes the merge evidence. [The fleet readback](fleet-validation.jsonl) records every action key, result, host, source snapshot and bundle digest, log digests, cleanup and CAS digests. Successful receipts and payloads were checked with published `pb_verify_claim`; failed actions supply no success receipt.

| Requirement | Target | Implemented and validated here | Deployed or workload proof |
| --- | --- | --- | --- |
| LIVE-01 | Conditional liveness; observer expiry implies neither withdrawal nor containment | Leg 3 continues only rc75 on the original action; a finite operator budget is never renewed | None for this repair; eventual qualification requires contention to drain |
| ID-01 | Sealed action identity remains fixed | Regression verifies one submission and continued observation of its key | No new identity behavior deployed |
| SAFE-03 | Missing evidence never establishes success | Existing terminal, receipt and progress gates remain authoritative; adjacent component controls run | No new live-canary proof |
| ACC-07 | Actual staged reader, origin refusal and accepted progress evidence | Existing gate retained; this fixture does not run the live staged reader | Owed by a separate authorized release or workload run |

The default observer may retain a publisher's publication lock indefinitely. The Principal owns approval of that tradeoff and disposition of the inherited #2296 guard failure. No test exemption, baseline growth, scheduler exception, runtime publication, service restart or staged-only waiver is included. Receipts prove re-readable records, not power-loss durability.

The task attachment contains the reports and verifier. On the fleet, rerun `collect-evidence.py --refresh` with `PB_REVIEW_CHECKOUT` set to the review worktree to recheck logs, source bundles and CAS payloads. Submission commands are retained in `run-validation.py`, `run-followup.py` and `run-integrated.py`.

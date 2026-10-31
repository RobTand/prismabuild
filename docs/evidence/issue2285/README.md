# Canary placement before publication, issue #2285

Fresh publication previously loaded the canary only to confirm its entry point. A
box-local publisher without an eligible local GPU worker could activate a generation
before the canary's submission discovered the publisher-host requirement.

The publisher now asks the checkout driver to check the actual GPU leg before
staging or activation. PB's bounded recorded-offer matcher checks the publisher
host, GPU, image, capability and CPU1/GPU1/mem16GiB demand. The future generation
tag waits until activation. Stage-only publication and `--no-canary` retain their
existing behavior. Feasibility creates no reservation and does not prove free capacity.

Source tested: `aa4ade710927ec66279825f209244dcd75775ef6`, based on
`f3546783c361b27bba12312e08c778039af3bc99`. The required rebase was completed
once before review. Later changes in this branch contain evidence only.

## Regression and negative controls

The new box-local regression failed before the fix with `DID NOT RAISE SystemExit`.
Action `3d2bce6731de747129dad773e2ae3e24ceb2854a2cd3b7746c0ce449a64d9d4b`
recorded one failed test. Its private mirror activated instead of refusing
impossible placement.

Three controls ran through published `pbtest` on copies of the fixed checkout:

| Control | Action | Result |
| --- | --- | --- |
| Remove placement preflight | `a26b695577b953e5e9e311d9e12581802f6cebb12fe896e020fb46e4b8fdf31f` | 10 failed, 3 passed |
| Require preflight for disabled and stage-only canaries | `440565ad239b332e761669648c6213d11444a5e6fcbbb38c169c3aed0346b967` | 2 failed, 11 passed |
| Require the unpublished future generation | `b52aac76f3efb7dd0e22961b5ead75902ebf205aaff285b3486dec4955096d00` | 1 failed, 12 passed |

No pre-existing test was removed or weakened. Four publication fixtures now bypass
only the new preflight seam while retaining their existing driver assertions.

## Integrated fleet validation

Each touched test file ran five times on x86 and ARM, twice under xdist. Run 1
also included eight neighboring canary files. Distinct `--durations` values gave
each repeat a distinct sealed action. Every run used the published `pbtest`,
priority -10, one native thread per worker, a 300-second action deadline and
1,800-second client wait. The default eight clients remained enabled. Aggregate
maximum demand was 20 CPUs, 48 GiB and zero GPUs.

| Architecture | Run | Workers per shard | Actions | Passed |
| --- | --- | --- | --- | --- |
| x86 | 1 | 1 | 13 | 217 |
| x86 | 2 | 1 | 5 | 135 |
| x86 | 3 | 1 | 5 | 135 |
| x86 | 4 | 2 | 5 | 135 |
| x86 | 5 | 2 | 5 | 135 |
| gb10 | 1 | 1 | 13 | 217 |
| gb10 | 2 | 1 | 5 | 135 |
| gb10 | 3 | 1 | 5 | 135 |
| gb10 | 4 | 2 | 5 | 135 |
| gb10 | 5 | 2 | 5 | 135 |

All 1,514 test executions passed, with zero skips. The 66 successful actions and four
expected failures are indexed in [actions.jsonl](actions.jsonl). Each row names
its exact terminal record, stdout log, recorded hashes and, for success, CAS
receipt, claim and payload. Terminal records, stdout bytes and digests were read.
`pb_verify_claim(hash_payload=true)` passed claim, receipt and payload checks for
each success. `attestation_verified` remained null. Expected failures had no
successful receipt. This proves the selected source paths, not a live publication.

The readback queried the exact 70 full action keys from the named reports, with
a ten-second deadline per MCP call, stdout tails capped at 1,000 lines and
131,072 bytes. Full retained logs were hashed independently. Every response was
complete, without timeout, unavailable records or ambiguous terminals. No claim
of whole-queue coverage is made.

On a fleet-mounted checkout, recheck the retained evidence bytes without executing
any test or publication:

```bash
python3 docs/evidence/issue2285/verify.py
```

Original submission commands, reports, mutations and the MCP claim verifier are
retained at `/mnt/shared/paperclip-runs/PRI-173/`. `validate.py` names the exact
file selection and demands. `verify.py` in that retained directory performs the
original published-MCP readback; its immutable per-report cache is under
`verified/`. The task attachment contains the same inputs for inspection.

[The merge acceptance record](merge-acceptance.json) states target, source
implementation and deployment status per scoped requirement. Independent review
and batch merge remain pending. No runtime generation, activation, restart, live
canary, staged-read workload or power-loss durability is claimed.

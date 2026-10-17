# Batch ownership and final composition

The batch owner now captures one commitments generation and answers named
questions about batches, prewrites, materializations, charges and retirement.
The lifecycle callers retain their locks, conditional reads and effect order.
`output_contract` owns identity and record vocabulary. `BatchLedger` owns batch
addresses and file intake/publication; `BatchCapture` owns transitions within
the captured generation. Own-write, foreign-write, retirement and spool path
policies remain separate questions over shared facts.

The six documented `produced_output` public bindings from issue #955 remain
direct identities of their actual owners. SDK version 6 and all 90 SDK exports
remain unchanged. Deprecated internal forwarding functions are removed.
`produced_output.template_path` owns the canonical template path; existing
bounded template readers keep their input validation and read points.

The isolated C13 delivery is `56e8606a98743039913dab2e8fa4b8f6ded2e10a`.
Root composed it with the qualified tier, census, residency, lifecycle, record
and drain owners at `7439ed043867a532c8ea120e3e133c3eb40a574a`. The seven jointly
changed functions match their parents after exact owner substitutions. The
obsolete observation readers stay deleted.

Review found one behavior change in the captured retirement adapter. An empty
`retiring: {}` record became a truthy `OriginDeleteIntent` object. The tick
could start unlinking without first filing a truthy durable intent. A crash
then left the batch accepting new consumers. The original implementation files
the intent first. Commit `6384103e9` restores that predicate through
`entry.is_retiring`, whose definition is `bool(self._entry.get("retiring"))`.
The three new controls interrupt the real tick before unlink and check the
public state and consumer refusal for absent, corrupt-empty and filed intents.

Root also expanded collapsed expressions without changing their syntax trees,
removed eight unused orchestration imports and three obsolete import-only
error adapters, reused the existing scope/prewrite address owners, and corrected
current owner references in comments and docstrings. The final executed source
is `1840aa0f0c4b53866a5363a7766399a63381e424`. The source audit checks every changed
module after the declared substitutions; error, lock, authority and file-effect
rules remain with their owners.

The evidence root is
`/mnt/shared/prismabuild-agent-evidence-rob/refactor-20261016/active/batch-ledger-completion`.

| Source and scope | Python 3.12 | Python 3.14 |
| --- | --- | --- |
| Original d2f plus the identical new controls and eight architecture cases | 11 passed | 11 passed |
| Extracted 56e plus the identical controls | 10 passed, corrupt-empty failed | 10 passed, corrupt-empty failed |
| Initial Root composition, 57 whole files | 614 passed | 614 passed |
| Corrected Root composition, 14 affected whole files | 307 passed | 307 passed |

The causal pairs are in `ROOT-empty-retirement`; the two composition cohorts
are in `ROOT-composition-gates` and `ROOT-corrected-composition-gates`. Root
independently checked actual interpreter/ELF bytes, source bundles, requests,
logs, input/result CAS, complete native outcomes and teardown, and every
physical cleanup flag. Failed controls retain their failure records and have
no success receipts.

The isolated C13 domain gate passed 2,134 cases with one local-ZFS capability
skip on Python 3.12. Its focused 45 cases passed on both interpreters. The
separate 863-case namespace gate finished with 447 passes and 416 failures.
Its population and primary/runtime/cleanup records are verified; qualification
is false. Root's C31 current adapter ports, source inventory and fresh current
captures remain open. Historical capture bytes are unchanged.

The following checks read retained source or execution records and run locally
without importing product code:

```bash
python3 docs/evidence/audit_c13_completion_equivalence.py \
  --checkout CHECKOUT --output source-proof.json
python3 docs/evidence/verify_c13_retirement_regression.py \
  --root EVIDENCE/ROOT-empty-retirement --output causal-proof.json \
  --python312-copy PYTHON312_ELF --python314-copy PYTHON314_ELF
```

These are scoped source and native qualifications. Combined C09/C12/C15
composition, final compatibility captures and whole-refactor gates remain
separate work. This change grants no runtime publication, deployment, workload
performance or physical rollout credit.

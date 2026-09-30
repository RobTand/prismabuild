# Experimental checkout-entry publication validation

CPU slice for PB #811. This is not a production checkout cache or a claim-path
speedup. No fleet runtime, retention policy, cache default, bulk-reader tier,
sealed campaign input, or staged-read requirement status changes.

## Finding

The experimental `verify_entry` checked pack and index bytes but ignored the
`manifest.json` that `build_entry` published. It accepted absent or malformed
publication records, records bound to another runtime generation/bundle, and
records claiming different pack/index identities. A positive memo also survived
manifest deletion or replacement because its stat identity covered only the
pack and index.

## Repair

- Include the publication manifest in the no-symlink regular-file identity.
- Read it with the existing stable no-follow reader and strict JSON decoder.
- Share the binding grammar between the experimental writer and verifier.
  Compare typed fields: the generation, verified CAS-bundle framing, actual
  pack/index names, sizes and digests, advertised refs and object format.
- Bind the expected generation to the validated executing generation already
  resolved by the harness. It is not inferred from a caller's cache manifest.
- Recheck the entry identity before memoizing verification. A changed manifest
  invalidates a positive memo; missing or inconsistent publication fails closed.

The existing CAS-bundle digest check, Git index verification, private-copy
verification and repository parity checks remain independent. Publication
metadata is not a reader lease, retirement proof, power-loss guarantee, deployed
support, or production-performance evidence. Those #811 acceptance steps remain
open. The staged-read ledger is intentionally unchanged.

## RED witness

PB action `a619312a3ee581cf3cc416e35c18d33eab694e0a2fbb1f78e2553a0022dea588`:
one attempt, terminal failed/exit 1 on sparky, **11 failed / 1 positive control
passed / 0 skipped**, 12 collected/ran/outcomes, no reconciliation gaps.
CPU-only, two worksteal workers, one native thread per worker, CPU 2/memory
2 GiB, priority -10, deadline 600 s. Failed actions publish no successful CAS
receipt; the terminal failure and pytest log are the evidence.

Pre-fix failure lines:

```text
E       AssertionError: unpublished or misbound cache entry accepted
E       AssertionError: manifest change reused a positive memo
```

Artifacts:
`/home/rob/tmp/claude-campaign-20260926/tmp/p2p3/prismabuild/811-publication-red-ts.json`
and `811-publication-red-ts.log` in the same directory.

## GREEN witness

Final-source PB action
`bed75cb0d8e086155340d4476237ad48b8df7606fa55a25a02149c7c975a8ab6`:
one attempt, done/exit 0 on sparky, **14 passed / 0 failed / 0 skipped**,
14 collected/ran/outcomes, no reconciliation gaps. The same CPU-only resource
contract was used as for RED. The result covers the publication regressions and
existing experiment-boundary tests; it uses synthetic pack/index bytes and a Git
double, not a production checkout or a real Git-pack qualification.

CAS receipt SHA-256:
`59073b739b34072c216e96b5facc7c98e12f9bc6101cfea99f7e5273f0978d3f`.
The terminal record, result byte length and result SHA-256 were checked. Full
worker attestation was not independently verified. Receipt and log:
`/home/rob/tmp/claude-campaign-20260926/tmp/p2p3/prismabuild/811-publication-final-green-ts.json`
and `811-publication-final-green-ts.log` in the same directory.

Submission (with `WINDOW_ACTIVE` absent immediately before the client):

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbtest.py \
  --checkout /home/rob/tmp/claude-campaign-20260926/wt-sol-pb-ts-6 \
  --python /home/rob/venvs/pb-cpu/bin/python --tag gb10 \
  --shards 1 --workers-per-shard 2 --threads-per-shard 1 --mem-gb 2 \
  --priority -10 --timeout-s 600 --wait-s 600 \
  --pytest-args '["--dist","worksteal","--durations","20"]' \
  --json /home/rob/tmp/claude-campaign-20260926/tmp/p2p3/prismabuild/811-publication-final-green-ts.json \
  tests/test_diag811_entry_publication.py tests/test_diag811_experiment_boundaries.py
```

The scoped merge record is
[`diag811_entry_publication_acceptance_2026-09-30.json`](diag811_entry_publication_acceptance_2026-09-30.json).
The staged-read ledger is not promoted. No new timing or delta has been measured.

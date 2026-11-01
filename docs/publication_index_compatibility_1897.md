# Publication-index capture handoff

The `publication-index` catalogue kind covers the advisory
`prismabuild.publication_index.v1` wire in three cases:

| Case | Exact writer output |
| --- | --- |
| `publication-index/leaf` | One facet bucket's `entries/<key>.<generation-hash>.json` |
| `publication-index/namespace-coverage` | `publication-index/v1/coverage.json` |
| `publication-index/bucket-coverage` | One facet bucket's `coverage.json` |

`compat_publication_index_adapters.py` calls `index_publication` for the leaf
and `Reconciliation.step` for the coverage markers. Every reader calls
`publication_candidates` after installing the incoming bytes and marking the
consumer boundary. Unknown fields are additive. An invalid schema prevents
complete candidate discovery.

Directory metadata, filesystem trust and the settled clock are explicit fixture
observations. Canonical row data lives in a private store. These cases prove
wire behavior, not physical durability, concurrent reconciliation, deployment
or a complete live support window.

The existing `historical_absence` procedure checks every immutable member of
each retained closure. It must find neither this wire ID nor any declared
reader or writer before recording an absence. An absence grants no historical
codec credit. The retained a174 closure and prepared bridge keep their existing
provenance and support classifications.

The PB lead owns capture and adoption through the procedure in
[Supported-window compatibility](compatibility_corpus_2026-10-16.md).
At the handed-off source pin, the admitted capture command is:

```sh
python3 tests/compat/support/record_compat.py \
  --generate /mnt/shared/paperclip-runs/PRI-193/publication-index-capture-SOURCE-01 \
  --kind publication-index
```

The handoff manifest supplies the exact source SHA and replaces `SOURCE` in
that output path. Capture produces `goldens.jsonl`, `historical.jsonl`,
`absent-participants.jsonl`, `qualification.json` and per-operation attestations.
The expected population is three current captures and one historical codec
observation or explicit source-bound absence per case and retained generation.
The generator reproduces each writer in independent isolated processes.

`test_publication_index_compat_source.py` checks source readiness without
adopting captures. The existing controller corpus suite receives all three
cases from the catalogue. Review requires accepted capture/adoption provenance,
the successful source-bound primary audit, release compatibility coverage and
focused/mandatory guards on both required architectures at the final integrated
source. Prior test cohorts keep their original source identity.

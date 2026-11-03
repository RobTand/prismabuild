# Shared public evidence view (#2701)

`prismabuild.public_evidence.public_evidence_view(value)` is a pure, stdlib-only
projection for export writers. It returns a fresh JSON-compatible envelope:

```json
{"schema":"prismabuild.public_evidence.v1","evidence":{"status":"running"}}
```

This is **derived evidence, not a canonical receipt**, authentication result or
verification API. No original schema, producer mapping or `receipt_sha256` is
exported, forged or recomputed. Receipt bytes, digests, verification semantics
and private recovery state remain the responsibility of their existing owners.
Keep the original object for private recovery and canonical verification; never
replace, persist over or rewrite historical immutable records with this view.

## Exact approved positions and values

At the root, a built-in `dict` is an evidence record; a built-in `list` contains
records or further lists. Within records, only these case-sensitive keys exist:

| Field | Accepted value |
| --- | --- |
| `status` | Exact built-in string: `pending`, `ready`, `running`, `succeeded`, `failed`, `cancelled`, `withdrawn`, `interrupted`, `timed_out`, `unknown` |
| `returncode` | Exact built-in int, signed 32-bit (negative signal codes included) |
| `action_key`, `action_manifest_sha256`, `stdout_sha256`, `stderr_sha256` | Exact built-in string, 64 lowercase hexadecimal characters |
| `started_at`, `finished_at`, `observed_at` | Finite exact built-in int/float Unix seconds, `0 <= value < 253402300800` (before year 10000); no timestamp strings or inferred conversion |
| `result` | Built-in dict containing only `sha256` (same hash validator) and/or `bytes` (exact built-in int, `0 <= value < 2**63`) |
| `children` | Built-in list of evidence records or nested lists under this same contract |

No other field or position is approved. For example, a status inside an unknown
mapping is not discovered or promoted. A result descriptor cannot carry child
records or additional metadata. Lists omit all scalar elements, including raw
launch argv. Unknown fields of **any spelling or case** are omitted without
traversing them. Arbitrary diagnostics, launch descriptions and raw recovery,
claim or control mappings are not export carriers. No blacklist, `str`, `repr`,
object serializer or scalar fallback is used. Non-string keys, subclasses of
built-in scalars/containers, booleans used as numbers and malformed approved
values are omitted. This API emits no logs or input-bearing errors.

Approval is by field **and position**, not just value shape. A hash-shaped value
under an unknown name is omitted. As with any explicit data contract, callers
must not deliberately relabel private values as approved public hashes; a
projection cannot tell a digest from a capability with identical bytes.

## Recursion, limits and purity

The root starts at depth zero; every visited list/record edge counts one level.
Branches beyond depth 16, containers longer than 1024 entries, ancestor cycles,
and visits after the first 4096 record/list nodes are omitted. Result descriptors
also have the 1024-entry limit and do not recurse. Empty nested containers are
omitted; empty root evidence is `{}`. These are omission limits, not truncation
or completeness certificates. Unknown mappings are never visited.

Approved keys are inserted in sorted order; list order is retained. Each call
uses fresh containers, with no input mutation or shared mutable references.
There is no clock, I/O, global state or publication. Results are deterministic
and idempotent: `public_evidence_view(public_evidence_view(x))` equals
`public_evidence_view(x)`, including at limits. A root carrying the exact public
schema marker is treated as an envelope: only its `evidence` is revalidated
with the same depth/budget. The marker is not trusted to bypass validation.

## Adoption boundary

This issue introduces only the shared API, its tests and this contract. Existing
final-record and public-report writers are **not wired here**. The dependent
adoption issue (#2702) must reuse this API, not fork field filters. Attempt
completion, authentication, canonical receipt verification, durable record
formats, placement and staged-read identities are unchanged. This source-only
addition establishes no deployed runtime support, staged-read conformance or
workload completion. The design and staged-read contract/ledger keep their
separate implementation, validation, deployment and workload-proof axes.

## Status command endings

Part 3a of #2718 connects the ordinary `pbstatus --json` `endings` writer
in [`main()`](../tools/fleet/pbstatus.py) to `public_evidence_view`.
The writer calls the local `_public_ending_adapter` for each raw census row.
It emits one `{schema, evidence}` envelope per row in source order.
An empty source list stays `[]`.
The adapter copies each row.
It maps the presentation status with this table:

| Source status | Public status |
| --- | --- |
| `executed`, `cache_hit` | `succeeded` |
| `failed`, `refused` | `failed` |
| `withdrawn` | `withdrawn` |
| `waiting` | `pending` |
| `running`, `cancelled`, `interrupted`, `timed_out` | Same name |
| `superseded`, recovery-unavailable, `unreadable`, missing, non-string, all other values | `unknown` |

The q41 rule uses only root markers.
A root `status` of exactly `superseded` maps to public `unknown`.
A root `current is False` with a dictionary-valued `superseded_by` also maps
 to `unknown`, even if the recorded status is `executed` or `failed`.
The adapter does not inspect that dictionary's contents.
A `superseded` history summary or `supersedes_terminal` alone does not mark
 the containing row as superseded.
A current successful row with history still maps to `succeeded`.
A `current` value of `None`, `current_unresolved`, or a terminal conflict
 alone does not establish supersession.
The adapter does not decode the action-client IPC wrapper.
It does not change unrelated census `state="superseded"` sections.

The adapter copies `timing_finished_unix` to `finished_at` only when present.
It does not use the `finished_unix` sorting fallback as execution evidence.
Census rows have no `started_at` or hashes.
The adapter does not invent them.
The shared view validates all evidence and omits all unapproved fields.
The adapter adds no separate filter.
Raw rows still control completeness, diagnostics, exit codes and the table.
Export does not change private verdicts, source rows or canonical receipts.

### Writer and test inventory

The only changed writer is the ordinary `main()` JSON object's `endings`
 value in [`tools/fleet/pbstatus.py`](../tools/fleet/pbstatus.py).
The focused tests are in [`tests/test_pbstatus.py`](../tests/test_pbstatus.py):

| Test | Coverage |
| --- | --- |
| `test_json_endings_are_public_evidence_envelopes_in_source_order` | Real lane and pool-schema records; exact ordered output; success, failure, cancellation, recovery-unavailable, both superseded forms and current success with history |
| `test_json_endings_map_statuses_through_the_public_table` | Every status rule; missing and unhashable values |
| `test_json_endings_recognize_superseded_only_from_root_markers` | q41 triggers and controls; no wrapper decoding |
| `test_json_endings_carry_no_private_sentinel` | Nested capability, claim, control and argv sentinels; leak assertion only on `endings` |
| `test_json_export_leaves_source_rows_and_records_unchanged` | Raw rows, record bytes, canonical receipt bytes and hashes, private status, exit code and every other JSON key |
| `test_public_ending_adapter_is_type_safe_and_does_not_mutate` | Fresh mapping; shared hash, size, return-code and timestamp validation; no fabricated time |

Only JSON assertions change in these existing tests in that same file:

- `test_endings_name_their_transport_and_their_slurm_state`
- `test_a_withdrawal_is_an_ending_and_is_listed_from_withdrawn`
- `test_an_ending_names_the_actions_own_status_in_its_own_column`
- `test_a_receipt_puts_the_ending_under_done`
- `test_a_record_that_cannot_be_parsed_is_a_row_not_a_silence`
- `test_an_unreadable_record_says_which_way_it_is_unreadable`
- `test_a_readable_ending_carries_no_unreadable_note`
- `test_a_purged_job_reads_as_unknown_rather_than_none`

Private-field assertions read census rows separately.
The purged-job test also passes raw census rows to `ending_lines`.
Existing terminal assertions keep their meaning.
`test_recent_bounds_how_many_records_are_read` checks the empty-list boundary.
`test_json_is_one_object_with_the_three_lists` checks the outer object.
The unchanged
[`test_pbstatus_ending_completeness_review.py`](../tests/test_pbstatus_ending_completeness_review.py)
checks incomplete reads and exit code 3.

**The safety claim covers only the structured `endings` value.**
It does not cover the whole command.
Part 3b / #2747 owns the deferred terminal table, notes, stderr, `scheduler`,
`unavailable_sections`, queue-root diagnostics and other JSON sections.
The operator-local section below describes view support, not adoption here.
Source support is not deployed runtime support.

## Operator-local view

`prismabuild.operator_view.operator_ending_view(record)` (#2726) calls the
unchanged `public_evidence_view` first, then adds only the following validated
root details inside `evidence`:

| Field | Additional accepted value |
| --- | --- |
| `host` | An exact built-in `str` matching `^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$` with `fullmatch` semantics: 1–253 ASCII characters, alphanumeric first, then alphanumeric, dot or hyphen. Trailing newlines, non-ASCII look-alikes, whitespace and slashes are rejected. |
| `transport` | An exact built-in `str` in the existing transport vocabulary derived from `submission_types.SubmissionRequest.transport` using `typing.get_type_hints` and `typing.get_args`; no copied transport literals or new vocabulary constant. |
| `status` | Only the exact built-in `str` `superseded` overrides the public result. Every other status is exactly whatever the public projection produced, including omission (not an inferred `unknown`). |

Keys are accepted only with exact built-in `str` type, by iterating the source
mapping's items rather than looking up untrusted keys. Subclasses are rejected;
there is no `str()`/`repr()` fallback. No free text, paths, argv, claim or control
fields, notes or reasons are added. The chosen transport source has a stdlib-only
import closure (`collections.abc`, `dataclasses`, `pathlib`, `typing`); it defines
types and helpers without import-time I/O. It avoids importing the heavy
`submission` or `fleet_submit` modules. This view derives transport membership
from that one existing annotation rather than adding a third transport constant.

**Implementer's interpretation, not additional issue requirements:** the return
shape is the same `{schema, evidence}` public envelope, with extras only inside
its root `evidence` record. Extras apply only to an ordinary root built-in `dict`
within the public size limit, never to children, result descriptors or list
roots. Callers with a list of endings must apply the function per record. An
otherwise empty public projection still receives a valid host (or transport or
exact superseded status). A root already carrying the exact public schema marker
is treated as an envelope and is **public-only**: no extras are extracted from
either its outer mapping or its unwrapped `evidence`. Unknown or invalid markers
leave an ordinary record. Re-passing an operator result to `public_evidence_view`
strips host, transport and superseded; the marker never bypasses revalidation.

The public depth, node and container-size limits are unchanged. The extra scan
runs only at depth zero and reuses the public root size limit (1024 entries);
over-limit roots get no extras. All recursive work, including cycles, remains
with the public projection. Results use fresh containers without input mutation.

This is **operator-local terminal output only**, for an authenticated operator on
the fleet host. Host placement, transport and superseded lineage are useful in
that local context but are deliberately not approved for public export. The
result must never be passed to writers: never saved, uploaded, published or used
in JSON output or machine records. `public_evidence_view` remains the only view
for those boundaries. This issue introduces the view only, with no production
importers; #2718 owns any later terminal-only adoption and its separate review.

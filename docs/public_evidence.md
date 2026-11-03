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

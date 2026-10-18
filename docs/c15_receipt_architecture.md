# C15 receipt ownership

The source base is `a962ed3fb2767ce6805110a18124f3cf01a166a3`.
This component work preserves SDK version 6 and existing receipt bytes.
The original card is [C15](../prismabuild-architecture-review-2026-10-13.html#c15).

## Two candidate structures

Candidate A gives a captured `GenerationReceipt` ownership of structural
identity, manifest membership and digest comparison. A caller reads once at
its established observation point, captures those bytes, then requests named
operations. Constructors perform no I/O. Caller adapters keep authorization,
read budgets, file custody, seals, caches and their error vocabulary.

Candidate B puts JSON decoding and digest comparison in a smaller leaf.
Separate supervisor, updater, publisher and retained-worker adapters validate
their complete receipt shape. This preserves the present duplication of the
schema, generation, commit and manifest rules. Adding a policy registry would
hide those differences without assigning ownership. I reject B for that reason.

Candidate A wins provided its capture remains structural. A receipt never
confers root authority. The shared owner must not perform a whole-generation
scan during a role handshake or a selected-member read.

## Caller usage and early signatures

```python
# Existing root budget and symlink rules stay at this boundary.
raw = bounded_read(root / "RUNTIME_VERSION.json", 1024 * 1024)
receipt = GenerationReceipt.capture(raw)
receipt.require_identity(root.name)
for relative in selected:
    data = bounded_read(root / relative, MAX_MEMBER)
    if not receipt.matches_bytes(relative, data):
        raise ValueError(f"published client hash mismatch: {relative}")

class GenerationReceipt:
    @classmethod
    def capture(cls, raw: bytes | str) -> GenerationReceipt: ...
    def require_generation(self, generation: str) -> dict: ...
    def require_commit(self) -> object: ...
    def require_identity(self, generation: str) -> dict: ...
    def member_digest(self, relative: str) -> str: ...
    def matches_bytes(self, relative: str, data: bytes) -> bool: ...
    def matches_file(self, relative: str, path: Path) -> bool: ...

class SourceDeclarations:
    @classmethod
    def capture(cls, source: bytes) -> SourceDeclarations: ...
    def protocol(self, name: str) -> int | None: ...
```

`observe` and `compare` retain the C17 identity-only contract. The requirement
for one strictness level cannot apply to both legacy identity observation and
integrity. `role_session` accepts nonempty identity strings without a schema or
manifest. `upgrade_client.desired` instead requires the published schema,
directory binding and 40-hex commit before reading privileged client blobs.
`runtime_publication` additionally requires nonempty canonical members before
copying into root custody. These are distinct questions. Integrity uses one
structural owner, with explicit additional authority and file questions.

Source capability declarations come from strict module-level AST literals.
Duplicate, nested, computed or ambiguous declarations cannot establish support.
No target source executes to discover a capability. Historical malformed source
controls must precede the deliberate replacement of regex semantics.

## Ordered boundaries

`supervise._published_generation` proves a direct authorized store child,
captures the receipt, then selects and hashes the supervisor. Failure retains
the current supervisor. `publish_runtime._sealed_generation` proves the sealed
directory, captures once, then explicitly verifies every member.
`_barrier_generation` asks for the selected updater's rollout declaration.

`upgrade_client.desired` keeps enrollment, the 1 MiB receipt budget, 4 MiB
member budgets, optional selection and exact copy/journal/recovery ordering.
`runtime_publication` keeps root custody, approval HMAC, the protected copy lock,
private path restrictions and stamp caches. `resource_scope` keeps the retained
store binding, seal and literal path checks and verifies the worker and selected
proxy closure. A present invalid proxy never becomes an absent candidate.

Scalar metadata readers keep their coercion, defaults, conditional reads and
error behavior. Core's attested single-file contract remains outside the change.
The source audit records embedded Python, aliases and fixture importers as well
as literal receipt paths. Frozen historical fixtures and golden records stay fixed.

## Installed reader sequence

The reader imports the existing `digest_primitives` owner, so the installed
closure has two leaves. The unpublished `6fa9` proof bridge does not establish
transport or recovery support for either leaf. Its worktree and evidence stay fixed.

A separately named prepared bridge is derived from fixed historical updater
source. It remains standalone and reads receipts through its old implementation.
It understands transport, updater-only loaded identity, and recovery presence
for `generation_receipt.py` and `digest_primitives.py`. An explicit receipt-member
recovery declaration fences transactions that can add or remove those leaves.
The final production updater has no inline receipt fallback.

The intended forward sequence is old updater to prepared standalone bridge,
bridge with no leaves to bridge with the exact leaf pair, then final updater.
The reverse sequence first restores the bridge while retaining the leaf pair,
then removes leaves under the bridge. The immutable old updater does not know
this closure: a private negative proves direct old-to-final transport installs
the importing updater without its unknown leaves, and its next intake fails.
The standalone bridge refuses unsupported final activation; final refuses a
reverse transaction to an unaware updater before staging, drain or journal
mutation. Root's C31 preparation and deployment gates must separately reject
the unsafe direct transition. Dependency bytes precede the importing
updater when copying; recovery must retain a runnable reader at each boundary.
The journal is recovered before a final updater needs its receipt reader.
C18's captured installed proof and previous proof metadata remain authoritative.

The movement publisher installer copies digest and receipt owners before its
consumer. Root reviews exact source bindings and crash controls. Prepared source
support grants no deployment, bridge convergence or rollback designation.
Root owns C31 integration and the final combined suites.

## Preserved weaker questions

`observe` requires only nonempty string identity fields. The retained wrapper
requires schema, generation and a files mapping, without a commit requirement.
The root updater validates identity before selecting files, preserving native
errors for a missing or malformed files field. Its existing string-form
40-hex commit question also accepts a 40-digit JSON integer. Publication and
retained-worker readers preserve that coercion; the supervisor separately
requires a string. Ordinary JSON duplicate keys remain last-wins wherever
the original caller used ordinary decoding. Existing strict wrapper decoding
remains strict. These differences are named operations, not configurable flags.

The capability change is a separate correction. Exact old publisher bytes
accept duplicate, nested, function-rebound and string-embedded declarations,
and fail to bind a second source read to the selected manifest. Five private
old-source REDs retain those outcomes. Current controls require a unique
module-level integer literal, allow an inline comment, and bind source bytes
to the receipt. No target module executes during capability discovery.

`receipt_owner` selects development sources only from explicit source layouts.
Root-owned code in those layouts must satisfy source and directory custody.
Installed layouts always validate the installed directory and each bounded,
nofollow regular leaf. Both captured sources compile before either executes.
Recovery runs before this loader, including when installed leaves are missing.
No alternate package, bytecode cache or legacy reader supplies a missing leaf.

## Explicit installed bootstrap and operator restoration

The amended P tool is a separate root operation for the exact a174 designation.
It installs the three final leaves before the inactive final updater, using the
existing copy operation in private same-filesystem staging. It writes only those
four live paths and keeps an independent journal and recovery closure. The
upgrader timer and service remain persistently fenced until acceptance. The
broker and serving processes keep running.

The publisher's explicit `--installed-bootstrap ASSESSMENT` activates an already
sealed generation against fresh authenticated root observations of a complete
final installed reader. It verifies PB qualification and source identities under
the publication lock. The pending marker excludes ordinary publication, survives
coordinator loss and retains the original a174 intent after a pointer move.
In-band process convergence and the existing mandatory canary run under the lock.
The default automatic direction checks and barrier guard remain unchanged.

The persistent designation retains a174's exact directory, commit, receipt and
content pins. `mode: operator-restoration` and `automatic_reachability: false`
mean rollback restores the journaled standalone updater before pointer reversal
while retaining `digest_primitives.py`, `generation_receipt.py` and
`park_proof.py`. This does not authorize automatic rollback, leaf removal or an
unpublished bridge. Actual root, crash/reverse and final record-window receipts
remain release gates. See the operating guide's installed bootstrap procedure.

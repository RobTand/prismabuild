# Supported-window compatibility

This work starts at `416a1eccebaea4ef224e0ddef8f6a1fe424756b4`.
The branch is `sol/refactor-compatibility-corpus-20261016`. The initial
T3 worktree at `2d9ee5191ef6e7ed79c59786b59f1ed59bc22a6f` was clean.
C16, B14, B15, resident test cleanup and C25 documentation remain in the base.
No children, external messages, publications or PR changes are authorized here.

## Contract and traced ownership

Card 31 requires a catalogue of wire identity, storage location, field policy,
accepted historical shapes and actual readers and writers. D10 adds both
reader directions, every writer variant, byte goldens and retained import
closure attestation. The accepted source review is
`20261015T003846Z-codex-to-claude-d01-d10-source-reply-3f753f.md`, D10,
in the project mailbox. The original card and frozen completion audits remain
historical records.

`residency_map.write_fragment`, `write_map` and `write_landing` validate then
atomically serialize documents. `read_fragments`, `read_map` and `read_landing`
reach the same domain validators. The validators close header and nested field
sets. `residency_map` imports `storage_tiers`, which imports
`filesystem_capacity`; a standalone captured reader cannot exercise this path.
`reader_lease` adds material, pin and retiring documents beside fragments.
`PoolQueue` owns queue publication and diagnostic writers. Its readers also
apply key, generation, identity and evidence rules, so a JSON parser cannot
substitute for them. Core and embedded helpers have separate attested imports.

Rob defines support as live worker generations plus a designated rollback
target. For this proposed refactor the target is explicitly
`a17477b2b206-1791745175-65dffa700dfb`, commit
`a17477b2b206adf79d89e65c7b9d09584f0bfa34`. This designation changes no
published pointer, deployed setting or support policy for future rollouts.
Retained directories alone confer no support. Offline WSL is excluded.

The complete read-only status refresh observed three live hosts, `dl380g10`,
`sparky` and `sparklina`, all on that commit. The original status and capture
times are retained in evidence. The Git object is absent from the shared
repository. The published receipt and every retained source file must therefore
be preserved and checked by SHA-256. The capture tool records Git availability
instead of claiming Git independently reproduced bytes it cannot read.

## Design alternatives

One design centralizes runtime codecs and imports all owners to register them.
That changes import and attestation contracts before proving compatibility,
and couples unrelated readers to optional fleet tooling. It is rejected.

The chosen design uses immutable metadata with lazy references to real owners.
Qualification selects concrete production adapters. Source inventory separately
enumerates schema literals, constructions and embedded callers without importing
product code. A new ID or unqualified construction is visible as a coverage gap.
The catalogue must not turn discovery into a claim of qualification.

The capture tool archives the complete receipt-sealed `src`, `tools` and `fleet`
trees, including dynamic import siblings and data. It verifies each source hash
before writing a new private capture, and preserves the exact raw receipt in
the archive. The runner uses `python -I -S`, replaces `sys.path` with those
trees and standard-library paths, and attests imported paths and bytes. No
installed package or candidate module may satisfy an old production import.

## Baseline

Published PrismaBuild action
`35f302be6c7db2d691f3036c02f069636bb56d2b506afc33980c9afc29ef9ba3`
passed 128 cases across residency maps, leases, egress receipts, denials,
resident-set records and claim shape. There were no skips. The worker was
`dl380g10`; controller exit was zero. The terminal, request, logs, source bundle,
runtime and receipt are retained under the evidence directory.

Bulk evidence belongs under
`/mnt/shared/prismabuild-agent-evidence-rob/refactor-20261016/active/compatibility-corpus/`.
No historical bytes are repaired in place. Failed attempts remain there.

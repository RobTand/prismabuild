# Slice 3a architecture

Caller: capture v2 cursor and coverage, read authoritative tier state, subscribe by exact tier ID, retain unconfirmed IDs inside the original read budget. Later consumer slices install that loop.

Grounding: InotifyQueue installs directory and byte-leaf watches before names-only reconstruction. Topic adapters own bounded paths and IDs; InputShapes owns coverage predicates. Current adapters cover host inputs, no tier paths. TierLedger owns existing tier-reservations paths; RecordStore owns tier/funding/commitment filenames. Claim-order is inside the commitment JSON, so it needs no new authoritative record or duplicate topic.

Candidate A: add tier cases directly to queue_bell_topics, HostInputShapes and InotifyQueue. Small initial diff, but duplicates the host-only dispatch and hardcodes more namespace roots in overflow/reconstruction. Rejected because watcher traversal would acquire domain knowledge.

Candidate B: first move namespace reconstruction selection into adapter dispatch and provide a composite input-shape contract. Keep existing host naming and behavior stable. Then add one bounded TierInputs adapter with topics tiers, tier-reservations, tier-funding and tier-commitments, plus separate TierInputShapes. Choose B: path knowledge stays in adapters and coverage checks, shared watch installation gains no tier conditionals.

Sketch:
TierInputs.topics -> tuple[Topic, ...]
TierInputs.allows(relative: Path) -> bool
TierInputs.hint(relative: Path) -> tuple[str,str] | None
TierInputs.hidden_input(relative: Path) -> bool
TierInputs.reconstruction_inputs(relative, entries) -> iterable
TierInputShapes.kind(relative: Path) -> str | None
InputShapes.kind(relative: Path) -> str | None
DirtyInputs.invalidate(topic, identity, deadline=...) retains dirty identifiers for all declared nonlegacy inputs; existing DirtyHostInputs callers migrate in the same wave if renamed.

No generic funding TTL: source bindings are generation-bound. Existing InputObservation.deadlines carries the actual validated policy horizons; later consumers must arm them. No consumers or live generation activate here.

Blocking first steps: preceding merge verified; RED fleet action c88879726fe5756e444ca4739ebfc1c6b44b6ebddb6a2c9c39ec27730b84b48d has 15 expected failures.
Independent workstreams: read-only path audit; source adapter implementation; deterministic tests and contract docs.
Shared mutable state: separate task worktrees and commits; merge only after independent source review.
Smallest safe decomposition: one slice and one PR; do not implement later consumers.

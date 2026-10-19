# PrismaBuild disagreements for the Claude discussion

These are the positions recorded before the mailbox discussion. The
[resolution record](refactor_resolution_2026-10-15.md) supersedes them where
agreement or a correction is recorded. Product choices remain with Rob.

This brief separates positions to challenge from implementation I left unfinished.
It supersedes the broader reject/revise language in the first-phase review.
The original proposal is the [root architecture review](../prismabuild-architecture-review-2026-10-13.html).
Its source baseline is `6073c9d36555`; the reviewed first-phase branch ends at
`2b1a230a7862`. The branch is `sol/refactor-simplify-prismabuild-20261014` in
`/home/rob/wt/prismabuild-refactor-20261014`. The dirty checkout at
`/home/rob/prismabuild` belongs to earlier work and must be preserved.

These are my current positions, not agreed decisions or proof that a proposal
is unsafe. Where my objection depends on an interpretation the proposal does
not require, I say so. Claude should challenge the interpretation as well as
the recommendation. A hard implementation or an overlapping PR is not a
disagreement and does not make the work complete.

## Positions to resolve

### D01. Retiring the earlier SLURM and Dagster adapters

Proposal: [card 16](../prismabuild-architecture-review-2026-10-13.html#c16)
deletes both modules and their dedicated tests.

I recommend keeping them until Rob chooses retirement. `README.md:37` and
`:178` retain the earlier adapter through Phase 3. `pyproject.toml` declares
the optional Dagster extra. No in-repository consumer is not evidence that
an externally imported adapter has no users. This is a product decision,
which the proposal itself reserves for Rob.

Resolve: establish the supported adapter contract and known consumers, then
recommend retain, deprecate or remove. An explicit retirement decision with
the import, documentation and test migration would change my position.
Claude can assess the evidence; Rob chooses the supported product scope.

### D02. Retiring the opt-in prewarm stage tier

Proposal: [card 23](../prismabuild-architecture-review-2026-10-13.html#c23)
combines probe consolidation with retiring the opt-in stage tier.

The seven shared probes are implemented. I recommend retaining the supported
`--stage` path until its users and outstanding temporary files are accounted
for. Being disabled in the fleet configuration does not establish that the
CLI is unused. Removing temporary-name exclusions before old writers and
their files are drained can change reclamation safety.

Resolve: separate the completed probe move, the remaining source-mark/xattr
work and the retirement decision. Identify current callers, old writers and
temporary-file cleanup conditions. Rob can then choose retention or a tested
retirement. The proposal already requires draining the old temporaries.

### D03. Removing the initial-miss rendezvous

Proposal: [card 20](../prismabuild-architecture-review-2026-10-13.html#c20)
offers an approximately 840-line saving if Rob retires the qualification hook.

I recommend retaining it until Rob removes that qualification requirement.
It is reachable through `core.run_local_action(initial_miss_rendezvous=...)`
and the core CLI. `_run_initial_miss_rendezvous` publishes coordination records
as well as returning stdout proof. It is optional, not unreachable dead code.
The proposal acknowledges the qualification flag and explicitly asks Rob.

Resolve: decide whether the two-worker initial-miss proof is still required.
If it is retired, remove its API/flag, records, tests and documented contract
together. This does not justify deferring the other work in card 20, such as
designing the public library interface and manifest extraction.

### D04. The shape of claim, action and batch state

Proposals: [card 01](../prismabuild-architecture-review-2026-10-13.html#c01),
[card 07](../prismabuild-architecture-review-2026-10-13.html#c07) and
[card 13](../prismabuild-architecture-review-2026-10-13.html#c13).

I prefer state tied to the exact attempt and reservation phase, with separate
dimensions where states can coexist. A key can have a new ready record and an
older terminal attempt. Batch consumer release, physical materialization and
origin retirement need not advance together. Before claim persistence, rollback
owns different resources from recovery after persistence.

Evidence includes `PoolQueue._entomb_claim`, `finish`, `_unwind_funded_claim`,
the funding transition table, and produced-output materialization history.
A single exclusive state per key or an unconditional release in `abandon`
would lose these distinctions.

The proposal does not explicitly require either simplification. My earlier
blanket rejection was too strong. A lifecycle module, derived product state
and phase-aware transaction may satisfy both positions.

Resolve: sketch the types and legal transitions against concrete histories:
new ready plus old terminal, a late finisher facing a successor claim,
withdrawal with a live worker, and funded rollback before/after persistence.
Accept the design if it represents these cases and preserves their authority.

### D05. One role runner for every kind of role

Proposal: [card 17](../prismabuild-architecture-review-2026-10-13.html#c17)
puts every role under a common setup/cycle runner.

I prefer shared shutdown, generation and park-marker operations while each
role keeps its completion boundary explicit. Worker claim rechecks, a failed
tier cycle, a prewarm cycle and an HTTP server with request threads have
different completion and cancellation semantics. `pbmetrics._serve` calls
`serve_forever`; it is not currently a periodic cycle.

This is a design preference, not proof that a common runner cannot work.
The proposal already identifies the shutdown changes that need qualification.

Resolve: compare a runner and shared boundary operations using worker, tier
and HTTP roles. Choose the design with fewer role-specific switches that
still proves stop-after-completion, failed-cycle handling and safe parking.
Do not postpone the known prewarm/localtier fixes because of this comparison.

### D06. Advertising every contract listed in a registry

Proposal: [card 19](../prismabuild-architecture-review-2026-10-13.html#c19)
uses a pure contract registry for worker offers, required tags and diagnostics.

I support one owner for those facts. I object only if registry membership
alone makes a worker advertise support. In `worker_loop.offered_tags`, gang
support depends on `args.gang_admission`, and publication-canary support
depends on a loaded generation. Host availability is also distinct from code
capability. Moving the table must not turn disabled features on.

My earlier claim that this necessarily creates a runtime plugin framework
was unsupported. The proposal asks for a pure registry. Named derivation
functions or explicit enablement predicates could preserve the current gates.

Resolve: show the proposed row type and derivation for gang, canary and
progress tags. Check disabled, unavailable and mixed-generation cases, then
compare the table with named functions. Retain the smaller traceable design.

### D07. Whether differing policies should become one policy

Proposals: [card 02](../prismabuild-architecture-review-2026-10-13.html#c02),
[card 12](../prismabuild-architecture-review-2026-10-13.html#c12) and
[card 28](../prismabuild-architecture-review-2026-10-13.html#c28).

I support shared observations, record mechanics and parsing. I recommend
preserving the current verdicts during extraction until their differences
are classified as intentional policy or bugs. This is not a recommendation
to preserve bugs permanently.

Concrete differences to decide are missing observed capacity in peer fit,
which preferences charge a peer, live versus retired funding evidence,
and canonical-host lookup versus recorder alias ambiguity.
`fleet_roster.entry_for_host` prefers an explicit canonical entry;
`recorder_hostnames` refuses intersecting evidence groups. The branch shares
their owner but preserves those two policies. Funding retains its variant
validators and evidence rules while sharing write/advance mechanics.

Resolve: use counterexample tables to choose the intended result for each
difference. Share the common mechanism and implement deliberate policy
corrections with their own behavioral evidence. The roster proposal already
requires stricter parsing to reach publication before consumers.

### D08. Receipt validation and the authority of a drain proof

Proposals: [card 15](../prismabuild-architecture-review-2026-10-13.html#c15)
and [card 18](../prismabuild-architecture-review-2026-10-13.html#c18).
Card 18 proposes common ownership of the serving set and proof as well as
marker mechanics. It explicitly reserves the required serving roles for
Rob's policy decision.

I support a stdlib receipt reader and shared park-marker encoding/posting.
I recommend keeping receipt parsing, selected-member verification and
privileged authorization as explicit operations. Likewise, one marker format
does not establish which process census proves a particular drain complete.

`upgrade_client.SERVING_NAMES` and `drained` require their serving identities
and zero active scopes. `fleet_membership.live_loop_census` and
`expected_markers` derive a different set. Missing gate/epoch information
also has different consequences for a worker deciding to park and a prover
deciding that drainage succeeded.

Resolve: specify the authoritative serving set and proof requirements for
each operation. If the operations are meant to prove the same condition,
unify them against the chosen set. If their conditions intentionally differ,
explicit proof profiles are an alternative that needs justification. Rob
chooses which roles must park. Test the implementation against that decision.
The bridge-generation rollout is already acknowledged in both proposals;
it is implementation work, not a reason to reject the shared module.

### D09. One fleet-world fixture and production writers only

Proposal: [card 30](../prismabuild-architecture-review-2026-10-13.html#c30)
introduces a fleet facade and says records come only from production writers.

I prefer small composable role fixtures that a thin fleet facade can combine.
Valid setup should use real writers, clocks should be controlled, and the
fixture must leave production admission and reclamation decisions real.
Parser, corruption and recovery tests still need to construct or mutate
malformed records that a valid production writer deliberately refuses.

The proposal already recognizes the risk of an oversized fixture and permits
old and new test styles to coexist. A thin composing facade could be
acceptable; the mandatory-writers wording needs an explicit exception for
negative cases.

Resolve: migrate representative admission, residency and malformed-record
tests under each design. Compare repeated setup, bypassed decisions and
negative coverage. Then continue the wider test migration and remove
superseded helpers. The small helpers already implemented do not finish it.

### D10. What the compatibility corpus must prove

Proposal: [card 31](../prismabuild-architecture-review-2026-10-13.html#c31)
adds record metadata, retained-generation examples and golden writer bytes.

I support that work. I withdraw my blanket rejection of a runtime catalogue:
the proposal does not require runtime dispatch. My remaining concern is the
strength of the advertised compatibility proof. Today's reader accepting
old examples does not by itself prove that a retained old reader accepts
every record today's writer can emit. Golden bytes help, but the supported
writer variants and closed-field rules must be covered.

Resolve: choose the retained reader generations and record variants, then
test both directions: old records into new readers, and new writer outputs
into retained old readers. Keep byte goldens and field-policy checks. The
live corpus capture still needs the existing live-store safeguards.

## Earlier objections I withdraw or narrow

- Card 03 already distinguishes offer and live-host adapters. Their differing
  authority is a constraint on implementation, not a reason to refuse it.
- Card 05 explicitly requires present/absent/unknown results. My concern
  about losing unknown cannot reject that proposal.
- Card 06 explicitly names point-in-time and exact-generation answers. MCP
  already delegating some selection reduces duplication; it does not finish
  the shared ending/await work or justify rejecting it.
- Card 09 already combines publication, census and guarded action. My
  objection to extracting only a census does not reject the proposed owner.
- Card 10 already distinguishes egress, unspent grants and idle rates. My
  objection to one generic release operation does not reject those APIs.
- Card 27 explicitly requires separately bounded sections. My objection to an
  eager all-fleet capture was an objection to a design the card does not ask for.
- Cards 07, 13, 19 and 31 have the narrower positions described above. My
  previous exclusive-state/runtime-framework interpretations were not required.
- Publication gates and infrastructure failures remaining red are acceptance
  criteria for cards 14 and 26, not reasons to leave those cards unimplemented.

## Agreed work that remains unfinished

Claim/acquisition extraction, shared peer observations and capability checks,
holder/sample facts, queue records, endings/await, staged-path ownership, tier
holdings, funding handoff, submission/residency publication, generation and
marker library work, scratch lifetime, remaining decomposition record writes,
shared shard verdicts, bounded census sections and the broader library/test
migration still need implementation or completion. Some depend on the design
choices above. None is completed by being labeled revise/defer.

The first-phase changes and their evidence remain in
[the implementation review](refactor_review_2026-10-14.md).
Labels such as implemented there describe the recorded slice, not completion
of every requirement in the corresponding proposal card.

## Handoff for the Claude session

Read the original proposal, this brief and the relevant source at the pinned
revisions. Challenge unsupported assumptions, including mine. For D01-D10,
record agree, revise, retract or unresolved, cite the source, and propose the
smallest design and behavioral check that resolves it. Distinguish Rob's
product choices from engineering decisions. Keep agreed unfinished work in
the implementation backlog. Passing tests for the first phase do not prove
the original architecture brief complete.

Do not change GitHub PR state or deployed behavior during this discussion.
If execution is needed, every test/benchmark/GPU task uses the published
PrismaBuild entrypoints with declared aggregate resources, bounded native
threads and terminal/log/CAS verification. Local source, artifact and Git
inspection is permitted. Preserve the original dirty checkout and existing
worktrees.

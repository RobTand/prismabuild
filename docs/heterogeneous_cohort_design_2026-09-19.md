# Heterogeneous admitted cohort: group barrier plus external-demand reservation (2026-09-19)

Design for [issue #567](https://github.com/RobTand/prismabuild/issues/567):
the admitted primitive for one measurement spanning hosts in two or more
classes. Diagnosis only was corrected in #651; nothing here executes,
admits, or changes a default. The privileged `nfsd` intervention stays a
separate needs-decision item and is deliberately not folded in.

## What is missing, precisely

Three gaps, each read off current main:

1. **One action carries one scope.** A pool `--measurement` may take
   `--host-class` (`tools/fleet/pbrun.py:3310`, which refuses `--anywhere`,
   not a class), and `host_class_scope` (`:3344`) seals it
   `platform_keyed` — one platform, one toolchain. The x86_64 server and
   the two aarch64 readers cannot be sealed as one measurement unit.
2. **No group barrier.** A campaign row is a `pbrun` command line and
   nothing else; the vocabulary is closed (`tools/fleet/pbcampaign.py:262`,
   unknown fields refused at `:645`). `run_windowed` (`:809`) publishes
   rows independently and refills as they drain. A wall-clock value in each
   payload is a rendezvous, not a barrier: a late claim misaligns the
   window and nothing prevents it.
3. **No external-demand reservation.** A measurement already excludes PB
   co-tenants in both directions (`src/prismabuild/adaptive_cpu.py:601-610`,
   `measurement_holder`; fresh idle required at `:583`). What is missing
   is demand the action does not own — root-owned `nfsd` threads on the
   server — held across the cohort rather than one attempt's slice.
   `--exclusive` is GPU capacity (`pbrun.py:5264-5265`), not service
   ownership, and the resource-authority contract excludes migrating
   external PIDs into the attempt (`docs/resource_authority.md`).

Related but different: #366 (within-class pool placement, closed), #458 /
#469 (publication barrier, not a measurement barrier). The root-owned
maintenance drain (`tools/fleet/resource_broker.py`, owner-tagged,
`maintenance_begin`/`maintenance_end`) refuses new scope creation but has
no cohort-member exception, so it is not a reserve-then-admit interface.

## Proposed primitive: a PB-owned cohort contract

A cohort is a sealed manifest above per-member actions, not a payload
convention and not a second dispatcher. Members keep their own platform
identity, placement, receipts and retry; the cohort adds exactly the joint
guarantee:

- **Cohort identity.** One `cohort_id` binds ordered role scopes (for the
  motivating case: one `x86_64` server-holder role, two `aarch64-sm121`
  reader roles) and the member action keys. Roles are ordered so evidence
  and verdicts name the same positions every run.
- **All-or-nothing admission.** Placements for every member are reserved
  before any member's window opens; a partial claim opens nothing. A
  member that fails, withdraws or times out aborts the cohort: no measured
  region launches anywhere after the abort.
- **PB-held prepare/commit barrier.** Members signal readiness to PB and
  are released together. Fail-closed: a member that is never released
  never starts its measured region, and a cohort that loses a member after
  release invalidates the group rather than reporting a partial A/B. Hosts
  and tokens are released only once the exact owned scopes prove empty.
- **Cohort-lifetime host exclusion.** Extend the existing measurement
  exclusion to the cohort's lifetime on every member host: no PB co-tenant
  on a member host from first admission to coordinated release, not just
  within one attempt's slice.
- **Declared external-service demand.** A member role may declare
  `external_service` demand (host, service, CPUs) attributable to a
  service on the target host. Admission checks it against live pressure
  and refuses the arm while foreign load is present; reservation is by a
  PB-held exclusivity window per host for the cohort lifetime, never by
  granting the action privilege over root-owned threads.
- **Aggregate evidence.** One cohort result carries the barrier evidence
  (release attestation, attempt set, lifetime exclusion) beside the
  per-member receipts. A rendezvous value may remain in payloads as a
  diagnostic; it is never the synchronisation.

## Explicitly not proposed

Weakening measurement identity (one scope per member stays), inflating
`--cpus` as service ownership, payload wall-clock as the barrier, or a
campaign-side dispatcher that re-implements placement. The benign cohort
needs no Rob-only decision; the privileged `nfsd` mask path does and stays
separate.

## Acceptance

A cohort binding ordered role scopes to member keys; a prepare/commit
barrier that releases members together; cohort-lifetime exclusion on every
member host; fail-closed abort with single release of hosts and tokens;
and an aggregate result beside per-member receipts. Until then #567 stays
open and the A/B's honest status is "runnable design, no admitted-valid
claim".

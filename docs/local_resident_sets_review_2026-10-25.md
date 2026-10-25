# Local resident sets: design review and measurement gate

Status: proposed continuation of [#1545](https://github.com/RobTand/prismabuild/issues/1545).
Principal Engineer approval is required before implementing #1732.
This document supersedes the original draft's current-state claims, not its
historical measurements. No host is enabled and no runtime is published here.

## What the caller declares

The existing declaration remains sufficient:

```sh
pbrun --resident-set SET_ID --data-manifest MANIFEST ... -- docker run ...
```

The caller names immutable inputs and the optional set. PB owns copying,
placement, leases, mount selection, and reclamation. A missing local copy must
not pin the row to a host. An input's tier policy still controls whether an
alternative source is legal. A set declaration does not waive staged-only reads.

The measurement deliverable is [the current gate record](results/gate_1730_2026-10-25.md).
The prior [v3 report](results/gate_1730_local_sets_v3.md) preserves the October 10
measurements and the October 12 supplied space ruling.

## Source-grounded ownership

At the base of this review, `b658e0912c34`:

| Boundary | Existing owner and behavior |
|---|---|
| Declaration and lease | `resident_sets.ResidentSets` stores the immutable manifest and folds the append-only lease journal. Explicit `resident_set` row references may extend an until lease, never its hard maximum. |
| Movement and integrity | `local_resident.copy` verifies each file while copying and publishes the whole tree. `sources` currently permits canonical fallback. That existing behavior is not staged-only qualification. |
| Reader lifetime | `local_resident.pin` takes the host flock, checks lease and resident state, and records action key, nonce, scope ID, and local root. `release_pin` uses the same flock. |
| Reclamation | `evict_resident_copy` takes the move lock and host flock, checks pins, records `evicting`, and renames. It deletes outside the host flock and releases occupancy only after deletion. |
| Placement | `pool` and `_gang` already prefer valid resident copies without changing eligibility. The gang preference expires 30 seconds after publication. This differs from the older draft's proposed ready-time wait. |
| Campaign close | `ResidentSets.close_campaign` and `pbcampaign close` already release matching effective leases. Close does not delete bytes or drop reader pins. |
| Container execution | `tools/fleet/docker` already enforces attempt ownership, CPU/memory bounds, and containment. It has no resident-set mount selection. Every attempt still defaults to `served_from: canonical`. |
| Enablement | `tools/fleet/local_tier_policy.json` has an empty host map. Code presence is separate from activation and live workload proof. |

The source path is declaration → immutable set and lease → host-owned copy →
attempt pin → proposed Docker mount → containment-backed pin release → eviction.
The rolling stage/RAM `tier_loop` remains a separate owner. It must not become
a second local-disk cache manager.

## Compare two designs

| Candidate | Public contract | Cost and decision |
|---|---|---|
| A: PB selects a per-container bind | Caller retains the command above. A resident-set resolver returns a local mount and pin, a permitted canonical route, or a refusal. The Docker adapter applies that result. | Recommended. Keeps source selection and lifetime policy together and preserves existing container paths. Needs measured bind behavior and exact cleanup ownership. |
| B: caller selects a local path | PB exposes candidate paths and pin operations. Every launcher validates the copy, obtains a pin, rewrites mounts, and handles fallback. | Rejected. Each caller must reproduce the same lease, pin, and source-policy decisions. That duplicates knowledge already owned by PB. |

Neither candidate adds a host-wide mount. A root namespace helper and an overlay
filesystem are outside this unit. Candidate A hides the full decision behind one
contract instead of exposing a sequence callers must coordinate. The Docker
adapter owns argument translation and container cleanup, not lease policy.

## Proposed contract for #1732

These are design signatures, not implemented APIs:

```python
@dataclass(frozen=True)
class LocalMount:
    source: Path
    destination: Path
    set_id: str
    host: str
    pin_token: str

@dataclass(frozen=True)
class CanonicalRoute:
    reason: str

@dataclass(frozen=True)
class Refused:
    reason: str

MountDecision = LocalMount | CanonicalRoute | Refused

# In local_resident.py. attempt identifies action_key, nonce, scope_id.
def resolve_container_mount(store, set_id, host, spec, attempt,
                            input_policy) -> MountDecision:
    ...
```

`CanonicalRoute` is valid only when the existing input policy authorizes that
source. Under the staged-read contract, a missing, expired, or corrupt local
copy may use an explicitly allowed staged alternative under its reader lease.
It may not silently open the declared HDD path. If no permitted alternative
exists, the resolver returns `Refused` before container launch. The original
draft's unconditional canonical fallback is superseded for staged-only inputs.
This review grants no TIER-04 exception. A diagnostic HDD leg requires Rob's
explicit authority naming ranges and reason on the sealed request.

The implementation sequence is:

1. Separate source decision policy from the Docker execution adapter before
   adding mount behavior. Reuse `local_resident.pin`, the set store, and the
   existing containment receipt. Add no second pin or lease system.
2. Acquire the attempt pin through `pin`, which already validates the lease and
   resident copy under the host flock. Re-read the authoritative copy state
   under that flock if resolution needs any additional copy fields.
3. Release the host flock before canonical-directory metadata inspection or
   any Docker operation. Compare whole-directory names and sizes with the
   manifest. The pin protects local bytes throughout this interval.
4. Add `--mount type=bind,src=LOCAL,dst=CANONICAL,readonly` only for a valid
   `LocalMount`. A missing source is a refusal, never an empty local directory.
   Never inject `-v` for this purpose.
5. On every abandoned local route, release the acquired pin immediately and
   retain the cause. On successful container creation, transfer pin cleanup to
   the existing attempt/container containment owner. Release only after the
   last container that captured the mount is proven stopped. An uncertain
   daemon reply or cleanup keeps the pin.
6. Preserve the selected mount route, set digest, host, and fallback/refusal
   cause with the container lifecycle receipt and immutable attempt outcome.
   Multiple launches must retain their individual decisions. The existing
   attempt `served_from` field is a mount-route summary for the declared set,
   not evidence that every payload read came from that tier. ID-07 still
   requires actual reader-open evidence.

No-set rows retain their prior Docker arguments. Their existing canonical stamp
remains a compatibility fact, not a new claim about staged-only compliance.
Required-local measurement semantics are deferred. A timing action can already
refuse an unavailable declared comparison arm without adding a new queue mode.

## Measurement plan and stop conditions

The current prerequisite probe observes exact available bytes and the mount
that backs each candidate. It neither reads A8S payloads nor copies a synthetic
performance corpus. Its tiny bind fixture records the immutable local image ID,
parent/nested mounts, read-only write refusal, and missing-source failure.

Do not start a full A8S copy when either D1 inequality fails. Do not benchmark
the canonical directory as HDD when `mountinfo` shows the manual NVMe overlay.
Do not substitute unrelated staged objects or a synthetic local file for A8S.

Once prerequisites permit the speed leg:

1. Freeze identical A8S files, ranges, and digests for staged NFS and local NVMe.
   PB movers prepare any needed staged copy under ordinary reservations. A
   fresh-copy experiment includes the copy and verification cost. Adoption of
   existing copies measures verification separately and never calls it free.
2. Submit each paired experiment with `--measurement`. Keep both sources in
   one action on the target host. Declare 16 CPUs for 16 readers, memory for
   processes plus the complete warm range, and one native thread per reader.
3. Interleave staged/local arms at 1, 4, and 16 streams. Retain start/end load,
   per-arm bytes, elapsed time, physical backing, and source identity. State
   client-cache and server-cache evidence separately. Advisory eviction is
   not proof of cold data. Do not call an 8 GiB pass warm under a 4 GiB cap.
4. Obtain server ARC/L2ARC counters at each arm boundary. If server-cold state
   cannot be established without disrupting another workload, report it as
   unmeasured. Do not flush global caches to manufacture a result.
5. Measure the actual consumer's read concurrency and expected reload count.
   For observed positive per-load saving S and preparation cost C, report
   `ceil(C / S)` reloads to repay preparation. If repeated-arm spread is
   comparable to S, the result is inconclusive. No positive S means no speed
   justification. Independence from NFS is a separate, explicitly priced
   product decision, not an inferred speed benefit.

For a copied set, the original phase-0 NVMe-write interference check also
remains owed. Measure it inside a controlled admitted GPU experiment with
aggregate CPU, memory, and GPU demand. An uncontrolled write alongside an
unrelated live job cannot establish the effect of the copy.

The old HDD-versus-stage acceptance leg remains owed until scoped Rob authority
exists. No bulk HDD leg is launched in this review. A no-go on a prerequisite
does not establish the missing cold/warm or consumer-payback measurements.

## Delivery gates and requirement status

| Requirement | Target | Implemented or deployed evidence in this unit |
|---|---|---|
| SC-01, SC-02 | PB owns placement, movement, and cleanup. | Existing source owners traced above. No new scheduler, application cache, or fleet configuration. |
| SC-03, ID-08, SAFE-03 | Keep target, code, validation, activation, and workload proof distinct. | This design and the linked gate record state separate scopes. A terminal probe receipt proves its own fixture only. |
| SM-02, SM-03, INV-07, SAFE-01 | Pin through use; delete before releasing occupancy. | Existing source has the pin/eviction ordering. Docker lifetime wiring remains proposed and unvalidated here. |
| INV-03, INV-06, TIER-01, TIER-04, SAFE-02 | No forbidden HDD fallback. | Proposed resolver preserves this target. Existing canonical fallback is not upgraded to compliant by this document. No policy exception is claimed. |
| ID-07, INV-04 | Attribute serving tier at actual open. | Mount probes and `served_from` cannot establish this. Consumer instrumentation remains owed. |
| BUD-01, TIER-05 | Separate capacity, occupancy, process memory, and readiness. | Exact destination headroom is measured. It does not publish residency or reserve a production set. |
| DUR-01 | State persistence limits. | Receipts establish recorded, re-readable results. They prove no power-loss durability. No storage policy changes. |

The central staged-read ledger is historical evidence, not a current fleet
census. This PR changes none of its implementation, deployment, or workload
proof axes. Its scoped acceptance record accompanies the measurement report.

The Principal Engineer reviews this design and the measurement before any
#1732 build. A later implementation PR must add deterministic tests for local
selection, legal fallback, forbidden fallback, immediate pin release, uncertain
container cleanup, and unchanged no-set arguments. It also needs fleet evidence.
The Principal Engineer releases merged runtime changes in a reviewed batch.

Before adoption or enablement, Rob still owns the unresolved D29 mover ruling,
any removal of manual systemd mounts, and any staged-only diagnostic exception.
This unit performs none of those actions. Fresh-copy headroom and adoption are
different gates: a D1 failure for another copy does not prove same-filesystem
adoption impossible. Adoption still needs a digest-verification run, active-mount
removal authority, and a window with no container retaining the recursive bind.

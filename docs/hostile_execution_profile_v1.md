# Hostile-v1 execution contract and delivery state

PB #2403 is the current tracker. PB owns the profile and its qualification;
TL2 thread `07fbf439-f22e-40d4-9e1c-b7cf2824aa57` owns TL2 source, integration
and its decision log. This document records PB source behavior and owed work.
It grants no host mutation, runtime activation or live-cohort authority.

## Current source boundary

The trusted path is `pbrun` to `submission.prepare` and `freeze_template`, then
the sealed CAS request, `PoolQueue.publish`, claim, `resource_scope`, the root
broker, `resource_payload.py`, and the worker's result collector. Today the
payload runs as the worker user. Its shell capture and result files share that
identity. Resource containment does not establish hostile isolation.

The new profile selection is separate from diagnostic `--profile` modes:

```sh
	pbrun.py --execution-profile hostile-v1 \
	  --execution-policy-sha256 ACTUAL_LANE_POLICY_SHA256 \
	  --cpus ACTUAL_CPUS --demand mem_gb=ACTUAL_MEMORY -- command
```

This is a source interface, not an executable qualification recipe. The current
published client does not expose these options and refuses them. A caller
must not emulate profile selection with environment variables or host tags.

The sealed `params.execution_profile` has exactly `schema`, `name` and
`policy_sha256`, with schema `prismabuild.execution_profile.v1` and name
`hostile-v1`. The policy digest binds the complete lane policy described below.
Changing the profile or policy changes the action key. Omission retains the
trusted request shape. Partial selectors, unknown versions and SLURM refuse.
Publication derives both `hostile-execution-v1` and `hostile-policy:<digest>`
from sealed intent. A tag alone supplies no profile. Claim rechecks demand
and both fences against the sealed request.

No worker advertises this capability. There is no execution backend or
authenticated hostile result collector in this delivery. Claim refuses before
spending an attempt, direct execution refuses before trusted cache lookup,
and ordinary worker attestations cannot certify the selected profile. This
keeps the identity work from silently running through a trusted lane.

## Policy and owner boundaries

The policy must bind the worker generation, isolation mechanism, immutable
root image, mount and environment allowlists, network policy, private cache
policy, output collector, redactor and cleanup protocol. A changed component
invalidates the affected lane qualification. Credential values belong only to
the trusted capture policy, never to the sealed action or payload environment.

The selected design extends the existing root broker with a per-slot sandbox
UID and no supplementary groups. The privileged helper owns mount, PID, IPC,
UTS and network namespaces, immutable dependency views, descriptor closure,
resource limits and syscall restrictions. Repo code receives private scratch,
cache and output paths. The trusted worker remains outside the payload and
owns status, exact-attempt log descriptors, extraction and receipt publication.

A separate per-action VM would give a stronger CPU boundary but does not cover
the required GPU lanes. A separate UID without namespaces would leave network,
host sockets and filesystem visibility unresolved. The broker namespace design
matches the existing exact-attempt resource owner and preserves trusted lanes.
The shared host kernel remains outside the hostile-code threat model.

## Guarantees and native acceptance

| Guarantee | Required mechanism and proof | Implemented/deployed state |
| --- | --- | --- |
| PB-H1 | Default-deny network before repo setup; only declared disposable fixtures share a private network; DNS, LAN, metadata, host/control-plane and provider routes fail | Owed; no hostile network backend or native proof |
| PB-H2 | Per-slot UID with no groups; allowlisted mounts, environment, descriptors and admitted devices; sentinel home, queue, CAS, host scratch and neighbour access fails | Owed; no sandbox host provisioning or native proof |
| PB-H3 | Read-only shared dependencies, private writable caches, verified trusted promotion; a weaker profile cannot satisfy a stronger key | Profile/key partition implemented; cache isolation and native poisoning proof owed |
| PB-H4 | Worker-owned status, collection and exact-attempt results; independently pinned test population; refuse replaced reports, forged output, missing/duplicate tests, runner injection and output symlinks | Unsupported-profile execution and receipt refusal implemented; authenticated collector and native proof owed |
| PB-H5 | Cross-action file/process isolation; aggregate CPU, memory, pids, scratch bytes/inodes and admitted device limits | Trusted resource containment exists; hostile enforcement and native neighbour/limit proof owed |
| PB-H6 | Exact stop proof, launch fence and private-state disposal across loss/restart; ambiguity retains capacity | Existing trusted cleanup machinery and merged #2272 are dependencies; hostile disposal and deployed loss proof owed |
| PB-H7 | Sanitize capture before any durable write, including split/encoded sentinel and failure paths; omit binary/undecodable bytes; bound retention | Owed; no integrated hostile capture or native no-raw-copy proof |

Source construction, component tests, source merge, runtime activation and
native qualification are separate facts. PB-Q1 through PB-Q7 in TL2's trust
contract remain the native acceptance questions. All attacks use sentinel
secrets, never live credentials. A fake, skipped case or controller check
does not qualify a native guarantee. A receipt identifies recorded bytes;
semantic honesty of arbitrary tests still requires review.

| Lane | PB-H1 | PB-H2 | PB-H3 | PB-H4 | PB-H5 | PB-H6 | PB-H7 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| dl380g10 CPU / x86 | unqualified | unqualified | unqualified | unqualified | unqualified | unqualified | unqualified |
| sparky CPU / ARM | unqualified | unqualified | unqualified | unqualified | unqualified | unqualified | unqualified |
| sparklina CPU / ARM | unqualified | unqualified | unqualified | unqualified | unqualified | unqualified | unqualified |
| sparky GB10 | unqualified | unqualified | unqualified | unqualified | unqualified | unqualified | unqualified |
| sparklina GB10 | unqualified | unqualified | unqualified | unqualified | unqualified | unqualified | unqualified |

Route benchmarks wait for lane-qualified PB-H1 and PB-H2. Live cohorts and
third-party imports wait for the complete profile. Trusted TL2 work retains
its existing provenance restriction and accepted risks. No schedule is a
delivery guarantee. Host changes require concrete proposals and the missing
operator authorization; approved production `tl2` identity/datasets, VM and
sandbox naming/privacy decisions are not requested again.

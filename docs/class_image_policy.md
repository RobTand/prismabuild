# Class-image policy: prepared, not approved or activated

`tools/fleet/class_images.json` is a v1 declaration using the existing
`prismabuild.container_class_requirements.v1` schema. Its `classes: {}` is
**deliberately pending**: GB10 evaluates as `class_unconfigured`. It is not an
approved, deployable fleet image policy, and does not assert that an empty set
satisfies GB10. No production required name, comparable content or store root
has been adopted. The schema and the GB10 class selector are existing PB contract
values, not newly inferred image approvals. x86 has no GB10 selector or inherited
requirements.

## Approval ledger and sequencing

Inspected source: PrismaQuant revision
`6d8288adc94b3c4b624c143ed372fb6513f32784`,
`prismaquant/kernels/joint_projection_reduce_qualification.json`.
It qualifies `prismaquant-glm-derivative:causal-exp-v1-20260908` for the recorded
joint-projection campaign runtime, shapes and device. It is a provenance lead,
not an exhaustive GB10 required-set approval or a Docker store-root approval.
Its digest
`d0256efb83294e879ca33dd2d3131e861221c415ac5b024c2415e51c5467f026`
uses PrismaQuant's `container_image_content.v1` projection. PB's comparable-content
projection differs: prepending `content:sha256:` would not translate that value.
The name and digest therefore have **not** been inserted into PB requirements.
Historical Spark inventories, smoke images, arbitrary hashes and fixture
`/var/lib/docker` roots supply no production authority.

Outstanding inputs, all unresolved:

* An approved exhaustive required-name set for GB10, including whether the above
  campaign name belongs in the fleet set and which other names are required.
* PB-comparable content references for every approved required name, observed and
  approved under PB's projection rather than converted from PrismaQuant hashes.
* Approved production Docker image-store roots for the intended endpoints.
* A completed GLM Stage A receipt. None was established by this package.
* W3 real-system qualification of Docker, GPU and kernel mediation availability.

Rob's GLM Stage A prerequisite remains binding. Reviewed requirements and support
belong in the next immutable runtime generation **after** that prerequisite and
necessary approvals; this change performs no live activation, staging deployment,
Docker/GPU qualification, or production state mutation. W3 owns real-system
evidence. Compatibility capture and strict adoption remain with the PB lead
(#2098); existing archival-runtime/TMPDIR issues #2162/#2178 are not repaired here.

## Publication and worker behavior

Both published fleet-tool layouts contain the same hashed, sealed requirements
member. GB10 roster entries select the generation-relative member. Selected data
is mandatory and schema-validated before publication; missing, unreadable,
malformed and invalid-schema selections refuse staging. Worker loading remains
bounded and generation-contained, without checkout fallback. A selected file
failure retains a non-None failed policy (`class_config_unavailable`), publishing
named evidence and continuing native discovery/admission instead of restoring
legacy container admission. Extra installed images remain irrelevant.

## Supported execution boundary

Native claims intentionally do not require image declarations. At actual
`run`, `create`, owned `exec` and their `container` aliases, the shim obtains
executor-owned configuration, then re-observes bounded inventory and evaluates
the existing class policy, including freshness, completeness, required names,
comparable contents and store identity. A previous offer/verdict is not authority.
Qualification precedes creation intents and daemon work-start. Inventory queries
and ordinary native work continue during class drift. Existing declared-image
sealing and claim-time checks are unchanged.

The synchronous executor holds an anonymous sealed memfd for each attempt, even
for explicit denial. The private locator is bound to owner/marker and the live
oldest context-holding ancestor in the same-UID lineage. Bounded process/fd scans,
process birth checks and immutable-file checks refuse uncertainty. Descendants
cannot substitute a nearer action-created context. No policy or satisfied flag
is taken from the sealed action or writable offer. Context is closed on attempt
completion and is not persisted in queue records.

On supported Linux x86-64/AArch64, Core uses a private ptrace runner to follow
ordinary descendant execs. The configured Docker executable, including absolute
execution and native children reading argv from input files, is redirected before
its user instructions to the existing shim. Interpreter identity, not merely an
argv pathname, is checked for script clients. The runner restores the executor's
private locator when redirecting; child environment replacements are not authority.
The sealed action and argv are not rewritten. Runtime identity attests the private
mediation and qualification helpers alongside Core.

A shim task may forward its daemon operation once; the forwarding permission
ends at that exec, not at process-tree exit. A client's ordinary helpers and
subsequent configured-client executions are mediated again, including exec
replacements. Class qualification precedes missing ownership metadata handling:
omitting owner or marker cannot turn work-start into inventory passthrough.
Linux nonleader-thread exec is reconciled to its new leader TID without changing
the native program's exit status.

Profilers stay outside the ptrace mediator; their actual relay/payload subtree is
mediated, including native descendants. Native py-spy sampling uses
`--nonblocking --subprocesses` so it does not compete for ptrace ownership, and the
profiler remains the process receiving profiler-flush signals. Nonblocking
sampling can omit inconsistent individual samples; it does not relax profile
usability requirements. Host remote-memory-read policy still applies; no
privileges, kernel settings or Docker installation are changed.

Only the local system Unix-socket Docker endpoint is qualified (the existing
`/var/run/docker.sock` and `/run/docker.sock` spellings). Context overrides, alternate
hosts and non-system endpoints cannot reuse local-store evidence. Unsupported
work-start routes such as start/compose remain refused rather than being claimed
as protected by PATH. Raw Docker APIs, copied/alternate clients, hostile same-UID
process debugging, and arbitrary payload confinement are **not** supported or
claimed; #2150 owns the broader same-UID runtime boundary.

The runner needs working ptrace exec/fork tracing, register access, sealed memfds
and readable genuine procfs. If these facilities are unavailable, it refuses
rather than executing an unmediated payload. Consequently a host lacking these
prerequisites is not a qualified execution host, even for a native action on a
runtime with a configured Docker client. No privileged namespace tool, host
permission change or fallback to PATH-only protection is assumed. CPU fake-client
tests prove control flow on the admitted arms, not deployment-wide kernel or real
Docker qualification.

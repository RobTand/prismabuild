# Automatic worker client upgrades

Every worker follows the published immutable runtime generation. Supervisors
re-execute from the new generation while preserving their PID and held lock;
active worker leases survive this transition. Worker loops finish their current
action before adopting a replacement generation. Privileged
clients live separately under `/opt/prismabuild-resource-broker`; publication
alone cannot replace those root-owned files.

Enroll each host once after installing a resource broker that supports the
maintenance protocol. As the authorized publishing user, stage both files on
local storage so NFS root squashing remains enabled:

```sh
pb_enrollment_dir=$(mktemp -d /tmp/pb-client-enrollment.XXXXXX)
cp /mnt/shared/prismabuild-fleet/repo/tools/fleet/{install_client_upgrader.sh,upgrade_client.py} "$pb_enrollment_dir/"
# Run this local installer using the host's authorized root administration method:
sudo bash "$pb_enrollment_dir/install_client_upgrader.sh"
```

This installs `prismabuild-client-upgrade.timer`, which checks after boot and every
60 seconds (plus up to ten seconds of jitter). The root-owned configuration at
`/etc/prismabuild/client-upgrade.json` explicitly authorizes the publisher of
`/mnt/shared/prismabuild-fleet/runtime-generations` to supply privileged client
code. This is an administrative trust delegation: manifest SHA-256 values prove
copy consistency, **not publisher authenticity**. Access to publish generations
must therefore be restricted to principals authorized to administer these hosts.
The updater executes only root-owned local Python under isolated interpreter
mode; it never imports or executes Python directly from the shared mount. To
preserve NFS `root_squash`, the updater starts its own root-owned export program
with real/effective/saved UID and primary GID set to the configured `reader_uid`
(default 1000), with no supplementary groups, before reading shared files. The
child returns bounded manifest and base64 member bytes through a local temporary
file; the root parent independently checks member names, identity, sizes and
hashes. It never changes shared permissions or requires root access to NFS.
Enrollment on root-squashed clients likewise copies the installer and updater
to local storage as the authorized publishing user before root installs them.

The desired manifest is the active generation's `RUNTIME_VERSION.json`. Its exact
hashes select `resource_broker.py`, `resource_payload.py`, `gpu_memory.py` and the
updater itself. The updater resolves the generation once, requires it to belong
to the enrolled store, rejects member symlinks, verifies copied bytes against the
manifest, and stages them locally. A generation change with identical client
hashes needs no broker restart. Activating an older generation intentionally
converges clients back to its bytes, provided it contains the maintenance-capable
broker and updater; a pre-enrollment generation lacking these members is refused.

Before replacing files, the updater asks the broker to close admission under the
same lock used by scope creation. Existing actions retain their scopes and finish
normally. Timer ticks report `draining` until the broker proves zero active
scopes. Only then does the updater stop the service, replace the verified local
files, start it, check health while admission remains closed, and reopen admission.
The `/run` drain gate is a worker-facing v1 mirror. Its canonical authority is
the root-only host-local `/var/lib/prismabuild-resource-broker/maintenance.json`,
so a named drain survives a host reboot and the broker restores the mirror before
it admits work. Begin commits the durable closed state before the mirror closes;
release commits the durable open state before the mirror opens. If the final
mirror publication fails after a release, the running broker retains admission
closed and reports an error; restart recovers the committed state. If the
volatile gate is absent, even a durable open record first becomes a persisted
`client-upgrade` boot hold. Only the updater's current-client, loaded-hash and
health checks can release it. Restoring an old open record cannot bypass boot
initialization. A missing, corrupt, untrusted, or previously initialized-but-now-missing
canonical record fails closed. Only the first adoption may migrate a valid old
`/run` gate, and it writes an adjacent initialized marker before the canonical
record so erased evidence is never mistaken for a new installation.
Scope creation racing with
the gate receives a retryable maintenance refusal. Unreleased scopes and unknown
or populated groups prevent upgrading; an idle-looking process list is not proof.

Workers treat a missing gate as closed, including during boot before this
independent root timer runs. If the installed files already match the desired
generation and the broker proves its loaded hashes and health, the updater
initializes a missing gate through an owned begin/end pair. It waits for zero
active scopes before release, including on later ticks that find its own drain
already held. A present open gate needs no extra begin/end; an unreadable gate
is not treated as missing. `current` requires an explicit open gate readback.
Broker errors, missing desired bytes or another drain holder keep admission
closed without restarting or terminating work.

Deploy this worker/updater pair together through normal publication, then
verify both loop generations and installed updater hashes on every host.
Workers awaiting initialization depend on the enrolled updater being healthy;
an old already-current updater does not create a missing gate. This requires
no installer or wire-protocol change, and normal journal-bound rollback remains
available. The resource-broker installer explicitly initializes the canonical
open record on a proven fresh installation; a rolling adoption instead migrates
the existing valid gate, preserving any named hold. Initialization refuses any
existing canonical record, marker or legacy gate, including dangling symlinks.
The upgraded updater requires both candidate broker and updater to support
durable maintenance once its installed broker has that capability. Missing or
stale running capability also refuses the transition before drain or service
mutation. First converge that updater/broker generation everywhere;
an older updater can still perform a pre-convergence rollback and consequently
does not preserve the reboot guarantee. Fleet rollout builds on this durable hold with an immutable epoch and two
quorums. See the [publication procedure](operating_prismabuild.md#publish-the-runtime-the-fleet-executes).
The updater persists `rollout-epoch.json` beside its local transaction state,
reads the shared epoch through a bounded unprivileged export, and retains its
drain until the shared resume decision proves every roster host rotated. It
verifies that decision's participant hashes and rechecks local health, installed
bytes and generation-bound parked processes before release. Missing, corrupt or
unavailable evidence retains the hold; coordinator timeout does not reopen it.
The pointer and epoch records are independently atomic reads, so a pointer move
can be observed between a desired-receipt read and a refreshed epoch view. That
coherent transition retains the durable hold and retries on the next tick; it
does not publish a participant failure. Verified local install, recovery, or
service-start failures still restore local bytes, retain the hold, and publish
the failure that permits coordinated rollback.

Both source and target generations must contain the same participating updater.
Deploy a new updater through an explicitly reviewed rolling bridge first and
verify its installed SHA-256 on every host. Once installed, an epoch-aware updater
refuses candidates without epoch support. Existing-generation rolling activation
still needs its own compatibility assessment; a symlink move is not proof of
installed client or worker compatibility.

During an epoch, journal recovery and transaction failure restore verified local
bytes while keeping admission closed. The coordinator may restore the source
pointer before resume, wait for every host's rollback proof and then authorize
release. Durable local state distinguishes a host that has released from one
still waiting, so a crash between release acknowledgement and shared posting can
retry its acknowledgement. Boot loss of the volatile gate requires fresh local
rotation proof before reopening. A completed epoch can be cleaned up locally
even after a subsequent generation has become live.

A drain records the holder that opened it, and only that holder reopens
admission. The updater states `client-upgrade` and releases nothing else: a tick
that finds a host already current leaves an operator's stop in place and reports
`held` with the holder's name, and a transaction that meets one stops before
touching the service. A caller that states no holder, including any client older
than this change, opens a drain recorded as `unattributed`; such a drain carries
no claim and stays releasable by every root caller, which is what lets a drain
opened through one broker close through the broker that replaced it mid-upgrade.
`maintenance_force_end` is the way past a holder who is gone, and it stamps
`forced_end_of` and `forced_end_by` into the gate so a forced release never
reads as an ordinary one. Beginning a drain that is already open never rewrites
the stated reason or the time admission closed.

A root-owned transaction journal and previous file copies precede service stop.
Failure restores the previous files and verifies the restarted broker before
reopening admission. A subsequent invocation recovers an interrupted transaction
first. Recovery rechecks live broker idleness before stopping it, so a crash after
reopening admission cannot interrupt newly admitted work. Failed recovery retains
the journal and reports an error. Upgrades serialize with a local file lock.

Inspect each host's `/var/lib/prismabuild-client-upgrade/status.json` or run:

```sh
/usr/bin/python3 -I /opt/prismabuild-resource-broker/upgrade_client.py --status
systemctl status prismabuild-client-upgrade.timer
journalctl -u prismabuild-client-upgrade.service
```

Status reports desired generation/commit/hashes, actual installed hashes, check
time and `current`, `draining`, `held`, `updated`, `rolled_back`, `rollout_*` or
`error`. `pbstatus` and the MCP status response include a bounded fleet rollout
summary with the pending phase and missing hosts. A timer being
active alone does not demonstrate convergence. Check the latest record and the
broker's health; preserve transaction and journal evidence on failure.

The service unit and enrollment configuration remain root-managed. Changing
those requires an explicit installer update. This mechanism upgrades PrismaBuild
clients; it does not upgrade Python, system packages, drivers or unrelated tools.

## Introducing a new privileged dependency

A deployed updater's recognized member set is part of the rollout contract.
Publishing a broker that requires a new file together with an updater that knows
that file does not suffice: the old updater copies only its known members, the
new broker cannot start, and rollback restores the old updater again.

Use an expand/converge sequence for `gpu_capacity.py`:

1. Publish a **bridge generation** containing this dependency-aware updater and
   the existing broker, payload helper and GPU-memory module. The bridge must
   not publish `src/prismabuild/gpu_capacity.py`. Existing four-member updaters
   can adopt this generation through their normal drain/health transaction.
2. Verify every enrolled worker's actual `upgrade_client.py` SHA-256 equals the
   bridge receipt and its timer/status is healthy. A generation pointer or a
   single worker's success is insufficient. Do not publish the dependent broker
   while any worker still has the old updater.
3. Publish the generation containing the new broker and
   `src/prismabuild/gpu_capacity.py`. The bridge updater recognizes the module
   only when that exact source path is present in the desired receipt, exports
   and rehashes it through the existing unprivileged reader, and installs the
   entire member set while the drained broker is stopped. The new broker's
   health response must include `gpu_capacity.py` in its loaded module hashes.

This requires two ordinary runtime publications and automatic timer convergence;
it does not require another per-host installer or manual client copy. Existing
work remains protected during both transactions. A future dependency unknown to
this updater requires an analogous bridge before its first dependent broker.

The transaction journals the union of old and desired members. A `null` previous
hash means that an explicitly recognized optional file was absent before the
upgrade; no fabricated empty-file backup is used. Rollback restores verified
previous bytes and removes newly introduced optional files before starting the
old broker. Conversely, a failed downgrade restores any removed dependency.
Required core members may never be journaled as absent. This existence state
survives interruption and is checked before service mutation. Health compares
exactly the modules actually installed: the old three-module broker is checked
without requiring the optional module, and the new four-module broker must
attest it. Activating a generation without the optional dependency removes it
inside the same stopped-service transaction.

A transaction involving an optional file also requires both the installed and candidate updaters to
declare `CLIENT_UPGRADE_PROTOCOL = 2`. The transaction can restart into its candidate or restored previous
updater after a crash, so its ability to interpret optional-file absence must
be established before closing admission. A candidate without this declaration
is refused before any service mutation, including a downgrade to a pre-bridge
updater while the optional file is installed. Use a dependency-aware bridge
with the old broker and no optional source as the rollback generation. Once
that removes the optional file safely, a separate later publication can restore
a pre-bridge updater if explicitly intended; reintroducing the dependency would
then require bridge convergence again.

`park_proof.py` is a separate updater-owned optional member at
`src/prismabuild/park_proof.py`. It participates in export, exact installed
hashes, the previous/target member union and presence/absence recovery. Broker
loaded equality excludes only this leaf and `upgrade_client.py`; GPU modules
remain broker-owned. `CLIENT_UPGRADE_PROOF_MEMBER_PROTOCOL = 2` declares this
transport/recovery contract, including prior proof metadata preservation. The
unpublished protocol 1 intermediate is not a capable rollback reader. The proof
protocol does not claim strict marker acceptance.
Whenever either side names the proof leaf, both the captured previous updater
and verified exported candidate must declare that capability as well as the
existing optional-member protocol 2.

One no-follow regular file descriptor supplies bounded previous proof bytes and
uid/gid/mode together. Missing files are explicit absence; unavailable, oversized
or nonregular files refuse before journal or service mutation. Worker-owned or
worker-writable regular files remain repairable through the existing owned
zero-scope drain, without receipt MATCH or physical park evidence. Unsafe captured
proof ownership or write mode still takes that repair path when client byte
digests already match. A trusted equal-byte installation stays current without
staging or service restart. Whenever the
member union includes the leaf, the journal requires `previous_proof_file`: null
for known absence, or exact integer `uid`, `gid`, `mode` fields for a previous
file. Missing legacy metadata is unknown and cannot authorize rollback. Recovery
validates it before service I/O, restores uid/gid and then permission mode on an
exclusive temporary file, fsyncs, and atomically replaces the installed path.
Root recopy therefore cannot turn retained worker-controlled bytes into trusted
code. The existing GPU/optional-member protocol 2 remains independent.

First converge a non-consuming bridge. An old installed reader exports only its
known names and can adopt that bridge while the proof source remains in the
shared runtime for roles. A subsequent fresh bridge tick introduces the leaf.
A direct candidate driver with an incapable installed rollback reader refuses.
Once introduced, downgrade to an incapable updater stays fenced. Rollback to a
capable bridge preserves the leaf. First-introduction failure can restore its
actual previous absence. The GPU instruction to publish a leaf-absent bridge
does not apply to this role-required shared source.

The updater captures one bounded installed byte map under its local upgrade
lock. Capability checks, previous digests and backup writes all use that same
map, including optional absence. Candidate capability uses the already verified
export bytes. The lock serializes cooperating updater transactions; independent
administrative replacement of installed files during a transaction is outside
this guarantee. Such changes must take the same lock or wait for the timer
transaction to finish.

The retained bridges do not import or consume proof code. The strict consumer
loads only the root-trusted installed `park_proof.py` sibling when physical
proof is requested. One no-follow regular FD supplies bounded coherent bytes
and uid/gid/mode. The updater hashes and compiles those same bytes in a fresh
module, requires its actual strict API, and removes temporary module registration.
It does not consult import caches, bytecode, the shared runtime or package code.
Missing, unsafe or incapable proof code yields unknown evidence, including with
an empty selected census.

Host drain and rotation use the installed owner's `HOST_QUIESCENT` profile,
exact gate keys and regular markers. Rotation retains its additional generation
paths, supervisor and worker requirements. A rollout rechecks the same captured
owner's drain identity after epoch refresh and before posting `drained`; its
existing positive numeric wire restriction remains additional to finite physical
identity. No future profile membership or reader-window default is selected.

Ordinary zero-scope repair remains independent of successful physical proof or
receipt MATCH. The loader is lazy: construction, current-member checks and
transaction recovery do not require a trusted proof leaf. Another named holder
and active rollout epochs retain their existing limits. Bridge qualification
requires deterministic captured-byte replacement and fresh-process journal/copy/
recovery histories. The private two-host transport qualifier uses a root
container with its installed updater and proof under private `/var/lib`.
Its runtime and rollout reads and marker writes use the installed program's
actual unprivileged subprocesses. The coordinator and fixture writes use worker
filesystem credentials. Native ARM/NFS reader evidence and all four distinct-host
scenario outcomes remain required before integration. Its fake broker and
process census do not establish physical-fleet quiescence.

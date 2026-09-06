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
The persistent drain gate survives a broker restart. Scope creation racing with
the gate receives a retryable maintenance refusal. Unreleased scopes and unknown
or populated groups prevent upgrading; an idle-looking process list is not proof.

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
time and `current`, `draining`, `updated`, `rolled_back` or `error`. A timer being
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

Rolling back the updater itself to a pre-bridge generation also rolls back its
recognized member set. Before subsequently deploying the dependent broker,
repeat the bridge convergence step. Retain a dependency-aware generation as the
ordinary rollback target once this migration has completed.

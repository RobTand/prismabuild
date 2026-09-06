# Per-attempt resource authority

The pool resource broker assigns each attempt an exact key, nonce and
root-owned systemd slice. A fixed privileged helper enters its own payload
cgroup by writing `0` to `cgroup.procs`, closes that descriptor, drops all
root identities and starts the user's command. It never migrates an external
numeric PID. The unprivileged client transfers standard I/O descriptors and
waits for the broker-owned process; cancellation targets the authenticated
attempt, including descendants and daemon-created containers.

The broker listens only on `/run/prismabuild/resources.sock`, authorizes the
configured local UID, and retains private authority records under
`/run/prismabuild/jobs`. This is resource isolation for trusted fleet work,
not a hostile multi-tenant security boundary. The fixed helper and its
ancestors must be root-owned and unwritable by callers. The runtime publisher
distributes source files; privileged execution uses installed copies under
`/opt/prismabuild-resource-broker`.

## Memory and accounting

Each parent slice receives the declared host-memory ceiling, a high watermark
at 90%, and no swap allowance. Its hierarchical CPU and memory counters include
the payload and its Docker children. The Docker shim fixes the parent slice,
container memory ceiling and inherited CPU mask and refuses unaccounted launch
routes. Missing or incomplete telemetry cannot authorize CPU lending.

Direct process group OOM uses the kernel's `memory.oom.group` setting. The
supported systemd version can reset that setting when Docker creates child
units; the aggregate memory ceiling still applies, but whole-attempt cleanup
requires observing the OOM and stopping the remaining owned scope. A healthy
attempt in another slice is not selected. Live qualification must demonstrate
that behavior rather than infer it from configuration.

GB10 CUDA allocations are not reliably charged to `memory.current`. A cgroup
ceiling alone therefore provides no device-memory guarantee. Per-process GPU
accounting and selective termination require separate qualification; GPU
utilization percentage is not a capacity or memory measurement. Consult the
readiness record for the measured scope and remaining limits.

## Installation and upgrades

Run `tools/fleet/install_resource_broker.sh` as root on each worker before
activating contained execution. The installer copies root-owned broker/helper
code, installs the systemd service and verifies it is active. It refuses to
overwrite an active service: an upgrade must first drain submissions for that
host, verify its attempts and job groups are idle, and stop the old service.
Then install the new revision and verify the service and installed file hashes.
Do not restart the broker merely because its process name matches a search.

The service protects its own modest memory allocation from host OOM. Payloads
explicitly reset that OOM preference before dropping privileges. They retain
host temporary-file visibility and networking; service-level restrictions that
would silently change those job semantics are deliberately absent.

## Recovery evidence

Claims persist the exact scope authority before launch. Cleanup must stop the
owned scope and prove it empty before releasing reservation tokens. Broker
failure leaves capacity held for recovery. Successful release retains an
authenticated `released_unix` record, so a worker crash between scope release
and queue cleanup can retry without touching a newly created process group.

Docker creation registers a pending intent before contacting the daemon.
Only an unambiguous successful CLI result clears that intent. A killed client,
daemon error or nonzero foreground result can leave a conservative pending
ticket. Cleanup then retains an empty frozen parent and a `retired_unix`
record: a late daemon request cannot recreate an unfrozen parent after the
reservation is released. Release reasserts the freeze rather than trusting a
previously persisted stop intention. These records and frozen empty groups
are retained recovery evidence, not running work. Do not delete them while a
daemon request could still be in flight; maintenance needs exact ownership,
terminal queue evidence and a quiescent daemon boundary.

Authority records live under `/run` and therefore survive service restarts,
not host reboots. Kernel groups and their processes disappear at reboot;
queue recovery must still reconcile the corresponding attempt rather than
invent a successful result.

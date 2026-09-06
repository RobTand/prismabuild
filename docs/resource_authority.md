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

Each parent slice receives the declared host-memory ceiling and no swap
allowance. There is no lower soft-throttle watermark: live qualification found
that it could stall allocations still below the declared ceiling. Its
hierarchical CPU and memory counters include
the payload and its Docker children. The Docker shim fixes the parent slice,
container memory ceiling and inherited CPU mask and refuses unaccounted launch
routes. Missing or incomplete telemetry cannot authorize CPU lending.

Direct process group OOM uses the kernel's `memory.oom.group` setting. The
supported systemd version can reset that setting when Docker creates child
units; the aggregate memory ceiling still applies, but whole-attempt cleanup
requires observing exhaustion of the parent limit and stopping the remaining
owned scope. The authority is an increase in the parent's
`memory.events.local` `oom` counter, recorded against the baseline at creation.
Hierarchical `memory.events` `oom_kill` counts remain diagnostic: a descendant
can safely hit its own smaller limit while the outer attempt stays within its
budget. Such a child failure does not terminate the outer attempt. A local OOM
stops the attempt even before a victim is counted, because allocation failure
already proves exhaustion of its aggregate budget. A healthy attempt in another
slice is not selected. Live qualification must demonstrate that behavior rather
than infer it from configuration.

The scope carries immutable `memory_max_bytes` and `gpu_memory_max_bytes`
budgets. The latter defaults to the former for compatibility; an explicit
GPU allowance can exceed system RAM on a discrete GPU host. Create retries and
lost-reply recovery must present the same two budgets. Clients defer explicit
GPU-budget creation when an older broker rejects the new field before creation.

GB10 CUDA allocations are not reliably charged to `memory.current`. The broker
therefore samples NVIDIA per-process memory and binds each observation to a
stable process start time and exact cgroup identity. Hardware telemetry supplies
each GPU UUID's physical memory domain: `shared_system`, `discrete`, or
`unknown`. CUDA unified virtual addressing is not evidence of shared DRAM.
On shared-system hardware, the system-memory lower bound is the larger of the
host cgroup charge and the largest known shared-GPU process report. On discrete
hardware, GPU VRAM never contributes to the system-RAM charge. Unknown devices
do not authorize a shared-memory inference. A mixed-device job includes only
its known shared-system GPU observations in its system-memory lower bound.

Two confirming observations above either the system/shared budget or the
separate GPU allowance select only that scope. Each budget has its own
confirmation counter; one violation of each is not two violations of either.
GPU allowance comparisons use the largest individual process/device report;
summed GPU reports remain diagnostic because shared/IPC allocations can overlap.
This avoids guessing ownership or double-counting shared memory, but can miss
aggregate excess spread across several small GPU processes. On shared-system
hardware the explicit GPU allowance is an additional cap within the physical
shared-memory budget, not another physical-memory reservation.

Predictive host-pressure handling additionally requires repeated pressure,
dominant attributable growth and projected excess of that job's own budget.
Discrete VRAM growth never enters this system-memory pressure calculation.
It can stop a rapidly growing job before current usage crosses its budget.
This forecast can misclassify a bounded allocation; it does not select static
healthy neighbors or foreign work. Polling cannot prevent every instantaneous
allocation spike or guarantee a response during a kernel stall. The budget
path has a real GB10 daemon qualification; projected-pressure behavior has
synthetic coverage without inducing a real host OOM.

## Installation and upgrades

Run `tools/fleet/install_resource_broker.sh` as root on each worker before
activating contained execution. The installer copies root-owned broker/helper
code, installs the systemd service and verifies it is active. It refuses to
overwrite an active service: an upgrade must first drain submissions for that
host, verify its attempts and job groups are idle, and stop the old service.
Then install the new revision and verify the service and installed file hashes.
Do not restart the broker merely because its process name matches a search.

After the initial installation, enroll the host with
`tools/fleet/install_client_upgrader.sh`. The automatic updater closes admission
through the broker's root-only maintenance API, waits for existing attempts,
and verifies the new running code before reopening admission. Failed upgrades
restore verified previous bytes. See [client upgrades](client_upgrade.md) for
the publication authority, status and recovery contract.

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

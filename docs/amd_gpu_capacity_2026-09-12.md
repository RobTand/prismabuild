# AMD GPU capacity on a WSL2 host — 2026-09-12

`gpu_capacity.devices()` read `/usr/bin/nvidia-smi` and nothing else, so every
AMD box offered `gpu: 0` whatever it was holding (#529). This records what the
AMD surfaces on `wsl-gpu` actually expose, what the reader published from them,
and the two things the NVIDIA contract has that this hardware does not.

All measurements below were taken on `wsl-gpu` — an RX 9070 XT (gfx1201, RDNA4)
under WSL2 on Ubuntu 26.04, ROCm and HIP 7.14 — on 2026-09-12. Per the scope
corollary in the fleet's principle on attested runtime claims, none of it is a
claim about the same card on a native Linux host, which nobody here has run.

## What WSL2 does not have, and why it is structural

There is no `amdgpu` kernel driver. The GPU is reached through `/dev/dxg`, the
WDDM paravirtualisation node, and the HSA runtime rides that:

```
$ amd-smi list      ERROR:root:Drivers not loaded (amdgpu, amd_hsmp, ionic, bnxt_en ...)
$ rocm-smi --json   # prints nothing
$ ls /dev/kfd /dev/dri
ls: cannot access '/dev/kfd': No such file or directory
$ ls /sys/class/drm/
version
```

So `amd-smi`, `rocm-smi`, `/sys/class/drm/card*/device/mem_info_vram_*` and the
`/dev/kfd` per-process interfaces are unavailable, not merely unread. Power
draw, power cap, current clocks and throttle reasons have no source at all on
this host, and neither does per-process VRAM.

## What it does have, and how the reader uses it

Two runtime readers, and the reader publishes only what both agree on.

`rocminfo` answers the HSA agent question. For Agent 2:

```
  Name:            gfx1201          Uuid:  GPU-9c30c352a59e5b7a
  Marketing Name:  AMD Radeon RX 9070 XT
  Device Type:     GPU              Compute Unit: 64      Wavefront Size: 32(0x20)
  Max Clock Freq. (MHz): 2400       Workgroup Max Size: 1024(0x400)
  Pool 1  Segment: GLOBAL; FLAGS: COARSE GRAINED   Size: 16577056(0xfcf220) KB
```

The UUID already carries the `GPU-` prefix `box_capacity._gpu_evidence`
requires. **The pool is the landmine.** `rocminfo` also prints a 28737164 KB
(27.4 GiB) GLOBAL pool that is host RAM on **Agent 1, the CPU**. A reader keyed
on pool order over-reports this card by 1.7x and would admit work that cannot
fit, so the reader keys on `Device Type: GPU` and refuses unless exactly one GPU
agent is reported.

The HIP runtime answers the live-memory question the agent report cannot, from a
short-lived subprocess, so no HIP context is opened inside the broker daemon:

```
$ /usr/bin/python3 <ctypes on /opt/rocm/lib/libamdhip64.so>
hipGetDeviceCount   1
hipMemGetInfo       free 15483645952   total 16974905344
hipDeviceGetAttribute  Integrated 0   WarpSize 32   ClockRate 2400000 kHz   MaxThreadsPerBlock 1024
real 0m0.103s
```

Four quantities are visible to both readers and all four must match before
anything is published: VRAM total (`16577056 KB` = `16974905344` bytes exactly),
wavefront/warp size, peak clock, and workgroup/threads-per-block. Disagreement
means one reader is describing a different device, or that a
`hipDeviceAttribute_t` value was renumbered under the constant the probe uses,
and neither is something to publish a capacity claim from. `hipMemGetInfo` is a whole-device
counter: it reported bytes already in use with no ROCm process of ours running,
which is the Windows compositor.

The memory domain comes from `hipDeviceAttributeIntegrated` rather than from the
card's name: `0` is `discrete`, `1` is `shared_system`, anything else is
`unknown` and refuses. `/opt/rocm/lib/libamdhip64.so` and `/usr/bin/rocminfo`
are root-owned and not caller-writable on this box, which is what lets the root
broker run them.

## Declaration one: no saturation instrument

The device record carries `telemetry_class: memory_only` beside
`vendor: amd`, and `power_w`, `power_reference_w`, `sm_clock_mhz` and `limited`
are `None` rather than a zero that would read as an idle device.

`adaptive_gpu` treats that declaration as a narrowing, not a relaxation. Power
is what authorizes two permissions — the concurrency probe that puts a second
job on a busy device, and a `measurement` action that needs a provably idle one
— and both are withheld: `low` is never true on such a device, so it runs **one
attributed job at a time** and admits no measurement action at all. Everything
else it already decides from evidence this hardware does have: identity, memory
domain, free VRAM against the requested budget, foreign holders, and host
memory/CPU pressure. A sample that merely *lost* its power counters without the
declaration keeps failing closed exactly as before; there is a test for that.

## Declaration two: ownership without bytes

Every GPU user under WSL2 holds `/dev/dxg` open, so the foreign-process
inventory is a census of that node's holders in `/proc`, attributed to broker
scopes by the same PID start-time and cgroup-identity checks the NVML path uses.
Measured, with one allocating process and one that only imported the GPU
library:

```
ALLOC 67312   IMPORTONLY 67311
HOLDER 67312 python 2 descriptors      HOLDERS 1
```

Importing a GPU library opens nothing; creating a context does. The census
therefore reports ownership. It reports no bytes, because the node exposes none.

Three consequences, all declared in the sample rather than inferred:

* `gpu_process_bytes: false` and `JobSample.gpu_budget_enforceable: false`. The
  Guard cannot confirm a GPU-allowance violation it has no counter for, so
  `gpu_memory_budget_exceeded` never fires here. On a discrete device the blast
  radius is bounded: the job's own `hipMalloc` fails, the host cgroup limit is
  untouched, and free VRAM still gates the next admission.
* Ownership is resolved, so a scope with a holder is `complete` and attribution
  works. That is a deliberate split of two things the old code conflated: rows
  whose owner is unknown still refuse, rows whose bytes are unknown no longer do.
* **On `shared_system` hardware without per-process bytes there is no
  system-memory lower bound to state**, so such a scope stays incomplete and
  offers nothing. This path is reachable only on a future integrated AMD part.

`foreign_inventory_scope: host_gpu_handles` says how far that census can see:
every GPU user among the processes this `/proc` lists, and nothing outside it.
A handle is matched by the character device's device number, not by the node's
inode, because a container runtime `mknod`s its own node for a device passed in
with `--device /dev/dxg`: same card, different filesystem and inode. Matching
inodes would have made a GPU user inside a container — the containerized ROCm
vLLM being built for this very box — invisible to the census, and opened the
offer onto a busy device. Three narrower blind spots remain and are not fixed
here: a holder in another PID namespace (a sibling WSL2 distro), a holder whose
thread-group leader has exited while its threads keep the descriptor, and a
holder that opened the card through a second path, since the census prefilters
on the link text before it stats. Completeness also assumes the census can read
every process's descriptor table, which is why it runs as root and refuses on
`PermissionError`; a `hidepid` mount would hide other users' processes without
raising one. Under
WSL2 a Windows-side GPU user is invisible to it — the compositor's bytes are in
`memory_used` with no holder to name. That is a real residual: co-resident
Windows GPU work degrades throughput here and will not be reported as a foreign
process. It cannot silently overcommit VRAM, because `memory_free_bytes` is
measured whoever holds the rest and admission refuses when the budget does not
fit; and PB work on this host is correctness work, for which WSL2 numbers are
receipts and never performance claims. An unreadable `/proc` descriptor table —
an unprivileged census that cannot see another user's handles — refuses rather
than reporting an empty foreign list.

One more transient, recorded so the next reader does not file it as a bug.
`box_window.gpu_power_reference()` runs on the worker once per action and calls
`devices()`, which on this host spawns the HIP probe: a ~100 ms holder of
`/dev/dxg` in the worker's own cgroup, which is foreign to the broker's census.
A broker sample landing inside that window lists one foreign process and the
offer reads `gpu: 0` for that sample, then recovers on the next one. It is a
self-healing flap in the safe direction, not a hole.

One implementation note worth keeping: the census runs `readlink` on every
descriptor and `stat` only on the ones that already name the device. Following a
descriptor onto a stalled network mount blocks in the kernel, and an earlier
version that stat'ed every descriptor hung past a 20-second timeout on this box.
The narrowed form completed in 5 ms.

## What this box offers once a generation ships it

One `discrete` device, 15.81 GiB VRAM, one GPU job at a time, no sharing probe,
no measurement actions, no enforced per-scope GPU allowance. Admission still
requires a fresh, complete, attributed broker snapshot, no foreign handle, free
VRAM for the declared budget, and host headroom.

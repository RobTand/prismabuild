# dl380g10 memory capacity, 2026-09-05

Issue [#191](https://github.com/RobTand/prismabuild/issues/191) exposed a stale
60 GiB fleet ceiling on dl380g10. The configured aggregate budget is now
192 GiB. This permits larger honest action reservations without increasing
the limits of existing actions.

The read-only host sample at Unix time 1788664644.835591 reported:

| Observation | Bytes |
|---|---:|
| Physical RAM (`MemTotal`) | 316241965056 |
| Available RAM (`MemAvailable`) | 247593562112 |
| Free swap (also total swap) | 68719472640 |
| PB parent cgroup `memory.current` | 5270425600 |

Memory PSI averages were zero. One running PB action reserved 48 GiB;
unrelated Java and QEMU processes were present and left alone. The new ceiling
leaves about 102.5 GiB outside PB's maximum aggregate reservation, covering
the observed non-PB consumption and additional host margin. Swap is not
counted as admission capacity. Live capacity observation can still reduce
the offer under host pressure; the configured ceiling is not a claim that
all physical RAM is always available to PB.

Before the change, the published client refused an x86 action requesting
128 GiB with `no recorded worker can run this action`. Validation uses a
128 GiB reservation to inspect its actual cgroup limit, without allocating
128 GiB merely to create load. Normal runtime publication lets idle loops
adopt the new shape while active attempts retain their identities and limits.

The reported Tessera failure, action `0c430625b444`, reached its own 48 GiB
cgroup limit and exited 137. Raising the host offer cannot retroactively
resize that action or make an undersized 48 GiB request sufficient. A caller
must declare the workload's aggregate peak memory requirement.

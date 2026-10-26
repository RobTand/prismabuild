# PRI-28 external-memory accounting correction

The earlier request for a new overlap contract was withdrawn. Its premise
confused the memory cgroup with the host `MemAvailable` counter. No human
answer or contract waiver was recorded.

## Original measurement

Rob's commit [`4b6cc60496`](https://github.com/RobTand/prismabuild/commit/4b6cc60496e8603030ee245a2eaa30c4cc4acc9d)
records sparky action `1244c3e5db31`. An 8 GiB CUDA allocation left
`memory.current` at 381 MB while `MemAvailable` fell by 8725 MiB.
The full trace is in `docs/memory_enforcement_2026-09-04.md` at that commit.
CUDA bypasses the cgroup charge but consumes the same physical memory that
`MemAvailable` measures. Hindsight was unavailable during the first revision; the
original committed measurement supplies the evidence.

With total memory of 128 GiB and available memory of 96 GiB, a foreign
32 GiB GPU allocation already accounts for the host's entire 32 GiB usage.
An additional disjoint foreign 32 GiB CPU allocation would leave 64 GiB
available. The old regression's hypothetical invisible GPU allocation was
unsupported. Action `4d448778fb1c9f1241a07cf47c5e695defba1833b7a2d555cb5e8f2179f8bb8b`
failed the assertion in that invalid model. It does not prove over-admission.

## Reproduced defect and correction

The Principal's [review](https://github.com/RobTand/prismabuild/pull/2289#issuecomment-6403454973)
reproduced a different over-admission on dl380g10 with action
`309eb752ebcc152e5db127287f78b50f528c08c91e4c1871bd5523d1b968a3b1`.
A 128 GiB host with 68 GiB available, 60 GiB of external CPU usage and an
unresident 40 GiB PB hold offered 100 GiB. The safe PB ceiling is 60 GiB
after the unchanged 8 GiB operating margin. The first correction passed
91 focused cases on dl380g10 and sparky, bound to source `a7d49816ed`.
The [evidence comment](https://github.com/RobTand/prismabuild/pull/2289#issuecomment-6404719154)
retains the full action keys and receipts.

The shared evidence owner now derives one external usage value:

```text
scope_credit_j = min(valid host_j + valid shared-system GPU lower_bound_j, budget_bytes_j)
PB_resident = sum(scope_credit_j)
external = max(0, host_total - host_available - PB_resident)
ceiling_gib = max(0, floor((host_total - external) / GiB) - margin_gib)
```

Only complete jobs in a fresh attributed broker inventory earn residency
credit. A missing or invalid `budget_bytes` earns no scope credit. A missing
or malformed residency component contributes zero. GPU reported sums can
overlap, so the calculation uses the broker's GPU lower bound for jobs
whose memory domain is `shared_system`. Mixed or discrete GPU bytes earn
no shared-memory credit. Existing held commitments still count once.

The Principal's [second review](https://github.com/RobTand/prismabuild/pull/2289#issuecomment-6404826897)
found that uncapped credit hides residency above a scope's reservation.
The broker Guard currently uses `max(host, GPU)`, so 30 GiB host plus
40 GiB CUDA can survive under a 40 GiB reservation. With another unresident
40 GiB hold and 4 GiB other usage, the old offer was 116 GiB; only 86 GiB
was safe. The new observer and pool regressions reproduced both over-offer
and an unsafe 8 GiB claim in dl380g10 action
`be58a3f6a2a42ab94d72279d8e24e24580ac25b15cc52d17a92c3b66b260ac29`.
Missing and invalid budgets also wrongly earned credit in action
`8157def004c817da969e68a45193fbb072fbace1e0929417d69bff8952ace8d6`.

Capping each scope leaves above-budget residency in external usage.
The resulting claim bound is equivalent to
`sum(max(reservation_j, residency_j)) + other_usage + request <= total - margin`.
No Guard enforcement change is included. [#450](https://github.com/RobTand/prismabuild/issues/450)
measured host plus GPU charges 4.4% above the `MemAvailable` drop in one
workload. They are near-disjoint; the existing 8 GiB margin absorbs the
observed overestimate, without proving that bound for every workload.

The rewritten regression covers both measured-consistent foreign usage
states. Additional cases cover external CPU memory beside an unresident hold,
PB host and CUDA residency, invalid residency credit, and a claim whose
trusted evidence becomes partial, absent, stale or unattributed. The latter
keeps unknown context during that claim pass instead of reusing CLEAR.

## Target and delivery status

| Requirement | Target | Source status | Validation and deployment |
|---|---|---|---|
| #2040, #2088 | Bound total PB commitments by physical memory less external usage, counting overlap once | Revised in the shared evidence owner | Review-2 red runs reproduced; final x86/GB10 receipts pending. Not deployed |
| #2089 | Defer unknown residency before commitments, elections and preemption | Existing fences retained; loss of trusted evidence closes a seeded claim pass | Prior 91-case x86/GB10 runs passed; final revised-head checks pending. Not deployed |
| #1661 | Qualify GB10 unified-memory admission on the fleet and close resolved tickets with merged proof | PR #2289 remains open | Principal review, merge and live qualification remain pending |

The human-only interaction `ef4b12e2-85b8-43d7-9977-e8bb107356a3` was
withdrawn with the original measurement as its reason. Neither of its
proposed options is needed. No runtime generation was published, no service
was restarted, and no live foreign-load stress was performed. The staged-read
policy and its separate merge, launch, deployment and completion acceptance
records remain required. This record claims no acceptance or completion.

# PRI-28 external-memory union decision

The proposed #2088 repair still cannot guarantee physical fit beside foreign
CPU and GPU allocations. The PR remains a draft while this admission contract
is unresolved. No runtime was published or restarted.

## Two physical states have the same published evidence

The broker publishes host total and available memory, PB scopes' host charges,
and foreign GPU byte counts. Foreign process rows contain no host charge or
evidence about overlap with the host counter. See `gpu_memory.collect` and
`gpu_capacity.collect`.

| Quantity | Both published samples |
|---|---:|
| Physical host memory | 128 GiB |
| Host available memory | 96 GiB |
| Foreign GPU bytes | 32 GiB |
| Resident PB scopes | none |
| Starting, unresident PB reservation | 40 GiB |
| Candidate PB memory | 40 GiB |

If the GPU allocation is already visible in host usage, external residency is
32 GiB. With the unchanged 8 GiB margin, PB's physical ceiling is 88 GiB.
If 32 GiB of foreign CPU memory is visible while a separate 32 GiB GPU
allocation is invisible to the host counter, external residency is 64 GiB.
PB's ceiling is then 56 GiB. The current proposal admits 80 GiB of PB
commitments in both states. `max(host_external, gpu_foreign)` also cannot
distinguish these states. Adding the two counters treats overlapping pages
as disjoint and conflicts with #2088's current wording.

The deterministic reproduction is
`tests/test_unified_memory_external_overlap_1661.py`. It uses fake capacity
and GPU evidence, without allocating GPU memory. Its assertion must remain
red until the evidence or admission contract changes.

PrismaBuild reproduced the unsafe commitment on dl380g10 with action
`4d448778fb1c9f1241a07cf47c5e695defba1833b7a2d555cb5e8f2179f8bb8b`.
The run ended with exit 1 and `1 failed in 0.42s`; the failing assertion
reported 80 GiB held. Its terminal record and retained stdout are in the
fleet queue. Failed runs publish no success receipt.

## Decision required from Rob

The existing acceptance requires both physical safety and counting overlapping
external allocations once. The published fields cannot establish both.
Two concrete alternatives preserve physical safety:

1. Defer new commitments when the external union cannot be established.
   This extends the unknown-residency fence to unknown overlap, even when
   foreign GPU bytes are known. Admission can resume when a trusted union
   measurement or proof of the relevant overlap is available.
2. Authorize a conservative upper-bound charge for the uncertain external
   union. This can refuse work that would physically fit when counters
   overlap. It changes the exact-once external-usage requirement and must
   label the value as an upper bound.

The recommendation is option 1. Neither alternative changes an action's
single `mem_gb` charge or its GPU subset cap. Neither grants authority to
publish a runtime or perform live foreign-load stress. The Principal's
independent review and the separate #1661 live acceptance remain required.

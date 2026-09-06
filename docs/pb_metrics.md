# PrismaBuild Prometheus metrics

`tools/fleet/pbmetrics.py` is a read-only Prometheus exporter for the pull
queue. It uses the same `pbstatus.read_pool` and `PoolQueue` readers as the
operator status screen. It does not probe remote hosts, make admission
decisions, change queue records, or monitor hardware continuously. Use Netdata
for live host CPU, RAM, pressure, power, and device telemetry.

Print one snapshot:

```bash
python3 tools/fleet/pbmetrics.py --once
```

Serve it:

```bash
python3 tools/fleet/pbmetrics.py --listen 0.0.0.0 --port 9877
```

## Running it, and keeping what it says

A snapshot answers "what is true right now". Every queue question that costs
real time is a question about change, so the exporter is only useful once
something runs it and something retains it. Install both on a box:

```bash
sudo /mnt/shared/prismabuild-fleet/repo/tools/fleet/install_pbmetrics.sh
```

That installs `prismabuild-metrics.service`, bound to `127.0.0.1:9469` and
running as the queue's owner, and appends a Netdata scrape job for it at
`/etc/netdata/go.d/prometheus.conf` (any existing file is copied aside first).
`PBMETRICS_PORT`, `PBMETRICS_QUEUE`, `PBMETRICS_USER`, `PBMETRICS_PYTHON` and
`PBMETRICS_RUNTIME` override the defaults. The script proves the exporter can
read the queue before it installs a unit that would otherwise restart-loop.

Netdata is the store, rather than a series appended under the queue, for the
reason the queue is being observed at all: writing history onto the shared mount
adds load to the resource whose contention is the most common thing you are
trying to see, and loses that history exactly when the mount is the problem.
Netdata is already on these boxes, already retains, and already runs anomaly
detection on what it holds.

The exporter reports the *whole fleet's* queue from whichever box runs it, so
one instance is enough for the queue-wide series and the `host` label is the
claiming box, not the observing one. Anything that must be measured *from* each
box -- shared-mount latency, local process counts -- is a per-box measurement
and does not belong to this exporter.

One refresh over the live queue measured 0.09-0.10s wall (708 `openat`,
47 `getdents64`). Reads are cached for ten seconds, so a scrape faster than that
buys repetition rather than resolution; the installed job scrapes every ten.

The default bind is `127.0.0.1:9469`. `GET /metrics` returns Prometheus text
format; other paths return 404. Reads are cached for 10 seconds by default so
multiple scrapers do not repeatedly walk the shared queue. Change that bound
with `--cache-seconds`. The default queue is
`/mnt/shared/prismabuild-fleet/pb-queue`; `--queue-root` selects a fixture or a
different pull queue.

## Metric contract

All metrics are gauges. The exporter deliberately has no process-lifetime
counters because they would reset on every exporter restart. The one rate it
reports, `prismabuild_attempt_recent_cores`, is not such a counter: the counters
differenced belong to each attempt's own cgroup, they are differenced only
against a previous reading of that same attempt -- identified by action key and
scope nonce together, so a key reused by a later attempt is never differenced
against an earlier one -- and a restart simply produces no sample until a second
refresh. Nothing is exposed per action key; the store is keyed by it in memory
and pruned each refresh to the claims that are live, so it cannot grow with the
queue's history. Labels are drawn
from bounded sets: worker host, resource, CPU/GPU job kind, terminal outcome,
timing statistic, timing phase, and memory domain. Action keys, command lines,
nonces, tokens, and result digests are never labels.

| Metric | Labels | Meaning |
| --- | --- | --- |
| `prismabuild_queue_items` | `state=ready\|claimed` | Count of records when the complete state directory was readable. A missing sample means unknown, not zero. |
| `prismabuild_queue_oldest_age_seconds` | `state` | Oldest active-record age. Zero means the readable state is empty. Invalid timestamps omit the sample. |
| `prismabuild_worker_up` | `host` | `1` for a fresh valid offer and `0` for a valid expired offer. Invalid offers are omitted. |
| `prismabuild_worker_offer_age_seconds` | `host` | Offer age when the timestamp is valid. |
| `prismabuild_worker_capacity` | `host,resource` | Configured capacity from a fresh offer. |
| `prismabuild_worker_observed_capacity` | `host,resource` | Windowed capacity after foreign work from a fresh offer. It can lag a falling observation by the worker's configured sample window. |
| `prismabuild_worker_memory_domain_info` | `host,domain` | Value `1` for each memory domain in a fresh offer. Domains are `shared_system`, `discrete`, or `unknown`. |
| `prismabuild_worker_memory_available_bytes` | `host` | Coarse host-available memory reported by a fresh offer, converted from its integer GiB observation. |
| `prismabuild_active_jobs` | `host,kind=cpu\|gpu` | Claims with fresh leases. A GPU-demanding claim is classified as GPU even though it also reserves CPU. A host with any stale or invalid claim has no active-job samples because its process liveness is unknown. |
| `prismabuild_reserved_resources` | `host,resource` | Sum of declared demand in valid claimed records, including stale and cleanup-pending claims that may retain reservations. Fresh workers emit zero for each resource dimension they declare when nothing is claimed. |
| `prismabuild_attempt_observed_resources` | `host,resource` | Aggregate telemetry only when every live claim on the host has a complete, fresh, nonce-matched resource-scope record. CPU is lifetime-average cores (`cpu_seconds / wall_seconds`); memory is current cgroup bytes. The host is omitted rather than partially summed if any live claim is unavailable -- read it beside `prismabuild_attempt_telemetry_unavailable_jobs`, which says whether an absent aggregate means an idle host or an unreadable one. Lifetime average cannot see a job that has stopped moving; `prismabuild_attempt_recent_cores` is the reading that can. |
| `prismabuild_attempt_telemetry_jobs` | `host` | Number of live claims covered by the corresponding aggregate telemetry. |
| `prismabuild_attempt_telemetry_unavailable_jobs` | `host` | Live claims whose resource-scope record could not be used. Emitted for every host with live claims, including zero, so a withheld aggregate is legible rather than indistinguishable from an idle box. |
| `prismabuild_attempt_telemetry_age_seconds` | `host` | Age of the oldest credible resource-scope sample among the host's live claims. Reported even when the aggregate is withheld, which is when it matters: a claim blocked on the shared mount keeps its lease while its sampler stops running. |
| `prismabuild_attempt_recent_cores` | `host` | Cores used since the exporter's previous refresh, differenced per attempt and summed over the host. Absent on the first refresh and whenever nothing could be differenced. |
| `prismabuild_attempt_recent_cores_jobs` | `host` | Live claims the recent-cores figure was differenced over. It is that figure's denominator, not a total. |
| `prismabuild_admission_evidence_age_seconds` | `host,resource=cpu\|gpu` | Age of persisted scheduler evidence. This is the last recorded evidence, not a live hardware sample. |
| `prismabuild_admission_plateau` | `host,resource=gpu` | Whether persisted GPU `power_feedback.status` last recorded a plateau. The age metric must be consulted with it. |
| `prismabuild_collection_success` | none | `1` when critical active-queue inputs and the selected terminal records were readable and valid, otherwise `0`. A stale valid worker offer does not make collection fail. |

Resource values use these units:

| `resource` | Unit and source |
| --- | --- |
| `cpu` | CPU cores from `cpu`. |
| `gpu` | Devices from `gpu`. |
| `memory_bytes` | Bytes, converted from `mem_gb` with `GiB = 2^30` bytes. |
| `gpu_memory_bytes` | Bytes, converted from `gpu_mem_gb` or `gpu_memory_gb`. |

Unknown resource names are omitted so a malformed or newly invented key cannot
grow label cardinality or silently acquire an implied unit.

## Recent terminal window

Terminal history is a bounded, restart-safe window rather than a counter:

| Metric | Labels | Meaning |
| --- | --- | --- |
| `prismabuild_terminal_outcomes` | `host,outcome` | Counts in the selected window. Known outcomes are `executed`, `cache_hit`, `failed`, `timeout`, `withdrawn`, `reset`, `finish_lost_race`, `unreadable`, and `unknown`; unrecognized statuses map to `unknown`. A terminal record with no trusted host uses the single `unknown` host value. |
| `prismabuild_terminal_outcomes_window_seconds` | none | Configured lookback, 3600 seconds by default. |
| `prismabuild_terminal_outcomes_window_jobs` | none | Records actually included. This is a gauge and may fall as records age out. |
| `prismabuild_terminal_outcomes_window_complete` | none | `1` when all terminal directories were accessible and the record cap was not reached; `0` means the window may be truncated or partly inaccessible. |
| `prismabuild_terminal_collection_success` | none | `1` when all terminal directories and selected records were readable. This distinguishes read failure from an otherwise valid cap-truncated window. |
| `prismabuild_queue_wait_seconds` | `host,stat=mean\|max` | Publish-to-claim duration for records with both timestamps. |
| `prismabuild_execution_seconds` | `host,stat=mean\|max` | Claim-to-finish duration for records with both timestamps. |
| `prismabuild_terminal_timing_window_jobs` | `host,phase=queue_wait\|execution` | Denominator for each host and timing phase. |

`--terminal-window-seconds` changes the lookback and `--terminal-limit` changes
the maximum records read. The default limit is 500. Each refresh stats retained
terminal directory entries to select the newest bounded set, then reads only
that set; it does not parse the full terminal history. If the cap is reached,
the completeness gauge is conservatively zero even when some capped records
later fall outside the time window.

The queue census is non-atomic: a worker may rename a record while a snapshot
is being read. A later cached refresh settles transient races. Terminal
retention also limits what can be observed, and deleting old records removes
them from the window even if their timestamps would otherwise qualify. Missing
or inaccessible values are omitted rather than emitted as false zeros; use the
collection and window-completeness gauges when alerting on absence.

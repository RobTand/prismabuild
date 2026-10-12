# Gate 1730: supplied space ruling and retained observations (v3, 2026-10-12)

Space is **GO on both Sparks by the supplied lead ruling**. Overall A8S local-copy acceptance is **UNPROVEN**: comparable A8S/payback measurements and complete bind proof are absent. k1 remains blocked on those missing legs; the old sparklina space failure no longer blocks it.

This is an offline evidence update, with no new host measurement, copy, Docker execution, cache manipulation, or execution verification. The conclusion superseded is the **2026-10-10 sparklina space failure**, including the resulting space-only NO-GO and k1 space blocker in the retained narrative. Its speed observations and their limitations remain historical evidence. The historical bind observations remain recorded; the narrative's partial bind pass does not establish the complete bind criterion below.

## Retained evidence

The following files are retained byte-for-byte:

- [2026-10-10 narrative](gate_1730_local_sets_2026-10-10.md), including exact canonical A8S size, reported action keys, memory/load limitations, and the superseded space-only decision.
- v1 raw reports: [sparky](gate_1730_sparky_2026-10-10.json), [sparklina](gate_1730_sparklina_2026-10-10.json). The narrative records that v2 superseded their speed arms.
- v2 raw reports: [sparky](gate_1730_sparky_2026-10-10_v2.json), [sparklina](gate_1730_sparklina_2026-10-10_v2.json). All speed and bind observations transcribed below come from these files.

## Supplied 2026-10-12 space evidence

The lead supplied sparklina available space of **314306564096 bytes**, A8S size of **175527382717 bytes**, a reported **163.47 GiB** set and **265.21 GiB** copy rule, and a **space GO on both Sparks**. Provenance: [supplied lead comment on issue #1730](https://github.com/RobTand/prismabuild/issues/1730#issuecomment-6128770540). This comment is cited as supplied provenance and was not fetched. No fresh sparky available-byte observation was supplied separately.

The copy inequality uses the exact supplied bytes, without replacing them with rounded GiB values:

```text
available >= 1.5 * set_bytes + 20 * 2**30
314306564096 >= 1.5 * 175527382717 + 20 * 1073741824
314306564096 >= 284765910555.5 bytes
margin = 29540653540.5 bytes
```

This establishes sparklina's copy inequality from the supplied values. The reported 163.47 GiB and 265.21 GiB are rounded descriptions, not raw inputs.

The separate D1 floor rule is:

```text
available - set_bytes >= 0.05 * filesystem_bytes
314306564096 - 175527382717 = 138779181379 bytes
138779181379 >= 0.05 * filesystem_bytes  # current filesystem_bytes not supplied
```

Current raw filesystem size, cleanup transcript, chosen destination and its filesystem, and Docker allowance are not supplied. Thus the lead's accepted space ruling is distinct from independent measurement and verification of every D1 input on both hosts. Historical rounded `root_vfs.total_gib`, `root_vfs.avail_gib`, `docker_root_vfs` and `d1_a8s` values in v2 are not fresh raw observations and cannot fill those gaps. The two inequalities are also implemented in [the retained probe's `d1_verdict`](../../tools/fleet/gate_1730_probe.py).

## Historical v2 speed observations

Each table is sourced from the linked host's v2 report. JSON paths use zero-based indices. The `JSON arm` column supplies the base path for each row: append `.streams`, `.cold`, `.bytes_gib`, `.wall_s`, or `.mib_s` for the respective cell. Host comes from `$.host`; source comes from `$.stage_source`, `$.hdd_source`, or `$.local_nvme`. Bytes read are the unit conversion of the recorded `.bytes_gib = 8.0`: `8 * 2**30 = 8589934592` bytes. Elapsed time and MiB/s are transcribed as recorded, without recomputing rates from rounded elapsed times.

“First” means the recorded `cold: true` arm and “repeated” means `cold: false`. These are pass labels, not demonstrated cache states. Tables sort streams as 1/4/16; the historical execution order in each source's `.arms` array was 16 first/repeated, then 4, then 1.

### sparky — [historical v2 report](gate_1730_sparky_2026-10-10_v2.json)

| Host | Source | Streams | Pass | Bytes read | Elapsed (s) | MiB/s | JSON arm |
|---|---|---:|---|---:|---:|---:|---|
| sparky | stage NFS | 1 | first | 8589934592 | 4.391 | 1865.8 | `$.stage_source.arms[4]` |
| sparky | stage NFS | 1 | repeated | 8589934592 | 5.688 | 1440.3 | `$.stage_source.arms[5]` |
| sparky | stage NFS | 4 | first | 8589934592 | 1.369 | 5984.8 | `$.stage_source.arms[2]` |
| sparky | stage NFS | 4 | repeated | 8589934592 | 1.372 | 5969.7 | `$.stage_source.arms[3]` |
| sparky | stage NFS | 16 | first | 8589934592 | 3.574 | 2291.9 | `$.stage_source.arms[0]` |
| sparky | stage NFS | 16 | repeated | 8589934592 | 1.042 | 7859.6 | `$.stage_source.arms[1]` |
| sparky | HDD pool NFS | 1 | first | 8589934592 | 5.318 | 1540.5 | `$.hdd_source.arms[4]` |
| sparky | HDD pool NFS | 1 | repeated | 8589934592 | 5.348 | 1531.8 | `$.hdd_source.arms[5]` |
| sparky | HDD pool NFS | 4 | first | 8589934592 | 1.855 | 4417.0 | `$.hdd_source.arms[2]` |
| sparky | HDD pool NFS | 4 | repeated | 8589934592 | 1.819 | 4502.6 | `$.hdd_source.arms[3]` |
| sparky | HDD pool NFS | 16 | first | 8589934592 | 1.34 | 6114.1 | `$.hdd_source.arms[0]` |
| sparky | HDD pool NFS | 16 | repeated | 8589934592 | 1.318 | 6215.6 | `$.hdd_source.arms[1]` |
| sparky | local NVMe (synthetic) | 1 | first | 8589934592 | 2.729 | 3001.7 | `$.local_nvme.arms[4]` |
| sparky | local NVMe (synthetic) | 1 | repeated | 8589934592 | 2.738 | 2992.4 | `$.local_nvme.arms[5]` |
| sparky | local NVMe (synthetic) | 4 | first | 8589934592 | 1.163 | 7042.0 | `$.local_nvme.arms[2]` |
| sparky | local NVMe (synthetic) | 4 | repeated | 8589934592 | 1.222 | 6701.2 | `$.local_nvme.arms[3]` |
| sparky | local NVMe (synthetic) | 16 | first | 8589934592 | 1.065 | 7689.8 | `$.local_nvme.arms[0]` |
| sparky | local NVMe (synthetic) | 16 | repeated | 8589934592 | 1.05 | 7805.2 | `$.local_nvme.arms[1]` |

### sparklina — [historical v2 report](gate_1730_sparklina_2026-10-10_v2.json)

| Host | Source | Streams | Pass | Bytes read | Elapsed (s) | MiB/s | JSON arm |
|---|---|---:|---|---:|---:|---:|---|
| sparklina | stage NFS | 1 | first | 8589934592 | 4.896 | 1673.3 | `$.stage_source.arms[4]` |
| sparklina | stage NFS | 1 | repeated | 8589934592 | 4.807 | 1704.3 | `$.stage_source.arms[5]` |
| sparklina | stage NFS | 4 | first | 8589934592 | 1.336 | 6132.0 | `$.stage_source.arms[2]` |
| sparklina | stage NFS | 4 | repeated | 8589934592 | 1.349 | 6071.5 | `$.stage_source.arms[3]` |
| sparklina | stage NFS | 16 | first | 8589934592 | 1.387 | 5904.6 | `$.stage_source.arms[0]` |
| sparklina | stage NFS | 16 | repeated | 8589934592 | 1.11 | 7377.7 | `$.stage_source.arms[1]` |
| sparklina | HDD pool NFS | 1 | first | 8589934592 | 2.007 | 4081.7 | `$.hdd_source.arms[4]` |
| sparklina | HDD pool NFS | 1 | repeated | 8589934592 | 1.484 | 5520.1 | `$.hdd_source.arms[5]` |
| sparklina | HDD pool NFS | 4 | first | 8589934592 | 1.265 | 6476.5 | `$.hdd_source.arms[2]` |
| sparklina | HDD pool NFS | 4 | repeated | 8589934592 | 1.422 | 5762.0 | `$.hdd_source.arms[3]` |
| sparklina | HDD pool NFS | 16 | first | 8589934592 | 7.591 | 1079.1 | `$.hdd_source.arms[0]` |
| sparklina | HDD pool NFS | 16 | repeated | 8589934592 | 1.224 | 6695.0 | `$.hdd_source.arms[1]` |
| sparklina | local NVMe (synthetic) | 1 | first | 8589934592 | 4.221 | 1940.6 | `$.local_nvme.arms[4]` |
| sparklina | local NVMe (synthetic) | 1 | repeated | 8589934592 | 3.971 | 2063.1 | `$.local_nvme.arms[5]` |
| sparklina | local NVMe (synthetic) | 4 | first | 8589934592 | 1.933 | 4237.8 | `$.local_nvme.arms[2]` |
| sparklina | local NVMe (synthetic) | 4 | repeated | 8589934592 | 1.904 | 4302.8 | `$.local_nvme.arms[3]` |
| sparklina | local NVMe (synthetic) | 16 | first | 8589934592 | 1.237 | 6622.2 | `$.local_nvme.arms[0]` |
| sparklina | local NVMe (synthetic) | 16 | repeated | 8589934592 | 1.249 | 6558.4 | `$.local_nvme.arms[1]` |

### Comparability, cache and load limits

Equal 8 GiB totals do **not** establish byte equality. In both v2 reports, `$.stage_source.arm_names` contains numeric names such as `0-1643568890`, while `$.hdd_source.arm_names` contains names such as `model-00001-of-00120.safetensors`. Stage records 38 files/42.761 GiB, whereas HDD records 128 files/163.473 GiB. Local NVMe reads synthetic files written with a varied pattern, as described in the [retained narrative](gate_1730_local_sets_2026-10-10.md#speed-mibs-fixed-8-gib-arm-16-files-x-512-mib-prefix); it is not an A8S copy. No manifest or digest/range comparison proves that these arms read identical A8S bytes. No same-A8S comparison or full-set timing proof is supplied.

The recorded load arrays remain available at each row's `.load` path in the two v2 reports. The [narrative's load discussion](gate_1730_local_sets_2026-10-10.md#speed-mibs-fixed-8-gib-arm-16-files-x-512-mib-prefix) reports one-minute load ranges of 5.49–7.76 on sparky and 5.39–7.46 on sparklina, uncontrolled fleet load, and the differing telemetry `read_bytes`/`rchar` counters. Every arm records `.idle_threads: 0`; per-worker elapsed values remain at `.per_thread_s`. Zero idle workers is not evidence of controlled background load.

The JSON `cold` labels remain recorded labels. `.cache_dropped` records 16 for first-pass arms and 0 for repeats; advisory eviction does not prove client-cold state. The 16-stream first arm ran before the 4/1-stream first arms, which reread server-side bytes. `$.stage_source.arcstats_before/after` and `$.hdd_source.arcstats_before/after` are null on both hosts. Repeated passes, null server ARC, uncontrolled load and the reported 4 GiB action cap do not prove client-cold or client-warm state; an 8 GiB arm could not all remain in that client cache. The narrative records that these were not `--measurement` actions. No observation here is newly measured or execution-verified, and no 163 GiB extrapolation or A8S loader/payback claim follows.

## Historical v2 Docker bind observations

These are transcriptions of each v2 report's `$.bind` object. Docker version is the server version from `$.bind.docker.stdout` (return code 0). References come from `$.bind.image`; registry references differ even though the tag matches.

| Host | Historical Docker server | Image reference used | Nested local-only marker | Missing-source result |
|---|---|---|---|---|
| sparky | 29.6.2 | `192.168.1.107/prismaquant/spark-vllm-nccl230:nightly-20260929` | `nested_shows_local_only: true`, rc 0; `marker.txt`, `second.txt`, `local-wins` | exit 125; bind source path does not exist |
| sparklina | 29.6.2 | `localhost/prismaquant/spark-vllm-nccl230:nightly-20260929` | `nested_shows_local_only: true`, rc 0; `marker.txt`, `second.txt`, `local-wins` | exit 125; bind source path does not exist |

In each report, `$.bind.calls[0]` is the baseline listing with `canonical.txt` and return code 0; `$.bind.calls[1]` is `nested bind ls+cat`, with `.returncode: 0` and the local marker output in `.stdout`. `$.bind.nested_shows_local_only` is true. `$.bind.calls[2]` is `mount with missing src`, with `.returncode: 125`, empty `.stdout`, and a missing bind-source error in `.stderr`.

The `.argv` fields record an existing `-v /mnt/shared:/mnt/shared:ro` and a nested `--mount type=bind,src=<local>,dst=/mnt/shared/gate-1730-<host>,readonly`. Historical local sources were `/var/tmp/gate-1730-src-w0_8_r7r` on sparky and `/var/tmp/gate-1730-src-qmnbascs` on sparklina; the failing source appended `/missing`. These temporary probe paths are not supplied current A8S destinations. The full calls and image listings remain in the raw reports (`$.bind.calls` and `$.bind.images.stdout`).

Immutable image digests are absent. A transcript of an attempted write failing through the read-only bind is also absent: recording `readonly` in argv and observing local-marker precedence does not demonstrate write refusal. The partial historical results support nested-local visibility and missing-source rejection, but **complete bind proof remains UNPROVEN on both hosts**.

## Decision rule and disposition

Each required leg is **GO** when supplied evidence establishes its criterion, **NO-GO** when supplied evidence contradicts it, and **UNPROVEN** otherwise.

Overall is **NO-GO** if any required leg fails. It is **GO** only when space and complete bind pass on both hosts, and measured load savings times a stated positive integer reload count strictly exceed measured copy cost under a stated workload, with disk risk recorded. Otherwise it is **UNPROVEN**. In time units, the payback condition is:

```text
reload_count is a stated positive integer
reload_count * (measured_remote_load_s - measured_local_load_s)
    > measured_full_set_copy_s
```

The paired loads must use comparable A8S bytes and the stated workload/cache/load conditions. Equality does not pass the strict payback inequality. No reload count, comparable measured A8S load saving, or measured full-set copy cost is supplied, so none is assumed.

| Required leg | sparky | sparklina | Evidence and remaining limit |
|---|---|---|---|
| Space | GO | GO | Supplied 2026-10-12 lead ruling; exact supplied sparklina bytes satisfy the copy inequality. Independent verification of all current D1 inputs remains outstanding. |
| Complete bind | UNPROVEN | UNPROVEN | Historical nested-local marker and missing-source exit 125 are recorded; immutable image digests and read-only write-failure transcripts are absent. |
| Comparable A8S/payback | UNPROVEN | UNPROVEN | Historical 8 GiB arms lack byte-equality proof and use synthetic local data; they supply neither full-set cost nor the required workload and positive integer reload count. |
| Overall | UNPROVEN | UNPROVEN | Space GO does not establish bind or payback. |

The two-host overall decision is **UNPROVEN**. k1 remains blocked on complete bind and comparable A8S/payback evidence, not the superseded space failure. Disk risk remains open: copying consumes local headroom, temporary copy space and Docker growth may compete, and current destination, cleanup, floor and Docker allowance evidence is missing. The lead's ruling does not supply a quantified disk-risk assessment for the intended workload.

## Operations follow-up

The following inputs and live collection steps remain outstanding. Only an authorized operator performs them; this writer has gathered no live evidence.

1. **Current space and disk risk:** collect raw available and filesystem-size bytes on both Sparks for the actual destination and Docker filesystem, including a fresh sparky available-byte observation. Record destination path/device, cleanup actions and before/after transcript, Docker allowance and expected growth, temporary copy headroom and disk-risk assessment. Verify both exact-byte D1 inequalities for the intended A8S destination; do not reconstruct fresh totals from historical rounded GiB fields.
2. **A8S identity and equal inputs:** obtain the current complete model manifest, exact file sizes and digests, selected range offsets/lengths, and equality evidence for identical A8S ranges across stage and local NVMe (and any explicitly authorized HDD comparison). Historical numeric stage names, HDD model names and synthetic local files do not supply that evidence. Record actual sources, local destination and successful full-set copy integrity.
3. **Controlled paired speed/cache evidence:** collect paired/interleaved timings at 1/4/16 streams over those identical ranges, with actual bytes read, elapsed times, per-worker activity and before/after load. Record eviction method and evidence of client state, server ARC/cache evidence or its unavailability, memory limits and campaign load. Demonstrate any claimed client-cold/client-warm state rather than promoting the old labels; do not infer it from repeats or activity counters.
4. **Workload and payback:** state the actual A8S loader/concurrency/workload and positive integer reload count; measure full-set copy cost and paired full-set remote/local load costs under that workload. Apply the strict savings-times-reloads inequality with recorded disk risk. Synthetic write throughput and 8 GiB read rates cannot replace these costs. If the workload runs vLLM, follow the explicit vLLM exemption in repository agent instructions; non-vLLM measurement work still submits through PrismaBuild.
5. **Complete bind proof:** collect immutable image digests for the images actually used on each host, exact Docker version/argv and source/destination context, and transcripts confirming local-byte visibility, missing-source rejection, and an attempted write failing through the read-only nested bind. Tag equality is not image identity. Existing historical transcripts must remain retained.
6. **Execution provenance:** reconcile the historical action keys reported in the [narrative](gate_1730_local_sets_2026-10-10.md): sparky `4c034a6c16e47d2d704912881042f4711ae4638e2c3c92925a2019eb3bbdedef` and sparklina `5d0a66beabc10df12f2bcaa52a371794103f21ca2afbac36677ca2301b8e4d7b`. Their attempt-bound terminal records, logs and CAS receipts remain unverified here. Obtain and verify those actual artifacts, and retain equivalent artifacts for future collection; use the existing [action-result reader](../../src/prismabuild/action_result.py), not invented receipt fields. A reported key or submission acknowledgement alone is not execution proof.
7. **Admission and policy:** for non-vLLM measurement/test jobs, use PrismaBuild with declared aggregate CPU/memory/GPU demand, bounded native threads and preserved assigned affinity. The planned fixed 8 GiB arm needs at least 17 CPUs, 24 GiB memory, zero GPUs and one native thread per worker. Verify target-platform support and deployed `progress-v1` plus `progress-helper-v1` support on the pool transport before progress-based host-specific submission. Source support alone is not deployed support. Retain historical evidence and the scoped HDD refusal unless explicit TIER-04 authority is supplied; no such authority is supplied here, and historical HDD observations do not authorize another HDD read. Apply the [execution policy](../agent_execution_policy.md), [operating guide](../operating_prismabuild.md) and [staged-read contract](../staged_read_contract_2026-09-20.md) to that separately authorized work.

No v3 measurement JSON, model manifest, Docker transcript, new action key or measurement is created by this update. This document records supplied evidence; it does not satisfy outstanding live-measurement acceptance.

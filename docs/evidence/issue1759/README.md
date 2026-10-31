# Funded READY demand in streaming pressure

Fixes #1759. `window_pressure` counted a fully funded READY row twice for a streaming newcomer: once in held fence tokens and again in queued demand. Publication and declared relief already exclude that row. Both relief paths now use queued new money.

On the same 20 GiB tier, the funded fixture holds 12 GiB and has an 11 GiB newcomer. Its 3 GiB shortfall fits within 6 GiB of eligible orphans. Counting the funded 4 GiB again makes the probe reject the additional relief as futile. The ordinary phase-pressure result is then 10 GiB instead of 11 GiB. Unfunded controls still require 13 GiB of free space.

The regression adds declared and streaming variants to the existing real queue tests. It checks exact pressure and runs the integrated tier cycle until the lead publishes. No sleeps or timing assertions were added.

## Fleet evidence

The RED production source is unchanged main `6185a6ee60f`; test-only commit `4f73cdef21e`. GREEN source is `6f99362ed1b07a664d6e049a366ad3e020bd6de2`. The one-time rebase found origin/main unchanged.

| Action | Result |
| --- | --- |
| `6ba5d38de9ea9cf855bf2430e9a3dbe869b04bd3267ff2e3fc0da7ca3abd969e` | 1 failed, 3 passed, 5 deselected in 2.42s |
| `2eedb35d3ab3cea1e95ef7e9a316eeaaa2397d012da03e9dd06abc01c60e6da8` | 9 passed in 3.34s |
| `43807ca111e4d5ee80a8f8e669b3f5878d0add82d4bce1e9494e2447475b3713` | 4 passed in 0.55s |
| `9af77f2ce05897db9c567957c0032dee97a8d79eb833e27d8eefe2451026c099` | 11 passed in 0.99s |
| `531edbad112bba7f9badd935e20a10a13dd852a5bf0c0ad94f9554e50bec320d` | 13 passed in 1.58s |
| `f98af0d12aa404c7070189607a2c787b502972e7cbf74b3a437081dbb3c4211c` | 16 passed in 1.67s |
| `4bc17055678e26026d5a25598b4d3afa70a6e8279ddd898afa9ed2284c3fb86b` | 2 passed in 0.72s |
| `6c5c3d12dad4b416638ac2a4519e2c60327e2563883ff48c5b6eaf1d1962e3ea` | 8 passed in 0.53s |

GREEN totals 63 passed, no skips, across seven files. These deterministic accounting tests do not repair a flake. All actions ran on dl380g10 through published `pbtest`, with 1 CPU, 4 GiB RAM, zero GPUs, one native thread, priority -10 and a 300-second execution limit per shard. The client retained default max-clients 8. Units were `pb-pri190-red2.service` and `pb-pri190-green-6f99362.service`.

[`fleet-evidence.json`](fleet-evidence.json) contains terminal outcomes, log identities, pytest output and success CAS receipts. All eight action/log observations were complete, all cleanup completed and all complete logs matched their recorded hashes. The RED action has no success receipt. Seven GREEN receipts passed full-manifest `PrismaBuildCAS.lookup`, and local-result claims passed every `pb_verify_claim(hash_payload=True)` check. All eight retained Git bundles and the changed production/test source files matched their parent commits.

The raw reports, observations, payloads, source comparisons and rerunnable read-only collection scripts are retained at `/mnt/shared/paperclip-runs/PRI-190/` and in the Paperclip evidence attachment. They can be read again with:

```sh
python3 /mnt/shared/paperclip-runs/PRI-190/collect_evidence.py red.json green.json
python3 /mnt/shared/paperclip-runs/PRI-190/check_sources.py
```

To repeat GREEN through the published client, use the persistent-unit command retained in `green-command.txt` in that attachment. PB owns its file sharding and placement.

[`merge-acceptance.json`](merge-acceptance.json) states the target and current evidence for INV-01, PRG-04, LIVE-01 and the preserved SM-02, SM-03 and SAFE-01 protections. The no-futile-relief bound, live-holder exclusion, leases and funding validation remain in their existing code. This is component qualification only. This task neither publishes nor restarts a fleet runtime, and proves no staged-only campaign completion.

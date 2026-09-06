# Grafana dashboard qualification, 2026-09-05

The versioned dashboard and collector are ready for deployment. Deployment to
the existing beelink instance is pending authenticated access: Grafana at
`192.168.1.120:3000` reports version 12.4.1 and a healthy database, but its
datasource/dashboard APIs return HTTP 401. Beelink rejected the available
keys for SSH user `rob`. No existing beelink dashboard, datasource or service
has been changed.

The dashboard has 27 panels/rows and 31 Prometheus expressions covering worker
health, queue progress, CPU/GPU job placement, reservations, host CPU/RAM/PSI,
GPU power, separate discrete framebuffer memory, admission evidence and recent
outcomes/timings. All three existing Netdata endpoints are reachable. The
exporter is read-only and emits bounded aggregate labels, with unknown and
stale state distinguished from zero activity.

All validation ran through published PrismaBuild, with native threads bounded
to one and portable placement:

| Qualification | Actual result | Action key |
|---|---|---|
| Publication, imports, CLI help, collector and safe deployment | 115 passed, no skips, Sparklina; 4 CPUs / 4 GiB | `252a3527a742fa252a49a8d6751ef169b97972f9093f6fe53fa5b88bb2ea3aa9` |
| Grafana 12.4.1 + Prometheus 3.5.0 + Firefox, real fleet metrics | Four scrape targets up; all 31 queries successful; no browser errors; Sparklina; 4 CPUs / 4 GiB | `fbec292fb892b514493752282ec676ebe8c1319b09d41eb60e93d97f68dd024c` |

Terminal exit status, canonical receipt digests and actual payload hashes were
independently checked. The full-stack run used temporary, loopback-only,
PB-contained containers and removed its exact owned containers on completion.
It did not install another production Grafana instance. The discrete VRAM
panel correctly returned no series because both GPU workers are GB10 with
shared system memory.

The preview and complete query/target report are retained at
`/mnt/shared/prismabuild-fleet/qualification/grafana/preview-20260906-c/`.
`dashboard.png` is an actual browser capture; `report.json` records the
observations. Receipt checks are in
`/home/rob/pb-logs/grafana-final-receipts.json`.

Earlier qualification caught missing exporter CLI help and a missing x86
`ensurepip` component. Help was added; the isolated preview environment now
uses the already-installed pip rather than depending on a system venv package.
The x86 run passed all datasource/query checks before that browser setup
failure. An earlier Sparky browser run also passed. These runs are retained
as diagnostic evidence, not represented as a successful beelink deployment.

Deployment must still inspect and reuse beelink's actual metric backend,
install the PB collector and additional scrape jobs, import the dashboard
using its real datasource UID, and verify authenticated readback/live queries.
The procedure is in [the deployment guide](../fleet/observability/README.md).

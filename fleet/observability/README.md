# PrismaBuild Grafana dashboard

`prismabuild.json` is the dashboard for the existing Grafana instance on
beelink (`http://192.168.1.120:3000`). Its stable UID is `prismabuild-fleet`.
It adds a PrismaBuild dashboard; it does not replace another dashboard or
change the instance's home page. Regenerate it after edits with
`python3 fleet/observability/build_dashboard.py`.

The dashboard combines two read-only sources:

- `tools/fleet/pbmetrics.py` reads PB offers, ready/claimed records, retained
  endings and saved admission evidence. See [the metric contract](../../docs/pb_metrics.md).
- Existing Netdata agents supply whole-host CPU/RAM/PSI and NVIDIA GPU power,
  temperature and framebuffer memory where available. These include unrelated
  host activity, which is relevant to PB admission.

No panel runs a scheduler decision or invokes a GPU workload. Unknown samples
remain missing. The GPU framebuffer panel intentionally shows only workers
whose PB observation declares discrete memory. Both current GB10 workers use
system RAM instead. A saved admission plateau is historical decision evidence;
its age is displayed separately. Terminal outcomes are rolling-window gauges
over retained records, not a permanent event counter or independent CAS audit.

## Deployment

First inspect the existing Grafana datasource and Prometheus configuration.
Reuse that backend where available. `prometheus.scrape.yml` contains **only
additional scrape jobs**, not a replacement configuration. Preserve existing
targets, credentials, retention and dashboards. The addresses were verified
on 2026-09-05; update targets when workers move or are added. The `host` label
must match PB offer names. Only the listed Netdata metrics are retained.

Install a committed PB checkout at `/opt/prismabuild-observability`, or adapt
the included service's `ExecStart` to an immutable release in Rob's service
directory. The exporter needs read access to the shared fleet mount. Install
`prismabuild-metrics.service` on one host that has that mount, ordinarily
Sparky; do not run a collector per worker or edit a sealed worker generation.
The service bounds its own memory and cannot write the queue. Restrict access
to the monitoring network using the host's existing network policy.

After validating and reloading the additional Prometheus scrape configuration,
confirm that `prismabuild` and the three `prismabuild-netdata` targets are up.
The Grafana Prometheus datasource must point to that backend. Use its actual
UID below. The deployment tool checks datasource health, saves a previous PB
dashboard version if one exists, preserves its folder, and uses Grafana's
version check to refuse concurrent changes.

```bash
python3 fleet/observability/deploy_dashboard.py \
  --url http://192.168.1.120:3000 \
  --auth-file /path/to/private/grafana-auth.json \
  --datasource-uid EXISTING_PROMETHEUS_UID \
  --backup-dir /path/to/dashboard-backups
```

The private auth file contains `{"token":"..."}` or
`{"username":"...","password":"..."}`. Never commit it. The script does not
print credentials or install a new Grafana instance. The supported API and
provisioning mechanisms are described in
[Grafana's provisioning documentation](https://grafana.com/docs/grafana/latest/administration/provisioning/).

## Qualification through PB

All tests, including the temporary stack and browser, must be admitted by PB.
Focused fixtures cover collector failure/staleness semantics and safe dashboard
deployment. The opt-in stack probe runs Grafana 12.4.1 (the observed beelink
version) and Prometheus 3.5.0 in temporary containers bound only to loopback.
It reads real fleet state, validates all panel expressions against collected
metrics, optionally renders Firefox, and removes only the containers it owns.
The exporter and browser are children of the admitted action.

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
  --cwd /home/rob/prismabuild --anywhere --cpus 4 --demand mem_gb=4 \
  --env OMP_NUM_THREADS=1 --env MKL_NUM_THREADS=1 --env OPENBLAS_NUM_THREADS=1 -- \
  /home/rob/venvs/pb-cpu/bin/python fleet/observability/qualify_dashboard.py \
  --artifacts /mnt/shared/prismabuild-fleet/qualification/grafana/UNIQUE_RUN \
  --screenshot
```

Keep the actual terminal, logs, CAS receipt and `report.json`. A successful
temporary-stack probe proves the dashboard can operate; it does not prove
deployment to beelink. That requires authenticated deployment, dashboard
readback and successful queries through beelink's actual datasource.

## Rollback

Restore a backed-up PB dashboard through Grafana's API using its current
version, or remove only UID `prismabuild-fleet` if it was newly created.
Remove only the two added scrape jobs and reload the backend. Stop/disable
only the PB metrics service if retiring collection. This leaves workers,
queues and other monitoring dashboards untouched.

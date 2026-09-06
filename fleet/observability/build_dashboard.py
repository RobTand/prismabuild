#!/usr/bin/env python3
"""Generate the versioned Grafana dashboard; this does not contact Grafana."""
import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "${datasource}"}
HOST = 'host=~"$host"'
PANELS = []


def panel(kind, title, x, y, w, h, queries=(), unit="short", description="", **extra):
    result = {
        "id": len(PANELS) + 1, "type": kind, "title": title,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "description": description, "datasource": DS,
        "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "palette-classic"},
            "noValue": "No observation", "decimals": 1, "min": 0}, "overrides": []},
        "targets": [{"refId": chr(65 + i), "expr": expr, "legendFormat": legend,
                     "datasource": DS, "range": kind == "timeseries", "instant": kind != "timeseries"}
                    for i, (expr, legend) in enumerate(queries)],
    }
    if kind == "timeseries":
        result["options"] = {"tooltip": {"mode": "multi", "sort": "desc"},
                             "legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull", "max"]}}
        result["fieldConfig"]["defaults"]["custom"] = {
            "drawStyle": "line", "lineWidth": 2, "fillOpacity": 10,
            "showPoints": "never", "spanNulls": False, "axisBorderShow": False,
        }
        if unit == "short":
            result["fieldConfig"]["defaults"]["custom"]["axisSoftMax"] = 1
    if kind == "stat":
        result["options"] = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                             "orientation": "auto", "textMode": "auto", "colorMode": "value",
                             "graphMode": "none", "justifyMode": "auto"}
        result["fieldConfig"]["defaults"]["decimals"] = 0
    result.update(extra)
    PANELS.append(result)
    return result


def row(title, y):
    PANELS.append({"id": len(PANELS) + 1, "type": "row", "title": title,
                   "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "collapsed": False, "panels": []})


def build():
    PANELS.clear()
    panel("text", "PrismaBuild / fleet operations", 0, 0, 24, 3, options={"mode": "markdown", "content":
          "**Useful work, visible across the fleet.** Queue → admission → execution → recorded outcome. "
          "Use the worker filter to compare placement and headroom. An empty ready queue means no waiting work.\n\n"
          "**GPU reading:** GB10 shares system RAM; power, active jobs and memory pressure provide context. "
          "Discrete GPU framebuffer memory is shown separately where available. Gaps mean missing observations, not zero load."})
    stats = [
        ("Live workers", f"sum(prismabuild_worker_up{{{HOST}}})", "short"),
        ("Waiting · fleet", 'prismabuild_queue_items{state="ready"}', "short"),
        ("Running · selected workers", f"sum(prismabuild_active_jobs{{{HOST}}})", "short"),
        ("GPU jobs · selected workers", f'sum(prismabuild_active_jobs{{{HOST},kind="gpu"}})', "short"),
        ("Oldest waiting · fleet", 'prismabuild_queue_oldest_age_seconds{state="ready"}', "s"),
        ("PB collection", "prismabuild_collection_success", "short"),
    ]
    for i, (title, expr, unit) in enumerate(stats):
        p = panel("stat", title, i * 4, 3, 4, 4, [(expr, "")], unit)
        if i == 5:
            p["fieldConfig"]["defaults"].update({"mappings": [{"type": "value", "options": {
                "0": {"text": "Incomplete", "color": "red"}, "1": {"text": "Current", "color": "green"}}}]})
    row("Work distribution & queue", 7)
    panel("timeseries", "Active jobs by worker", 0, 8, 12, 8,
          [(f"prismabuild_active_jobs{{{HOST}}}", "{{host}} · {{kind}}")], description="Claimed attempts, including setup and cleanup. GPU jobs are classified separately from CPU-only jobs.")
    panel("timeseries", "Ready and claimed · whole fleet", 12, 8, 12, 8,
          [("prismabuild_queue_items", "{{state}}")], description="Queue counts are fleet-wide: a ready action has not yet been assigned a worker.")
    p = panel("table", "Worker capacity and reservations · now", 0, 16, 24, 7, [
        (f"prismabuild_worker_up{{{HOST}}}", "Live"),
        (f"sum by(host)(prismabuild_active_jobs{{{HOST}}})", "Active jobs"),
        (f'prismabuild_worker_capacity{{{HOST},resource="cpu"}}', "CPU capacity"),
        (f'prismabuild_reserved_resources{{{HOST},resource="cpu"}}', "CPU reserved"),
        (f'prismabuild_worker_capacity{{{HOST},resource="memory_bytes"}} / 1073741824', "RAM budget · GiB"),
        (f'prismabuild_reserved_resources{{{HOST},resource="memory_bytes"}} / 1073741824', "RAM reserved · GiB"),
        (f'prismabuild_worker_capacity{{{HOST},resource="gpu"}}', "Physical GPUs"),
    ], description="CPU reservations are declared peak demand, not measured consumption. Adaptive CPU lending can make reservations exceed physical capacity. RAM remains fully reserved.")
    for target in p["targets"]:
        target["format"] = "table"
        target["expr"] = "max by(host) (" + target["expr"] + ")"
    p["transformations"] = [{"id": "joinByField", "options": {"byField": "host", "mode": "outerTabular"}},
        {"id": "organize", "options": {"excludeByName": {"Time": True, "Time 1": True, "Time 2": True, "Time 3": True,
             "Time 4": True, "Time 5": True, "Time 6": True, "__name__": True, "job": True, "instance": True, "resource": True},
         "renameByName": {"host": "Worker", **{f"Value #{chr(65+i)}": target["legendFormat"] for i,target in enumerate(p["targets"])}}}}]
    row("GPU activity & memory domains", 23)
    panel("timeseries", "GPU power draw", 0, 24, 12, 8,
          [(f'netdata_nvidia_smi_gpu_power_draw_Watts_average{{job="prismabuild-netdata",{HOST}}}', "{{host}} · {{product_name}}")], "watt",
          "Measured by Netdata. On GB10, NVML power is GPU-reported power, not whole-system energy. Low power alone does not prove useful headroom.")
    panel("timeseries", "Concurrent GPU jobs", 12, 24, 12, 8,
          [(f'prismabuild_active_jobs{{{HOST},kind="gpu"}}', "{{host}}")], description="Adaptive sharing can run several generation jobs on one physical GPU. Measurements and exclusive jobs retain isolation.")
    panel("timeseries", "System RAM · used and PB reservation", 0, 32, 12, 8, [
        (f'sum by(host)(netdata_system_ram_MiB_average{{job="prismabuild-netdata",{HOST},dimension="used"}}) * 1048576', "{{host}} · used"),
        (f'prismabuild_reserved_resources{{{HOST},resource="memory_bytes"}}', "{{host}} · PB reserved"),
        (f'prismabuild_worker_memory_available_bytes{{{HOST}}}', "{{host}} · available")], "bytes",
          "System RAM includes shared GB10 CPU/GPU memory. Netdata used RAM excludes reclaimable cache; available is the worker's coarse MemAvailable observation. Reservations are peak budgets, not resident usage.")
    panel("timeseries", "Discrete GPU framebuffer memory", 12, 32, 12, 8,
          [(f'netdata_nvidia_smi_gpu_frame_buffer_memory_usage_B_average{{job="prismabuild-netdata",{HOST},dimension=~"used|free"}} and on(host) prismabuild_worker_memory_domain_info{{domain="discrete"}}', "{{host}} · {{dimension}}")], "bytes",
          "Only discrete-memory workers appear here. GB10 uses shared system RAM and should have no framebuffer-memory series. Missing values are not zero or spare capacity.")
    row("CPU use, admission & pressure", 40)
    panel("timeseries", "Host CPU busy", 0, 41, 8, 8,
          [(f'sum by(host)(netdata_system_cpu_percentage_average{{job="prismabuild-netdata",{HOST},dimension!~"idle|iowait"}})', "{{host}}")], "percent",
          "Whole-host CPU activity, including work outside PB. I/O wait is excluded from useful CPU busy time.")
    panel("timeseries", "CPU and RAM pressure · 10-second PSI", 8, 41, 8, 8, [
        (f'netdata_system_cpu_some_pressure_percentage_average{{job="prismabuild-netdata",{HOST},dimension="some 10"}}', "{{host}} · CPU"),
        (f'netdata_system_memory_some_pressure_percentage_average{{job="prismabuild-netdata",{HOST},dimension="some 10"}}', "{{host}} · RAM")], "percent",
          "Pressure measures time work stalls waiting for resources. Rising pressure can close admission without terminating healthy jobs.")
    panel("timeseries", "GPU admission plateau · last decision", 16, 41, 8, 8,
          [(f'prismabuild_admission_plateau{{{HOST},resource="gpu"}}', "{{host}}")], description="1 means the saved GPU feedback state detected a power-response plateau. This is historical decision evidence; check its age below, not a live saturation guarantee.")
    panel("timeseries", "Admission evidence age", 0, 49, 12, 7,
          [(f"prismabuild_admission_evidence_age_seconds{{{HOST}}}", "{{host}} · {{resource}}")], "s",
          "Time since the last persisted admission observation. It can grow normally while the queue is empty. Workers sample again before granting adaptive admission.")
    panel("timeseries", "Collection and host telemetry health", 12, 49, 12, 7, [
        ("prismabuild_collection_success", "PB collection complete"),
        (f'up{{job="prismabuild-netdata",{HOST}}}', "{{host}} · Netdata reachable")], description="0 indicates incomplete collection or an unreachable telemetry endpoint. Other panels leave unknown values as gaps.")
    row("Recorded outcomes · rolling hour", 56)
    panel("timeseries", "Outcomes by worker · last hour", 0, 57, 12, 8,
          [(f"prismabuild_terminal_outcomes{{{HOST}}}", "{{host}} · {{outcome}}")], description="Rolling-window gauges over retained terminal records, not monotonic counters. Executed means exit 0 was recorded; this dashboard does not independently verify every CAS payload. Failed includes intentional red tests and workload failures.")
    panel("timeseries", "Queue wait · recent completed work", 12, 57, 12, 8,
          [(f"prismabuild_queue_wait_seconds{{{HOST}}}", "{{host}} · {{stat}}")], "s",
          "Mean and maximum publish-to-claim delay among recent retained endings. Retried work can include earlier waiting time; this is not a histogram percentile.")
    panel("timeseries", "Execution time · recent completed work", 0, 65, 12, 7,
          [(f"prismabuild_execution_seconds{{{HOST}}}", "{{host}} · {{stat}}")], "s")
    panel("text", "Reading this dashboard", 12, 65, 12, 7, options={"mode": "markdown", "content":
          "- **Ready = 0:** there is no waiting work to distribute.\n"
          "- **Ready grows while a worker is idle:** check placement requirements, memory budgets, pressure and admission evidence.\n"
          "- **Reservations exceed observed CPU use:** jobs have variable demand; PB learns when it can safely admit more.\n"
          "- **GB10 framebuffer panel is empty:** expected. Its GPU shares system RAM.\n"
          "- **Failure ≠ scheduler failure:** inspect the action log, terminal record and CAS receipt.\n\n"
          "The collector is read-only. It cannot admit, cancel or resize work. Queue metrics refresh every 10 seconds; host telemetry is sampled independently. Retained terminal history is bounded and is not a permanent audit ledger."})
    # Leave room for the introduction at ordinary laptop/desktop widths.
    PANELS[0]["gridPos"]["h"] = 5
    for item in PANELS[1:]:
        item["gridPos"]["y"] += 2
    return {
        "uid": "prismabuild-fleet", "title": "PrismaBuild · Fleet & Work", "description": "Queue, adaptive admission, worker activity and outcomes across the PrismaBuild fleet.",
        "tags": ["prismabuild", "fleet", "gpu"], "schemaVersion": 41, "version": 1,
        "editable": True, "timezone": "browser", "refresh": "10s",
        "time": {"from": "now-1h", "to": "now"},
        "timepicker": {"refresh_intervals": ["10s", "30s", "1m", "5m"]},
        "annotations": {"list": []}, "links": [],
        "templating": {"list": [
            {"name": "datasource", "label": "Metrics", "type": "datasource", "query": "prometheus",
             "current": {"text": "PrismaBuild", "value": "prismabuild-prometheus"}, "refresh": 1},
            {"name": "host", "label": "Worker", "type": "query", "datasource": DS,
             "query": "label_values(prismabuild_worker_capacity, host)", "refresh": 1,
             "multi": True, "includeAll": True, "allValue": ".*", "sort": 1,
             "current": {"text": ["All"], "value": ["$__all"]}},
        ]}, "panels": PANELS,
    }


if __name__ == "__main__":
    Path(__file__).with_name("prismabuild.json").write_text(json.dumps(build(), indent=2) + "\n")

"""Phase 0 guard calibration: interleaved probe runs on dl380g10. No source change.

Probe mimics testcost timing workload at short scale. Full testcost capture
5a4dcf687cb4 runs 876 s wall with 4 workers on CPUs 0-3 (4 CPUs / 9 GB,
telemetry cpu_user 3444 s vs sys 17 s, mem peak 593 MB). 150 full runs would
hold the host 35 h, so this probe keeps the same shape (4 parallel workers on
CPUs 0-3, user-space CPU-bound sha256 plus streaming memory copy, CPU 0
included) at ~1.4 s per run. Neighbor load is 4 CPU-bound loops on the named
CPUs. Host samples use the same counters as adaptive_cpu and the same
_idle_statistics max/window rule from _judge_idle.
"""
import glob
import hashlib
import json
import os
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path

PROBE_CPUS = [0, 1, 2, 3]
MEM_MB = 64
MEM_ITERS = 100
CASES = {
    "idle": [],
    "smt": [40, 41, 42, 43],
    "samenode": [4, 5, 6, 7],
    "othernode": [10, 11, 12, 13],
    "othersocket": [20, 21, 22, 23],
}
CPU_ITERS = 300

WORKER_SRC = (
    "import hashlib,os,time;"
    "data=os.urandom(1048576);"
    "t0=time.perf_counter();"
    f"[(hashlib.sha256(data).digest()) for _ in range({CPU_ITERS})];"
    "t1=time.perf_counter();"
    f"src=bytearray(os.urandom({MEM_MB}*1048576));dst=bytearray(len(src));"
    "t2=time.perf_counter();"
    f"[dst.__setitem__(slice(None),src) for _ in range({MEM_ITERS})];"
    "t3=time.perf_counter();"
    "print(f'{t1-t0:.4f} {t3-t2:.4f}')"
)

LOAD_SRC = "import hashlib,os;data=os.urandom(65536);[hashlib.sha256(data).digest() for _ in iter(int,1)]"


def read_counters(cpus):
    values = {}
    try:
        for line in Path("/proc/stat").read_text().splitlines():
            f = line.split()
            if not f or not f[0].startswith("cpu") or not f[0][3:].isdigit():
                continue
            cpu = int(f[0][3:])
            if cpu not in cpus:
                continue
            ticks = [int(x) for x in f[1:9]]
            if len(ticks) != 8:
                return None
            total = sum(ticks)
            values[str(cpu)] = [total - ticks[3] - ticks[4], total]
        psi = next(line for line in Path("/proc/pressure/cpu").read_text().splitlines()
                   if line.startswith("some "))
        pressure = int(dict(p.split("=") for p in psi.split()[1:])["total"])
    except (OSError, ValueError, StopIteration):
        return None
    if len(values) != len(cpus):
        return None
    return {"cpus": values, "psi_total": pressure, "sampled_unix": time.time()}


def observe(prev, cur):
    elapsed = cur["sampled_unix"] - prev["sampled_unix"]
    deltas = [(v[0] - prev["cpus"][k][0], v[1] - prev["cpus"][k][1])
              for k, v in cur["cpus"].items()]
    psi_delta = cur["psi_total"] - prev["psi_total"]
    if not all(0 <= b <= t and t > 0 for b, t in deltas) or psi_delta < 0:
        return None
    per = {k: b / t for k, (b, t) in zip(cur["cpus"], deltas)}
    return {"busy_cpus": sum(b / t for b, t in deltas),
            "psi_some": min(1.0, psi_delta / (elapsed * 1e6)),
            "per_cpu_busy": per, "interval_s": elapsed}


def start_load(cpus):
    procs = []
    for c in cpus:
        p = subprocess.Popen(["taskset", "-c", str(c), sys.executable, "-c", LOAD_SRC],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(p)
    return procs


def stop_load(procs):
    for p in procs:
        try:
            p.terminate()
        except OSError:
            pass
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                p.kill()
            except OSError:
                pass


def run_probe_once():
    procs = []
    for c in PROBE_CPUS:
        p = subprocess.Popen(["taskset", "-c", str(c), sys.executable, "-c", WORKER_SRC],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        procs.append(p)
    t0 = time.perf_counter()
    outs = []
    for p in procs:
        try:
            o, _ = p.communicate(timeout=120)
            outs.append(o.strip())
        except subprocess.TimeoutExpired:
            p.kill()
            outs.append("TIMEOUT")
    wall = time.perf_counter() - t0
    cpu_ts, mem_ts = [], []
    for o in outs:
        try:
            a, b = o.split()
            cpu_ts.append(float(a))
            mem_ts.append(float(b))
        except (ValueError, IndexError):
            pass
    return wall, cpu_ts, mem_ts


def main():
    seed = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    rounds = int(sys.argv[2]) if len(sys.argv) > 2 else 15
    rng = random.Random(1000 + seed)
    all_cpus = list(range(80))
    rows = []
    # Topology (self-contained receipt).
    topo = {"node0": open("/sys/devices/system/node/node0/cpulist").read().strip(),
            "node1": open("/sys/devices/system/node/node1/cpulist").read().strip(),
            "node2": open("/sys/devices/system/node/node2/cpulist").read().strip(),
            "node3": open("/sys/devices/system/node/node3/cpulist").read().strip(),
            "cpu0_siblings": open("/sys/devices/system/cpu/cpu0/topology/thread_siblings_list").read().strip(),
            "cpu0_package": open("/sys/devices/system/cpu/cpu0/topology/physical_package_id").read().strip(),
            "cpu10_package": open("/sys/devices/system/cpu/cpu10/topology/physical_package_id").read().strip(),
            "cpu20_package": open("/sys/devices/system/cpu/cpu20/topology/physical_package_id").read().strip()}
    # Host idle baseline with 80-CPU identity (window 256 per code).
    baseline = None
    for p in glob.glob("/tmp/prismabuild-admission-*/**/idle-baseline.json", recursive=True):
        try:
            d = json.load(open(p))
            if d.get("identity") == 80 and d.get("schema") == "prismabuild.idle_baseline.v1":
                samples = [s for s in d.get("samples", []) if isinstance(s, dict)]
                if len(samples) >= 10:
                    baseline = {"path": p, "n": len(samples)}
                    for field in ("busy_cpus", "psi_some"):
                        vals = [float(s[field]) for s in samples]
                        mean = sum(vals) / len(vals)
                        top = max(vals)
                        baseline[field] = {"mean": round(mean, 6), "max": round(top, 6),
                                           "margin": round(top - mean, 6),
                                           "stdev": round(statistics.pstdev(vals), 6) if len(vals) > 1 else 0}
                    baseline["span_s"] = round(samples[-1]["sampled_unix"] - samples[0]["sampled_unix"], 3)
                    baseline["window_bound"] = 256
                    break
        except (OSError, ValueError, KeyError):
            continue
    for rnd in range(rounds):
        order = list(CASES)
        rng.shuffle(order)
        for oi, case in enumerate(order):
            neigh = CASES[case]
            procs = start_load(neigh)
            time.sleep(0.2)
            c0 = read_counters(all_cpus)
            wall, cpu_ts, mem_ts = run_probe_once()
            c1 = read_counters(all_cpus)
            stop_load(procs)
            obs = observe(c0, c1) if c0 and c1 else None
            rows.append({"seed": seed, "round": rnd, "order": oi, "case": case,
                         "neighbors": neigh, "probe_cpus": PROBE_CPUS,
                         "wall_s": round(wall, 4),
                         "cpu_s": [round(x, 4) for x in cpu_ts],
                         "mem_s": [round(x, 4) for x in mem_ts],
                         "host_busy": round(obs["busy_cpus"], 4) if obs else None,
                         "host_psi": round(obs["psi_some"], 6) if obs else None,
                         "cpu0_busy": round(obs["per_cpu_busy"].get("0", -1), 4) if obs else None,
                         "per_cpu_0_3": {k: round(obs["per_cpu_busy"].get(k, -1), 4)
                                         for k in ("0", "1", "2", "3")} if obs else None})
    print(json.dumps({"host": open("/proc/sys/kernel/hostname").read().strip()
                      if Path("/proc/sys/kernel/hostname").exists() else "dl380g10",
                      "topo": topo, "baseline": baseline,
                      "probe": {"cpu_iters": CPU_ITERS, "mem_mb": MEM_MB,
                                "mem_iters": MEM_ITERS, "worker": WORKER_SRC},
                      "rows": rows}, indent=1, sort_keys=True))


if __name__ == "__main__":
    main()

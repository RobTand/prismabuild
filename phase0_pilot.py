"""Phase 0 pilot: topology, idle baseline, kernel sizing. No source change."""
import glob
import hashlib
import json
import os
import statistics
import subprocess
import sys
import time


def run(cmd):
    return subprocess.check_output(cmd, text=True, timeout=30)


def main():
    out = {}
    out["host"] = run(["hostname"]).strip()
    out["lscpu"] = run(["lscpu"])
    # Sysfs topology for probe CPUs 0-3 and neighbors.
    topo = {}
    for cpu in [0, 1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 20, 21, 22, 23, 40, 41, 42, 43]:
        for name in ("thread_siblings_list", "physical_package_id"):
            p = f"/sys/devices/system/cpu/cpu{cpu}/topology/{name}"
            try:
                topo[f"cpu{cpu}/{name}"] = open(p).read().strip()
            except OSError as exc:
                topo[f"cpu{cpu}/{name}"] = f"ERR {exc}"
    nodes = {}
    for node in glob.glob("/sys/devices/system/node/node*"):
        for name in ("cpulist",):
            p = os.path.join(node, name)
            try:
                nodes[f"{os.path.basename(node)}/{name}"] = open(p).read().strip()
            except OSError as exc:
                nodes[f"{os.path.basename(node)}/{name}"] = f"ERR {exc}"
    out["topo_cpus"] = topo
    out["topo_nodes"] = nodes
    # Idle baseline files on this box.
    cands = glob.glob("/tmp/prismabuild-admission-*/**/idle-baseline.json", recursive=True)
    cands += glob.glob("/tmp/**/idle-baseline.json", recursive=True)
    out["baseline_candidates"] = cands[:20]
    baselines = []
    for p in cands[:5]:
        try:
            d = json.load(open(p))
            samples = d.get("samples", [])
            # Compute _idle_statistics for busy_cpus and psi_some over full window.
            for field in ("busy_cpus", "psi_some"):
                vals = [float(s[field]) for s in samples
                        if isinstance(s, dict) and isinstance(s.get(field), (int, float))]
                if vals:
                    mean = sum(vals) / len(vals)
                    top = max(vals)
                    base = {"path": p, "field": field, "n": len(vals),
                            "mean": round(mean, 6), "max": round(top, 6),
                            "margin": round(top - mean, 6),
                            "stdev": round(statistics.pstdev(vals), 6) if len(vals) > 1 else 0}
                    if samples:
                        try:
                            base["span_s"] = round(float(samples[-1].get("sampled_unix", 0))
                                                   - float(samples[0].get("sampled_unix", 0)), 3)
                        except (TypeError, ValueError):
                            pass
                    baselines.append(base)
            # Identity and schema.
            baselines.append({"path": p, "schema": d.get("schema"),
                              "identity": d.get("identity"),
                              "nsamples": len(samples)})
            if len(baselines) > 12:
                break
        except (OSError, ValueError) as exc:
            baselines.append({"path": p, "error": str(exc)})
    out["baselines"] = baselines
    # Pilot kernels: fixed sha256 (CPU) and streaming copy (memory), pinned 0-3.
    # Sizes tuned so one iteration takes 1-3 s on dl380g10.
    buf = hashlib.sha256(b"phase0-testcost-proxy-v1").digest() * 1024
    sizes = {"sha256_iters": 4000, "copy_mb": 256, "copy_iters": 8}
    out["kernel_sizes"] = sizes
    data = os.urandom(1 << 20)
    t0 = time.perf_counter()
    for _ in range(sizes["sha256_iters"]):
        hashlib.sha256(data).digest()
    t1 = time.perf_counter()
    out["pilot_sha256_s"] = round(t1 - t0, 4)
    src = bytearray(os.urandom(sizes["copy_mb"] * (1 << 20) // 8))
    dst = bytearray(len(src))
    t0 = time.perf_counter()
    for _ in range(sizes["copy_iters"]):
        dst[:] = src
    t1 = time.perf_counter()
    out["pilot_copy_s"] = round(t1 - t0, 4)
    out["pilot_taskset"] = run(["taskset", "-c", "0-3", "sh", "-c", "taskset -p $$"])
    print(json.dumps(out, indent=1, sort_keys=True))


if __name__ == "__main__":
    main()

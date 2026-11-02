"""Bazel --run_under wrapper recording action CPU bursts and process counts.

Use only for calibration, with --nocache_test_results. Reads the action's
cgroup every 100 ms, including short-lived descendants in CPU accounting.
Writes sizing.json to TEST_UNDECLARED_OUTPUTS_DIR, preserving the test exit.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def cgroup():
    relative = next(line.split(':', 2)[2] for line in
                    Path('/proc/self/cgroup').read_text().splitlines() if line.startswith('0::'))
    root = Path('/sys/fs/cgroup')
    nested = root / relative.lstrip('/')
    return nested if (nested / 'cpu.stat').exists() else root


def sample(root):
    cpu = dict(line.split() for line in (root / 'cpu.stat').read_text().splitlines())
    # Count every live member, including blocked children. cgroup CPU usage
    # includes children which exit between samples; process counts are lower bounds.
    processes = set((root / 'cgroup.procs').read_text().split()) - {str(os.getpid())}
    return time.monotonic(), int(cpu['usage_usec']) / 1e6, len(processes)


def main():
    root = cgroup()
    settings = {name: (root / name).read_text().strip() for name in
                ('cpu.max', 'cpu.weight', 'memory.max', 'cpuset.cpus.effective') if (root / name).exists()}
    previous = first = sample(root)
    burst, processes, samples = 0.0, 0, 0
    child = subprocess.Popen(sys.argv[1:])
    while True:
        try:
            status = child.wait(timeout=0.1)
        except subprocess.TimeoutExpired:
            status = None
        current = sample(root)
        elapsed = current[0] - previous[0]
        if elapsed > 0:
            burst = max(burst, (current[1] - previous[1]) / elapsed)
        processes = max(processes, current[2])
        samples += 1
        previous = current
        if status is not None:
            break
    result = dict(wall_seconds=current[0] - first[0], cpu_seconds=current[1] - first[1],
                  burst_cpu=burst, processes=processes, samples=samples,
                  interval_seconds=0.1, cgroup_settings=settings, exit_code=status)
    peak = root / 'memory.peak'
    if peak.exists():
        result['peak_memory_bytes'] = int(peak.read_text())
    outputs = Path(os.environ['TEST_UNDECLARED_OUTPUTS_DIR'])
    outputs.mkdir(parents=True, exist_ok=True)
    (outputs / 'sizing.json').write_text(json.dumps(result, indent=2) + '\n')
    print('PB_TEST_SIZING ' + json.dumps(result), flush=True)
    return status


if __name__ == '__main__':
    sys.exit(main())

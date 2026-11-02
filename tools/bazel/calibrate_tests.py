"""Bazel --run_under wrapper recording action CPU bursts and process counts.

Use only for calibration, with --nocache_test_results. Reads the action's
cgroup every 100 ms, including short-lived descendants in CPU accounting.
Writes sizing.json to TEST_UNDECLARED_OUTPUTS_DIR, preserving the test exit.
For a runner-lane target, pass --runner-cgroup before the test command to
sample only its newly delegated cgroup, never the shared runner VM.
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


def runner_cgroups(base, existing):
    """New delegated test cgroups, without inspecting process argv or env."""
    if not base.exists():
        return []
    return sorted(path for path in base.iterdir()
                  if path not in existing and (path / 'cpu.stat').is_file())


def memory_usage(root, *, proc_root=Path('/proc'), page_size=None):
    """Cgroup high-water mark, or sampled aggregate RSS if unavailable.

    Some runner kernels expose no memory controller. Only numeric statm
    counters of this test cgroup's members are read; never argv or environ.
    Shared pages can be counted more than once by the RSS fallback.
    """
    peak = root / 'memory.peak'
    if peak.exists():
        return int(peak.read_text()), 'cgroup-memory.peak'
    page_size = page_size or os.sysconf('SC_PAGE_SIZE')
    resident = 0
    for pid in (root / 'cgroup.procs').read_text().split():
        try:
            resident += int((proc_root / pid / 'statm').read_text().split()[1]) * page_size
        except (FileNotFoundError, ProcessLookupError):
            pass  # A sampled child can exit before its counter is read.
    return resident, 'sampled-sum-rss'


def record(root, first, current, burst, processes, samples, status, *, runner,
           peak_memory, memory_source):
    """Keep a checkpoint even when Bazel terminates the wrapper at timeout."""
    settings = {name: (root / name).read_text().strip() for name in
                ('cpu.max', 'cpu.weight', 'memory.max', 'cpuset.cpus.effective') if (root / name).exists()}
    complete = status is not None
    result = dict(wall_seconds=current[0] - first[0], cpu_seconds=current[1] - first[1],
                  burst_cpu=burst, processes=processes, samples=samples,
                  interval_seconds=0.1, cgroup_settings=settings, exit_code=status,
                  complete=complete,
                  peak_memory_bytes=peak_memory, memory_source=memory_source,
                  isolation='runner-delegated-cgroup' if runner else 'remote-action-cgroup')
    outputs = Path(os.environ['TEST_UNDECLARED_OUTPUTS_DIR'])
    outputs.mkdir(parents=True, exist_ok=True)
    temporary = outputs / 'sizing.json.tmp'
    temporary.write_text(json.dumps(result, indent=2) + '\n')
    temporary.replace(outputs / 'sizing.json')
    if complete or samples % 10 == 0:
        prefix = 'PB_TEST_SIZING ' if complete else 'PB_TEST_SIZING_PARTIAL '
        print(prefix + json.dumps(result), flush=True)


def main():
    command = sys.argv[1:]
    runner = command[:1] == ['--runner-cgroup']
    if runner:
        command = command[1:]
    delegated = Path('/sys/fs/cgroup/prismabuild-tests')
    existing = set(delegated.iterdir()) if runner and delegated.exists() else set()
    root = None if runner else cgroup()
    previous = first = (time.monotonic(), 0.0, 0) if runner else sample(root)
    burst, processes, samples = 0.0, 0, 0
    peak_memory = 0
    child = subprocess.Popen(command)
    measurement_error = None
    while True:
        try:
            status = child.wait(timeout=0.1)
        except subprocess.TimeoutExpired:
            status = None
        if runner:
            candidates = runner_cgroups(delegated, existing)
            if len(candidates) == 1 and measurement_error is None:
                root = candidates[0]
            elif len(candidates) > 1:
                # Finish and reap the test before refusing ambiguous evidence.
                measurement_error = 'more than one new delegated test cgroup'
                root = None
        current = sample(root) if root is not None else (time.monotonic(), 0.0, 0)
        elapsed = current[0] - previous[0]
        if elapsed > 0:
            burst = max(burst, (current[1] - previous[1]) / elapsed)
        processes = max(processes, current[2])
        samples += 1
        previous = current
        if root is not None:
            memory, memory_source = memory_usage(root)
            peak_memory = max(peak_memory, memory)
            record(root, first, current, burst, processes, samples, status, runner=runner,
                   peak_memory=peak_memory, memory_source=memory_source)
        if status is not None:
            break
    if root is None:
        raise RuntimeError(measurement_error or 'no new delegated test cgroup')
    return status


if __name__ == '__main__':
    sys.exit(main())

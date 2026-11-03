#!/usr/bin/env python3
"""Join GetExecution metadata and calibration samples to derive test sizes.

Input samples map labels to lists of calibrate_tests.py's sizing.json records.
Execution input is BuildBuddyService/GetExecution's JSON response. This tool
reads saved evidence only; it never runs tests or handles account credentials.
"""
import argparse
from collections import defaultdict
from datetime import datetime
import json
import math
from pathlib import Path


def seconds(start, end):
    return (datetime.fromisoformat(end.replace('Z', '+00:00')) -
            datetime.fromisoformat(start.replace('Z', '+00:00'))).total_seconds()


def summarize_test_sizing(executions, samples):
    actions = defaultdict(list)
    for action in executions.get('execution', []):
        if action.get('actionMnemonic') == 'TestRunner':
            actions[action['targetLabel']].append(action)
    rows = []
    for label, observations in sorted(samples.items()):
        # Runner-lane actions share the runner VM's cgroup with sibling tests.
        # Only isolated remote executor samples justify per-action requests.
        if label not in actions:
            continue
        burst = max(sample['burst_cpu'] for sample in observations)
        peak = max(sample.get('peak_memory_bytes', 0) for sample in observations)
        processes = max(sample['processes'] for sample in observations)
        wall, cpu, queue = [], [], []
        for action in actions[label]:
            md = action['executedActionMetadata']
            wall.append(seconds(md['executionStartTimestamp'], md['executionCompletedTimestamp']))
            cpu.append(int(md['usageStats'].get('cpuNanos', 0)) / 1e9)
            peak = max(peak, int(md['usageStats'].get('peakMemoryBytes', 0)))
            queue.append(seconds(md['queuedTimestamp'], md['workerStartTimestamp']))
        # Ignore the sampler's small overhead around the one-CPU boundary.
        # Round real multi-core bursts up, without padding every target to 4.
        estimate = math.ceil(burst) if burst > 1.25 else 1
        properties = {}
        if estimate > 1:
            # A nonzero compute-unit request disables measured AND predicted
            # sizing. Explicit CPU and memory override its coupled defaults.
            memory_gb = max(0.5, math.ceil(peak * 1.25 / (256 * 1024**2)) / 4)
            properties = {'EstimatedComputeUnits': '1', 'EstimatedCPU': str(estimate),
                          'EstimatedMemory': f'{memory_gb:g}GB'}
        rows.append(dict(label=label, burst_cpu=round(burst, 3), processes=processes,
                         peak_memory_bytes=peak, average_cpu=round(sum(cpu) / sum(wall), 3),
                         wall_seconds=round(sum(wall), 3), queue_seconds=round(sum(queue), 3),
                         exec_properties=properties))
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--executions', type=Path, required=True)
    parser.add_argument('--samples', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(summarize_test_sizing(json.loads(args.executions.read_text()),
                               json.loads(args.samples.read_text())), indent=2))


if __name__ == '__main__':
    main()

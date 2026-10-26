"""#2062: a source-level read baseline is not a deployed syscall attribution.

The checkout-only reproducer's small callable seam lets these tests audit its
accounting independently. No live broker, vendor utility or tracing privilege
is needed. Use pytest -s to retain the synthetic report as JSON.
"""
from collections import Counter
import importlib.util

from prismabuild.digest_primitives import raw_sha256, sorted_json
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
REPRODUCER = ROOT / "tools/maintenance/broker_read_baseline.py"
SOURCES = ("tools/fleet/resource_broker.py", "src/prismabuild/gpu_memory.py",
           "src/prismabuild/gpu_capacity.py")


def _brb_load_reproducer():
    assert REPRODUCER.is_file(), (
        "#2062 needs a checkout-only deterministic broker read-count reproducer; "
        "do not optimize the broker to satisfy this measurement issue")
    spec = importlib.util.spec_from_file_location("broker_read_baseline", REPRODUCER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _brb_totals(report):
    """Flatten phase/case rows without trusting their aggregate counters."""
    totals = Counter()
    for case in report["cases"]:
        for row in case["reads"]:
            for field in ("attempts", "reads", "bytes"):
                totals[row["path"], field] += row[field]
    return totals


def test_broker_read_baseline_is_measured_and_phase_separated(tmp_path, monkeypatch):
    read_baseline = _brb_load_reproducer()
    from prismabuild import gpu_memory

    observed = Counter()
    collector_calls = []
    root = tmp_path / "synthetic"
    original_collect = gpu_memory.collect

    def collect(*args, **kwargs):
        collector_calls.append((args, kwargs))
        return original_collect(*args, **kwargs)

    def observe_read(original):
        def wrapped(path, *args, **kwargs):
            relative = None
            try:
                relative = path.relative_to(root).as_posix()
            except ValueError:
                pass
            if relative is not None:
                observed[relative, "attempts"] += 1
            result = original(path, *args, **kwargs)
            if relative is not None:
                observed[relative, "reads"] += 1
                observed[relative, "bytes"] += len(result.encode("utf-8") if isinstance(result, str) else result)
            return result
        return wrapped

    def no_real_subprocess(*args, **kwargs):
        pytest.fail("synthetic baseline must inject vendor results, not launch a host command")

    monkeypatch.setattr(gpu_memory, "collect", collect)
    monkeypatch.setattr(Path, "read_text", observe_read(Path.read_text))
    monkeypatch.setattr(Path, "read_bytes", observe_read(Path.read_bytes))
    monkeypatch.setattr(subprocess, "run", no_real_subprocess)
    monkeypatch.setattr(subprocess, "Popen", no_real_subprocess)
    report = read_baseline.run_synthetic(root, populations=(0, 8, 64), cycles=2)
    assert report["measurement_kind"] == "synthetic_python_file_reads"
    for source in SOURCES:
        assert report["sources"][source] == raw_sha256((ROOT / source).read_bytes())
    assert collector_calls, "exercise the production collector, not a formula or copied loop"
    assert observed, "actual fixture reads must be observed"
    assert _brb_totals(report) == observed, "report every fixture read, including setup, with returned bytes"
    cases = report["cases"]
    for phase in ("collector", "monitor", "request"):
        assert any(case["phase"] == phase for case in cases)
    for scenario in ("empty", "no_gpu", "gpu_error", "gpu_present"):
        matching = [case for case in cases if case["phase"] == "collector" and case["scenario"] == scenario]
        assert {case["unrelated_pids"] for case in matching} == {0, 8, 64}
    assert any(case["active_scopes"] == 0 for case in cases)
    assert any(case["active_scopes"] > 0 for case in cases)
    for case in cases:
        assert case["iterations"] > 0
        assert case["active_scopes"] >= 0 and case["unrelated_pids"] >= 0
        assert isinstance(case["subprocesses"], list)
        for child in case["subprocesses"]:
            assert child["argv"] and child["calls"] > 0
            assert child["outcome"] and child["stdout_bytes"] >= 0
            assert child["measurement_kind"] == "stubbed_not_traced"
        for row in case["reads"]:
            assert not Path(row["path"]).is_absolute() and ".." not in Path(row["path"]).parts
            assert row["file_class"] and row["file_class"] != "other"
            assert 0 <= row["reads"] <= row["attempts"] and row["bytes"] >= 0
    assert any(case["subprocesses"] for case in cases), "vendor calls need separate accounting"
    # The request path must expose the installed-file health-check candidate.
    assert any(row["file_class"] == "installed_privileged"
               for case in cases if case["phase"] == "request" for row in case["reads"])
    # The monitor path must include real OOM-event counter collection.
    assert any(row["path"].endswith("memory.events.local")
               for case in cases if case["phase"] == "monitor" for row in case["reads"])
    print(sorted_json(report))


def test_broker_read_baseline_counts_are_repeatable(tmp_path):
    read_baseline = _brb_load_reproducer()

    def projection(report):
        return [(case["phase"], case["scenario"], case["unrelated_pids"],
                 case["active_scopes"], case["iterations"], case["reads"],
                 [(child["calls"], child["stdout_bytes"], child["outcome"])
                  for child in case["subprocesses"]]) for case in report["cases"]]

    first = read_baseline.run_synthetic(tmp_path / "first", populations=(0, 8, 64), cycles=2)
    again = read_baseline.run_synthetic(tmp_path / "again", populations=(0, 8, 64), cycles=2)
    assert projection(first) == projection(again), "no host-load, PID-order or timing-dependent counters"


def test_w2_nvidia_read_counts_remove_w1_unrelated_pid_slope(tmp_path):
    """Reuse W1's actual collector/monitor observers, not a second cost model."""
    report = _brb_load_reproducer().run_synthetic(tmp_path / 'w2', populations=(0, 8, 64), cycles=2)
    projection = []
    for case in report['cases']:
        if case['phase'] not in ('collector', 'monitor'):
            continue
        projection.append(dict(phase=case['phase'], scenario=case['scenario'],
                               unrelated_pids=case['unrelated_pids'],
                               reads=sum(row['reads'] for row in case['reads']),
                               bytes=sum(row['bytes'] for row in case['reads']),
                               identity_attempts=sum(row['attempts'] for row in case['reads']
                                                     if row['file_class'] == 'proc_identity'),
                               membership_attempts=sum(row['attempts'] for row in case['reads']
                                                       if row['file_class'] == 'proc_membership'),
                               vendor_calls=sum(child['calls'] for child in case['subprocesses'])))
    print(sorted_json(dict(measurement_kind=report['measurement_kind'], sources=report['sources'],
                           w1_unrelated_pid_slope_per_two_calls=dict(reads=4, bytes=142),
                           observations=projection)))
    for row in projection:
        expected = 4 if row['scenario'] == 'gpu_present' else 0
        assert row['identity_attempts'] == row['membership_attempts'] == expected, (
            'only GPU candidates need before/after identity reads; W1 host census must disappear', row)
    for phase in ('collector', 'monitor'):
        for scenario in ('empty', 'no_gpu', 'gpu_error', 'gpu_present'):
            matching = [row for row in projection if row['phase'] == phase and row['scenario'] == scenario]
            assert len(matching) == 3
            assert len({(row['reads'], row['bytes']) for row in matching}) == 1, (
                'unrelated host PID population must not add collector or monitor file reads', matching)

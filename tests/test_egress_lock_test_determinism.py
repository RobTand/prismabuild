"""D-19 acceptance for #1766, independent of the test being repaired.

These source checks deliberately do not execute the old timing workload: a
loaded host is not needed to demonstrate that it samples a real stopwatch and
bounds process/event waits. They supplement, not replace, the target's real
lock-scope, publication, deletion, tier-token and receipt assertions.

Use an explicit synthetic clock object for deterministic duration accounting;
raw calls to the system time module do not establish correctness. Unbounded
Event waits and joins are allowed: ordering must be established by events, not
by whether a timeout expires. The action runner owns external hang containment.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest


_TARGET = Path(__file__).with_name(
    "test_an_egress_holds_the_stage_lock_only_for_its_act.py")


@pytest.fixture
def egress_lock_test_source():
    source = _TARGET.read_text()
    return source, ast.parse(source, filename=str(_TARGET))


def test_egress_lock_test_does_not_sample_a_system_stopwatch(egress_lock_test_source):
    source, tree = egress_lock_test_source
    time_modules = set()
    imported_clocks = set()
    clocks = {"monotonic", "monotonic_ns", "perf_counter", "perf_counter_ns",
              "time", "time_ns", "process_time", "process_time_ns"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            time_modules.update(alias.asname or alias.name for alias in node.names
                                if alias.name == "time")
        elif isinstance(node, ast.ImportFrom) and node.module == "time":
            imported_clocks.update(alias.asname or alias.name for alias in node.names
                                   if alias.name in clocks)
    calls = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        direct = isinstance(function, ast.Name) and function.id in imported_clocks
        qualified = (isinstance(function, ast.Attribute)
                     and isinstance(function.value, ast.Name)
                     and function.value.id in time_modules and function.attr in clocks)
        if direct or qualified:
            calls.append((node.lineno, ast.get_source_segment(source, node)))
    assert not calls, (
        "Replace real stopwatch observations/latency bounds with phase ordering, "
        "operation counts or an explicit synthetic clock, not a larger bound: "
        f"{sorted(calls)}")


def test_egress_lock_test_has_no_sleep_or_bounded_event_process_wait(
        egress_lock_test_source):
    source, tree = egress_lock_test_source
    violations = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        name = (function.attr if isinstance(function, ast.Attribute)
                else function.id if isinstance(function, ast.Name) else "")
        if name == "sleep":
            # Even sleep(0) is unnecessary for ordering; permit that harmless
            # spelling, but not a positive, computed or otherwise unknown delay.
            zero = (len(node.args) == 1 and isinstance(node.args[0], ast.Constant)
                    and node.args[0].value == 0 and not node.keywords)
            if not zero:
                violations.append((node.lineno, ast.get_source_segment(source, node)))
        elif name in {"wait", "join"}:
            # In this file these are Event.wait(timeout) and Process.join(timeout).
            # None is an unbounded wait; any other supplied bound is disallowed.
            bounds = list(node.args) + [keyword.value for keyword in node.keywords
                                       if keyword.arg in {"timeout", None}]
            if any(not (isinstance(bound, ast.Constant) and bound.value is None)
                   for bound in bounds):
                violations.append((node.lineno, ast.get_source_segment(source, node)))
    assert not violations, (
        "Use explicit synchronization/fakes with no sleep or timeout decision: "
        f"{sorted(violations)}")

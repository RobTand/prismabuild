"""Report one progress unit per verified durable child (#1666).

A coordinator that awaits a decomposed batch runs beside this reporter.
The reporter polls the native CAS for each awaited child's receipt, verifies
the receipt, the result blob digest and manifest membership through
``decomposition.child_result_for_key``, and commits one unit per
distinct child whose result verifies. A child with several tasks still counts one unit.

Children already durable when the reporter starts are a verified baseline
and count zero. A non-verifying child, queue waits, admission, logs and
heartbeats count zero. The reporter commits through
``prismabuild.progress.commit`` only, and never touches the queue: it counts
durable results, not queue rows.
"""
from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


class DurableChildReporter:
    """Count distinct newly durable children of one awaited batch."""

    def __init__(
        self,
        *,
        cas,
        parent_key: str,
        plan_key: str,
        child_keys: Sequence[str],
        controller_state: str | Path,
        child_requests: Mapping[str, Mapping[str, Any]] | None = None,
        phase: str | None = None,
        unit: str | None = None,
    ) -> None:
        self.cas = cas
        self.parent_key = str(parent_key)
        self.plan_key = str(plan_key)
        self.child_keys = [str(key) for key in child_keys]
        self.controller_state = str(controller_state)
        self.child_requests = (
            {str(key): dict(request)
             for key, request in dict(child_requests or {}).items()})
        self.phase = phase
        self.unit = "child" if unit is None else str(unit)
        self.baseline: set[str] = set()
        self.counted: set[str] = set()
        self.verified: dict[str, dict[str, Any]] = {}

    def establish_baseline(self) -> set[str]:
        """Mark children already durable at start; they count zero."""

        found: set[str] = set()
        for child_key in self.child_keys:
            from . import decomposition

            manifest = decomposition.child_result_for_key(
                self.cas, child_key, parent_key=self.parent_key,
                plan_key=self.plan_key, supplied_requests=self.child_requests)
            if manifest is not None:
                found.add(child_key)
                self.verified[child_key] = manifest
        self.baseline |= found
        return set(found)

    def newly_durable(self) -> list[str]:
        """Distinct children durable since the baseline, in key order."""

        from .pool import PoolQueue

        custody, _ = PoolQueue._awaited_controller_custody(
            self.controller_state, parent_key=self.parent_key, plan_key=self.plan_key)
        if custody is None:
            return []
        fresh: list[str] = []
        for child_key in sorted(set(self.child_keys)):
            if child_key in self.baseline or child_key in self.counted:
                continue
            if child_key not in custody or not custody[child_key]["confirmed"]:
                continue
            from . import decomposition

            manifest = decomposition.child_result_for_key(
                self.cas, child_key, parent_key=self.parent_key,
                plan_key=self.plan_key, supplied_requests=self.child_requests)
            if manifest is None:
                continue
            self.verified[child_key] = manifest
            self.counted.add(child_key)
            fresh.append(child_key)
        return fresh

    @property
    def units(self) -> int:
        """Distinct newly durable children committed so far."""

        return len(self.counted)

    def commit(self, *, commit=None) -> int:
        """Commit the current count; return the units committed."""

        if commit is None:
            from . import progress as progress_mod

            commit = progress_mod.commit
        commit(float(self.units), self.phase, unit=self.unit)
        return self.units


def run_reporter(
    *,
    cas,
    parent_key: str,
    plan_key: str,
    child_keys: Sequence[str],
    controller_state: str | Path,
    child_requests: Mapping[str, Mapping[str, Any]] | None = None,
    phase: str | None = None,
    unit: str | None = None,
    poll_s: float = 5.0,
    stop_after_s: float | None = None,
) -> DurableChildReporter:
    """Run beside a coordinator: one unit per newly durable child (#1666)."""

    reporter = DurableChildReporter(
        cas=cas, parent_key=parent_key, plan_key=plan_key,
        child_keys=child_keys, child_requests=child_requests,
        controller_state=controller_state,
        phase=phase, unit=unit)
    reporter.establish_baseline()
    reporter.commit()
    started = time.monotonic()
    while True:
        fresh = reporter.newly_durable()
        if fresh:
            reporter.commit()
        if stop_after_s is not None and time.monotonic() - started >= stop_after_s:
            return reporter
        time.sleep(max(0.1, float(poll_s)))



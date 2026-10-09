"""Report one progress unit per verified durable child (#1666).

A coordinator that awaits a decomposed batch runs beside this reporter.
The reporter polls the native CAS for each awaited child's receipt, verifies
the receipt, the result blob digest and manifest membership through
``pool.verify_durable_child_result``, and commits one unit per distinct child
whose result verifies. A child with several tasks still counts one unit.

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
        child_requests: Mapping[str, Mapping[str, Any]] | None = None,
        phase: str | None = None,
        unit: str | None = None,
    ) -> None:
        self.cas = cas
        self.parent_key = str(parent_key)
        self.plan_key = str(plan_key)
        self.child_keys = [str(key) for key in child_keys]
        self.child_requests = (
            {str(key): dict(request)
             for key, request in dict(child_requests or {}).items()})
        self.phase = phase
        self.unit = "child" if unit is None else str(unit)
        self.baseline: set[str] = set()
        self.counted: set[str] = set()
        self.verified: dict[str, dict[str, Any]] = {}

    def _request_of(self, child_key: str) -> Mapping[str, Any] | None:
        if child_key in self.child_requests:
            return self.child_requests[child_key]
        try:
            return self.cas.read_action_request(child_key)
        except (OSError, ValueError):
            return None

    def _verifies(self, child_key: str) -> dict[str, Any] | None:
        from . import pool as pool_mod

        request = self._request_of(child_key)
        if request is None:
            return None
        params = request.get("params")
        batch = (params.get("logical_batch")  # type: ignore[union-attr]
                 if isinstance(params, Mapping) else None)
        if (not isinstance(batch, Mapping)
                or batch.get("parent_key") != self.parent_key
                or batch.get("plan_key") != self.plan_key):
            return None
        try:
            receipt = self.cas.lookup(request)
        except Exception:  # noqa: BLE001 -- an unreadable CAS reads as absent
            return None
        if receipt is None:
            return None
        try:
            manifest = pool_mod.verify_durable_child_result(
                self.cas, request, receipt, child_key=child_key)
        except Exception:  # noqa: BLE001 -- tamper reads as not durable
            return None
        if manifest is None:
            return None
        return dict(manifest)

    def establish_baseline(self) -> set[str]:
        """Mark children already durable at start; they count zero."""

        found: set[str] = set()
        for child_key in self.child_keys:
            manifest = self._verifies(child_key)
            if manifest is not None:
                found.add(child_key)
                self.verified[child_key] = manifest
        self.baseline |= found
        return set(found)

    def newly_durable(self) -> list[str]:
        """Distinct children durable since the baseline, in key order."""

        fresh: list[str] = []
        for child_key in sorted(set(self.child_keys)):
            if child_key in self.baseline or child_key in self.counted:
                continue
            manifest = self._verifies(child_key)
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
        if stop_after_s is None and not fresh:
            time.sleep(max(0.1, float(poll_s)))
            continue
        if stop_after_s is None:
            time.sleep(max(0.1, float(poll_s)))
            continue
        time.sleep(0.0)


def reporter_state_path(progress_path: str | Path) -> str:
    """Where a coordinator's reporter keeps its baseline, beside progress."""

    return str(progress_path) + ".durable-children"

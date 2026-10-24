"""Subprocess reporter + real worker acceptance for the #2100 lifecycle harness.

This is a helper, not a collected test or a worker-loop emulator. The caller
must first admit/claim its consumer with PoolQueue.claim. Keep that claim alive
until all samples finish; this helper neither claims nor finishes queue work.

    reporter = ClaimedProgressReporter(queue, consumer_key, owner, scratch_dir)
    p1 = reporter.commit("p1", 1)   # child calls production progress.commit
    # caller performs lifecycle steps governed by accepted p1 ...
    p2 = reporter.commit("p2", 2)
    refusals = reporter.check_rejections()

Each child completes before ProgressWatch.sample runs: no sleep/poll race and
no overwritten p1 before worker acceptance. Accepted AND rejected observations
are published through PoolQueue.write_lease under the existing claim identity.
The worker ProgressWatch itself, not a fixture or hand-built accepted phase,
is the authority. This does not exercise the full worker launch/monitor loop.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
from typing import Any

from prismabuild import pool, progress, record_store


# Loading the production leaf by path also works when the child interpreter
# cannot import prismabuild. All valid records go through its public commit.
_CHILD = r"""
import json, os, pathlib, runpy, sys
helper = runpy.run_path(os.environ['PRISMABUILD_ACTION_PROGRESS_HELPER'])
request = json.loads(sys.argv[1])
if request['operation'] == 'commit':
    if not helper['commit'](request['units'], request['phase'], unit='lifecycle-units'):
        raise RuntimeError('progress.commit did not write a record')
else:
    # Deliberate wire corruption: commit correctly refuses invalid units locally,
    # so bypass ONLY its writer to prove that the worker also rejects them.
    path = pathlib.Path(os.environ['PRISMABUILD_ACTION_PROGRESS_PATH'])
    temporary = path.with_name(path.name + '.invalid.' + str(os.getpid()))
    try:
        temporary.write_text(json.dumps({
            'schema': helper['PROGRESS_RECORD_SCHEMA_V1'],
            'token': os.environ['PRISMABUILD_ACTION_PROGRESS_TOKEN'],
            'phase': request['phase'], 'units_completed': True,
        }), encoding='utf-8')
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
print(json.dumps({'pid': os.getpid(), 'operation': request['operation']}))
"""


class ClaimedProgressReporter:
    """Drive one launch token and one real ProgressWatch for a claimed consumer.

    ``directory`` must be fresh per attempt and must not be a real worker's
    progress channel. ``policy`` defaults to linear p1,p2 with 60s allowances;
    pass the launch's resolved ProgressPolicy if it has one. No environment is
    changed in the parent. ``timeout_s`` bounds each child, not action liveness.
    """

    def __init__(
        self, queue: pool.PoolQueue, action_key: str, owner: str,
        directory: Path, *, policy: pool.ProgressPolicy | None = None,
        python: str = sys.executable, timeout_s: float = 15.0,
    ) -> None:
        if not math.isfinite(timeout_s) or timeout_s <= 0:
            raise ValueError('timeout_s must be finite and positive')
        self.queue = queue
        self.action_key = action_key
        self.owner = owner
        self.python = python
        self.timeout_s = timeout_s
        self.claim = self._claimed()
        directory = Path(directory).resolve()
        directory.mkdir(parents=True, exist_ok=True)
        # Never unlink a live channel or reuse a previous attempt's path/token.
        self.path = directory / ('progress-' + secrets.token_hex(16) + '.json')
        self.token = secrets.token_hex(32)
        self.policy = policy or pool.ProgressPolicy(
            (pool.ProgressPhase('p1', 60.0, None),
             pool.ProgressPhase('p2', 60.0, None)), None)
        self.watch = pool.ProgressWatch(
            self.path, self.token, self.policy, started=time.monotonic())
        self.events: list[dict[str, Any]] = []

    def _claimed(self) -> dict[str, Any]:
        claim = record_store.value_or_raise(
            self.queue.records.item(pool.CLAIMED, self.action_key))
        if not isinstance(claim, dict) or claim.get('claimed_by') != self.owner:
            raise AssertionError('progress consumer must already be claimed by owner')
        return dict(claim)

    def _spawn(self, phase: str, units: int | float, *,
               token: str | None = None, invalid: bool = False) -> dict[str, Any]:
        self._claimed()
        env = os.environ.copy()
        env.update({
            progress.ACTION_PROGRESS_PATH_ENV: str(self.path),
            progress.ACTION_PROGRESS_TOKEN_ENV: self.token if token is None else token,
            progress.ACTION_PROGRESS_PHASES_ENV: json.dumps(
                [phase.name for phase in self.policy.phases]),
            progress.ACTION_PROGRESS_ALLOWANCES_ENV: json.dumps({
                phase.name: phase.effective_grace_s for phase in self.policy.phases}),
            progress.ACTION_PROGRESS_HELPER_ENV: str(Path(progress.__file__).resolve()),
        })
        result = subprocess.run(
            [self.python, '-c', _CHILD, json.dumps({
                'operation': 'invalid' if invalid else 'commit',
                'phase': phase, 'units': units})],
            env=env, text=True, capture_output=True, timeout=self.timeout_s,
            check=False,
        )
        if result.returncode != 0:
            raise AssertionError(
                f'progress subprocess exited {result.returncode}: {result.stderr}')
        child = json.loads(result.stdout)
        if child['pid'] == os.getpid():
            raise AssertionError('reporter did not run in a subprocess')
        return child

    def _accepted_state(self) -> tuple[Any, ...]:
        w = self.watch
        return (w.accepted, w.units_high_water, w.phase_index,
                w.phases_entered, frozenset(w.phases_granted),
                w.last_advance_monotonic, w.last_real_advance_monotonic,
                w.last_advance_unix, w.first_advance_monotonic,
                dict(w.last_accepted) if w.last_accepted else None,
                w.stall_deadline())

    def _sample(self, label: str, child: dict[str, Any], *,
                expected: bool, reason: str | None = None) -> dict[str, Any]:
        before = self._accepted_state()
        rejected_before = self.watch.rejected
        accepted = self.watch.sample(now=time.monotonic())
        if accepted is not expected:
            raise AssertionError(f'{label}: acceptance={accepted}, expected={expected}; '
                                 f'reason={self.watch.last_rejection}')
        if not expected:
            if (self._accepted_state() != before
                    or self.watch.rejected != rejected_before + 1
                    or self.watch.last_rejection != reason):
                raise AssertionError(f'{label}: refusal changed accepted state or '
                                     f'wrong reason: {self.watch.last_rejection}')
        observation = self.watch.as_record(now=time.monotonic())
        self.queue.write_lease(
            self.action_key, owner=self.owner, claim_snapshot=self.claim,
            progress_observation=observation)
        lease = record_store.read_document(
            self.queue.records.lease_path(self.action_key))
        if (not isinstance(lease, dict)
                or lease.get('progress_observation') != observation):
            raise AssertionError('accepted worker observation was not retained in lease')
        event = {'label': label, 'accepted': accepted,
                 'reason': None if accepted else self.watch.last_rejection,
                 'child': child, 'observation': observation}
        self.events.append(event)
        return event

    def commit(self, phase: str, units: int | float) -> dict[str, Any]:
        """Run production commit in a child; require real worker advancement."""
        return self._sample(phase, self._spawn(phase, units), expected=True)

    def check_rejections(self) -> list[dict[str, Any]]:
        """After p2, prove repeated/regressing/stale/invalid buy no allowance.

        Linear ProgressWatch calls a regressing same-phase report ``replayed``;
        cyclic mode has the more specific ``regressing`` reason. A foreign token
        is launch staleness, not an old reported_unix (which is advisory only).
        """
        if self.watch.last_accepted is None or self.watch.units_high_water <= 0:
            raise ValueError('accept positive progress before checking rejections')
        phase = self.policy.phases[self.watch.phase_index].name
        units = self.watch.units_high_water
        events = []
        for label, value, token, invalid, reason in (
            ('repeated', units, None, False, 'replayed'),
            ('regressing', 0, None, False,
             'regressing' if self.policy.cycle else 'replayed'),
            ('stale', units + 1, self.token + '-previous-launch', False, 'foreign token'),
            ('invalid', units + 1, None, True, 'units_completed is not a finite count'),
        ):
            child = self._spawn(phase, value, token=token, invalid=invalid)
            events.append(self._sample(label, child, expected=False, reason=reason))
        return events

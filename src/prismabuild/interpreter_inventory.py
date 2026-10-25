"""Bounded fresh interpreter observations and host-local request scheduling."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import fcntl
import json
import math
import os
from pathlib import Path
import stat
import time

from . import _bounded_reader, immutable_files
from .digest_primitives import raw_sha256
from .capability_eligibility import InterpreterAnswers, LiveInterpreters, Presence

INVENTORY_LIMIT = 32
REQUEST_TTL_S = 3600.0
PROBE_TIMEOUT_S = 1.0
REFRESH_TIMEOUT_S = 2.0


def _private_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        info = os.fstat(descriptor)
        if info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise OSError('interpreter inventory directory is not private to this uid')
    finally:
        os.close(descriptor)


def _read_inventory(path: Path | None) -> tuple[dict[str, float], str]:
    if path is None:
        return {}, ''
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, 'rb') as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > 32768:
                raise ValueError('invalid interpreter inventory file')
            record = json.loads(handle.read(32769))
        paths = record.get('requested', {})
        cursor = record.get('cursor', '')
        if not isinstance(paths, dict) or not isinstance(cursor, str):
            raise ValueError('invalid interpreter inventory metadata')
        requested = {name: float(stamp) for name, stamp in paths.items()
                     if isinstance(name, str) and isinstance(stamp, (int, float))
                     and math.isfinite(stamp)}
        if len(requested) > INVENTORY_LIMIT:
            raise ValueError('interpreter inventory exceeds its bound')
        return requested, cursor
    except FileNotFoundError:
        return {}, ''


def _probe_interpreter(path: str, inventory_path: Path | None) -> str:
    descriptor = None
    try:
        if inventory_path is not None:
            name = raw_sha256(path.encode())
            descriptor = os.open(inventory_path.parent / f'interpreter-{name}.lock',
                                 os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return 'interpreter_probe_busy'
        return LiveInterpreters().answer(path).value
    finally:
        if descriptor is not None:
            os.close(descriptor)


@dataclass(frozen=True)
class InterpreterObservation:
    answers: InterpreterAnswers
    diagnostics: Mapping[str, str]
    inventory_path: Path | None
    requested: Mapping[str, float]
    retained: Mapping[str, float]
    cursor_before: str
    cursor_after: str
    observed_unix: float

    def detail(self) -> dict[str, object]:
        return {'interpreter_probes': dict(self.diagnostics)} if self.diagnostics else {}

    def commit(self) -> None:
        """Merge captured scheduling progress while the publisher holds its lock."""
        if self.inventory_path is None:
            return
        try:
            current, cursor = _read_inventory(self.inventory_path)
            for name, stamp in (*self.retained.items(), *self.requested.items()):
                current[name] = max(current.get(name, stamp), stamp)
            current = {name: stamp for name, stamp in current.items()
                       if self.observed_unix - stamp < REQUEST_TTL_S}
            # The observed window wins ties so an empty sibling refresh retains it.
            selected = sorted(current, key=lambda name: (
                current[name], name in self.retained, name), reverse=True)[:INVENTORY_LIMIT]
            if cursor == self.cursor_before:
                cursor = self.cursor_after
            immutable_files.write_json_atomic(self.inventory_path, {
                'requested': {name: current[name] for name in selected}, 'cursor': cursor},
                make_parent=False)
        except (OSError, ValueError, TypeError):
            # Scheduling metadata is advisory; failed retention cannot suppress an offer.
            return


def prepare_interpreters(
    items: Sequence[object], *, inventory_path: Path | None = None,
    abandoned: list | None = None, now: float | None = None,
) -> InterpreterObservation:
    """Capture current answers outside publication locks and publication deadlines."""
    now = time.time() if now is None else now
    abandoned = [] if abandoned is None else abandoned
    requested = {item['interpreter']: now for item in items
                 if isinstance(item, dict) and isinstance(item.get('interpreter'), str)}
    diagnostics: dict[str, str] = {}
    try:
        if inventory_path is not None:
            _private_directory(inventory_path.parent)
        retained, cursor = _read_inventory(inventory_path)
    except (OSError, ValueError, TypeError):
        retained, cursor = {}, ''
        diagnostics['inventory'] = 'interpreter_inventory_unavailable'
    candidates = {name: stamp for name, stamp in retained.items()
                  if now - stamp < REQUEST_TTL_S}
    candidates.update(requested)
    names = sorted(candidates)
    rotated = [name for name in names if name > cursor] + [name for name in names if name <= cursor]
    window = rotated[:INVENTORY_LIMIT]
    present: set[str] = set()
    absent: set[str] = set()
    deadline = _bounded_reader.Deadline(REFRESH_TIMEOUT_S)
    last = cursor
    for name in window:
        if (deadline.remaining() or 0) <= 0:
            break
        last = name
        result = _bounded_reader.bounded(
            'interpreter-probe', lambda name=name: _probe_interpreter(name, inventory_path),
            deadline=deadline, abandoned=abandoned, cap_s=PROBE_TIMEOUT_S,
            announce_retained=False)
        answer = result.get('value') if result.get('status') == 'ok' else None
        if answer == Presence.PRESENT.value:
            present.add(name)
        elif answer == Presence.ABSENT.value:
            absent.add(name)
        else:
            diagnostics[name] = ('interpreter_probe_timeout'
                                 if result.get('status') == 'timed_out'
                                 else str(answer or 'interpreter_probe_error'))
    return InterpreterObservation(
        InterpreterAnswers(frozenset(present), frozenset(absent)), diagnostics,
        inventory_path, requested, {name: candidates[name] for name in window},
        cursor, last, now)

"""Fresh interpreter observations survive sibling and drain publications."""
from __future__ import annotations

from threading import Event

from prismabuild import capability_eligibility as eligibility
from test_a_draining_worker_stays_visible import DRAIN_GATE, _load_loop


def _poll(tmp_path, monkeypatch, rows, *, draining=False):
    loop = _load_loop(tmp_path, monkeypatch, DRAIN_GATE if draining else {'draining': False})
    monkeypatch.setattr(loop, 'discover_ready_snapshot', lambda *args, **kwargs:
        loop.DiscoveryResult('ready', rows, None, ''))
    admitted = []
    monkeypatch.setattr(loop.pool.PoolQueue, 'serve_once', lambda *args, **kwargs:
        admitted.append(True))
    assert loop._run_loop(Event()) == 0
    offers = loop.pool.PoolQueue(tmp_path / 'pb-queue')._offer_records()
    assert len(offers) == 1
    assert bool(admitted) is not draining
    return offers[0]


def _executable(tmp_path, name='python'):
    path = tmp_path / name
    path.write_text('#!/bin/sh\n')
    path.chmod(0o700)
    return str(path)


def test_empty_sibling_poll_observes_retained_interpreter_names(tmp_path, monkeypatch):
    path = _executable(tmp_path)
    first = _poll(tmp_path, monkeypatch, [{'interpreter': path}])
    assert eligibility.InterpreterAnswers.from_offer(first).answer(path) is eligibility.Presence.PRESENT
    empty = _poll(tmp_path, monkeypatch, [])
    assert eligibility.InterpreterAnswers.from_offer(empty).answer(path) is eligibility.Presence.PRESENT


def test_drain_poll_freshly_observes_retained_interpreter_names(tmp_path, monkeypatch):
    path = _executable(tmp_path)
    _poll(tmp_path, monkeypatch, [{'interpreter': path}])
    drain = _poll(tmp_path, monkeypatch, [], draining=True)
    assert eligibility.InterpreterAnswers.from_offer(drain).answer(path) is eligibility.Presence.PRESENT
    from pathlib import Path
    Path(path).unlink()
    deleted = _poll(tmp_path, monkeypatch, [], draining=True)
    assert eligibility.InterpreterAnswers.from_offer(deleted).answer(path) is eligibility.Presence.ABSENT
    assert all(value == 0 for value in deleted['observed_capacity'].values())

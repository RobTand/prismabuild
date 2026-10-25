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


def test_worker_timeouts_stay_unknown_and_do_not_stop_publication_or_admission(tmp_path, monkeypatch):
    import os
    from prismabuild import interpreter_inventory as inventory
    slow = _executable(tmp_path, 'z-slow-python')
    fast = _executable(tmp_path, 'a-fast-python')
    fifo = tmp_path / 'blocked-probe'
    os.mkfifo(fifo)
    original = eligibility.LiveInterpreters.answer

    def answer(self, path):
        if path == slow:
            descriptor = os.open(fifo, os.O_RDONLY)
            os.close(descriptor)
        return original(self, path)

    monkeypatch.setattr(eligibility.LiveInterpreters, 'answer', answer)
    monkeypatch.setattr(inventory, 'PROBE_TIMEOUT_S', .1)
    rows = [{'interpreter': slow}, {'interpreter': fast}]
    for draining, requested in [(False, rows), (False, []), (False, []), (True, [])]:
        offer = _poll(tmp_path, monkeypatch, requested, draining=draining)
        observed = eligibility.InterpreterAnswers.from_offer(offer)
        assert observed.answer(slow) is eligibility.Presence.UNKNOWN
        assert observed.answer(fast) is eligibility.Presence.PRESENT
        assert offer['observed_detail']['interpreter_probes'][slow] == 'interpreter_probe_timeout'
    monkeypatch.setattr(eligibility.LiveInterpreters, 'answer', original)
    recovered = _poll(tmp_path, monkeypatch, [])
    assert eligibility.InterpreterAnswers.from_offer(recovered).answer(slow) is eligibility.Presence.PRESENT


def test_metadata_retains_names_and_request_times_without_answers(tmp_path):
    import json
    from prismabuild import interpreter_inventory as inventory
    from pathlib import Path
    path = _executable(tmp_path)
    saved = tmp_path / 'inventory.json'
    observation = inventory.prepare_interpreters([{'interpreter': path}], inventory_path=saved, now=100.)
    observation.commit()
    assert json.loads(saved.read_text()) == {'requested': {path: 100.}, 'cursor': path}
    Path(path).chmod(0o600)
    empty = inventory.prepare_interpreters([], inventory_path=saved, now=200.)
    assert empty.answers.answer(path) is eligibility.Presence.ABSENT
    empty.commit()
    expired = inventory.prepare_interpreters([], inventory_path=saved, now=3700.)
    assert expired.answers.answer(path) is eligibility.Presence.UNKNOWN
    expired.commit()
    assert json.loads(saved.read_text())['requested'] == {}


import pytest


@pytest.mark.parametrize('population', [33, 65, 97])
def test_stable_overflow_rotates_fairly_across_empty_and_smaller_siblings(tmp_path, monkeypatch, population):
    from prismabuild import interpreter_inventory as inventory
    paths = [str(tmp_path / f'python-{index:03d}') for index in range(population)]
    saved = tmp_path / 'inventory.json'
    seen = set()
    for _ in range((population + 31) // 32):
        observed = inventory.prepare_interpreters([{'interpreter': path} for path in paths],
                                                  inventory_path=saved)
        observed.commit()
        seen.update(observed.answers.absent)
        assert len(observed.answers.absent) <= 32
        for requested in ([], [{'interpreter': paths[0]}]):
            sibling = inventory.prepare_interpreters(requested, inventory_path=saved)
            sibling.commit()
    assert seen == set(paths)


def test_sibling_commit_merges_new_request_names_instead_of_erasing_them(tmp_path):
    from prismabuild import interpreter_inventory as inventory
    saved = tmp_path / 'inventory.json'
    left = _executable(tmp_path, 'left')
    right = _executable(tmp_path, 'right')
    first = inventory.prepare_interpreters([{'interpreter': left}], inventory_path=saved)
    second = inventory.prepare_interpreters([{'interpreter': right}], inventory_path=saved)
    second.commit()
    first.commit()
    refreshed = inventory.prepare_interpreters([], inventory_path=saved)
    assert refreshed.answers.present == frozenset([left, right])


def test_inventory_read_and_commit_failures_do_not_suppress_current_answers(tmp_path, monkeypatch):
    from prismabuild import interpreter_inventory as inventory
    saved = tmp_path / 'inventory.json'
    saved.write_text('broken metadata')
    path = _executable(tmp_path)
    observation = inventory.prepare_interpreters([{'interpreter': path}], inventory_path=saved)
    assert observation.answers.answer(path) is eligibility.Presence.PRESENT
    observation.commit()
    assert observation.diagnostics['inventory'] == 'interpreter_inventory_unavailable'


def test_refresh_budget_advances_past_unresolved_names(tmp_path, monkeypatch):
    from prismabuild import interpreter_inventory as inventory
    remaining = [2.]

    class Deadline:
        def __init__(self, seconds):
            remaining[0] = seconds

        def remaining(self):
            return remaining[0]

    seen = []

    def timeout(section, read, *, cap_s, **kwargs):
        seen.append(read.__defaults__[0])
        assert cap_s == 1.
        remaining[0] -= cap_s
        return {'status': 'timed_out'}

    monkeypatch.setattr(inventory._bounded_reader, 'Deadline', Deadline)
    monkeypatch.setattr(inventory._bounded_reader, 'bounded', timeout)
    saved = tmp_path / 'inventory.json'
    rows = [{'interpreter': str(tmp_path / f'python-{index}')} for index in range(4)]
    first = inventory.prepare_interpreters(rows, inventory_path=saved)
    first.commit()
    assert seen == [row['interpreter'] for row in rows[:2]]
    assert not first.answers.present and not first.answers.absent
    second = inventory.prepare_interpreters(rows, inventory_path=saved)
    second.commit()
    assert seen == [row['interpreter'] for row in rows]


def test_retained_probe_fences_the_same_path_across_siblings(tmp_path, monkeypatch):
    import os
    import signal
    import time
    from prismabuild import interpreter_inventory as inventory
    saved = tmp_path / 'inventory.json'
    path = _executable(tmp_path)
    fifo = tmp_path / 'blocked-reader'
    os.mkfifo(fifo)
    entered = tmp_path / 'entered'
    original = eligibility.LiveInterpreters.answer

    def answer(self, name):
        entered.write_text(entered.read_text() + 'called\n' if entered.exists() else 'called\n')
        descriptor = os.open(fifo, os.O_RDONLY)
        os.close(descriptor)
        return original(self, name)

    def retain(pid, section, started, abandoned):
        abandoned.append({'pid': pid, 'starttime_ticks': inventory._bounded_reader._starttime_ticks(pid),
                          'section': section, 'since_unix': time.time()})

    monkeypatch.setattr(eligibility.LiveInterpreters, 'answer', answer)
    monkeypatch.setattr(inventory._bounded_reader, '_stop_reader', retain)
    monkeypatch.setattr(inventory, 'PROBE_TIMEOUT_S', .1)
    abandoned = []
    try:
        first = inventory.prepare_interpreters([{'interpreter': path}], inventory_path=saved,
                                              abandoned=abandoned)
        assert first.answers.answer(path) is eligibility.Presence.UNKNOWN
        assert len(abandoned) == 1
        sibling = inventory.prepare_interpreters([{'interpreter': path}], inventory_path=saved)
        assert sibling.answers.answer(path) is eligibility.Presence.UNKNOWN
        assert sibling.diagnostics[path] == 'interpreter_probe_busy'
        assert entered.read_text() == 'called\n'
    finally:
        for record in abandoned:
            os.kill(record['pid'], signal.SIGKILL)
            os.waitpid(record['pid'], 0)
    monkeypatch.setattr(eligibility.LiveInterpreters, 'answer', original)
    recovered = inventory.prepare_interpreters([{'interpreter': path}], inventory_path=saved)
    assert recovered.answers.answer(path) is eligibility.Presence.PRESENT

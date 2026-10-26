"""Observe real collection with the named PB owner and historical file set."""
import cProfile
import json
import os
from pathlib import Path
import pstats
import time
import types

import pytest

DEST = Path(os.environ['PRI191_OUTPUT'])
PROFILED = os.environ['PRI191_PROFILED'] == '1'
owner = types.ModuleType('pri191_selection_owner')
owner_path = Path(os.environ['PRI191_OWNER'])
exec(compile(owner_path.read_text(), str(owner_path), 'exec'), owner.__dict__)
counts = {'parents_after_construction': 0}
active = False


class CountedPath(type(Path())):
    @property
    def parents(self):
        if active:
            counts['parents_after_construction'] += 1
        return super().parents


files = json.loads(Path(os.environ['PRI191_FILES']).read_text())
if PROFILED:
    owner.Path = CountedPath
selection = owner.selection_plugin({
    'files': files, 'roots': [[name, 'file'] for name in files]})
profiler = cProfile.Profile() if PROFILED else None
observation = {'profiled': PROFILED, 'counts': counts, 'outcomes': {},
               'collection': None, 'collected': []}


def pytest_configure(config):
    global active
    config.pluginmanager.register(selection, 'pri191-selection')
    active = True


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_collection(session):
    start = time.monotonic()
    if profiler:
        profiler.enable()
    yield
    if profiler:
        profiler.disable()
    observation['collection'] = [start, time.monotonic()]


def pytest_collection_finish(session):
    observation['collected'] = [item.nodeid for item in session.items]


def pytest_runtest_logreport(report):
    if report.when == 'call' or report.failed or report.skipped:
        outcomes = observation['outcomes']
        outcomes[report.outcome] = outcomes.get(report.outcome, 0) + 1


def pytest_sessionfinish(session, exitstatus):
    global active
    active = False
    worker = getattr(session.config, 'workerinput', {}).get('workerid', 'controller')
    observation.update(exitstatus=int(exitstatus), worker=worker,
                       selection=selection.report())
    if profiler:
        profiler.dump_stats(str(DEST / (worker + '.prof')))
        rows = []
        for (filename, line, name), (primitive, calls, own, cumulative, callers) in pstats.Stats(profiler).stats.items():
            if filename == str(owner_path) or 'pathlib' in filename:
                rows.append({'filename': filename, 'line': line, 'name': name,
                             'calls': calls, 'primitive_calls': primitive})
        observation['call_counts'] = rows
    (DEST / (worker + '.json')).write_text(json.dumps(observation, sort_keys=True) + '\n')

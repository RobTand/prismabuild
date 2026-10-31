"""Opt-in pytest sys.monitoring recorder. Python 3.12+, stdlib only.

Enable with ``-p tia_record`` and PB_TIA_ROOT/PB_TIA_OUTPUT/PB_TIA_COMMIT.
Subprocess bootstrapping is installed only into this run's output directory.
"""
from __future__ import annotations

import atexit
import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import sys
import uuid

_STATE = None


class Recorder:
    def __init__(self, root: Path, output: Path, commit: str):
        self.root = root.resolve()
        self.output = output.resolve()
        self.commit = commit
        self.tests: dict[str, set[str]] = {}
        self.files: dict[str, str] = {}
        self.codes: dict[object, str | None] = {}
        self.catalog = {}
        for directory in ('src', 'tools'):
            for path in sorted((self.root / directory).rglob('*.py')):
                self.catalog[str(path)] = (path.relative_to(self.root).as_posix(),
                                          hashlib.sha256(path.read_bytes()).hexdigest())
        self.fixture_calls: list[set[str]] = []
        self.node = os.environ.get('PB_TIA_NODE', '')
        self.tool = None
        self.pid = os.getpid()
        self.token = uuid.uuid4().hex
        self.output.mkdir(parents=True, exist_ok=True)
        for tool in (4, 3, 5, 2, 1, 0):
            try:
                sys.monitoring.use_tool_id(tool, 'prismabuild-tia')
            except ValueError:
                continue
            self.tool = tool
            break
        if self.tool is None:
            raise RuntimeError('TIA cannot acquire a sys.monitoring tool ID')
        sys.monitoring.register_callback(self.tool, sys.monitoring.events.PY_START, self.start)
        sys.monitoring.set_events(self.tool, sys.monitoring.events.PY_START)
        atexit.register(self.close)
        if hasattr(os, 'register_at_fork'):
            os.register_at_fork(after_in_child=self.after_fork)

    def after_fork(self):
        self.tests = {}
        self.codes = {}
        self.files = {}
        self.pid = os.getpid()
        self.token = uuid.uuid4().hex
        sys.monitoring.restart_events()

    def key(self, code):
        if code in self.codes:
            return self.codes[code]
        result = None
        if code.co_name != '<module>':
            filename = code.co_filename
            if not filename.startswith('/'):
                filename = str(self.root / filename)
            entry = self.catalog.get(filename)
            if entry:
                relative, content_hash = entry
                result = relative + '::' + code.co_qualname
                self.files[relative] = content_hash
        self.codes[code] = result
        return result

    def start(self, code, offset):
        if self.node:
            key = self.key(code)
            if key:
                self.tests.setdefault(self.node, set()).add(key)
                for calls in self.fixture_calls:
                    calls.add(key)
        return sys.monitoring.DISABLE

    def boundary(self, node: str):
        self.node = node
        if node:
            self.tests.setdefault(node, set())
            os.environ['PB_TIA_NODE'] = node
        else:
            os.environ.pop('PB_TIA_NODE', None)
        sys.monitoring.restart_events()

    def flush(self):
        record = {'schema': 'prismabuild.tia_record.v1', 'source_commit': self.commit,
                  'python': platform.python_version(), 'files': dict(sorted(self.files.items())),
                  'tests': {node: sorted(values) for node, values in sorted(self.tests.items())}}
        target = self.output / f'{self.pid}-{self.token}.json'
        temporary = target.with_suffix('.tmp')
        temporary.write_text(json.dumps(record, sort_keys=True, separators=(',', ':')) + '\n')
        temporary.replace(target)

    def close(self):
        if self.tool is not None:
            sys.monitoring.set_events(self.tool, 0)
            sys.monitoring.register_callback(self.tool, sys.monitoring.events.PY_START, None)
            sys.monitoring.free_tool_id(self.tool)
            self.tool = None
        self.flush()


def bootstrap():
    global _STATE
    if _STATE is None and os.environ.get('PB_TIA_OUTPUT') and os.environ.get('PB_TIA_ROOT'):
        if not hasattr(sys, 'monitoring'):
            raise RuntimeError('TIA recording requires Python 3.12 or later')
        _STATE = Recorder(Path(os.environ['PB_TIA_ROOT']), Path(os.environ['PB_TIA_OUTPUT']),
                          os.environ['PB_TIA_COMMIT'])
        os.environ['PB_TIA_ROOT'] = str(_STATE.root)
        os.environ['PB_TIA_OUTPUT'] = str(_STATE.output)
    return _STATE


def pytest_configure(config):
    state = bootstrap()
    if state is None:
        raise RuntimeError('TIA needs PB_TIA_ROOT, PB_TIA_OUTPUT and PB_TIA_COMMIT')
    boot = state.output / 'bootstrap'
    boot.mkdir(exist_ok=True)
    # The file has an opt-in environment check, even when PYTHONPATH leaks to another invocation.
    target = boot / 'sitecustomize.py'
    temporary = boot / f'sitecustomize-{os.getpid()}.tmp'
    temporary.write_text('import os\nif os.environ.get("PB_TIA_OUTPUT"):\n import tia_record\n tia_record.bootstrap()\n')
    temporary.replace(target)
    entries = [str(boot), str(Path(__file__).resolve().parent)]
    if os.environ.get('PYTHONPATH'):
        entries.append(os.environ['PYTHONPATH'])
    os.environ['PYTHONPATH'] = os.pathsep.join(entries)


def pytest_runtest_setup(item):
    _STATE.boundary(item.nodeid)


def pytest_runtest_call(item):
    # Cached fixtures execute once but can affect every consumer. Preserve their dependencies.
    for fixture in item._request._fixture_defs.values():
        _STATE.tests[item.nodeid].update(getattr(fixture, '_pb_tia_calls', set()))
    sys.monitoring.restart_events()


def pytest_fixture_setup(fixturedef, request):
    calls: set[str] = set()
    _STATE.fixture_calls.append(calls)
    sys.monitoring.restart_events()
    try:
        return (yield)
    finally:
        _STATE.fixture_calls.pop()
        fixturedef._pb_tia_calls = calls


pytest_fixture_setup.pytest_impl = {'wrapper': True}


def pytest_runtest_teardown(item, nextitem):
    try:
        return (yield)
    finally:
        _STATE.boundary('')


pytest_runtest_teardown.pytest_impl = {'wrapper': True}


def pytest_sessionfinish(session, exitstatus):
    _STATE.flush()
    if os.environ.get('PB_TIA_EMIT') == '1':
        # PB's CAS-captured stdout retains the data without a shared output directory.
        records = [json.loads(path.read_text()) for path in sorted(_STATE.output.glob('*.json'))]
        encoded = gzip.compress(json.dumps(records, sort_keys=True).encode(), mtime=0)
        print('tia-record: ' + base64.b64encode(encoded).decode(), flush=True)

pytest_runtest_setup.pytest_impl = {'tryfirst': True}

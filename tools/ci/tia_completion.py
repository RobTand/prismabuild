"""Write explicit collection and execution evidence for one evaluation arm."""
import json
import os
from pathlib import Path

_COLLECTED = None
_FINISHED = set()
_COLLECTION_ERRORS = set()
_INTERRUPTION = None


def pytest_tia_completion_collection_finish(session):
    global _COLLECTED
    _COLLECTED = sorted(item.nodeid for item in session.items)


def pytest_tia_completion_runtest_logreport(report):
    if report.when == 'teardown':
        _FINISHED.add(report.nodeid)


def pytest_tia_completion_collectreport(report):
    if report.failed:
        _COLLECTION_ERRORS.add(report.nodeid)


def pytest_tia_completion_keyboard_interrupt(excinfo):
    global _INTERRUPTION
    _INTERRUPTION = excinfo.typename


def pytest_tia_completion_sessionfinish(session, exitstatus):
    destination = Path(os.environ['PB_TIA_COMPLETION'])
    record = {'schema': 'prismabuild.tia_completion.v1', 'exitstatus': int(exitstatus),
              'collection_complete': _COLLECTED is not None and not _COLLECTION_ERRORS,
              'collected': _COLLECTED, 'finished': sorted(_FINISHED),
              'collection_errors': sorted(_COLLECTION_ERRORS), 'interruption': _INTERRUPTION}
    temporary = destination.with_suffix('.tmp')
    temporary.write_text(json.dumps(record, sort_keys=True) + '\n')
    temporary.replace(destination)


pytest_tia_completion_collection_finish.pytest_impl = {'specname': 'pytest_collection_finish'}
pytest_tia_completion_runtest_logreport.pytest_impl = {'specname': 'pytest_runtest_logreport'}
pytest_tia_completion_collectreport.pytest_impl = {'specname': 'pytest_collectreport'}
pytest_tia_completion_keyboard_interrupt.pytest_impl = {'specname': 'pytest_keyboard_interrupt'}
pytest_tia_completion_sessionfinish.pytest_impl = {'specname': 'pytest_sessionfinish', 'trylast': True}

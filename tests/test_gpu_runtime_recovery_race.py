"""CDI repairs overlapping the first fault must authorize event-driven recovery."""
import os
import threading
from functools import partial

from prismabuild import gpu_capacity
from test_gpu_container_runtime import ERRORS, MISSING, daemon_row, runtime_authority
from test_resource_broker import create, container_ticket

__all__ = ['runtime_authority']
_REAL_OUTCOME = gpu_capacity.container_launch_outcome
_REAL_WATCH = gpu_capacity.CDIChangeWatch


def test_cdi_repair_during_first_fault_check_requests_one_readiness_probe(
        runtime_authority, monkeypatch):
    _, authority, backend, runtime = runtime_authority
    _, record = create(authority)
    ticket = container_ticket(authority, record, monkeypatch)
    ready = threading.Event()
    checking = threading.Event()
    predicate_reached = threading.Event()
    submitted = threading.Event()
    tokens = []
    original_lock = authority.lock

    class ObservationLock:
        def __enter__(self):
            if threading.current_thread() is runtime.thread and checking.is_set():
                # Correct serialization may block here until observation latches.
                predicate_reached.set()
            return original_lock.__enter__()

        def __exit__(self, *args):
            return original_lock.__exit__(*args)

    authority.lock = runtime.lock = ObservationLock()

    class ObservedWatch(_REAL_WATCH):
        def __init__(self, directory):
            super().__init__(directory)
            self.calls = 0

        def next_change(self, stop_fd):
            self.calls += 1
            if self.calls == 1:
                ready.set()
            else:
                # The old unlocked predicate has already discarded the repair.
                predicate_reached.set()
            return super().next_change(stop_fd)

    monkeypatch.setattr(gpu_capacity, 'CDIChangeWatch', ObservedWatch)

    def submit(token):
        tokens.append(token)
        submitted.set()

    runtime.thread = threading.Thread(target=runtime._watch, args=(submit,), daemon=True)
    runtime.thread.start()
    assert ready.wait(5)

    def old_source_exists(path):
        assert path == MISSING
        checking.set()
        # One inotify event, with no intermediate writes in the watched tree.
        replacement = runtime.cdi_dir.parent / 'nvidia.new'
        replacement.write_text('containerEdits:\n  mounts:\n    - hostPath: /bin/true\n')
        replacement.replace(runtime.cdi_dir / 'nvidia.yaml')
        assert predicate_reached.wait(5), 'watcher did not handle the real repair event'
        return False

    monkeypatch.setattr(gpu_capacity, 'container_launch_outcome',
                        partial(_REAL_OUTCOME, source_exists=old_source_exists))
    row = daemon_row(record, ERRORS[0]['daemon_error'])
    row['Config']['Labels']['prismabuild.shim'] = ticket
    backend.inspect_container = lambda cid: row
    reply = authority.handle(os.getuid(), os.getpid(), {
        'op': 'container_outcome', 'scope_id': record['scope_id'],
        'ticket': ticket, 'container_id': row['Id']})
    assert reply['launch_outcome']['kind'] == 'host_fault'
    assert submitted.wait(5), 'CDI repair was consumed before latching without scheduling recovery'
    os.write(runtime.stop_write, b'x')
    runtime.thread.join()
    assert tokens == [runtime.state['probe_token']]
    assert runtime.state['probe_event']['reason'] == 'cdi_changed'

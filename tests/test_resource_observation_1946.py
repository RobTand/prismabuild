"""Local observation producer acceptance; no bell, live queue, or GPU required.

The slice needs a local consumer seam before slice 7 exists. This test pins a
small INTERNAL (not SDK or broker wire) adapter in resource_observation:

    LocalObservationSource(paths={label: Path}, pidfds={label: fd})
    source.generation -> int
    source.wait(after_generation, timeout_s=...) -> (generation, set_of_labels)
    source.close()

Construction arms sources before returning; generations latch invalidations
across observation/wait registration. A timeout has no labels and does not
advance generation. Pidfds are borrowed, identify an exact process, and generate
one invalidation, not an endlessly readable healthy-renewal event. Labels are
hints to reread authority, NEVER evidence that a whole scope is empty. Paths
include atomic snapshot/adaptive replacements and socket endpoint lifecycles.
The adapter may delegate to existing machinery; these tests prescribe no thread,
selector, or on-disk/wire representation.
"""
from contextlib import ExitStack
import importlib
import importlib.util
import os
import socket
import subprocess
import sys
import threading

import pytest


def local_source(**kwargs):
    name = 'prismabuild.resource_observation'
    assert importlib.util.find_spec(name) is not None, (
        '#1946 has no local resource observation producer/consumer adapter')
    implementation = importlib.import_module(name)
    factory = getattr(implementation, 'LocalObservationSource', None)
    assert callable(factory), '#1946 needs an armed, latched local observation source'
    return factory(**kwargs)


def changed(source, cursor, label):
    generation, labels = source.wait(cursor, timeout_s=1.0)
    assert generation > cursor and label in labels, (
        f'{label} changed without a latched resource invalidation: '
        f'{cursor=}, {generation=}, {labels=}')
    return generation


def drained_cursor(source, cursor):
    # File writes can yield more than one native event. Consume the bounded
    # burst before a second mutation so an old queued event cannot satisfy it.
    for _ in range(10):
        generation, labels = source.wait(cursor, timeout_s=0.05)
        if not labels:
            assert generation == cursor
            return cursor
        cursor = generation
    pytest.fail('local observation source did not become quiet after a bounded event burst')


@pytest.mark.parametrize('filename', [
    'gpu-capacity.json', 'cpu-sample.json', 'gpu-state.json', 'profiles.json',
])
def test_equal_capacity_proof_recovery_is_not_suppressed(tmp_path, filename):
    path = tmp_path / filename
    # Capacity is deliberately identical. Only proof/freshness recovers.
    path.write_text('{"capacity":{"cpu":2,"gpu":1},"complete":false,"sampled_unix":0}')
    source = local_source(paths={'resource': path}, pidfds={})
    try:
        cursor = source.generation
        replacement = tmp_path / 'replacement'
        replacement.write_text('{"capacity":{"cpu":2,"gpu":1},"complete":true,"sampled_unix":100}')
        os.replace(replacement, path)
        cursor = changed(source, cursor, 'resource')
        cursor = drained_cursor(source, cursor)
        # Re-arm the pathname, not just its original inode. Byte-identical
        # replacement still invalidates a proof tied to the former inode.
        replacement.write_bytes(path.read_bytes())
        os.replace(replacement, path)
        changed(source, cursor, 'resource')
    finally:
        source.close()


def test_path_loss_and_recovery_are_both_latched(tmp_path):
    path = tmp_path / 'cpu-sample.json'
    path.write_text('{}')
    source = local_source(paths={'cpu': path}, pidfds={})
    try:
        cursor = source.generation
        path.unlink()
        cursor = changed(source, cursor, 'cpu')
        cursor = drained_cursor(source, cursor)
        # No new ready publication, no capacity increase, no shared mirror.
        path.write_text('{}')
        changed(source, cursor, 'cpu')
    finally:
        source.close()


def test_broker_socket_unlink_and_restart_notify_without_broker_wire_changes(tmp_path):
    path = tmp_path / 'broker.sock'
    first = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    second = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    first.bind(str(path)); first.listen()
    source = None
    try:
        source = local_source(paths={'broker_socket': path}, pidfds={})
        cursor = source.generation
        first.close()
        path.unlink()
        cursor = changed(source, cursor, 'broker_socket')
        cursor = drained_cursor(source, cursor)
        second.bind(str(path)); second.listen()
        changed(source, cursor, 'broker_socket')
    finally:
        if source is not None:
            source.close()
        first.close(); second.close()


def test_pidfd_exit_is_one_identity_bound_hint_not_scope_empty_proof(tmp_path):
    # The child waits on a pipe, not a repeated sleep. No process outside this
    # admitted test action is signalled. Keep a second child alive to model a
    # surviving member of the same attempt; notification must not kill it.
    command = [sys.executable, '-c', 'import sys; sys.stdin.buffer.read(1)']
    def finish_child(child):
        if not child.stdin.closed:
            child.stdin.close()
        child.wait(timeout=5)

    with ExitStack() as cleanup:
        leader = subprocess.Popen(command, stdin=subprocess.PIPE)
        cleanup.callback(finish_child, leader)
        descendant = subprocess.Popen(command, stdin=subprocess.PIPE)
        cleanup.callback(finish_child, descendant)
        descriptor = os.pidfd_open(leader.pid)
        cleanup.callback(os.close, descriptor)
        source = local_source(paths={}, pidfds={'leader': descriptor})
        cleanup.callback(source.close)
        cursor = source.generation
        leader.stdin.close()
        assert leader.wait(timeout=5) == 0
        cursor = changed(source, cursor, 'leader')
        assert descendant.poll() is None, 'leader exit is not scope emptiness'
        generation, labels = source.wait(cursor, timeout_s=0.05)
        assert generation == cursor and not labels, (
            'level-triggered exited pidfd repeatedly invalidates without new evidence')
        source.close()
        # The source borrows rather than takes ownership of caller descriptors.
        os.fstat(descriptor)


def test_change_during_blocked_wait_wakes(tmp_path):
    path = tmp_path / 'gpu-capacity.json'
    path.write_text('{}')
    source = local_source(paths={'gpu': path}, pidfds={})
    entered = threading.Event()
    done = threading.Event()
    results = []

    def wait_for_change():
        entered.set()
        results.append(source.wait(cursor, timeout_s=30))
        done.set()

    cursor = source.generation
    waiter = threading.Thread(target=wait_for_change, daemon=True)
    try:
        waiter.start()
        assert entered.wait(1)
        path.write_text('{"complete":true}')
        assert done.wait(1), 'a local resource change did not interrupt observation wait'
        generation, labels = results[0]
        assert generation > cursor and 'gpu' in labels
        waiter.join(timeout=1)
        assert not waiter.is_alive()
    finally:
        source.close()
        waiter.join(timeout=1)


def test_close_cancels_wait_without_any_resource_event(tmp_path):
    path = tmp_path / 'gpu-capacity.json'
    path.write_text('{}')
    source = local_source(paths={'gpu': path}, pidfds={})
    cursor = drained_cursor(source, source.generation)
    entered = threading.Event()
    done = threading.Event()
    errors = []

    def cancellable_wait():
        entered.set()
        try:
            source.wait(cursor, timeout_s=30)
        except BaseException as error:
            errors.append(error)
        finally:
            done.set()

    waiter = threading.Thread(target=cancellable_wait, daemon=True)
    try:
        waiter.start()
        assert entered.wait(1)
        assert not done.wait(0.1), 'unchanged healthy sources must not continually wake'
        source.close()
        assert done.wait(1), 'close must cancel a local observation wait'
        waiter.join(timeout=1)
        assert not waiter.is_alive()
        assert not errors, errors
        source.close()
    finally:
        source.close()
        waiter.join(timeout=1)

"""Issue 2074: exercise the existing negative control with delayed progress visibility.

This deliberately invokes the editable characterization test rather than copying
its synchronization.  The production reader still validates every snapshot with
the actual launch identity; only consumer-visible readiness is delayed until an
unknown heartbeat has been emitted.  No clock or elapsed-time threshold decides
correctness, and the characterization test retains ownership of child cleanup.
"""
from __future__ import annotations

import threading

import test_c26_progress_characterization as subject


def test_forged_display_waits_for_valid_zero_after_unknown(tmp_path, monkeypatch):
    unknown_seen = threading.Event()
    zero_seen = threading.Event()
    launch_identities = []
    returned_by_thread = threading.local()
    releases = []
    reads_before_ready = []
    running_snapshots = []
    original_loader = subject.mq.load_outcomes
    original_event = subject.mq.Store.event
    original_popen = subject.REAL_POPEN
    original_write = subject.os.write

    class DelayedProgressOwner:
        def __init__(self, owner):
            self.owner = owner

        def __getattr__(self, name):
            return getattr(self.owner, name)

        def read_progress(self, path, **binding):
            snapshot = self.owner.read_progress(path, **binding)
            # Validate against the environment of the real launched driver, not
            # an identity obtained from an untrusted companion or display line.
            assert launch_identities, 'progress read before this run was launched'
            launched = launch_identities[-1]
            assert binding['run_id'] == launched['PRISMABUILD_PBTEST_RUN_ID']
            assert binding['launch_entrypoint'] == launched['PRISMABUILD_PBTEST_ENTRYPOINT']
            assert binding['generation'] == str(subject.mq.generation_root(
                subject.Path(binding['launch_entrypoint'])))
            if not unknown_seen.is_set():
                reads_before_ready.append(snapshot)
                returned_by_thread.snapshot = None
                return None
            returned_by_thread.snapshot = snapshot
            if (snapshot is not None and snapshot['state'] == 'running'
                    and snapshot['completed_shards'] == 0):
                running_snapshots.append(snapshot)
            return snapshot

    def delayed_owner(path):
        return DelayedProgressOwner(original_loader(path))

    def observe_heartbeat(store, batch, message):
        if 'run candidate waiting ' in message:
            if 'unknown shard(s) reported' in message:
                unknown_seen.set()
            elif message.endswith(': 0 shard(s) reported'):
                snapshot = getattr(returned_by_thread, 'snapshot', None)
                if (unknown_seen.is_set() and snapshot is not None
                        and snapshot['state'] == 'running'
                        and snapshot['completed_shards'] == 0):
                    zero_seen.set()
        return original_event(store, batch, message)

    def observe_launch(command, **kwargs):
        environment = kwargs.get('env', {})
        if 'PRISMABUILD_PBTEST_RUN_ID' in environment:
            launch_identities.append(environment.copy())
        return original_popen(command, **kwargs)

    def observe_release(fd, data):
        if data == b'x':
            # Never refuse cleanup, even if readiness was wrong.  Check after
            # the existing test has released the FIFO and reaped its children.
            releases.append(zero_seen.is_set())
        return original_write(fd, data)

    monkeypatch.setattr(subject.mq, 'load_outcomes', delayed_owner)
    monkeypatch.setattr(subject.mq.Store, 'event', observe_heartbeat)
    monkeypatch.setattr(subject, 'REAL_POPEN', observe_launch)
    monkeypatch.setattr(subject.os, 'write', observe_release)

    subject.test_mergeq_heartbeat_does_not_count_a_forged_display_line(tmp_path, monkeypatch)

    assert reads_before_ready, 'the controlled startup interval was not exercised'
    assert unknown_seen.is_set(), 'startup must remain unknown without accepted progress'
    assert running_snapshots, 'no identity-validated running zero-completed record was exposed'
    assert zero_seen.is_set(), 'no subsequent heartbeat consumed that running record'
    assert releases and all(releases), 'payload released before the validated zero heartbeat'
    log = tmp_path / 'out' / 'candidate.log'
    assert 'shard  99 ok       forged display line, no client has ended' in log.read_text(), (
        'forged display negative control lost')

"""Coordinate a holder's real finish with the resignation driver's terminal read."""
from contextlib import contextmanager
import threading

from prismabuild import pool
import fleet_membership


@contextmanager
def finish_after_terminal_probe(monkeypatch, queue, snapshot):
    probed = threading.Event()
    finished = threading.Event()
    cancelled = threading.Event()
    errors = []
    key = snapshot["action_key"]
    read_as_owner = fleet_membership._as_queue_owner

    def conclude():
        try:
            assert probed.wait(10), "resign never checked the held attempt's terminal"
            if cancelled.is_set():
                return
            assert queue.records.item_path(pool.record_store.CLAIMED, key).exists()
            assert not queue.records.item_path(pool.record_store.WITHDRAWN, key).exists()
            queue.finish(key, status="executed", detail={}, claim_snapshot=snapshot)
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    def observe(queue_root, function, *args):
        result = read_as_owner(queue_root, function, *args)
        if function == "_resign_terminal_of" and result is None and not probed.is_set():
            probed.set()
            assert finished.wait(10), "holder did not finish after its terminal probe"
            assert not errors, errors
        return result

    holder = threading.Thread(target=conclude, daemon=True)
    with monkeypatch.context() as patch:
        patch.setattr(fleet_membership, "_as_queue_owner", observe)
        holder.start()
        try:
            yield finished
        finally:
            cancelled.set()
            probed.set()
            holder.join(timeout=10)
            assert not holder.is_alive(), "holder thread did not settle"
    assert not errors, errors

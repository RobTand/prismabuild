"""Fixture-owned diagnostic copies finish before protected snapshots begin."""
from types import SimpleNamespace

import test_checkout_recovery_1465 as contract


def test_recovery_fixture_settles_deferred_diagnostic_copies(tmp_path, monkeypatch):
    pending = []
    completed = []
    snapshots = contract.pool.cpu_admission.adaptive_snapshot

    def deferred_publish(local, shared):
        pending.append(shared)

        def finish(timeout):
            snapshots.copy_snapshot(local, shared)
            pending.remove(shared)
            completed.append(shared)
            return 0

        return SimpleNamespace(wait=finish)

    monkeypatch.setattr(snapshots, "publish", deferred_publish)
    create = contract.make_case.__wrapped__(tmp_path, monkeypatch)
    case = create()
    assert completed and not pending, "fixture returned before its diagnostic copies finished"
    before = case.preserved()
    assert case.prepare()["status"] == "planned"
    assert case.preserved() == before

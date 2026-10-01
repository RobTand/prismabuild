"""A retired, settled scope whose kernel group vanished can close administratively (#1403).

The broker half of #1403: a normal maintenance pass treats an exact retired,
empty, frozen scope as inactive while reclaim may still be pending (see
``test_a_retired_tombstone_gives_its_memory_back_without_being_removed``).
When the kernel group is removed out of band -- the reporter stopped the slice
by hand -- inventory no longer has a row for it, the retired branch (which
requires a kernel group) is skipped, and the record is counted active
forever.  Restarting the broker does not help; no operator path clears it.

The approved direction, tested here before any source change: a stopped +
retired + settled record with a complete empty container settlement, a
complete healthy inventory with no unknown namespace group, and a fresh
``backend.exists`` false may be released as *metadata* -- with an explicit
``maintenance_cleanup`` reason that the scope disappeared externally and
reclaim was not verified -- while nothing signals, reclaims or releases a
kernel group.  Every uncertainty retains the record as active, and an
idempotent repeat writes nothing.  No admin operation is involved.
"""

from __future__ import annotations

import json

import pytest

from test_resource_broker import authority
from test_a_settled_tombstone_is_reaped_and_a_live_scope_is_not import (
    SETTLEMENT,
    _pass,
    _retired,
)


def _state(a, scope):
    return a.state_dir / (scope + ".json")


def _vanish(b, scope):
    """The out-of-band slice stop: the kernel group is simply not there."""

    b.groups.pop(scope, None)
    b.identity.pop(scope, None)
    b.charge.pop(scope, None)
    b.stopped[:] = [name for name in b.stopped if name != scope]


def test_a_settled_retired_scope_whose_group_vanished_is_released(
        authority, monkeypatch):
    """Proof, absence, then metadata only: the wedge the broker review describes."""

    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    before_fields = json.loads(_state(a, scope).read_text())
    retired_unix = before_fields["retired_unix"]
    _vanish(b, scope)
    b.ops.clear()

    exists_calls = []
    real_exists = b.exists

    def counting_exists(name):
        exists_calls.append(name)
        return real_exists(name)

    monkeypatch.setattr(b, "exists", counting_exists)

    status = _pass(a)
    assert status["health"] is True, status["errors"]
    assert status["errors"] == []
    assert status["active_scopes"] == 0
    assert scope not in status["active_scope_ids"]
    assert scope in exists_calls, "absence must be re-checked, not assumed"
    assert b.ops == [], "a missing group is never stopped, reclaimed or released"

    stored = json.loads(_state(a, scope).read_text())
    assert stored["released_unix"] > 0
    assert stored["retired_unix"] == retired_unix
    assert stored["stopped_unix"] == before_fields["stopped_unix"]
    assert stored["cgroup_identity"] == before_fields["cgroup_identity"]
    assert stored["container_settlement"] == SETTLEMENT
    reason = stored["maintenance_cleanup"]
    assert isinstance(reason, str) and reason
    assert reason != "settled container transaction", (
        "metadata retirement must not claim the reclaim the normal path proves")

    first = _state(a, scope).read_bytes()
    second = _pass(a)
    assert second["health"] is True and second["active_scopes"] == 0
    assert _state(a, scope).read_bytes() == first, (
        "an idempotent repeat writes nothing")
    assert b.ops == []


def test_a_vanished_scope_keeps_failed_reclaim_evidence_unclaimed(
        authority, monkeypatch):
    """Old reclaim evidence survives; the release does not invent a reclaim."""

    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    path = _state(a, scope)
    evidence = {
        "reclaimed_unix": 1_789_000_111.0,
        "reclaim_before_bytes": 4096,
        "reclaim_after_bytes": None,
        "reclaim_page_bytes_after": None,
        "reclaim_complete": False,
        "reclaim_error": "controller unavailable",
    }
    # The pass acts on the record it loaded; seed both that record and the
    # file so the release has the old evidence to preserve.
    a.records[scope].update(evidence)
    stored = json.loads(path.read_text())
    stored.update(evidence)
    path.write_text(json.dumps(stored))
    _vanish(b, scope)
    b.ops.clear()

    status = _pass(a)
    assert status["health"] is True and status["active_scopes"] == 0
    assert b.ops == []
    after = json.loads(path.read_text())
    assert after["released_unix"] > 0
    assert after["reclaim_before_bytes"] == 4096
    assert after["reclaim_after_bytes"] is None
    assert after["reclaim_page_bytes_after"] is None
    assert after["reclaim_complete"] is False
    assert after["reclaim_error"] == "controller unavailable"
    assert after["reclaimed_unix"] == 1_789_000_111.0
    assert after["container_settlement"] == SETTLEMENT
    assert after["maintenance_cleanup"] != "settled container transaction"


def test_a_vanished_scope_with_unresolved_tickets_is_retained(
        authority, monkeypatch):
    """No settlement means the container transaction is still open."""

    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch, settle=False)
    _vanish(b, scope)
    b.ops.clear()
    path = _state(a, scope)
    before = path.read_bytes()

    status = _pass(a)
    assert scope in status["active_scope_ids"]
    assert json.loads(path.read_text()).get("released_unix") is None
    assert path.read_bytes() == before
    assert b.ops == []

    _pass(a)
    assert path.read_bytes() == before, "a retained record is not rewritten"


def test_a_vanished_scope_with_malformed_settlement_is_retained(
        authority, monkeypatch):
    """``settled_unix`` alone is not proof; the stored evidence is validated."""

    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    path = _state(a, scope)
    malformed = {**SETTLEMENT, "marker_absent": False}
    # The pass acts on the record it loaded; seed both that record and the
    # file with the malformed stored settlement.
    a.records[scope]["container_settlement"] = malformed
    stored = json.loads(path.read_text())
    stored["container_settlement"] = malformed
    path.write_text(json.dumps(stored))
    _vanish(b, scope)
    b.ops.clear()
    before = path.read_bytes()

    status = _pass(a)
    assert scope in status["active_scope_ids"]
    assert path.read_bytes() == before
    assert b.ops == []


def test_a_vanished_scope_with_an_unknown_namespace_group_is_retained(
        authority, monkeypatch):
    """A picture with a group this broker does not own is not a clean one."""

    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    _vanish(b, scope)
    b.groups["foreign.scope"] = {"budget": 1, "populated": False}
    b.ops.clear()
    path = _state(a, scope)
    before = path.read_bytes()

    status = _pass(a)
    assert status["health"] is False
    assert any("unknown broker namespace group" in error for error in status["errors"])
    assert scope in status["active_scope_ids"]
    assert path.read_bytes() == before
    assert b.ops == []


def test_a_vanished_scope_with_unreadable_inventory_is_retained(
        authority, monkeypatch):
    """A pass that cannot read its own namespace must not retire on it."""

    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    _vanish(b, scope)
    b.ops.clear()

    def unreadable():
        raise OSError("kernel inventory unavailable")

    monkeypatch.setattr(b, "inventory", unreadable)
    path = _state(a, scope)
    before = path.read_bytes()

    status = _pass(a)
    assert status["health"] is False
    assert scope in status["active_scope_ids"]
    assert path.read_bytes() == before
    assert b.ops == []


def test_a_vanished_scope_with_an_unhealthy_kernel_is_retained(
        authority, monkeypatch):
    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    _vanish(b, scope)
    b.ops.clear()
    monkeypatch.setattr(b, "healthy", lambda: False)
    path = _state(a, scope)
    before = path.read_bytes()

    status = _pass(a)
    assert status["health"] is False
    assert scope in status["active_scope_ids"]
    assert path.read_bytes() == before
    assert b.ops == []


@pytest.mark.parametrize("replacement", [False, True])
def test_a_group_that_reappears_before_the_absence_check_is_never_touched(
        authority, monkeypatch, replacement):
    """Inventory predates the absence check; a reappearing group wins the race.

    The pass's inventory does not name the scope, but by the time absence is
    re-checked a group exists at that name again.  It must be retained, and
    the replacement must reach no backend operation at all.
    """

    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    _vanish(b, scope)
    real_inventory = b.inventory

    def inventory_without():
        rows = real_inventory()
        rows.pop(scope, None)
        return rows

    monkeypatch.setattr(b, "inventory", inventory_without)
    b.groups[scope] = {"budget": 64 * 1024**2, "populated": False}
    if replacement:
        b.identity[scope] = [64, 700_777]
    b.ops.clear()
    path = _state(a, scope)
    before = path.read_bytes()

    status = _pass(a)
    assert scope in status["active_scope_ids"]
    assert path.read_bytes() == before
    assert [op for op, _ in b.ops if op in ("stop", "release", "reclaim")] == [], (
        "a reappearing or replacement group is never signalled or reclaimed")
    assert b.groups[scope] == {"budget": 64 * 1024**2, "populated": False}
    assert scope not in b.stopped
    if replacement:
        assert b.identity[scope] == [64, 700_777]


def test_a_failed_metadata_write_keeps_the_scope_active_and_retries(
        authority, monkeypatch):
    """A durable write failure must not release the scope in memory only.

    The replacement record is persisted before the in-memory record changes;
    a failed write leaves the record active and the pass unhealthy, and a
    later successful pass still releases it.  No kernel operation is involved
    either way.
    """

    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    _vanish(b, scope)
    b.ops.clear()
    path = _state(a, scope)
    before = path.read_bytes()

    module_globals = a._maintenance_status.__func__.__globals__
    real_atomic = module_globals["_atomic"]

    def unwritable(target, value, **kwargs):
        if target == path:
            raise OSError("durable metadata write failed")
        return real_atomic(target, value, **kwargs)

    monkeypatch.setitem(module_globals, "_atomic", unwritable)
    status = _pass(a)
    assert status["health"] is False
    assert any("durable metadata write failed" in error for error in status["errors"])
    assert scope in status["active_scope_ids"]
    assert "released_unix" not in a.records[scope]
    assert path.read_bytes() == before
    assert b.ops == []

    monkeypatch.setitem(module_globals, "_atomic", real_atomic)
    status = _pass(a)
    assert status["health"] is True and status["active_scopes"] == 0
    after = json.loads(path.read_text())
    assert after["released_unix"] > 0
    assert after["maintenance_cleanup"] != "settled container transaction"
    assert b.ops == []

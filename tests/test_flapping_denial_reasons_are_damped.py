"""The reason ring damps a flap instead of evicting its own history (#1006).

#991 gave every action a reason ring, ``denial-transitions/<key>.json``, that
keeps the newest 16 ``{unix, host, reason, decision_reason, published_unix}``
entries and appends one whenever a host's reason changes.  A reason that
flips back and forth at a threshold -- ``host_pressure`` against ``admitted``
as PSI crosses the gate -- is a *change* on every single pass, so it appended
a fresh entry every flip and, within 16 flips, evicted the very first
transition: the one that diagnoses where the starvation began.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool  # noqa: E402
import test_a_kill_names_what_it_waited_on as fx  # noqa: E402


def test_forty_flaps_keep_the_first_transition_of_each_reason(tmp_path, monkeypatch):
    """A synthetic clock drives ~40 A<->B flips after an initial reason."""

    queue = pool.PoolQueue(tmp_path / "queue")
    item = fx._publish_export(queue, fx._hexkey("owner"), "flapping")
    key = str(item["action_key"])
    clock = [1000.0]
    monkeypatch.setattr(pool, "_now", lambda: clock[0])

    queue.record_denial(item, "measurement_holder")
    clock[0] += 1.0
    flap_start = clock[0]
    total_flaps = 40
    for index in range(total_flaps):
        queue.record_denial(
            item, "host_pressure" if index % 2 == 0 else "admitted")
        clock[0] += 1.0

    history = queue.denial_transitions(key)

    # Bounded cost: three distinct reasons ever appeared, so the ring holds
    # three entries -- not forty-one, and not truncated to sixteen with the
    # first one gone.
    assert [entry["reason"] for entry in history] == [
        "measurement_holder", "host_pressure", "admitted"]
    first = history[0]
    assert first["unix"] == 1000.0
    assert first["count"] == 1
    assert first["last_unix"] == 1000.0

    pressure, admitted = history[1], history[2]
    # host_pressure landed on the even flaps (0, 2, .., 38): 20 of them.
    assert pressure["unix"] == flap_start
    assert pressure["count"] == 20
    assert pressure["last_unix"] == flap_start + (total_flaps - 2)
    # admitted landed on the odd flaps (1, 3, .., 39): 20 of them.
    assert admitted["unix"] == flap_start + 1.0
    assert admitted["count"] == 20
    assert admitted["last_unix"] == flap_start + (total_flaps - 1)


def test_a_flap_back_to_the_first_reason_is_damped_not_re_appended(tmp_path):
    """A<->B<->A: the ring keeps two entries, not three."""

    queue = pool.PoolQueue(tmp_path / "queue")
    item = fx._publish_export(queue, fx._hexkey("owner"), "back-and-forth")
    key = str(item["action_key"])

    queue.record_denial(item, "host_pressure")
    queue.record_denial(item, "measurement_holder")
    queue.record_denial(item, "host_pressure")

    history = queue.denial_transitions(key)

    assert [entry["reason"] for entry in history] == [
        "host_pressure", "measurement_holder"]
    assert history[0]["count"] == 2
    assert history[1]["count"] == 1


def test_distinct_reasons_still_evict_the_oldest_past_the_ring_bound(tmp_path):
    """No repeats: the ring's existing bound is unchanged (the #991 shape)."""

    queue = pool.PoolQueue(tmp_path / "queue")
    item = fx._publish_export(queue, fx._hexkey("owner"), "genuinely-new")
    total = pool.MAX_DENIAL_TRANSITIONS + 4
    for index in range(total):
        queue.record_denial(item, f"reason_{index}")

    history = queue.denial_transitions(str(item["action_key"]))

    assert len(history) == pool.MAX_DENIAL_TRANSITIONS
    assert history[-1]["reason"] == f"reason_{total - 1}"
    assert history[0]["reason"] == f"reason_{total - pool.MAX_DENIAL_TRANSITIONS}"


def test_three_reasons_across_three_passes_still_all_land_in_order(tmp_path):
    """The #991 acceptance, undisturbed by damping: no reason repeats here."""

    queue = pool.PoolQueue(tmp_path / "queue")
    item = fx._publish_export(queue, fx._hexkey("owner"), "no-repeats")
    key = str(item["action_key"])

    for reason in ("reservation_busy", "measurement_holder", "host_pressure"):
        queue.record_denial(item, reason)

    history = queue.denial_transitions(key)

    assert [entry["reason"] for entry in history] == [
        "reservation_busy", "measurement_holder", "host_pressure"]
    assert all(entry["count"] == 1 for entry in history)

"""Preference grace and diagnostic ages survive passes and retire together."""
import pytest

from prismabuild import _peer_placement as placement


FIRST = ("a" * 64, "1.0")
OTHER = ("b" * 64, "1.0")
CAPACITY = placement.PeerCapacity({"cpu": 2}, {"cpu": 2}, 2)


def _view(history, clock):
    return placement.PlacementPass(
        lambda: [], lambda host, offer: None, history=history,
        wall_clock=lambda: clock["unix"])


@pytest.mark.parametrize("preference", list(placement.TimedPreference))
def test_grace_survives_a_new_pass_and_expires_at_twenty_seconds(monkeypatch, preference):
    clock = {"monotonic": 10., "unix": 100.}
    monkeypatch.setattr(placement.time, "monotonic", lambda: clock["monotonic"])
    history = placement.PlacementHistory()
    assert not _view(history, clock).timer_expired(preference, FIRST)
    clock.update(monotonic=29.999, unix=-1000.)
    assert not _view(history, clock).timer_expired(preference, FIRST)
    clock["monotonic"] = 30.
    view = _view(history, clock)
    assert view.timer_expired(preference, FIRST)
    assert not view.timer_expired(preference, (FIRST[0], "2.0"))
    other_preference = next(value for value in placement.TimedPreference if value is not preference)
    assert not view.timer_expired(other_preference, FIRST)


@pytest.mark.parametrize("retirement", ["empty", "absent", "claimed"])
def test_retirement_resets_both_graces_and_cpu_host_age(monkeypatch, retirement):
    clock = {"monotonic": 10., "unix": 100.}
    monkeypatch.setattr(placement.time, "monotonic", lambda: clock["monotonic"])
    history = placement.PlacementHistory()
    first = _view(history, clock)
    for generation in (FIRST, OTHER):
        for preference in placement.TimedPreference:
            assert not first.timer_expired(preference, generation)
        assert first.cpu_host_evidence(generation, "peer", CAPACITY, {"cpu": 1})["yielding_s"] == 0.
    clock.update(monotonic=31., unix=151.)
    second = _view(history, clock)
    for preference in placement.TimedPreference:
        assert second.timer_expired(preference, FIRST)
    assert second.cpu_host_evidence(FIRST, "peer", CAPACITY, {"cpu": 1})["yielding_s"] == 51.
    if retirement == "empty":
        history.clear()
    elif retirement == "absent":
        history.retain({OTHER})
    else:
        history.discard(FIRST)
    third = _view(history, clock)
    for preference in placement.TimedPreference:
        assert not third.timer_expired(preference, FIRST)
        assert third.timer_expired(preference, OTHER) is (retirement != "empty")
    assert third.cpu_host_evidence(FIRST, "peer", CAPACITY, {"cpu": 1})["yielding_s"] == 0.
    assert third.cpu_host_evidence(OTHER, "peer", CAPACITY, {"cpu": 1})["yielding_s"] == (
        0. if retirement == "empty" else 51.)

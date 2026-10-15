"""Receipt loss cannot start tier work or erase a real completed-unit proof."""
import signal
import time

import pytest

from test_prewarm_requires_known_receipts import KNOWN
from tier_receipt_fixture import complete, finish, replace, start, state, unknown_boundary


@pytest.mark.parametrize("unknown_side", ["loaded", "published"])
def test_atomic_repair_resumes_with_the_first_known_loaded_identity(tmp_path, unknown_side):
    role = start(tmp_path, None if unknown_side == "loaded" else KNOWN,
                 None if unknown_side == "published" else KNOWN)
    try:
        prefix = unknown_boundary(role)
        state(role, cycles=0, layout=False)
        if unknown_side == "published":
            replace(tmp_path / "loaded.json", {**KNOWN, "generation": "replacement"})
        replace(tmp_path / f"{unknown_side}.json", KNOWN)
        output = complete(role, prefix, adoption=1)
        assert b"tier-runtime-moved" not in output
        state(role, cycles=1, layout=True)
    finally:
        finish(role)


@pytest.mark.parametrize("unknown_side", ["loaded", "published"])
@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
def test_unknown_wait_is_interruptible(tmp_path, unknown_side, stop_signal):
    role = start(tmp_path, None if unknown_side == "loaded" else KNOWN,
                 None if unknown_side == "published" else KNOWN, interval=3600)
    try:
        prefix = unknown_boundary(role)
        state(role, cycles=0, layout=False)
        role.child.send_signal(stop_signal)
        output = complete(role, prefix, cycles=0, adoption=0, timeout=3)
        assert b"tier-runtime-moved" not in output
        state(role, cycles=0, layout=False)
    finally:
        finish(role)


@pytest.mark.parametrize("fence", ["construct", "layout", "adopt"])
@pytest.mark.parametrize("once", [True, False])
def test_each_startup_fence_observes_publication_again(tmp_path, fence, once):
    role = start(tmp_path, KNOWN, KNOWN, mode="fence-" + fence, once=once)
    try:
        prefix = unknown_boundary(role)
        assert (tmp_path / "fence-reached").exists()
        state(role, cycles=0, layout=fence != "construct")
        if not once:
            replace(tmp_path / "published.json", KNOWN)
        output = complete(role, prefix, code=75 if once else 0, cycles=int(not once),
                          adoption=int(fence == "adopt") if once else 1)
        assert b"tier-runtime-moved" not in output
        state(role, cycles=int(not once), layout=fence != "construct" or not once)
    finally:
        finish(role)


@pytest.mark.parametrize("after_cycle", [False, True])
@pytest.mark.parametrize("once", [False, True])
def test_unknown_receipts_preserve_the_actual_completed_idle_park(tmp_path, after_cycle, once):
    role = start(tmp_path, KNOWN, KNOWN if after_cycle else None, mode="park",
                 closed=not after_cycle, once=once)
    try:
        output = complete(role, code=75 if once else 0, cycles=int(after_cycle),
                          adoption=int(after_cycle))
        assert b"tier-runtime-moved" not in output
        markers = state(role, cycles=int(after_cycle), layout=after_cycle, parked=1)
        assert markers[0].name.endswith("-101.5")
    finally:
        finish(role)


@pytest.mark.parametrize("proof_failure", ["rotate-epoch", "marker-failure", "nonfinite-epoch"])
def test_unknown_cannot_replace_physical_park_proof(tmp_path, proof_failure):
    after_cycle = proof_failure == "rotate-epoch"
    role = start(tmp_path, KNOWN, KNOWN if after_cycle else None, mode=proof_failure,
                 closed=not after_cycle, epoch=float("nan") if proof_failure == "nonfinite-epoch" else 101.5)
    try:
        output = complete(role, code=75, cycles=int(after_cycle), adoption=int(after_cycle))
        assert b"tier-runtime-moved" not in output
        markers = state(role, cycles=int(after_cycle), layout=after_cycle,
                        parked=int(proof_failure != "marker-failure"))
        if after_cycle:
            assert markers[0].name.endswith("-101.5")
            assert not any(marker.name.endswith("-202.5") for marker in markers)
    finally:
        finish(role)


@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
@pytest.mark.parametrize("once", [True, False])
def test_completed_cycle_accounting_finishes_after_publication_loss(tmp_path, stop_signal, once):
    role = start(tmp_path, KNOWN, KNOWN, mode="lose-stop", once=once, stop_signal=stop_signal)
    try:
        output = complete(role, adoption=1)
        assert b"tier-runtime-moved" not in output
        state(role, cycles=1, layout=True)
    finally:
        finish(role)


def test_completed_once_cycle_does_not_need_a_new_receipt_authorization(tmp_path):
    role = start(tmp_path, KNOWN, KNOWN, mode="lose-only", once=True)
    try:
        output = complete(role, adoption=1)
        assert b"tier-runtime-moved" not in output
        state(role, cycles=1, layout=True)
    finally:
        finish(role)


@pytest.mark.parametrize("once", [True, False])
def test_actual_parse_failure_and_stop_never_prove_a_completed_cycle(tmp_path, once):
    role = start(tmp_path, KNOWN, KNOWN, mode="fail-stop", once=once)
    try:
        output = complete(role, code=75, cycles=0, failed=1, adoption=1)
        assert b"tier-runtime-moved" not in output
        state(role, cycles=0, layout=True, failed=True)
    finally:
        finish(role)


@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
def test_stop_during_actual_receipt_read_finishes_the_unit_before_park(tmp_path, stop_signal):
    role = start(tmp_path, KNOWN, KNOWN, mode="held-read", stop_signal=stop_signal)
    try:
        held = bytearray()
        while len(held) < 4:
            chunk = role.control.recv(4 - len(held))
            assert chunk, "actual receipt read did not reach its barrier"
            held.extend(chunk)
        assert held == b"held"
        (tmp_path / "published.json").unlink()
        replace(tmp_path / "maintenance.json", {"draining": True, "changed_unix": 101.5})
        role.child.send_signal(stop_signal)
        deadline = time.monotonic() + 3
        while True:
            role.control.sendall(b"?")
            reply = role.control.recv(1)
            assert reply in (b"0", b"1")
            if reply == b"1":
                break
            assert time.monotonic() < deadline, "actual stop Event was not set"
        assert role.child.poll() is None
        assert list((tmp_path / "parked").iterdir()) == []
        assert not (tmp_path / "floor-finished").exists()
        assert role.queue.tiers() == []
        role.control.sendall(b"r")
        output = complete(role, adoption=1)
        assert b"tier-runtime-moved" not in output
        markers = state(role, cycles=1, layout=True, parked=1)
        assert markers[0].name.endswith("-101.5")
    finally:
        finish(role)


def test_known_generation_drift_after_a_real_cycle_retires(tmp_path):
    role = start(tmp_path, KNOWN, KNOWN, mode="move")
    try:
        output = complete(role, adoption=1)
        assert b"tier-runtime-moved" in output
        state(role, cycles=1, layout=True)
    finally:
        finish(role)

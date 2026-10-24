"""GPU pressure reclaims CPU occupancy even without physical CPU tokens (#1929)."""
import time

import pytest

from prismabuild import pool
from test_gpu_evicts_cpu_guests import guest_fleet


@pytest.fixture(params=[2, 0], ids=["memory-tokens", "metadata-only"])
def borrowed_guest(guest_fleet, monkeypatch, request):
    fleet = guest_fleet
    clock = [time.time()]
    monkeypatch.setattr(pool, "_now", lambda: clock[0])
    monkeypatch.setattr(pool.cpu_admission.time, "time", lambda: clock[0])
    state = {"busy_cpus": 0., "psi_some": 0.}

    def sample():
        return {"sampled_unix": clock[0], "cpu_count": 8, "interval_s": 1., **state}

    key = "a" * 64
    fleet.queue.publish(action_key=key, cas_root="/cas", checkout_root="/co",
        worker_script="/w.py", resources={"cpu": 2, "mem_gb": request.param},
        retry_safe=True, max_attempts=3, priority=20)
    guest = fleet.claim()
    assert guest["action_key"] == key and guest["cpu_guest"] is True
    assert guest["cpu_allocation"] == {"preferred": [0, 1], "fallback": []}
    ledger = fleet.queue.ledger()
    # Reader seam: retain a pre-QoS borrow after its donor returned the physical
    # tokens. This priority-20 guest cannot acquire a new opportunistic loan.
    # Current eligibility and acquisition are exercised in test_cpu_lending.
    path = ledger.held_dir / key / pool.cpu_admission.METADATA
    metadata = pool.cpu_admission.read_json(path)
    metadata.pop('cpu_qos', None)
    metadata.pop('cpu_weight', None)
    metadata.update(borrowing=True, borrowed_cpu=2, shape='shape', admitted_unix=clock[0])
    for token in (ledger.held_dir / key).glob('cpu-*'):
        token.rename(ledger.free_dir / token.name)
    pool.cpu_admission.write_json(path, metadata)
    assert ledger.holder_tokens(key) == ({"mem_gb": request.param} if request.param else {})
    assert pool.cpu_admission.read_json(
        ledger.held_dir / key / pool.cpu_admission.METADATA)["borrowed_cpu"] == 2
    assert ledger.available()["cpu"] == 8  # pressure, not a physical token gap
    return fleet, key, guest, sample


@pytest.mark.parametrize("foreign", [False, True], ids=["pool-pressure", "foreign-pressure"])
def test_gpu_host_pressure_reclaims_fully_borrowed_guest(borrowed_guest, monkeypatch, foreign):
    fleet, key, guest, sample = borrowed_guest
    other = fleet.publish("d", priority=30)
    other_claim = fleet.claim()
    assert other_claim["action_key"] == other and other_claim["cpu_guest"] is True
    gpu = fleet.publish("b", gpu=True, priority=-10)

    def pressured(controller, item, demand, **kwargs):
        controller.last_decision = {"reason": "host_pressure", "held_cpus": [0, 1],
                                    "foreign_cpus": [7] if foreign else []}
        return None

    monkeypatch.setattr(pool.cpu_admission.Controller, "decision", pressured)
    assert fleet.claim(adaptive_cpu=True, cpu_sampler=sample, gpu_sampler=lambda: {}) is None
    denial = next(row for row in fleet.queue.latest_denials({gpu})[gpu]
                  if row["host"] == "sparky")
    assert denial["evidence"]["decision"]["reason"] == "host_pressure"
    assert denial["evidence"]["drain_resolves"] is (not foreign)
    assert bool(fleet.stopped(key)) is (not foreign)
    if not foreign:
        assert fleet.stopped(key)[0]["preempted_by"] == gpu
        retry = pool.record_store.read_document(fleet.queue.records.item_path(pool.READY, key))
        assert retry["priority"] == 20 and retry["attempts"] == 1
        assert retry["supersedes_withdrawal"]["published_unix"] == guest["published_unix"]
        # A second pass cannot select another guest while this one's borrowed
        # allocation (possibly with no physical tokens at all) awaits cleanup.
        assert fleet.claim(adaptive_cpu=True, cpu_sampler=sample, gpu_sampler=lambda: {}) is None
        assert not fleet.stopped(other)
        fleet.finish(guest, "withdrawn")
        assert fleet.queue.ledger().available()["cpu"] == 6
        fleet.finish(other_claim)
        assert fleet.queue.ledger().available()["cpu"] == 8


def test_pressure_to_token_drain_waits_for_borrowed_guest_cleanup(borrowed_guest, monkeypatch):
    fleet, key, guest, sample = borrowed_guest
    other = fleet.publish("d", priority=30)
    other_claim = fleet.claim()
    assert other_claim["action_key"] == other and other_claim["cpu_guest"] is True
    gpu = fleet.publish("b", gpu=True, cpu=7, priority=-10)
    ledger = fleet.queue.ledger()
    assert ledger.available()["cpu"] == 6

    def pressured(controller, item, demand, **kwargs):
        controller.last_decision = {"reason": "host_pressure", "held_cpus": [0, 1],
                                    "foreign_cpus": []}
        return None

    monkeypatch.setattr(pool.cpu_admission.Controller, "decision", pressured)
    assert fleet.claim(adaptive_cpu=True, cpu_sampler=sample, gpu_sampler=lambda: {}) is None
    assert fleet.stopped(key)[0]["preempted_by"] == gpu
    assert not fleet.stopped(other)

    # Change refusal paths while the borrowed allocation still awaits cleanup.
    # Even a holder with no physical tokens must serialize the next handoff.
    for _ in range(2):
        assert fleet.claim() is None  # non-adaptive, physical CPU token shortage
        assert not fleet.stopped(other)
        assert len(fleet.stopped(key)) == 1
        assert key in ledger.held_keys()
        assert fleet.queue.records.item_path(pool.CLAIMED, key).exists()
        assert ledger.available()["cpu"] == 6

    fleet.finish(guest, "withdrawn")
    assert key not in ledger.held_keys()
    assert not fleet.queue.records.item_path(pool.CLAIMED, key).exists()
    assert ledger.available()["cpu"] == 6  # borrowed CPUs return no physical tokens
    assert fleet.claim() is None
    assert fleet.stopped(other)[0]["preempted_by"] == gpu
    assert fleet.claim() is None
    assert len(fleet.stopped(other)) == 1
    assert ledger.available()["cpu"] == 6
    fleet.finish(other_claim, "withdrawn")
    taken = fleet.claim()
    assert taken["action_key"] == gpu
    fleet.finish(taken)
    assert ledger.available() == fleet.capacity


@pytest.mark.parametrize("mode,cpus,evicts", [
    ("exclusive", None, True),
    (pool.DRAIN_HELD_CPUS, [0], True),  # CPU zero is a real allocation
    (pool.DRAIN_HELD_CPUS, [2, 3], False),
])
def test_pressure_drains_use_borrowed_cpu_allocation(borrowed_guest, mode, cpus, evicts):
    fleet, key, _, _ = borrowed_guest
    gpu = fleet.publish("b", gpu=True)
    result = fleet.queue._preempt_background_holder(
        fleet.queue.ledger(), action_key=gpu, demand={"cpu": 1, "gpu": 1},
        priority=0, guest_drain=mode, held_cpus=cpus)
    assert (result is not None) is evicts
    assert bool(fleet.stopped(key)) is evicts


def test_borrowed_allocation_does_not_count_as_returned_cpu_tokens(borrowed_guest):
    fleet, key, _, _ = borrowed_guest
    pinned = fleet.publish("d", cpu=2, tags=["sparky"], retry_safe=False, max_attempts=1)
    assert fleet.claim()["action_key"] == pinned
    gpu = fleet.publish("b", gpu=True, cpu=8)
    assert fleet.queue.ledger().available()["cpu"] == 6
    assert fleet.queue._preempt_background_holder(
        fleet.queue.ledger(), action_key=gpu, demand={"cpu": 8, "gpu": 1},
        priority=0, guest_drain="tokens") is None
    assert not fleet.stopped(key)


@pytest.mark.parametrize("mode", [pool.DRAIN_HELD_CPUS, "tokens"])
@pytest.mark.parametrize("pending", ["finish_pending", "container_cleanup_pending"])
def test_pending_borrower_blocks_another_guest_handoff(borrowed_guest, pending, mode):
    fleet, key, guest, _ = borrowed_guest
    other = fleet.publish("d", priority=30)
    assert fleet.claim()["action_key"] == other
    fleet.queue.records.replace_record_at(fleet.queue.records.item_path(pool.CLAIMED, key),
        {**guest, "cpu_guest": False, pending: {"pending": True}})
    gpu = fleet.publish("b", gpu=True)
    assert fleet.queue._preempt_background_holder(
        fleet.queue.ledger(), action_key=gpu,
        demand={"cpu": 8 if mode == "tokens" else 1, "gpu": 1},
        priority=0, guest_drain=mode, held_cpus=[0, 1]) is None
    assert not fleet.stopped(key) and not fleet.stopped(other)


@pytest.fixture
def real_loan_guest(guest_fleet, monkeypatch):
    """A #1929 guest lends one CPU through the real legacy admission gate."""
    fleet = guest_fleet
    clock = [time.time()]
    monkeypatch.setattr(pool, '_now', lambda: clock[0])
    monkeypatch.setattr(pool.cpu_admission.time, 'time', lambda: clock[0])
    def sample():
        return {'sampled_unix': clock[0], 'cpu_count': 8, 'interval_s': 1.,
                'busy_cpus': .4, 'psi_some': 0., 'psi_avg10': 0., 'psi_avg60': 0.,
                'per_cpu_busy': {str(cpu): .05 for cpu in range(8)}}
    donor = fleet.publish('a', cpu=4, priority=20)
    guest = fleet.claim(adaptive_cpu=True, cpu_sampler=sample)
    assert guest['action_key'] == donor and guest['cpu_guest']
    borrower = fleet.publish('c', cpu=5, priority=20, tags=['sparky'])
    ledger = fleet.queue.ledger()
    start = clock[0]
    claimed = None
    for step in range(1, 4):
        clock[0] = start + step
        pool.cpu_admission.write_json(
            pool.cpu_admission.Controller(ledger, fleet.tiers).base / 'telemetry' / (donor + '.json'),
            {'action_key': donor, 'complete': True, 'nonce': 'e' * 32,
             'sampled_unix': clock[0], 'wall_seconds': float(step), 'cpu_seconds': step * .1})
        claimed = fleet.claim(adaptive_cpu=True, cpu_sampler=sample)
        if claimed is not None:
            break
    assert claimed and claimed['action_key'] == borrower and not claimed.get('cpu_guest')
    assert claimed['cpu_admission']['borrowed_cpus'] == [0]
    assert claimed['cpu_admission']['cap'] == 0
    assert claimed['cpu_admission']['gate_reason'] == 'legacy_adaptive'
    assert not (pool.cpu_admission.Controller(ledger, fleet.tiers).base / 'cpu-lending.json').exists()
    return fleet, guest, claimed


def test_guest_drain_cannot_reuse_a_surviving_real_borrow(real_loan_guest):
    fleet, guest, borrower = real_loan_guest
    ledger = fleet.queue.ledger()
    assert ledger.available().get('cpu', 0) == 0
    assert ledger.admission_available(releasing=[guest['action_key']])['cpu'] == 3
    gpu = fleet.publish('b', gpu=True, cpu=4)
    assert fleet.queue._preempt_background_holder(ledger, action_key=gpu,
        demand={'cpu': 4, 'gpu': 1}, priority=0, guest_drain='tokens') is None
    assert not fleet.stopped(guest['action_key'])
    fleet.finish(guest)
    assert ledger.available()['cpu'] == 4
    assert ledger.admission_available()['cpu'] == 3
    gpu_row = pool.record_store.read_document(fleet.queue.records.item_path(pool.READY, gpu))
    _, first = fleet.queue._gpu_first_order([gpu_row], ledger=ledger,
        total=fleet.capacity, tags=frozenset({'sparky', 'gb10'}),
        has_gpu=True, gpu_controller=None)
    assert gpu not in first
    assert fleet.claim() is None
    assert not fleet.stopped(borrower['action_key'])
    fleet.finish(borrower)
    assert fleet.claim()['action_key'] == gpu


def test_borrowed_ids_and_gate_survive_release_in_attempt_and_ending(real_loan_guest):
    fleet, guest, borrower = real_loan_guest
    fleet.finish(guest)
    evidence = borrower['cpu_admission']
    path = fleet.queue.finish(borrower['action_key'], status='executed',
        detail={'returncode': 0}, claim_snapshot=borrower)
    ending = pool.record_store.read_document(path)
    assert borrower['action_key'] not in fleet.queue.ledger().held_keys()
    assert ending['cpu_admission'] == evidence
    attempts = fleet.queue.attempt_outcomes(ending)
    assert len(attempts) == 1 and attempts[0]['cpu_admission'] == evidence
    assert attempts[0]['cpu_weight'] == 100 and attempts[0]['cpu_qos_selected'] == 'legacy'


def test_gpu_drain_reclaims_an_orphaned_metadata_only_real_borrow(guest_fleet, monkeypatch):
    fleet = guest_fleet
    clock = [time.time()]
    monkeypatch.setattr(pool, '_now', lambda: clock[0])
    monkeypatch.setattr(pool.cpu_admission.time, 'time', lambda: clock[0])
    def sample():
        return {'sampled_unix': clock[0], 'cpu_count': 8, 'interval_s': 1.,
                'busy_cpus': 0., 'psi_some': 0.,
                'per_cpu_busy': {str(cpu): 0. for cpu in range(8)}}
    donor = fleet.publish('a', cpu=8, tags=['sparky'])
    owner = fleet.claim(adaptive_cpu=True, cpu_sampler=sample)
    assert owner and owner['action_key'] == donor and not owner.get('cpu_guest')
    borrower = 'c' * 64
    fleet.queue.publish(action_key=borrower, cas_root='/cas', checkout_root='/co',
        worker_script='/w.py', resources={'cpu': 1}, priority=20,
        retry_safe=True, max_attempts=3)
    ledger = fleet.queue.ledger()
    controller = pool.cpu_admission.Controller(ledger, fleet.tiers)
    start, guest = clock[0], None
    for step in range(1, 4):
        clock[0] = start + step
        pool.cpu_admission.write_json(controller.base / 'telemetry' / (donor + '.json'), {
            'action_key': donor, 'complete': True, 'nonce': 'e' * 32,
            'sampled_unix': clock[0], 'wall_seconds': float(step), 'cpu_seconds': step * .1})
        guest = fleet.claim(adaptive_cpu=True, cpu_sampler=sample)
        if guest is not None:
            break
    assert guest and guest['action_key'] == borrower and guest['cpu_guest']
    assert guest['cpu_admission']['borrowed_cpus'] == [0]
    assert ledger.holder_tokens(borrower) == {}
    fleet.finish(owner)
    assert ledger.available()['cpu'] == 8 and ledger.admission_available()['cpu'] == 7
    gpu = fleet.publish('b', gpu=True, cpu=8)
    assert fleet.claim() is None
    assert fleet.stopped(borrower)[0]['preempted_by'] == gpu
    fleet.finish(guest, 'withdrawn')
    assert fleet.claim()['action_key'] == gpu

"""Actual local adaptive-state writers and policy readers with supplied observations."""
from __future__ import annotations

import base64
from contextlib import contextmanager
import json
from pathlib import Path
import runpy
import subprocess
from types import SimpleNamespace
import uuid

C, G, D = "c" * 64, "7" * 32, "d" * 64
TIERS = {"preferred": [0, 1], "fallback": []}


@contextmanager
def owned_publications():
    original, children = subprocess.Popen, []
    def started(*args, **kwargs):
        child = original(*args, **kwargs)
        children.append(child)
        return child
    subprocess.Popen = started
    try:
        yield
    finally:
        subprocess.Popen = original
        for child in children:
            assert child.wait(timeout=10) == 0, "fixture-owned publication child failed"


def sample():
    return {"sampled_unix": 100., "cpu_count": 2, "busy_cpus": 0., "psi_some": 0., "interval_s": 1.}


def context(store, load):
    cpu, pool, core = (load("prismabuild." + name) for name in ("adaptive_cpu", "pool", "core"))
    cpu.BOX_STATE_ROOT = store / "states"
    ledger = pool.ResourceLedger(store / "queue/reservations", "compat-host")
    ledger.ensure_capacity({"cpu": 2})
    ledger.configure_cpu_tiers(TIERS)
    controller = cpu.Controller(ledger, TIERS)
    controller.sample = sample
    body = runpy.run_path(str(Path(__file__).with_name("compat_core_adapters.py")))["action_body"](core, store)
    action = core.seal_action(body)
    cas = core.PrismaBuildCAS(store / "cas")
    cas.publish_action_request(action)
    item = {"action_key": action["action_key"], "cas_root": str(cas.root), "claimed_unix": 50., "resources": {"cpu": 1}}
    return cpu, ledger, controller, item


def completion(cpu, ledger, item, nonce):
    return cpu.record_completion(ledger, item, {"complete": True, "action_key": item["action_key"],
        "sampled_unix": 100., "nonce": nonce, "cpu_seconds": 1., "wall_seconds": 2., "memory_peak_bytes": 8})


def jobs(cpu, ledger, controller, item, measured):
    shape = cpu.action_identity(item)[0]
    handle = ledger.begin_acquire(C, {"cpu": 1}, adaptive={"declared_cpu": 1, "measurement": False,
        "shape": shape, "admitted_unix": 90.}, cpu_tiers=TIERS)
    assert handle and ledger.commit_acquire(C, handle) > 0
    path = controller.base / "telemetry" / (C + ".json")
    def observe(at, wall, used):
        cpu.write_json(path, {"complete": True, "action_key": C, "nonce": G, "sampled_unix": at,
            "cpu_seconds": used, "wall_seconds": wall, "memory_peak_bytes": 8})
    observe(99., 10., 1.)
    assert controller.decision(item, {"cpu": 1}, owner=None)
    if measured:
        observe(100., 11., 1.1)
        assert controller.decision(item, {"cpu": 1}, owner=None)
    return lambda: observe(100., 11., 1.1)


def registered_borrow(cpu, pool, core, store):
    """Current writers obtain authority through the real policy and ledger."""
    tiers = {'preferred': [0, 1, 2, 3], 'fallback': [4, 5, 6, 7]}
    ledger = pool.ResourceLedger(store / 'loan-queue/reservations', 'compat-host')
    ledger.ensure_capacity({'cpu': 8})
    ledger.configure_cpu_tiers(tiers)
    clock = [97.]
    cpu.time.time = lambda: clock[0]
    def sampled():
        return {'sampled_unix': clock[0], 'cpu_count': 8, 'interval_s': 1.,
                'busy_cpus': .4, 'psi_some': 0., 'psi_avg10': 0., 'psi_avg60': 0.,
                'per_cpu_busy': {str(c): .1 if c < 4 else 0. for c in range(8)}}
    controller = cpu.Controller(ledger, tiers, sampler=sampled, ramp_cap=2)
    state = {}
    for stamp in range(66, 97):
        state = cpu.cpu_lending.advance_lending_history(
            state, {**sampled(), 'sampled_unix': stamp}, identity=controller._lending_identity,
            cpus=controller.cpus, preferred=tiers['preferred'], now=stamp, cap=2)
    controller.write_state('cpu-lending.json', state)
    body = runpy.run_path(str(Path(__file__).with_name('compat_core_adapters.py')))['action_body'](core, store)
    cas = core.PrismaBuildCAS(store / 'loan-cas')
    items = []
    for role, count in (('donor', 4), ('borrower', 1)):
        action = core.seal_action({**body, 'params': {**body['params'], 'loan_fixture': role}})
        cas.publish_action_request(action)
        items.append({'action_key': action['action_key'], 'cas_root': str(cas.root),
                      'cpu_qos': 'opportunistic', 'priority': -10, 'resources': {'cpu': count}})
    donor, item = items
    meta = controller.decision(donor, donor['resources'], owner=None)
    handle = ledger.begin_acquire(donor['action_key'], donor['resources'], adaptive=meta, cpu_tiers=tiers)
    assert handle and ledger.commit_acquire(donor['action_key'], handle)
    for at in (98., 99., 100.):
        clock[0] = at
        cpu.write_json(controller.base / 'telemetry' / (donor['action_key'] + '.json'), {
            'action_key': donor['action_key'], 'nonce': G, 'complete': True,
            'sampled_unix': at, 'wall_seconds': at - 97., 'cpu_seconds': (at - 97.) * .1})
        controller._host_sample = None
        meta = controller.decision(item, item['resources'], owner=None)
    assert meta and meta['borrowing']
    handle = ledger.begin_acquire(item['action_key'], item['resources'], adaptive=meta, cpu_tiers=tiers)
    assert handle and ledger.commit_acquire(item['action_key'], handle)
    return ledger, controller, item, meta


def operation(request, store, load):
    family, variant = request["case"].split("/", 1)
    writing = request["op"] == "write"
    cpu, ledger, controller, item = context(store, load)
    if family == "state-cpu-profile":
        path = controller.base / "profiles.json"
        if writing:
            if variant == "live":
                jobs(cpu, ledger, controller, item, True)
            else:
                for index in range(3 if variant != "single" else 1):
                    assert completion(cpu, ledger, item, str(index))
                if variant == "replay":
                    before = path.read_bytes()
                    assert not completion(cpu, ledger, item, "2") and path.read_bytes() == before
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            if variant == "live":
                assert completion(cpu, ledger, item, "consumer")
                shape = cpu.action_identity(item)[0]
                assert cpu.read_json(path)[shape]["samples"] == 2
                return {"samples": 2}
            result = controller.decision(item, {"cpu": 1}, owner=None)
            assert result and result["cost"] == (1. if variant == "single" else .625), result
            return {"cost": result["cost"]}
    elif family == "state-cpu-jobs":
        path = controller.base / "jobs.json"
        observe = jobs(cpu, ledger, controller, item, variant == "measured")
        if not writing:
            path.write_bytes(base64.b64decode(request["raw"]))
            observe()
            request["_consumer_boundary"]()
            result = controller.decision(item, {"cpu": 1}, owner=None)
            assert result and abs(result["active_cpu_cost"] - .125) < 1e-9, result
            return {"active_cpu_cost": result["active_cpu_cost"]}
    elif family == "state-cpu-borrow":
        path = controller.base / "last-borrow.json"
        metadata = {"borrowing": True, "sampled_unix": 100.}
        registered = hasattr(cpu, 'cpu_lending')
        if registered:
            ledger, controller, item, metadata = registered_borrow(cpu, load('prismabuild.pool'), load('prismabuild.core'), store)
            path = controller.base / 'last-borrow.json'
            if not writing and variant == 'admitted':
                controller.admitted(metadata)
        if writing:
            previous = controller.admitted(metadata)
            if variant == "returned":
                controller.withdrew(metadata, previous)
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            if variant == "returned":
                previous = controller.admitted(metadata)
                assert previous.get("sampled_unix", 0) == 0 and "borrow_id" not in previous, previous
                return {"empty_previous": True}
            metadata["borrow_id"] = G
            controller.withdrew(metadata, {})
            assert cpu.read_json(path) == {}, "foreign owner or sample cannot return this borrowing"
            return {"returned": True}
    elif family == "state-export-rates":
        path = controller.base / cpu.EXPORT_RATES
        family_name = "compat" if variant == "family" else None
        if writing:
            for at in (50., 60., 70., 80.):
                assert cpu.learn_export(ledger, D, C, at, 20., family=family_name)
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            result = cpu.export_slots(cpu.read_json(path), D, family_name)
            assert result[0] == 2, result
            return {"slots": result[0]}
    elif family == "state-cpu-sample":
        controller = cpu.Controller(ledger, TIERS)
        path = controller.base / "cpu-sample.json"
        if writing:
            current = {"sampled_unix": 90., "cpus": {"0": [0, 10], "1": [0, 10]}, "psi_total": 0.}
            cpu.counters = lambda cpus: dict(current)
            cpu.control_plane_counters = lambda cpus: {}
            cpu.boot_ticks = lambda: 100
            controller.sample()
            current.update(sampled_unix=100., cpus={"0": [10, 20], "1": [10, 20]})
            assert controller.sample()["busy_cpus"] == 2.
            if variant == "cached":
                assert controller.sample()["busy_cpus"] == 2.
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            result = controller.sample()
            assert result["busy_cpus"] == 2., result
            return result
    else:
        gpu = load("prismabuild.adaptive_gpu")
        controller = gpu.Controller(ledger)
        path = controller.base / "gpu-state.json"
        device = {"uuid": "GPU-compat", "name": next(iter(gpu.DECLARED_GPU_POWER_REFERENCE_W)),
                  "power_w": 1000., "power_reference_w": 2000., "power_reference_scope": "soc_tdp"}
        if writing:
            state = {"power_window": []}
            gpu.record_power_peak(state, device)
            controller._write_state(state)
            if variant != "power":
                ticket = controller.reserve_probe({"probe": True, "sample_id": "compat-sample", "sampled_unix": 100.,
                    "device_uuid": device["uuid"], "admitted_unix": 100., "members_before": [], "action_key": C})
                assert ticket
                if variant == "returned":
                    controller.return_probe(ticket)
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            result = gpu.reporting_power_reference(device, gpu.host_local_power_state(ledger.base))
            assert result[0] == 1000., result
            return {"watts": result[0]}
    return {"raw": base64.b64encode(path.read_bytes()).decode(),
            "fixture_facts": ["private telemetry/cache/ledger files and actual writers; supplied counters, device observations and attributed totals; no physical performance or admission proof"]}


def operate(request, store, load):
    nonce = uuid.uuid4
    if request["case"].split("/", 1)[0] in ("state-cpu-borrow", "state-gpu"):
        uuid.uuid4 = lambda: SimpleNamespace(hex=G)
    try:
        with owned_publications():
            return operation(request, store, load)
    finally:
        uuid.uuid4 = nonce

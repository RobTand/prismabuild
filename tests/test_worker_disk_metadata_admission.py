"""Refs #1008: the configured worker offer must reach real host admission.

The diagnostic assume-idle case is the primary old-source reproduction: a
parsed Namespace supplies the opt-in even before its CLI exists, so RED is a
real ``never_fits_capacity`` denial, not an unrecognized argument. The second
case keeps the actual observer and adaptive CPU policy, controlling only their
external facts. Neither case establishes deployed containment or disk isolation.

Only PoolQueue.execute is replaced at the action boundary. main, declaration,
live capacity, observation, bounded discovery/publication, full-demand claims,
token acquisition and finish all run normally against one private host ledger.
"""
from __future__ import annotations

import importlib.util
import json
import os
import socket
import threading
import time
from pathlib import Path

import pytest

from prismabuild import pool

REPO = Path(__file__).resolve().parents[1]
WORKER = REPO / "tools" / "fleet" / "worker_loop.py"
KIND = "disk_metadata"
DEMAND = {"cpu": 1, "mem_gb": 1, KIND: 1}
FIRST = "a" * 64
SECOND = "b" * 64
ORDINARY = "c" * 64
WAIT_S = 30.0


def _read(path):
    return json.loads(path.read_text())


def _denial(queue, key):
    base = pool.cpu_admission.local_state_base(queue.ledger().base)
    records = _read(base / pool.CLAIM_DENIALS)["records"]
    matches = [record for record in records.values()
               if record["action_key"] == key]
    assert len(matches) == 1, records
    return matches[0]


def _publish(queue, key, *, resources, priority):
    queue.publish(
        action_key=key, cas_root=queue.root.parent / "cas",
        worker_script=REPO / "tools" / "prismabuild_worker.py",
        checkout_root=queue.root.parent / "checkout", resources=resources,
        priority=priority, max_attempts=1, retry_safe=False,
    )


def _kernel_counters(cpus, *, now=None):
    now = time.time() if now is None else now
    return {"sampled_unix": now, "psi_total": 0,
            "cpus": {str(cpu): [0, int(now * 100)] for cpu in cpus}}


def _load_worker():
    spec = importlib.util.spec_from_file_location("metadata_worker_test", WORKER)
    assert spec is not None and spec.loader is not None
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    return worker


def _fixture_worker(monkeypatch, tmp_path, *, assume_idle, metadata_capacity=1):
    worker = _load_worker()

    # Paths are fixture inputs, not patched gate/runtime verdicts. The normal
    # readers see an explicitly open gate and matching immutable/live receipts.
    monkeypatch.setattr(worker, "SH", tmp_path)
    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": False}))
    monkeypatch.setattr(worker, "MAINTENANCE_GATE", gate)
    for attr, name in (("GENERATION_VERSION", "loaded-runtime.json"),
                       ("RUNTIME_VERSION", "published-runtime.json")):
        path = tmp_path / name
        path.write_text(json.dumps({"commit": "d" * 40,
                                    "generation": "metadata-fixture"}))
        monkeypatch.setattr(worker, attr, path)
    monkeypatch.setattr(worker, "PUBLICATION_LOCK_ROOT", tmp_path / "publication")
    monkeypatch.setattr(worker, "ROLE_LOCK_ROOT", tmp_path / "roles")
    monkeypatch.setattr(pool.cpu_admission, "BOX_STATE_ROOT", tmp_path / "box-state")

    # No affinity is widened or changed. Two IDs inside the inherited mask are
    # deterministic preferred topology facts; fallback placement is not this
    # resource regression. The admitting parent must reserve at least two CPUs.
    cpus = sorted(os.sched_getaffinity(0))[:2]
    assert len(cpus) == 2, "fixture requires two inherited PB-assigned CPUs"
    tiers = {"preferred": cpus, "fallback": []}
    monkeypatch.setattr(worker.cpu_topology, "inherited_tiers", lambda: tiers)
    monkeypatch.setattr(worker.box_capacity, "mem_available_gb", lambda: 64)
    monkeypatch.setattr(worker.box_capacity, "run_queue", lambda: 0.0)
    monkeypatch.setattr(pool.cpu_admission, "counters", _kernel_counters)
    monkeypatch.setattr(pool.cpu_admission, "control_plane_counters", lambda _cpus: {})

    # Approved ancillary seam: REAL cache, private root, deterministic empty
    # external inventory probe. No Docker and no default /tmp cache access.
    inventory_type = worker.container_images.InventoryCache
    monkeypatch.setattr(worker.container_images, "InventoryCache", lambda:
                        inventory_type(root=tmp_path / "images",
                                       probe=lambda _argv, **_kwargs: b""))

    parser = worker.build_parser()
    args = parser.parse_args([
        "--once", "--all-cores", "--cpu-slots", "2", "--mem-gb", "8",
        "--gpu-slots", "0", "--class", "metadata-fixture", "--poll-s", "0",
        "--observe-samples", "1", *(["--assume-idle"] if assume_idle else []),
    ])
    if metadata_capacity is not None:
        args.disk_metadata_capacity = metadata_capacity
    # Only the parsed-input seam is substituted. In particular do NOT patch
    # declared_host_capacity to insert the resource the old code is missing.
    monkeypatch.setattr(parser, "parse_args", lambda: args)
    monkeypatch.setattr(worker, "build_parser", lambda: parser)

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    if not assume_idle:
        # Seed the sampler's prior kernel counters, not a fabricated admission
        # verdict. Controller.sample computes and persists its own fresh delta;
        # missing attempt attribution earns no borrowing credit.
        base = pool.cpu_admission.local_state_base(queue.ledger().base)
        pool.cpu_admission.write_json(
            base / "cpu-sample.json", _kernel_counters(cpus, now=time.time() - 2))
    return worker, queue, args


@pytest.mark.parametrize("assume_idle", [True, False],
                         ids=["diagnostic-assume-idle-red", "observed-adaptive"])
def test_worker_disk_metadata_offer_serializes_tagged_actions_only(
    monkeypatch, tmp_path, assume_idle,
):
    worker, queue, _args = _fixture_worker(
        monkeypatch, tmp_path, assume_idle=assume_idle)
    host = socket.gethostname()
    ledger = queue.ledger(host)
    sibling = pool.PoolQueue(queue.root)
    assert sibling.ledger().base == ledger.base
    _publish(queue, FIRST, resources=DEMAND, priority=20)
    _publish(queue, SECOND, resources=DEMAND, priority=10)

    entered = threading.Event()
    release = threading.Event()
    worker_done = threading.Event()
    failures = []
    executions = []
    first_claim = []

    def execute(actual_queue, item, *, python, timeout_s, containment=False):
        try:
            assert containment is True  # keep the worker's production request
            key = item["action_key"]
            executions.append(key)
            assert item["resources"] == DEMAND
            assert item["claimed_host"] == host
            assert ledger.holder_tokens(key) == DEMAND
            assert ledger.held()[KIND] == 1
            assert ledger.available().get(KIND, 0) == 0
            if key == FIRST:
                first_claim.append(dict(item))
                entered.set()
                assert release.wait(WAIT_S), "sibling checks did not release execution"
            else:
                assert key == SECOND
                assert not queue.item_path(pool.CLAIMED, FIRST).exists()
                assert not (ledger.held_dir / FIRST).exists()
            # No payload, scope or Docker marker was created. This is admission
            # component evidence, not a broker waiver or containment proof.
            return {"action_key": key, "status": "executed", "returncode": 0}
        except BaseException as exc:
            failures.append(exc)
            raise

    monkeypatch.setattr(pool.PoolQueue, "execute", execute)

    def inspect_held_and_try_sibling():
        ordinary = None
        try:
            deadline = time.monotonic() + WAIT_S
            while not entered.wait(0.05):
                if worker_done.is_set():
                    return  # old code never entered execution; diagnose below
                assert time.monotonic() < deadline, "worker did not reach execution"

            offer = _read(queue.root / "workers" / f"{host}.json")
            for field in ("capacity", "observed_capacity"):
                assert offer[field][KIND] == 1
                assert offer[field]["cpu"] == 2
                assert offer[field]["mem_gb"] == 8
            if not assume_idle:
                assert offer["observed_detail"]["mem_roof_gib"] == 8
                metadata = _read(ledger.held_dir / FIRST / pool.cpu_admission.METADATA)
                assert metadata["declared_cpu"] == 1
                assert metadata["borrowing"] is False

            assert ledger.capacity() == {"cpu": 2, "mem_gb": 8, KIND: 1}
            assert ledger.held_keys() == [FIRST]
            assert ledger.held() == DEMAND
            assert ledger.available() == {"cpu": 1, "mem_gb": 7}
            claimed = _read(queue.item_path(pool.CLAIMED, FIRST))
            assert claimed["resources"] == DEMAND
            assert claimed["claimed_host"] == host
            claim_args = {"tags": offer["tags"],
                          "capacity": offer["observed_capacity"],
                          "cpu_tiers": offer["cpu_tiers"],
                          "adaptive_cpu": not assume_idle}
            assert sibling.claim(owner="metadata-sibling", **claim_args) is None
            denial = _denial(sibling, SECOND)
            assert denial["reason"].startswith("reservation_unavailable"), denial
            assert denial["evidence"]["demand"] == DEMAND
            assert denial["evidence"]["token_shortage"] == {
                "resource": KIND, "requested": 1, "available": 0}
            assert queue.item_path(pool.READY, SECOND).exists()
            assert not queue.item_path(pool.CLAIMED, SECOND).exists()
            assert ledger.holder_tokens(FIRST) == DEMAND

            # A new, higher-priority ordinary row is scanned normally before
            # the denied tagged row. It takes only spare CPU/memory: cooperation
            # among declarers is NOT isolation from ordinary same-host work.
            _publish(sibling, ORDINARY, resources={"cpu": 1, "mem_gb": 1}, priority=30)
            ordinary = sibling.claim(owner="ordinary-sibling", **claim_args)
            assert ordinary is not None and ordinary["action_key"] == ORDINARY
            assert ledger.holder_tokens(ORDINARY) == {"cpu": 1, "mem_gb": 1}
            assert ledger.held()[KIND] == 1
            assert ledger.available().get("cpu", 0) == 0
            sibling.finish(ORDINARY, status="executed", claim_snapshot=ordinary)
            ordinary = None
            assert _read(queue.item_path(pool.DONE, ORDINARY))["status"] == "executed"
            assert ledger.holder_tokens(FIRST) == DEMAND
            assert ledger.available() == {"cpu": 1, "mem_gb": 7}
        except BaseException as exc:
            failures.append(exc)
        finally:
            try:
                if ordinary is not None:
                    sibling.finish(ORDINARY, status="failed", claim_snapshot=ordinary)
            except BaseException as exc:
                failures.append(exc)
            release.set()

    checker = threading.Thread(target=inspect_held_and_try_sibling,
                               name="metadata-sibling-check", daemon=True)
    checker.start()
    try:
        # main stays in the main thread: its real signal registration/restoration
        # is not mocked. Its --once poll owns the actual first claim and finish.
        result = worker.main()
    finally:
        worker_done.set()
        release.set()
        checker.join(WAIT_S)
    assert not checker.is_alive(), "sibling fixture exceeded bounded join"
    if failures:
        raise failures[0]
    assert result == 0

    if not entered.is_set():
        # Establish the attributable old-code failure before emitting RED. Any
        # other refusal, publication miss or execution error fails separately.
        offer = _read(queue.root / "workers" / f"{host}.json")
        denials = {key: _denial(queue, key) for key in (FIRST, SECOND)}
        for denial in denials.values():
            assert denial["reason"] == "never_fits_capacity", denials
            assert denial["evidence"]["demand"] == DEMAND
            assert denial["evidence"]["reservation_demand"] == DEMAND
            assert denial["evidence"]["capacity_total"] == {"cpu": 2, "mem_gb": 8}
        assert KIND not in offer["capacity"]
        assert KIND not in offer["observed_capacity"]
        assert executions == []
        assert ledger.held_keys() == []
        assert all(queue.item_path(pool.READY, key).exists() for key in (FIRST, SECOND))
        pytest.fail(
            "worker.main never entered execution: both real claims denied "
            "never_fits_capacity for disk_metadata=1; persisted declared/live "
            f"offer omits the kind (cpu=2, mem_gb=8). Actual denials: {denials}"
        )

    # serve_once must actually finish FIRST before the other queue reacquires
    # the SAME metadata token for SECOND; no raw release or acquire helper.
    assert executions == [FIRST]
    terminal = _read(queue.item_path(pool.DONE, FIRST))
    assert terminal["status"] == "executed"
    assert terminal["claimed_host"] == host
    assert terminal["claimed_unix"] == first_claim[0]["claimed_unix"]
    assert not queue.lease_path(FIRST).exists()
    assert ledger.held_keys() == []
    assert ledger.available() == {"cpu": 2, "mem_gb": 8, KIND: 1}

    # A second actual main poll republishes and observes the same host shape,
    # then uses ordinary serve_once/finish to execute the remaining tagged row.
    assert worker.main() == 0
    if failures:
        raise failures[0]
    assert executions == [FIRST, SECOND]
    assert _read(queue.item_path(pool.DONE, SECOND))["status"] == "executed"
    assert not queue.item_path(pool.CLAIMED, SECOND).exists()
    assert not queue.lease_path(SECOND).exists()
    assert ledger.held_keys() == []
    assert ledger.capacity() == {"cpu": 2, "mem_gb": 8, KIND: 1}
    assert ledger.available() == ledger.capacity()
    offer = _read(queue.root / "workers" / f"{host}.json")
    assert offer["capacity"][KIND] == offer["observed_capacity"][KIND] == 1


@pytest.mark.parametrize("argv, expected", [([], 0),
    (["--disk-metadata-capacity", "0"], 0),
    (["--disk-metadata-capacity", "1"], 1)])
def test_worker_metadata_capacity_cli_and_declaration(argv, expected):
    worker = _load_worker()
    parser = worker.build_parser()
    args = parser.parse_args(argv)
    worker.validate_args(parser, args)
    assert args.disk_metadata_capacity == expected
    assert worker.declared_host_capacity(args, cores=2) == {
        "cpu": 2, "mem_gb": 96, KIND: expected}


@pytest.mark.parametrize("value", ["-1", "2", "1.5", "x", ""])
def test_worker_metadata_capacity_cli_refuses_invalid_values(value, capsys):
    worker = _load_worker()
    with pytest.raises(SystemExit) as error:
        worker.build_parser().parse_args(["--disk-metadata-capacity", value])
    assert error.value.code == 2
    assert "--disk-metadata-capacity" in capsys.readouterr().err


@pytest.mark.parametrize("value", [-1, 2, 0.5, 1.0, True, "1", None])
def test_worker_metadata_capacity_namespace_validation(value, capsys):
    # The integrated regression supplies a parsed Namespace. Keep this seam
    # fail-closed too: int 0/1 only, not equality-compatible floats or bools.
    worker = _load_worker()
    parser = worker.build_parser()
    args = parser.parse_args([])
    args.disk_metadata_capacity = value
    with pytest.raises(SystemExit) as error:
        worker.validate_args(parser, args)
    assert error.value.code == 2
    assert "--disk-metadata-capacity" in capsys.readouterr().err


def _assert_disabled_offer(queue):
    offer = _read(queue.root / "workers" / f"{socket.gethostname()}.json")
    assert offer["capacity"][KIND] == 0
    assert offer["observed_capacity"][KIND] == 0
    assert queue.placeable({"tags": [], "resources": DEMAND}) is False


@pytest.mark.parametrize("capacity", [None, 0], ids=["default", "explicit-zero"])
def test_disabled_worker_admits_ordinary_and_refuses_tagged_work(
    monkeypatch, tmp_path, capacity,
):
    worker, queue, _args = _fixture_worker(
        monkeypatch, tmp_path, assume_idle=False, metadata_capacity=capacity)
    ledger = queue.ledger()
    executions, failures = [], []
    _publish(queue, ORDINARY, resources={"cpu": 1, "mem_gb": 1}, priority=20)
    _publish(queue, SECOND, resources=DEMAND, priority=10)

    def execute(actual_queue, item, *, python, timeout_s, containment=False):
        try:
            assert containment is True
            assert item["action_key"] == ORDINARY
            executions.append(item["action_key"])
            assert ledger.holder_tokens(ORDINARY) == {"cpu": 1, "mem_gb": 1}
            assert ledger.capacity().get(KIND, 0) == 0
            _assert_disabled_offer(queue)
            return {"status": "executed", "action_key": ORDINARY, "returncode": 0}
        except BaseException as exc:
            failures.append(exc)
            raise

    monkeypatch.setattr(pool.PoolQueue, "execute", execute)
    assert worker.main() == 0
    if failures:
        raise failures[0]
    assert executions == [ORDINARY]
    assert _read(queue.item_path(pool.DONE, ORDINARY))["status"] == "executed"
    assert worker.main() == 0
    _assert_disabled_offer(queue)
    assert _denial(queue, SECOND)["reason"] == "never_fits_capacity"
    assert queue.item_path(pool.READY, SECOND).exists()
    assert ledger.held_keys() == []
    assert ledger.capacity() == ledger.available() == {"cpu": 2, "mem_gb": 8}
    assert not queue.lease_path(ORDINARY).exists()


@pytest.mark.parametrize("held_on_disable", [False, True],
                         ids=["previously-free", "running-holder"])
def test_worker_metadata_opt_out_retires_free_not_held_and_can_reenable(
    monkeypatch, tmp_path, held_on_disable,
):
    worker, queue, args = _fixture_worker(monkeypatch, tmp_path, assume_idle=False)
    ledger = queue.ledger()
    executions, failures = [], []
    _publish(queue, FIRST, resources=DEMAND, priority=20)

    def execute(actual_queue, item, *, python, timeout_s, containment=False):
        try:
            key = item["action_key"]
            executions.append(key)
            assert containment is True
            if key == ORDINARY:
                _assert_disabled_offer(queue)
                assert ledger.holder_tokens(ORDINARY) == {"cpu": 1, "mem_gb": 1}
                if held_on_disable:
                    assert ledger.holder_tokens(FIRST) == DEMAND
                    assert ledger.capacity()[KIND] == 1
                    assert ledger.available().get(KIND, 0) == 0
                else:
                    assert ledger.capacity().get(KIND, 0) == 0
            else:
                assert key in (FIRST, SECOND)
                assert ledger.holder_tokens(key) == DEMAND
                assert ledger.capacity()[KIND] == 1
                if key == FIRST and held_on_disable:
                    # A real sibling main poll (nested on the main thread so
                    # signal handling is real) offers zero while FIRST runs.
                    # It may retire only FREE tokens, then admit ordinary work.
                    args.disk_metadata_capacity = 0
                    _publish(queue, ORDINARY, resources={"cpu": 1, "mem_gb": 1},
                             priority=30)
                    _publish(queue, SECOND, resources=DEMAND, priority=10)
                    assert worker.main() == 0
                    assert worker.main() == 0  # actual tagged sibling denial
                    _assert_disabled_offer(queue)
                    assert ledger.holder_tokens(FIRST) == DEMAND
                    assert ledger.held_keys() == [FIRST]
                    denial = _denial(queue, SECOND)
                    assert denial["reason"].startswith("reservation_unavailable"), denial
                    assert denial["evidence"]["token_shortage"] == {
                        "resource": KIND, "requested": 1, "available": 0}
            return {"status": "executed", "action_key": key, "returncode": 0}
        except BaseException as exc:
            failures.append(exc)
            raise

    monkeypatch.setattr(pool.PoolQueue, "execute", execute)
    assert worker.main() == 0
    if failures:
        raise failures[0]
    assert ledger.held_keys() == []
    assert _read(queue.item_path(pool.DONE, FIRST))["status"] == "executed"
    # Real finish returned the token even if a sibling offered zero while it
    # was held. The next disabled poll, not a raw release/resize helper, removes it.
    assert ledger.capacity() == ledger.available() == {"cpu": 2, "mem_gb": 8, KIND: 1}
    args.disk_metadata_capacity = 0
    if not held_on_disable:
        _publish(queue, ORDINARY, resources={"cpu": 1, "mem_gb": 1}, priority=30)
        _publish(queue, SECOND, resources=DEMAND, priority=10)
        assert worker.main() == 0
        if failures:
            raise failures[0]
    assert worker.main() == 0
    _assert_disabled_offer(queue)
    assert _denial(queue, SECOND)["reason"] == "never_fits_capacity"
    assert queue.item_path(pool.READY, SECOND).exists()
    assert ledger.capacity() == ledger.available() == {"cpu": 2, "mem_gb": 8}
    assert ledger.held_keys() == []

    args.disk_metadata_capacity = 1
    assert worker.main() == 0
    if failures:
        raise failures[0]
    assert executions == [FIRST, ORDINARY, SECOND]
    assert _read(queue.item_path(pool.DONE, SECOND))["status"] == "executed"
    assert not queue.item_path(pool.READY, SECOND).exists()
    assert ledger.held_keys() == []
    assert ledger.capacity() == ledger.available() == {"cpu": 2, "mem_gb": 8, KIND: 1}
    assert all(not queue.lease_path(key).exists() for key in (FIRST, ORDINARY, SECOND))

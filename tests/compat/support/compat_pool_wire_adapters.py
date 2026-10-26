"""Private queue writers with their ordering, heartbeat and immutable-log readers."""
from __future__ import annotations

import base64
import inspect
import json
from pathlib import Path
import runpy

C, OWNER, TIER = "c" * 64, "compat-owner", "prismabuild-stage:compat"


def borrowed_claim_record(queue, core, pool, store, record):
    """Current queue writers retain an actual cap-zero legacy loan."""
    cpu = pool.cpu_admission
    tiers = {'preferred': list(range(4)), 'fallback': list(range(4, 8))}
    ledger = queue.ledger('compat-host')
    ledger.ensure_capacity({'cpu': 8, 'mem_gb': 8})
    ledger.configure_cpu_tiers(tiers)
    clock = [97.]
    cpu.time.time = lambda: clock[0]
    def sample():
        return {'sampled_unix': clock[0], 'cpu_count': 8, 'interval_s': 1.,
                'busy_cpus': .4, 'psi_some': 0., 'psi_avg10': 0., 'psi_avg60': 0.,
                'per_cpu_busy': {str(c): .1 if c < 4 else 0. for c in range(8)}}
    controller = cpu.Controller(ledger, tiers, sampler=sample)
    body = runpy.run_path(str(Path(__file__).with_name('compat_core_adapters.py')))['action_body'](core, store)
    donor = core.seal_action({**body, 'params': {**body['params'], 'loan_fixture': 'donor'}})
    cas = core.PrismaBuildCAS(store / 'cas')
    cas.publish_action_request(donor)
    item = {'action_key': donor['action_key'], 'cas_root': str(cas.root), 'resources': {'cpu': 4}}
    with controller.locked():
        metadata = controller.decision(item, item['resources'], owner=None)
        handle = ledger.begin_acquire(item['action_key'], item['resources'], adaptive=metadata, cpu_tiers=tiers)
        assert handle and ledger.commit_acquire(item['action_key'], handle)
    for at in (98., 99., 100.):
        clock[0] = at
        cpu.write_json(controller.base / 'telemetry' / (item['action_key'] + '.json'), {
            'action_key': item['action_key'], 'nonce': 'e' * 32, 'complete': True,
            'sampled_unix': at, 'wall_seconds': at - 97., 'cpu_seconds': (at - 97.) * .1})
        if at < 100.:
            with controller.locked():
                controller._host_sample = None
                controller.decision(record, record['resources'], owner=None)
    claimed = queue.claim(owner=OWNER, capacity={'cpu': 8, 'mem_gb': 8}, cpu_tiers=tiers,
                          adaptive_cpu=True, cpu_sampler=sample)
    assert claimed and claimed['action_key'] == record['action_key']
    assert claimed['cpu_admission']['borrowed_cpus'] == [0]
    return claimed


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    if family == "q-outcome" and variant.endswith("writer"):
        return runpy.run_path(str(Path(__file__).with_name("compat_terminal_adapters.py")))["operate"](request, store, load)
    pool, core = load("prismabuild.pool"), load("prismabuild.core")
    queue = pool.PoolQueue(store / "queue")
    queue.records.ensure_layout()
    write = request["op"] == "write"

    def publish():
        body = runpy.run_path(str(Path(__file__).with_name("compat_core_adapters.py")))["action_body"]
        action = core.seal_action(body(core, store))
        cas = core.PrismaBuildCAS(store / "cas")
        cas.publish_action_request(action)
        kw = {"max_attempts": 2, "retry_safe": True} if variant == "retry-safe" else {"max_attempts": 1, "retry_safe": False}
        if variant == "tagged":
            kw.update(tags=["cpu"], priority=-10, priority_reason="compat", interpreter="/usr/bin/python3")
        if variant == "repository":
            kw["tested_repository"] = "compat-repository"
        path = queue.publish(action_key=action["action_key"], cas_root=cas.root,
                             checkout_root="/compat/checkout", worker_script="/compat/worker.py",
                             resources={"cpu": 1, "mem_gb": 1}, **kw)
        return path, json.loads(path.read_bytes())

    if family == "q-host-hold":
        holds = load("prismabuild.host_hold")
        path = holds.host_hold_path(queue.root, "compat-host")
        if write:
            holds.change_host_hold(queue.root, "compat-host", owner="compat", reason="load",
                                   now=100., expires_unix=200.)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            value = holds.read_host_hold(queue.root, "compat-host", now=300. if variant == "expired" else 150.)
            assert value is not None and value.error is None, "actual hold consumer refused"
            assert value.expired is (variant == "expired")
            return value.fields()
    elif family == "q-item":
        if write:
            path, record = publish()
        else:
            record = json.loads(base64.b64decode(request["raw"]))
            # The action fixture determines the key independently of incoming bytes.
            body = runpy.run_path(str(Path(__file__).with_name("compat_core_adapters.py")))["action_body"]
            key = core.seal_action(body(core, store))["action_key"]
            request["_consumer_boundary"]()
            assert queue._ready_record_usable(record, key), "actual queue usability consumer refused the publication"
            return record
    elif family == "q-offer":
        path = queue.root / pool.WORKERS / "compat-host.json"
        if write:
            kw = {}
            if variant in ("observed", "capabilities", "draining"):
                kw.update(observed_capacity={"cpu": 2}, foreign={"cpu": 1}, observed_detail={"note": "compat"},
                          cpu_tiers={"preferred": [0], "fallback": []}, loops=1, timeout_ceiling_s=60.0,
                          progress_contracts=["prismabuild.action_progress.v1"], addresses=["192.0.2.1"])
            if variant == "capabilities":
                answers = {"observed_images": [], "interpreters": ["/usr/bin/python3"],
                           "interpreters_absent": ["/compat/missing"], "local_dependency_answers": {},
                           "dependency_files": ["/compat/input"], "dependency_files_absent": ["/compat/absent"]}
                if "capabilities" in inspect.signature(queue.announce).parameters:
                    wire = {**answers, "container_images": answers["observed_images"],
                            "local_dependencies": answers["local_dependency_answers"]}
                    kw["capabilities"] = pool.capability_eligibility.OfferCapabilities.from_offer(wire)
                else:
                    kw.update(answers)
            if variant == "draining":
                kw.update(state="live", drain_owner="compat", drain_reason="qualification", drain_changed_unix=99.0)
            queue.announce(host="compat-host", tags=["cpu"], has_gpu=False, capacity={"cpu": 2, "mem_gb": 2},
                           runtime_commit="1" * 40, **kw)
    elif family == "q-intent":
        path = queue.records.item_path(pool.INTENT, C)
        if write:
            queue._write_claim_intent(C, owner=OWNER)
    elif family == "q-lease":
        path = queue.records.lease_path(C)
        queue.records.replace_record_at(queue.records.item_path(pool.CLAIMED, C),
                                {"action_key": C, "claimed_by": OWNER, "claimed_host": "compat-host"})
        if write:
            kw = {} if variant == "idle" else {"child_pid": 2}
            if variant == "observations":
                kw.update(container_owner="compat-container", execution_observation={"running": True},
                          progress_observation={"phase": "compat"})
            queue.write_lease(C, owner=OWNER, **kw)
    elif family == "q-attempt":
        _, record = publish()
        if write and variant == "executed":
            record = borrowed_claim_record(queue, core, pool, store, record)
        record.update(claimed_by=OWNER, claimed_host="compat-host", claimed_unix=100.0,
                      finished_unix=100.0, finished_host="compat-host")
        status = "executed" if variant == "legacy-retry" else variant
        if variant == "legacy-retry":
            record.pop("retry_safe", None)
        disposition = pool.DONE if status in ("executed", "cache_hit") else pool.FAILED
        links = queue.archive_attempt(record, attempt=1, status=status, disposition=disposition,
                                      detail={"stdout": "compat \u03bb\n", "stderr": "", "returncode": 0 if disposition == pool.DONE else 1})
        record.update(attempts=1, attempt_history=links)
        path = queue.records.attempt_path(record, 1)
    elif family == "q-outcome":
        _, record = publish()
        key = record["action_key"]
        if write:
            if variant in ("withdrawn", "preempted"):
                queue.withdraw(key, reason="compat", by="compat",
                               preempted_by=C if variant == "preempted" else None, signal_child=False)
                path = queue.records.item_path(pool.WITHDRAWN, key)
            else:
                if variant == "executed":
                    record = borrowed_claim_record(queue, core, pool, store, record)
                else:
                    # Older unscoped cases remain unchanged and need no broker.
                    queue.records.item_path(pool.READY, key).unlink()
                    record.update(claimed_by=OWNER, claimed_host="compat-host", claimed_unix=100.0)
                    queue.records.replace_record_at(queue.records.item_path(pool.CLAIMED, key), record)
                path = queue.finish(key, status=variant, detail={"returncode": 0 if variant != "failed" else 1,
                                                               "stdout": "compat", "stderr": ""})
        else:
            path = queue.records.item_path(pool.WITHDRAWN if variant in ("withdrawn", "preempted") else
                                   pool.FAILED if variant == "failed" else pool.DONE, key)
    elif family == "q-tier-commitment":
        path = queue.records.tier_commitment_path(TIER)
        if write:
            queue.file_tier_commitment({"tier_id": TIER, "waiting": [C], "admitted": []})
    elif family == "q-stage-holder":
        stage = store / "stage"
        stage.mkdir()
        path = queue.records.stage_ownership_holder_path(stage)
        if write:
            with queue.stage_ownership_lock(str(stage)):
                queue.write_stage_ownership_holder(stage, role="compat", action_key=C if variant == "action" else None)
    else:
        raise ValueError(f"unknown pool wire adapter: {family}")
    if write:
        return {"raw": base64.b64encode(path.read_bytes()).decode()}
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.chmod(0o644)
    path.write_bytes(base64.b64decode(request["raw"]))
    request["_consumer_boundary"]()
    if family == "q-offer":
        item = {"tags": ["cpu"], "resources": {"cpu": 1, "mem_gb": 1}}
        if variant == "capabilities":
            item.update(interpreter="/usr/bin/python3", requires_files=[{"path": "/compat/input", "sha256": "a" * 64}])
        result = queue._matching_offers(item, live=queue.offers())
        assert len(result) == 1 and result[0]["host"] == "compat-host", "offer did not remain discoverable"
    elif family == "q-intent":
        result = queue.claim_intent_host(C, {"published_unix": 99.0})
        assert result == "compat-host", "intent did not bind the claimant's host"
    elif family == "q-lease":
        result = queue.lease_age(C)
        assert result == 0.0, "heartbeat did not govern lease freshness"
    elif family == "q-attempt":
        path.chmod(0o444)
        result = queue.attempt_outcomes(record)
        assert len(result) == 1 and result[0]["stdout"] == "compat \u03bb\n", "attempt did not verify its actual log bytes"
    elif family == "q-outcome":
        reader = runpy.run_path(str(Path(__file__).with_name("compat_terminal_adapters.py")))["terminal_reader"](load)
        result = reader(path, 100.0, report_unreadable=True)
        assert result and result["action_key"] == key and result["status"] == ("withdrawn" if variant == "preempted" else variant)
    elif family == "q-tier-commitment":
        result = queue.tier_commitment(TIER)
        assert result and result["waiting"] == [C], "commitment consumer lost its waiting action"
    else:
        result = queue.stage_ownership_holder(stage)
        assert result["recorded"] and result["role"] == "compat", "holder consumer refused the record"
    return result

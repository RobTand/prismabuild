"""Operational prepaid writer integration: the real sequence, tiny files (R7).

Exercises the production call path end to end on real fixtures:

* `admit_funded_window` reports the delivered prepaid binding (declaration
  only; per-batch funding is the authority);
* `publish_prepaid_batch` recovers the movement template from the
  producer's OWN sealed request (content-addressed owner) and seals the
  mover through the ORDINARY movement construction
  (`movement_actions.seal_movement_action`),
  resolves interpreter/tools/stage-root/placement off the announced TIER
  RECORD, seals the batch data manifest as the request's input, then runs
  stage intent -> publish -> fund from the EXISTING window -> `commit_batch`
  (no second acquisition), with free credits exhausted;
* the sealed request's OWN argv executes under the real local action
  runner (derived mover identity, real copy/verify/fragment by the fleet
  tool), and the recorded receipt retires the batch through the real
  egress;
* a second sequential batch funds from disjoint window names and the
  retired capacity returns (retire -> reclaim -> owner finish);
* a restart at the transfer boundary (fund durable, commit not yet filed)
  recovers through the same public APIs;
* committed-but-unclaimed funding survives `release_output_funding`;
  `abort_prewrite` refuses while a pool intent cites the prewrite and
  succeeds after the intent is retired; staged intents never select the
  same credit name twice.

Fixture concessions (dev scope, all disclosed): the producer request is
a real sealed CAS request over a fixture checkout (production's comes
from the pbrun/PQ submission machinery); `--unpaced` is passed explicitly as
fixture behavior via `command_extra` (production seals the ordinary paced
command); the argv executes through `pb.run_local_action` in a subprocess
whose environment drops the outer launcher's reader-identity bundle -- a
real storage-owner box carries no foreign tuple, and the production
identity check itself is untouched; the claim is `PoolQueue.claim`
directly rather than a fleet worker loop; `safe_release_instance` is only
shown retaining (its full path needs the accepted reader_lease SDK).
"""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import prismabuild.core as pb  # noqa: E402
import prismabuild.pool as pool  # noqa: E402
import prismabuild.produced_output as po  # noqa: E402
import stage_release  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
KIND = "stage_gib"
REPO = Path(__file__).resolve().parents[1]


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _template(prefix: str) -> dict:
    body = {
        "schema": po.TEMPLATE_SCHEMA_V1,
        "version": 1,
        "template_id": "prepaid-integ-v1",
        "output_prefix": prefix,
        "slots": {"s0": {"class": "payload"}, "s1": {"class": "checkpoint"}},
        "durable_maxima": {
            "payload_max_bytes": 1 << 20,
            "checkpoint_max_bytes": 1 << 20,
            "temp_max_bytes": 1 << 20,
        },
        "working_demands": {TIER: {"minimum_gib": 1, "window_gib": 2}},
        "permitted_tiers": [TIER],
    }
    return po.validate_template(body)


def _queue(tmp_path: Path, gib: int = 4) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {KIND: gib})
    return q


def _broker_control(q: pool.PoolQueue, owner: str) -> dict:
    from prismabuild import resource_scope
    nonce = secrets.token_hex(16)
    scope = resource_scope.ResourceScope(
        owner, nonce, 1 * 1024 ** 3,
        q.root / "telemetry" / f"{owner}.json")
    token = secrets.token_hex(32)
    unit = ("prismabuild-job"
            + hashlib.sha256((owner + nonce).encode()).hexdigest()[:32]
            + ".slice")
    scope._adopt_created_scope({
        "scope_id": unit, "token": token,
        "cgroup_path": str(Path("/sys/fs/cgroup/prismabuild.slice") / unit),
    })
    control = scope.control_record()
    path = q.item_path(pool.CLAIMED, owner)
    live = pool._read_json(path)
    assert live is not None
    live["resource_scope"] = control
    pool._write_json_atomic(path, live)
    return control


def _bind(q: pool.PoolQueue, template: dict, owner: str):
    terms = po.owner_demand_terms(template)
    q.publish(action_key=owner, cas_root="/cas", worker_script="/w.py",
              checkout_root="/co", resources={"cpu": 1, "mem_gb": 1, **terms},
              produced_output_template=template)
    claimed = q.claim(owner="w-owner")
    assert claimed is not None and claimed["action_key"] == owner
    control = _broker_control(q, owner)
    env = {"PRISMABUILD_ACTION_KEY": owner,
           "PRISMABUILD_ACTION_NONCE": control["nonce"],
           "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
    po.declare_template(q.root, template)
    inst = po.bind_instance(q, template, owner_action_key=owner,
                            claim_snapshot=claimed, env=env)
    po.declare_instance(q.root, inst)
    assert po.admit_instance(q, inst, template)["ok"] is True
    return inst


def _descriptors(tmp_path: Path, template: dict, inst: dict, tag: str,
                  payload: bytes) -> list[dict]:
    origin = Path(template["output_prefix"])
    origin.mkdir(parents=True, exist_ok=True)
    path = origin / f"{tag}.bin"
    path.write_bytes(payload)
    slot = "s0" if tag.startswith("p") else "s1"
    cls = "payload" if slot == "s0" else "checkpoint"
    return [po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": slot,
        "artifact_class": cls, "path": str(path),
        "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest(),
        "producer_generation": po.mint_generation(),
        "owner_action_key": inst["owner_action_key"],
        "owner_attempt": dict(inst["owner_attempt"]),
    }, template, inst)]


def _prewrite(q, inst, template, batch_id, tier, descs):
    classes = {"payload": 0, "checkpoint": 0, "temp": 0}
    for d in descs:
        classes[str(d["artifact_class"])] += int(d["bytes"])
    out = po.require_prewrite(q, inst, template, batch_id=batch_id,
                              tier=tier, class_bytes=classes,
                              paths=sorted(str(d["path"]) for d in descs))
    assert out.get("ok") is True, out
    return out


def _announce_tier(q: pool.PoolQueue, stage_root: Path) -> None:
    """File the tier record the storage role announces (tier_loop shape).

    The driver resolves the mover's interpreter, tools, stage root and
    placement host ONLY from this record -- the ordinary movement-node
    path -- so the test announces the same facts tier_loop.py would: the
    box's own interpreter, the checkout's fleet tools, a stage root
    registered to this queue (#628 ownership, unchanged).
    """
    import socket
    stage_root.mkdir(parents=True, exist_ok=True)
    assert stage_release.register_stage_root(
        q, tier_id=TIER, stage_root=stage_root) == "registered"
    record = {
        "tier": "stage",
        "tier_id": TIER,
        "host": socket.gethostname(),
        "mountpoint": str(stage_root),
        "mover_python": sys.executable,
        "mover_tools_root": str(REPO / "tools" / "fleet"),
    }
    path = Path(q.root) / "tiers" / f"{TIER}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    pool._write_json_atomic(path, record)


def _producer_request(tmp_path: Path, cas_root: Path) -> str:
    """Seal and file the PRODUCER's own request; return its action key.

    The driver recovers the movement template from exactly this request at
    runtime (inputs/closure/environment/scope/task/cwd), so the fixture
    owner IS the parent request's content-addressed key, as in production.
    The closure covers a fixture checkout carrying the fleet tools -- the
    child mover inherits it and executes against the same checkout.
    """
    checkout = tmp_path / "mover-checkout"
    tools = checkout / "tools" / "fleet"
    tools.mkdir(parents=True, exist_ok=True)
    for name in ("stage_move.py", "prewarm_loop.py", "stage_release.py"):
        (tools / name).write_bytes((REPO / "tools" / "fleet" / name)
                                   .read_bytes())
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/produced-prepaid-producer",
                 "definition_version": "v1", "task_class": "generation",
                 "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": ["/bin/true"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [],
        "code_closure": pb.build_code_closure(
            checkout, ["tools/fleet/stage_move.py",
                       "tools/fleet/prewarm_loop.py",
                       "tools/fleet/stage_release.py"]),
        "params": {"cwd": "."},
        "environment": {"variables": {"PATH": "/usr/bin:/bin"},
                        "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    }
    action = pb.seal_action(body)
    pb.PrismaBuildCAS(cas_root).publish_action_request(action)
    return str(action["action_key"])


def _execute_mover(q: pool.PoolQueue, cas_root: Path, mover: str,
                   checkout: Path) -> dict:
    """Execute the SEALED request argv through the real local executor.

    The subprocess drops the outer launcher's reader-identity bundle (a
    real storage-owner box carries no foreign tuple): the production
    identity check stays exactly as deployed, and the runner derives the
    mover's own identity from the action in hand.
    """
    runner = Path(q.root) / "run-local-action.py"
    runner.write_text(
        "import json, sys\n"
        "from pathlib import Path\n"
        "from prismabuild import core as pb\n"
        "req, cas, root = sys.argv[1], sys.argv[2], sys.argv[3]\n"
        "out = pb.run_local_action(json.loads(Path(req).read_text()),\n"
        "                          cas_root=cas, checkout_root=root,\n"
        "                          timeout_seconds=180)\n"
        "print(json.dumps(sorted(out)))\n")
    request_path = Path(cas_root) / "requests" / mover[:2] / f"{mover}.json"
    scrub = {k: v for k, v in os.environ.items()
             if k not in (pb.ACTION_NONCE_ENV, pb.ACTION_SCOPE_ENV,
                          pb.READER_HELPER_ROOT_ENV, pb.ACTION_KEY_ENV)}
    scrub["PYTHONPATH"] = str(REPO / "src")
    done = subprocess.run(
        [sys.executable, str(runner), str(request_path),
         str(cas_root), str(checkout)],
        env=scrub, capture_output=True, text=True, timeout=240)
    assert done.returncode == 0, done.stdout + done.stderr
    receipt = q.move_record(mover)
    assert isinstance(receipt, dict), "mover recorded no receipt"
    return receipt


def test_admit_funded_window_reports_delivered_binding(tmp_path: Path) -> None:
    q = _queue(tmp_path)
    template = _template(str(tmp_path / "outputs"))
    owner = _hexkey("adm-owner")
    bound = _bind(q, template, owner)
    out = po.admit_funded_window(q, bound, template, need_gib_per_tier={TIER: 2})
    assert out.get("ok") is True, out
    assert out.get("mode") == "prepaid-per-batch"
    assert out.get("declaration_only") is True
    assert out.get("authority") == "per-batch-funding"
    assert out.get("owner_demand_terms") == po.owner_demand_terms(template)
    assert out.get("batch_ref_schema") == pool.PRODUCED_OUTPUT_BATCH_REF_SCHEMA_V1


def test_prepaid_writer_end_to_end_with_real_mover(tmp_path: Path) -> None:
    """One batch through the production path, free credits exhausted."""
    cas_root = tmp_path / "cas"
    owner = _producer_request(tmp_path, cas_root)
    q = _queue(tmp_path, gib=4)
    ledger = q.tier_ledger(TIER)
    template = _template(str(tmp_path / "outputs"))
    inst = _bind(q, template, owner)
    payload = bytes(range(256)) * 8  # 2048 real bytes
    descs = _descriptors(tmp_path, template, inst, "p1", payload)
    _prewrite(q, inst, template, "b1", TIER, descs)
    cas_root = tmp_path / "cas"
    stage_root = tmp_path / "stage"
    _announce_tier(q, stage_root)
    publish = dict(producer_action_key=owner,
                   command_extra=["--unpaced"])

    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, **publish)
    assert res.get("ok") is True, res
    assert res.get("funding") == "prepaid"
    mover = str(res["mover_key"])
    # Retry with identical inputs re-derives the same sealed key and every
    # step answers a typed duplicate: a restart may simply re-call.
    retry = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, **publish)
    assert retry.get("ok") is True, retry
    assert str(retry["mover_key"]) == mover
    # No second acquisition: the producer's window moved, free untouched.
    assert ledger.holder_tokens(owner).get(KIND, 0) == 1
    assert ledger.holder_tokens(mover).get(KIND, 0) == 1
    free_after_fund = ledger.available().get(KIND, 0)
    assert free_after_fund == 2

    # Exhaust free credits: only the prepaid cover can admit the mover.
    squatter = _hexkey("integ-squatter")
    assert ledger.acquire(squatter, {KIND: free_after_fund}) is True
    assert ledger.available().get(KIND, 0) == 0

    claimed = q.claim(owner="w-integ-mover")
    assert claimed is not None and claimed["action_key"] == mover
    entry = (claimed.get("tier_funding") or {}).get(TIER)
    assert isinstance(entry, dict) and entry.get("variant") == "output"
    rec = q.read_output_funding(mover, TIER)
    assert rec is not None and rec["state"] == "consumed"

    # The REAL mover: the sealed request's own argv executes; the fleet
    # tool copies, verifies and files its fragment under the batch
    # namespace, and records its own receipt.
    receipt = _execute_mover(q, cas_root, mover,
                             tmp_path / "mover-checkout")
    assert receipt["complete"] is True, receipt
    total = sum(int(d["bytes"]) for d in descs)
    assert receipt["bytes_staged"] == total
    staged = Path(stage_root) / "p1.bin"
    assert staged.read_bytes() == payload
    q.finish(mover, status="executed")

    retired = po.retire_batch(q, inst, template, "b1",
                              stage_root=str(stage_root),
                              residency_root=po.output_fragment_root(
                                  q.root / pool.RESIDENCY))
    assert retired.get("ok") is True, retired
    # The egress returned the fence: usable capacity is back.
    assert ledger.holder_tokens(mover).get(KIND, 0) == 0
    assert ledger.available().get(KIND, 0) == 1


def test_second_batch_window_reuse_and_cleanup(tmp_path: Path) -> None:
    """A second sequential batch funds from disjoint names; capacity returns."""
    cas_root = tmp_path / "cas"
    owner = _producer_request(tmp_path, cas_root)
    q = _queue(tmp_path, gib=4)
    ledger = q.tier_ledger(TIER)
    template = _template(str(tmp_path / "outputs"))
    inst = _bind(q, template, owner)
    stage_root = tmp_path / "stage"
    _announce_tier(q, stage_root)
    results = []
    for tag, batch_id in (("p1", "b1"), ("p2", "b2")):
        payload = (b"a" * 700) if tag == "p1" else (b"b" * 900)
        descs = _descriptors(tmp_path, template, inst, tag, payload)
        _prewrite(q, inst, template, batch_id, TIER, descs)
        res = po.publish_prepaid_batch(
            q, inst, template, descs, batch_id=batch_id, tier=TIER,
            cas_root=cas_root, producer_action_key=owner,
            command_extra=["--unpaced"])
        assert res.get("ok") is True, res
        mover = str(res["mover_key"])
        # Sequential use: claim, execute the sealed argv, finish, retire.
        claimed = q.claim(owner=f"w-{tag}")
        assert claimed is not None and claimed["action_key"] == mover
        receipt = _execute_mover(q, cas_root, mover,
                                 tmp_path / "mover-checkout")
        assert receipt["complete"] is True, receipt
        q.finish(mover, status="executed")
        assert po.retire_batch(
            q, inst, template, batch_id, stage_root=str(stage_root),
            residency_root=po.output_fragment_root(
                q.root / pool.RESIDENCY))["ok"] is True
        results.append(res)

    # Disjoint credit names: the second batch never reused the first's.
    first = q.read_output_funding(str(results[0]["mover_key"]), TIER)
    second = q.read_output_funding(str(results[1]["mover_key"]), TIER)
    assert set(first["tokens"]).isdisjoint(set(second["tokens"]))

    # Cleanup returns usable capacity: dispose origins, reclaim, finish.
    for tag in ("p1", "p2"):
        (Path(template["output_prefix"]) / f"{tag}.bin").unlink()
    for batch_id in ("b1", "b2"):
        assert po.reclaim_origin(q, inst, template,
                                 batch_id=batch_id)["ok"] is True
    q.finish(owner, status="executed")
    assert ledger.holder_tokens(owner).get(KIND, 0) == 0
    assert ledger.available().get(KIND, 0) == ledger.capacity().get(KIND, 0)


def test_restart_at_transfer_boundary_recovers(tmp_path: Path) -> None:
    """Fund durable, crash before commit; restart recovers via public APIs."""
    owner = _hexkey("restart-owner")
    q = _queue(tmp_path, gib=4)
    ledger = q.tier_ledger(TIER)
    template = _template(str(tmp_path / "outputs"))
    inst = _bind(q, template, owner)
    payload = b"c" * 512
    descs = _descriptors(tmp_path, template, inst, "p1", payload)
    _prewrite(q, inst, template, "b1", TIER, descs)
    _announce_tier(q, tmp_path / "stage")
    ref = pool.PoolQueue.build_produced_output_batch_ref(
        instance=inst, template=template, batch_id="b1",
        descriptors=descs, tier_id=TIER)
    manifest = str(ref["manifest_digest"])
    total = int(ref["range_end_bytes"])
    checkout = tmp_path / "co-restart"
    checkout.mkdir(parents=True, exist_ok=True)
    (checkout / "task.py").write_text("print('restart-mover')\n")
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/restart", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"],
                 "working_directory": ".", "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(
            checkout, ["task.py"]),
        "params": {"produced_output_batch": dict(ref)},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    }
    action = pb.seal_action(body)
    cas_root = tmp_path / "cas-restart"
    pb.PrismaBuildCAS(cas_root).publish_action_request(action)
    mover = str(action["action_key"])
    assert q.stage_output_intent(
        tier_id=TIER, owner_key=owner, mover_key=mover, instance=inst,
        template=template, batch_id="b1", descriptors=descs)["ok"] is True
    q.publish(action_key=mover, cas_root=str(cas_root),
              worker_script="/w.py", checkout_root=str(checkout),
              resources={"cpu": 1, "mem_gb": 1, f"{KIND}@{TIER}": 1},
              residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                         "manifest_sha256": manifest, "manifest_bytes": total,
                         "range_start_bytes": 0, "range_end_bytes": total})
    assert q.fund_output_batch(
        tier_id=TIER, owner_key=owner, mover_key=mover, instance=inst,
        template=template, batch_id="b1", descriptors=descs)["ok"] is True
    assert ledger.holder_tokens(mover).get(KIND, 0) == 1

    # --- crash boundary: transferring + filed prewrite, no commit ---
    # Restart: the same recovery APIs a new process would call.
    committed = po.commit_batch(q, inst, template, descs, batch_id="b1",
                                tier=TIER, mover_key=mover)
    assert committed.get("ok") is True, committed
    assert committed.get("funding") == "prepaid"
    # ...and the funded claim still admits the mover.
    squatter = _hexkey("restart-squatter")
    free = ledger.available().get(KIND, 0)
    if free:
        assert ledger.acquire(squatter, {KIND: free}) is True
    claimed = q.claim(owner="w-restart")
    assert claimed is not None and claimed["action_key"] == mover


def test_committed_unclaimed_funding_survives_release(tmp_path: Path) -> None:
    """D1: release never retires a committed batch's funding."""
    owner = _producer_request(tmp_path, tmp_path / "cas")
    q = _queue(tmp_path)
    template = _template(str(tmp_path / "outputs"))
    inst = _bind(q, template, owner)
    descs = _descriptors(tmp_path, template, inst, "p1", b"d" * 300)
    _prewrite(q, inst, template, "b1", TIER, descs)
    _announce_tier(q, tmp_path / "stage")
    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=tmp_path / "cas", producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res.get("ok") is True, res
    mover = str(res["mover_key"])
    generation = str(res["generation"])
    # Mover READY, never claimed, no receipt/lease/terminal: every legacy
    # release precondition holds -- only the filed-commit guard protects.
    assert q.release_output_funding(mover, TIER, generation=generation) is False
    rec = q.read_output_funding(mover, TIER)
    assert rec is not None and rec["state"] == "transferring"
    # The claim still works after the refused release.
    claimed = q.claim(owner="w-d1")
    assert claimed is not None and claimed["action_key"] == mover


def test_abort_guards_the_precommit_authority(tmp_path: Path) -> None:
    """D3: abort refuses while a pool intent cites the prewrite."""
    owner = _hexkey("d3-owner")
    q = _queue(tmp_path)
    template = _template(str(tmp_path / "outputs"))
    inst = _bind(q, template, owner)
    descs = _descriptors(tmp_path, template, inst, "p1", b"e" * 128)
    _prewrite(q, inst, template, "b1", TIER, descs)
    mover = _hexkey("d3-mover")
    staged = q.stage_output_intent(
        tier_id=TIER, owner_key=owner, mover_key=mover, instance=inst,
        template=template, batch_id="b1", descriptors=descs)
    assert staged.get("ok") is True, staged
    generation = str(staged["generation"])
    # Origin files exist, but the guard must refuse before file checks'
    # retain would: the intent itself is the earlier, recoverable refusal.
    refused = po.abort_prewrite(q, inst, template, batch_id="b1")
    assert refused.get("ok") is False
    assert refused.get("refusal") == "prepaid-intent-exists-retain", refused
    # Retire the never-started intent first, then abort proceeds.
    assert q.release_output_funding(mover, TIER,
                                    generation=generation) is True
    # Origin files now decide: present files retain (producer disposal).
    again = po.abort_prewrite(q, inst, template, batch_id="b1")
    assert again.get("ok") is False
    assert again.get("refusal") == "abort-files-present-retain", again
    (Path(template["output_prefix"]) / "p1.bin").unlink()
    done = po.abort_prewrite(q, inst, template, batch_id="b1")
    assert done.get("ok") is True and done.get("aborted") is True, done


def test_staged_intents_never_share_credit_names(tmp_path: Path) -> None:
    """D2: two staged intents select disjoint names; exhaustion refuses."""
    owner = _hexkey("d2-owner")
    q = _queue(tmp_path, gib=4)
    template = _template(str(tmp_path / "outputs"))
    inst = _bind(q, template, owner)
    descs1 = _descriptors(tmp_path, template, inst, "p1", b"f" * 64)
    descs2 = _descriptors(tmp_path, template, inst, "p2", b"f" * 64)
    _prewrite(q, inst, template, "b1", TIER, descs1)
    _prewrite(q, inst, template, "b2", TIER, descs2)
    first = q.stage_output_intent(
        tier_id=TIER, owner_key=owner, mover_key=_hexkey("d2-m1"),
        instance=inst, template=template, batch_id="b1",
        descriptors=descs1)
    assert first.get("ok") is True, first
    second = q.stage_output_intent(
        tier_id=TIER, owner_key=owner, mover_key=_hexkey("d2-m2"),
        instance=inst, template=template, batch_id="b2",
        descriptors=descs2)
    assert second.get("ok") is True, second
    assert set(first["tokens"]).isdisjoint(set(second["tokens"]))
    # Both names spoken and the window exhausted: deterministic refusal,
    # never a wedged third intent over an already-promised credit.
    descs3 = _descriptors(tmp_path, template, inst, "p3x", b"g" * 32)
    _prewrite(q, inst, template, "b3", TIER, descs3)
    third = q.stage_output_intent(
        tier_id=TIER, owner_key=owner, mover_key=_hexkey("d2-m3"),
        instance=inst, template=template, batch_id="b3", descriptors=descs3)
    assert third.get("ok") is False
    assert third.get("refusal") == "tier-reservation-unavailable", third

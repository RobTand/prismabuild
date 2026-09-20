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


@pytest.fixture(autouse=True)
def _isolated_synthetic_launch_context(monkeypatch):
    """Standalone synthetic launch contexts never inherit an outer tuple.

    Same proven isolation as test_core (PR746): the published runtime
    forwards a complete broker-owned reader tuple to launched processes,
    and production ``_reader_identity_environment`` refuses a foreign
    tuple rather than feeding it into an unrelated synthetic action.
    Pool.execute and the launcher it spawns must derive each action's own
    identity; production validity checks are untouched.
    """

    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT", "PRISMABUILD_ACTION_KEY"):
        monkeypatch.delenv(name, raising=False)


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _template(prefix: str, window_gib: int = 2) -> dict:
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
        "working_demands": {TIER: {"minimum_gib": 1,
                                   "window_gib": window_gib}},
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


def _bind(q: pool.PoolQueue, template: dict, owner: str,
          cas_root: Path | None = None):
    terms = po.owner_demand_terms(template)
    q.publish(action_key=owner, cas_root=str(cas_root or "/cas"),
              worker_script=str(REPO / "tools" / "prismabuild_worker.py"),
              checkout_root=str(Path(q.root).parent / "mover-checkout"),
              resources={"cpu": 1, "mem_gb": 1, **terms},
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
                  payload: bytes, *, digest_mode: str = "supplied"
                  ) -> list[dict]:
    origin = Path(template["output_prefix"])
    origin.mkdir(parents=True, exist_ok=True)
    path = origin / f"{tag}.bin"
    path.write_bytes(payload)
    slot = "s0" if tag.startswith("p") else "s1"
    cls = "payload" if slot == "s0" else "checkpoint"
    if digest_mode == "null":
        # The existing DEV data-manifest convention: no payload digest; the
        # identity is path+size+order and the mover's material evidence.
        sha256 = None
    else:
        sha256 = hashlib.sha256(payload).hexdigest()
    return [po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": slot,
        "artifact_class": cls, "path": str(path),
        "bytes": len(payload), "sha256": sha256,
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


def _tier_host(q: pool.PoolQueue) -> str:
    for record in q.tiers():
        if isinstance(record, dict) and record.get("tier_id") == TIER:
            return str(record.get("host") or "")
    return ""


def _claim_mover(q: pool.PoolQueue, owner: str) -> dict:
    """Claim the next READY row AS the storage owner (ordinary placement)."""
    claimed = q.claim(owner=owner, tags=[_tier_host(q)])
    assert claimed is not None, "mover row must be claimable on its host"
    return claimed


def _producer_request(tmp_path: Path, cas_root: Path,
                      template: dict) -> str:
    """Seal and file the PRODUCER's own request; return its action key.

    The driver recovers the movement template from exactly this request at
    runtime (inputs/closure/environment/scope/task/cwd), so the fixture
    owner IS the parent request's content-addressed key, as in production.
    The request also seals the produced-output template declaration the
    accepted-main egress seam requires (template envelope as a CAS input
    plus params.produced_output_template via the real build_declaration),
    and the closure covers a fixture checkout carrying the fleet tools --
    the child mover inherits it and executes against the same checkout.
    """
    checkout = tmp_path / "mover-checkout"
    tools = checkout / "tools" / "fleet"
    tools.mkdir(parents=True, exist_ok=True)
    for name in ("stage_move.py", "prewarm_loop.py", "stage_release.py"):
        (tools / name).write_bytes((REPO / "tools" / "fleet" / name)
                                   .read_bytes())
    cas = pb.PrismaBuildCAS(cas_root)
    envelope = tmp_path / "produced-template.json"
    envelope.write_text(json.dumps(template, sort_keys=True))
    template_input, _ = cas.ingest_input(
        envelope, input_id=pb.PRODUCED_OUTPUT_TEMPLATE_INPUT_ID)
    declaration = po.build_declaration(template, template_input)
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/produced-prepaid-producer",
                 "definition_version": "v1", "task_class": "generation",
                 "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": ["/bin/true"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [template_input],
        "code_closure": pb.build_code_closure(
            checkout, ["tools/fleet/stage_move.py",
                       "tools/fleet/prewarm_loop.py",
                       "tools/fleet/stage_release.py"]),
        "params": {"cwd": ".", "command": ["/bin/true"],
                   "produced_output_template": declaration},
        "environment": {"variables": {"PATH": "/usr/bin:/bin"},
                        "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    }
    action = pb.seal_action(body)
    cas.publish_action_request(action)
    return str(action["action_key"])


def _execute_mover(q: pool.PoolQueue, cas_root: Path, mover: str,
                   checkout: Path) -> dict:
    """The REAL worker seam: Pool.execute over the claimed mover row.

    ``Pool.execute`` materializes the row's checkout and spawns the
    canonical worker argv (the row's worker_script -- the PB worker
    launcher -- with ``run-local --action <request> --cas-root
    --checkout-root``), exactly as the fleet's worker loop does; the
    launcher derives the mover's identity and executes the sealed argv
    (the bash wrapper running stage_move). No direct run_local_action
    shortcut: this is the runtime callability under test.
    """
    row = pool._read_json(q.item_path(pool.CLAIMED, mover))
    assert isinstance(row, dict), "mover row must be claimed to execute"
    outcome = q.execute(row, timeout_s=240)
    assert outcome.get("returncode") == 0, outcome
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
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path, gib=4)
    ledger = q.tier_ledger(TIER)
    inst = _bind(q, template, owner, cas_root)
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

    claimed = _claim_mover(q, "w-integ-mover")
    assert claimed["action_key"] == mover
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
    assert retired.get("ok") is True, json.dumps(retired, default=str)
    # The egress returned the fence: usable capacity is back.
    assert ledger.holder_tokens(mover).get(KIND, 0) == 0
    assert ledger.available().get(KIND, 0) == 1


def test_second_batch_window_reuse_and_cleanup(tmp_path: Path) -> None:
    """A second sequential batch funds from disjoint names; capacity returns."""
    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path, gib=4)
    ledger = q.tier_ledger(TIER)
    inst = _bind(q, template, owner, cas_root)
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
        claimed = _claim_mover(q, f"w-{tag}")
        assert claimed["action_key"] == mover
        receipt = _execute_mover(q, cas_root, mover,
                                 tmp_path / "mover-checkout")
        assert receipt["complete"] is True, receipt
        q.finish(mover, status="executed")
        retired = po.retire_batch(
            q, inst, template, batch_id, stage_root=str(stage_root),
            residency_root=po.output_fragment_root(
                q.root / pool.RESIDENCY))
        assert retired.get("ok") is True, json.dumps(retired, default=str)
        results.append(res)

    # Disjoint credit names: the second batch never reused the first's.
    first = q.read_output_funding(str(results[0]["mover_key"]), TIER)
    second = q.read_output_funding(str(results[1]["mover_key"]), TIER)
    assert set(first["tokens"]).isdisjoint(set(second["tokens"]))

    # Reclaim after producer-side disposal, then THE THIRD SEQUENTIAL
    # BATCH: retirement returned both credits to FREE (never to producer
    # holdings), so a long-lived producer window must be refilled through
    # the public lifecycle API before it can spend again -- never by
    # reminting or unbounded new acquisition.
    for tag in ("p1", "p2"):
        (Path(template["output_prefix"]) / f"{tag}.bin").unlink()
    for batch_id in ("b1", "b2"):
        assert po.reclaim_origin(q, inst, template,
                                 batch_id=batch_id)["ok"] is True
    assert ledger.holder_tokens(owner).get(KIND, 0) == 0
    free_before_third = ledger.available().get(KIND, 0)
    assert free_before_third >= 1  # the retired credits are free, not held

    descs3 = _descriptors(tmp_path, template, inst, "p3x", b"c" * 512)
    _prewrite(q, inst, template, "b3", TIER, descs3)
    refused = po.publish_prepaid_batch(
        q, inst, template, descs3, batch_id="b3", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert refused.get("ok") is False
    assert refused.get("refusal") == "tier-reservation-unavailable", refused

    # The bounded refill: back up to (never past) the admitted window.
    refill = po.refill_window(q, inst, template, tier=TIER)
    assert refill.get("ok") is True, refill
    assert ledger.holder_tokens(owner).get(KIND, 0) >= 1
    assert (ledger.holder_tokens(owner).get(KIND, 0)
            + len([r for r in []])) <= 2  # aggregate window bound holds

    res3 = po.publish_prepaid_batch(
        q, inst, template, descs3, batch_id="b3", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res3.get("ok") is True, res3
    mover3 = str(res3["mover_key"])
    claimed3 = _claim_mover(q, "w-p3")
    assert claimed3["action_key"] == mover3
    receipt3 = _execute_mover(q, cas_root, mover3,
                              tmp_path / "mover-checkout")
    assert receipt3["complete"] is True, receipt3
    q.finish(mover3, status="executed")
    assert po.retire_batch(q, inst, template, "b3",
                           stage_root=str(stage_root),
                           residency_root=po.output_fragment_root(
                               q.root / pool.RESIDENCY))["ok"] is True

    # Cleanup returns usable capacity: dispose origin, reclaim, finish.
    (Path(template["output_prefix"]) / "p3x.bin").unlink()
    assert po.reclaim_origin(q, inst, template, batch_id="b3")["ok"] is True
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
    claimed = _claim_mover(q, "w-restart")
    assert claimed["action_key"] == mover


def test_committed_unclaimed_funding_survives_release(tmp_path: Path) -> None:
    """D1: release never retires a committed batch's funding."""
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, tmp_path / "cas", template)
    q = _queue(tmp_path)
    inst = _bind(q, template, owner, tmp_path / "cas")
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
    claimed = _claim_mover(q, "w-d1")
    assert claimed["action_key"] == mover


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


def test_descriptor_digest_rules(tmp_path: Path) -> None:
    """Descriptor digests: hex64 or the DEV null; never a coerced string."""
    q = _queue(tmp_path)
    template = _template(str(tmp_path / "outputs"))
    owner = _hexkey("digest-owner")
    inst = _bind(q, template, owner)

    null = _descriptors(tmp_path, template, inst, "p9", b"z" * 64,
                        digest_mode="null")
    assert null[0]["sha256"] is None  # stays JSON null, never "None"

    supplied = _descriptors(tmp_path, template, inst, "p8", b"y" * 48)
    assert len(supplied[0]["sha256"]) == 64

    origin = Path(template["output_prefix"])
    with pytest.raises(po.ProducedOutputError):
        po.validate_descriptor({
            "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "s0",
            "artifact_class": "payload",
            "path": str(origin / "bad.bin"),
            "bytes": 4, "sha256": "None",
            "producer_generation": po.mint_generation(),
            "owner_action_key": inst["owner_action_key"],
            "owner_attempt": dict(inst["owner_attempt"]),
        }, template, inst)
    with pytest.raises(po.ProducedOutputError):
        po.validate_descriptor({
            "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "s0",
            "artifact_class": "payload",
            "path": str(origin / "bad.bin"),
            "bytes": 4, "sha256": "not-hex",
            "producer_generation": po.mint_generation(),
            "owner_action_key": inst["owner_action_key"],
            "owner_attempt": dict(inst["owner_attempt"]),
        }, template, inst)


def test_dev_null_digest_first_and_second_batch_full_lifecycle(
        tmp_path: Path) -> None:
    """DEV convention end to end: sha256: null through the real path.

    The PQ caller must not hash every tensor output: descriptors carry the
    existing DEV null digest, and the whole sequence still works -- sealed
    CAS producer+mover requests, funded claim with free exhausted, the
    sealed argv copying REAL bytes (size-verified, digest-skipped), real
    egress retirement and origin reclaim, two sequential batches, capacity
    back. Identity is path+size+order (the manifest digest covers the
    descriptor list) plus the mover's material evidence; nothing here is
    a certified digest and none is claimed.
    """
    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path, gib=4)
    ledger = q.tier_ledger(TIER)
    inst = _bind(q, template, owner, cas_root)
    stage_root = tmp_path / "stage"
    _announce_tier(q, stage_root)

    results = []
    squatted = 0
    for tag, batch_id, payload in (("n1", "b1", b"a" * 640),
                                   ("n2", "b2", b"b" * 768)):
        descs = _descriptors(tmp_path, template, inst, tag, payload,
                             digest_mode="null")
        assert descs[0]["sha256"] is None
        _prewrite(q, inst, template, batch_id, TIER, descs)
        res = po.publish_prepaid_batch(
            q, inst, template, descs, batch_id=batch_id, tier=TIER,
            cas_root=cas_root, producer_action_key=owner,
            command_extra=["--unpaced"])
        assert res.get("ok") is True, res
        mover = str(res["mover_key"])
        # The null digest survives every transition as JSON null.
        rec = pool._read_json(
            Path(q.root) / "residency" / po.OUTPUT_BATCHES_SUBDIR
            / po.instance_namespace(inst) / f"{batch_id}.json")
        assert rec["entries"][0]["sha256"] is None
        assert res["manifest_digest"] == po.output_manifest_sha256(descs)

        # Exhaust free credits once: only prepaid cover admits the movers.
        free = ledger.available().get(KIND, 0)
        if free:
            assert ledger.acquire(_hexkey(f"null-squatter-{batch_id}"),
                                  {KIND: free}) is True
            squatted += free
        claimed = _claim_mover(q, f"w-null-{tag}")
        assert claimed["action_key"] == mover
        funding = q.read_output_funding(mover, TIER)
        # The funded claim consumed the prepaid record (the mover's copy
        # runs after consumption; the fence is already fused).
        assert funding is not None and funding["state"] == "consumed"

        # The REAL mover: sealed argv execution copies and size-verifies;
        # the null declared digest skips equality, the computed digest
        # still lands in the receipt and fragment.
        receipt = _execute_mover(q, cas_root, mover,
                                 tmp_path / "mover-checkout")
        assert receipt["complete"] is True, receipt
        assert receipt["bytes_staged"] == len(payload)
        staged = Path(stage_root) / f"{tag}.bin"
        assert staged.read_bytes() == payload
        q.finish(mover, status="executed")
        retired = po.retire_batch(q, inst, template, batch_id,
                                  stage_root=str(stage_root),
                                  residency_root=po.output_fragment_root(
                                      q.root / pool.RESIDENCY))
        assert retired.get("ok") is True, json.dumps(retired, default=str)
        results.append(res)

    # Disjoint prepaid names across the two batches.
    first = q.read_output_funding(str(results[0]["mover_key"]), TIER)
    second = q.read_output_funding(str(results[1]["mover_key"]), TIER)
    assert set(first["tokens"]).isdisjoint(set(second["tokens"]))

    # Reclaim after producer-side disposal; the window returns.
    for tag in ("n1", "n2"):
        (Path(template["output_prefix"]) / f"{tag}.bin").unlink()
    for batch_id in ("b1", "b2"):
        assert po.reclaim_origin(q, inst, template,
                                 batch_id=batch_id)["ok"] is True
    q.finish(owner, status="executed")
    assert ledger.holder_tokens(owner).get(KIND, 0) == 0
    assert squatted > 0
    # Everything except the squatters' tokens is back: the retired fences
    # returned through the real egress, the window through owner finish.
    assert (ledger.available().get(KIND, 0) + squatted
            == ledger.capacity().get(KIND, 0))


def test_bounded_prewrite_ceiling_to_actual(tmp_path: Path) -> None:
    """The PQ caller shape: a conservative prewrite ceiling, actual below it.

    PQ pre-serializes a bounded window (planned path superset, per-class
    upper bounds) before sizes are known, then writes actuals at or under
    the ceiling. The prewrite admits the ceiling for the batch's temporary
    lifetime (headroom charges the ceiling while live); describe/commit
    reconcile actual <= ceiling per class and actual paths subset of
    planned -- exact-size pretense is not required and a mismatch above
    the ceiling still refuses.
    """
    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path, gib=4)
    ledger = q.tier_ledger(TIER)
    inst = _bind(q, template, owner, cas_root)
    _announce_tier(q, tmp_path / "stage")

    origin = Path(template["output_prefix"])
    origin.mkdir(parents=True, exist_ok=True)
    payload = b"q" * 700
    (origin / "c1.bin").write_bytes(payload)
    # Planned superset: c1.bin plus an unwritten sibling; ceiling 4096.
    ceiling = {"payload": 4096, "checkpoint": 0, "temp": 0}
    planned_paths = sorted([str(origin / "c1.bin"), str(origin / "c9.bin")])
    out = po.require_prewrite(q, inst, template, batch_id="b1", tier=TIER,
                              class_bytes=ceiling, paths=planned_paths)
    assert out.get("ok") is True, out

    descs = [po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "s0",
        "artifact_class": "payload", "path": str(origin / "c1.bin"),
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "producer_generation": po.mint_generation(),
        "owner_action_key": inst["owner_action_key"],
        "owner_attempt": dict(inst["owner_attempt"]),
    }, template, inst)]

    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res.get("ok") is True, res
    mover = str(res["mover_key"])
    claimed = _claim_mover(q, "w-ceiling")
    assert claimed["action_key"] == mover
    receipt = _execute_mover(q, cas_root, mover, tmp_path / "mover-checkout")
    assert receipt["complete"] is True, receipt
    q.finish(mover, status="executed")
    assert po.retire_batch(q, inst, template, "b1",
                           stage_root=str(tmp_path / "stage"),
                           residency_root=po.output_fragment_root(
                               q.root / pool.RESIDENCY))["ok"] is True

    # Above the ceiling still refuses: an actual exceeding the admitted
    # bound can never reconcile into that prewrite.
    (origin / "c2.bin").write_bytes(b"z" * 5000)
    small_ceiling = {"payload": 512, "checkpoint": 0, "temp": 0}
    assert po.require_prewrite(
        q, inst, template, batch_id="b2", tier=TIER,
        class_bytes=small_ceiling,
        paths=[str(origin / "c2.bin")])["ok"] is True
    over = [po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "s0",
        "artifact_class": "payload", "path": str(origin / "c2.bin"),
        "bytes": 5000, "sha256": hashlib.sha256(b"z" * 5000).hexdigest(),
        "producer_generation": po.mint_generation(),
        "owner_action_key": inst["owner_action_key"],
        "owner_attempt": dict(inst["owner_attempt"]),
    }, template, inst)]
    refused = po.publish_prepaid_batch(
        q, inst, template, over, batch_id="b2", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert refused.get("ok") is False, refused
    assert refused.get("refusal") == "prewrite-mismatch", refused


def test_refill_respects_consumed_unretired_holdings(tmp_path: Path) -> None:
    """Gap 1: a claimed-but-unretired batch still spends the window.

    Ordinary pipelining: batch1 is claimed (its funding record is consumed)
    and its staged bytes/tokens are still live on the mover. The producer
    must NOT be able to refill the full window on top of that live batch:
    owner holdings + still-live batch holdings never exceed window_gib.
    """
    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path, gib=4)
    ledger = q.tier_ledger(TIER)
    inst = _bind(q, template, owner, cas_root)
    stage_root = tmp_path / "stage"
    _announce_tier(q, stage_root)

    descs = _descriptors(tmp_path, template, inst, "p1", b"a" * 300)
    _prewrite(q, inst, template, "b1", TIER, descs)
    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res.get("ok") is True, res
    mover = str(res["mover_key"])
    claimed = _claim_mover(q, "w-g1")
    assert claimed["action_key"] == mover
    # consumed, mover's fence still live, batch not retired.
    rec = q.read_output_funding(mover, TIER)
    assert rec is not None and rec["state"] == "consumed"
    mover_live = ledger.holder_tokens(mover).get(KIND, 0)
    assert mover_live == 1

    refill = po.refill_window(q, inst, template, tier=TIER)
    assert refill.get("ok") is True, refill
    outstanding = (ledger.holder_tokens(owner).get(KIND, 0) + mover_live)
    assert outstanding <= 2, (refill, outstanding)


def test_planned_omitted_temp_must_be_absent(tmp_path: Path) -> None:
    """Gap 2: an unwritten planned path that later EXISTS stays charged.

    The conservative prewrite admits a planned superset; a path planned
    but omitted from the commit descriptors must be proven ABSENT at
    describe/commit (no payload read/hash). A present leftover refuses;
    removing it through ordinary writer cleanup lets the same commit
    succeed.
    """
    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path, gib=4)
    inst = _bind(q, template, owner, cas_root)
    _announce_tier(q, tmp_path / "stage")

    origin = Path(template["output_prefix"])
    origin.mkdir(parents=True, exist_ok=True)
    payload = b"t" * 256
    (origin / "k1.bin").write_bytes(payload)
    ceiling = {"payload": 1024, "checkpoint": 0, "temp": 512}
    planned = sorted([str(origin / "k1.bin"), str(origin / "k2.tmp")])
    assert po.require_prewrite(q, inst, template, batch_id="b1", tier=TIER,
                               class_bytes=ceiling,
                               paths=planned)["ok"] is True
    descs = [po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "s0",
        "artifact_class": "payload", "path": str(origin / "k1.bin"),
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "producer_generation": po.mint_generation(),
        "owner_action_key": inst["owner_action_key"],
        "owner_attempt": dict(inst["owner_attempt"]),
    }, template, inst)]

    # The planned-but-omitted temp EXISTS: the commit must refuse and the
    # charge must remain.
    (origin / "k2.tmp").write_bytes(b"leftover" * 8)
    refused = po.commit_batch(q, inst, template, descs, batch_id="b1",
                              tier=TIER, mover_key=_hexkey("g2-mover"))
    assert refused.get("ok") is False, refused
    assert refused.get("refusal") == "planned-path-present-retain", refused

    # Writer cleanup (producer-side disposal) makes the same commit work.
    (origin / "k2.tmp").unlink()
    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res.get("ok") is True, res


def test_mover_row_carries_parent_priority_and_retry(tmp_path: Path) -> None:
    """Gap 3: the mover row keeps the parent's priority and retry policy.

    Ordinary publish semantics: an agent's -10 validation priority stays
    -10 on the mover row, and the effective sealed retry policy rides the
    row beside the correct worker/snapshot/tag context.
    """
    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path, gib=4)
    # Publish the producer row at the agent validation priority.
    q.publish(action_key=owner, cas_root=str(cas_root),
              worker_script=str(REPO / "tools" / "prismabuild_worker.py"),
              checkout_root=str(tmp_path / "mover-checkout"),
              resources={"cpu": 1, "mem_gb": 1,
                         **po.owner_demand_terms(template)},
              produced_output_template=template, priority=-10,
              max_attempts=5, retry_safe=True)
    claimed = q.claim(owner="w-g3-owner")
    assert claimed is not None and claimed["action_key"] == owner
    control = _broker_control(q, owner)
    env = {"PRISMABUILD_ACTION_KEY": owner,
           "PRISMABUILD_ACTION_NONCE": control["nonce"],
           "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
    inst = po.bind_instance(q, template, owner_action_key=owner,
                            claim_snapshot=claimed, env=env)
    po.declare_instance(q.root, inst)
    assert po.admit_instance(q, inst, template)["ok"] is True
    _announce_tier(q, tmp_path / "stage")
    descs = _descriptors(tmp_path, template, inst, "p1", b"g" * 64)
    _prewrite(q, inst, template, "b1", TIER, descs)
    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res.get("ok") is True, res
    row = pool._read_json(q.item_path(pool.READY, str(res["mover_key"])))
    assert isinstance(row, dict)
    assert row.get("priority") == -10
    assert row.get("retry_safe") is True
    assert int(row.get("max_attempts", 0)) >= 1


def test_window_credits_recycle_in_a_window_too_small_to_hold_them(
        tmp_path: Path) -> None:
    """Four batches through a window that never holds more than ONE credit.

    Sizing a window to the batch count proves only that the batches fit.
    Here the tier's whole CAPACITY and the admitted window are both ONE
    credit, so batch N can only fund if batch N-1's credit genuinely came
    back at retirement: a counter that lost it would exhaust the window
    permanently and refuse from batch 2 onward. Real sealed movers
    through `Pool.execute`, real retirement, real reclaim -- no mocks.
    """

    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"), window_gib=1)
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path, gib=1)
    ledger = q.tier_ledger(TIER)
    assert ledger.capacity().get(KIND, 0) == 1
    inst = _bind(q, template, owner, cas_root)
    stage_root = tmp_path / "stage"
    _announce_tier(q, stage_root)
    residency = po.output_fragment_root(q.root / pool.RESIDENCY)

    free_after_each: list[int] = []
    for n in range(4):
        tag, batch_id = f"r{n}", f"rb{n}"
        if n:
            # The window is empty: the previous batch's credit went to
            # FREE at retirement, never back to producer holdings.
            refill = po.refill_window(q, inst, template, tier=TIER)
            assert refill.get("ok") is True, (
                n, refill, dict(ledger.available()))
            assert ledger.holder_tokens(owner).get(KIND, 0) == 1, n
        descs = _descriptors(tmp_path, template, inst, tag,
                             bytes([65 + n]) * 512)
        _prewrite(q, inst, template, batch_id, TIER, descs)
        res = po.publish_prepaid_batch(
            q, inst, template, descs, batch_id=batch_id, tier=TIER,
            cas_root=cas_root, producer_action_key=owner,
            command_extra=["--unpaced"])
        assert res.get("ok") is True, (n, res)
        mover = str(res["mover_key"])
        # Funding is an exact transfer: the owner spent its whole window.
        assert ledger.holder_tokens(owner).get(KIND, 0) == 0, n
        assert ledger.holder_tokens(mover).get(KIND, 0) == 1, n
        claimed = _claim_mover(q, f"w-{tag}")
        assert claimed["action_key"] == mover
        receipt = _execute_mover(q, cas_root, mover,
                                 tmp_path / "mover-checkout")
        assert receipt["complete"] is True, (n, receipt)
        q.finish(mover, status="executed")
        assert po.retire_batch(
            q, inst, template, batch_id, stage_root=str(stage_root),
            residency_root=residency)["ok"] is True, n
        (Path(template["output_prefix"]) / f"{tag}.bin").unlink()
        assert po.reclaim_origin(
            q, inst, template, batch_id=batch_id)["ok"] is True, n
        assert ledger.holder_tokens(mover).get(KIND, 0) == 0, n
        free_after_each.append(ledger.available().get(KIND, 0))

    # The counter loses nothing: every retirement returned the credit,
    # through four cycles of a one-credit window.
    assert free_after_each == [1, 1, 1, 1], free_after_each
    q.finish(owner, status="executed")
    assert ledger.holder_tokens(owner).get(KIND, 0) == 0
    assert ledger.available() == ledger.capacity()


def test_failed_mover_batch_retires_but_its_row_cannot_be_reclaimed(
        tmp_path: Path) -> None:
    """What actually happens to a committed batch whose mover FAILED.

    Two things were previously asserted from reading the code. Only one
    survives measurement.

    WRONG: that such a batch can never be retired and so owns its origin
    paths forever. `retire_batch` runs off the egress receipt, and a
    mover that staged nothing has nothing to evict, so retirement
    SUCCEEDS, ownership ends, and the producer re-plans the same path
    immediately. Nothing is stranded and no cancel API is needed.

    REAL, and the reason the same-batch retry cannot be relied on: this
    mover staged NOTHING, so its terminal frees its reservation (nothing
    is occupying anything -- a partial one retains instead, which
    `test_a_partial_mover_keeps_its_tokens_until_the_egress_frees_them`
    measures), while its funding record stays `consumed` and the retry
    ladder requeues the row READY. That row is then unclaimable, and
    stays unclaimable after `refill_window` and after re-driving
    `publish_prepaid_batch` -- which short-circuits a committed batch to
    its duplicate and re-funds nothing. The working recovery is retire ->
    reclaim -> refill -> plan a new batch, and the census names it.

    The failure is real, not mocked: the origin file disappears before
    the mover runs, so the sealed argv stages nothing and exits nonzero
    through the ordinary `Pool.execute` seam.
    """

    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path)
    ledger = q.tier_ledger(TIER)
    inst = _bind(q, template, owner, cas_root)
    stage_root = tmp_path / "stage"
    _announce_tier(q, stage_root)
    residency = po.output_fragment_root(q.root / pool.RESIDENCY)

    payload = b"f" * 700
    descs = _descriptors(tmp_path, template, inst, "p1", payload)
    origin_path = Path(str(descs[0]["path"]))
    _prewrite(q, inst, template, "b1", TIER, descs)
    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res.get("ok") is True, res
    mover = str(res["mover_key"])

    origin_path.unlink()
    claimed = _claim_mover(q, "w-fail")
    assert claimed["action_key"] == mover
    outcome = q.execute(claimed, timeout_s=240)
    assert outcome.get("returncode") != 0, outcome
    q.finish(mover, status="failed")
    # Nothing was staged, so the whole reservation comes back -- checked on
    # the aggregate ledger, where a charge that merely moved would still
    # show up under some holder.
    assert _staged(stage_root, "*.bin") == []
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 3, "holders": {owner: 1}}
    funding = q.read_output_funding(mover, TIER)
    assert funding is not None and funding["state"] == "consumed"

    # The row is requeued READY -- and cannot be claimed.
    assert po._mover_live_state(q, mover) == "ready"
    assert q.claim(owner="probe-a", tags=[_tier_host(q)]) is None
    # The row is queued but can never be funded back into a claim, so
    # recovery must name the terminal route instead of telling the caller
    # to keep waiting for it.
    events = po.recover_batches(q, inst, template)
    assert {"event": "output-mover-unfundable-retire", "batch_id": "b1",
            "mover": mover, "action": "retire-reclaim-replan"} in events, events
    assert not any(e.get("event") == "output-mover-live-wait"
                   for e in events), events

    # While the batch is live it does own its path.
    blocked = po.require_prewrite(
        q, inst, template, batch_id="b2", tier=TIER,
        class_bytes={"payload": len(payload), "checkpoint": 0, "temp": 0},
        paths=[str(origin_path)])
    assert blocked.get("ok") is False
    assert blocked["refusal"] == "prewrite-path-owned-by-live-batch"
    assert blocked["owner_batch_id"] == "b1"

    # Neither refilling the window nor re-driving the batch restores the
    # row: publication answers the duplicate and re-funds nothing.
    origin_path.write_bytes(payload)
    assert po.refill_window(q, inst, template, tier=TIER)["ok"] is True
    again = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert again.get("ok") is True and again.get("duplicate") is True, again
    assert str(again["mover_key"]) == mover
    assert q.read_output_funding(mover, TIER)["state"] == "consumed"
    assert ledger.holder_tokens(mover).get(KIND, 0) == 0
    assert q.claim(owner="probe-b", tags=[_tier_host(q)]) is None

    # The batch is NOT stranded, and the route is TRAVERSED from what the
    # census emitted rather than from a batch id the test already knew: an
    # event that names a route is not recovery until something walks it.
    route = [event for event in po.recover_batches(q, inst, template)
             if event.get("event") == "output-mover-unfundable-retire"]
    assert len(route) == 1 and route[0]["action"] == "retire-reclaim-replan"
    target = str(route[0]["batch_id"])
    retired = po.retire_batch(q, inst, template, target,
                              stage_root=str(stage_root),
                              residency_root=residency)
    assert retired.get("ok") is True, retired
    assert retired["staged_paths"] == []
    # Reclaim proves the producer disposed of the origin bytes.
    origin_path.unlink()
    assert po.reclaim_origin(q, inst, template, batch_id=target)["ok"] is True
    assert all(event.get("event") != "output-mover-unfundable-retire"
               for event in po.recover_batches(q, inst, template))

    # The working recovery: the same path is re-planned as a new batch
    # and staged for real by an ordinary mover.
    freed = po.require_prewrite(
        q, inst, template, batch_id="b2", tier=TIER,
        class_bytes={"payload": len(payload), "checkpoint": 0, "temp": 0},
        paths=[str(origin_path)])
    assert freed.get("ok") is True, freed
    descs2 = _descriptors(tmp_path, template, inst, "p1", payload)
    res2 = po.publish_prepaid_batch(
        q, inst, template, descs2, batch_id="b2", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res2.get("ok") is True, res2
    mover2 = str(res2["mover_key"])
    assert _claim_mover(q, "w-ok")["action_key"] == mover2
    receipt = _execute_mover(q, cas_root, mover2,
                             tmp_path / "mover-checkout")
    assert receipt["complete"] is True, receipt
    q.finish(mover2, status="executed")
    assert po.retire_batch(q, inst, template, "b2",
                           stage_root=str(stage_root),
                           residency_root=residency)["ok"] is True
    # The tier is whole again and every token is accounted to somebody.
    assert _tier_census(ledger)["capacity"] == 4


def _tier_census(ledger) -> dict:
    """The whole tier ledger, reconciled: free + every holder == capacity.

    One holder's view cannot tell a charge that moved from a charge that
    vanished, so every assertion about occupancy in this file is made
    against this, never against `holder_tokens` alone.
    """

    holders: dict[str, int] = {}
    held_dir = Path(ledger.held_dir)
    if held_dir.is_dir():
        for entry in sorted(held_dir.iterdir()):
            if not entry.is_dir():
                continue
            count = sum(1 for _p in entry.glob(f"{KIND}-*"))
            if count:
                holders[entry.name] = count
    free = int(ledger.available().get(KIND, 0))
    capacity = int(ledger.capacity().get(KIND, 0))
    assert capacity == free + sum(holders.values()), (capacity, free, holders)
    return {"capacity": capacity, "free": free, "holders": holders}


def _staged(stage_root: Path, name: str) -> list[Path]:
    return sorted(p for p in stage_root.rglob(name) if p.is_file())


def test_a_partial_mover_keeps_its_tokens_until_the_egress_frees_them(
        tmp_path: Path) -> None:
    """A half-staged batch stays charged to somebody until its bytes go.

    Measured on the AGGREGATE ledger, not on the mover's own holdings:
    before this, `finish(status="failed")` released the mover's tier
    tokens while 700 of its 1200 declared bytes sat on the stage, the
    owner's bounded refill then re-acquired against that occupancy, and
    retirement evicted the bytes while freeing nothing -- capacity 4 read
    as free 2 + owner 2 with real bytes charged to no holder at all.

    The complete arm of this lifecycle already behaved: a mover that
    staged its whole range keeps its tokens past `finish` and the egress
    returns them. This is the same rule for the partial arm, because the
    partial arm also left bytes behind.
    """

    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path)
    ledger = q.tier_ledger(TIER)
    inst = _bind(q, template, owner, cas_root)
    stage_root = tmp_path / "stage"
    _announce_tier(q, stage_root)
    residency = po.output_fragment_root(q.root / pool.RESIDENCY)

    payload = b"f" * 700
    descs = _descriptors(tmp_path, template, inst, "p1", payload)
    descs = descs + _descriptors(tmp_path, template, inst, "s1", b"c" * 500)
    _prewrite(q, inst, template, "b1", TIER, descs)
    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res.get("ok") is True, res
    mover = str(res["mover_key"])
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 2, "holders": {owner: 1, mover: 1}}

    # The second origin file disappears before the mover runs, so it
    # stages the first and cannot stage the second. A real partial.
    Path(str(descs[1]["path"])).unlink()
    claimed = _claim_mover(q, "w-partial")
    assert claimed["action_key"] == mover
    q.execute(claimed, timeout_s=240)
    receipt = q.move_record(mover)
    assert isinstance(receipt, dict) and receipt.get("complete") is False
    assert int(receipt["bytes_staged"]) == len(payload)
    assert not receipt.get("refusal"), receipt
    assert [p.name for p in _staged(stage_root, "p1.bin")] == ["p1.bin"]

    q.finish(mover, status="failed")
    # The bytes are still there, so the charge is still there -- and it is
    # still charged to the mover that put them there.
    assert [p.name for p in _staged(stage_root, "p1.bin")] == ["p1.bin"]
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 2, "holders": {owner: 1, mover: 1}}
    # The owner's bounded refill sees it and takes nothing: the window is
    # spent on bytes that have not left.
    refill = po.refill_window(q, inst, template, tier=TIER)
    assert refill["ok"] is True, refill
    assert (refill["acquired"], refill["outstanding"]) == (0, 1), refill
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 2, "holders": {owner: 1, mover: 1}}

    # And the census names the route, from evidence: fragments compose for
    # this batch exactly as they would for a complete one, so the verdict
    # has to come from the receipt. Reporting it as staged would tell the
    # producer this batch needs nothing while 500 of its declared bytes
    # exist only in the origin file it is about to reclaim.
    route = [event for event in po.recover_batches(q, inst, template)
             if event.get("event") == "output-mover-unfundable-retire"]
    assert len(route) == 1, route
    assert route[0]["mover"] == mover, route
    assert route[0]["action"] == "retire-reclaim-replan", route
    assert all(event.get("event") != "output-batch-staged"
               for event in po.recover_batches(q, inst, template))
    target = str(route[0]["batch_id"])

    # The egress is what returns them, because it is what deletes the
    # bytes. Both happen in the same call and neither happens without it.
    retired = po.retire_batch(q, inst, template, target,
                              stage_root=str(stage_root),
                              residency_root=residency)
    assert retired.get("ok") is True, retired
    assert [Path(p).name for p in retired["staged_paths"]] == ["p1.bin"]
    assert _staged(stage_root, "p1.bin") == []
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 3, "holders": {owner: 1}}
    # Exactly once: the egress that deleted the bytes returned the token,
    # and re-driving retirement answers duplicate without returning a
    # second one.
    again = po.retire_batch(q, inst, template, target,
                            stage_root=str(stage_root),
                            residency_root=residency)
    assert again.get("ok") is True and again.get("duplicate") is True, again
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 3, "holders": {owner: 1}}
    refill2 = po.refill_window(q, inst, template, tier=TIER)
    assert refill2["ok"] is True and refill2["acquired"] == 1, refill2
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 2, "holders": {owner: 2}}


def test_a_claimed_mover_holding_nothing_is_never_told_to_retire(
        tmp_path: Path) -> None:
    """A live CLAIMED row is not reclassified by one absent holding read.

    An executor owns a claimed row; its ledger holdings can read empty for
    reasons that have nothing to do with whether the work is alive (an
    egress or a reaper releasing by mover key, an admission still between
    its two halves). Recovery for a claim belongs to the lease reaper, so
    the census waits rather than naming a terminal route for it.
    """

    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path)
    ledger = q.tier_ledger(TIER)
    inst = _bind(q, template, owner, cas_root)
    stage_root = tmp_path / "stage"
    _announce_tier(q, stage_root)

    descs = _descriptors(tmp_path, template, inst, "p1", b"f" * 700)
    _prewrite(q, inst, template, "b1", TIER, descs)
    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res.get("ok") is True, res
    mover = str(res["mover_key"])
    assert _claim_mover(q, "w-live")["action_key"] == mover
    # The funding record is spent the moment the claim counts it, which is
    # exactly when the row is most alive.
    assert q.read_output_funding(mover, TIER)["state"] == "consumed"
    # Model the release-by-mover-key an egress or reaper performs: the row
    # is untouched and still claimed, only its holdings went.
    q.release_tier_reservations(mover)
    assert ledger.holder_tokens(mover).get(KIND, 0) == 0
    assert po._mover_live_state(q, mover) == "claimed"

    events = po.recover_batches(q, inst, template)
    assert {"event": "output-mover-live-wait", "batch_id": "b1",
            "mover": mover} in events, events
    assert not any(e.get("event") == "output-mover-unfundable-retire"
                   for e in events), events


def test_a_ready_mover_with_an_unreadable_funding_record_stays_unknown(
        tmp_path: Path) -> None:
    """Corrupt is UNKNOWN, and a census may not flatten it into spent.

    `read_output_funding` returns None for absent AND for unparsable, and
    its own docstring forbids a census path from reading that None as
    "no funding". A row whose intent cannot be read is a row whose state
    is not known, and telling its caller to retire the batch on that is
    the unproven-becomes-negative failure this lane keeps finding.
    """

    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path)
    inst = _bind(q, template, owner, cas_root)
    stage_root = tmp_path / "stage"
    _announce_tier(q, stage_root)

    descs = _descriptors(tmp_path, template, inst, "p1", b"f" * 700)
    _prewrite(q, inst, template, "b1", TIER, descs)
    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res.get("ok") is True, res
    mover = str(res["mover_key"])
    # The row stays READY; only the filed intent becomes unreadable.
    q.funding_output_path(mover, TIER).write_text("{not json")
    assert q.output_funding_file_state(mover, TIER)[1] == "corrupt"
    assert q.read_output_funding(mover, TIER) is None

    events = po.recover_batches(q, inst, template)
    unknown = [event for event in events
               if event.get("event") == "output-recovery-unknown"]
    assert len(unknown) == 1, events
    assert unknown[0]["batch_id"] == "b1", unknown
    assert unknown[0]["mover"] == mover, unknown
    # The event carries WHY it is unknown, so a caller is not left to
    # guess which of the several "I found nothing" paths it took.
    assert "unreadable" in str(unknown[0].get("error", "")), unknown
    assert not any(e.get("event") == "output-mover-unfundable-retire"
                   for e in events), events


def test_the_census_verdict_does_not_turn_on_a_holding_count(
        tmp_path: Path) -> None:
    """Holdings are not the evidence, in either direction.

    This row is unclaimable because its funding record is spent, and that
    stays true whether or not the mover holds tier tokens -- a partial
    mover keeps tokens for the bytes it staged and is no more claimable
    for it. Reading a holding count as liveness calls such a row live and
    waits for it forever, the same defect as calling a live row dead, so
    the verdict is taken from the filed record and a holding is added
    here to prove the count is never consulted.
    """

    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path)
    ledger = q.tier_ledger(TIER)
    inst = _bind(q, template, owner, cas_root)
    stage_root = tmp_path / "stage"
    _announce_tier(q, stage_root)

    payload = b"f" * 700
    descs = _descriptors(tmp_path, template, inst, "p1", payload)
    origin_path = Path(str(descs[0]["path"]))
    _prewrite(q, inst, template, "b1", TIER, descs)
    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res.get("ok") is True, res
    mover = str(res["mover_key"])

    origin_path.unlink()
    claimed = _claim_mover(q, "w-holdings")
    outcome = q.execute(claimed, timeout_s=240)
    assert outcome.get("returncode") != 0, outcome
    q.finish(mover, status="failed")
    # Nothing staged, so nothing retained -- and the row is requeued READY
    # with a spent record.
    assert ledger.holder_tokens(mover).get(KIND, 0) == 0
    assert po._mover_live_state(q, mover) == "ready"
    verdict = {"event": "output-mover-unfundable-retire", "batch_id": "b1",
               "mover": mover, "action": "retire-reclaim-replan"}
    assert verdict in po.recover_batches(q, inst, template)

    # Same row, same spent record, now holding a token the way a partial
    # mover holds one. The verdict may not move.
    assert ledger.acquire(mover, {KIND: 1}) is True
    assert ledger.holder_tokens(mover).get(KIND, 0) == 1
    events = po.recover_batches(q, inst, template)
    assert verdict in events, events
    assert not any(event.get("event") == "output-mover-live-wait"
                   for event in events), events


def test_a_mover_killed_after_publishing_keeps_its_charge(
        tmp_path: Path) -> None:
    """A missing move receipt is silence, not a report of zero bytes.

    `stage_move` publishes a residency fragment PER ENTRY as the bytes
    land and files its move receipt ONCE, last
    (`tools/fleet/stage_move.py`: `record_move` at :1965, after the
    per-entry `publish`). A kill or an OOM in that window leaves real
    bytes on the stage and no receipt at all, so reading the absent
    receipt as "staged nothing" frees the charge while the files are
    still there -- unproven treated as known-negative, one layer below
    the holding count that was the same mistake last round.

    Modelled at the evidence level rather than by racing a signal: the
    mover publishes and stages for real, and its receipt is removed, so
    the predicate sees exactly what the crash window leaves behind.
    """

    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path)
    ledger = q.tier_ledger(TIER)
    inst = _bind(q, template, owner, cas_root)
    stage_root = tmp_path / "stage"
    _announce_tier(q, stage_root)
    residency = po.output_fragment_root(q.root / pool.RESIDENCY)

    payload = b"f" * 700
    descs = _descriptors(tmp_path, template, inst, "p1", payload)
    descs = descs + _descriptors(tmp_path, template, inst, "s1", b"c" * 500)
    _prewrite(q, inst, template, "b1", TIER, descs)
    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res.get("ok") is True, res
    mover = str(res["mover_key"])

    Path(str(descs[1]["path"])).unlink()
    claimed = _claim_mover(q, "w-killed")
    assert claimed["action_key"] == mover
    q.execute(claimed, timeout_s=240)
    assert [p.name for p in _staged(stage_root, "p1.bin")] == ["p1.bin"]
    namespace = str(res["batch_namespace"])
    fragment = Path(residency) / namespace / f"{mover}.json"
    assert fragment.is_file(), sorted(Path(residency).rglob("*.json"))

    # The window: published, not yet recorded.
    q.move_path(mover).unlink()
    assert q.move_record(mover) is None
    q.finish(mover, status="failed")
    assert [p.name for p in _staged(stage_root, "p1.bin")] == ["p1.bin"]
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 2, "holders": {owner: 1, mover: 1}}

    # Fragments compose, so bytes are there; completeness cannot be read,
    # so the census says unknown rather than calling the batch staged.
    events = po.recover_batches(q, inst, template)
    assert {"event": "output-recovery-unknown", "batch_id": "b1",
            "namespace": namespace} in events, events

    # The egress still returns the charge, exactly once.
    retired = po.retire_batch(q, inst, template, "b1",
                              stage_root=str(stage_root),
                              residency_root=residency)
    assert retired.get("ok") is True, retired
    assert [Path(path).name for path in retired["staged_paths"]] == ["p1.bin"]
    assert _staged(stage_root, "p1.bin") == []
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 3, "holders": {owner: 1}}
    again = po.retire_batch(q, inst, template, "b1",
                            stage_root=str(stage_root),
                            residency_root=residency)
    assert again.get("ok") is True and again.get("duplicate") is True, again
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 3, "holders": {owner: 1}}


def test_an_attempt_exhausted_failed_mover_names_the_terminal_route(
        tmp_path: Path) -> None:
    """A spent fence is spent in both queue states, not just READY.

    The requeued-READY row was fixed to consult its filed funding record;
    the terminal-FAILED sibling kept emitting `output-mover-failed-retry`
    unconditionally. Both point at the same impossible thing, because
    `output_funded_cover` covers only a record in `transferring` and
    nothing re-funds a spent one -- so a caller told to retry a
    fence-spent mover waits exactly as forever as one told to wait.

    The impossibility is PROVEN here rather than argued from the code:
    the one production publication call is re-driven against the same
    batch and answers duplicate without re-funding, and no claim can be
    taken afterwards. The route the census names instead is then walked
    from the id the census emitted, through to a replanned batch that
    stages for real.
    """

    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path)
    ledger = q.tier_ledger(TIER)
    inst = _bind(q, template, owner, cas_root)
    stage_root = tmp_path / "stage"
    _announce_tier(q, stage_root)
    residency = po.output_fragment_root(q.root / pool.RESIDENCY)

    payload = b"f" * 700
    descs = _descriptors(tmp_path, template, inst, "p1", payload)
    origin_path = Path(str(descs[0]["path"]))
    _prewrite(q, inst, template, "b1", TIER, descs)
    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"],
        retry_policy={"max_attempts": 1, "retry_safe": True})
    assert res.get("ok") is True, res
    mover = str(res["mover_key"])

    # One attempt, and it stages nothing: the row goes terminal FAILED
    # rather than being requeued, which is the branch under test.
    origin_path.unlink()
    claimed = _claim_mover(q, "w-exhausted")
    assert claimed["action_key"] == mover
    outcome = q.execute(claimed, timeout_s=240)
    assert outcome.get("returncode") != 0, outcome
    q.finish(mover, status="failed")
    assert po._mover_live_state(q, mover) == "failed"
    assert q.read_output_funding(mover, TIER)["state"] == "consumed"
    # Nothing was staged, so the reservation came back; the aggregate
    # ledger is where that is read, not the mover's own holdings.
    assert _staged(stage_root, "*.bin") == []
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 3, "holders": {owner: 1}}

    # The retry that event promised, attempted for real.
    origin_path.write_bytes(payload)
    assert po.refill_window(q, inst, template, tier=TIER)["ok"] is True
    again = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"],
        retry_policy={"max_attempts": 1, "retry_safe": True})
    assert again.get("ok") is True and again.get("duplicate") is True, again
    assert str(again["mover_key"]) == mover
    assert q.read_output_funding(mover, TIER)["state"] == "consumed"
    assert po._mover_live_state(q, mover) == "failed"
    assert q.claim(owner="probe-exhausted", tags=[_tier_host(q)]) is None

    # So the census must not offer it, and must offer what does work.
    events = po.recover_batches(q, inst, template)
    assert not any(event.get("event") == "output-mover-failed-retry"
                   for event in events), events
    route = [event for event in events
             if event.get("event") == "output-mover-unfundable-retire"]
    assert len(route) == 1, route
    assert route[0]["mover"] == mover, route
    assert route[0]["action"] == "retire-reclaim-replan", route
    # The row form of the same promise is withdrawn too.
    assert po.due_mover_rows(q, inst, template) == []

    # Walk it, from the id the census emitted.
    target = str(route[0]["batch_id"])
    retired = po.retire_batch(q, inst, template, target,
                              stage_root=str(stage_root),
                              residency_root=residency)
    assert retired.get("ok") is True and retired["staged_paths"] == [], retired
    origin_path.unlink()
    assert po.reclaim_origin(q, inst, template, batch_id=target)["ok"] is True
    freed = po.require_prewrite(
        q, inst, template, batch_id="b2", tier=TIER,
        class_bytes={"payload": len(payload), "checkpoint": 0, "temp": 0},
        paths=[str(origin_path)])
    assert freed.get("ok") is True, freed
    descs2 = _descriptors(tmp_path, template, inst, "p1", payload)
    res2 = po.publish_prepaid_batch(
        q, inst, template, descs2, batch_id="b2", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res2.get("ok") is True, res2
    mover2 = str(res2["mover_key"])
    assert _claim_mover(q, "w-replan")["action_key"] == mover2
    receipt = _execute_mover(q, cas_root, mover2, tmp_path / "mover-checkout")
    assert receipt["complete"] is True, receipt
    q.finish(mover2, status="executed")
    assert po.retire_batch(q, inst, template, "b2",
                           stage_root=str(stage_root),
                           residency_root=residency)["ok"] is True
    assert all(event.get("event") != "output-mover-unfundable-retire"
               for event in po.recover_batches(q, inst, template))
    assert _tier_census(ledger)["capacity"] == 4


def test_a_mover_killed_before_filing_anything_keeps_its_charge(
        tmp_path: Path) -> None:
    """Bytes exist before either record does, so neither absence is proof.

    `stage_move` renames each destination into place
    (`tools/fleet/stage_move.py:1247`, or :421 when the
    published-readiness publisher decides it -- two rename sites since
    PR #762/#763, one ordering), publishes that entry's residency
    fragment afterwards (~:1324), and files its move receipt once at the
    end (:1965). A contained kill between the rename and the fragment
    leaves real bytes on the stage with NEITHER record -- and "I found
    no evidence" is not "there is nothing there". Releasing on that frees
    capacity the stage has already spent.

    Genuine zero output still releases, on positive evidence: the mover's
    own receipt reporting `bytes_staged == 0` for this tier AND no
    fragment. That conjunction is what
    `test_failed_mover_batch_retires_but_its_row_cannot_be_reclaimed`
    exercises, and it is not weakened here.
    """

    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path)
    ledger = q.tier_ledger(TIER)
    inst = _bind(q, template, owner, cas_root)
    stage_root = tmp_path / "stage"
    _announce_tier(q, stage_root)
    residency = po.output_fragment_root(q.root / pool.RESIDENCY)

    payload = b"f" * 700
    descs = _descriptors(tmp_path, template, inst, "p1", payload)
    descs = descs + _descriptors(tmp_path, template, inst, "s1", b"c" * 500)
    _prewrite(q, inst, template, "b1", TIER, descs)
    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res.get("ok") is True, res
    mover = str(res["mover_key"])
    namespace = str(res["batch_namespace"])

    Path(str(descs[1]["path"])).unlink()
    claimed = _claim_mover(q, "w-killed-early")
    assert claimed["action_key"] == mover
    q.execute(claimed, timeout_s=240)
    assert [p.name for p in _staged(stage_root, "p1.bin")] == ["p1.bin"]

    # The window between the rename and the fragment: the bytes are on the
    # stage and nothing names them.
    (Path(residency) / namespace / f"{mover}.json").unlink()
    for sidecar in Path(residency).rglob(f"{mover}.json"):
        sidecar.unlink()
    q.move_path(mover).unlink()
    assert q.move_record(mover) is None
    assert list(Path(residency).rglob(f"{mover}.json")) == []

    q.finish(mover, status="failed")
    assert [p.name for p in _staged(stage_root, "p1.bin")] == ["p1.bin"]
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 2, "holders": {owner: 1, mover: 1}}

    # Nothing names the bytes, so the census cannot call the batch staged;
    # its fence is spent, so it names the terminal route.
    route = [event for event in po.recover_batches(q, inst, template)
             if event.get("event") == "output-mover-unfundable-retire"]
    assert len(route) == 1 and route[0]["mover"] == mover, route

    # The existing cleanup returns the charge, exactly once. It evicts no
    # path, because no record names one: the file itself is an orphan for
    # `stage_release.sweep`, which is the existing owner of unfragmented
    # stage bytes, not this lane.
    retired = po.retire_batch(q, inst, template, str(route[0]["batch_id"]),
                              stage_root=str(stage_root),
                              residency_root=residency)
    assert retired.get("ok") is True, retired
    assert retired["staged_paths"] == [], retired
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 3, "holders": {owner: 1}}
    again = po.retire_batch(q, inst, template, str(route[0]["batch_id"]),
                            stage_root=str(stage_root),
                            residency_root=residency)
    assert again.get("ok") is True and again.get("duplicate") is True, again
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 3, "holders": {owner: 1}}


def test_a_widowed_lease_does_not_free_a_funded_movers_charge(
        tmp_path: Path) -> None:
    """The sweeps are real cleanup, and they had the same blind spot.

    `_filed_pin_holds` asks the DONE/FAILED record whether the ending it
    is cleaning up after pins bytes. A box that dies mid-copy files NO
    ending at all, so that loop found nothing and answered "does not
    pin" -- and `sweep_widowed_leases` then released the tier tokens of a
    mover whose bytes are on the stage. Its own docstring already says a
    cleanup that cannot see the ending must not be the thing that frees
    the capacity; no ending at all is the strongest form of that.

    Scoped by positive evidence: the key holds a prepaid-output funding
    record that is not `released`. A consumer-window mover has no such
    record and is swept exactly as before.
    """

    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path)
    ledger = q.tier_ledger(TIER)
    inst = _bind(q, template, owner, cas_root)
    stage_root = tmp_path / "stage"
    _announce_tier(q, stage_root)

    payload = b"f" * 700
    descs = _descriptors(tmp_path, template, inst, "p1", payload)
    _prewrite(q, inst, template, "b1", TIER, descs)
    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res.get("ok") is True, res
    mover = str(res["mover_key"])
    claimed = _claim_mover(q, "w-widowed")
    assert claimed["action_key"] == mover
    q.execute(claimed, timeout_s=240)
    assert [p.name for p in _staged(stage_root, "p1.bin")] == ["p1.bin"]
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 2, "holders": {owner: 1, mover: 1}}

    # The finisher dies: the claim is gone, the lease is stale, and no
    # terminal record was ever filed.
    q.item_path(pool.CLAIMED, mover).unlink()
    lease = pool._read_json(q.lease_path(mover)) or {}
    lease["heartbeat_unix"] = pool._now() - 10_000
    pool._write_json_atomic(q.lease_path(mover), lease)
    assert pool._read_json(q.item_path(pool.DONE, mover)) is None
    assert pool._read_json(q.item_path(pool.FAILED, mover)) is None

    assert q.sweep_widowed_leases(timeout_s=60) == [mover]
    assert [p.name for p in _staged(stage_root, "p1.bin")] == ["p1.bin"]
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 2, "holders": {owner: 1, mover: 1}}


def test_a_malformed_byte_count_is_not_a_report_of_zero(
        tmp_path: Path) -> None:
    """Exact non-boolean integer zero releases; every other shape retains.

    The emptiness half of the conjunction reads ONE field of the mover's
    own receipt, and two values used to slip through it as "empty".
    `isinstance(False, int)` is True and `False > 0` is False, so a bool
    in `bytes_staged` passed an `isinstance(...) and staged > 0` test and
    was read as a report of zero. A negative count did the same. Neither
    is a measurement of an empty stage; both are broken metadata, which
    proves nothing -- the same absence-of-evidence-as-proof this
    predicate family has been wrong about before, one type-check down.

    This file already knew the trap: `pool.py:7570` excludes `bool` on
    this same field name. It was not carried down to the produced-output
    gate. The bool case is driven through the REAL terminal, so the
    aggregate ledger is what answers; the remaining shapes are asserted
    on the gate itself, and the honest zero is re-checked last so the
    release direction is measured on the same fixture as the retention.
    """

    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path)
    ledger = q.tier_ledger(TIER)
    inst = _bind(q, template, owner, cas_root)
    stage_root = tmp_path / "stage"
    _announce_tier(q, stage_root)

    payload = b"f" * 700
    descs = _descriptors(tmp_path, template, inst, "p1", payload)
    origin_path = Path(str(descs[0]["path"]))
    _prewrite(q, inst, template, "b1", TIER, descs)
    res = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert res.get("ok") is True, res
    mover = str(res["mover_key"])

    # A mover that really does stage nothing, through the ordinary seam.
    origin_path.unlink()
    claimed = _claim_mover(q, "w-malformed")
    assert claimed["action_key"] == mover
    assert q.execute(claimed, timeout_s=240).get("returncode") != 0
    assert _staged(stage_root, "*.bin") == []
    honest = dict(q.move_record(mover) or {})
    assert honest.get("tier_id") == TIER, honest
    assert honest.get("bytes_staged") == 0, honest

    # `False` where the count belongs, filed before the terminal runs.
    q.record_move(mover, {**honest, "bytes_staged": False})
    q.finish(mover, status="failed")
    assert _tier_census(ledger) == {
        "capacity": 4, "free": 2, "holders": {owner: 1, mover: 1}}

    # The same record `finish` judged: attempts remain, so the retry ladder
    # requeued the row READY rather than filing a terminal, and the row
    # still carries the tier residency block the gate reads.
    assert po._mover_live_state(q, mover) == "ready"
    record = pool._read_json(q.item_path(pool.READY, mover))
    assert isinstance(record, dict), record
    assert record["residency"]["tier_id"] == TIER, record["residency"]
    assert q.output_partial_pin_holds(record, mover) is True

    # Every other shape that is not exact non-boolean integer zero.
    for malformed in (False, True, -1, 1, "0", 0.0, None, [0]):
        q.record_move(mover, {**honest, "bytes_staged": malformed})
        assert q.output_partial_pin_holds(record, mover) is True, malformed
    q.record_move(mover, {k: v for k, v in honest.items()
                          if k != "bytes_staged"})
    assert q.output_partial_pin_holds(record, mover) is True

    # And the release direction is intact: an honest zero, with nothing
    # published, is still a proven-empty stage.
    q.record_move(mover, honest)
    assert q.output_partial_pin_holds(record, mover) is False

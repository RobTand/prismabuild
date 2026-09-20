"""Produced-output lifecycle correction (issue #743).

Focused scope only: prewrite/retire/safe_release lifecycle against the
accepted reader SDK. No commit-batch funding changes (liveness739), no
pbrun/pool publish (#735), no pool funding/membership, no tier_loop/movers,
no PQ writers.

Behavioral RED vs parent 241bf9a: the SDK-independent tests below FAIL on
the parent (bare terminal releases, missing SDK releases as
sdk-absent-structural, funding intents release on metadata absence) and
PASS here. SDK-connected tests skip on checkouts without the accepted
package and prove the corrected path here.
"""
from __future__ import annotations

import hashlib
import json
import secrets
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, produced_output as po  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402

try:
    from prismabuild import reader_lease as rlc  # coherent package only
    HAS_SDK = True
except ImportError:
    rlc = None  # type: ignore[assignment]
    HAS_SDK = False

STAGE_TIER = "prismabuild-stage:dl380g10"
STAGE_BARE = "stage_gib"
OWNER = "c" * 64


def _template(output_prefix: str, template_id: str = "life-v1") -> dict:
    return po.validate_template({
        "schema": po.TEMPLATE_SCHEMA_V1,
        "version": 1,
        "template_id": template_id,
        "output_prefix": output_prefix,
        "slots": {
            "boundary-0": {"class": "payload"},
            "checkpoint-0": {"class": "checkpoint"},
        },
        "durable_maxima": {
            "payload_max_bytes": 1 << 20,
            "checkpoint_max_bytes": 1 << 20,
            "temp_max_bytes": 1 << 20,
        },
        "working_demands": {
            STAGE_TIER: {"minimum_gib": 1, "window_gib": 2},
        },
        "permitted_tiers": [STAGE_TIER],
    })


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(STAGE_TIER, {STAGE_BARE: 8})
    return queue


def _publish_claim(queue: pool.PoolQueue, owner: str = OWNER) -> dict:
    queue.publish(action_key=owner, cas_root="/cas", worker_script="/w.py",
                  checkout_root="/co", resources={"cpu": 1, "mem_gb": 1})
    claimed = queue.claim(owner="fixture-worker")
    assert claimed is not None and claimed["action_key"] == owner
    return claimed


def _file_broker_control(queue: pool.PoolQueue, owner: str) -> dict:
    from prismabuild import resource_scope

    nonce = secrets.token_hex(16)
    scope = resource_scope.ResourceScope(
        owner, nonce, 1 * 1024 ** 3,
        queue.root / "telemetry" / f"{owner}.json")
    token = secrets.token_hex(32)
    unit = ("prismabuild-job"
            + hashlib.sha256((owner + nonce).encode()).hexdigest()[:32]
            + ".slice")
    scope._adopt_created_scope({
        "scope_id": unit, "token": token,
        "cgroup_path": str(Path("/sys/fs/cgroup/prismabuild.slice") / unit),
    })
    control = scope.control_record()
    path = queue.item_path(pool.CLAIMED, owner)
    live = pool._read_json(path)
    assert live is not None
    live["resource_scope"] = control
    pool._write_json_atomic(path, live)
    return control


def _bind(queue: pool.PoolQueue, template: dict, owner: str = OWNER):
    claimed = _publish_claim(queue, owner)
    control = _file_broker_control(queue, owner)
    env = {"PRISMABUILD_ACTION_KEY": owner,
           "PRISMABUILD_ACTION_NONCE": control["nonce"],
           "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
    po.declare_template(queue.root, template)
    instance = po.bind_instance(queue, template, owner_action_key=owner,
                                claim_snapshot=claimed, env=env)
    po.declare_instance(queue.root, instance)
    return {"instance": instance, "claimed": claimed, "control": control,
            "env": env}


def _finish_bare(queue: pool.PoolQueue, owner: str, claimed: dict) -> None:
    queue.finish(owner, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)


def test_missing_sdk_retains(tmp_path: Path) -> None:
    """No SDK object: production release retains (parent released okTrue)."""

    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance, claimed = bound["instance"], bound["claimed"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    _finish_bare(queue, OWNER, claimed)
    refused = po.safe_release_instance(queue, instance, template)
    assert refused["ok"] is False
    assert "lease-sdk-missing" in str(refused["refusal"])


def test_bare_terminal_without_proof_retains(tmp_path: Path) -> None:
    """ANY bare DONE by key alone is not containment (parent released)."""

    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance, claimed = bound["instance"], bound["claimed"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    _finish_bare(queue, OWNER, claimed)
    if not HAS_SDK:
        pytest.skip("needs accepted reader_lease")
    refused = po.safe_release_instance(
        queue, instance, template, lease_sdk=rlc)
    assert refused["ok"] is False
    assert "retain" in str(refused["refusal"])
    assert "lease-sdk" not in str(refused["refusal"])


def test_funding_intent_absent_mover_retains(tmp_path: Path) -> None:
    """Funding-only intent with an absent mover retains (parent released)."""

    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance, claimed = bound["instance"], bound["claimed"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    funding_dir = po._funding_dir(queue.root, instance)
    funding_dir.mkdir(parents=True, exist_ok=True)
    po._write_funding(funding_dir / "ghost.funding.json", {
        "batch_id": "ghost", "tier": STAGE_TIER,
        "mover_key": "a" * 64, "batch_gib": 1,
        "manifest_digest": "0" * 64})
    _finish_bare(queue, OWNER, claimed)
    # Without an SDK the parent released the ghost holders as
    # sdk-absent-structural; production retains on the missing SDK first.
    refused = po.safe_release_instance(queue, instance, template)
    assert refused["ok"] is False
    assert "lease-sdk-missing" in str(refused["refusal"])
    # With the coherent SDK the intent itself retains for the
    # funding/reconciliation lane -- metadata absence never proves the
    # mover wrote nothing.
    if HAS_SDK:
        sdk_refused = po.safe_release_instance(
            queue, instance, template, lease_sdk=rlc)
        assert sdk_refused == {"ok": False,
                               "refusal": "funding-intent-reconcile-retain",
                               "batch_id": "ghost"}


def test_prewrite_abort_keeps_headroom_and_duplicate_commit_stable(
        tmp_path: Path) -> None:
    """Abort proves exact path absence; duplicates never mint headroom."""

    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    target = str(origin / "boundary-0.pt")
    assert po.require_prewrite(
        queue, instance, template, batch_id="pw", tier=STAGE_TIER,
        class_bytes={"payload": 64, "checkpoint": 0, "temp": 0},
        paths=[target])["ok"] is True
    Path(target).write_bytes(b"A" * 64)
    refused = po.abort_prewrite(queue, instance, template, batch_id="pw")
    assert refused == {"ok": False, "refusal": "abort-files-present-retain",
                       "path": target}
    # Headroom still reserved: a competing reservation still refuses.
    assert po.require_prewrite(
        queue, instance, template, batch_id="other", tier=STAGE_TIER,
        class_bytes={"payload": 1 << 20, "checkpoint": 0, "temp": 0},
        paths=[str(origin / "other.pt")])["ok"] is False
    Path(target).unlink()
    assert po.abort_prewrite(queue, instance, template,
                             batch_id="pw") == {
        "ok": True, "batch_id": "pw", "aborted": True}
    # Durable-origin quota stays distinct from the tier window credit.
    assert po.checked_instance_maxima(template)["payload_max_bytes"] == 1 << 20
    assert po.owner_demand_terms(template) == {
        f"{STAGE_BARE}@{STAGE_TIER}": 2}


@pytest.mark.skipif(not HAS_SDK, reason="needs accepted reader_lease")
def test_wrong_nonce_terminal_retains(tmp_path: Path) -> None:
    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance, claimed = bound["instance"], bound["claimed"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    _finish_bare(queue, OWNER, claimed)
    # Terminal telemetry names another nonce: exact-attempt proof fails.
    done_path = queue.item_path(pool.DONE, OWNER)
    done = pool._read_json(done_path)
    assert isinstance(done, dict)
    done["resource_telemetry"] = {
        "action_key": OWNER, "nonce": "0" * 32,
        "scope_unit": "prismabuild-job" + "0" * 32 + ".slice",
        "host": "elsewhere"}
    pool._write_json_atomic(done_path, done)
    refused = po.safe_release_instance(
        queue, instance, template, lease_sdk=rlc)
    assert refused["ok"] is False


@pytest.mark.skipif(not HAS_SDK, reason="needs accepted reader_lease")
def test_unreadable_census_retains(tmp_path: Path) -> None:
    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance, claimed = bound["instance"], bound["claimed"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    _finish_bare(queue, OWNER, claimed)
    # A non-pin file under our owner namespace taints the SDK census while
    # the structural per-namespace scan stays empty.
    leases_dir = rlc.leases_root(queue) / OWNER
    leases_dir.mkdir(parents=True, exist_ok=True)
    (leases_dir / "junk.txt").write_text("not a pin")
    refused = po.safe_release_instance(
        queue, instance, template, lease_sdk=rlc)
    assert refused["ok"] is False
    assert "taint" in str(refused["refusal"]) or "retain" in str(
        refused["refusal"])


@pytest.mark.skipif(not HAS_SDK, reason="needs accepted reader_lease")
def test_current_claim_retains_and_pending_tickets_retain(
        tmp_path: Path) -> None:
    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    # Live claim on the exact attempt retains without reaching containment.
    refused = po.safe_release_instance(
        queue, instance, template, lease_sdk=rlc)
    assert refused == {"ok": False, "refusal": "owner-active-retain"}


@pytest.mark.skipif(not HAS_SDK, reason="needs accepted reader_lease")
def test_connected_finish_releases_once_and_failure_never_ok_true(
        tmp_path: Path, monkeypatch) -> None:
    """Real socket authority + pool finish -> proof -> release (GREEN)."""

    import threading

    import resource_broker
    from prismabuild import resource_scope

    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)

    class _Kernel:
        def __init__(self):
            self.groups = {}

        def create(self, scope, budget):
            self.groups[scope] = {"populated": False}
            return {"cgroup_path": f"/sys/fs/cgroup/prismabuild.slice/{scope}"}

        def stop(self, scope):
            self.groups[scope]["populated"] = False

        def empty(self, scope):
            return scope not in self.groups or not self.groups[scope]["populated"]

        def exists(self, scope):
            return scope in self.groups

        def release(self, scope):
            if self.groups[scope]["populated"]:
                raise ValueError("scope still populated")
            self.groups.pop(scope)

    authority = resource_broker.Authority(
        tmp_path / "broker-state", __import__("os").getuid(), _Kernel(),
        max_memory_bytes=1024 ** 3)
    endpoint = tmp_path / "broker.sock"
    server = resource_broker.Server(
        str(endpoint), resource_broker.Handler)
    server.authority = authority
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01},
        daemon=True)
    thread.start()
    try:
        monkeypatch.setattr(resource_scope, "BROKER_SOCKET", endpoint)
        owner = "d" * 64
        terms = po.owner_demand_terms(template)
        queue.publish(action_key=owner, cas_root="/cas",
                      worker_script="/w.py", checkout_root="/co",
                      resources={"cpu": 1, "mem_gb": 1, **terms},
                      produced_output_template=template)
        claimed = queue.claim(owner="life-worker")
        assert claimed is not None and claimed["action_key"] == owner
        nonce = secrets.token_hex(16)
        scope = resource_scope.ResourceScope(
            owner, nonce, 64 * 1024 ** 2, tmp_path / "telemetry.json",
            socket_path=endpoint)
        control = scope.create()
        live = pool._read_json(queue.item_path(pool.CLAIMED, owner))
        assert isinstance(live, dict)
        live["resource_scope"] = control
        pool._write_json_atomic(queue.item_path(pool.CLAIMED, owner), live)
        env = {"PRISMABUILD_ACTION_KEY": owner,
               "PRISMABUILD_ACTION_NONCE": control["nonce"],
               "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
        po.declare_template(queue.root, template)
        instance = po.bind_instance(
            queue, template, owner_action_key=owner,
            claim_snapshot=claimed, env=env)
        po.declare_instance(queue.root, instance)
        assert po.admit_instance(queue, instance, template)["ok"] is True
        terminal = queue.finish(owner, status="executed", detail={})
        assert terminal == queue.item_path(pool.DONE, owner)
        proof = rlc.read_scope_attestation(queue, owner, nonce)
        assert isinstance(proof, dict) and proof["scope_empty"] is True
        first = po.safe_release_instance(
            queue, instance, template, lease_sdk=rlc)
        assert first["ok"] is True and first["released"] == 0
        assert first["lease_proof"] == "sdk-census-clean"
        second = po.safe_release_instance(
            queue, instance, template, lease_sdk=rlc)
        assert second["ok"] is True and second["released"] == 0
        # A leftover batch-namespace token releases exactly once: craft a
        # retired batch record (mover already egressed, fragment gone) with
        # one remainder token, as a partial transfer would leave.
        digest = "f" * 64
        ns = po.batch_namespace(instance, "batch-left", digest)
        mover = "e" * 64
        commitments_path = po._commitments_path(queue.root, instance)
        record = {"batches": {
            "batch-left": {
                "manifest_digest": digest, "batch_namespace": ns,
                "tier": STAGE_TIER, "mover_key": mover,
                "class_bytes": {"payload": 0, "checkpoint": 0, "temp": 0},
                "retired": True}},
            "admission": {"minimum_gib": {STAGE_TIER: 1},
                          "window_gib": {STAGE_TIER: 2},
                          "bound_unix": 0.0}}
        po._write_commitments(commitments_path, record)
        assert queue.tier_ledger(STAGE_TIER).acquire(ns, {STAGE_BARE: 1})
        third = po.safe_release_instance(
            queue, instance, template, lease_sdk=rlc)
        assert third["ok"] is True and third["released"] == 1
        fourth = po.safe_release_instance(
            queue, instance, template, lease_sdk=rlc)
        assert fourth["ok"] is True and fourth["released"] == 0
        # A ledger failure retains instead of reporting ok True.
        assert queue.tier_ledger(STAGE_TIER).acquire(ns, {STAGE_BARE: 1})
        real_ledger = queue.tier_ledger

        def boom(tier_id):
            ledger = real_ledger(tier_id)
            if tier_id == STAGE_TIER:
                def failing(holder):
                    raise OSError("simulated ledger failure")

                ledger.release = failing  # type: ignore[method-assign]
            return ledger

        monkeypatch.setattr(queue, "tier_ledger", boom)
        failing = po.safe_release_instance(
            queue, instance, template, lease_sdk=rlc)
        assert failing["ok"] is False
        assert "unknown-retain" in str(failing["refusal"])
    finally:
        try:
            server.shutdown()
        finally:
            thread.join(timeout=10)
            server.server_close()

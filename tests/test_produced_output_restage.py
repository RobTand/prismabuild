"""Repeat materialization over one committed produced-output batch.

The bounded-window path stages a batch, lets a reader consume it, retires the
copy to give the window credit back, and -- when a later read needs the same
bytes -- stages the SAME batch again over the SAME origin files. The contract
audit proved the existing API cannot do that last step; these tests pin what
it does instead (the characterization), then exercise the one transition that
closes it (`produced_output.ensure_batch_materialized`).

Everything here runs on real primitives and tiny payloads: real sealed CAS
requests, the real `Pool.execute` worker seam driving the fleet's own
`stage_move`, the real `stage_release` egress, the real ledger, and real
`reader_lease` SDK pins. Nothing is stubbed and no shape is asserted in place
of a fact.

Fixture concessions (dev scope, all disclosed, same as the prepaid writer
integration suite this is modelled on): the producer request is a real sealed
CAS request over a fixture checkout (production's comes from the pbrun/PQ
submission machinery); `--unpaced` is passed explicitly through
`command_extra` because a sandbox has no ZFS pacer (production seals the
ordinary paced command); the claim is `PoolQueue.claim` directly rather than a
fleet worker loop.
"""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import socket
import sys
import threading
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import prismabuild.core as pb  # noqa: E402
import prismabuild.pool as pool  # noqa: E402
import prismabuild.produced_output as po  # noqa: E402
from prismabuild import residency_map as map_mod  # noqa: E402
import stage_release  # noqa: E402

try:
    from prismabuild import reader_lease as rlc
    HAS_SDK = True
except ImportError:  # pragma: no cover - the SDK is on main
    rlc = None  # type: ignore[assignment]
    HAS_SDK = False

TIER = "prismabuild-stage:dl380g10"
KIND = "stage_gib"
REPO = Path(__file__).resolve().parents[1]
DOWNSTREAM = "d" * 64


class _Crash(BaseException):
    """A process death, not an exception the lane may catch and convert."""


@pytest.fixture(autouse=True)
def _isolated_synthetic_launch_context(monkeypatch):
    """Standalone synthetic launch contexts never inherit an outer tuple."""

    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT", "PRISMABUILD_ACTION_KEY"):
        monkeypatch.delenv(name, raising=False)


# --------------------------------------------------------------------------
# Fixtures (same construction as tests/test_prepaid_writer_integration.py)
# --------------------------------------------------------------------------

def _template(prefix: str, window_gib: int = 1,
              maxima: int = 1 << 20) -> dict:
    body = {
        "schema": po.TEMPLATE_SCHEMA_V1,
        "version": 1,
        "template_id": "restage-v1",
        "output_prefix": prefix,
        "slots": {"s0": {"class": "payload"}, "s1": {"class": "checkpoint"}},
        "durable_maxima": {
            "payload_max_bytes": maxima,
            "checkpoint_max_bytes": maxima,
            "temp_max_bytes": maxima,
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
          cas_root: Path) -> dict:
    terms = po.owner_demand_terms(template)
    q.publish(action_key=owner, cas_root=str(cas_root),
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


def _descriptors(template: dict, inst: dict, tag: str, payload: bytes, *,
                 digest_mode: str = "null") -> list[dict]:
    """One descriptor over a real origin file.

    The DEV null digest is the DEFAULT here on purpose: it is the case the
    restage proof has to carry, because nothing in the manifest can then say
    the bytes are the committed ones.
    """

    origin = Path(template["output_prefix"])
    origin.mkdir(parents=True, exist_ok=True)
    path = origin / f"{tag}.bin"
    path.write_bytes(payload)
    slot = "s0" if tag.startswith("p") else "s1"
    cls = "payload" if slot == "s0" else "checkpoint"
    sha256 = None if digest_mode == "null" else hashlib.sha256(
        payload).hexdigest()
    return [po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": slot,
        "artifact_class": cls, "path": str(path),
        "bytes": len(payload), "sha256": sha256,
        "producer_generation": po.mint_generation(),
        "owner_action_key": inst["owner_action_key"],
        "owner_attempt": dict(inst["owner_attempt"]),
    }, template, inst)]


def _prewrite(q, inst, template, batch_id, descs) -> dict:
    classes = {"payload": 0, "checkpoint": 0, "temp": 0}
    for d in descs:
        classes[str(d["artifact_class"])] += int(d["bytes"])
    out = po.require_prewrite(q, inst, template, batch_id=batch_id,
                              tier=TIER, class_bytes=classes,
                              paths=sorted(str(d["path"]) for d in descs))
    assert out.get("ok") is True, out
    return out


def _announce_tier(q: pool.PoolQueue, stage_root: Path) -> None:
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


def _claim_mover(q: pool.PoolQueue, worker: str) -> dict:
    claimed = q.claim(owner=worker, tags=[_tier_host(q)])
    assert claimed is not None, "mover row must be claimable on its host"
    return claimed


def _producer_request(tmp_path: Path, cas_root: Path, template: dict) -> str:
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
        "task": {"definition_id": "tests/produced-restage-producer",
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


def _execute_mover(q: pool.PoolQueue, mover: str) -> dict:
    row = pool._read_json(q.item_path(pool.CLAIMED, mover))
    assert isinstance(row, dict), "mover row must be claimed to execute"
    outcome = q.execute(row, timeout_s=240)
    assert outcome.get("returncode") == 0, outcome
    receipt = q.move_record(mover)
    assert isinstance(receipt, dict), "mover recorded no receipt"
    return receipt


class _World:
    """One bound producer instance with an announced tier, ready to stage."""

    def __init__(self, tmp_path: Path, *, window_gib: int = 1,
                 gib: int = 4, maxima: int = 1 << 20):
        self.tmp = tmp_path
        self.cas_root = tmp_path / "cas"
        self.template = _template(str(tmp_path / "outputs"),
                                  window_gib=window_gib, maxima=maxima)
        self.owner = _producer_request(tmp_path, self.cas_root, self.template)
        self.q = _queue(tmp_path, gib=gib)
        self.inst = _bind(self.q, self.template, self.owner, self.cas_root)
        self.stage_root = tmp_path / "stage"
        _announce_tier(self.q, self.stage_root)
        self.ledger = self.q.tier_ledger(TIER)
        self.out_base = po.output_fragment_root(self.q.root / pool.RESIDENCY)
        self.publish_kwargs = dict(producer_action_key=self.owner,
                                   command_extra=["--unpaced"])

    # -- convenience wrappers -------------------------------------------
    def first_publish(self, batch_id: str, descs: list[dict]) -> dict:
        res = po.publish_prepaid_batch(
            self.q, self.inst, self.template, descs, batch_id=batch_id,
            tier=TIER, cas_root=self.cas_root, **self.publish_kwargs)
        assert res.get("ok") is True, res
        return res

    def run_mover(self, mover: str, worker: str) -> dict:
        claimed = _claim_mover(self.q, worker)
        assert claimed["action_key"] == mover, claimed["action_key"]
        receipt = _execute_mover(self.q, mover)
        assert receipt["complete"] is True, receipt
        self.q.finish(mover, status="executed")
        return receipt

    def retire(self, batch_id: str) -> dict:
        return po.retire_batch(
            self.q, self.inst, self.template, batch_id,
            stage_root=str(self.stage_root),
            residency_root=str(self.out_base))

    def ensure(self, batch_id: str) -> dict:
        return po.ensure_batch_materialized(
            self.q, self.inst, self.template, batch_id=batch_id,
            cas_root=self.cas_root, **self.publish_kwargs)

    def commitments(self) -> dict:
        path = po._commitments_path(self.q.root, self.inst)
        return json.loads(path.read_text())

    def entry(self, batch_id: str) -> dict:
        return self.commitments()["batches"][batch_id]

    def batch_record_path(self, batch_id: str) -> Path:
        return (Path(self.q.root) / "residency" / po.OUTPUT_BATCHES_SUBDIR
                / po.instance_namespace(self.inst) / f"{batch_id}.json")

    def pin(self, mover: str, manifest: str, total: int, *,
            token: str | None = None) -> dict:
        return rlc.acquire(
            self.q, consumer_action_key=po.batch_namespace(
                self.inst, "b1", manifest),
            attempt={"nonce": secrets.token_hex(16),
                     "scope_id": "downstream-scope"},
            tier_id=TIER, epoch="",
            span={"start_bytes": 0, "end_bytes": total},
            holder={"host": socket.gethostname(), "worker": "downstream",
                    "pid": os.getpid()},
            acquire_token=token or secrets.token_hex(16),
            covers=[{"mover_action_key": mover, "manifest_sha256": manifest}],
            expected=None, owner_action_key=DOWNSTREAM,
            residency_root=str(self.out_base))


# --------------------------------------------------------------------------
# 1. Characterization: what the EXISTING API does with a retired batch
# --------------------------------------------------------------------------

def test_the_existing_api_cannot_restage_a_retired_batch(tmp_path: Path) -> None:
    """The gap the audit found, measured on the shipping code path.

    After a real forward stage and a real retirement, the existing writer API
    is asked for those bytes again. It does not refuse and it does not stage:
    it answers a typed DUPLICATE naming the SPENT mover, whose fence is
    `consumed`, whose row is terminal, and whose staged file is gone. The only
    other route the existing API offers -- committing the same origins as a
    NEW logical batch -- charges the durable maxima a second time for one set
    of bytes. Neither is a re-materialization, which is why one had to be
    built.
    """

    world = _World(tmp_path, maxima=4096)
    payload = b"R" * 700
    descs = _descriptors(world.template, world.inst, "p1", payload)
    _prewrite(world.q, world.inst, world.template, "b1", descs)
    first = world.first_publish("b1", descs)
    mover0 = str(first["mover_key"])
    manifest = str(first["manifest_digest"])
    ns = str(first["batch_namespace"])
    world.run_mover(mover0, "w-char-1")
    assert (world.stage_root / "p1.bin").read_bytes() == payload
    assert world.retire("b1").get("ok") is True
    assert not (world.stage_root / "p1.bin").exists()

    # (a) The writer path answers a duplicate over the spent mover and stages
    #     nothing at all.
    again = po.publish_prepaid_batch(
        world.q, world.inst, world.template, descs, batch_id="b1", tier=TIER,
        cas_root=world.cas_root, **world.publish_kwargs)
    assert again.get("ok") is True, again
    assert again.get("duplicate") is True
    assert str(again["mover_key"]) == mover0
    record = world.q.read_output_funding(mover0, TIER)
    assert record is not None and str(record["state"]) == "consumed"
    assert po._mover_live_state(world.q, mover0) == pool.DONE
    assert map_mod.read_fragments(world.out_base, ns) == []
    assert not (world.stage_root / "p1.bin").exists()

    # ...and the recovery census names no route back to a staged copy: the
    # batch simply reads retired.
    events = po.recover_batches(world.q, world.inst, world.template)
    assert [e["event"] for e in events] == ["output-batch-retired"], events
    assert po.due_mover_rows(world.q, world.inst, world.template) == []

    # (b) The only remaining existing route is a SECOND logical batch over the
    #     same origin files, which charges the same bytes twice.
    committed_payload = po._class_sums(world.commitments()["batches"])["payload"]
    assert committed_payload == len(payload)
    second = po.require_prewrite(
        world.q, world.inst, world.template, batch_id="b2", tier=TIER,
        class_bytes={"payload": len(payload), "checkpoint": 0, "temp": 0},
        paths=[str(descs[0]["path"])])
    assert second.get("ok") is True, second
    outstanding = po._outstanding_sums(world.q.root, world.inst, None)
    assert outstanding["payload"] == 2 * len(payload), outstanding


# --------------------------------------------------------------------------
# 2. The transition: forward read -> retire -> second read, one durable charge
# --------------------------------------------------------------------------

@pytest.mark.skipif(not HAS_SDK, reason="needs the accepted reader_lease SDK")
def test_a_retired_batch_is_read_again_from_a_new_materialization(
        tmp_path: Path) -> None:
    """Forward read, retire, second read -- same bytes, no second charge.

    The whole transition on real primitives: a real mover stages the batch, a
    real SDK pin reads it, the real egress retires it and returns the window
    credit, `ensure_batch_materialized` stages the SAME batch again under a
    new sealed mover, and a real SDK pin reads it a second time. The durable
    origin charge is identical before and after -- one logical batch, one
    charge -- and the window credit spent by the successor is exactly
    ceil(bytes / GiB), the same as the first materialization's.
    """

    world = _World(tmp_path, window_gib=1, gib=4)
    payload = bytes(range(256)) * 4
    descs = _descriptors(world.template, world.inst, "p1", payload)
    total = len(payload)
    _prewrite(world.q, world.inst, world.template, "b1", descs)
    first = world.first_publish("b1", descs)
    mover0 = str(first["mover_key"])
    manifest = str(first["manifest_digest"])
    ns = str(first["batch_namespace"])
    charge_before = po._class_sums(world.commitments()["batches"])

    # The producer's window paid for it; nothing came from free.
    assert world.ledger.holder_tokens(world.owner).get(KIND, 0) == 0
    assert world.ledger.holder_tokens(mover0).get(KIND, 0) == 1
    world.run_mover(mover0, "w-fwd")
    assert (world.stage_root / "p1.bin").read_bytes() == payload

    # A real forward read.
    read1 = world.pin(mover0, manifest, total)
    assert read1.get("ok") is True, read1
    assert po.retire_batch(
        world.q, world.inst, world.template, "b1",
        stage_root=str(world.stage_root),
        residency_root=str(world.out_base)).get("refusal") == "egress-incomplete"
    assert rlc.release(world.q, read1["pin_id"], read1["ref_id"],
                       consumer_action_key=DOWNSTREAM,
                       residency_root=str(world.out_base)) is True

    retired = world.retire("b1")
    assert retired.get("ok") is True, retired
    assert str(retired["mover_key"]) == mover0
    assert int(retired["generation"]) == 0
    assert world.ledger.holder_tokens(mover0).get(KIND, 0) == 0
    assert not (world.stage_root / "p1.bin").exists()

    # The window credit came back to free; the producer re-takes its own
    # admitted window through the existing lifecycle primitive.
    refill = po.refill_window(world.q, world.inst, world.template, tier=TIER)
    assert refill.get("ok") is True, refill
    assert world.ledger.holder_tokens(world.owner).get(KIND, 0) == 1

    ensured = world.ensure("b1")
    assert ensured.get("ok") is True, ensured
    assert ensured["state"] == "materializing"
    assert int(ensured["generation"]) == 1
    mover1 = str(ensured["mover_key"])
    assert mover1 != mover0
    assert str(ensured["batch_namespace"]) == ns
    assert str(ensured["manifest_digest"]) == manifest
    # Exactly the same window price as the first materialization, taken from
    # the producer's window by transfer, never from free.
    assert world.ledger.holder_tokens(mover1).get(KIND, 0) == 1
    assert world.ledger.holder_tokens(world.owner).get(KIND, 0) == 0

    # The successor's own row is claimable through the prepaid cover even with
    # free exhausted, which is what proves the committed batch authorizes it.
    squatter = "5" * 64
    free = world.ledger.available().get(KIND, 0)
    if free:
        assert world.ledger.acquire(squatter, {KIND: free}) is True
    world.run_mover(mover1, "w-rev")
    assert (world.stage_root / "p1.bin").read_bytes() == payload

    # A real second read, over the new material.
    read2 = world.pin(mover1, manifest, total)
    assert read2.get("ok") is True, read2
    assert read2["pin_id"] != read1["pin_id"], (
        "a new materialization must not reuse the old generation's pin name")

    # One logical batch, one durable charge: unchanged across forward and
    # reverse staging.
    assert po._class_sums(world.commitments()["batches"]) == charge_before
    entry = world.entry("b1")
    assert entry["retired"] is True
    assert len(entry["materializations"]) == 1
    assert entry["materializations"][0]["state"] == "funded"
    assert entry["materializations"][0]["retired"] is False

    assert rlc.release(world.q, read2["pin_id"], read2["ref_id"],
                       consumer_action_key=DOWNSTREAM,
                       residency_root=str(world.out_base)) is True


@pytest.mark.skipif(not HAS_SDK, reason="needs the accepted reader_lease SDK")
def test_an_old_generation_reader_cannot_borrow_the_new_material(
        tmp_path: Path) -> None:
    """The successor's proof is the successor's; nothing inherits it.

    A reader that names the RETIRED generation's mover in its covers must not
    be pinned against bytes a later materialization put there. Object identity
    is `(namespace, epoch, path, length, materialization generation, digest,
    backend identity)` and the per-mover material sidecar is minted per
    publish run, so the old mover's proof is deleted by its egress and the new
    one is filed under a different key: the old-generation acquire has nothing
    to resolve.
    """

    world = _World(tmp_path)
    payload = b"G" * 512
    descs = _descriptors(world.template, world.inst, "p1", payload)
    _prewrite(world.q, world.inst, world.template, "b1", descs)
    first = world.first_publish("b1", descs)
    mover0, manifest = str(first["mover_key"]), str(first["manifest_digest"])
    world.run_mover(mover0, "w-gen0")
    assert world.retire("b1").get("ok") is True
    assert po.refill_window(world.q, world.inst, world.template,
                            tier=TIER).get("ok") is True
    ensured = world.ensure("b1")
    assert ensured.get("ok") is True, ensured
    mover1 = str(ensured["mover_key"])
    world.run_mover(mover1, "w-gen1")
    assert (world.stage_root / "p1.bin").read_bytes() == payload

    stale = world.pin(mover0, manifest, len(payload))
    assert stale.get("ok") is not True, stale
    fresh = world.pin(mover1, manifest, len(payload))
    assert fresh.get("ok") is True, fresh
    assert rlc.release(world.q, fresh["pin_id"], fresh["ref_id"],
                       consumer_action_key=DOWNSTREAM,
                       residency_root=str(world.out_base)) is True


# --------------------------------------------------------------------------
# 3. Bounded window: every prewrite is admitted, only reads cost credit
# --------------------------------------------------------------------------

def test_every_prewrite_completes_before_the_first_read_on_one_credit(
        tmp_path: Path) -> None:
    """K writes, one window credit: publication is what costs, not writing.

    `require_prewrite` admits the whole write plan without a physical token --
    that is why the bounded-window path delays FIRST publication until an
    actual read rather than adding an origin-only commit. Here four batches
    are fully written and budgeted while the tier ledger never moves, and only
    the batch a reader actually asks for takes the single window credit.
    """

    world = _World(tmp_path, window_gib=1, gib=4, maxima=1 << 16)
    free_before = world.ledger.available().get(KIND, 0)
    owner_before = world.ledger.holder_tokens(world.owner).get(KIND, 0)
    assert owner_before == 1, "the producer holds exactly its declared window"

    plans = []
    for index in range(4):
        payload = bytes([65 + index]) * (600 + index)
        descs = _descriptors(world.template, world.inst, f"p{index}", payload)
        _prewrite(world.q, world.inst, world.template, f"b{index}", descs)
        plans.append((f"b{index}", descs, payload))
        # Not one token has moved: K prewrites, zero physical credit.
        assert world.ledger.available().get(KIND, 0) == free_before
        assert world.ledger.holder_tokens(
            world.owner).get(KIND, 0) == owner_before

    # The first actual read publishes exactly one batch, and it costs exactly
    # one window credit -- the bound the whole path is sized against.
    batch_id, descs, payload = plans[0]
    first = world.first_publish(batch_id, descs)
    mover = str(first["mover_key"])
    assert world.ledger.holder_tokens(mover).get(KIND, 0) == 1
    assert world.ledger.holder_tokens(world.owner).get(KIND, 0) == 0
    assert world.ledger.available().get(KIND, 0) == free_before
    world.run_mover(mover, "w-window")
    assert (world.stage_root / "p0.bin").read_bytes() == payload
    # The other three are still uncommitted, still budgeted, still unstaged.
    for other_id, _descs, _payload in plans[1:]:
        assert other_id not in world.commitments()["batches"]


# --------------------------------------------------------------------------
# 4. Refusals: a live pin, and an origin that changed underneath
# --------------------------------------------------------------------------

@pytest.mark.skipif(not HAS_SDK, reason="needs the accepted reader_lease SDK")
def test_a_live_pin_blocks_both_the_retirement_and_the_successor(
        tmp_path: Path) -> None:
    """An ACTIVE pin holds the material, so no successor may exist.

    Retirement is what makes a successor legal, and a live pin is what makes
    retirement refuse. The two are one gate, not two: while a reader holds the
    current materialization, `retire_batch` refuses `egress-incomplete` and
    `ensure_batch_materialized` reports the copy live and publishes nothing --
    no second mover row, no second funding record, no second credit.
    """

    world = _World(tmp_path)
    payload = b"P" * 800
    descs = _descriptors(world.template, world.inst, "p1", payload)
    _prewrite(world.q, world.inst, world.template, "b1", descs)
    first = world.first_publish("b1", descs)
    mover0, manifest = str(first["mover_key"]), str(first["manifest_digest"])
    world.run_mover(mover0, "w-pinned")
    held = world.pin(mover0, manifest, len(payload))
    assert held.get("ok") is True, held

    blocked = world.retire("b1")
    assert blocked.get("ok") is not True
    assert blocked.get("refusal") == "egress-incomplete", blocked

    live = world.ensure("b1")
    assert live.get("ok") is True, live
    assert live["state"] == "live"
    assert str(live["mover_key"]) == mover0
    assert int(live["generation"]) == 0
    assert "materializations" not in world.entry("b1")
    assert world.ledger.holder_tokens(mover0).get(KIND, 0) == 1

    assert rlc.release(world.q, held["pin_id"], held["ref_id"],
                       consumer_action_key=DOWNSTREAM,
                       residency_root=str(world.out_base)) is True
    assert world.retire("b1").get("ok") is True


def test_a_same_size_origin_change_is_refused_before_any_funding(
        tmp_path: Path) -> None:
    """Identity, not length: a rewritten file of equal size never restages.

    The descriptor carries the DEV null digest, so length is all a size check
    could see -- and a rewritten file of identical length passes it. The proof
    is the identity tuple captured at first commit, rechecked here, and the
    refusal lands BEFORE a materialization row, a funding record or a single
    token moves.
    """

    world = _World(tmp_path)
    payload = b"A" * 640
    descs = _descriptors(world.template, world.inst, "p1", payload)
    origin = Path(str(descs[0]["path"]))
    _prewrite(world.q, world.inst, world.template, "b1", descs)
    first = world.first_publish("b1", descs)
    mover0 = str(first["mover_key"])
    world.run_mover(mover0, "w-same-size")
    assert world.retire("b1").get("ok") is True
    assert po.refill_window(world.q, world.inst, world.template,
                            tier=TIER).get("ok") is True

    # Same length, different bytes: exactly the case a size check cannot see.
    origin.write_bytes(b"B" * 640)
    assert os.lstat(origin).st_size == len(payload)

    owner_before = world.ledger.holder_tokens(world.owner).get(KIND, 0)
    free_before = world.ledger.available().get(KIND, 0)
    refused = world.ensure("b1")
    assert refused.get("ok") is False, refused
    assert refused.get("refusal") == "restage-origin-changed", refused
    assert refused.get("step") == "origin"
    # Nothing was filed and nothing was funded.
    assert "materializations" not in world.entry("b1")
    assert world.ledger.holder_tokens(world.owner).get(KIND, 0) == owner_before
    assert world.ledger.available().get(KIND, 0) == free_before


def test_a_batch_with_no_identity_proof_fails_restage_explicitly(
        tmp_path: Path) -> None:
    """A batch filed before the proof existed is never retroactively blessed.

    The current bytes may well be the right ones. Nothing can say so, and a
    restage that assumed it would copy whatever is there now under the
    authority of a manifest that never described it.
    """

    world = _World(tmp_path)
    payload = b"L" * 576
    descs = _descriptors(world.template, world.inst, "p1", payload)
    _prewrite(world.q, world.inst, world.template, "b1", descs)
    first = world.first_publish("b1", descs)
    world.run_mover(str(first["mover_key"]), "w-legacy")
    assert world.retire("b1").get("ok") is True
    assert po.refill_window(world.q, world.inst, world.template,
                            tier=TIER).get("ok") is True

    # Rewrite the filed record as a pre-proof one would have looked.
    path = world.batch_record_path("b1")
    record = json.loads(path.read_text())
    record.pop("origin_identity")
    os.chmod(path, 0o600)
    path.write_text(json.dumps(record, sort_keys=True,
                               separators=(",", ":")) + "\n")

    refused = world.ensure("b1")
    assert refused.get("ok") is False, refused
    assert refused.get("refusal") == "restage-origin-proof-missing", refused
    assert "materializations" not in world.entry("b1")


def test_a_reclaimed_origin_can_never_be_restaged(tmp_path: Path) -> None:
    """The durable charge was released on proven absence; so were the bytes."""

    world = _World(tmp_path)
    payload = b"C" * 512
    descs = _descriptors(world.template, world.inst, "p1", payload)
    origin = Path(str(descs[0]["path"]))
    _prewrite(world.q, world.inst, world.template, "b1", descs)
    first = world.first_publish("b1", descs)
    world.run_mover(str(first["mover_key"]), "w-reclaim")
    assert world.retire("b1").get("ok") is True
    origin.unlink()
    assert po.reclaim_origin(world.q, world.inst, world.template,
                             batch_id="b1") == {
        "ok": True, "batch_id": "b1", "reclaimed": True}
    refused = world.ensure("b1")
    assert refused.get("ok") is False, refused
    assert refused.get("refusal") == "origin-reclaimed-no-restage", refused


# --------------------------------------------------------------------------
# 5. Locking: the egress never runs under this lane's ownership lock
# --------------------------------------------------------------------------

def test_retirement_holds_no_output_prefix_lock_while_the_egress_runs(
        tmp_path: Path, monkeypatch) -> None:
    """The egress takes a mover transition lock and a stage-root ownership
    lock; this lane must not be holding an ownership lock of its own above it.

    `posix_lock.held` nests freely on the SAME thread, so an in-thread probe
    would report success whatever the code does. The probe therefore runs on
    another thread, where the per-path mutex is real exclusion: if retirement
    still held the output-prefix lock across `stage_release.evict`, this
    acquire would come back False.
    """

    world = _World(tmp_path)
    payload = b"K" * 704
    descs = _descriptors(world.template, world.inst, "p1", payload)
    _prewrite(world.q, world.inst, world.template, "b1", descs)
    first = world.first_publish("b1", descs)
    world.run_mover(str(first["mover_key"]), "w-lock")

    prefix = str(world.inst["output_prefix"])
    observed: list[bool] = []
    real_evict = stage_release.evict

    def _probing_evict(queue, mover_action_key, **kwargs):
        def _probe():
            with queue.stage_ownership_lock(prefix,
                                            blocking=False) as acquired:
                observed.append(bool(acquired))

        thread = threading.Thread(target=_probe)
        thread.start()
        thread.join(timeout=30)
        return real_evict(queue, mover_action_key, **kwargs)

    monkeypatch.setattr(stage_release, "evict", _probing_evict)
    retired = world.retire("b1")
    assert retired.get("ok") is True, retired
    assert observed == [True], (
        "the output-prefix ownership lock was held across the egress")

    # And the lock really does exclude another thread when it IS held, so the
    # assertion above is a measurement and not a vacuous True.
    control: list[bool] = []
    with world.q.stage_ownership_lock(prefix):
        def _probe_held():
            with world.q.stage_ownership_lock(prefix,
                                              blocking=False) as acquired:
                control.append(bool(acquired))

        thread = threading.Thread(target=_probe_held)
        thread.start()
        thread.join(timeout=30)
    assert control == [False]


# --------------------------------------------------------------------------
# 6. Replay, concurrency, and every crash prefix
# --------------------------------------------------------------------------

def _staged_world(tmp_path: Path, *, tag: str = "p1",
                  payload: bytes = b"X" * 600) -> tuple[_World, list[dict]]:
    """A world whose batch b1 is committed, staged, read and retired."""

    world = _World(tmp_path)
    descs = _descriptors(world.template, world.inst, tag, payload)
    _prewrite(world.q, world.inst, world.template, "b1", descs)
    first = world.first_publish("b1", descs)
    world.run_mover(str(first["mover_key"]), "w-setup")
    assert world.retire("b1").get("ok") is True
    assert po.refill_window(world.q, world.inst, world.template,
                            tier=TIER).get("ok") is True
    return world, descs


def _assert_funded_once(world: _World, mover: str) -> None:
    """Exactly one generation, one funding record, one credit."""

    entry = world.entry("b1")
    mats = entry["materializations"]
    assert len(mats) == 1, mats
    assert str(mats[0]["mover_key"]) == mover
    assert int(mats[0]["generation"]) == 1
    record = world.q.read_output_funding(mover, TIER)
    assert record is not None and str(record["state"]) == "transferring"
    assert len(record["tokens"]) == 1
    assert world.ledger.holder_tokens(mover).get(KIND, 0) == 1
    assert world.ledger.holder_tokens(world.owner).get(KIND, 0) == 0


def test_a_replayed_ensure_answers_the_same_generation(tmp_path: Path) -> None:
    """Calling it twice is calling it once: same key, same generation."""

    world, _descs = _staged_world(tmp_path)
    one = world.ensure("b1")
    assert one.get("ok") is True, one
    two = world.ensure("b1")
    assert two.get("ok") is True, two
    assert str(two["mover_key"]) == str(one["mover_key"])
    assert two["state"] == "live", two
    assert two.get("duplicate") is True
    assert int(two["generation"]) == 1
    _assert_funded_once(world, str(one["mover_key"]))


def test_concurrent_ensures_collapse_onto_one_epoch(tmp_path: Path) -> None:
    """Two callers, one materialization.

    The intent is appended under the output-prefix ownership lock, so the
    loser of that race finds the winner's filed row and resumes it rather than
    minting a second generation. A caller that loses one of the pool's
    non-blocking transition races defers by name; what must never happen is
    two generations, two funding records or two credits.
    """

    world, _descs = _staged_world(tmp_path)
    results: list[dict] = []
    lock = threading.Lock()

    def _run():
        out = world.ensure("b1")
        with lock:
            results.append(out)

    threads = [threading.Thread(target=_run) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)

    assert len(results) == 2, results
    entry = world.entry("b1")
    assert len(entry["materializations"]) == 1, entry["materializations"]
    mover = str(entry["materializations"][0]["mover_key"])
    for out in results:
        if out.get("ok"):
            assert str(out["mover_key"]) == mover, out
        else:
            assert str(out.get("refusal")) in (
                "funding-race-deferred", "materialization-live-retain"), out
    # Whatever the race did, a serial re-call finishes the one epoch.
    final = world.ensure("b1")
    assert final.get("ok") is True, final
    assert str(final["mover_key"]) == mover
    _assert_funded_once(world, mover)


@pytest.mark.parametrize("prefix_name", [
    "before-pool-intent", "before-publish", "after-ready", "after-fund",
    "before-commit",
])
def test_a_crash_at_every_prefix_resumes_without_double_funding(
        tmp_path: Path, prefix_name: str) -> None:
    """Kill it at each boundary; the restart re-drives the SAME intent.

    The materialization intent is filed under the ownership lock before any
    pool side effect, so every prefix leaves a durable resumption point naming
    the sealed successor key. A restart re-calls `ensure_batch_materialized`,
    which re-drives that exact row through the same idempotent steps: never a
    fresh generation, never a second funding record, never a second credit.
    """

    world, _descs = _staged_world(tmp_path)

    def _boom(*args, **kwargs):
        raise _Crash(prefix_name)

    # A context of its own, NOT the shared `monkeypatch` fixture: this test
    # must undo its own crash injection without also undoing the autouse
    # fixture's launch-context isolation. `monkeypatch.undo()` on the shared
    # instance restores this admitted action's own PRISMABUILD_ACTION_* tuple
    # into the environment, and the mover the resume publishes then refuses a
    # foreign reader identity -- a test artefact, not a lane defect.
    with pytest.MonkeyPatch.context() as crash:
        if prefix_name == "before-pool-intent":
            crash.setattr(world.q, "stage_output_intent", _boom)
        elif prefix_name == "before-publish":
            crash.setattr(po, "_publish_output_mover_row", _boom)
        elif prefix_name == "after-ready":
            crash.setattr(world.q, "fund_output_batch", _boom)
        elif prefix_name == "after-fund":
            crash.setattr(po, "_reconcile_materialization_funding", _boom)
        else:
            crash.setattr(po, "_mark_materialization_funded_locked", _boom)

        with pytest.raises(_Crash):
            world.ensure("b1")

    # The intent survived the crash and names the sealed successor. It is
    # still `intent` at every prefix: the funded flag is the LAST thing the
    # transition files, after the pool record reads transferring and the
    # credit is in the mover's hands.
    entry = world.entry("b1")
    mats = entry["materializations"]
    assert len(mats) == 1, mats
    crashed_mover = str(mats[0]["mover_key"])
    assert int(mats[0]["generation"]) == 1
    assert str(mats[0]["state"]) == "intent", mats

    resumed = world.ensure("b1")
    assert resumed.get("ok") is True, resumed
    assert str(resumed["mover_key"]) == crashed_mover
    assert int(resumed["generation"]) == 1
    assert resumed.get("resumed") is True
    _assert_funded_once(world, crashed_mover)

    # ...and the resumed successor is a real mover the fleet can run.
    world.run_mover(crashed_mover, f"w-{prefix_name}")
    assert (world.stage_root / "p1.bin").exists()


def test_an_unfunded_successor_keeps_its_intent_and_finishes_later(
        tmp_path: Path) -> None:
    """No window credit is invented: the intent stands and resumes.

    `ensure_batch_materialized` funds only by exact transfer from the
    producer's own window. With that window spent it reports the ordinary
    `tier-reservation-unavailable` -- and leaves its filed intent standing, so
    the refill the producer owes it is followed by a plain re-call that
    completes the same generation.
    """

    world = _World(tmp_path)
    payload = b"U" * 620
    descs = _descriptors(world.template, world.inst, "p1", payload)
    _prewrite(world.q, world.inst, world.template, "b1", descs)
    first = world.first_publish("b1", descs)
    world.run_mover(str(first["mover_key"]), "w-unfunded")
    assert world.retire("b1").get("ok") is True
    # Deliberately NOT refilled, and free is taken away so refill cannot help.
    assert world.ledger.holder_tokens(world.owner).get(KIND, 0) == 0
    squatter = "7" * 64
    free = world.ledger.available().get(KIND, 0)
    assert world.ledger.acquire(squatter, {KIND: free}) is True

    refused = world.ensure("b1")
    assert refused.get("ok") is False, refused
    assert refused.get("refusal") == "tier-reservation-unavailable", refused
    entry = world.entry("b1")
    assert len(entry["materializations"]) == 1
    pending = str(entry["materializations"][0]["mover_key"])
    assert str(entry["materializations"][0]["state"]) == "intent"
    # The census names the pending intent rather than calling the batch done.
    events = po.recover_batches(world.q, world.inst, world.template)
    assert [e["event"] for e in events] == [
        "output-materialization-intent-pending"], events

    assert world.ledger.release(squatter) >= 1
    assert po.refill_window(world.q, world.inst, world.template,
                            tier=TIER).get("ok") is True
    finished = world.ensure("b1")
    assert finished.get("ok") is True, finished
    assert str(finished["mover_key"]) == pending
    _assert_funded_once(world, pending)


# --------------------------------------------------------------------------
# 7. The censuses recognise the live successor
# --------------------------------------------------------------------------

def test_every_census_follows_the_active_materialization(
        tmp_path: Path) -> None:
    """retire, recover, due-rows, scope-tick and release all agree on WHICH.

    A restaged batch's own `retired` flag describes a copy that is already
    gone. Every path that used to read it must read the active materialization
    instead, or a live successor is reported as needing nothing while its
    window credit sits charged to a mover nobody is watching.
    """

    world, _descs = _staged_world(tmp_path)
    # Retired everywhere, before the successor exists.
    assert [e["event"] for e in po.recover_batches(
        world.q, world.inst, world.template)] == ["output-batch-retired"]
    assert po.due_mover_rows(world.q, world.inst, world.template) == []
    assert po.output_scope_tick(world.q, {}) == []

    ensured = world.ensure("b1")
    assert ensured.get("ok") is True, ensured
    mover1 = str(ensured["mover_key"])

    # Published and funded, not yet staged: a live mover to wait for, not a
    # retired batch and not a row to invent.
    events = po.recover_batches(world.q, world.inst, world.template)
    assert [e["event"] for e in events] == ["output-mover-live-wait"], events
    assert str(events[0]["mover"]) == mover1
    rows = po.due_mover_rows(world.q, world.inst, world.template)
    assert rows == [], rows  # already READY; nothing to publish

    world.run_mover(mover1, "w-census")
    staged = po.recover_batches(world.q, world.inst, world.template)
    assert [e["event"] for e in staged] == ["output-batch-staged"], staged
    tick = po.output_scope_tick(world.q, {})
    assert [e["event"] for e in tick] == ["output-batch-staged"], tick
    assert int(tick[0]["generation"]) == 1

    # A live successor is an active batch: release must retain.
    if HAS_SDK:
        released = po.safe_release_instance(
            world.q, world.inst, world.template, lease_sdk=rlc)
        assert released.get("ok") is False
        assert released.get("refusal") == "active-batches-retain", released

    # A new writer may not claim the origin paths while the successor reads
    # them -- ownership follows the active materialization, not the flag.
    blocked = po.require_prewrite(
        world.q, world.inst, world.template, batch_id="b2", tier=TIER,
        class_bytes={"payload": 600, "checkpoint": 0, "temp": 0},
        paths=[str(Path(world.template["output_prefix"]) / "p1.bin")])
    assert blocked.get("ok") is False, blocked
    assert blocked.get("refusal") == "prewrite-path-owned-by-live-batch"
    assert str(blocked["owner_kind"]) == "batch"

    # Retiring the batch now retires the SUCCESSOR, by name.
    retired = world.retire("b1")
    assert retired.get("ok") is True, retired
    assert str(retired["mover_key"]) == mover1
    assert int(retired["generation"]) == 1
    assert world.ledger.holder_tokens(mover1).get(KIND, 0) == 0
    entry = world.entry("b1")
    assert entry["materializations"][0]["retired"] is True
    assert isinstance(entry["materializations"][0]["staged_paths"], list)
    assert [e["event"] for e in po.recover_batches(
        world.q, world.inst, world.template)] == ["output-batch-retired"]


def test_a_malformed_materialization_list_retains_everywhere(
        tmp_path: Path) -> None:
    """Unknown ownership fails RETAIN: no census reads it as "nothing here"."""

    world, _descs = _staged_world(tmp_path)
    ensured = world.ensure("b1")
    assert ensured.get("ok") is True, ensured
    path = po._commitments_path(world.q.root, world.inst)
    record = json.loads(path.read_text())
    record["batches"]["b1"]["materializations"] = [{"mover_key": "nope"}]
    path.write_text(json.dumps(record, sort_keys=True) + "\n")

    assert [e["event"] for e in po.recover_batches(
        world.q, world.inst, world.template)] == ["output-recovery-unknown"]
    assert po.due_mover_rows(world.q, world.inst, world.template) == []
    assert [e["event"] for e in po.output_scope_tick(world.q, {})] == [
        "output-recovery-unknown"]
    retired = world.retire("b1")
    assert retired.get("ok") is False, retired
    assert str(retired.get("refusal")).startswith("unknown-retain"), retired
    state = po.materialization_state(world.q, world.inst, world.template,
                                     batch_id="b1")
    assert state.get("ok") is False, state
    if HAS_SDK:
        released = po.safe_release_instance(
            world.q, world.inst, world.template, lease_sdk=rlc)
        assert released.get("ok") is False
        assert str(released.get("refusal")).startswith("unknown-retain")


def _two_real_generations(tmp_path: Path) -> tuple[_World, str, str]:
    """A real batch carrying two real, PB-sealed materialization rows.

    Generation 1 staged, read and retired; generation 2 staged and live. Every
    key, tier and staged path below comes from that real history -- the
    corruption tests permute a genuine record rather than inventing one.
    """

    world, _descs = _staged_world(tmp_path)
    one = world.ensure("b1")
    assert one.get("ok") is True, one
    world.run_mover(str(one["mover_key"]), "w-m1")
    assert world.retire("b1").get("ok") is True
    assert po.refill_window(world.q, world.inst, world.template,
                            tier=TIER).get("ok") is True
    two = world.ensure("b1")
    assert two.get("ok") is True, two
    world.run_mover(str(two["mover_key"]), "w-m2")
    return world, str(one["mover_key"]), str(two["mover_key"])


def _assert_retains(world: _World) -> None:
    """Every census and every reclaim path refuses this batch."""

    assert [e["event"] for e in po.recover_batches(
        world.q, world.inst, world.template)] == ["output-recovery-unknown"]
    assert po.due_mover_rows(world.q, world.inst, world.template) == []
    assert [e["event"] for e in po.output_scope_tick(world.q, {})] == [
        "output-recovery-unknown"]
    retired = world.retire("b1")
    assert retired.get("ok") is False, retired
    assert str(retired.get("refusal")).startswith("unknown-retain"), retired
    state = po.materialization_state(world.q, world.inst, world.template,
                                     batch_id="b1")
    assert state.get("ok") is False, state
    ensured = world.ensure("b1")
    assert ensured.get("ok") is False, ensured
    if HAS_SDK:
        released = po.safe_release_instance(
            world.q, world.inst, world.template, lease_sdk=rlc)
        assert released.get("ok") is False, released
        assert str(released.get("refusal")).startswith("unknown-retain")


def _install(world: _World, rows: list[dict], *,
             batch_retired: bool = True) -> None:
    """Write a permuted history back over the real commitments record."""

    path = po._commitments_path(world.q.root, world.inst)
    record = json.loads(path.read_text())
    record["batches"]["b1"]["materializations"] = rows
    record["batches"]["b1"]["retired"] = batch_retired
    path.write_text(json.dumps(record, sort_keys=True) + "\n")


def test_a_live_earlier_generation_under_a_retired_latest_retains(
        tmp_path: Path) -> None:
    """[gen1 live, gen2 retired] is shape-valid and must still fail RETAIN.

    Every row passes the per-row check, so nothing local objects; reading the
    LAST row alone would answer "retired" and authorize a reclaim while
    generation 1 still owns a stage copy. The history is illegal as a whole.
    """

    world, _m1, _m2 = _two_real_generations(tmp_path)
    rows = world.entry("b1")["materializations"]
    assert len(rows) == 2, rows
    assert rows[0]["retired"] is True and rows[1]["retired"] is False
    _install(world, [{**rows[0], "retired": False},
                     {**rows[1], "retired": True}])
    _assert_retains(world)


def test_two_live_generations_retain(tmp_path: Path) -> None:
    """Two live rows reach the active read the same way. They must not."""

    world, _m1, _m2 = _two_real_generations(tmp_path)
    rows = world.entry("b1")["materializations"]
    _install(world, [{**rows[0], "retired": False}, dict(rows[1])])
    _assert_retains(world)


def test_a_successor_over_an_unretired_first_copy_retains(
        tmp_path: Path) -> None:
    """A successor cannot exist while the batch's own first copy is live."""

    world, _m1, _m2 = _two_real_generations(tmp_path)
    rows = world.entry("b1")["materializations"]
    _install(world, [dict(rows[0]), dict(rows[1])], batch_retired=False)
    _assert_retains(world)


def test_a_successor_repeating_a_spent_mover_key_retains(
        tmp_path: Path) -> None:
    """The old terminal key stays terminal: it can never name a successor."""

    world, _m1, _m2 = _two_real_generations(tmp_path)
    entry = world.entry("b1")
    rows = entry["materializations"]
    first_key = str(entry["mover_key"])
    assert len(first_key) == 64
    _install(world, [dict(rows[0]),
                     {**rows[1], "mover_key": first_key}])
    _assert_retains(world)
    _install(world, [dict(rows[0]),
                     {**rows[1], "mover_key": str(rows[0]["mover_key"])}])
    _assert_retains(world)

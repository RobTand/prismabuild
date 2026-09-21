"""A produced-output mover seals the producer's OWN checkout snapshot.

Live regression, 2026-09-21 (one-shot Stage A cycle). A pbrun producer request
carried ``params.checkout_snapshot`` (commit ``23ccdf60...``) and its sealed
input; ``produced_output._seal_output_mover`` built the mover's params from
``cwd`` alone, so the child's sealed request said nothing about which tree it
runs from. The row it was published under *did* materialize the snapshot --
``_producer_launch_context`` reuses the producer row's addressing -- and
``core._verify_pbrun_checkout_identity`` therefore fell through to the
``fleet/pbrun`` closure-stamp proof: it compared the materialized snapshot
tree against the producer's pre-snapshot stamp (the source HEAD plus its
working-tree delta). No materialized snapshot can satisfy that. The worker
refused the row in 0.76 s, before ``stage_move`` moved a byte, and the mover
was requeued ready until its owner timed out.

The child's proof has to be the SNAPSHOT proof, which belongs to
``params.checkout_snapshot`` and says the tree is the sealed commit, clean,
with its sealed ancestry. pbrun's own movement construction already carries
that param (``movement_actions.seal_movement_action`` copies
``_MOVEMENT_PARAM_KEYS``); the produced-output lane simply never gave it one.
These tests seal the real thing: a producer request built by pbrun's own
stage-A/stage-B sealers over a real dirty worktree, a real v2 bundle in a real
CAS, the production ``publish_prepaid_batch`` / ``ensure_batch_materialized``
calls, the real ``materialize._execution_checkout`` tree, the ordinary
``core.preflight_action`` the worker runs, and the real ``Pool.execute``
worker seam.

Fixture concessions (dev scope, same as the prepaid writer integration suite
this is modelled on): ``--unpaced`` is passed explicitly through
``command_extra`` because a sandbox has no ZFS pacer (production seals the
ordinary paced command); the claim is ``PoolQueue.claim`` directly rather than
a fleet worker loop; pbrun's submitter handles (``pbrun.SH``) are redirected
into the fixture so ``freeze_action_template`` ingests into a temp CAS.
"""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "tools" / "fleet"))

import pbrun  # noqa: E402
import prismabuild.core as pb  # noqa: E402
import prismabuild.materialize as materialize  # noqa: E402
import prismabuild.pool as pool  # noqa: E402
import prismabuild.produced_output as po  # noqa: E402
from test_prepaid_writer_integration import (  # noqa: E402
    KIND, TIER, _announce_tier, _bind, _broker_control, _claim_mover,
    _descriptors, _prewrite, _producer_request, _queue, _template,
)

#: What a missing child record used to produce, and what must never come back:
#: the legacy stamp proof comparing a materialized snapshot to a pre-snapshot
#: closure identity.
LEGACY_STAMP_REFUSAL = (
    "live pbrun checkout identity differs from its sealed stamp")


@pytest.fixture(autouse=True)
def _isolated_synthetic_launch_context(monkeypatch):
    """Standalone synthetic launch contexts never inherit an outer tuple."""

    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT", "PRISMABUILD_ACTION_KEY"):
        monkeypatch.delenv(name, raising=False)


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True, text=True, check=True,
    ).stdout


def _source_repo(tmp_path: Path, subdirectory: str) -> Path:
    """A small Git worktree, dirt included, in the shape pbrun seals.

    The uncommitted edit is the point: pbrun seals the tree it is GIVEN, so
    the sealed commit is not the submitter's HEAD and the producer's own
    closure stamp can never describe the materialized snapshot. The declared
    cwd -- and so the snapshot's ``subdirectory`` -- carries the payload the
    mover is checked against.
    """

    root = tmp_path / "producer-src"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.name", "PrismaBuild test")
    _git(root, "config", "user.email", "t@example.invalid")
    cwd = root if subdirectory == "." else root / subdirectory
    cwd.mkdir(parents=True, exist_ok=True)
    (cwd / "task.py").write_text("print('producer')\n", encoding="utf-8")
    (cwd / "payload.txt").write_text(
        "sealed by the producer\n", encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "producer source")
    (cwd / "payload.txt").write_text(
        "sealed by the producer, uncommitted\n", encoding="utf-8")
    return root


def _read_request(cas_root: Path, key: str) -> dict:
    """The sealed request a worker would read, in contract form."""

    path = Path(cas_root) / "requests" / key[:2] / f"{key}.json"
    return pb.validate_action(json.loads(path.read_text()))


def _execute(world, mover_key: str) -> dict:
    """``Pool.execute`` over the claimed row, surfacing the worker's stderr.

    The same real seam the fleet's worker loop uses (materialize the row's
    checkout, launch the row's worker script, run the sealed argv). The
    stderr tail is in the assertion message because at the base revision the
    failure IS that message: the worker's checkout-identity preflight.
    """

    row = pool._read_json(world.q.item_path(pool.CLAIMED, mover_key))
    assert isinstance(row, dict) and row["action_key"] == mover_key
    outcome = world.q.execute(row, timeout_s=240)
    assert outcome.get("returncode") == 0, (
        f"the sealed mover must run: rc={outcome.get('returncode')} "
        f"stderr={str(outcome.get('stderr'))[-2000:]}")
    receipt = world.q.move_record(mover_key)
    assert isinstance(receipt, dict), "mover recorded no receipt"
    return receipt


class _Producer:
    """One pbrun-sealed producer bound to a live queue, tier and instance."""

    def __init__(self, tmp_path: Path, monkeypatch, *,
                 subdirectory: str = ".") -> None:
        self.tmp = tmp_path
        self.template = _template(str(tmp_path / "outputs"))
        envelope = tmp_path / "produced-template.json"
        envelope.write_text(json.dumps(self.template, sort_keys=True))
        source = _source_repo(tmp_path, subdirectory)
        cwd = source if subdirectory == "." else source / subdirectory
        # ``freeze_action_template`` seals against the submitter's shared
        # handles; redirect them into the fixture, never the live CAS.
        monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
        demand = {"cpu": 1, "mem_gb": 1,
                  **po.owner_demand_terms(self.template)}
        frozen = pbrun.freeze_action_template(
            command=["/bin/true"], cwd=cwd, logical_cwd=subdirectory,
            demand=demand, placement={"required_tags": []},
            variables={"PATH": "/usr/bin:/bin"}, determinism="stochastic",
            retry_policy={"max_attempts": 1, "retry_safe": False},
            host_class=None, measurement=False, transport="pool",
            pool_measurement_class=False, data_manifest_path=None,
            produced_output_template_path=str(envelope),
            checkout_snapshot_max_bytes=512 * 1024 * 1024, snapshot_refs=[],
            exclusive=False, gpu_memory_gb=None, execution_timeout_s=None,
            progress=None, profile=None, container_image_refs=(),
            wrapper_dir=tmp_path / "wrapper")
        self.cas = frozen["cas"]
        self.action = pbrun.seal_action_from_template(frozen)
        self.cas.publish_action_request(self.action)
        self.key = str(self.action["action_key"])
        self.snapshot = self.action["params"]["checkout_snapshot"]
        assert self.snapshot["schema"] == pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V2
        assert self.snapshot["input"] in self.action["inputs"]
        self.q = _queue(tmp_path, gib=4)
        # The producer ROW carries the same snapshot addressing the live
        # pbrun submission published: this is what the mover inherits.
        self.q.publish(
            action_key=self.key, cas_root=str(self.cas.root),
            worker_script=str(REPO / "tools" / "prismabuild_worker.py"),
            checkout_snapshot=self.snapshot, resources=demand,
            produced_output_template=self.template)
        claimed = self.q.claim(owner="w-producer")
        assert claimed is not None and claimed["action_key"] == self.key
        control = _broker_control(self.q, self.key)
        env = {"PRISMABUILD_ACTION_KEY": self.key,
               "PRISMABUILD_ACTION_NONCE": control["nonce"],
               "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
        po.declare_template(self.q.root, self.template)
        self.inst = po.bind_instance(self.q, self.template,
                                     owner_action_key=self.key,
                                     claim_snapshot=claimed, env=env)
        po.declare_instance(self.q.root, self.inst)
        assert po.admit_instance(self.q, self.inst, self.template)["ok"] is True
        self.stage_root = tmp_path / "stage"
        _announce_tier(self.q, self.stage_root)

    def publish_kwargs(self) -> dict:
        return {"producer_action_key": self.key,
                "command_extra": ["--unpaced"]}

    def first_publish(self, batch_id: str, descs: list[dict]) -> dict:
        out = po.publish_prepaid_batch(
            self.q, self.inst, self.template, descs, batch_id=batch_id,
            tier=TIER, cas_root=self.cas.root, **self.publish_kwargs())
        assert out.get("ok") is True, out
        return out

    def materialize(self, key: str, action: dict):
        """The tree the row builds, through the materializer workers use."""

        item = {"action_key": key, "cas_root": str(self.cas.root),
                "checkout_snapshot": action["params"]["checkout_snapshot"]}
        return materialize._execution_checkout(
            item, local_checkout_root=self.tmp / "checkouts")


@pytest.mark.parametrize("subdirectory", [".", "package"])
def test_the_first_mover_seals_the_producers_checkout_snapshot(
        tmp_path: Path, monkeypatch, subdirectory: str) -> None:
    """Publication + the real worker seam, on the materialized snapshot."""

    world = _Producer(tmp_path, monkeypatch, subdirectory=subdirectory)
    payload = bytes(range(256)) * 4  # 1024 real bytes
    descs = _descriptors(tmp_path, world.template, world.inst, "p1", payload)
    _prewrite(world.q, world.inst, world.template, "b1", TIER, descs)
    first = world.first_publish("b1", descs)
    mover_key = str(first["mover_key"])

    mover = _read_request(world.cas.root, mover_key)
    assert mover["task"]["definition_id"] == "fleet/pbrun"
    # The child's sealed params are the producer's OWN validated record: the
    # exact commit, parent, refs, subdirectory and input row.
    assert mover["params"].get("checkout_snapshot") == world.snapshot, (
        "the produced mover did not seal the producer's checkout snapshot, so "
        "its request cannot prove the tree the row materializes")
    assert world.snapshot["input"] in mover["inputs"]
    assert world.snapshot["subdirectory"] == subdirectory
    # The row it will be claimed under materializes that same snapshot.
    row = pool._read_json(world.q.item_path(pool.READY, mover_key))
    assert row is not None and row["checkout_snapshot"] == world.snapshot
    # ...and nothing else of the producer's params was copied wholesale.
    assert "produced_output_template" not in mover["params"]
    assert mover["params"]["command"] != world.action["params"]["command"]
    assert mover["params"]["demand"] != world.action["params"]["demand"]

    with world.materialize(mover_key, mover) as root:
        # The producer's own proof passes on this tree...
        pb.preflight_action(world.action, cas_root=world.cas.root,
                            checkout_root=root)
        # ...and so does the child's, because the child carries the record.
        pb.preflight_action(mover, cas_root=world.cas.root,
                            checkout_root=root)
        # A tree that is no longer the sealed commit is refused, and the
        # refusal is the snapshot proof -- not the legacy stamp.
        (root / "payload.txt").write_text("tampered\n", encoding="utf-8")
        with pytest.raises(
                pb.ActionContractError, match="differs from its sealed commit"
        ) as refusal:
            pb.preflight_action(mover, cas_root=world.cas.root,
                                checkout_root=root)
        assert LEGACY_STAMP_REFUSAL not in str(refusal.value)

    with world.materialize(mover_key, mover) as root:
        # A CLEAN tree whose HEAD is not the sealed commit is refused by the
        # identity half of the snapshot proof. An empty commit keeps the
        # snapshot's own tree -- closure stamp included -- and moves only
        # HEAD, so the refusal is the proof under test rather than the
        # closure check that runs before it.
        repository = Path(_git(root, "rev-parse", "--show-toplevel").strip())
        _git(repository, "-c", "user.name=PrismaBuild test",
             "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false",
             "commit", "-q", "--allow-empty", "-m", "a later commit")
        with pytest.raises(
                pb.ActionContractError, match="differs from its sealed commit"
        ):
            pb.preflight_action(mover, cas_root=world.cas.root,
                                checkout_root=root)

    # The REAL worker seam: Pool.execute materializes the row's snapshot,
    # preflights the sealed request and runs the sealed argv.
    claimed = _claim_mover(world.q, "w-first-snapshot")
    assert claimed["action_key"] == mover_key
    receipt = _execute(world, mover_key)
    assert receipt["complete"] is True, receipt
    assert receipt["bytes_staged"] == len(payload)
    assert (world.stage_root / "p1.bin").read_bytes() == payload
    world.q.finish(mover_key, status="executed")


def test_a_repeat_materialization_seals_the_same_snapshot(
        tmp_path: Path, monkeypatch) -> None:
    """The successor mover seals the record too, and proves the same tree."""

    world = _Producer(tmp_path, monkeypatch)
    payload = b"R" * 512
    descs = _descriptors(tmp_path, world.template, world.inst, "p1", payload)
    _prewrite(world.q, world.inst, world.template, "b1", TIER, descs)
    first = world.first_publish("b1", descs)
    mover0 = str(first["mover_key"])
    claimed = _claim_mover(world.q, "w-gen0-snapshot")
    assert claimed["action_key"] == mover0
    receipt = _execute(world, mover0)
    assert receipt["complete"] is True, receipt
    world.q.finish(mover0, status="executed")
    retired = po.retire_batch(
        world.q, world.inst, world.template, "b1",
        stage_root=str(world.stage_root),
        residency_root=po.output_fragment_root(world.q.root / pool.RESIDENCY))
    assert retired.get("ok") is True, retired
    refill = po.refill_window(world.q, world.inst, world.template, tier=TIER)
    assert refill.get("ok") is True, refill

    ensured = po.ensure_batch_materialized(
        world.q, world.inst, world.template, batch_id="b1",
        cas_root=world.cas.root, **world.publish_kwargs())
    assert ensured.get("ok") is True, ensured
    assert int(ensured["generation"]) == 1
    mover1_key = str(ensured["mover_key"])
    assert mover1_key != mover0
    mover1 = _read_request(world.cas.root, mover1_key)
    assert mover1["params"].get("checkout_snapshot") == world.snapshot
    row1 = pool._read_json(world.q.item_path(pool.READY, mover1_key))
    assert row1 is not None and row1["checkout_snapshot"] == world.snapshot

    with world.materialize(mover1_key, mover1) as root:
        pb.preflight_action(mover1, cas_root=world.cas.root,
                            checkout_root=root)
        assert (root / "payload.txt").read_text() == (
            "sealed by the producer, uncommitted\n")


def test_a_declared_snapshot_that_is_not_inheritable_is_refused(
        tmp_path: Path) -> None:
    """Malformed, or an input the child does not inherit: fail closed.

    Both are refused at SEAL time by ``_seal_output_mover``'s use of the
    existing ``core.validate_pbrun_checkout_snapshot`` and the child's own
    input list -- never sealed for a worker to discover after a claim.
    """

    template = _template(str(tmp_path / "outputs"))
    valid = {
        "schema": pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1,
        "commit": "a" * 40, "subdirectory": ".",
        "input": {"id": pb.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID,
                  "sha256": "b" * 64, "bytes": 4096},
    }
    cases = [
        ({**valid, "commit": "not-an-object-id"},
         "producer-checkout-snapshot-invalid"),
        (valid, "producer-checkout-snapshot-input-missing"),
    ]
    for index, (declared, expected) in enumerate(cases):
        root = tmp_path / f"case-{index}"
        root.mkdir()
        cas_root = root / "cas"
        owner = _declared_snapshot_producer(root, cas_root, template, declared)
        q = _queue(root, gib=4)
        inst = _bind(q, template, owner, cas_root)
        descs = _descriptors(root, template, inst, "p1", b"x" * 128)
        _prewrite(q, inst, template, "b1", TIER, descs)
        out = po.publish_prepaid_batch(
            q, inst, template, descs, batch_id="b1", tier=TIER,
            cas_root=cas_root, producer_action_key=owner,
            command_extra=["--unpaced"])
        assert out.get("ok") is False, (index, out)
        assert out.get("step") == "parent-request", (index, out)
        assert expected in str(out.get("refusal")), (index, out)


def test_a_producer_without_a_snapshot_seals_the_legacy_mover(
        tmp_path: Path) -> None:
    """Absence stays absence: a non-snapshot producer's child is unchanged.

    The synthetic producer this suite has always used declares no
    ``params.checkout_snapshot``; its row is addressed by ``checkout_root``.
    The mover must keep that shape -- the legacy ``fleet/pbrun`` stamp proof
    is the one that legitimately applies to it -- rather than inventing a
    record or refusing the batch.
    """

    cas_root = tmp_path / "cas"
    template = _template(str(tmp_path / "outputs"))
    owner = _producer_request(tmp_path, cas_root, template)
    q = _queue(tmp_path, gib=4)
    inst = _bind(q, template, owner, cas_root)
    _announce_tier(q, tmp_path / "stage")
    descs = _descriptors(tmp_path, template, inst, "p1", b"L" * 256)
    _prewrite(q, inst, template, "b1", TIER, descs)
    out = po.publish_prepaid_batch(
        q, inst, template, descs, batch_id="b1", tier=TIER,
        cas_root=cas_root, producer_action_key=owner,
        command_extra=["--unpaced"])
    assert out.get("ok") is True, out
    mover = _read_request(cas_root, str(out["mover_key"]))
    assert "checkout_snapshot" not in mover["params"]
    assert mover["params"]["cwd"] == "."
    row = pool._read_json(q.item_path(pool.READY, str(out["mover_key"])))
    assert row is not None and row.get("checkout_root") is not None
    assert f"{KIND}@{TIER}" in row["resources"]


def _declared_snapshot_producer(tmp_path: Path, cas_root: Path, template: dict,
                                snapshot: object) -> str:
    """One sealed producer request whose params declare ``snapshot``.

    Deliberately NOT the real pbrun path: it exists to hand the seal-time
    validator a record the real path cannot produce (the positive path above
    uses ``freeze_action_template``). Its declared inputs are only the
    produced-output template envelope, which is the "input not inherited"
    case for a valid record, and the malformed record never gets that far.
    """

    cas = pb.PrismaBuildCAS(cas_root)
    envelope = tmp_path / "declared-template.json"
    envelope.write_text(json.dumps(template, sort_keys=True))
    template_input, _ = cas.ingest_input(
        envelope, input_id=pb.PRODUCED_OUTPUT_TEMPLATE_INPUT_ID)
    declaration = po.build_declaration(template, template_input)
    checkout = tmp_path / "declared-checkout"
    checkout.mkdir(parents=True, exist_ok=True)
    (checkout / "task.py").write_text("print('producer')\n", encoding="utf-8")
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/declared-snapshot-producer",
                 "definition_version": "v1", "task_class": "generation",
                 "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": ["/bin/true"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [template_input],
        "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {"cwd": ".", "command": ["/bin/true"],
                   "checkout_snapshot": snapshot,
                   "produced_output_template": declaration},
        "environment": {"variables": {"PATH": "/usr/bin:/bin"},
                        "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    }
    action = pb.seal_action(body)
    cas.publish_action_request(action)
    return str(action["action_key"])

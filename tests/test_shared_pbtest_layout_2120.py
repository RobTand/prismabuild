"""#2120: opt-in pbtest shares proved source, never an attempt's writable state.

The worker contract exercised here is deliberately private.  A real pbtest
seal is opted in through pbtest._source_readonly_action, a private sealed-recipe
transform; no SDK argument or arbitrary-pbrun sharing permission is implied.

W3's materializer context yields checkout_root, an opaque generation, and
preflight(action).  The latter still validates each action, returning the core
worker attestation; only the generation's snapshot proof may be reused.  Worker
integration is exercised through PoolQueue.execute, not a second layout API.
PRISMABUILD_RESULT_PATH is the worker-owned absolute declared-result destination
for capture.  Callers must not be able to seal that override.
"""
from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
import os
from pathlib import Path
import threading

import pytest

from test_pbtest import pbtest  # establishes the existing fleet/source imports
import test_pbtest_identifies_its_checkout_once as plan_fixture
import test_producer_snapshot_is_proven as snapshot_fixture
from test_producer_snapshot_is_proven import sealed  # noqa: F401 -- pytest fixture
from prismabuild import core, materialize, pool
from prismabuild.digest_primitives import canonical_sha256, raw_sha256
from prismabuild.movement_actions import standard_capture_argv


MODE = "pbtest-source-readonly-v1"


def _reseal(action, change):
    body = deepcopy(action)
    body.pop("action_key")
    change(body)
    return core.seal_action(body)


def _item(action, cas):
    return {
        "action_key": action["action_key"],
        "cas_root": str(cas.root),
        "checkout_snapshot": action["params"]["checkout_snapshot"],
    }


def _shared():
    factory = getattr(materialize, "_shared_source_checkout", None)
    assert callable(factory), "#2120 needs a reusable, proved shared-source lease"
    return factory


def test_one_generation_clones_and_proves_once_but_attests_each_action(
        sealed, tmp_path, monkeypatch):
    action, cas, _ = sealed
    sibling = _reseal(action, lambda body: body["params"].update(shard=2))
    counts = Counter()

    def counted(name, function):
        def call(*args, **kwargs):
            counts[name] += 1
            return function(*args, **kwargs)
        return call

    git = materialize._run_materializer_git

    def materializer_git(argv, **kwargs):
        # The existing materializer uses init + fetch, not git clone.
        if "clone" in argv or "init" in argv:
            counts["materialization"] += 1
        return git(argv, **kwargs)

    monkeypatch.setattr(materialize, "_run_materializer_git", materializer_git)
    monkeypatch.setattr(core, "git_checkout_identity",
                        counted("identity", core.git_checkout_identity))
    monkeypatch.setattr(core, "_verify_pbrun_checkout_ancestry",
                        counted("ancestry", core._verify_pbrun_checkout_ancestry))
    factory = _shared()
    roots, generations, attestations = [], [], []
    # Keep a reader alive: W3 need not retain idle generations indefinitely.
    with factory(_item(action, cas), local_checkout_root=tmp_path / "local"):
        for candidate in (action, sibling, action):
            with factory(_item(candidate, cas),
                         local_checkout_root=tmp_path / "local") as lease:
                roots.append(lease.checkout_root)
                generations.append(lease.generation)
                attestations.append(lease.preflight(candidate))
    assert roots[0] == roots[1] == roots[2]
    assert generations[0] == generations[1] == generations[2]
    assert generations[0] is not None
    assert counts == {"materialization": 1, "identity": 1, "ancestry": 1}
    assert [a["action_key"] for a in attestations] == [
        action["action_key"], sibling["action_key"], action["action_key"]]
    assert attestations[0]["attestation_sha256"] != attestations[1]["attestation_sha256"]


@pytest.mark.parametrize("defect", ["closure", "missing-input", "snapshot-binding"])
def test_warm_snapshot_proof_does_not_skip_the_next_actions_contract(
        sealed, tmp_path, defect):
    action, cas, _ = sealed

    def corrupt(body):
        if defect == "closure":
            closure = body["code_closure"]
            closure["files"][0]["sha256"] = "0" * 64
            # Keep the seal internally valid: refusal belongs to per-action
            # checkout validation, not seal construction or fixture setup.
            closure["closure_sha256"] = canonical_sha256({
                "schema": closure["schema"], "files": closure["files"]})
        elif defect == "missing-input":
            body["inputs"].append({"id": "unavailable-shard-input",
                                   "sha256": "0" * 64, "bytes": 1})
        else:
            body["inputs"] = []

    bad = _reseal(action, corrupt)
    with _shared()(_item(action, cas),
                   local_checkout_root=tmp_path / "local") as lease:
        lease.preflight(action)
        with pytest.raises(core.ActionContractError):
            lease.preflight(bad)
        # A rejected action must not poison the valid generation's proof.
        assert lease.preflight(action)["action_key"] == action["action_key"]


# This code is committed/snapshotted by the existing pbtest fixture and runs as
# the actual shard, not as a mocked worker.  The external append-only observation
# file also makes an automatic replay of a source-writing test observable.
PROBE = '''import json, os, pathlib, sys, time

def test_probe(tmp_path, capsys):
    # Only the opted-in concurrent test seals these external rendezvous vars.
    # The first shard cannot finish before the second reaches its test body.
    peers = None
    if os.environ.get("PB2120_BARRIER_DIR"):
        barrier = pathlib.Path(os.environ["PB2120_BARRIER_DIR"])
        expected = int(os.environ["PB2120_BARRIER_PARTIES"])
        (barrier / pathlib.Path(__file__).name).touch()
        deadline = time.monotonic() + 20
        while len(list(barrier.iterdir())) < expected:
            assert time.monotonic() < deadline, "concurrent shards did not overlap"
            time.sleep(0.02)
        peers = len(list(barrier.iterdir()))
    record = {
        "barrier_peers": peers,
        "file": pathlib.Path(__file__).name,
        "cwd": str(pathlib.Path.cwd()),
        "tmp": os.environ.get("TMPDIR"),
        "pycache": sys.pycache_prefix,
        "dontwritebytecode": sys.dont_write_bytecode,
        "basetemp": str(tmp_path.parent),
        "result": os.environ.get("PRISMABUILD_RESULT_PATH"),
        "saw_previous_write": pathlib.Path("source-write-2120").exists(),
    }
    with open(os.environ["PB2120_OBSERVATIONS"], "a") as stream:
        stream.write(json.dumps(record) + "\\n")
    with capsys.disabled():
        print("PB2120 shard " + record["file"], flush=True)
    if os.environ.get("PB2120_WRITE_SOURCE") == "1":
        pathlib.Path("source-write-2120").write_text("must not reach another shard")
'''


@pytest.fixture
def pbtest_plan(tmp_path, monkeypatch):
    original = plan_fixture._checkout
    # Use the worker's existing local-root configuration, never a host cache.
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "worker-checkouts")
    monkeypatch.setattr(materialize, "LOCAL_CHECKOUT_ROOT", tmp_path / "worker-checkouts")

    def checkout(path, files):
        root = original(path, files)
        for test in sorted((root / "tests").glob("test_*.py")):
            test.write_text(PROBE, encoding="utf-8")
        return root

    monkeypatch.setattr(plan_fixture, "_checkout", checkout)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    observations = tmp_path / "observations.jsonl"
    code, _, store = plan_fixture._counted_run(
        tmp_path, monkeypatch, shards=2, wait_s=2,
        extra=("--tmpdir", str(scratch), "--env",
               f"PB2120_OBSERVATIONS={observations}"))
    # The fixture publishes but has no claiming worker.  This is a timeout,
    # not a refused plan; both real seals must have reached the store.
    assert code == 1
    actions = plan_fixture._sealed(store)
    assert len(actions) == 2
    assert all(action["task"]["definition_id"] == "fleet/pbrun" for action in actions)
    assert all(action["params"].get("checkout_snapshot") for action in actions)
    return actions, core.PrismaBuildCAS(store / "cas"), observations


def _opt_in(action):
    transform = getattr(pbtest, "_source_readonly_action", None)
    assert callable(transform), "#2120 needs an explicit sealed read-only capture recipe"
    sealed = transform(action)
    assert sealed["params"]["execution_layout"] == MODE
    assert sealed["code_closure"] == action["code_closure"]
    assert sealed["params"]["checkout_snapshot"] == action["params"]["checkout_snapshot"]
    assert sealed["task"]["result_path"] == action["task"]["result_path"]
    core.validate_action(sealed)
    return sealed


def _execute(actions, cas, tmp_path, concurrent=False):
    queue = pool.PoolQueue(tmp_path / "executing-queue")
    outcomes, items = [], []
    worker = Path(__file__).resolve().parents[1] / "tools" / "prismabuild_worker.py"
    for action in actions:
        cas.publish_action_request(action)
        queue.publish(
            action_key=action["action_key"], cas_root=cas.root,
            checkout_snapshot=action["params"]["checkout_snapshot"],
            worker_script=worker,
            resources={"cpu": 1, "mem_gb": 1},
            retry_safe=True,
            retry_scope=action["params"].get("retry_policy", {}).get("retry_scope"),
        )
        item = queue.claim(owner="pb2120-test-worker",
                           capacity={"cpu": 2, "mem_gb": 4})
        assert item is not None
        assert item["action_key"] == action["action_key"]
        if concurrent:
            items.append(item)
        else:
            outcomes.append(queue.execute(item, timeout_s=60))
    if concurrent:
        # Admit every shard before launching; ordered futures retain the action
        # order even though their probe observations are append-order records.
        with ThreadPoolExecutor(max_workers=len(items)) as executor:
            futures = [executor.submit(queue.execute, item, timeout_s=60)
                       for item in items]
            outcomes = [future.result() for future in futures]
    return outcomes


def _observed(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def _payload(cas, action):
    receipt = cas.lookup(action)
    assert receipt is not None, "the shard's declared result must reach CAS"
    output = receipt["result"]
    return output["sha256"], cas._blob_path(output["sha256"]).read_bytes()


def test_genuine_pbtest_wrong_cwd_is_rejected_after_warm_proof(
        pbtest_plan, tmp_path, monkeypatch):
    raw, cas, _ = pbtest_plan
    good = _opt_in(raw[0])
    bad = _reseal(good, lambda body: body["params"].update(cwd="elsewhere"))
    base = tmp_path / "cwd-workers"
    with _shared()(_item(good, cas), local_checkout_root=base) as anchor:
        anchor.preflight(good)

        def no_identity(*args, **kwargs):
            pytest.fail("warm per-action cwd check reran generation Git proof")

        monkeypatch.setattr(core, "git_checkout_identity", no_identity)
        with _shared()(_item(bad, cas), local_checkout_root=base) as lease:
            assert lease.checkout_root == anchor.checkout_root
            with pytest.raises(core.ActionContractError, match="cwd"):
                lease.preflight(bad)


@pytest.mark.parametrize("fault", ["schema", "cwd"])
def test_pbrun_bad_stamp_is_rejected_after_warm_snapshot_proof(
        tmp_path, monkeypatch, fault):
    # Small real Git snapshot, with malformed stamp bytes genuinely included
    # in both the closure and snapshot (not an accidental closure mismatch).
    checkout = snapshot_fixture._producer_checkout(tmp_path)
    stamp_path = checkout / (core.PBRUN_STAMP_PREFIX + "2120.json")
    stamp = {"cwd": ".", **core.git_checkout_identity(checkout)}
    if fault == "schema":
        stamp["schema"] = "not-a-pbrun-stamp-field"
    else:
        stamp["cwd"] = "elsewhere"
    stamp_path.write_text(json.dumps(stamp))
    producer = snapshot_fixture._producer_action(checkout)

    def include_stamp(body):
        body["code_closure"] = core.build_code_closure(
            checkout, ["task.py", stamp_path.name])

    producer = _reseal(producer, include_stamp)
    cas = core.PrismaBuildCAS(tmp_path / "stamp-cas")
    producer, _ = snapshot_fixture.fleet_submit.seal_checkout_into_action(
        producer, cas=cas, checkout_root=checkout)

    def pbrun(body):
        body["task"]["definition_id"] = "fleet/pbrun"
        body["params"]["cwd"] = "."

    bad = _reseal(producer, pbrun)
    base = tmp_path / "stamp-workers"
    with _shared()(_item(producer, cas), local_checkout_root=base) as anchor:
        anchor.preflight(producer)

        def no_identity(*args, **kwargs):
            pytest.fail("bad stamp check reran generation Git proof")

        monkeypatch.setattr(core, "git_checkout_identity", no_identity)
        with _shared()(_item(bad, cas), local_checkout_root=base) as lease:
            assert lease.checkout_root == anchor.checkout_root
            with pytest.raises(core.ActionContractError, match="stamp"):
                lease.preflight(bad)


def test_opted_in_real_shards_share_cwd_but_not_writable_paths_or_results(
        pbtest_plan, tmp_path, monkeypatch):
    raw, cas, observations = pbtest_plan
    barrier = tmp_path / "shard-barrier"
    barrier.mkdir()

    def rendezvous(body):
        body["environment"]["variables"].update({
            "PB2120_BARRIER_DIR": str(barrier),
            "PB2120_BARRIER_PARTIES": "2",
        })

    actions = [_reseal(_opt_in(action), rendezvous) for action in raw]
    counts = Counter()
    lock = threading.Lock()
    original_git = materialize._run_materializer_git
    original_identity = core.git_checkout_identity

    def materializer_git(argv, **kwargs):
        with lock:
            for operation in ("init", "checkout"):
                if operation in argv:
                    counts[operation] += 1
        return original_git(argv, **kwargs)

    def identity(*args, **kwargs):
        with lock:
            counts["identity"] += 1
        return original_identity(*args, **kwargs)

    # Count only worker materialization/proof, never fixture planning Git.
    monkeypatch.setattr(materialize, "_run_materializer_git", materializer_git)
    monkeypatch.setattr(core, "git_checkout_identity", identity)
    base = tmp_path / "worker-checkouts"
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", base)
    # Keep a source reader alive through every assertion. Idle retention is
    # not part of the shared-generation contract.
    with _shared()(_item(actions[0], cas), local_checkout_root=base) as anchor:
        source = anchor.checkout_root
        anchor.preflight(actions[0])
        outcomes = _execute(actions, cas, tmp_path, concurrent=True)
        assert all(outcome["status"] == "executed" for outcome in outcomes), outcomes
        assert counts == {"init": 1, "checkout": 1, "identity": 1}
        records = _observed(observations)
        assert len(records) == 2
        assert all(record["barrier_peers"] == 2 for record in records)
        assert all(Path(record["cwd"]) == source for record in records), (
            "#2120 must reuse source, not merely make private clones read-only")
        assert all(record["dontwritebytecode"] for record in records)
        for key in ("tmp", "basetemp", "result"):
            paths = [record[key] for record in records]
            assert all(paths), f"worker did not supply private {key}: {records}"
            assert len(set(paths)) == 2, f"attempts share {key}"
            assert all(Path(path).is_absolute() and
                       not Path(path).is_relative_to(source) for path in paths)
        # PYTHONDONTWRITEBYTECODE alone is sufficient; a pycache prefix is
        # optional, but if supplied it cannot point back into shared source.
        bytecode_paths = [record["pycache"] for record in records if record["pycache"]]
        assert all(not Path(path).is_relative_to(source) for path in bytecode_paths)
        # Independent action receipts and distinct declared results, not just
        # two successful launcher exits. Probe append order is nondeterministic.
        assert len({action["action_key"] for action in actions}) == 2
        assert len({cas.receipt_path(action["action_key"]) for action in actions}) == 2
        receipts = [cas.lookup(action) for action in actions]
        assert all(receipt is not None for receipt in receipts)
        assert len({receipt["receipt_sha256"] for receipt in receipts}) == 2
        assert all(receipt["action_key"] == action["action_key"]
                   for action, receipt in zip(actions, receipts))
        payloads = [_payload(cas, action) for action in actions]
        assert payloads[0][0] != payloads[1][0]
        for record in records:
            marker = ("PB2120 shard " + record["file"]).encode()
            matching = [data for _, data in payloads if marker in data]
            assert len(matching) == 1
            assert b"1 passed" in matching[0]
        # Neither helper compilation nor Python/pytest caches may change the
        # proved checkout, including Git-ignored cache files.
        assert source.is_dir()
        assert not (source / "source-write-2120").exists()
        assert all(not record["saw_previous_write"] for record in records)
        assert not (source / ".cache").exists()
        assert not list(source.rglob("__pycache__"))
        assert not (source / ".pytest_cache").exists()
        assert all(not (source / action["task"]["result_path"]).exists()
                   for action in actions)
        # The explicit postcheck must not count as another worker proof.
        assert original_identity(source) == {
            "head": actions[0]["params"]["checkout_snapshot"]["commit"],
            "dirty_sha256": raw_sha256(b""),
        }


@pytest.mark.parametrize("arbitrary_pbrun", [False, True])
def test_without_opt_in_keeps_private_checkouts(pbtest_plan, tmp_path, arbitrary_pbrun):
    actions, cas, observations = pbtest_plan

    def private(body):
        body["params"].pop("execution_layout", None)
        if arbitrary_pbrun:
            command = [body["params"]["command"][0], "-c",
                       "import json,os,pathlib; "
                       "pathlib.Path('ordinary-source-write').write_text('allowed'); "
                       "f=open(os.environ['PB2120_OBSERVATIONS'],'a'); "
                       "f.write(json.dumps({'cwd':str(pathlib.Path.cwd())})+'\\n'); f.close(); "
                       "print('ordinary pbrun')"]
            body["params"]["command"] = command
            body["task"]["argv"] = standard_capture_argv(
                command, body["task"]["result_path"],
                path_prefix=body["environment"]["variables"]["PATH"].split(os.pathsep)[0])

    # Neither ordinary pbtest nor a general source-writing pbrun opts in by
    # resemblance.  Source writes must keep working on the private path.
    actions = [_reseal(action, private) for action in actions]
    outcomes = _execute(actions, cas, tmp_path)
    assert all(outcome["status"] == "executed" for outcome in outcomes), outcomes
    records = _observed(observations)
    assert len(records) == 2
    assert records[0]["cwd"] != records[1]["cwd"]


def test_source_write_fails_without_replay_and_next_shard_gets_clean_source(
        pbtest_plan, tmp_path):
    raw, cas, observations = pbtest_plan
    writer = _reseal(_opt_in(raw[0]), lambda body: body["environment"]["variables"].update(
        PB2120_WRITE_SOURCE="1"))
    reader = _opt_in(raw[1])
    outcomes = _execute([writer, reader], cas, tmp_path)
    records = _observed(observations)
    # Permission refusal or detection/quarantine can end the first attempt;
    # neither permits silently re-running a test that already had side effects.
    assert len(records) == 2, "a source-writing shard was replayed"
    assert records[0]["file"] != records[1]["file"]
    assert outcomes[0]["status"] == "failed", outcomes
    assert outcomes[1]["status"] == "executed", outcomes
    # A denied write may leave the same clean generation reusable.  What
    # matters is failure without a private replay, and a clean next reader.
    assert not records[1]["saw_previous_write"]


def test_caller_cannot_seal_the_workers_result_override(pbtest_plan, tmp_path):
    raw, cas, observations = pbtest_plan
    hostile = tmp_path / "caller-chosen-result.json"

    def override(body):
        body["environment"]["variables"]["PRISMABUILD_RESULT_PATH"] = str(hostile)

    # Refusal may occur at sealing or worker admission, but must precede argv.
    try:
        action = _reseal(_opt_in(raw[0]), override)
        outcomes = _execute([action], cas, tmp_path)
    except (core.ActionContractError, pool.PoolContractError) as exc:
        assert "PRISMABUILD_RESULT_PATH" in str(exc) or "reserved" in str(exc).lower()
    else:
        assert outcomes[0]["status"] != "executed", outcomes
        assert "PRISMABUILD_RESULT_PATH" in str(outcomes) or "reserved" in str(outcomes).lower()
    assert not observations.exists(), "reserved-variable conflict reached shard argv"
    assert not hostile.exists()

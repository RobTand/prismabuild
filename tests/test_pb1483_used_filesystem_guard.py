"""PB1483 shared guard: the all-used-filesystem floor and aggregate P/K custody.

Source-level behavior only. The hand-built bindings here stand in for the
operational registration: its capture/provenance/adoption path requires
authorized PrismaBuild runs and is deliberately not exercised. Frames are real
native captures of the test's own filesystem (tmpfs/ext4 supported paths).
The private fixture has no NFS-export users: its export roster is explicitly
empty. The fleet's native nfsd FSID table and operational capture are not
qualified here; the admitted PermissionError remains an operational prerequisite.
"""
import contextvars
import json
import os
import socket
import subprocess
import sys
import threading
import uuid
from pathlib import Path

import pytest

from prismabuild import core, filesystem_capacity as fs, pool
from prismabuild.local_scratch import LocalScratchError

HOST = socket.gethostname()
OPERATION_KEY = "operation-p-" + "a" * 64
GIB = fs.GIB


def envelope(role, roots, *, resource="spool_gb", max_bytes=2 * GIB, classes=None):
    classes = sorted(fs._ROLES[role]) if classes is None else list(classes)
    return {"schema": fs.OPERATION_SCHEMA,
            "operations": {role: [{"root": str(root), "max_bytes": max_bytes,
                                   "classes": classes, "resource": resource}
                                  for root in roots]}}


def build_binding(root, *, kinds=("spool_gb",), owner=None, primary=None,
                  age_s=0.0, physical_key=None):
    frame = fs.capture_filesystem_capacity(str(root))
    if age_s:
        frame = {**frame, "sampled_unix": frame["sampled_unix"] - age_s}
    if physical_key is not None:
        frame = {**frame, "physical_key": physical_key}
    owner = owner or {"type": "host", "id": HOST}
    primary = primary or {"type": "host", "id": HOST}
    generation = uuid.uuid4().hex
    body = {"schema": fs.BINDING_SCHEMA, "owner": owner, "generation": generation,
            "complete": True,
            "constraints": [{"kinds": sorted(kinds), "frame": frame,
                             "primary": primary}],
            "primaries": [{"physical_key": frame["physical_key"],
                           "aliases": [{"owner": owner, "kinds": sorted(kinds),
                                        "generation": generation}]}],
            "registration": {"action_key": "b" * 64, "published_unix": 1.0,
                             "attempt": 1}}
    return {**body, "binding_sha256": core.canonical_sha256(body)}


def reseal(record):
    body = {k: v for k, v in record.items() if k != "binding_sha256"}
    return {**body, "binding_sha256": core.canonical_sha256(body)}


def install_binding(queue, record):
    base = queue.root / pool.RESERVATIONS / record["owner"]["id"]
    pool._write_json_atomic(base / fs.BINDING_FILE, record)
    return base


def owner_ref():
    return {"type": "host", "id": HOST}


@pytest.fixture(autouse=True)
def private_export_roster(monkeypatch):
    monkeypatch.setattr(fs, "_native_export_rows", lambda: [])


@pytest.fixture
def queue(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    return queue


@pytest.fixture
def used_root(tmp_path):
    root = tmp_path / "used-root"
    root.mkdir()
    return root


@pytest.fixture
def registered(queue, used_root):
    install_binding(queue, build_binding(used_root))
    queue.ledger(HOST).ensure_capacity({"spool_gb": 8})
    return queue, used_root


def test_floor_predicate_is_exact_ceil_five_percent_plus_allowance():
    verdict = fs.filesystem_floor(100, 5, 0)
    assert verdict == {"size_bytes": 100, "free_bytes": 5, "floor_bytes": 5,
                       "allowance_bytes": 0, "required_bytes": 5}
    with pytest.raises(LocalScratchError):
        fs.filesystem_floor(100, 4, 0)
    with pytest.raises(LocalScratchError):
        fs.filesystem_floor(100, 5, 1)
    assert fs.filesystem_floor(100, 6, 1)["required_bytes"] == 6
    for bad in ((True, 5, 0), (100, True, 0), (100, 5, True), (99.0, 5, 0),
                (0, 0, 0), (100, 101, 0), (100, -1, 0), (100, 5, -1)):
        with pytest.raises(LocalScratchError):
            fs.filesystem_floor(*bad)


def test_operation_envelope_validation_and_terms(tmp_path):
    assert fs.terms(envelope("coordinator", [tmp_path]), "coordinator") == {"spool_gb": 2}
    assert fs.terms(envelope("coordinator", [tmp_path], max_bytes=GIB + 1),
                    "coordinator") == {"spool_gb": 2}
    with pytest.raises(LocalScratchError):  # missing a mandatory role class
        fs.operation(envelope("coordinator", [tmp_path], classes=["source"]), "coordinator")
    with pytest.raises(LocalScratchError):  # unknown class
        fs.operation(envelope("coordinator", [tmp_path],
                              classes=["source", "cas", "queue", "logs", "nope"]),
                     "coordinator")
    with pytest.raises(LocalScratchError):  # unknown byte kind
        fs.operation(envelope("coordinator", [tmp_path], resource="bank_hog"), "coordinator")
    with pytest.raises(LocalScratchError):  # duplicate write roots
        fs.operation(envelope("coordinator", [tmp_path, tmp_path]), "coordinator")


def test_unregistered_population_is_unknown_not_zero(queue, used_root):
    with pytest.raises(LocalScratchError, match="owners unavailable"):
        with fs.admission(queue, [used_root], intent=envelope("coordinator", [used_root]),
                          role="coordinator"):
            pass


def test_legacy_held_tokens_without_registration_refuse(queue, used_root):
    ledger = queue.ledger(HOST)
    holder = ledger.held_dir / ("0" * 64)
    holder.mkdir(parents=True, exist_ok=True)
    (holder / "spool_gb-0000").write_bytes(b"")
    with pytest.raises(LocalScratchError, match="lack canonical registration"):
        with fs.admission(queue, [used_root], intent=envelope("coordinator", [used_root]),
                          role="coordinator"):
            pass


def test_binding_schema_refuses_tamper_and_partial_installs(registered):
    queue, root = registered
    base = queue.root / pool.RESERVATIONS / HOST / fs.BINDING_FILE
    record = fs._read_binding(queue.root, owner_ref())
    assert record["complete"] is True
    install_binding(queue, reseal({**record, "complete": False}))  # durably INCOMPLETE
    with pytest.raises(LocalScratchError):
        fs._read_binding(queue.root, owner_ref())
    install_binding(queue, reseal({**record, "registration": {"action_key": "zz",
                                                              "published_unix": 1.0,
                                                              "attempt": 1}}))
    with pytest.raises(LocalScratchError, match="capture unknown"):
        fs._read_binding(queue.root, owner_ref())
    changed = {**record,
               "constraints": [{**record["constraints"][0], "kinds": ["filesystem_gib"]}]}
    pool._write_json_atomic(base, changed)  # body changed, seal not recomputed
    with pytest.raises(LocalScratchError, match="changed"):
        fs._read_binding(queue.root, owner_ref())
    install_binding(queue, record)
    assert fs._read_binding(queue.root, owner_ref()) == record


def test_duplicate_physical_constraints_in_one_owner_refuse(queue, used_root):
    record = build_binding(used_root)
    record["constraints"].append(dict(record["constraints"][0]))
    install_binding(queue, reseal(record))
    with pytest.raises(LocalScratchError, match="duplicate physical constraint"):
        fs._read_binding(queue.root, owner_ref())


def test_stale_and_foreign_frames_refuse(registered):
    queue, root = registered
    stale = build_binding(root, age_s=pool.OFFER_TIMEOUT_S + 1)
    install_binding(queue, stale)
    with pytest.raises(LocalScratchError, match="stale"):
        with fs.admission(queue, [root], intent=envelope("coordinator", [root]),
                          role="coordinator"):
            pass
    real_key = fs.capture_filesystem_capacity(str(root))["physical_key"]
    foreign = build_binding(root, physical_key=[*real_key[:2], "feed:face"])
    install_binding(queue, foreign)
    with pytest.raises(LocalScratchError, match="unique native physical owner"):
        with fs.admission(queue, [root], intent=envelope("coordinator", [root]),
                          role="coordinator"):
            pass


def test_reserve_operation_bad_keys_and_roles_refuse(registered):
    queue, root = registered
    with pytest.raises(LocalScratchError, match="operation-p key"):
        with fs.reserve_operation(queue, envelope("coordinator", [root]), [root],
                                  role="coordinator",
                                  operation_key=OPERATION_KEY.replace("operation-p-", "")):
            pass
    with pytest.raises(LocalScratchError, match="role unknown"):
        with fs.reserve_operation(queue, envelope("coordinator", [root]), [root],
                                  role="movement", operation_key=OPERATION_KEY):
            pass
    with pytest.raises(LocalScratchError, match="claimed action owner"):
        with fs.reserve_operation(queue, envelope("worker", [root]), [root],
                                  role="worker"):
            pass


def test_committed_p_custody_joins_releases_and_retains(registered):
    queue, root = registered
    ledger = queue.ledger(HOST)
    other = root.parent / "used-other"
    other.mkdir()
    intent = envelope("coordinator", [root, other])
    with fs.reserve_operation(queue, intent, [root, other], role="coordinator",
                              operation_key=OPERATION_KEY):
        assert {n for n in pool.held_names_visible(ledger, OPERATION_KEY)
                if n.startswith("spool_gb")} == {"spool_gb-0000", "spool_gb-0001"}
        assert fs._held(ledger, {"spool_gb"}) == 2
        # Nested join: admission only with the exact committed bound (new
        # read paths may join), no second commit, no release here.
        with fs.reserve_operation(queue, intent, [other], role="coordinator",
                                  operation_key=OPERATION_KEY):
            assert len(pool.held_names_visible(ledger, OPERATION_KEY)) == 2
        assert len(pool.held_names_visible(ledger, OPERATION_KEY)) == 2
        # A new top-level context cannot take the key already held by P.
        # Reusing the enclosing context would be a legitimate nested join.
        def duplicate():
            with fs.reserve_operation(queue, intent, [root], role="coordinator",
                                      operation_key=OPERATION_KEY):
                pytest.fail("duplicate top-level custody was admitted")
        with pytest.raises(LocalScratchError, match="already holds tokens"):
            contextvars.Context().run(duplicate)
    assert pool.held_names_visible(ledger, OPERATION_KEY) == set()
    with pytest.raises(RuntimeError):
        with fs.reserve_operation(queue, intent, [root], role="coordinator",
                                  operation_key=OPERATION_KEY):
            raise RuntimeError("operation failed after the floor was proven")
    assert len(pool.held_names_visible(ledger, OPERATION_KEY)) == 2  # retained
    ledger.release(OPERATION_KEY)  # only the recorded owner releases


def test_nested_join_refuses_growth_beyond_the_committed_bound(registered):
    queue, root = registered
    other = root.parent / "used-other"
    other.mkdir()
    intent = envelope("coordinator", [root])
    ledger = queue.ledger(HOST)
    with fs.reserve_operation(queue, intent, [root], role="coordinator",
                              operation_key=OPERATION_KEY):
        bigger = envelope("coordinator", [root, other], max_bytes=4 * GIB)
        with pytest.raises(LocalScratchError, match="exceeds its committed bound"):
            with fs.reserve_operation(queue, bigger, [other], role="coordinator",
                                      operation_key=OPERATION_KEY):
                pass
        assert len(pool.held_names_visible(ledger, OPERATION_KEY)) == 2
        # The exact bound still joins with a new read path, uncharged.
        with fs.reserve_operation(queue, intent, [other], role="coordinator",
                                  operation_key=OPERATION_KEY):
            assert len(pool.held_names_visible(ledger, OPERATION_KEY)) == 2
    assert pool.held_names_visible(ledger, OPERATION_KEY) == set()


SECOND_PROCESS_CONTROL = """
import json, socket, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from prismabuild import filesystem_capacity as fs, pool
queue = pool.PoolQueue(Path(sys.argv[2]))
root = Path(sys.argv[3])
host = socket.gethostname()
# The P body holds no guard lock: a second process takes the registration
# exclusion nonblocking and reads the retained charges from the census.
with fs._registration_lock(fs._queue_identity(queue.root)) as acquired:
    assert acquired, "guard exclusion still held during the P body"
census = fs._held(queue.ledger(host), {"spool_gb"})
assert census >= 2, f"second process cannot read the retained charges: {census}"
envelope = {"schema": fs.OPERATION_SCHEMA, "operations": {"coordinator": [
    {"root": str(root), "max_bytes": fs.GIB,
     "classes": sorted(fs._ROLES["coordinator"]), "resource": "spool_gb"}]}}
with fs.admission(queue, [root], intent=envelope, role="coordinator"):
    pass
print(json.dumps({"lock_access": True, "held_gib": census}))
"""


def test_second_process_keeps_lock_access_and_reads_p_charges(registered):
    queue, root = registered
    source_root = str(Path(__file__).resolve().parents[1] / "src")
    intent = envelope("coordinator", [root])
    with fs.reserve_operation(queue, intent, [root], role="coordinator",
                              operation_key=OPERATION_KEY):
        completed = subprocess.run(
            [sys.executable, "-c", SECOND_PROCESS_CONTROL, source_root,
             str(queue.root), str(root)],
            capture_output=True, text=True, timeout=120)
        assert completed.returncode == 0, completed.stderr
        assert json.loads(completed.stdout) == {"lock_access": True, "held_gib": 2}
    assert pool.held_names_visible(queue.ledger(HOST), OPERATION_KEY) == set()


def test_second_process_keeps_lock_access_inside_nested_window(registered):
    queue, root = registered
    other = root.parent / "used-other"
    other.mkdir()
    source_root = str(Path(__file__).resolve().parents[1] / "src")
    intent = envelope("coordinator", [root, other])
    with fs.reserve_operation(queue, intent, [root, other], role="coordinator",
                              operation_key=OPERATION_KEY):
        # Nested join with the exact committed bound: while the NESTED body
        # runs, no guard lock is held either.
        with fs.reserve_operation(queue, intent, [other], role="coordinator",
                                  operation_key=OPERATION_KEY):
            completed = subprocess.run(
                [sys.executable, "-c", SECOND_PROCESS_CONTROL, source_root,
                 str(queue.root), str(root)],
                capture_output=True, text=True, timeout=120)
            assert completed.returncode == 0, completed.stderr
            assert json.loads(completed.stdout) == {"lock_access": True,
                                                    "held_gib": 2}
        assert len(pool.held_names_visible(queue.ledger(HOST), OPERATION_KEY)) == 2
    assert pool.held_names_visible(queue.ledger(HOST), OPERATION_KEY) == set()


def test_coordinator_demand_may_commit_honest_extra_kinds(registered):
    queue, root = registered
    ledger = queue.ledger(HOST)
    ledger.ensure_capacity({"cpu": 1})
    with pytest.raises(LocalScratchError, match="understates"):
        with fs.reserve_operation(queue, envelope("coordinator", [root]), [root],
                                  role="coordinator", operation_key=OPERATION_KEY,
                                  demand={"spool_gb": 1}):
            pass
    with fs.reserve_operation(queue, envelope("coordinator", [root]), [root],
                              role="coordinator", operation_key=OPERATION_KEY,
                              demand={"spool_gb": 2, "cpu": 1}):
        names = pool.held_names_visible(ledger, OPERATION_KEY)
        assert {n for n in names if n.startswith("spool_gb")} == {
            "spool_gb-0000", "spool_gb-0001"}
        assert "cpu-0000" in names


def test_admission_refuses_inside_provenance_and_registration_scopes(registered):
    queue, root = registered
    token = fs._IN_PROVENANCE.set(True)
    try:
        with pytest.raises(LocalScratchError, match="provenance materialization"):
            with fs.admission(queue, [root], intent=envelope("coordinator", [root]),
                              role="coordinator"):
                pass
    finally:
        fs._IN_PROVENANCE.reset(token)
    transaction = fs._TRANSACTION.set({"identity": object(), "pid": os.getpid(),
                                       "thread": threading.get_ident()})
    try:
        with pytest.raises(LocalScratchError, match="live used-filesystem transaction"):
            fs.install_registration(queue, [])
    finally:
        fs._TRANSACTION.reset(transaction)

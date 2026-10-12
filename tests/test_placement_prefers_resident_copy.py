"""prismabuild#1733: offers and the gang vote prefer resident hosts, admit without copy.

A row that names a resident set runs faster where its copy is resident,
but the copy is a preference, never admission: a fit host without one
still claims, and a row without a set keeps the prior order exactly.
"""
from __future__ import annotations

import json
import secrets
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from prismabuild import _gang, core as pb, pool, resident_sets as rs
from admitted_queue_fixture import AdmittedQueueFixture
from test_resident_sets_records import source as resident_source

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbgang  # noqa: E402
import pbrun  # noqa: E402

HOSTS = ("ahost", "zhost")
SET_ID = "c" * 64


@pytest.fixture(autouse=True)
def fixed_clock(monkeypatch):
    monkeypatch.setattr(time, "time", lambda: 2_000_000.0)


def _damage_metadata(path, raw):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.chmod(0o600)
    path.write_bytes(raw)
    assert path.read_bytes() == raw


def _rewrite_body(store, set_id, body, *, rehash=False):
    if rehash:
        copies = {host: store.read_copy(set_id, host) for host in body["hosts"]}
        manifest_bytes = json.dumps(body["manifest"], sort_keys=True,
                                    separators=(",", ":"), ensure_ascii=True).encode("ascii")
        set_id = pb.raw_sha256(manifest_bytes)
        body["set_id"] = set_id
        for host, copy in copies.items():
            copy["set_id"] = set_id
            _damage_metadata(store.copy_path(set_id, host), json.dumps(copy).encode())
    _damage_metadata(store.set_path(set_id), json.dumps(body).encode())
    return set_id


def _resident_world(tmp_path, *, resident_host="zhost", lease_now=100, hosts=HOSTS):
    """A queue with a two-host set; a resident copy only where asked."""
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    for host in hosts:
        queue.mint_tier_capacity("local:" + host, {"local_gib": 1})
    root, manifest = resident_source(tmp_path)
    store = rs.ResidentSets(tmp_path / "queue")
    record = store.publish(manifest=manifest, canonical_root=str(root),
                           hosts=list(hosts),
                           lease={"until": lease_now + 3600, "hard_max": lease_now + 7200},
                           created_by="test", now=lease_now)
    set_id = record["set_id"]
    if resident_host is not None:
        store.write_copy(set_id, resident_host, {"state": "resident", "local_root": "/tmp/x",
                                                 "verification": [], "bytes": 7,
                                                 "completed_unix": lease_now + 20})
    return queue, store, set_id


def _announce(queue, host):
    queue.announce(host=host, tags=["x86"], has_gpu=False,
                   capacity={"cpu": 4, "mem_gb": 16})


def _publish(queue, key, **kwargs):
    queue.publish(action_key=key, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", resources={"cpu": 1, "mem_gb": 1},
                  tags=["x86"], **kwargs)


def test_offer_sort_puts_the_resident_host_first(tmp_path):
    queue, store, set_id = _resident_world(tmp_path)
    for host in HOSTS:
        _announce(queue, host)
    _publish(queue, "a" * 64, resident_set=set_id)
    item = json.loads(queue.item_path(pool.READY, "a" * 64).read_text())
    assert queue.placeable_hosts(item) == ["zhost", "ahost"]


@pytest.mark.parametrize("set_value", [None, ""])
def test_a_row_without_a_set_keeps_the_prior_order(tmp_path, monkeypatch, set_value):
    queue, store, set_id = _resident_world(tmp_path)
    for host in HOSTS:
        _announce(queue, host)
    _publish(queue, "b" * 64)
    item = json.loads(queue.item_path(pool.READY, "b" * 64).read_text())
    if set_value is not None:
        item["resident_set"] = set_value
        pool._write_json_atomic(queue.item_path(pool.READY, "b" * 64), item)

    def unexpected_read(*args):
        pytest.fail("a plain row read resident metadata")

    monkeypatch.setattr(rs.ResidentSets, "read", unexpected_read)
    monkeypatch.setattr(rs.ResidentSets, "read_copy", unexpected_read)
    assert queue.placeable_hosts(item) == ["ahost", "zhost"]
    admitted = AdmittedQueueFixture(queue, capacity={"cpu": 4, "mem_gb": 16},
                                    default_demand={"cpu": 1, "mem_gb": 1})
    assert admitted.claim(tags=["x86"])["action_key"] == "b" * 64


def test_a_row_with_a_set_but_no_copy_keeps_order_and_still_claims(tmp_path):
    queue, store, set_id = _resident_world(tmp_path, resident_host=None)
    for host in HOSTS:
        _announce(queue, host)
    queue.publish(action_key="d" * 64, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", resources={"cpu": 1, "mem_gb": 1},
                  resident_set=set_id)
    item = json.loads(queue.item_path(pool.READY, "d" * 64).read_text())
    assert queue.placeable_hosts(item) == ["ahost", "zhost"]
    admitted = AdmittedQueueFixture(queue, capacity={"cpu": 1, "mem_gb": 2},
                                    default_demand={"cpu": 1, "mem_gb": 1})
    claimed = admitted.claim()
    assert claimed is not None and claimed["action_key"] == "d" * 64
    assert claimed["served_from"] == "canonical"


def _assert_fallback(queue, set_id, monkeypatch):
    for host in HOSTS:
        _announce(queue, host)
    _publish(queue, "f" * 64, resident_set=set_id)
    item = pool._read_json(queue.item_path(pool.READY, "f" * 64))
    assert queue.resident_copy_hosts(set_id) == set()
    assert queue.placeable_hosts(item) == list(HOSTS)
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "ahost")
    admitted = AdmittedQueueFixture(queue, capacity={"cpu": 4, "mem_gb": 16},
                                    default_demand={"cpu": 1, "mem_gb": 1})
    claimed = admitted.claim(tags=["x86"])
    assert claimed is not None and claimed["action_key"] == "f" * 64
    assert claimed["served_from"] == "canonical"


@pytest.mark.parametrize("raw", [b"{", b"\xff", b"[]", b"null", b"1", b"true", b'"body"'])
def test_unparseable_or_nonobject_body_allows_fallback(tmp_path, monkeypatch, raw):
    queue, store, set_id = _resident_world(tmp_path)
    _damage_metadata(store.set_path(set_id), raw)
    _assert_fallback(queue, set_id, monkeypatch)


@pytest.mark.parametrize(("field", "value"), [
    ("schema", "wrong"), ("set_id", SET_ID), ("immutable", False), ("immutable", 1),
    ("canonical_root", None), ("canonical_root", "relative"),
    ("canonical_root", "/"), ("canonical_root", "/canonical/.."),
    ("canonical_root", "/canonical/"), ("canonical_root", "/canonical//data"),
    ("hosts", None), ("hosts", "zhost"), ("hosts", []),
    ("hosts", ["zhost", "zhost"]), ("hosts", ["zhost", ""]),
    ("hosts", ["zhost", "../bad"]), ("hosts", ["zhost", "/bad"]),
    ("hosts", ["zhost", "bad host"]), ("hosts", ["zhost", "-bad"]),
    ("hosts", ["zhost", 1]), ("hosts", ["zhost", None]),
    ("hosts", ["zhost", {}]), ("hosts", ["zhost", ["ahost"]]),
    ("manifest", {}),
])
def test_invalid_body_rejects_the_whole_preference(tmp_path, monkeypatch, field, value):
    queue, store, set_id = _resident_world(tmp_path)
    body = store.read(set_id)
    body[field] = value
    _rewrite_body(store, set_id, body)
    _assert_fallback(queue, set_id, monkeypatch)


@pytest.mark.parametrize("damage", [
    "schema", "entry-count", "total-bytes", "offset", "null-sha", "bad-sha",
    "duplicate-path", "outside-root", "nonfinite", "root", "double-slash-root",
])
def test_digest_matching_bad_manifest_allows_fallback(tmp_path, monkeypatch, damage):
    queue, store, set_id = _resident_world(tmp_path)
    body = store.read(set_id)
    manifest = body["manifest"]
    entry = manifest["entries"][0]
    if damage == "schema":
        manifest["schema"] = "wrong"
    elif damage == "entry-count":
        manifest["entry_count"] += 1
    elif damage == "total-bytes":
        manifest["total_bytes"] += 1
    elif damage == "offset":
        entry["offset"] = 1
    elif damage == "null-sha":
        entry["sha256"] = None
    elif damage == "bad-sha":
        entry["sha256"] = "invalid"
    elif damage == "duplicate-path":
        manifest["entries"].append({**entry, "offset": 1})
        manifest["entry_count"] = 2
        manifest["total_bytes"] *= 2
    elif damage == "outside-root":
        body["canonical_root"] += "/other"
    elif damage == "nonfinite":
        manifest["annotations"]["bad"] = float("nan")
    else:
        prefix = "//" if damage == "double-slash-root" else "/"
        body["canonical_root"] = prefix
        manifest["mount_prefix"] = prefix + "canonical"
        entry["path"] = prefix + "canonical/weights"
        pb.validate_data_manifest(manifest)
        assert Path(entry["path"]).is_relative_to(Path(prefix))
    set_id = _rewrite_body(store, set_id, body, rehash=True)
    _assert_fallback(queue, set_id, monkeypatch)


@pytest.mark.parametrize("root", ["/", "//"])
def test_root_spelling_never_confers_preference(tmp_path, root):
    queue = pool.PoolQueue(tmp_path / "queue")
    store = rs.ResidentSets(queue.root)
    manifest = pb.validate_data_manifest({
        "schema": pb.DATA_MANIFEST_SCHEMA_V1, "produced_by": {}, "annotations": {},
        "mount_prefix": root + "canonical", "entry_count": 1, "total_bytes": 7,
        "entries": [{"path": root + "canonical/weights", "offset": 0,
                     "bytes": 7, "sha256": "a" * 64}],
    })
    set_id = pb.raw_sha256(pb.compact_ascii_json_bytes(manifest))
    _damage_metadata(store.set_path(set_id), json.dumps({
        "schema": rs.SET_SCHEMA, "set_id": set_id, "immutable": True,
        "manifest": manifest, "canonical_root": root, "hosts": list(HOSTS),
    }).encode())
    _damage_metadata(store.copy_path(set_id, "zhost"), json.dumps({
        "schema": rs.COPY_SCHEMA, "set_id": set_id, "host": "zhost", "state": "resident",
    }).encode())
    for host in HOSTS:
        _announce(queue, host)
    item = {"resident_set": set_id, "tags": ["x86"], "resources": {"cpu": 1},
            "published_unix": time.time()}
    assert queue.resident_copy_hosts(set_id) == set()
    assert queue.placeable_hosts(item) == list(HOSTS)
    assert _gang.resident_hold(queue, {"group": "f" * 32, "size": 2}, {"index": 0},
                               item, "ahost", time.time(), live=queue.offers(),
                               prefer_s=pool.RESIDENT_COPY_PREFER_S) is None


@pytest.mark.parametrize("damage", [
    "absent", "copying", "evicting", "unknown", "schema", "set-id", "host",
    "json", "utf8", "array", "null", "number", "bool", "string", "missing", "unreadable",
])
def test_bad_copy_is_ignored_independently(tmp_path, monkeypatch, damage):
    queue, store, set_id = _resident_world(tmp_path)
    path = store.copy_path(set_id, "zhost")
    copy = store.read_copy(set_id, "zhost")
    if damage in {"absent", "copying", "evicting", "unknown"}:
        copy["state"] = damage
    elif damage in {"schema", "set-id", "host"}:
        copy[{"set-id": "set_id"}.get(damage, damage)] = "wrong"
    if damage == "missing":
        path.unlink()
    elif damage == "unreadable":
        original = rs.ResidentSets.read_copy

        def read_copy(self, requested, host):
            if host == "zhost":
                raise PermissionError("injected unreadable copy")
            return original(self, requested, host)

        monkeypatch.setattr(rs.ResidentSets, "read_copy", read_copy)
    else:
        raw = {"json": b"{", "utf8": b"\xff", "array": b"[]",
               "null": b"null", "number": b"1", "bool": b"true",
               "string": b'"copy"'}.get(damage, json.dumps(copy).encode())
        _damage_metadata(path, raw)
    _assert_fallback(queue, set_id, monkeypatch)
    store.write_copy(set_id, "ahost", {"state": "resident"})
    assert queue.resident_copy_hosts(set_id) == {"ahost"}
    assert queue.placeable_hosts({"resident_set": set_id, "tags": ["x86"],
                                  "resources": {"cpu": 1}}) == list(HOSTS)


def test_unreadable_body_allows_fallback(tmp_path, monkeypatch):
    queue, store, set_id = _resident_world(tmp_path)

    def unreadable(*args):
        raise PermissionError("injected unreadable body")

    monkeypatch.setattr(rs.ResidentSets, "read", unreadable)
    _assert_fallback(queue, set_id, monkeypatch)


@pytest.mark.parametrize("set_id", [None, "", "A" * 64, "a" * 63, "g" * 64, "../bad"])
def test_invalid_set_id_has_no_preference(tmp_path, set_id):
    assert pool.PoolQueue(tmp_path).resident_copy_hosts(set_id) == set()


def test_preference_reads_metadata_without_canonical_payload(tmp_path, monkeypatch):
    queue, store, set_id = _resident_world(tmp_path)
    body = store.read(set_id)
    Path(body["manifest"]["entries"][0]["path"]).unlink()
    Path(body["canonical_root"]).rmdir()

    def unexpected_scan(*args):
        pytest.fail("preference scanned canonical payload")

    monkeypatch.setattr(rs, "directory_files", unexpected_scan)
    assert queue.resident_copy_hosts(set_id) == {"zhost"}


def test_resident_order_preserves_resource_and_tag_eligibility(tmp_path):
    hosts = ("ahost", "bhost", "yhost", "zhost", "weak", "other")
    queue, store, set_id = _resident_world(tmp_path, hosts=hosts)
    for host in ("bhost", "zhost", "weak", "other"):
        store.write_copy(set_id, host, {"state": "resident"})
    for host in hosts:
        queue.announce(host=host, tags=["arm" if host == "other" else "x86"],
                       has_gpu=False, capacity={"cpu": 1 if host == "weak" else 4})
    plain = {"tags": ["x86"], "resources": {"cpu": 2}}
    assert queue.placeable_hosts(plain) == ["ahost", "bhost", "yhost", "zhost"]
    assert queue.placeable_hosts({**plain, "resident_set": set_id}) == [
        "bhost", "zhost", "ahost", "yhost"]
    assert pool.PoolQueue(tmp_path / "no-offers").placeable_hosts(plain) is None


from test_measurement_drains_gpu_backfill import fleet as fleet_fixture

fleet = fleet_fixture
CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 120}
TIERS = {"preferred": list(range(20)), "fallback": []}
GHOSTS = ("sparklina", "sparky")


@pytest.fixture()
def resident_gang(fleet, tmp_path, monkeypatch):
    """An idle two-host gang fleet with a set resident only on sparky."""
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    for host in GHOSTS:
        queue.mint_tier_capacity("local:" + host, {"local_gib": 1})
        queue.announce(host=host, tags=["gb10", host, _gang.TAG], has_gpu=True,
                       capacity=dict(CAPACITY))
    root, manifest = resident_source(tmp_path)
    store = rs.ResidentSets(queue.root)
    record = store.publish(manifest=manifest, canonical_root=str(root),
                           hosts=list(GHOSTS),
                           lease={"until": clock[0] + 3600, "hard_max": clock[0] + 7200},
                           created_by="test", now=clock[0])
    set_id = record["set_id"]
    store.write_copy(set_id, "sparky", {"state": "resident", "local_root": "/tmp/x",
                                        "verification": [], "bytes": 7,
                                        "completed_unix": clock[0]})

    def gclaim(host, *, advance=0.01):
        monkeypatch.setattr(pool.socket, "gethostname", lambda: host)
        tick(advance)
        result = queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
                             has_gpu=True, tags=["gb10", host, _gang.TAG])
        return None if result is None else result["action_key"]

    def members(name, *, resident_set=True, tags=None):
        """Two portable members: either host may take either member."""
        group = secrets.token_hex(16)
        cas = pb.PrismaBuildCAS(tmp_path / "cas")
        checkout = tmp_path / "checkout"
        rows = []
        for index in range(2):
            clock[0] += 0.001
            gang = {"group": group, "size": 2, "index": index}
            action = pb.seal_action({
                "schema": pb.ACTION_SCHEMA_V2,
                "task": {"definition_id": "tests/gang-member", "definition_version": "v1",
                         "task_class": "generation", "determinism": "deterministic",
                         "artifact_family": "generic", "artifact_kind": "generic",
                         "argv": [sys.executable, "task.py"], "working_directory": ".",
                         "result_path": f"{name}-{index}"},
                "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
                "params": {"gpu_exclusive": False, "execution_timeout_s": 3600, "gang": gang},
                "environment": {"variables": {}, "toolchain": {}},
                "execution_scope": {"portability": "portable", "platform_key": None,
                                    "host_class": None},
            })
            cas.publish_action_request(action)
            key = action["action_key"]
            queue.publish(action_key=key, cas_root=str(cas.root), checkout_root=str(checkout),
                          worker_script="worker.py",
                          resources={"cpu": 2, "gpu": 1, "mem_gb": 100},
                          needs_gpu=True, tags=tags or ["gb10"], priority=0, gang=gang,
                          max_attempts=1, resident_set=set_id if resident_set else None)
            rows.append(pool._read_json(queue.item_path(pool.READY, key)))
        _gang.publish_group(queue, group, rows)
        return group, [row["action_key"] for row in rows]

    return queue, clock, tick, gclaim, denial, members


def test_gang_vote_waits_for_the_resident_host_then_starts_canonical(resident_gang,
                                                                     monkeypatch):
    queue, clock, tick, gclaim, denial, members = resident_gang
    group, (first, second) = members("resident-vote")
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "deferred_for_resident_copy"
    tick(pool.RESIDENT_COPY_PREFER_S + 1)
    assert gclaim("sparky") is None
    assert gclaim("sparklina") == second
    assert gclaim("sparky") == first
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
    terminal = json.loads(queue.finish(second, status="executed").read_text())
    assert queue.attempt_outcomes(terminal)[0]["served_from"] == "canonical"


def test_gang_skips_a_resident_host_that_holds_a_sibling(resident_gang):
    """A non-resident host elects at once when the resident host is busy."""
    queue, clock, tick, gclaim, denial, members = resident_gang
    group, (first, second) = members("resident-busy")
    assert gclaim("sparky") is None
    assert denial(first, "sparky")["reason"] == "gang_waiting_for_peers"
    assert _gang.elections(queue, group, 2)[0]["host"] == "sparky"
    assert gclaim("sparklina") == second
    assert gclaim("sparky") == first


def test_gang_waits_while_the_resident_host_is_free(resident_gang):
    """The reverse poll order still waits for the free resident host."""
    queue, clock, tick, gclaim, denial, members = resident_gang
    group, (first, second) = members("resident-free")
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "deferred_for_resident_copy"
    assert _gang.elections(queue, group, 2) == {}
    assert gclaim("sparky") is None
    assert denial(first, "sparky")["reason"] == "gang_waiting_for_peers"
    assert gclaim("sparklina") == second
    assert gclaim("sparky") == first


@pytest.mark.parametrize("published", [None, True, False, "1999999", float("nan"),
                                       float("inf"), -float("inf"), 2_000_001.0])
def test_resident_hold_ignores_a_future_publish_stamp(tmp_path, published):
    queue, store, set_id = _resident_world(tmp_path, lease_now=100)
    for host in HOSTS:
        _announce(queue, host)
    _publish(queue, "e" * 64, resident_set=set_id)
    item = json.loads(queue.item_path(pool.READY, "e" * 64).read_text())
    record = {"group": "f" * 32, "size": 2}
    entry = {"index": 0, "action_key": "e" * 64}
    live = queue.offers()
    now = time.time()
    assert _gang.resident_hold(
        queue, record, entry, {**item, "published_unix": now - 1},
        "ahost", now, live=live, prefer_s=pool.RESIDENT_COPY_PREFER_S) is not None
    invalid = {**item, "published_unix": published}
    if published is None:
        invalid.pop("published_unix")
    assert _gang.resident_hold(
        queue, record, entry, invalid,
        "ahost", now, live=live, prefer_s=pool.RESIDENT_COPY_PREFER_S) is None


def test_gang_deadline_is_publication_based_and_expires_exactly(resident_gang, monkeypatch):
    queue, clock, tick, gclaim, denial, members = resident_gang
    group, (first, second) = members("resident-deadline")
    published = pool._read_json(queue.item_path(pool.READY, first))["published_unix"]
    assert pool.RESIDENT_COPY_PREFER_S == 30.0
    deadline = published + pool.RESIDENT_COPY_PREFER_S
    for now in (published + 1, published + 10, deadline - 0.001, deadline - 0.001):
        clock[0] = now
        assert gclaim("sparklina", advance=0) is None
        evidence = denial(first, "sparklina")
        assert evidence["reason"] == "deferred_for_resident_copy"
        assert evidence["evidence"]["wait_remaining_s"] == pytest.approx(deadline - now)
        assert _gang.elections(queue, group, 2) == {}
        assert not list(_gang.state_dir(queue, group).glob("ready-*.json"))
    clock[0] = deadline
    assert gclaim("sparklina", advance=0) is None
    assert denial(first, "sparklina")["reason"] == "gang_waiting_for_peers"
    assert _gang.elections(queue, group, 2)[0]["host"] == "sparklina"
    assert (_gang.state_dir(queue, group) / "ready-0.json").exists()
    assert gclaim("sparky", advance=0) == second
    assert gclaim("sparklina", advance=0) == first
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
    terminal = json.loads(queue.finish(first, status="executed").read_text())
    assert queue.attempt_outcomes(terminal)[0]["served_from"] == "canonical"


@pytest.mark.parametrize("kind", ["no-set", "no-copy", "ineligible-copy", "bad-body",
                                  "nonfinite-manifest"])
def test_gang_without_a_valid_matching_copy_elects_immediately(resident_gang, kind):
    queue, clock, tick, gclaim, denial, members = resident_gang
    group, (first, second) = members("no-locality-wait", resident_set=kind != "no-set",
                                     tags=["sparklina"] if kind == "ineligible-copy" else None)
    item = pool._read_json(queue.item_path(pool.READY, first))
    if kind not in {"no-set", "ineligible-copy"}:
        store = rs.ResidentSets(queue.root)
        set_id = item["resident_set"]
        if kind == "no-copy":
            store.write_copy(set_id, "sparky", {"state": "absent"})
        elif kind == "bad-body":
            body = store.read(set_id)
            body["hosts"] = ["sparky", None]
            _rewrite_body(store, set_id, body)
        else:
            body = store.read(set_id)
            body["manifest"]["annotations"]["bad"] = float("nan")
            damaged_id = _rewrite_body(store, set_id, body, rehash=True)
            for key in (first, second):
                path = queue.item_path(pool.READY, key)
                row = pool._read_json(path)
                row["resident_set"] = damaged_id
                pool._write_json_atomic(path, row)
    assert gclaim("sparklina", advance=0) is None
    assert denial(first, "sparklina")["reason"] == "gang_waiting_for_peers"
    assert _gang.elections(queue, group, 2)[0]["host"] == "sparklina"
    assert (_gang.state_dir(queue, group) / "ready-0.json").exists()
    if kind != "ineligible-copy":
        assert gclaim("sparky", advance=0) == second
        assert gclaim("sparklina", advance=0) == first


def test_pbgang_forwards_the_resident_set_to_pbrun(tmp_path):
    members = [{"tag": "sparky", "argv": ["/bin/true"], "resident_set": SET_ID},
               {"tag": "sparklina", "argv": ["/bin/true"], "resident_set": "d" * 64}]
    path = tmp_path / "gang.json"
    path.write_text(json.dumps({"members": members}))
    manifest = pbgang._pbgang_load_manifest(path)
    args = SimpleNamespace(cwd=tmp_path)
    for index, member in enumerate(manifest["members"]):
        command = pbgang.member_command(args, manifest, member, group="0" * 32, index=index)
        flags = command[:command.index("--")]
        parsed = pbrun.parse_args([*flags[2:], "--", "/bin/true"])
        assert parsed.resident_set == member["resident_set"]
        omitted = dict(member)
        omitted.pop("resident_set")
        ordinary = pbgang.member_command(args, manifest, omitted, group="0" * 32, index=index)
        position = command.index("--resident-set")
        assert command[:position] + command[position + 2:] == ordinary

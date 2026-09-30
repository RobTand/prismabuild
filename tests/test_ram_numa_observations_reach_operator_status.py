"""RAM NUMA observations reach the existing operator CLI, without admission policy.

#1032 observation-only slice: real discovery, token derivation, queue mint and
announcement, and public --starvation JSON. All evidence/queues are private.
These tests do not prove physical page placement, provisioning or live balance.
"""
from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
import pbstatus  # noqa: E402
import tier_loop  # noqa: E402
from prismabuild import pool, storage_tiers  # noqa: E402

HOST = "dl380g10"
RAM_TIER = storage_tiers.tier_id("ram", HOST)
GIB = storage_tiers.GIB
NOW = 2_000_000.0
SAMPLED = NOW - 5
OBSERVATIONS = ("node_memfree_bytes", "node_shmem_bytes")
DEFAULT_FREE = {"0": 13 * 1024, "2": 987 * 1024}
DEFAULT_SHMEM = {"0": 765 * 1024, "2": 21 * 1024}


def _no_zfs(argv: list[str]) -> str:
    raise OSError(f"no {argv[0]} on this fixture box")


class _RamEvidence:
    """Filesystem facts only; no replacement admission, sampler or tier record."""

    def __init__(self, root: Path, *, ids: tuple[int, ...] = (0, 2)) -> None:
        self.root = root
        self.ids = ids
        self.mount = root / "ram"
        self.proc = root / "proc"
        self.nodes = root / "sys" / "devices" / "system" / "node"
        self.block = root / "sys" / "block"
        self.by_id = root / "dev" / "disk" / "by-id"
        for directory in (self.mount, self.proc, self.nodes, self.block, self.by_id):
            directory.mkdir(parents=True)
        # Actual fixture totals are independent of the policy maximum/window.
        # 147 + 147 = 294 GiB; the single-node control has all 294 GiB.
        self.total_kib = 294 * GIB // len(ids) // 1024
        membership = ",".join(map(str, ids))
        (self.nodes / "has_memory").write_text(membership + "\n")
        (self.nodes / "online").write_text(membership + "\n")
        for node in ids:
            directory = self.nodes / f"node{node}"
            directory.mkdir()
            (directory / "cpulist").write_text("\n" if node == 2 else "0-19\n")
            self.write_node(node, free=13 if node == 0 else 987,
                            shmem=765 if node == 0 else 21)
        options = "rw,noswap,size=256G"
        if len(ids) > 1:
            options += f",mpol=interleave:{membership}"
        self.options = options.split(",")
        (self.proc / "mounts").write_text(
            f"tmpfs {self.mount} tmpfs {options} 0 0\n")
        (self.proc / "meminfo").write_text(
            f"MemTotal: {294 * GIB // 1024} kB\nMemAvailable: 1 kB\n")
        (self.proc / "arcstats").write_text(
            f"c_max 4 {22 * GIB}\nsize 4 {11 * GIB}\n"
            f"arc_meta_used 4 {5 * GIB}\n")
        (self.proc / "pressure").mkdir()
        (self.proc / "pressure" / "io").write_text(
            "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
            "full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n")
        self.policy = {
            "schema": storage_tiers.RAM_TIER_POLICY_SCHEMA_V1,
            "mountpoint": str(self.mount), "ceiling_gib_max": 256,
            # Explicit test input, not a new production default.
            "window_gib_default": 112, "arc_floor_gib": 20,
            "system_reserve_gib": 16, "prefill_depth": None,
        }

    def node_file(self, node: int) -> Path:
        return self.nodes / f"node{node}" / "meminfo"

    def write_node(self, node: int, *, free: int, shmem: int) -> None:
        self.node_file(node).write_text(
            f"Node {node} MemTotal: {self.total_kib} kB\n"
            f"Node {node} MemFree: {free} kB\n"
            f"Node {node} Shmem: {shmem} kB\n"
            f"Node {node} Dirty: 17 kB\n")

    def statvfs(self, path: str) -> os.statvfs_result:
        if path != str(self.mount):
            raise OSError(f"no fixture tmpfs at {path}")
        block = GIB // 1024
        return os.statvfs_result(
            (1024, 1024, 256 * block, 200 * block, 200 * block,
             1_000_000, 900_000, 900_000, 0, 255))

    def filesystem_inputs(self) -> dict:
        return {
            "runner": _no_zfs, "statvfs": self.statvfs,
            "arcstats_path": str(self.proc / "arcstats"),
            "sysfs": str(self.block), "by_id": str(self.by_id),
            "proc_pressure": str(self.proc / "pressure" / "io"),
            "proc_mounts": str(self.proc / "mounts"),
            "meminfo_path": str(self.proc / "meminfo"),
            "memory_numa_root": str(self.nodes),
        }

    def discover(self, **kwargs) -> dict[str, dict[str, object]]:
        # The cycle can supply its actual policy, holds, receipts and time.
        # Only private filesystem/kernel facts and the absent ZFS tool are
        # injected; discovery/admission/token assembly remain production.
        return storage_tiers.discover_tiers(**{
            "host": HOST, "now": SAMPLED, "ram_policy": self.policy,
            "rows_held_gib": 88, **kwargs, **self.filesystem_inputs(),
        })


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    stamp = [NOW]
    monkeypatch.setattr(pbstatus.time, "time", lambda: stamp[0])
    monkeypatch.setattr(pool, "_now", lambda: stamp[0])
    return stamp


def _assert_admitted(record: dict[str, object], *, ids: tuple[int, ...] = (0, 2)
                     ) -> dict[str, int]:
    admission = record["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["admissible"] is True, admission
    assert admission["reason"] is None
    assert admission["mem_total_bytes"] == 294 * GIB
    assert admission["node_memtotal_bytes"] == {
        str(node): 294 * GIB // len(ids) for node in ids}
    assert record["memory_nodes"] == list(ids)
    assert record["ceiling_bytes"] == record["size_bytes"] == 256 * GIB
    assert record["capacity_bytes"] == 200 * GIB
    assert record["window_gib"] == 112
    assert isinstance(record["epoch"], str) and record["epoch"]
    tokens = storage_tiers.tier_tokens(record)
    assert tokens == {storage_tiers.RAM_CAPACITY_KIND: 112}
    return tokens


def _queue(root: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(root)
    queue.ensure_layout()
    return queue


def _publish(queue: pool.PoolQueue, record: dict[str, object]) -> dict[str, object]:
    tokens = storage_tiers.tier_tokens(record)
    queue.mint_tier_capacity(str(record["tier_id"]), tokens)
    path = queue.announce_tier(record, now=NOW)
    persisted = json.loads(path.read_text())
    # Publish the exact discovered record, not a replacement RAM dictionary.
    assert persisted == dict(record, announced_unix=NOW)
    for kind, count in tokens.items():
        assert queue.tier_ledger(str(record["tier_id"])).capacity()[kind] == count
    return persisted


def _cli(queue: pool.PoolQueue, capsys: pytest.CaptureFixture[str]) -> dict:
    assert pbstatus.main(["--starvation", "--queue-root", str(queue.root)]) == 0
    blob = json.loads(capsys.readouterr().out)
    assert blob["schema"] == pbstatus.STARVATION_SCHEMA_V1
    assert blob["complete"] is True
    return {row["tier_id"]: row for row in blob["tiers"]}


def _assert_observations(record: dict[str, object], persisted: dict[str, object],
                         operator: dict, *, free: Mapping[str, int | None] | None,
                         shmem: Mapping[str, int | None] | None,
                         nodes: list[int] | None = None) -> None:
    witness = {"discovered": record, "persisted": persisted,
               "operator": operator, "tokens": storage_tiers.tier_tokens(record)}
    for field, expected in zip(OBSERVATIONS, (free, shmem), strict=True):
        # Status first: the primary old-code RED is the missing operator
        # observation, not a KeyError, new helper or source-only assertion.
        assert operator.get(field) == expected, (
            f"missing or incorrect operator NUMA observation {field}; "
            "admitted RAM and persisted announcement must expose sampled bytes",
            witness)
        for carrier in (operator, record, persisted):
            assert field in carrier, ("unknown must be explicit null", field, witness)
            assert carrier.get(field) == expected, witness
            if expected is not None:
                observed = carrier.get(field)
                assert isinstance(observed, dict)
                assert list(observed) == list(expected)
                assert all(type(key) is str for key in observed)
                assert all(value is None or type(value) is int
                           for value in observed.values())
    expected_nodes = [0, 2] if nodes is None and free is not None else nodes
    for carrier in (operator, record, persisted):
        assert "memory_nodes" in carrier, witness
        assert carrier.get("memory_nodes") == expected_nodes, witness


def test_primary_real_discovery_announced_ram_observations_reach_starvation_cli(
        tmp_path: Path, clock: list[float], capsys: pytest.CaptureFixture[str]) -> None:
    evidence = _RamEvidence(tmp_path / "evidence")
    assert (evidence.nodes / "node2" / "cpulist").read_text() == "\n"
    record = storage_tiers.discover_tiers(
        host=HOST, now=SAMPLED, ram_policy=evidence.policy, rows_held_gib=88,
        **evidence.filesystem_inputs())[RAM_TIER]
    _assert_admitted(record)
    assert record["mount_options"] == evidence.options
    assert record["sampled_unix"] == SAMPLED
    queue = _queue(tmp_path / "pb-queue")
    persisted = _publish(queue, record)
    operator = _cli(queue, capsys)[RAM_TIER]
    assert operator["ledger_capacity"][storage_tiers.RAM_CAPACITY_KIND] == 112
    assert operator["sampled_age_s"] == 5.0
    _assert_observations(record, persisted, operator,
                         free=DEFAULT_FREE, shmem=DEFAULT_SHMEM)


@pytest.mark.parametrize("ids", [(0, 2), (2,)])
def test_zero_kib_conversion_and_single_cpu_less_node_are_observations_not_pressure(
        tmp_path: Path, clock: list[float], capsys: pytest.CaptureFixture[str],
        ids: tuple[int, ...]) -> None:
    evidence = _RamEvidence(tmp_path / "evidence", ids=ids)
    for node in ids:
        evidence.write_node(node, free=0, shmem=123 if node == 2 else 0)
    record = evidence.discover()[RAM_TIER]
    _assert_admitted(record, ids=ids)
    queue = _queue(tmp_path / "pb-queue")
    persisted = _publish(queue, record)
    _assert_observations(
        record, persisted, _cli(queue, capsys)[RAM_TIER],
        free={str(node): 0 for node in ids},
        shmem={str(node): (123 if node == 2 else 0) * 1024 for node in ids},
        nodes=list(ids))


@pytest.mark.parametrize("value", ["00000000000000000123", "12345678901234567890"])
def test_up_to_twenty_digit_optional_counters_convert_to_exact_integer_bytes(
        tmp_path: Path, clock: list[float], capsys: pytest.CaptureFixture[str],
        value: str) -> None:
    evidence = _RamEvidence(tmp_path / "evidence")
    for node in evidence.ids:
        evidence.node_file(node).write_text(
            f"Node {node} MemTotal: {evidence.total_kib} kB\n"
            f"Node {node} MemFree: {value} kB\nNode {node} Shmem: {value} kB\n")
    record = evidence.discover()[RAM_TIER]
    _assert_admitted(record)
    queue = _queue(tmp_path / "pb-queue")
    persisted = _publish(queue, record)
    expected = {str(node): int(value) * 1024 for node in evidence.ids}
    _assert_observations(record, persisted, _cli(queue, capsys)[RAM_TIER],
                         free=expected, shmem=expected)


@pytest.mark.parametrize("metric,field", [
    ("MemFree", "node_memfree_bytes"), ("Shmem", "node_shmem_bytes"),
])
@pytest.mark.parametrize("lines", [
    pytest.param("", id="missing"),
    pytest.param("Node 0 {metric}: garbage kB\n", id="malformed"),
    pytest.param("Node 0 {metric}: -1 kB\n", id="negative"),
    pytest.param("Node 0 {metric}: +1 kB\n", id="signed"),
    pytest.param("Node 0 {metric}: 1.5 kB\n", id="fractional"),
    pytest.param("Node 0 {metric}: 123456789012345678901 kB\n", id="over-20-digits"),
    pytest.param("Node 0 {metric}: 1 kB\nNode 0 {metric}: 1 kB\n", id="duplicate"),
    pytest.param("Node 0 {metric}: 1 kB\nNode 0 {metric}: bad kB\n",
                 id="valid-plus-malformed-duplicate"),
    pytest.param("Node 2 {metric}: 1 kB\n", id="wrong-node"),
    pytest.param("Node 0 {metric}: 1 kB\nNode 2 {metric}: 2 kB\n",
                 id="valid-plus-wrong-node"),
    pytest.param("{metric}: 1 kB\n", id="no-node"),
    pytest.param("Node 0 Other{metric}: 1 kB\n", id="wrong-field"),
    pytest.param("Node 0 {metric}: 1 MB\n", id="wrong-unit"),
    pytest.param("Node 0 {metric}: 1 kB\nNode 0 {metric}: 2 MB\n",
                 id="valid-plus-wrong-unit"),
    pytest.param("Node 0 {metric}: 1 KB\n", id="unit-case"),
    pytest.param("Node 0 {metric}: 1\n", id="unit-missing"),
    pytest.param("Node 0 {metric}: 1 kB junk\n", id="trailing-junk"),
])
def test_bad_optional_counter_is_per_node_unknown_without_changing_admission(
        tmp_path: Path, clock: list[float], capsys: pytest.CaptureFixture[str],
        metric: str, field: str, lines: str) -> None:
    evidence = _RamEvidence(tmp_path / "evidence")
    other = "Node 0 Shmem: 765 kB\n" if metric == "MemFree" else "Node 0 MemFree: 13 kB\n"
    evidence.node_file(0).write_text(
        f"Node 0 MemTotal: {evidence.total_kib} kB\n" + other
        + lines.format(metric=metric))
    record = evidence.discover()[RAM_TIER]
    _assert_admitted(record)
    free: dict[str, int | None] = dict(DEFAULT_FREE)
    shmem: dict[str, int | None] = dict(DEFAULT_SHMEM)
    (free if field == "node_memfree_bytes" else shmem)["0"] = None
    queue = _queue(tmp_path / "pb-queue")
    persisted = _publish(queue, record)
    _assert_observations(record, persisted, _cli(queue, capsys)[RAM_TIER],
                         free=free, shmem=shmem)


def test_memtotal_only_evidence_still_mints_with_all_optional_values_unknown(
        tmp_path: Path, clock: list[float], capsys: pytest.CaptureFixture[str]) -> None:
    evidence = _RamEvidence(tmp_path / "evidence")
    for node in evidence.ids:
        evidence.node_file(node).write_text(
            f"Node {node} MemTotal: {evidence.total_kib} kB\n")
    record = evidence.discover()[RAM_TIER]
    _assert_admitted(record)
    queue = _queue(tmp_path / "pb-queue")
    persisted = _publish(queue, record)
    _assert_observations(record, persisted, _cli(queue, capsys)[RAM_TIER],
                         free={"0": None, "2": None}, shmem={"0": None, "2": None})


@pytest.mark.parametrize("damage", [
    "missing-total", "zero-total", "malformed-total", "negative-total",
    "duplicate-total", "wrong-node-total", "wrong-unit-total", "missing-meminfo",
    "unreadable-meminfo", "invalid-utf8-meminfo", "missing-membership",
    "empty-membership", "malformed-membership", "unsupported-membership",
    "incomplete-membership", "unreadable-membership", "invalid-utf8-membership",
])
def test_invalid_snapshot_refuses_and_announces_whole_unknown_observation_maps(
        tmp_path: Path, clock: list[float], capsys: pytest.CaptureFixture[str],
        damage: str) -> None:
    evidence = _RamEvidence(tmp_path / "evidence")
    node_file = evidence.node_file(0)
    membership = evidence.nodes / "has_memory"
    total_lines = {
        "missing-total": "",
        "zero-total": "Node 0 MemTotal: 0 kB\n",
        "malformed-total": "Node 0 MemTotal: invalid kB\n",
        "negative-total": "Node 0 MemTotal: -1 kB\n",
        "duplicate-total": (f"Node 0 MemTotal: {evidence.total_kib} kB\n" * 2),
        "wrong-node-total": f"Node 2 MemTotal: {evidence.total_kib} kB\n",
        "wrong-unit-total": f"Node 0 MemTotal: {evidence.total_kib} MB\n",
    }
    if damage in total_lines:
        node_file.write_text(total_lines[damage]
                             + "Node 0 MemFree: 13 kB\nNode 0 Shmem: 765 kB\n")
    elif damage == "missing-meminfo":
        node_file.unlink()
    elif damage == "unreadable-meminfo":
        node_file.unlink()
        node_file.mkdir()  # Deterministic OSError even when the runner is root.
    elif damage == "invalid-utf8-meminfo":
        node_file.write_bytes(b"\xff\n")
    elif damage == "missing-membership":
        membership.unlink()
    elif damage == "unreadable-membership":
        membership.unlink()
        membership.mkdir()
    elif damage == "invalid-utf8-membership":
        membership.write_bytes(b"\xff\n")
    else:
        membership.write_text({
            "empty-membership": "\n", "malformed-membership": "0,\n",
            "unsupported-membership": "0-4096\n", "incomplete-membership": "0-2\n",
        }[damage])
    record = evidence.discover()[RAM_TIER]
    admission = record["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["admissible"] is False
    assert admission["reason"] == "ram_numa_topology_unreadable"
    assert record["memory_nodes"] is None
    assert storage_tiers.tier_tokens(record) == {}
    queue = _queue(tmp_path / "pb-queue")
    persisted = _publish(queue, record)
    operator = _cli(queue, capsys)[RAM_TIER]
    assert operator["ledger_capacity"].get(storage_tiers.RAM_CAPACITY_KIND, 0) == 0
    _assert_observations(record, persisted, operator, free=None, shmem=None)


def test_membership_change_discards_all_observations_and_refuses_without_new_epoch(
        tmp_path: Path, clock: list[float], capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch) -> None:
    evidence = _RamEvidence(tmp_path / "evidence")
    before = evidence.discover()[RAM_TIER]
    _assert_admitted(before)
    read_text = Path.read_text
    reads = 0

    def changing_membership(path: Path, *args, **kwargs) -> str:
        nonlocal reads
        if path == evidence.nodes / "has_memory":
            reads += 1
            return "0,2\n" if reads == 1 else "0\n"
        return read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", changing_membership)
    record = evidence.discover()[RAM_TIER]
    assert reads == 2
    admission = record["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["reason"] == "ram_numa_topology_unreadable"
    assert admission["admissible"] is False
    assert storage_tiers.tier_tokens(record) == {}
    assert record["epoch"] == before["epoch"]
    queue = _queue(tmp_path / "pb-queue")
    persisted = _publish(queue, record)
    _assert_observations(record, persisted, _cli(queue, capsys)[RAM_TIER],
                         free=None, shmem=None)


def test_discovery_uses_one_membership_bracket_for_totals_free_and_shmem(
        tmp_path: Path, clock: list[float], capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch) -> None:
    evidence = _RamEvidence(tmp_path / "evidence")
    read_text = Path.read_text
    reads: list[str] = []

    def observed_read(path: Path, *args, **kwargs) -> str:
        if evidence.nodes in path.parents:
            reads.append(str(path.relative_to(evidence.nodes)))
        return read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", observed_read)
    record = evidence.discover()[RAM_TIER]
    _assert_admitted(record)
    assert reads == ["has_memory", "node0/meminfo", "node2/meminfo", "has_memory"]
    queue = _queue(tmp_path / "pb-queue")
    persisted = _publish(queue, record)
    _assert_observations(record, persisted, _cli(queue, capsys)[RAM_TIER],
                         free=DEFAULT_FREE, shmem=DEFAULT_SHMEM)
    assert reads == ["has_memory", "node0/meminfo", "node2/meminfo", "has_memory"]


def test_resampling_diagnostics_changes_values_but_not_admission_epoch_or_tokens(
        tmp_path: Path, clock: list[float], capsys: pytest.CaptureFixture[str]) -> None:
    evidence = _RamEvidence(tmp_path / "evidence")
    before = evidence.discover()[RAM_TIER]
    _assert_admitted(before)
    queue = _queue(tmp_path / "pb-queue")
    persisted_before = _publish(queue, before)
    operator_before = _cli(queue, capsys)[RAM_TIER]
    # Balanced second sample versus deliberately unequal primary sample.
    for node in evidence.ids:
        evidence.write_node(node, free=101, shmem=202)
    after = evidence.discover(now=SAMPLED + 1)[RAM_TIER]
    _assert_admitted(after)
    assert after["epoch"] == before["epoch"]
    assert after["ram_admission"] == before["ram_admission"]
    assert storage_tiers.tier_tokens(after) == storage_tiers.tier_tokens(before)
    assert after["sampled_unix"] == SAMPLED + 1
    persisted_after = _publish(queue, after)
    operator_after = _cli(queue, capsys)[RAM_TIER]
    assert operator_after["sampled_age_s"] == 4.0
    _assert_observations(before, persisted_before, operator_before,
                         free=DEFAULT_FREE, shmem=DEFAULT_SHMEM)
    _assert_observations(after, persisted_after, operator_after,
                         free={"0": 101 * 1024, "2": 101 * 1024},
                         shmem={"0": 202 * 1024, "2": 202 * 1024})


def test_old_non_ram_and_unannounced_rows_have_explicit_unknown_observations(
        tmp_path: Path, clock: list[float], capsys: pytest.CaptureFixture[str]) -> None:
    evidence = _RamEvidence(tmp_path / "evidence")
    tiers = evidence.discover()
    _assert_admitted(tiers[RAM_TIER])
    queue = _queue(tmp_path / "pb-queue")
    legacy = {key: value for key, value in tiers[RAM_TIER].items()
              if key not in (*OBSERVATIONS, "memory_nodes")}
    _publish(queue, legacy)
    arc_id = storage_tiers.tier_id("arc", HOST)
    # Real discovered non-RAM tier with misleading additive persisted fields:
    # status must not claim RAM node observations on the ARC lane.
    arc = dict(tiers[arc_id], memory_nodes=[0, 2],
               node_memfree_bytes=DEFAULT_FREE, node_shmem_bytes=DEFAULT_SHMEM)
    _publish(queue, arc)
    unannounced = storage_tiers.tier_id("ram", "unannounced-fixture")
    queue.mint_tier_capacity(unannounced, {storage_tiers.RAM_CAPACITY_KIND: 1})
    rows = _cli(queue, capsys)
    assert rows[RAM_TIER]["announced"] is True
    assert rows[arc_id]["tier_kind"] == "arc"
    assert rows[unannounced]["announced"] is False
    assert rows[RAM_TIER]["sampled_age_s"] == rows[arc_id]["sampled_age_s"] == 5.0
    assert rows[unannounced]["sampled_age_s"] is None
    for tier_id in (RAM_TIER, arc_id, unannounced):
        for field in (*OBSERVATIONS, "memory_nodes"):
            assert field in rows[tier_id], ("unknown must be explicit null", rows[tier_id])
            assert rows[tier_id].get(field) is None, rows[tier_id]


def _file_state(root: Path) -> dict[str, tuple[bytes, int]]:
    return {str(path.relative_to(root)): (path.read_bytes(), path.stat().st_mtime_ns)
            for path in root.rglob("*") if path.is_file()}


def test_status_reads_persisted_ram_observations_without_sampling_or_writes(
        tmp_path: Path, clock: list[float], capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch) -> None:
    evidence = _RamEvidence(tmp_path / "evidence")
    record = evidence.discover()[RAM_TIER]
    _assert_admitted(record)
    queue = _queue(tmp_path / "pb-queue")
    persisted = _publish(queue, record)
    for node in evidence.ids:
        evidence.write_node(node, free=0, shmem=0)
    read_text = Path.read_text

    def no_status_sampling(path: Path, *args, **kwargs) -> str:
        if (evidence.nodes in path.parents
                or Path(storage_tiers.MEMORY_NUMA_ROOT) in path.parents):
            raise AssertionError("status must not resample producer NUMA evidence")
        return read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", no_status_sampling)
    before = _file_state(tmp_path)
    operator = _cli(queue, capsys)[RAM_TIER]
    assert _file_state(tmp_path) == before
    _assert_observations(record, persisted, operator,
                         free=DEFAULT_FREE, shmem=DEFAULT_SHMEM)


def test_liveness_refresh_keeps_sample_stamp_and_observations_aging(
        tmp_path: Path, clock: list[float], capsys: pytest.CaptureFixture[str]) -> None:
    evidence = _RamEvidence(tmp_path / "evidence")
    record = evidence.discover()[RAM_TIER]
    _assert_admitted(record)
    queue = _queue(tmp_path / "pb-queue")
    queue.mint_tier_capacity(RAM_TIER, storage_tiers.tier_tokens(record))
    liveness = tier_loop.Liveness(interval_s=5)
    liveness.begin_cycle()
    liveness.announce(queue, record)
    original = json.loads(queue.tier_record_path(RAM_TIER).read_text())
    for node in evidence.ids:
        evidence.write_node(node, free=0, shmem=0)
    clock[0] += 100
    liveness.checkpoint("fixture-progress")
    refreshed = json.loads(queue.tier_record_path(RAM_TIER).read_text())
    assert refreshed["announced_unix"] > original["announced_unix"]
    assert refreshed["sampled_unix"] == original["sampled_unix"] == SAMPLED
    assert refreshed["epoch"] == record["epoch"]
    assert refreshed["liveness_refresh"]["refreshes"] == 1
    assert refreshed["liveness_refresh"]["minted_unix"] == original["announced_unix"]
    assert storage_tiers.tier_tokens(refreshed) == storage_tiers.tier_tokens(record)
    operator = _cli(queue, capsys)[RAM_TIER]
    assert operator["sampled_age_s"] == 105.0
    _assert_observations(record, refreshed, operator,
                         free=DEFAULT_FREE, shmem=DEFAULT_SHMEM)


def test_real_tier_cycle_mints_and_announces_discovered_observations_to_status(
        tmp_path: Path, clock: list[float], capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch) -> None:
    evidence = _RamEvidence(tmp_path / "evidence")
    policy_path = tmp_path / "ram-policy.json"
    policy_path.write_text(json.dumps(evidence.policy))
    # Redirect the actual policy reader to a private file; do not replace it.
    monkeypatch.setattr(storage_tiers, "RAM_POLICY_FILE", str(policy_path))
    assert tier_loop.load_ram_policy() == evidence.policy
    real_statvfs = os.statvfs

    def private_statvfs(path):
        if str(path) == str(evidence.mount):
            return evidence.statvfs(str(path))
        return real_statvfs(path)

    # The mint's own writable resample sees the same private tmpfs facts.
    monkeypatch.setattr(tier_loop.os, "statvfs", private_statvfs)
    queue = _queue(tmp_path / "pb-queue")
    queue.ledger(HOST).ensure_capacity({"mem_gb": 294})
    assert queue.ledger(HOST).acquire("fixture-row", {"mem_gb": 88})
    assert queue.rows_host_memory_held(HOST) == 88
    announced = tier_loop.cycle(
        queue, host=HOST, source_pool="fixture-source",
        receipts=tier_loop.ReceiptCache(), now=SAMPLED, discover=evidence.discover)
    record = next(row for row in announced if row["tier_id"] == RAM_TIER)
    _assert_admitted(record)
    assert record["stage_root_owner"] == "registered"
    assert queue.tier_ledger(RAM_TIER).capacity()[storage_tiers.RAM_CAPACITY_KIND] == 112
    persisted = json.loads(queue.tier_record_path(RAM_TIER).read_text())
    assert persisted == dict(record, announced_unix=NOW)
    assert tier_loop.LAST_CYCLE["completed"] is True
    # Cycle event logs are not the CLI's one JSON blob.
    capsys.readouterr()
    _assert_observations(record, persisted, _cli(queue, capsys)[RAM_TIER],
                         free=DEFAULT_FREE, shmem=DEFAULT_SHMEM)

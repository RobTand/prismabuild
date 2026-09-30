"""Producer wiring from injected daemon bytes; never live Docker qualification."""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import container_images as ci  # noqa: E402
from test_container_image_inventory import (  # noqa: E402
    GLM_TAG, ID_A, _fixture, _fixture_row, _inspect_row, _line,
)
from test_container_class_requirements import _policy  # noqa: E402

STORE = "/var/lib/docker"


class Probe:
    def __init__(self, box="sparky", *, info=None, inspected=None, clock=None):
        self.listing = _fixture(box, "ls.txt")
        self.inspected = _fixture(box, "inspect.jsonl") if inspected is None else inspected
        self.info = json.dumps(STORE).encode() if info is None else info
        self.clock = clock
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        if self.clock is not None:
            self.clock[0] += 1
        if "ls" in argv:
            return self.listing
        if "inspect" in argv:
            return self.inspected
        assert "info" in argv
        return self.info


def refreshed(tmp_path, probe, **kwargs):
    cache = ci.InventoryCache(root=tmp_path / "inventory", probe=probe,
                              clock=lambda: 1000.0, docker="/mock/docker", **kwargs)
    entries = cache.get()
    record = json.loads((cache.root / cache.record_name).read_text())
    return cache, entries, record


def verdict(record, store=STORE):
    want = ci.content_ref(_fixture_row("sparky", GLM_TAG))
    policy = _policy(names={GLM_TAG: want})
    policy["classes"]["gb10"]["store_root"] = store
    return ci.class_image_verdict(policy, "gb10", record, now=1000.0)


@pytest.mark.parametrize("box", ["sparky", "sparklina"])
def test_cache_refresh_produces_observed_store_and_named_contents(tmp_path, box):
    probe = Probe(box)
    cache, entries, record = refreshed(tmp_path, probe)
    assert entries is not None
    assert record["store_root"] == STORE
    assert record["image_contents"][GLM_TAG] == ci.content_ref(_fixture_row(box, GLM_TAG))
    assert verdict(record)["status"] == "satisfied"
    assert verdict(record)["authority"] == "supplied_snapshot_only"
    assert len(probe.calls) == 3
    assert sum("inspect" in argv for argv, _ in probe.calls) == 1
    assert cache.get() == entries
    assert len(probe.calls) == 3


def test_store_evidence_is_not_copied_from_requirements(tmp_path):
    probe = Probe(info=json.dumps("/actual/daemon/store").encode())
    _, entries, record = refreshed(tmp_path, probe)
    assert entries is not None
    assert record["store_root"] == "/actual/daemon/store"
    assert verdict(record)["reason"] == "image_store_mismatch"
    assert verdict(record, "/actual/daemon/store")["status"] == "satisfied"


@pytest.mark.parametrize("info", [
    pytest.param(b"", id="empty"),
    pytest.param(b"not JSON", id="malformed"),
    pytest.param(b"null", id="null"),
    pytest.param(b"{}", id="wrong-shape"),
    pytest.param(b'"relative"', id="relative"),
    pytest.param(b'"/var/lib/../docker"', id="traversal"),
    pytest.param(b'"/var/lib/docker/"', id="noncanonical"),
    pytest.param(b'"/docker\\u0000"', id="nul"),
    pytest.param(b'"' + b"x" * (ci.MAX_INVENTORY_BYTES + 1) + b'"', id="oversized"),
])
def test_bad_supplemental_info_keeps_legacy_inventory_unknown_for_class(tmp_path, info):
    probe = Probe(info=info)
    cache, entries, record = refreshed(tmp_path, probe)
    legacy = ci.observe(probe=Probe())
    assert entries == legacy and entries is not None
    assert verdict(record)["status"] == "unknown"
    assert record.get("store_root") is None
    assert cache.get() == entries
    assert sum("info" in argv for argv, _ in probe.calls) == 1


@pytest.mark.parametrize("failure", ["absent", "exception", "decode"])
def test_info_probe_failure_does_not_discard_valid_references(tmp_path, failure):
    class FailingProbe(Probe):
        def __call__(self, argv, **kwargs):
            if "info" in argv:
                self.calls.append((argv, kwargs))
                if failure == "exception":
                    raise OSError("metadata unavailable")
                return None if failure == "absent" else b"\xff"
            return super().__call__(argv, **kwargs)
    probe = FailingProbe()
    _, entries, record = refreshed(tmp_path, probe)
    assert entries is not None and entries == ci.observe(probe=Probe())
    assert verdict(record)["status"] == "unknown"
    assert sum("info" in argv for argv, _ in probe.calls) == 1


def test_three_reads_share_endpoint_environment_and_decreasing_budget(tmp_path, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(ci.time, "monotonic", lambda: clock[0])
    monkeypatch.setenv("DOCKER_HOST", "tcp://foreign:2375")
    monkeypatch.setenv("DOCKER_CONTEXT", "foreign")
    probe = Probe(clock=clock)
    _, entries, record = refreshed(tmp_path, probe, timeout_s=5)
    assert entries is not None and verdict(record)["status"] == "satisfied"
    assert [kwargs["timeout_s"] for _, kwargs in probe.calls] == [5, 4, 3]
    for argv, kwargs in probe.calls:
        assert argv[:3] == ["/mock/docker", "--host", ci.LOCAL_ENDPOINT]
        assert "DOCKER_HOST" not in kwargs["env"]
        assert "DOCKER_CONTEXT" not in kwargs["env"]
        assert kwargs["limit"] <= ci.MAX_INVENTORY_BYTES


def test_exhausted_shared_budget_keeps_refs_without_starting_info(tmp_path, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(ci.time, "monotonic", lambda: clock[0])
    probe = Probe(clock=clock)
    _, entries, record = refreshed(tmp_path, probe, timeout_s=2)
    assert entries is not None
    assert record.get("store_root") is None
    assert verdict(record)["status"] == "unknown"
    assert len(probe.calls) == 2


def test_invalid_named_projection_keeps_valid_legacy_references(tmp_path):
    rows = [json.loads(line) for line in _fixture("sparky", "inspect.jsonl").decode().splitlines()
            if line]
    rows[0].pop("RepoTags", None)
    inspected = ("\n".join(json.dumps(row) for row in rows) + "\n").encode()
    probe = Probe(inspected=inspected)
    _, entries, record = refreshed(tmp_path, probe)
    assert entries is not None
    assert record.get("image_contents") is None
    assert verdict(record)["status"] == "unknown"
    assert sum("inspect" in argv for argv, _ in probe.calls) == 1


def test_required_tag_drift_is_not_excused_by_content_under_another_name(tmp_path):
    rows = [json.loads(line) for line in _fixture("sparky", "inspect.jsonl").decode().splitlines()
            if line]
    for row in rows:
        if GLM_TAG in (row.get("RepoTags") or []):
            row["RepoTags"] = ["unrelated:tag"]
    inspected = ("\n".join(json.dumps(row) for row in rows) + "\n").encode()
    _, entries, record = refreshed(tmp_path, Probe(inspected=inspected))
    assert entries is not None
    assert verdict(record)["reason"] == "required_image_missing"


def test_stale_refresh_does_not_retain_old_supplemental_proof(tmp_path):
    probe = Probe()
    cache, entries, first = refreshed(tmp_path, probe)
    assert first.get("store_root") == STORE
    cache.clock = lambda: 1031.0
    probe.info = b"null"
    assert cache.get() == entries
    latest = json.loads((cache.root / cache.record_name).read_text())
    assert latest["observed_unix"] == 1031.0
    assert latest.get("store_root") is None
    want = ci.content_ref(_fixture_row("sparky", GLM_TAG))
    assert ci.class_image_verdict(_policy(names={GLM_TAG: want}), "gb10", latest,
                                  now=1031.0)["status"] == "unknown"
    assert sum("ls" in argv for argv, _ in probe.calls) == 2
    assert sum("info" in argv for argv, _ in probe.calls) == 2


def test_empty_inventory_is_observed_empty_not_unknown(tmp_path):
    probe = Probe()
    probe.listing = b""
    _, entries, record = refreshed(tmp_path, probe)
    assert entries == frozenset()
    assert record["store_root"] == STORE
    assert record["image_contents"] == {}
    assert verdict(record)["reason"] == "required_image_missing"
    assert sum("inspect" in argv for argv, _ in probe.calls) == 0
    assert sum("info" in argv for argv, _ in probe.calls) == 1


def test_supplemental_metadata_cannot_overflow_a_valid_legacy_record(tmp_path, monkeypatch):
    monkeypatch.setattr(ci, "MAX_INVENTORY_BYTES", 1024)
    row = json.loads(_inspect_row(ID_A))
    row["RepoTags"] = [f"image-{i}:tag" for i in range(30)]
    inspected = (json.dumps(row) + "\n").encode()
    assert len(inspected) <= ci.MAX_INVENTORY_BYTES
    probe = Probe(inspected=inspected)
    probe.listing = _line(ID_A)
    cache, entries, record = refreshed(tmp_path, probe)
    assert entries is not None and ID_A in entries
    assert record.get("image_contents") is None
    assert record.get("store_root") is None
    assert len((cache.root / cache.record_name).read_bytes()) <= ci.MAX_INVENTORY_BYTES
    assert cache.get() == entries
    assert len(probe.calls) == 3


def test_legacy_observe_contract_does_not_add_an_info_probe():
    probe = Probe()
    entries = ci.observe(probe=probe)
    assert entries is not None
    assert len(probe.calls) == 2
    assert all("info" not in argv for argv, _ in probe.calls)

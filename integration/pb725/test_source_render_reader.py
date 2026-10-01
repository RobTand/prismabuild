"""Refs #725: producer-retained source/BF16 expectations and actual SDK1 movers.

Three behaviors, parameterized independently over the two real reader seams.
The default fixture deliberately remains inactive until parent-verified RED.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import os
from contextlib import contextmanager, nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest
from pb725_scaffold import (
    OriginPayloadOpened,
    PayloadTripwire,
    move_manifest,
    persist_case,
    private_runtime,
    setup_complete,
)


class NativeAcquisitionUnproven(AssertionError):
    """Observer limit, never a behavioral RED witness."""


def _native_origin_handles(identity):
    """Independent Linux kernel census; no CPython open-audit inference."""
    device, inode = identity
    found = set()
    for line in Path("/proc/self/maps").read_text().splitlines():
        fields = line.split(maxsplit=5)
        major, minor = (int(part, 16) for part in fields[3].split(":"))
        if (os.makedev(major, minor), int(fields[4])) == identity:
            found.add(("map", *fields[:5]))
    for path in Path("/proc/self/fd").iterdir():
        try:
            info = os.fstat(int(path.name))
        except OSError:
            continue  # /proc's directory descriptor can close between census and stat.
        if (info.st_dev, info.st_ino) == (device, inode):
            found.add(("fd", path.name, str(device), str(inode)))
    return found


class SourceObserver:
    def __init__(self, source, layer, monkeypatch, evidence):
        self.source = source
        self.layer = layer
        self.monkeypatch = monkeypatch
        self.evidence = evidence
        self.pool_calls = []
        self.native_acquisitions = []
        self.header_reads = []
        self.real_opener = layer.safe_open
        self.real_pread = os.pread
        evidence.update(source_pool_calls=self.pool_calls,
                        source_native_acquisitions=self.native_acquisitions,
                        source_header_reads=self.header_reads)

    def census(self, identity):
        try:
            return _native_origin_handles(identity)
        except (OSError, ValueError, IndexError) as error:
            message = f"Linux native acquisition census unavailable: {error}"
            self.evidence["source_observer_limit"] = message
            raise NativeAcquisitionUnproven(message) from error

    def pool_opener(self, path, **kwargs):
        info = os.stat(path)
        identity = (info.st_dev, info.st_ino)
        assert identity == self.source.identity, "source observer operand identity changed"
        before = self.census(identity)
        self.pool_calls.append(str(path))
        handle = self.real_opener(path, **kwargs)  # Real native constructor, never a fake handle.
        try:
            acquired = self.census(identity) - before
            if not acquired:
                message = "no newly acquired exact-origin native map/descriptor after real safe_open"
                self.evidence["source_observer_limit"] = message
                raise NativeAcquisitionUnproven(message)
            record = {"path": str(path), "device": identity[0], "inode": identity[1],
                      "new_kernel_handles": sorted(acquired)}
            self.native_acquisitions.append(record)
        finally:
            handle.__exit__(None, None, None)  # Release actual handle BEFORE trip/refusal.
            del handle
        remaining = self.census(identity) - before
        record["handles_after_close"] = sorted(remaining)
        if remaining:
            message = "new native origin handles survived real handle close"
            self.evidence["source_observer_limit"] = message
            raise NativeAcquisitionUnproven(message)
        print("PB725_SOURCE_NATIVE_ACQUIRED " + json.dumps(record), flush=True)
        raise OriginPayloadOpened(f"forbidden origin native acquisition: {path}")

    def bounded_pread(self, fd, count, offset):
        info = os.fstat(fd)
        if (info.st_dev, info.st_ino) == self.source.identity:
            assert 0 <= offset < offset + count <= self.source.header_base, (
                "origin pread escaped bounded header", offset, count, self.source.header_base)
            self.header_reads.append({"fd": fd, "offset": offset, "bytes": count,
                                      "device": info.st_dev, "inode": info.st_ino})
        return self.real_pread(fd, count, offset)

    @contextmanager
    def armed(self):
        with self.monkeypatch.context() as patch:
            patch.setattr(self.layer, "safe_open", self.pool_opener)
            patch.setattr(os, "pread", self.bounded_pread)
            yield self


@pytest.fixture
def reader_policy():
    """Execute the unchanged reviewed application's default strict tier policy."""
    from prismaquant.staged_tier_policy import (
        DEFAULT_ALLOWED_TIERS, staged_tier_policy_test_context,
    )

    return staged_tier_policy_test_context(DEFAULT_ALLOWED_TIERS)


@pytest.fixture(params=("source", "render"))
def chain(tmp_path, monkeypatch, request):
    if not os.environ.get("PB725_BINDING_PATH"):
        pytest.fail("opt-in setup missing: use the admitted run_activation.py launcher")
    binding = json.loads(Path(os.environ["PB725_BINDING_PATH"]).read_text())
    assert binding["schema"] == "prismabuild.pb725.source_render_integration.v1"
    evidence = {"case": request.node.name, "reader_kind": request.param,
                "binding_scope": binding["scope"], "expected_policy": ["ram", "ssd"]}
    installed_root = Path(os.environ["PB725_PACKAGE_ROOT"]).resolve()
    observed_chain = None
    with private_runtime(monkeypatch, binding) as runtime:
        try:
            import torch
            from safetensors.torch import save_file

            layer = importlib.import_module("prismaquant.layer_streaming")
            plan = importlib.import_module("prismaquant.source_read_plan")
            pwc = importlib.import_module("prismaquant.production_weight_cache")
            operands = binding["operands"]
            assert operands["render_format"] == "BF16"
            assert operands["prefetch_max_workers"] == 1
            origin = tmp_path / "origin"
            origin.mkdir()
            tensor = torch.arange(operands["tensor_elements"], dtype=torch.float32).reshape(
                operands["shape"])
            source_names = operands["source_names"]
            source_expected = {name: (tensor + index).clone()
                               for index, name in enumerate(source_names)}
            source_path = origin / "source.safetensors"
            print("PB725_FIXTURE_PHASE producer " + request.node.name, flush=True)
            save_file(source_expected, str(source_path))
            # Retain expectations and real absolute selections BEFORE staging, not origin rereads.
            spans = [plan.selection_spans({str(source_path): [(name, name)]})[0]
                     for name in source_names]
            _, header_base, _ = plan.read_safetensors_header(str(source_path))
            source_bytes = source_path.read_bytes()
            source_info = source_path.stat()
            source = SimpleNamespace(path=source_path, names=source_names, spans=spans,
                expected=source_expected, header_base=header_base,
                identity=(source_info.st_dev, source_info.st_ino))
            source_entry = {"path": spans[0][0], "offset": spans[0][1],
                            "bytes": spans[0][2] - spans[0][1],
                            "sha256": hashlib.sha256(source_bytes[spans[0][1]:spans[0][2]]).hexdigest()}
            assert source_entry["offset"] > 0
            assert spans[0][2] <= spans[1][1], "omitted valid span must not overlap coverage"
            weights, render_expected, renders = {}, {}, []
            for index, qname in enumerate(operands["render_names"]):
                weight = (tensor + index).to(torch.bfloat16)
                rendered = pwc.render_production_weight(
                    weight, "BF16", qname=qname, activations={}, levers={})
                assert rendered.device.type == "cpu" and rendered.dtype == torch.bfloat16
                render_expected[qname] = rendered.detach().clone().contiguous()
                pwc._store_rendered_weight_entry(weights=weights, cache_dir_path=origin,
                    qname=qname, fmt="BF16", tensor=rendered, weight_dtype=torch.bfloat16)
                path = origin / weights[(qname, "BF16")]
                raw = path.read_bytes()
                assert len(raw) <= operands["max_file_bytes"]
                renders.append({"path": str(path), "offset": 0, "bytes": len(raw),
                                "sha256": hashlib.sha256(raw).hexdigest(), "qname": qname})
            render_entry = {name: renders[0][name] for name in ("path", "offset", "bytes", "sha256")}
            moved = move_manifest(runtime, tmp_path, monkeypatch,
                entries=[source_entry, render_entry], origin=origin, tier=operands["tier_id"],
                scope="source-render-only", producer="pb725-source-render-integration",
                case_name=request.node.name, evidence=evidence)
            assert moved.resolver.staged_read(Path(renders[0]["path"]),
                expected_sha256=renders[0]["sha256"])
            assert moved.resolver.staged_read(Path(renders[1]["path"]),
                expected_sha256=renders[1]["sha256"]) is None
            assert plan.uncovered_spans([source_entry, render_entry], spans) == [spans[1]]
            tripwire = PayloadTripwire([row["path"] for row in renders],
                                      moved.staged_paths, moved.leases)
            observer = SourceObserver(source, layer, monkeypatch, evidence)
            evidence.update({"source_spans": spans, "source_header_base": header_base,
                             "source_identity": list(source.identity), "render_references": renders,
                             "producer_expectations_retained_before_staging": True,
                             "lru_max_bytes": operands["lru_max_bytes"],
                             "max_file_bytes": operands["max_file_bytes"], "prefetch_max_workers": 1})
            observed_chain = SimpleNamespace(
                kind=request.param, torch=torch, source=source, layer=layer, pwc=pwc,
                renders=renders, render_expected=render_expected, operands=operands,
                resolver=moved.resolver, tripwire=tripwire, leases=moved.leases,
                tier=moved.tier, evidence=evidence, observer=observer,
                source_entry=source_entry, render_entry=render_entry)
            setup_complete(evidence, request.node.name)
            yield observed_chain
        finally:
            persist_case(evidence, observed_chain, installed_root, request.node.name)


def _read(chain, index):
    policy = importlib.import_module("prismaquant.staged_tier_policy").active_policy()
    chain.allowed_tiers_at_prefetch = None if policy is None else sorted(policy)
    if chain.kind == "source":
        with chain.layer._source_safe_open(str(chain.source.path), framework="pt") as reader:
            assert reader._handle is None, "strict source must create NoPoolHandle"
            return reader.get_tensor(chain.source.names[index])
    row = chain.renders[index]
    key = (row["qname"], "BF16")
    cache = chain.pwc.ProductionWeightCache(weights={key: row["path"]}, levers={})
    cache.enable_lru(chain.operands["lru_max_bytes"])
    cache.require_file_load_sha256({key: row["sha256"]},
                                  max_file_bytes=chain.operands["max_file_bytes"])
    try:
        assert cache.prefetch([key], max_workers=1) == 1
        tensor = cache.get(*key, resident_only=True)
        assert 0 < cache._lru_bytes <= chain.operands["lru_max_bytes"]
        receipt = cache.file_load_receipt(key, tensor)
        assert receipt["path"] == row["path"] and receipt["sha256"] == row["sha256"]
        chain.evidence["render_load_receipt"] = receipt
        return tensor
    finally:
        cache.release_resident_tensors([key])
        cache.disable_file_load_receipts()


def _assert_released(chain):
    assert list(chain.leases.glob("*.lease.json")) == []


def test_default_strict_serves_stage_without_origin_payload(chain, reader_policy):
    with reader_policy, chain.tripwire, chain.observer.armed():
        got = _read(chain, 0)
    expected = (chain.source.expected[chain.source.names[0]] if chain.kind == "source"
                else chain.render_expected[chain.renders[0]["qname"]])
    assert chain.torch.equal(got.view(chain.torch.uint8), expected.view(chain.torch.uint8))
    assert chain.tripwire.origin_opens == [] and chain.observer.pool_calls == []
    if chain.kind == "source":
        assert chain.observer.header_reads, "bounded origin header access must remain observable"
    assert chain.tripwire.staged_opens
    entry = chain.source_entry if chain.kind == "source" else chain.render_entry
    key = f"{entry['offset']}:{entry['path']}"
    report = chain.resolver.report()
    assert report["bytes_from_stage"] == entry["bytes"]
    assert report["bytes_from_pool"] == 0
    if chain.kind == "render" and chain.allowed_tiers_at_prefetch is None:
        witness = {"staged_opens": list(chain.tripwire.staged_opens),
                   "pins_at_open": list(chain.tripwire.pins_at_open),
                   "bytes_from_stage": report["bytes_from_stage"],
                   "tensor_matches_producer": True, "range_ref": key}
        chain.evidence["inactive_render_stage_pin_witness"] = witness
        print("PB725_RENDER_INACTIVE_STAGE_PIN_WITNESS " + json.dumps(witness), flush=True)
    assert chain.tripwire.pins_at_open, (
        "actual staged consumption had no live PB pin/lease at staged descriptor open")
    serving = report["serving_tiers"][-1]
    assert serving["serving_tier"] == "stage" and serving["tier_id"] == chain.tier
    assert serving["pin_id"] and serving["range_ref"] == key
    assert serving["lease_id"] == serving["pin_id"]
    assert any(pin["pin_id"] == serving["pin_id"] and pin["refs"]
               and any(row["key"] == key and row["sha256"] == entry["sha256"]
                       and row["bytes"] == entry["bytes"]
                       and row["stage_path"] in chain.tripwire.staged_opens
                       for row in pin["entries"])
               for pin in chain.tripwire.pins_at_open)
    _assert_released(chain)


def test_valid_uncovered_refuses_before_payload(chain, reader_policy):
    policy = importlib.import_module("prismaquant.staged_tier_policy")
    reason = "readset-not-staged" if chain.kind == "source" else "staged-not-serving"
    with (reader_policy, chain.tripwire, chain.observer.armed(),
          pytest.raises(policy.TierPolicyRefused, match=reason)):
        _read(chain, 1)
    assert chain.tripwire.origin_opens == [] and chain.observer.pool_calls == []
    assert chain.tripwire.staged_opens == []
    assert chain.resolver.report()["bytes_from_stage"] == 0
    assert chain.resolver.report()["bytes_from_pool"] == 0
    _assert_released(chain)


def test_inactive_uncovered_actual_origin_control(chain):
    policy = importlib.import_module("prismaquant.staged_tier_policy")
    assert policy.active_policy() is None
    # Covered inactive PWC still selects stage. This control MUST use the valid omitted shard.
    with (chain.tripwire, chain.observer.armed(),
          pytest.raises(OriginPayloadOpened, match="forbidden origin")):
        _read(chain, 1)
    if chain.kind == "source":
        assert chain.observer.pool_calls == [str(chain.source.path)]
        (acquisition,) = chain.observer.native_acquisitions
        assert acquisition["new_kernel_handles"] and acquisition["handles_after_close"] == []
        assert chain.tripwire.origin_opens == []  # Source native proof is NOT CPython audit proof.
    else:
        assert chain.tripwire.origin_opens == [chain.renders[1]["path"]]
    assert chain.tripwire.staged_opens == []
    _assert_released(chain)
    print("PB725_SOURCE_RENDER_OLD_BOUNDARY_CAUGHT " + chain.kind, flush=True)

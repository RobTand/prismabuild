"""Compressed manifests retain exact read order with separate byte bounds."""
import gzip
import hashlib
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb
from test_a_data_manifest_names_bytes_exactly import _manifest


def test_compressed_manifest_can_exceed_the_stored_byte_ceiling(tmp_path, monkeypatch):
    manifest = _manifest()
    manifest["entries"] = [
        {"path": f"/mnt/shared/captures/layer-{i:05d}/activation.pt",
         "offset": 0, "bytes": i + 1, "sha256": None}
        for i in range(100)
    ]
    manifest["entry_count"] = len(manifest["entries"])
    manifest["total_bytes"] = sum(e["bytes"] for e in manifest["entries"])
    raw = json.dumps(manifest).encode()
    compressed = gzip.compress(raw, mtime=0)
    assert len(compressed) < 1024 < len(raw)
    monkeypatch.setattr(pb, "DATA_MANIFEST_MAX_BYTES", 1024)
    plain, packed = tmp_path / "plain.json", tmp_path / "packed"
    plain.write_bytes(raw)
    packed.write_bytes(compressed)
    with pytest.raises(pb.ActionContractError, match="exceeds"):
        pb.load_data_manifest(plain)
    assert pb.load_data_manifest(packed) == pb.validate_data_manifest(manifest)


def test_compressed_input_is_validated_after_cas_round_trip(tmp_path):
    manifest = _manifest()
    packed = tmp_path / "data.json.gz"
    packed.write_bytes(gzip.compress(json.dumps(manifest).encode(), mtime=0))
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    entry, _ = cas.ingest_input(packed, input_id=pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
    assert pb.load_data_manifest(cas.input_path(entry)) == pb.validate_data_manifest(manifest)


@pytest.mark.parametrize("encoding", ["identity", "gzip"])
def test_submitter_seals_the_wire_digest_and_compressed_encoding(tmp_path, monkeypatch, encoding):
    from test_pbrun_host_class import _sealed_body

    raw = json.dumps(_manifest()).encode()
    wire = gzip.compress(raw, mtime=0) if encoding == "gzip" else raw
    source = tmp_path / "manifest"
    source.write_bytes(wire)
    body = _sealed_body(
        ["--anywhere", "--data-manifest", str(source), "--", "true"],
        monkeypatch, tmp_path,
    )
    summary = body["params"]["data_manifest"]
    assert summary["input"]["sha256"] == hashlib.sha256(wire).hexdigest()
    assert summary["input"]["bytes"] == len(wire)
    assert summary["entry_count"] == 2 and summary["total_bytes"] == 30
    assert summary["input"] in body["inputs"]
    if encoding == "gzip":
        assert summary["content_encoding"] == "gzip"
    else:
        assert set(summary) == {"input", "mount_prefix", "entry_count", "total_bytes"}


def test_decompression_ceiling_is_applied_before_json_parsing(tmp_path, monkeypatch):
    source = tmp_path / "bomb"
    source.write_bytes(gzip.compress(b" " * 100_000, mtime=0))
    monkeypatch.setattr(pb, "DATA_MANIFEST_MAX_DECODED_BYTES", 1024)
    with pytest.raises(pb.ActionContractError, match="decoded data manifest exceeds 1024"):
        pb.load_data_manifest(source)


@pytest.mark.parametrize("fault", ["truncated", "crc", "trailing", "concatenated", "utf8"])
def test_invalid_compressed_streams_refuse(tmp_path, fault):
    raw = json.dumps(_manifest()).encode()
    wire = gzip.compress(raw, mtime=0)
    if fault == "truncated":
        wire = wire[:-3]
    elif fault == "crc":
        wire = wire[:-8] + bytes([wire[-8] ^ 1]) + wire[-7:]
    elif fault == "trailing":
        wire += b"unbound bytes"
    elif fault == "concatenated":
        wire += gzip.compress(b" ", mtime=0)
    elif fault == "utf8":
        wire = gzip.compress(b"\xff", mtime=0)
    source = tmp_path / "manifest"
    source.write_bytes(wire)
    with pytest.raises(pb.ActionContractError):
        pb.load_data_manifest(source)


@pytest.mark.parametrize("fault", ["path", "duplicate", "totals", "count"])
def test_compression_does_not_bypass_the_manifest_contract(tmp_path, monkeypatch, fault):
    manifest = _manifest()
    if fault == "path":
        manifest["entries"][0]["path"] = "/outside/the/mount"
    elif fault == "duplicate":
        manifest["entries"][1] = manifest["entries"][0]
        manifest["total_bytes"] = 20
    elif fault == "totals":
        manifest["total_bytes"] += 1
    else:
        monkeypatch.setattr(pb, "DATA_MANIFEST_MAX_ENTRIES", 1)
    source = tmp_path / "manifest"
    source.write_bytes(gzip.compress(json.dumps(manifest).encode(), mtime=0))
    with pytest.raises(pb.ActionContractError):
        pb.load_data_manifest(source)


def test_stored_compressed_bytes_are_bounded_independently(tmp_path, monkeypatch):
    source = tmp_path / "manifest"
    source.write_bytes(gzip.compress(json.dumps(_manifest()).encode(), mtime=0))
    monkeypatch.setattr(pb, "DATA_MANIFEST_MAX_BYTES", source.stat().st_size - 1)
    with pytest.raises(pb.ActionContractError, match="data manifest exceeds"):
        pb.load_data_manifest(source)


def test_exact_decoded_limit_is_accepted(tmp_path, monkeypatch):
    raw = json.dumps(_manifest()).encode()
    source = tmp_path / "manifest"
    source.write_bytes(gzip.compress(raw, mtime=0))
    monkeypatch.setattr(pb, "DATA_MANIFEST_MAX_DECODED_BYTES", len(raw))
    assert pb.load_data_manifest(source)["entry_count"] == 2

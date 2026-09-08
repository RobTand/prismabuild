"""A small compressed profile must not expand past the validation budget (#372)."""
import gzip
import json

import pytest

from prismabuild import core as pb


def _trace(size):
    body = json.dumps({"traceEvents": [{"ph": "X"}], "padding": ""}).encode()
    return body + b" " * (size - len(body))


@pytest.mark.parametrize("members", [1, 2])
def test_compressed_trace_expansion_is_refused_before_json_parsing(
    tmp_path, monkeypatch, members
):
    budget = 1024
    monkeypatch.setattr(pb, "PROFILE_BLOB_BUDGET_BYTES", budget)
    body = _trace(budget + 1)
    middle = len(body) // 2
    blob = (gzip.compress(body) if members == 1 else
            gzip.compress(body[:middle]) + gzip.compress(body[middle:]))
    assert len(blob) < budget
    path = tmp_path / "trace.json.gz"
    path.write_bytes(blob)

    def no_parse(*args, **kwargs):
        pytest.fail("oversized decoded trace reached the JSON parser")

    monkeypatch.setattr(pb.json, "loads", no_parse)
    with pytest.raises(pb.ProfileUnusable, match="decoded.*profile budget"):
        pb.TorchProfileBackend().read_profile(path)


@pytest.mark.parametrize("compressed", [False, True])
def test_trace_exactly_at_decoded_budget_is_accepted(tmp_path, monkeypatch, compressed):
    monkeypatch.setattr(pb, "PROFILE_BLOB_BUDGET_BYTES", 1024)
    body = _trace(1024)
    path = tmp_path / "trace.json.gz"
    path.write_bytes(gzip.compress(body) if compressed else body)
    record = pb.TorchProfileBackend().read_profile(path)
    assert record["events"] == 1
    assert record["compressed"] is compressed
    assert record["decoded_bytes"] == 1024


def test_plain_trace_reader_also_bounds_its_input(tmp_path, monkeypatch):
    monkeypatch.setattr(pb, "PROFILE_BLOB_BUDGET_BYTES", 1024)
    path = tmp_path / "trace.json"
    path.write_bytes(_trace(1025))
    with pytest.raises(pb.ProfileUnusable, match="profile budget"):
        pb.read_chrome_trace(path)


def test_compressed_input_is_read_with_a_decoded_limit(tmp_path, monkeypatch):
    """Bound the decoder's read, rather than checking after full expansion."""
    monkeypatch.setattr(pb, "PROFILE_BLOB_BUDGET_BYTES", 1024)
    path = tmp_path / "trace.json.gz"
    path.write_bytes(gzip.compress(_trace(1024 * 32)))
    original = gzip.GzipFile.read
    reads = []

    def bounded_read(self, size=-1):
        assert 0 <= size <= 1025, "unbounded gzip output allocation"
        reads.append(size)
        return original(self, size)

    monkeypatch.setattr(gzip.GzipFile, "read", bounded_read)
    with pytest.raises(pb.ProfileUnusable, match="decoded.*profile budget"):
        pb.read_chrome_trace(path)
    assert reads

"""Sentinel-only component cases; these do not qualify a hostile worker lane."""
from __future__ import annotations

import base64
import io
import json
from urllib.parse import quote_from_bytes

import pytest

from prismabuild.hostile_capture import CapturePolicy, TextCapture, capture_text


SENTINEL = 'PB_SENTINEL_12345678?/"é'


@pytest.mark.parametrize("encoding", ["raw", "base64", "base64url", "hex", "upperhex", "url", "json"])
@pytest.mark.parametrize("chunk", [1, 2, 7, 4096])
def test_split_and_encoded_sentinels_are_removed_before_any_file_write(tmp_path, encoding, chunk):
    raw = SENTINEL.encode()
    encoded = {"raw": raw, "base64": base64.b64encode(raw),
               "base64url": base64.urlsafe_b64encode(raw).rstrip(b"="),
               "hex": raw.hex().encode(), "upperhex": raw.hex().upper().encode(),
               "url": quote_from_bytes(raw, safe="").encode(),
               "json": json.dumps(SENTINEL, ensure_ascii=True)[1:-1].encode()}[encoding]
    policy = CapturePolicy([SENTINEL], max_output_bytes=4096)
    source = io.BytesIO(b"before " + encoded + b" after\n")
    path = tmp_path / "sanitized.log"
    observed = []
    with path.open("wb", buffering=0) as stream:
        class Sink:
            def write(self, value):
                observed.append(value)
                assert encoded not in value
                return stream.write(value)
        stats = capture_text(source, Sink(), policy, chunk_bytes=chunk)
    assert path.read_bytes() == b"before [REDACTED] after\n"
    assert stats["redactions"] == 1
    assert stats["finished"] is True
    assert SENTINEL not in repr(policy)
    assert SENTINEL not in repr(stats)


def test_overlapping_literals_use_one_longest_replacement():
    capture = TextCapture(CapturePolicy(["sentinel-12345678", "sentinel-12345678-extra"], max_output_bytes=1024))
    result = b"".join(capture.feed(bytes([byte])) for byte in b"<sentinel-12345678-extra>") + capture.finish()
    assert result == b"<[REDACTED]>"
    assert capture.statistics()["redactions"] == 1


@pytest.mark.parametrize("value", [b"\x00binary", b"bad\xffutf8", b"tail\xe2\x82", b"control\x7f"])
def test_binary_or_undecodable_bytes_are_omitted(value):
    capture = TextCapture(CapturePolicy([SENTINEL], max_output_bytes=1024))
    assert capture.feed(value) + capture.finish() == b""
    assert capture.statistics()["omitted_nontext"] is True


def test_output_ceiling_keeps_complete_utf8_characters():
    capture = TextCapture(CapturePolicy([SENTINEL], max_output_bytes=5))
    result = capture.feed("😀😀abcdef".encode()) + capture.finish()
    assert result == "😀".encode()
    assert capture.statistics()["emitted_bytes"] == 4
    assert capture.statistics()["truncated"] is True


def test_utf8_and_clear_text_survive_one_byte_chunks():
    value = "begin 😀\né end\t\r".encode()
    capture = TextCapture(CapturePolicy([SENTINEL], max_output_bytes=1024))
    result = b""
    for byte in value:
        piece = capture.feed(bytes([byte]))
        piece.decode("utf-8", "strict")
        result += piece
    result += capture.finish()
    assert result == value
    assert capture.finish() == b""
    with pytest.raises(ValueError, match="finished"):
        capture.feed(b"later")


def test_partial_sink_writes_never_receive_raw_values():
    written = bytearray()
    class Partial:
        def write(self, value):
            written.extend(value[:2])
            return min(2, len(value))
    stats = capture_text(io.BytesIO(SENTINEL.encode()), Partial(), CapturePolicy([SENTINEL], max_output_bytes=1024))
    assert written == b"[REDACTED]"
    assert stats["emitted_bytes"] == 10


def test_a_stalled_sink_fails_without_a_raw_fallback():
    class Stalled:
        def write(self, _value):
            return 0
    with pytest.raises(OSError, match="no valid progress"):
        capture_text(io.BytesIO(SENTINEL.encode()), Stalled(), CapturePolicy([SENTINEL], max_output_bytes=1024))


@pytest.mark.parametrize("literals,limit", [([], 1024), ([None], 1024), (["short"], 1024),
                                          (["x" * 1025], 1024), ([SENTINEL], True), ([SENTINEL], 0)])
def test_policy_errors_do_not_echo_private_values(literals, limit):
    with pytest.raises(ValueError) as raised:
        CapturePolicy(literals, max_output_bytes=limit)
    assert SENTINEL not in str(raised.value)

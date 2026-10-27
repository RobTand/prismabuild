"""Sentinel-only component cases; these do not qualify a hostile worker lane."""
from __future__ import annotations

import base64
import io
import json
from urllib.parse import quote_from_bytes

import pytest

from prismabuild.hostile_capture import CapturePolicy, TextCapture, capture_text


SENTINEL = 'PB_SENTINEL_12345678?/"é'


@pytest.mark.parametrize("encoding", ["raw", "base64", "base64-unpadded", "base64url",
                                      "base64url-padded", "hex", "upperhex", "url", "url-lower", "json"])
@pytest.mark.parametrize("chunk", [1, 2, 7, 4096])
def test_split_and_encoded_sentinels_are_removed_before_any_file_write(tmp_path, encoding, chunk):
    raw = SENTINEL.encode()
    quoted = quote_from_bytes(raw, safe="").encode()
    lower_quoted = quoted.replace(b"%3F", b"%3f").replace(b"%2F", b"%2f").replace(b"%C3", b"%c3").replace(b"%A9", b"%a9")
    encoded = {"raw": raw, "base64": base64.b64encode(raw),
               "base64-unpadded": base64.b64encode(raw).rstrip(b"="),
               "base64url": base64.urlsafe_b64encode(raw).rstrip(b"="),
               "base64url-padded": base64.urlsafe_b64encode(raw),
               "hex": raw.hex().encode(), "upperhex": raw.hex().upper().encode(),
               "url": quoted, "url-lower": lower_quoted,
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
    assert SENTINEL not in repr(stats)


def test_overlapping_literals_use_one_longest_replacement():
    capture = TextCapture(CapturePolicy(["sentinel-12345678", "sentinel-12345678-extra"], max_output_bytes=1024))
    result = b"".join(capture.feed(bytes([byte])) for byte in b"<sentinel-12345678-extra>") + capture.finish()
    assert result == b"<[REDACTED]>"
    assert capture.statistics()["redactions"] == 1


@pytest.mark.parametrize("chunk", [1, 7, 4096])
def test_staggered_secret_occurrences_redact_their_complete_union(chunk):
    capture = TextCapture(CapturePolicy(["AAAABBBBCCCC", "CCCCDDDDEEEE"], max_output_bytes=1024))
    source = b"<AAAABBBBCCCCDDDDEEEE>"
    result = b"".join(capture.feed(source[n:n + chunk]) for n in range(0, len(source), chunk))
    result += capture.finish()
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


@pytest.mark.parametrize("literals,limit,reason", [
    ([], 1024, "bounded private literals"), ([None], 1024, "must be text"),
    ([SENTINEL], True, "positive output ceiling"), ([SENTINEL], 0, "positive output ceiling"),
])
def test_invalid_policy_is_refused_with_its_boundary_reason(literals, limit, reason):
    with pytest.raises(ValueError, match=reason):
        CapturePolicy(literals, max_output_bytes=limit)


@pytest.mark.parametrize("literal", ["short", SENTINEL + "x" * 1024, SENTINEL + "\ud800", "REDACTED"])
def test_rejected_private_literals_never_enter_error_details(literal):
    with pytest.raises(ValueError) as raised:
        CapturePolicy([literal], max_output_bytes=1024)
    assert literal not in str(raised.value)
    assert raised.value.__context__ is None


def test_nonblocking_empty_read_is_not_a_successful_end_of_stream():
    class Unready:
        def read(self, _count):
            return None
    with pytest.raises(OSError, match="did not return byte data"):
        capture_text(Unready(), io.BytesIO(), CapturePolicy([SENTINEL], max_output_bytes=1024))


def test_an_overlapping_chain_stays_bounded_and_has_no_raw_suffix():
    capture = TextCapture(CapturePolicy(["AAAAAAAA"], max_output_bytes=1024))
    output = bytearray()
    for _ in range(8192):
        output.extend(capture.feed(b"A"))
        assert sum(map(len, capture._chunks)) <= 128
    output.extend(capture.finish())
    assert output == b"[REDACTED]"
    assert capture.statistics()["redactions"] == 1


def test_separate_occurrences_keep_their_clear_text_separator():
    stream = io.BytesIO(b"<sentinel-12345678>-<sentinel-12345678>")
    target = io.BytesIO()
    stats = capture_text(stream, target, CapturePolicy(["sentinel-12345678"], max_output_bytes=1024), chunk_bytes=1)
    assert target.getvalue() == b"<[REDACTED]>-<[REDACTED]>"
    assert stats["redactions"] == 2


def test_late_terminal_controls_omit_pending_output_and_report_a_gap():
    capture = TextCapture(CapturePolicy(["12345678"], max_output_bytes=1024))
    assert capture.feed(b"A" * 128) == b"A" * 113
    assert capture.feed(b"\x1b[31mred") + capture.finish() == b""
    assert capture.statistics()["omitted_nontext"] is True


def test_direct_feed_cannot_exceed_the_input_buffer_budget():
    capture = TextCapture(CapturePolicy([SENTINEL], max_output_bytes=1024))
    with pytest.raises(ValueError, match="exceeds one MiB"):
        capture.feed(b"A" * (1024 * 1024 + 1))


@pytest.mark.parametrize("chunk", [1, 3, 64, 1024])
def test_streaming_union_matches_an_independent_whole_buffer_oracle(chunk):
    import random

    rng = random.Random(2383)
    literals = ["AAAABBBB", "BBBBCCCC", "CCCCDDDD", "AAAAAAAA", "ABCDABCD"]
    for _ in range(40):
        text = "".join(rng.choice("ABCD") for _ in range(32))
        text += rng.choice(literals) + rng.choice(literals)
        text += "AAAABBBBCCCCDDDD" + "A" * rng.randrange(8, 40)
        # Batch oracle: enumerate every literal occurrence before merging.
        intervals = sorted((start, start + len(value)) for start in range(len(text))
                           for value in literals if text.startswith(value, start))
        union = []
        for start, end in intervals:
            if union and start < union[-1][1]:
                union[-1] = (union[-1][0], max(union[-1][1], end))
            else:
                union.append((start, end))
        expected, cursor = [], 0
        for start, end in union:
            expected.extend((text[cursor:start], "[REDACTED]"))
            cursor = end
        expected.append(text[cursor:])
        target = io.BytesIO()
        capture_text(io.BytesIO(text.encode()), target,
                     CapturePolicy(literals, max_output_bytes=4096), chunk_bytes=chunk)
        assert target.getvalue() == "".join(expected).encode()

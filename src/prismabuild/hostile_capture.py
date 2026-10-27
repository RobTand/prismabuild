"""Bounded text capture sanitizes bytes before handing them to a durable sink.

The caller is a trusted collector outside the payload. This component does not
isolate a payload or authenticate its output and is not yet wired to a worker.
"""
from __future__ import annotations

import base64
import codecs
from collections import deque
import json
import re
from urllib.parse import quote_from_bytes


ALGORITHM = "prismabuild.capture.literal-redaction.v2"
REPLACEMENT = b"[REDACTED]"
MAX_CHUNK_BYTES = 1024 * 1024


class _LiteralMatcher:
    """A bounded Aho–Corasick table; no private pattern enters a global cache."""

    def __init__(self, patterns):
        self.longest = max(map(len, patterns))
        self.edges, self.failure, self.length = [{}], [0], [0]
        for pattern in sorted(patterns):
            state = 0
            for byte in pattern:
                if byte not in self.edges[state]:
                    self.edges[state][byte] = len(self.edges)
                    self.edges.append({})
                    self.failure.append(0)
                    self.length.append(0)
                state = self.edges[state][byte]
            self.length[state] = max(self.length[state], len(pattern))
        pending = deque(self.edges[0].values())
        while pending:
            state = pending.popleft()
            for byte, child in self.edges[state].items():
                pending.append(child)
                fallback = self.failure[state]
                while fallback and byte not in self.edges[fallback]:
                    fallback = self.failure[fallback]
                self.failure[child] = self.edges[fallback].get(byte, 0)
                self.length[child] = max(self.length[child], self.length[self.failure[child]])

    def match_byte(self, state, byte):
        while state and byte not in self.edges[state]:
            state = self.failure[state]
        return self.edges[state].get(byte, 0)


class CapturePolicy:
    """Private literal patterns; repr and statistics expose no secret values."""

    def __init__(self, literals, *, max_output_bytes: int):
        if (not isinstance(literals, (list, tuple)) or not 1 <= len(literals) <= 64
                or type(max_output_bytes) is not int or max_output_bytes <= 0):
            raise ValueError("capture needs bounded private literals and a positive output ceiling")
        patterns = set()
        for literal in literals:
            if not isinstance(literal, str):
                raise ValueError("capture literals must be text")
            if any(0xD800 <= ord(char) <= 0xDFFF for char in literal):
                raise ValueError("capture literals must be valid UTF-8 text")
            raw = literal.encode("utf-8", errors="strict")
            if not 8 <= len(raw) <= 1024:
                raise ValueError("capture literal byte length must be between 8 and 1024")
            quoted = quote_from_bytes(raw, safe="").encode("ascii")
            quoted_lower = re.sub(rb"%[0-9A-F]{2}", lambda m: m[0].lower(), quoted)
            patterns.update((raw, quoted, quoted_lower, raw.hex().encode("ascii"),
                             raw.hex().upper().encode("ascii"),
                             json.dumps(literal, ensure_ascii=True)[1:-1].encode("ascii")))
            for encoder in (base64.b64encode, base64.urlsafe_b64encode):
                encoded = encoder(raw)
                patterns.update((encoded, encoded.rstrip(b"=")))
        if sum(map(len, patterns)) > 512 * 1024:
            raise ValueError("capture pattern budget exceeded")
        self._matcher = _LiteralMatcher(patterns)
        state = 0
        for byte in REPLACEMENT:
            state = self._matcher.match_byte(state, byte)
            if self._matcher.length[state]:
                raise ValueError("capture replacement conflicts with the private policy")
        self.max_output_bytes = max_output_bytes


def _utf8_boundary(value: bytes, cut: int) -> int:
    while 0 < cut < len(value) and value[cut] & 0xC0 == 0x80:
        cut -= 1
    return cut


class TextCapture:
    def __init__(self, policy: CapturePolicy):
        self._policy = policy
        self._decoder = codecs.getincrementaldecoder("utf-8")("strict")
        self._chunks = deque()
        self._head_offset = 0
        self._spans = deque()
        self._state = 0
        self._seen = self._consumed = self._redacted_until = 0
        self._ring = bytearray(policy._matcher.longest + 4)
        self._finished = False
        self._input_bytes = 0
        self._emitted_bytes = 0
        self._redactions = 0
        self._omitted_nontext = False
        self._truncated = False

    def _accept(self, chunk: bytes, *, final: bool) -> bool:
        try:
            decoded = self._decoder.decode(chunk, final=final)
            if any(ord(char) < 32 and char not in "\t\n\r" or 127 <= ord(char) <= 159 for char in decoded):
                raise ValueError("binary control byte")
        except (UnicodeError, ValueError):
            self._omitted_nontext = True
            self._clear_raw()
            return False
        data = decoded.encode("utf-8")
        if data:
            self._chunks.append(data)
        matcher = self._policy._matcher
        for byte in data:
            self._ring[self._seen % len(self._ring)] = byte
            self._seen += 1
            self._state = matcher.match_byte(self._state, byte)
            length = matcher.length[self._state]
            if not length:
                continue
            start, end = self._seen - length, self._seen
            while self._spans and start < self._spans[-1][1]:
                start = min(start, self._spans.pop()[0])
            if start < self._redacted_until:
                self._redacted_until = end
            else:
                self._spans.append((start, end))
        return True

    def _clear_raw(self):
        self._chunks.clear()
        self._spans.clear()
        self._decoder.reset()
        self._head_offset = 0
        self._consumed = self._seen

    def _consume(self, count, *, retain):
        pieces = []
        while count:
            chunk = self._chunks[0]
            take = min(count, len(chunk) - self._head_offset)
            if retain:
                pieces.append(chunk[self._head_offset:self._head_offset + take])
            count -= take
            self._consumed += take
            self._head_offset += take
            if self._head_offset == len(chunk):
                self._chunks.popleft()
                self._head_offset = 0
        return b"".join(pieces)

    def _emit(self, *, final: bool) -> bytes:
        # Keep the possible beginning of every future match. Known redacted
        # spans may be consumed immediately, including an arbitrarily long
        # chain of overlapping matches; its raw bytes never accumulate.
        if self._redacted_until > self._consumed:
            self._consume(self._redacted_until - self._consumed, retain=False)
        cut = self._seen if final else max(0, self._seen - self._policy._matcher.longest + 1)
        while self._consumed < cut < self._seen and self._ring[cut % len(self._ring)] & 0xC0 == 0x80:
            cut -= 1
        pieces = []
        while self._spans and self._spans[0][0] < cut:
            start, end = self._spans.popleft()
            pieces.append(self._consume(start - self._consumed, retain=True))
            self._consume(end - start, retain=False)
            pieces.append(REPLACEMENT)
            self._redactions += 1
            self._redacted_until = end
        if cut > self._consumed:
            pieces.append(self._consume(cut - self._consumed, retain=True))
        return self._limit(b"".join(pieces))

    def _limit(self, sanitized: bytes) -> bytes:
        """Apply the retained-byte ceiling after sanitization."""
        room = max(0, self._policy.max_output_bytes - self._emitted_bytes)
        if len(sanitized) > room:
            self._truncated = True
            sanitized = sanitized[:_utf8_boundary(sanitized, room)]
            self._clear_raw()
        self._emitted_bytes += len(sanitized)
        return sanitized

    def feed(self, chunk: bytes) -> bytes:
        if self._finished:
            raise ValueError("capture is already finished")
        if not isinstance(chunk, bytes):
            raise TypeError("capture consumes bytes")
        if len(chunk) > MAX_CHUNK_BYTES:
            raise ValueError("capture input chunk exceeds one MiB")
        self._input_bytes += len(chunk)
        if self._omitted_nontext:
            return b""
        if self._truncated:
            return b""
        return self._emit(final=False) if self._accept(chunk, final=False) else b""

    def finish(self) -> bytes:
        if self._finished:
            return b""
        self._finished = True
        if self._omitted_nontext or self._truncated or not self._accept(b"", final=True):
            self._clear_raw()
            return b""
        return self._emit(final=True)

    def statistics(self) -> dict[str, object]:
        return {"algorithm": ALGORITHM, "input_bytes": self._input_bytes,
                "emitted_bytes": self._emitted_bytes, "redactions": self._redactions,
                "omitted_nontext": self._omitted_nontext, "truncated": self._truncated,
                "finished": self._finished}


def capture_text(source, sink, policy: CapturePolicy, *, chunk_bytes=65536) -> dict[str, object]:
    """Write only sanitized text; raw input never reaches the supplied sink."""
    if type(chunk_bytes) is not int or not 1 <= chunk_bytes <= MAX_CHUNK_BYTES:
        raise ValueError("capture chunk size must be within one MiB")
    capture = TextCapture(policy)
    def write(value):
        while value:
            count = sink.write(value)
            if type(count) is not int or not 0 < count <= len(value):
                raise OSError("sanitized capture sink made no valid progress")
            value = value[count:]
    while True:
        chunk = source.read(chunk_bytes)
        if not isinstance(chunk, bytes):
            raise OSError("capture source did not return byte data")
        if not chunk:
            break
        write(capture.feed(chunk))
    write(capture.finish())
    return capture.statistics()

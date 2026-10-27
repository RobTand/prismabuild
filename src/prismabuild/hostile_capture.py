"""Bounded text capture sanitizes bytes before handing them to a durable sink.

The caller is a trusted collector outside the payload. This component does not
isolate a payload or authenticate its output and is not yet wired to a worker.
"""
from __future__ import annotations

import base64
import codecs
import json
import re
from urllib.parse import quote_from_bytes


ALGORITHM = "prismabuild.capture.literal-redaction.v1"
REPLACEMENT = b"[REDACTED]"


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
        ordered = sorted(patterns, key=lambda value: (-len(value), value))
        self._pattern = re.compile(b"|".join(re.escape(value) for value in ordered))
        self._longest = max(map(len, ordered))
        self.max_output_bytes = max_output_bytes


def _utf8_boundary(value: bytes, cut: int) -> int:
    while 0 < cut < len(value) and value[cut] & 0xC0 == 0x80:
        cut -= 1
    return cut


class TextCapture:
    def __init__(self, policy: CapturePolicy):
        self._policy = policy
        self._decoder = codecs.getincrementaldecoder("utf-8")("strict")
        self._pending = b""
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
            self._pending = b""
            return False
        self._pending += decoded.encode("utf-8")
        return True

    def _emit(self, *, final: bool) -> bytes:
        cut = len(self._pending) if final else max(0, len(self._pending) - self._policy._longest + 1)
        cut = _utf8_boundary(self._pending, cut)
        for match in self._policy._pattern.finditer(self._pending):
            if match.start() < cut < match.end():
                cut = match.start()
                break
        safe, self._pending = self._pending[:cut], self._pending[cut:]
        sanitized, count = self._policy._pattern.subn(REPLACEMENT, safe)
        self._redactions += count
        room = max(0, self._policy.max_output_bytes - self._emitted_bytes)
        if len(sanitized) > room:
            self._truncated = True
            sanitized = sanitized[:_utf8_boundary(sanitized, room)]
        self._emitted_bytes += len(sanitized)
        return sanitized

    def feed(self, chunk: bytes) -> bytes:
        if self._finished:
            raise ValueError("capture is already finished")
        if not isinstance(chunk, bytes):
            raise TypeError("capture consumes bytes")
        self._input_bytes += len(chunk)
        if self._omitted_nontext:
            return b""
        if self._truncated:
            self._pending = b""
            return b""
        return self._emit(final=False) if self._accept(chunk, final=False) else b""

    def finish(self) -> bytes:
        if self._finished:
            return b""
        self._finished = True
        if self._omitted_nontext or self._truncated or not self._accept(b"", final=True):
            self._pending = b""
            return b""
        return self._emit(final=True)

    def statistics(self) -> dict[str, object]:
        return {"algorithm": ALGORITHM, "input_bytes": self._input_bytes,
                "emitted_bytes": self._emitted_bytes, "redactions": self._redactions,
                "omitted_nontext": self._omitted_nontext, "truncated": self._truncated,
                "finished": self._finished}


def capture_text(source, sink, policy: CapturePolicy, *, chunk_bytes=65536) -> dict[str, object]:
    """Write only sanitized text; raw input never reaches the supplied sink."""
    if type(chunk_bytes) is not int or not 1 <= chunk_bytes <= 1024 * 1024:
        raise ValueError("capture chunk size must be within one MiB")
    capture = TextCapture(policy)
    def write(value):
        while value:
            count = sink.write(value)
            if type(count) is not int or not 0 < count <= len(value):
                raise OSError("sanitized capture sink made no valid progress")
            value = value[count:]
    while chunk := source.read(chunk_bytes):
        write(capture.feed(chunk))
    write(capture.finish())
    return capture.statistics()

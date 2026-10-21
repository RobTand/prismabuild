"""Event-driven role fixtures; the per-test alarm owns hang detection."""
import os


def read_until_output(child, marker: bytes) -> bytes:
    output = bytearray()
    while marker not in output:
        chunk = os.read(child.stdout.fileno(), 65536)
        assert chunk, bytes(output).decode(errors="replace")
        output.extend(chunk)
    return bytes(output)


def stop_role_child(child) -> None:
    if child.poll() is None:
        child.kill()
    child.stdout.close()
    child.wait()

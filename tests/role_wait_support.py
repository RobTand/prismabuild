"""Shared role output and child cleanup fixtures."""
import os
import selectors


def read_until_output(child, marker: bytes) -> bytes:
    output = bytearray()
    with selectors.DefaultSelector() as selector:
        selector.register(child.stdout, selectors.EVENT_READ)
        while marker not in output:
            assert selector.select(5), bytes(output).decode(errors="replace")
            chunk = os.read(child.stdout.fileno(), 65536)
            assert chunk, bytes(output).decode(errors="replace")
            output.extend(chunk)
    return bytes(output)


def stop_role_child(child) -> None:
    if child.poll() is None:
        child.kill()
    child.communicate(timeout=5)

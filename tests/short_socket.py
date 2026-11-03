"""Private AF_UNIX endpoints independent of the test workspace depth."""
from contextlib import contextmanager
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from collections.abc import Iterator


@contextmanager
def short_socket_path() -> Iterator[Path]:
    """Keep the directory alive until the caller has closed its server."""
    with TemporaryDirectory(prefix="pb-", dir="/tmp") as directory:
        endpoint = Path(directory) / "socket"
        length = len(os.fsencode(endpoint))
        if length > 107:
            raise ValueError(f"AF_UNIX endpoint exceeds 107 encoded bytes: {length} bytes")
        yield endpoint

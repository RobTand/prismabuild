"""A rejected ingestion closes every descriptor it opened.

Issue #90: `_copy_to_staging` allocated the staging descriptor with
`tempfile.mkstemp` and only handed it to `os.fdopen` after the regular-file
check.  A directory or FIFO source is refused between those two points, so the
descriptor stayed open on an unlinked staging file and the caller's descriptor
count grew by one per rejected input.  The staging directory looked empty, so
filesystem cleanup alone did not reveal it.  In a long-lived Python caller,
repeated invalid inputs exhausted the descriptor limit and blocked later valid
work.

`_copy_to_staging` is also the copy step in `publish_result`, so both callers
are covered by the one fix.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import core as pb  # noqa: E402

REJECTIONS = 32


def _open_descriptors() -> dict[int, str]:
    result: dict[int, str] = {}
    for path in Path("/proc/self/fd").iterdir():
        try:
            result[int(path.name)] = os.readlink(path)
        except (FileNotFoundError, OSError):
            continue
    return result


def _reject(cas: pb.PrismaBuildCAS, source: Path, input_id: str) -> None:
    with pytest.raises(pb.LocalActionError):
        cas.ingest_input(source, input_id=input_id)


@pytest.mark.parametrize("kind", ["directory", "fifo"])
def test_repeated_rejected_inputs_leave_the_descriptor_count_unchanged(
    tmp_path: Path, kind: str,
) -> None:
    """Refusing an unsupported source must not cost a descriptor each time."""

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    source = tmp_path / f"{kind}-input"
    if kind == "directory":
        source.mkdir()
    else:
        os.mkfifo(source)

    # One rejection first, so the CAS has created every directory it needs and
    # the baseline is not measured across that setup.
    _reject(cas, source, f"invalid-{kind}")
    before = _open_descriptors()
    for _ in range(REJECTIONS):
        _reject(cas, source, f"invalid-{kind}")
    after = _open_descriptors()

    leaked = {fd: target for fd, target in after.items() if fd not in before}
    assert leaked == {}, f"{len(leaked)} staging descriptors leaked: {leaked}"
    assert list((cas.root / ".staging").iterdir()) == []


def test_a_regular_input_still_ingests_after_repeated_rejections(
    tmp_path: Path,
) -> None:
    """The refusals must not leave the CAS unable to do valid work."""

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    directory = tmp_path / "directory-input"
    directory.mkdir()
    fifo = tmp_path / "fifo-input"
    os.mkfifo(fifo)
    for _ in range(REJECTIONS):
        _reject(cas, directory, "invalid-directory")
        _reject(cas, fifo, "invalid-fifo")

    payload = tmp_path / "payload.bin"
    payload.write_bytes(b"valid ingest\n")
    entry, won = cas.ingest_input(payload, input_id="model/valid")

    assert won
    assert entry["id"] == "model/valid"
    assert cas.input_path(entry).read_bytes() == b"valid ingest\n"


def test_rejected_source_closes_staged_inode_before_unlink(tmp_path, monkeypatch):
    closed = set()
    observed = []
    real_close, real_unlink = os.close, os.unlink

    def close(descriptor):
        info = os.fstat(descriptor)
        closed.add((info.st_dev, info.st_ino))
        return real_close(descriptor)

    def unlink(path, *, dir_fd=None):
        if str(path).startswith(".payload."):
            info = os.stat(path, dir_fd=dir_fd, follow_symlinks=False)
            observed.append((info.st_dev, info.st_ino) in closed)
        return real_unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(pb.os, "close", close)
    monkeypatch.setattr(pb.os, "unlink", unlink)
    source = tmp_path / "directory"
    source.mkdir()
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    _reject(cas, source, "test/rejected")
    assert observed == [True]
